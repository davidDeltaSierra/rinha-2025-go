package rest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"rinha/database"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/valyala/fasthttp"
)

type paymentDTO struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

type Summary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type PaymentSummary struct {
	Default  Summary `json:"default"`
	Fallback Summary `json:"fallback"`
}

func PaymentsController(body []byte, flush func()) {
	id, amount := parsePayment(body)
	flush()
	paymentHandler(id, amount, nil, nil)
}

func PaymentSummaryController(from, to string) []byte {
	summary, _ := getPaymentsSummary(from, to)
	marshal, _ := json.Marshal(summary)
	return marshal
}

func getPaymentsSummary(from string, to string) (*PaymentSummary, error) {
	db, err := database.GetConnectionPool()
	if err != nil {
		return nil, err
	}
	baseQuery := `
		SELECT handler, COUNT(*) as total_requests, SUM(amount) as total_amount
		FROM public.payments
	`
	if (from != "") && (to != "") {
		baseQuery += " WHERE created_at between $1 and $2 "
	}
	baseQuery += " GROUP BY handler"
	ctx := context.Background()
	var rows pgx.Rows
	if (from != "") && (to != "") {
		rows, err = db.Query(ctx, baseQuery, from, to)
	} else {
		rows, err = db.Query(ctx, baseQuery)
	}
	if err != nil {
		return nil, err
	}
	summary := PaymentSummary{}
	for rows.Next() {
		var handler string
		var totalRequests int
		var totalAmount float64
		if err := rows.Scan(&handler, &totalRequests, &totalAmount); err != nil {
			return nil, err
		}
		switch handler {
		case "default":
			summary.Default = Summary{TotalRequests: totalRequests, TotalAmount: totalAmount}
		case "fallback":
			summary.Fallback = Summary{TotalRequests: totalRequests, TotalAmount: totalAmount}
		}
	}
	rows.Close()
	return &summary, nil
}

func tryPay(id string, amount float64, requestedAt string, req *fasthttp.Request, resp *fasthttp.Response) (error, *fasthttp.Request, *fasthttp.Response) {
	if req == nil {
		jsonStr := fmt.Sprintf(
			`{"correlationId":"%s","amount":%.2f,"requestedAt":"%s"}`,
			id,
			amount,
			requestedAt,
		)
		req = fasthttp.AcquireRequest()
		resp = fasthttp.AcquireResponse()
		req.SetRequestURI(pDefault + "/payments")
		req.Header.SetMethod(fasthttp.MethodPost)
		req.Header.SetContentType("application/json")
		req.SetBodyString(jsonStr)
	}
	err := paymentProcessorClient.Do(req, resp)
	if err != nil {
		return err, req, resp
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		return errors.New("Status code: " + strconv.Itoa(resp.StatusCode())), req, resp
	}
	return nil, req, resp
}

func savePayment(id string, amount float64, requestedAt string, handler string) error {
	connectionPool, err := database.GetConnectionPool()
	if err != nil {
		return err
	}
	_, err = connectionPool.Exec(
		context.Background(),
		"INSERT INTO public.payments (correlation_id, amount, handler, created_at) VALUES ($1, $2, $3, $4)",
		id,
		amount,
		handler,
		requestedAt,
	)
	return nil
}

func paymentHandler(id string, amount float64, req *fasthttp.Request, resp *fasthttp.Response) {
	var err error
	requestedAt := time.Now().UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
	for {
		err, req, resp = tryPay(id, amount, requestedAt, req, resp)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	err = savePayment(id, amount, requestedAt, "default")
	if err != nil {
		fmt.Println("Erro ao salvar pagamento:", err)
	}
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
}

func parsePayment(line []byte) (correlationID string, amount float64) {
	start := bytes.Index(line, []byte(`"correlationId":"`))
	if start == -1 {
		return "", 0
	}
	start += len(`"correlationId":"`)
	end := bytes.IndexByte(line[start:], '"')
	if end == -1 {
		return "", 0
	}
	cidBytes := line[start : start+end]
	correlationID = string(append([]byte(nil), cidBytes...))
	start = bytes.Index(line, []byte(`"amount":`))
	if start == -1 {
		return correlationID, 0
	}
	start += len(`"amount":`)
	end = bytes.IndexByte(line[start:], '}')
	if end == -1 {
		return correlationID, 0
	}
	amtBytes := line[start : start+end]
	amtStr := string(append([]byte(nil), amtBytes...))
	amount, _ = strconv.ParseFloat(amtStr, 64)
	return correlationID, amount
}
