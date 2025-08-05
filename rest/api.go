package rest

import (
	"context"
	"errors"
	"github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/valyala/fasthttp"
	"rinha/database"
	"strconv"
	"sync"
	"time"
)

const pDefault = "http://payment-processor-default:8080"
const pFallback = "http://payment-processor-fallback:8080"

type paymentDTO struct {
	CorrelationID string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

type Summary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type PaymentSummary struct {
	Default  Summary `json:"default"`
	Fallback Summary `json:"fallback"`
}

var paymentProcessorClient = fasthttp.Client{
	ReadTimeout:         6 * time.Second,
	MaxIdleConnDuration: 60 * time.Second,
	MaxConnsPerHost:     2,
}
var paymentDTOChannel = make(chan *paymentDTO, 16000)
var paymentBodyChannel = make(chan []byte, 16000)

var paymentDTOPool = sync.Pool{
	New: func() any {
		return new(paymentDTO)
	},
}
var paymentBytePool = sync.Pool{
	New: func() any {
		b := make([]byte, 256)
		return &b
	},
}

func SetupAPI() {
	for i := 0; i < 1; i++ {
		go func() {
			for payment := range paymentDTOChannel {
				paymentHandler(payment)
			}
		}()
	}
	for i := 0; i < 2; i++ {
		go func() {
			for bytes := range paymentBodyChannel {
				payment := paymentDTOPool.Get().(*paymentDTO)
				json.Unmarshal(bytes, payment)
				payment.RequestedAt = time.Now().UTC()
				paymentDTOChannel <- payment
			}
		}()
	}
}

func PaymentsController(ctx *fasthttp.RequestCtx) {
	paymentBodyChannel <- ctx.Request.Body()
	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

func PaymentSummaryController(ctx *fasthttp.RequestCtx) {
	from := string(ctx.QueryArgs().Peek("from"))
	to := string(ctx.QueryArgs().Peek("to"))

	summary, err := getPaymentsSummary(&from, &to)
	if err != nil {
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetContentType("application/json")
	if err := json.NewEncoder(ctx).Encode(summary); err != nil {
		ctx.Error("Failed to encode response", fasthttp.StatusInternalServerError)
	}
}

func HealthcheckController(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func getPaymentsSummary(from *string, to *string) (*PaymentSummary, error) {
	db, err := database.GetConnectionPool()
	if err != nil {
		return nil, err
	}
	baseQuery := `
		SELECT handler, COUNT(*) as total_requests, SUM(amount) as total_amount
		FROM public.payments
	`
	if (from != nil && *from != "") && (to != nil && *to != "") {
		baseQuery += " WHERE created_at between $1 and $2 "
	}
	baseQuery += " GROUP BY handler"
	ctx := context.Background()
	var rows pgx.Rows
	if (from != nil && *from != "") && (to != nil && *to != "") {
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

func tryPay(body *paymentDTO, processor string) (err error) {
	bufPtr := paymentBytePool.Get().(*[]byte)
	data, err := json.Marshal(body)
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	if err != nil {
		goto cleanup
	}
	*bufPtr = append(*bufPtr, data...)
	req.SetRequestURI(processor + "/payments")
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	req.SetBody(*bufPtr)

	err = paymentProcessorClient.Do(req, resp)
	if err != nil {
		goto cleanup
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		err = errors.New("Status code: " + strconv.Itoa(resp.StatusCode()))
		goto cleanup
	}

cleanup:
	*bufPtr = (*bufPtr)[:0]
	paymentBytePool.Put(bufPtr)
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	return err
}

func savePayment(body *paymentDTO, handler string) error {
	connectionPool, err := database.GetConnectionPool()
	if err != nil {
		return err
	}
	_, err = connectionPool.Exec(
		context.Background(),
		"INSERT INTO public.payments (correlation_id, amount, handler, created_at) VALUES ($1, $2, $3, $4)",
		body.CorrelationID,
		body.Amount,
		handler,
		body.RequestedAt,
	)
	return nil
}

func paymentHandler(body *paymentDTO) {
	//@TODO AINDA AJUSTANDO ESSE MANO
	err := tryPay(body, pDefault)
	if err != nil {
		paymentHandler(body)
		return
	}
	savePayment(body, "default")
	*body = paymentDTO{}
	paymentDTOPool.Put(body)
}
