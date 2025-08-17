package rest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"rinha/database"
	"strconv"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/valyala/fasthttp"
)

const pDefault = "http://payment-processor-default:8080"
const pFallback = "http://payment-processor-fallback:8080"

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

var workers, _ = strconv.Atoi(os.Getenv("WORKERS"))

var paymentDTOPool = sync.Pool{
	New: func() any {
		return new(paymentDTO)
	},
}
var paymentProcessorBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}
var paymentsChannel = make(chan *paymentDTO, 64000)
var paymentProcessorClient = fasthttp.Client{
	MaxIdleConnDuration: 60 * time.Second,
}

var defaultHealthBroadcast = NewHealthBroadcast()

func SetupAPI() {
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			sentinel := workerID == 0
			updates := defaultHealthBroadcast.Subscribe()
			singleMonitorCopy := &Monitor{true, false}

			go func() {
				for m := range updates {
					singleMonitorCopy = m
				}
			}()

			for payment := range paymentsChannel {
				payment.RequestedAt = time.Now().UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
				paymentHandler(singleMonitorCopy, sentinel, payment, nil, nil, nil)
			}
		}(i)
	}
}

func PaymentsController(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusAccepted)
	payment := paymentDTOPool.Get().(*paymentDTO)
	_ = json.Unmarshal(ctx.PostBody(), payment)
	paymentsChannel <- payment
}

func PaymentSummaryController(ctx *fasthttp.RequestCtx) {
	from := string(ctx.QueryArgs().Peek("from"))
	to := string(ctx.QueryArgs().Peek("to"))
	time.Sleep(500 * time.Millisecond)
	summary, err := getPaymentsSummary(from, to)
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

func tryPay(gateway string, body *paymentDTO, req *fasthttp.Request, resp *fasthttp.Response, buff *bytes.Buffer) (error, *fasthttp.Request, *fasthttp.Response, *bytes.Buffer) {
	if req == nil {
		buff = paymentProcessorBufferPool.Get().(*bytes.Buffer)
		_ = json.NewEncoder(buff).Encode(body)
		req = fasthttp.AcquireRequest()
		resp = fasthttp.AcquireResponse()
		req.SetRequestURI(gateway + "/payments")
		req.Header.SetMethod(fasthttp.MethodPost)
		req.Header.SetContentType("application/json")
		req.SetBody(buff.Bytes())
	}
	err := paymentProcessorClient.Do(req, resp)
	if err != nil {
		return err, req, resp, buff
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		return errors.New("Status code: " + strconv.Itoa(resp.StatusCode())), req, resp, buff
	}
	return nil, req, resp, buff
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

func paymentHandler(monitor *Monitor, sentinel bool, body *paymentDTO, req *fasthttp.Request, resp *fasthttp.Response, buff *bytes.Buffer) {
	var err error
	var handle string
	for {
		for !sentinel && !monitor.Critical && !monitor.Health {
			time.Sleep(time.Millisecond)
		}
		err, req, resp, buff = tryPay(pDefault, body, req, resp, buff)
		if err != nil {
			defaultHealthBroadcast.SetHealth(false)
			if !monitor.Critical {
				if sentinel {
					time.Sleep(10 * time.Millisecond)
				}
				continue
			}
			err, req, resp, buff = tryPay(pFallback, body, req, resp, buff)
			if err != nil {
				time.Sleep(10 * time.Millisecond)
				continue
			} else {
				handle = "fallback"
			}
		} else {
			handle = "default"
		}
		if sentinel && handle == "default" {
			defaultHealthBroadcast.SetHealth(true)
		}
		err = savePayment(body, handle)
		if err != nil {
			fmt.Println("Erro ao salvar pagamento:", err)
		}
		buff.Reset()
		paymentProcessorBufferPool.Put(buff)
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		*body = paymentDTO{}
		paymentDTOPool.Put(body)
		break
	}
}
