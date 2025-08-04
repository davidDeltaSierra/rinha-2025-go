package rest

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/valyala/fasthttp"
	"log"
	"net"
	"rinha/database"
	"sync"
	"time"
)

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

var paymentDTOPool = sync.Pool{
	New: func() any {
		return new(paymentDTO)
	},
}

var unixBytePool = sync.Pool{
	New: func() any {
		b := make([]byte, 256)
		return &b
	},
}
var dispatcher = make(chan []byte, 32000)
var unixConn net.Conn

func PaymentsController(ctx *fasthttp.RequestCtx) {
	dispatcher <- ctx.PostBody()
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

func SetupMaster() {
	var err error
	unixConn, err = net.Dial("unix", socketPath)
	if err != nil {
		fmt.Println("Erro ao conectar ao socket:", err)
		panic(err)
	}
	go func() {
		for body := range dispatcher {
			bufPtr := unixBytePool.Get().(*[]byte)
			buf := append((*bufPtr)[:0], body...)
			buf = append(buf, '\n')
			_, err := unixConn.Write(buf)
			if err != nil {
				log.Println("Erro ao enviar dados para o worker:", err)
			}
			*bufPtr = buf[:0]
			unixBytePool.Put(bufPtr)
		}
	}()
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
	summary := &PaymentSummary{}
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
	return summary, nil
}
