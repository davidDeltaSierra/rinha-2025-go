package rest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/valyala/fasthttp"
	"net"
	"os"
	"rinha/database"
	"sync"
	"time"
)

const socketPath = "/tmp/worker-sock/worker.sock"
const pDefault = "http://payment-processor-default:8080"
const pFallback = "http://payment-processor-fallback:8080"

var paymentBytePool = sync.Pool{
	New: func() any {
		b := make([]byte, 256)
		return &b
	},
}
var scannerBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 256)
		return &b
	},
}
var client = &fasthttp.Client{
	MaxIdleConnDuration: 60 * time.Second,
	MaxConnsPerHost:     2,
}
var paymentDTOChannel = make(chan *paymentDTO, 16000)
var coonChannel = make(chan net.Conn, 16000)

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

	err = client.Do(req, resp)
	if err != nil {
		goto cleanup
	}
	if resp.StatusCode() != fasthttp.StatusOK {
		err = errors.New("Status code: " + fasthttp.StatusMessage(resp.StatusCode()))
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

func paymentHandler(body *paymentDTO) error {
	err := tryPay(body, pDefault)
	if err != nil {
		return err
	}
	err = savePayment(body, "default")
	if err != nil {
		return err
	}
	*body = paymentDTO{}
	paymentDTOPool.Put(body)
	return nil
}

func paymentsConsumerHandler(payment *paymentDTO) {
	//@TODO AJUSTAR FLUXO PARA FALLBACK (TAVA BUGADO)
	err := paymentHandler(payment)
	if err != nil {
		paymentsConsumerHandler(payment)
	}
}

func SetupWorker() {
	os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Println("Erro ao iniciar socket:", err)
		panic(err)
	}
	err = os.Chmod(socketPath, 0666)
	if err != nil {
		fmt.Println("Error ao liberar permissões:", err)
		panic(err)
	}
	for i := 0; i < 2; i++ {
		go func() {
			for payment := range paymentDTOChannel {
				paymentsConsumerHandler(payment)
			}
		}()
	}
	for i := 0; i < 2; i++ {
		go func() {
			for conn := range coonChannel {
				err := handleConnection(conn)
				if err != nil {
					fmt.Println("error handling connection: ", err)
				}
			}
		}()
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão:", err)
			continue
		}
		coonChannel <- conn
	}
}

func handleConnection(conn net.Conn) (err error) {
	var bufPtr = scannerBufferPool.Get().(*[]byte)
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(*bufPtr, 256)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Bytes()
		payment := paymentDTOPool.Get().(*paymentDTO)
		if err = json.Unmarshal(line, payment); err != nil {
			goto cleanup
		}
		payment.RequestedAt = time.Now().UTC()
		paymentDTOChannel <- payment
	}
	if err = scanner.Err(); err != nil {
		goto cleanup
	}
cleanup:
	conn.Close()
	*bufPtr = (*bufPtr)[:0]
	scannerBufferPool.Put(bufPtr)
	return err
}
