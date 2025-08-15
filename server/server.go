package server

import (
	"bytes"
	"fmt"
	"os"
	"rinha/rest"
	"sync"

	"github.com/panjf2000/gnet/v2"
)

var (
	successResp = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	healthPath  = []byte("/healthcheck")
	summaryPath = []byte("/payments-summary")
	paymentPath = []byte("/payments")
	bodyPool    = sync.Pool{
		New: func() any {
			return make([]byte, 0, 256)
		},
	}
	jobQueue = make(chan []byte, 32000)
)

func NewHttpEventHandler(sock string) *HttpEventHandler {
	return &HttpEventHandler{
		sock: sock,
	}
}

type HttpEventHandler struct {
	gnet.BuiltinEventEngine
	sock string
}

func (s *HttpEventHandler) OnBoot(eng gnet.Engine) gnet.Action {
	err := os.Chmod(s.sock, 0666)
	if err != nil {
		panic(err)
	}
	go func() {
		for buf := range jobQueue {
			headersEnd := bytes.Index(buf, []byte("\r\n\r\n"))
			var body []byte
			if headersEnd >= 0 && headersEnd+4 < len(buf) {
				body = buf[headersEnd+4:]
			}
			if len(body) > 50 {
				rest.PaymentsController(body, func() {
					bodyPool.Put(buf[:0])
				})
				continue
			}
			bodyPool.Put(buf[:0])
		}
	}()
	return gnet.None
}

func (s *HttpEventHandler) OnTraffic(c gnet.Conn) gnet.Action {
	data, _ := c.Next(-1)
	i := bytes.IndexByte(data, '\n')
	firstLine := data[:i]
	if bytes.Contains(firstLine, healthPath) {
		c.AsyncWrite(successResp, nil)
		return gnet.None
	}
	if bytes.Contains(firstLine, summaryPath) {
		qIdx := bytes.IndexByte(firstLine, '?')
		var from, to []byte
		if qIdx >= 0 {
			query := firstLine[qIdx+1:]
			from = getQueryParam(query, []byte("from"))
			to = getQueryParam(query, []byte("to"))
		}
		body := rest.PaymentSummaryController(string(from), string(to))
		resp := []byte("HTTP/1.1 200 OK\r\n")
		resp = append(resp, []byte(fmt.Sprintf("Content-Length: %d\r\n", len(body)))...)
		resp = append(resp, []byte("Content-Type: application/json\r\n\r\n")...)
		resp = append(resp, body...)
		c.AsyncWrite(resp, nil)
		return gnet.None
	}
	if bytes.Contains(firstLine, paymentPath) {
		c.AsyncWrite(successResp, nil)
		buf := bodyPool.Get().([]byte)
		buf = append(buf, data...)
		jobQueue <- buf
	}
	return gnet.None
}

func getQueryParam(query, key []byte) []byte {
	keyLen := len(key)
	if keyLen == 0 {
		return nil
	}
	for i := 0; i <= len(query)-keyLen-1; i++ {
		if query[i] == key[0] && bytes.HasPrefix(query[i:], key) && i+keyLen < len(query) && query[i+keyLen] == '=' {
			start := i + keyLen + 1
			for j := start; j < len(query); j++ {
				if query[j] == '&' || query[j] == ' ' {
					return query[start:j]
				}
			}
			return query[start:]
		}
	}
	return nil
}
