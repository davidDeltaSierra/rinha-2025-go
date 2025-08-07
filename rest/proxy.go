package rest

import (
	"github.com/valyala/fasthttp"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	backends = []string{
		"/tmp/app1/unix.sock",
		"/tmp/app2/unix.sock",
	}
	currentIndex           uint32
	clients                []*fasthttp.HostClient
	gatewayPostBodyChannel = make(chan []byte, 32000)
	gatewayPostBodyPool    = sync.Pool{
		New: func() any {
			return make([]byte, 0, 256)
		},
	}
)

func InitializeClients() {
	for _, path := range backends {
		c := fasthttp.HostClient{
			IsTLS:               false,
			MaxConns:            550,
			MaxIdleConnDuration: 2 * time.Minute,
			Dial: func(p string) fasthttp.DialFunc {
				return func(addr string) (net.Conn, error) {
					return net.Dial("unix", p)
				}
			}(path),
		}
		clients = append(clients, &c)
	}
	for i := 0; i < 4; i++ {
		go func() {
			for body := range gatewayPostBodyChannel {
				gatewayPostBodyHandler(body)
			}
		}()
	}
}

func roundRobin() *fasthttp.HostClient {
	index := atomic.AddUint32(&currentIndex, 1)
	return clients[index%uint32(len(clients))]
}

func GatewayHandler(ctx *fasthttp.RequestCtx) {
	if ctx.IsPost() {
		buf := gatewayPostBodyPool.Get().([]byte)
		body := ctx.Request.Body()
		buf = buf[:len(body)]
		copy(buf, body)
		gatewayPostBodyChannel <- buf
		ctx.SetStatusCode(fasthttp.StatusAccepted)
		return
	}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	ctx.Request.URI().CopyTo(req.URI())
	req.Header.SetMethodBytes(ctx.Method())
	req.SetBodyRaw(ctx.Request.Body())
	if err := roundRobin().Do(req, resp); err != nil {
		ctx.Error("Backend request failed", fasthttp.StatusServiceUnavailable)
		log.Printf("Erro ao encaminhar: %v", err)
	} else {
		ctx.Response.SetStatusCode(resp.StatusCode())
		resp.Header.CopyTo(&ctx.Response.Header)
		ctx.Response.SetBodyRaw(resp.Body())
	}
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
}

func gatewayPostBodyHandler(body []byte) {
	client := roundRobin()
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetBodyRaw(body)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.URI().SetPath("/payments")
	req.SetHost("sock")
	err := client.Do(req, resp)
	if err != nil {
		log.Printf("Erro ao encaminhar: %v", err)
	}
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	gatewayPostBodyPool.Put(body[:0])
}
