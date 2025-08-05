package rest

import (
	"github.com/valyala/fasthttp"
	"log"
	"net"
	"sync/atomic"
	"time"
)

var (
	backends = []string{
		"/tmp/app1.sock",
		"/tmp/app2.sock",
	}
	currentIndex uint32
	clients      []*fasthttp.HostClient
)

func InitializeClients() {
	for _, path := range backends {
		p := path
		c := fasthttp.HostClient{
			IsTLS:               false,
			MaxConns:            550,
			MaxIdleConnDuration: 60 * time.Second,
			Dial: func(addr string) (net.Conn, error) {
				return net.Dial("unix", p)
			},
		}
		clients = append(clients, &c)
	}
}

func getNextClient() *fasthttp.HostClient {
	index := atomic.AddUint32(&currentIndex, 1)
	return clients[index%uint32(len(clients))]
}

func GatewayHandler(ctx *fasthttp.RequestCtx) {
	client := getNextClient()
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	ctx.Request.URI().CopyTo(req.URI())
	req.Header.SetMethodBytes(ctx.Method())
	req.SetBodyRaw(ctx.Request.Body())
	ctx.Request.Header.CopyTo(&req.Header)
	if err := client.Do(req, resp); err != nil {
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
