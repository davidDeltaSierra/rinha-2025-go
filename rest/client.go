package rest

import (
	"net"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

const pDefault = "http://payment-processor-default:8080"
const pFallback = "http://payment-processor-fallback:8080"

var (
	cachedIP string
	dialOnce sync.Once
	dialer   = func(addr string) (net.Conn, error) {
		dialOnce.Do(func() {
			ips, err := net.LookupIP("payment-processor-default")
			if err == nil && len(ips) > 0 {
				cachedIP = ips[0].String()
			}
		})
		if cachedIP == "" {
			return net.Dial("tcp", addr)
		}
		host, port, _ := net.SplitHostPort(addr)
		if host != "payment-processor-default" {
			return net.Dial("tcp", addr)
		}
		return net.Dial("tcp", net.JoinHostPort(cachedIP, port))
	}
	paymentProcessorClient = fasthttp.Client{
		MaxIdleConnDuration: 60 * time.Second,
		Dial:                dialer,
	}
)
