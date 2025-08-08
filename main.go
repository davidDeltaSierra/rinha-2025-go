package main

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"net"
	"os"
	"rinha/rest"
	"runtime"
	"runtime/debug"
	"strconv"
)

func main() {
	GOMAXPROCS, _ := strconv.Atoi(os.Getenv("GOMAXPROCS"))
	runtime.GOMAXPROCS(GOMAXPROCS)
	debug.SetGCPercent(-1)
	fmt.Println("init api")
	sock := os.Getenv("SOCK")
	os.Remove(sock)
	l, err := net.Listen("unix", sock)
	if err != nil {
		fmt.Println("Erro ao iniciar socket:", err)
		panic(err)
	}
	err = os.Chmod(sock, 0666)
	if err != nil {
		fmt.Println("Error ao liberar permissões:", err)
		panic(err)
	}
	rest.SetupAPI()
	if err := fasthttp.Serve(l, router); err != nil {
		panic(err)
	}
}

func router(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/payments":
		rest.PaymentsController(ctx)
	case "/payments-summary":
		rest.PaymentSummaryController(ctx)
	case "/healthcheck":
		rest.HealthcheckController(ctx)
	default:
		ctx.Error("Not Found", fasthttp.StatusNotFound)
	}
}
