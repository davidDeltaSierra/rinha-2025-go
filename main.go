package main

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"os"
	"rinha/rest"
	"time"
)

func main() {
	isMaster := os.Getenv("GATEWAY")
	if isMaster == "true" {
		fmt.Println("init gateway")
		time.Sleep(2 * time.Second)
		rest.SetupMaster()
		if err := fasthttp.ListenAndServe(":8080", router); err != nil {
			panic(err)
		}
		return
	}
	fmt.Println("init worker")
	rest.SetupWorker()
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
