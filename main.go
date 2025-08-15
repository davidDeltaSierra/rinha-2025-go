package main

import (
	"log"
	"os"
	"rinha/server"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/panjf2000/gnet/v2"
)

func main() {
	GOMAXPROCS, _ := strconv.Atoi(os.Getenv("GOMAXPROCS"))
	runtime.GOMAXPROCS(GOMAXPROCS)
	debug.SetGCPercent(-1)
	var sock = os.Getenv("SOCK")
	os.Remove(sock)
	echo := server.NewHttpEventHandler(sock)
	log.Fatal(gnet.Run(echo, "unix://"+sock, gnet.WithMulticore(true)))
}
