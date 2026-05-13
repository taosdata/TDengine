package main

// @title taosAdapter
// @version 1.0
// @description taosAdapter restful API

// @host http://127.0.0.1:6041
// @query.collection.format multi

import (
	"net"
	"net/http"

	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/system"
)

var logger = log.GetLogger("TCP")

func main() {
	r := system.Init()
	system.Start(r, func(server *http.Server) {
		ln, err := net.Listen("tcp", server.Addr)
		if err != nil {
			logger.Fatalf("listen: %s", err)
		}
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("listen: %s", err)
		}
	})
}
