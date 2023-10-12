package main

import (
	"net"
	"net/http"
	"taosainternal/config"

	"github.com/gin-contrib/static"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/system"
)

var logger = log.GetLogger("main")

func main() {
	router := system.Init()
	router.StaticFile("/", "/usr/local/taos/share/admin/home.html")
	router.Use(static.Serve("/", static.LocalFile("/usr/local/taos/share/admin", false)))
	ssl := config.SSl{}
	ssl.SetValue()
	system.Start(router, func(server *http.Server) {
		ln, err := net.Listen("tcp4", server.Addr)
		if err != nil {
			logger.Fatalf("listen: %s\n", err)
		}
		if ssl.Enable {
			defer ln.Close()
			if err := server.ServeTLS(ln, ssl.CertFile, ssl.KeyFile); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("listen: %s\n", err)
			}
		} else {
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("listen: %s\n", err)
			}
		}
	})
}
