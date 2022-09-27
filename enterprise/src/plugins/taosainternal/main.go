package main

import (
	"net/http"
	"runtime"
	"taosainternal/config"

	_ "taosainternal/admin"

	"github.com/gin-contrib/static"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/system"
)

var logger = log.GetLogger("main")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	router := system.Init()
	router.StaticFile("/", "/usr/local/taos/share/admin/home.html")
	router.Use(static.Serve("/", static.LocalFile("/usr/local/taos/share/admin", false)))
	ssl := config.SSl{}
	ssl.SetValue()
	system.Start(router, func(server *http.Server) {
		if ssl.Enable {
			if err := server.ListenAndServeTLS(ssl.CertFile, ssl.KeyFile); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("listen: %s\n", err)
			}
		} else {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("listen: %s\n", err)
			}
		}
	})
}
