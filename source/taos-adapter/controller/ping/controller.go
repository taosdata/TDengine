package ping

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/monitor"
)

type Controller struct {
}

func (c Controller) Init(r gin.IRouter) {
	r.GET("-/ping", func(c *gin.Context) {
		action := c.Query("action")
		if action == "query" {
			if monitor.QueryPaused() {
				c.Status(http.StatusServiceUnavailable)
				return
			}
			c.Status(http.StatusOK)
			return
		}
		if monitor.AllPaused() {
			c.Status(http.StatusServiceUnavailable)
			return
		}
		c.Status(http.StatusOK)
	})
}

func init() {
	r := &Controller{}
	controller.AddController(r)
}
