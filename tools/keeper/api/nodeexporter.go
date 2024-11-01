package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/taosdata/taoskeeper/process"
)

type NodeExporter struct {
	processor *process.Processor
}

func NewNodeExporter(processor *process.Processor) *NodeExporter {
	return &NodeExporter{processor: processor}
}

func (z *NodeExporter) Init(c gin.IRouter) {
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(z.processor)
	c.GET("metrics", z.myMiddleware(promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))
}

func (z *NodeExporter) myMiddleware(next http.Handler) gin.HandlerFunc {
	return func(c *gin.Context) {
		z.processor.Process()
		// call Prometheus handler
		next.ServeHTTP(c.Writer, c.Request)
	}
}
