package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func NewCheckHealth(version string) *CheckHealth {
	return &CheckHealth{version: version}
}

type CheckHealth struct {
	version string
}

func (h *CheckHealth) Init(c gin.IRouter) {
	c.GET("check_health", func(context *gin.Context) {
		context.JSON(http.StatusOK, map[string]string{"version": h.version})
	})
}
