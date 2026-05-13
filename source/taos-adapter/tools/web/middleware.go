package web

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

func SetTaosErrorCode(c *gin.Context, code int) {
	c.Set("taos_error_code", fmt.Sprintf("0x%04x", code))
}
