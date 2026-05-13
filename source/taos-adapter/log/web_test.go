package log_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

// @author: xftan
// @date: 2021/12/14 15:07
// @description: test gin log middleware
func TestGinLog(t *testing.T) {
	log.ConfigLog()
	router := gin.New()
	router.Use(log.GinLog())
	router.Use(log.GinRecoverLog())
	router.POST("/rest/sql", func(c *gin.Context) {
		c.Status(200)
	})
	router.POST("/panic", func(_ *gin.Context) {
		panic("test")
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/rest/sql", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/panic", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 500, w.Code)
}
