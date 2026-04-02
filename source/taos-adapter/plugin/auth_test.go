package plugin

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

// @author: xftan
// @date: 2021/12/14 15:09
// @description: test auth middleware
func TestAuth(t *testing.T) {
	router := gin.Default()
	router.GET("/", Auth(func(c *gin.Context, code int, err error) {
		c.AbortWithStatusJSON(code, err)
	}), func(c *gin.Context) {
		u, p, token, err := GetAuth(c)
		if assert.NoError(t, err) {
			assert.Equal(t, u, "root")
			assert.Equal(t, p, "taosdata")
			assert.Equal(t, token, "")
		}
		c.Status(200)
	})
	router.GET("/token_test", Auth(func(c *gin.Context, code int, err error) {
		c.AbortWithStatusJSON(code, err)
	}), func(c *gin.Context) {
		u, p, token, err := GetAuth(c)
		if assert.NoError(t, err) {
			assert.Equal(t, u, "")
			assert.Equal(t, p, "")
			assert.Equal(t, token, "test_token")
		}
		c.Status(200)
	})
	router.GET("/unknown", Auth(func(c *gin.Context, code int, err error) {
		c.AbortWithStatusJSON(code, err)
	}), func(c *gin.Context) {
		u, p, token, err := GetAuth(c)
		assert.Error(t, err)
		assert.Equal(t, "", u)
		assert.Equal(t, "", p)
		assert.Equal(t, "", token)
		c.Status(401)
	})
	RegisterGenerateAuth(router)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic wrong_basic")
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/genauth/root/taosdata/aaa", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	token := w.Body.Bytes()
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	tokenStr := base64.StdEncoding.EncodeToString(token)
	req.Header.Set("Authorization", "Basic "+tokenStr)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/genauth//taosdata/aaa", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/token_test", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Bearer test_token")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/unknown", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Bearerx test_token")
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/unknown", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("", "")
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)
}
