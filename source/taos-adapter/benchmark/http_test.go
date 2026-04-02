package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func BenchmarkRoute(b *testing.B) {
	router := setupRouter()
	w := httptest.NewRecorder()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("POST", "/rest/sql", nil)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		router.ServeHTTP(w, req)
		assert.Equal(b, 200, w.Code)
	}
}

// @author: xftan
// @date: 2021/12/14 14:59
// @description: test gin.Router
func Test_setupRouter(t *testing.T) {
	router := setupRouter()
	w := httptest.NewRecorder()
	tests := []struct {
		name string
	}{
		{
			name: "common",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("POST", "/rest/sql", nil)
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)
		})
	}
}
