package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestCheckHealthInit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	h := NewCheckHealth("1.2.3")
	h.Init(r)

	req := httptest.NewRequest(http.MethodGet, "/check_health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body map[string]string
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "1.2.3", body["version"])
}
