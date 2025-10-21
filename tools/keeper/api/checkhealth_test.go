package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestCheckHealthInit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	h := NewCheckHealth("1.2.3")
	h.Init(r)

	req := httptest.NewRequest(http.MethodGet, "/check_health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want=200", w.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if body["version"] != "1.2.3" {
		t.Fatalf("version=%q, want=%q", body["version"], "1.2.3")
	}
}
