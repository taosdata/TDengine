package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestZabbixInit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	z := &Zabbix{}
	r := gin.New()
	z.Init(r)

	check := func(path string) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path, w.Code)
		}

		var resp struct {
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%s unmarshal response error: %v", path, err)
		}

		// Accept both null or empty array
		if string(resp.Data) != "null" {
			var arr []any
			if err := json.Unmarshal(resp.Data, &arr); err != nil {
				t.Fatalf("%s data is not array/null: %s", path, string(resp.Data))
			}
			if len(arr) != 0 {
				t.Fatalf("%s data length = %d, want 0", path, len(arr))
			}
		}
	}

	check("/zabbix/float")
	check("/zabbix/string")
}

func TestZabbix_sortLabel(t *testing.T) {
	z := &Zabbix{}

	if out := z.sortLabel(nil); out != "" {
		t.Fatalf("expected empty string, got %q", out)
	}
	if out := z.sortLabel(map[string]string{}); out != "" {
		t.Fatalf("expected empty string, got %q", out)
	}

	in := map[string]string{
		"c": "3",
		"a": "1",
		"b": "2",
	}
	out := z.sortLabel(in)
	want := "a=1_b=2_c=3"
	if out != want {
		t.Fatalf("sortLabel mismatch, want %q, got %q", want, out)
	}
}
