package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestVgID(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_vgid")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	assert.NoError(t, testtools.EnsureDBCreated("test_vgid"))

	w = httptest.NewRecorder()
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	err := encoder.Encode([]string{"d0", "d1"})
	assert.NoError(t, err)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_vgid/vgid", buffer)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatal("get vgID fail ", w.Code, w.Body)
	}
	var result tableVgIDResp
	err = json.NewDecoder(w.Body).Decode(&result)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result.VgIDs))
	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_vgid")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}
