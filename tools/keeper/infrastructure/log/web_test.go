// Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package log

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestGinLog_StatusNot200_TriggersErrorPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	r.Use(func(c *gin.Context) {
		c.Status(http.StatusInternalServerError)
		c.Next()
	})
	r.Use(GinLog())

	r.GET("/err", func(c *gin.Context) {})

	req := httptest.NewRequest(http.MethodGet, "/err", nil)
	req.Header.Set("X-QID", "123")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusInternalServerError)
	}
}

func TestGinLog_Status200_TriggersDebugPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	r.Use(GinLog())
	r.GET("/ok", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set("X-QID", "456")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusOK)
	}
}

func Test_recoverLogWrite_LogsErrorAndReturnsLen(t *testing.T) {
	var buf bytes.Buffer
	lg := logrus.New()
	lg.SetOutput(&buf)
	lg.SetLevel(logrus.ErrorLevel)

	r := &recoverLog{logger: lg}
	msg := "panic: something went wrong"
	n, err := r.Write([]byte(msg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(msg) {
		t.Fatalf("n=%d, want=%d", n, len(msg))
	}

	out := buf.String()
	if !strings.Contains(out, msg) {
		t.Fatalf("logged output missing message; got: %q", out)
	}
}

func TestGinRecoverLog_HandlesPanicWithWriter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(GinRecoverLog())

	r.GET("/panic", func(c *gin.Context) { panic("boom") })

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
