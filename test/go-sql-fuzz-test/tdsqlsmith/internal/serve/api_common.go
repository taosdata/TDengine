// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

package serve

// api_common.go holds shared API endpoints such as health checks and auth verification.
//
// api_common.go 持有共享的 API 端点，例如健康检查与鉴权校验。

import (
	"net/http"
	"time"
)

// handleHealth responds to GET requests with a simple status and current server time.
//
// handleHealth 对 GET 请求返回简单的状态与当前服务器时间。
func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now()})
}

// handleAuthVerify confirms that the request carries a valid API token, returning ok on success.
//
// handleAuthVerify 确认请求携带有效的 API token，成功时返回 ok。
func (s *server) handleAuthVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if !s.requireAuth(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
