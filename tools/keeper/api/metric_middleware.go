// Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

package api

import (
	"bytes"
	"io"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taoskeeper/infrastructure/log"
)

const maxRequestBodySize = 1 << 20 // 1MB - maximum request body size for metric endpoints

var middlewareLogger = log.GetLogger("METRIC_MIDDLEWARE")

// MetricCacheMiddleware AOP middleware (synchronous version)
func MetricCacheMiddleware(parser *MetricParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Fast path 1: non-POST request
		if c.Request.Method != "POST" {
			c.Next()
			return
		}

		// Fast path 2: path not matched
		path := c.Request.URL.Path
		if !shouldCachePath(path) {
			c.Next()
			return
		}

		// Limit request body size to prevent DoS attacks
		limitedReader := io.LimitReader(c.Request.Body, maxRequestBodySize)
		body, err := io.ReadAll(limitedReader)
		if err != nil {
			c.Next()
			return
		}

		// Check if body was truncated (exceeded max size)
		if len(body) == maxRequestBodySize {
			middlewareLogger.Warn("Request body exceeded 1MB limit, may have been truncated")
		}
		c.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		// Synchronous parsing (~5µs only, negligible latency)
		if err := parser.ParseAndStore(c, body); err != nil {
			middlewareLogger.Debugf("Failed to parse metrics: %v", err)
		}

		c.Next()
	}
}

// shouldCachePath matches paths
var cachePaths = []string{
	"/general-metric",
	"/taosd-cluster-basic",
	"/slow-sql-detail-batch",
	"/adapter_report",
}

func shouldCachePath(path string) bool {
	for _, prefix := range cachePaths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}
