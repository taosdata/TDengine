// Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("WEB")

	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))

		logger := logger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)
		statusCode := c.Writer.Status()

		startTime := time.Now()
		c.Next()
		endTime := time.Now()
		latencyTime := endTime.Sub(startTime)
		reqMethod := c.Request.Method
		reqUri := c.Request.RequestURI

		clientIP := c.ClientIP()

		if statusCode != 200 {
			logger.Errorf("finish request, status_code:%3d, latency:%v, client_ip:%s, method:%s, uri:%s", statusCode, latencyTime, clientIP, reqMethod, reqUri)
			return
		}
		logger.Debugf("finish request, status_code:%3d, latency:%v, client_ip:%s, method:%s, uri:%s", statusCode, latencyTime, clientIP, reqMethod, reqUri)
	}
}

type recoverLog struct {
	logger logrus.FieldLogger
}

func (r *recoverLog) Write(p []byte) (n int, err error) {
	r.logger.Errorln(string(p))
	return len(p), nil
}

func GinRecoverLog() gin.HandlerFunc {
	logger := GetLogger("WEB")
	return func(c *gin.Context) {
		writer := &recoverLog{logger: logger}
		gin.RecoveryWithWriter(writer)(c)
	}
}
