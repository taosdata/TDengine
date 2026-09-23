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

package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func NewCheckHealth(version string) *CheckHealth {
	return &CheckHealth{version: version}
}

type CheckHealth struct {
	version string
}

func (h *CheckHealth) Init(c gin.IRouter) {
	c.GET("check_health", func(context *gin.Context) {
		context.JSON(http.StatusOK, map[string]string{"version": h.version})
	})
}
