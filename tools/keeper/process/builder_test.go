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

package process

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/infrastructure/config"
)

func TestExpandMetricsFromConfig(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Metrics{
		Tables: []string{"t1", "t1", "t2", "t2"},
		Database: config.Database{
			Name: "db",
		},
	}

	assert.Panics(t, func() {
		_, _ = ExpandMetricsFromConfig(ctx, nil, cfg)
	})
}
