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

package monitor

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewNormalCollector(t *testing.T) {
	c, err := NewNormalCollector()
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, c.p)
}

func TestNormalCollectorCpuPercentRange(t *testing.T) {
	c, err := NewNormalCollector()
	assert.NoError(t, err)

	_, err = c.CpuPercent()
	assert.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	val, err := c.CpuPercent()
	assert.NoError(t, err)

	assert.False(t, math.IsNaN(val) || math.IsInf(val, 0))
	assert.GreaterOrEqual(t, val, 0.0)
	assert.LessOrEqual(t, val, 100.0)
}

func TestNormalCollectorMemPercentRange(t *testing.T) {
	c, err := NewNormalCollector()
	assert.NoError(t, err)

	val, err := c.MemPercent()
	assert.NoError(t, err)
	if math.IsNaN(val) || math.IsInf(val, 0) {
		t.Fatalf("MemPercent returned invalid value: %v", val)
	}
	if val < 0 || val > 100 {
		t.Fatalf("MemPercent out of expected range [0,100]: %v", val)
	}
}
