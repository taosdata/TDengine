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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestPerformanceCollector_Describe(t *testing.T) {
	// Create empty Reporter directly, avoiding InitConfig()
	reporter := &Reporter{}
	collector := NewPerformanceCollector(reporter)

	ch := make(chan *prometheus.Desc, 10)
	go func() {
		collector.Describe(ch)
		close(ch)
	}()

	descriptions := 0
	for range ch {
		descriptions++
	}

	// Should have 2 metric descriptions
	assert.Equal(t, 2, descriptions)
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{"int", 123, int64(123)},
		{"int64", int64(456), int64(456)},
		{"float64", 789.0, int64(789)},
		{"string", "100", int64(100)},
		{"nil", nil, int64(0)},
		{"invalid string", "abc", int64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toInt64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
