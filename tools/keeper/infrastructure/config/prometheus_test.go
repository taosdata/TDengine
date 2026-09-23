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

package config

import (
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// Verify that camelCase TOML keys under [prometheus] actually populate the
// struct fields through viper.Unmarshal (mapstructure), i.e. the toml tags
// are sufficient and explicit values are not silently dropped.
func TestPrometheusUnmarshalCase(t *testing.T) {
	f, err := os.CreateTemp("", "keeper-*.toml")
	assert.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = f.WriteString("[prometheus]\nincludeTables = [\"taos_slow_sql_detail\"]\ncacheTTL = 123\n")
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	v := viper.New()
	v.SetConfigFile(f.Name())
	v.SetDefault("prometheus.includeTables", []string{})
	v.SetDefault("prometheus.cacheTTL", 300)
	assert.NoError(t, v.ReadInConfig())

	var c Config
	assert.NoError(t, v.Unmarshal(&c))
	t.Logf("prometheus section decoded as: %+v", c.Prometheus)
	assert.Equal(t, []string{"taos_slow_sql_detail"}, c.Prometheus.IncludeTables)
	assert.Equal(t, 123, c.Prometheus.CacheTTL)
}
