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
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestInitTDengine_SkipVerifyDefaultTrue(t *testing.T) {
	os.Unsetenv("TAOS_KEEPER_TDENGINE_SKIPVERIFY")

	viper.Reset()
	pflag.CommandLine = pflag.NewFlagSet("test", pflag.ExitOnError)

	initTDengine()

	assert.Equal(t, true, viper.GetBool("tdengine.skipVerify"))

	var conf Config
	err := viper.Unmarshal(&conf)
	assert.NoError(t, err)
	assert.Equal(t, true, conf.TDengine.SkipVerify)
}

func TestInitTDengine_SkipVerifyFromConfigFile(t *testing.T) {
	os.Unsetenv("TAOS_KEEPER_TDENGINE_SKIPVERIFY")

	viper.Reset()
	pflag.CommandLine = pflag.NewFlagSet("test", pflag.ExitOnError)

	initTDengine()

	configContent := `
[tdengine]
host = "127.0.0.1"
usessl = true
skipVerify = false
`
	viper.SetConfigType("toml")
	err := viper.ReadConfig(strings.NewReader(configContent))
	assert.NoError(t, err)

	var conf Config
	err = viper.Unmarshal(&conf)
	assert.NoError(t, err)

	assert.Equal(t, true, conf.TDengine.Usessl)
	assert.Equal(t, false, conf.TDengine.SkipVerify)
}

func TestInitTDengine_SkipVerifyFromEnv(t *testing.T) {
	os.Setenv("TAOS_KEEPER_TDENGINE_SKIPVERIFY", "false")
	defer os.Unsetenv("TAOS_KEEPER_TDENGINE_SKIPVERIFY")

	viper.Reset()
	pflag.CommandLine = pflag.NewFlagSet("test", pflag.ExitOnError)

	initTDengine()

	assert.Equal(t, false, viper.GetBool("tdengine.skipVerify"))
}
