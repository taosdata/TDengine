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

package config

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type TDengineRestful struct {
	Host       string `toml:"host"`
	Port       int    `toml:"port"`
	Username   string `toml:"username"`
	Password   string `toml:"password"`
	Usessl     bool   `toml:"usessl"`
	SkipVerify bool   `toml:"skipVerify"`
}

func (t TDengineRestful) String() string {
	type noStringer TDengineRestful
	safe := noStringer(t)
	safe.Password = "[REDACTED]"
	return fmt.Sprintf("%+v", safe)
}

func initTDengine() {
	viper.SetDefault("tdengine.host", "127.0.0.1")
	_ = viper.BindEnv("tdengine.host", "TAOS_KEEPER_TDENGINE_HOST")
	pflag.String("tdengine.host", "127.0.0.1", `TDengine server's ip. Env "TAOS_KEEPER_TDENGINE_HOST"`)

	viper.SetDefault("tdengine.port", 6041)
	_ = viper.BindEnv("tdengine.port", "TAOS_KEEPER_TDENGINE_PORT")
	pflag.Int("tdengine.port", 6041, `TDengine REST server(taosAdapter)'s port. Env "TAOS_KEEPER_TDENGINE_PORT"`)

	viper.SetDefault("tdengine.username", "root")
	_ = viper.BindEnv("tdengine.username", "TAOS_KEEPER_TDENGINE_USERNAME")
	pflag.String("tdengine.username", "root", `TDengine server's username. Env "TAOS_KEEPER_TDENGINE_USERNAME"`)

	viper.SetDefault("tdengine.password", "taosdata")
	_ = viper.BindEnv("tdengine.password", "TAOS_KEEPER_TDENGINE_PASSWORD")
	pflag.String("tdengine.password", "taosdata", `TDengine server's password. Env "TAOS_KEEPER_TDENGINE_PASSWORD"`)

	viper.SetDefault("tdengine.usessl", false)
	_ = viper.BindEnv("tdengine.usessl", "TAOS_KEEPER_TDENGINE_USESSL")
	pflag.Bool("tdengine.usessl", false, `TDengine server use ssl or not. Env "TAOS_KEEPER_TDENGINE_USESSL"`)

	viper.SetDefault("tdengine.skipVerify", true)
	_ = viper.BindEnv("tdengine.skipVerify", "TAOS_KEEPER_TDENGINE_SKIPVERIFY")
	pflag.Bool("tdengine.skipVerify", true, `Skip TDengine server TLS certificate verification, default true for compatibility. Set false to verify the server certificate. Env "TAOS_KEEPER_TDENGINE_SKIPVERIFY"`)
}
