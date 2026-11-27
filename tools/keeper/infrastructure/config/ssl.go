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
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type SSL struct {
	Enable   bool   `toml:"enable"`
	CertFile string `toml:"certFile"`
	KeyFile  string `toml:"keyFile"`
}

func initSSL() {
	viper.SetDefault("ssl.enable", false)
	_ = viper.BindEnv("ssl.enable", "TAOS_KEEPER_SSL_ENABLE")
	pflag.Bool("ssl.enable", false, `enable ssl. Env "TAOS_KEEPER_SSL_ENABLE"`)

	viper.SetDefault("ssl.certFile", "")
	_ = viper.BindEnv("ssl.certFile", "TAOS_KEEPER_SSL_CERT_FILE")
	pflag.String("ssl.certFile", "", `ssl cert file path. Env "TAOS_KEEPER_SSL_CERT_FILE"`)

	viper.SetDefault("ssl.keyFile", "")
	_ = viper.BindEnv("ssl.keyFile", "TAOS_KEEPER_SSL_KEY_FILE")
	pflag.String("ssl.keyFile", "", `ssl key file path. Env "TAOS_KEEPER_SSL_KEY_FILE"`)
}
