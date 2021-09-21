/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package utils

import (
	"encoding/json"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Port     uint16 `json:"port,omitempty" yaml:"port,omitempty"`
	Database string `json:"database,omitempty" yaml:"database,omitempty"`
	RuleFile string `json:"ruleFile,omitempty" yaml:"ruleFile,omitempty"`
	Log      struct {
		Level string `json:"level,omitempty" yaml:"level,omitempty"`
		Path  string `json:"path,omitempty" yaml:"path,omitempty"`
	} `json:"log" yaml:"log"`
	TDengine  string `json:"tdengine,omitempty" yaml:"tdengine,omitempty"`
	Receivers struct {
		AlertManager string `json:"alertManager,omitempty" yaml:"alertManager,omitempty"`
		Console      bool   `json:"console"`
	} `json:"receivers" yaml:"receivers"`
}

var Cfg Config

func LoadConfig(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()

	e = yaml.NewDecoder(f).Decode(&Cfg)
	if e != nil {
		f.Seek(0, 0)
		e = json.NewDecoder(f).Decode(&Cfg)
	}

	return e
}
