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

package dataimport

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/pelletier/go-toml"
)

var (
	cfg  Config
	once sync.Once
)

// Config include all scene import config
type Config struct {
	UserCases map[string]CaseConfig
}

// CaseConfig include the sample data config and tdengine config
type CaseConfig struct {
	Format              string
	FilePath            string
	Separator           string
	StName              string
	SubTableName        string
	Timestamp           string
	TimestampType       string
	TimestampTypeFormat string
	Tags                []FieldInfo
	Fields              []FieldInfo
}

// FieldInfo is field or tag info
type FieldInfo struct {
	Name string
	Type string
}

// LoadConfig will load the specified file config
func LoadConfig(filePath string) Config {
	once.Do(func() {
		filePath, err := filepath.Abs(filePath)
		if err != nil {
			panic(err)
		}
		fmt.Printf("parse toml file once. filePath: %s\n", filePath)
		tree, err := toml.LoadFile(filePath)
		if err != nil {
			panic(err)
		}

		bytes, err := json.Marshal(tree.ToMap())
		if err != nil {
			panic(err)
		}

		err = json.Unmarshal(bytes, &cfg.UserCases)
		if err != nil {
			panic(err)
		}
	})
	return cfg
}
