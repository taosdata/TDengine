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

package config_test

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/testutil"
	"github.com/taosdata/taoskeeper/version"
)

func TestConfig(t *testing.T) {
	data := fmt.Sprintf(`
# Start with debug middleware for gin
debug = true
# Listen port, default is 6043
port = 9000
# log level
loglevel = "error"
# go pool size
gopoolsize = 5000
# interval for TDengine metrics
RotationInterval = "10s"
[tdengine]
address = "http://localhost:6041"
authtype = "Basic"
username = "%s"
password = "%s"
`, testutil.TestUsername(), testutil.TestPassword())
	var c config.Config
	_, err := toml.Decode(data, &c)
	if err != nil {
		t.Error(err)
		return
	}
	assert.EqualValues(t, c, c)
	fmt.Print(c)
}

func TestBakConfig(t *testing.T) {
	isOk := copyConfigFile()
	if isOk {
		config.Name = "aaa"
		config.InitConfig()
		config.Name = "taoskeeper"
	}
}

func copyConfigFile() bool {
	var sourceFile string
	var destinationFile string
	switch runtime.GOOS {
	case "windows":
		sourceFile = fmt.Sprintf("C:\\%s\\cfg\\%s.toml", version.CUS_NAME, "taoskeeper")
		destinationFile = fmt.Sprintf("C:\\%s\\cfg\\%s.toml", version.CUS_NAME, "keeper")
	default:
		sourceFile = fmt.Sprintf("/etc/%s/%s.toml", version.CUS_PROMPT, "taoskeeper")
		destinationFile = fmt.Sprintf("/etc/%s/%s.toml", version.CUS_PROMPT, "keeper")
	}
	_, err := os.Stat(sourceFile)
	if os.IsNotExist(err) {
		return false
	}

	source, err := os.Open(sourceFile) // open the source file
	if err != nil {
		panic(err)
	}
	defer source.Close()

	destination, err := os.Create(destinationFile) // create the destination file
	if err != nil {
		panic(err)
	}
	defer destination.Close()
	_, err = io.Copy(destination, source) // copy the contents of source to destination file
	if err != nil {
		panic(err)
	}
	return true
}
