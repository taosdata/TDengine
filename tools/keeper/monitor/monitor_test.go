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

package monitor

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/go-utils/web"
	"github.com/taosdata/taoskeeper/api"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util"
)

func TestStart(t *testing.T) {
	conf := config.InitConfig()
	if conf == nil {
		panic("config error")
	}
	conf.Env.InCGroup = true
	cpuCgroupDir := "/sys/fs/cgroup/cpu"
	if _, err := os.Stat(cpuCgroupDir); os.IsNotExist(err) {
		conf.Env.InCGroup = false
	}
	log.ConfigLog()
	router := web.CreateRouter(false, &conf.Cors, false)
	conf.Metrics.Database.Name = "monitor"
	reporter := api.NewReporter(conf)
	reporter.Init(router)
	conf.RotationInterval = "1s"
	StartMonitor("", conf, reporter)
	time.Sleep(2 * time.Second)
	for k := range SysMonitor.outputs {
		SysMonitor.Deregister(k)
	}

	conn, err := db.NewConnectorWithDb(conf.TDengine.Username, conf.TDengine.Password, conf.TDengine.Host, conf.TDengine.Port, conf.Metrics.Database.Name, conf.TDengine.Usessl)
	assert.NoError(t, err)
	conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", conf.Metrics.Database.Name), util.GetQidOwn(config.Conf.InstanceID))

}

func TestParseUint(t *testing.T) {
	num, err := util.ParseUint("-1", 10, 8)
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(0), num)
	num, err = util.ParseUint("0", 10, 8)
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(0), num)
	num, err = util.ParseUint("257", 10, 8)
	assert.Equal(t, "strconv.ParseUint: parsing \"257\": value out of range", err.Error())
	assert.Equal(t, uint64(0), num)
}
