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
	"github.com/taosdata/taoskeeper/util"

	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
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
	for k, _ := range SysMonitor.outputs {
		SysMonitor.Deregister(k)
	}

	conn, err := db.NewConnectorWithDb(conf.TDengine.Username, conf.TDengine.Password, conf.TDengine.Host, conf.TDengine.Port, conf.Metrics.Database.Name, conf.TDengine.Usessl)
	assert.NoError(t, err)
	conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", conf.Metrics.Database.Name), util.GetQidOwn())

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
