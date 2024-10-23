package monitor

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/taosdata/taoskeeper/api"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util/pool"
)

var logger = log.GetLogger("monitor")

func StartMonitor(identity string, conf *config.Config, reporter *api.Reporter) {
	if len(identity) == 0 {
		hostname, err := os.Hostname()
		if err != nil {
			logger.WithError(err).Panic("can not get hostname")
		}
		if len(hostname) > 40 {
			hostname = hostname[:40]
		}
		identity = fmt.Sprintf("%s:%d", hostname, conf.Port)
	}

	systemStatus := make(chan SysStatus)
	_ = pool.GoroutinePool.Submit(func() {
		var (
			cpuPercent  float64
			memPercent  float64
			totalReport int
		)

		for status := range systemStatus {
			if status.CpuError == nil {
				cpuPercent = status.CpuPercent
			}
			if status.MemError == nil {
				memPercent = status.MemPercent
			}

			totalResp := reporter.GetTotalRep()
			for i := 0; i < 3; i++ {
				totalReport = totalResp.Load().(int)
				if totalResp.CompareAndSwap(totalReport, 0) {
					break
				}
				logger.Warn("Reset keeper_monitor total resp via cas fail! Maybe to many concurrent ")
				reporter.GetTotalRep().Store(0)
			}

			kn := md5.Sum([]byte(identity))
			sql := fmt.Sprintf("insert into `keeper_monitor_%s` using keeper_monitor tags ('%s') values ( now, "+
				" %f, %f, %d)", hex.EncodeToString(kn[:]), identity, cpuPercent, memPercent, totalReport)
			conn, err := db.NewConnectorWithDb(conf.TDengine.Username, conf.TDengine.Password, conf.TDengine.Host,
				conf.TDengine.Port, conf.Metrics.Database.Name, conf.TDengine.Usessl)
			if err != nil {
				logger.WithError(err).Errorf("connect to database error")
				return
			}

			ctx := context.Background()
			if _, err = conn.Exec(ctx, sql); err != nil {
				logger.Errorf("execute sql: %s, error: %s", sql, err)
			}

			if err := conn.Close(); err != nil {
				logger.WithError(err).Errorf("close connection error")
			}
		}
	})
	SysMonitor.Register(systemStatus)
	interval, err := time.ParseDuration(conf.RotationInterval)
	if err != nil {
		panic(err)
	}
	Start(interval, conf.Env.InCGroup)
}
