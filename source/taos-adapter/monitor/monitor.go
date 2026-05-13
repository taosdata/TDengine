package monitor

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/monitor"
)

var logger = log.GetLogger("MON")

const (
	NormalStatus = uint32(0)
	PauseStatus  = uint32(1)
)

var (
	pauseStatus = NormalStatus
	queryStatus = NormalStatus
)
var (
	cpuPercent = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "taosadapter",
			Subsystem: "system",
			Name:      "cpu_percent",
			Help:      "Percentage of all cpu used by the program",
		},
	)
	memPercent = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "taosadapter",
			Subsystem: "system",
			Name:      "mem_percent",
			Help:      "Percentage of all memory used by the program",
		},
	)
)

var identity string

func StartMonitor() {
	if len(config.Conf.Monitor.Identity) != 0 {
		identity = config.Conf.Monitor.Identity
	} else {
		hostname, err := os.Hostname()
		if err != nil {
			logger.WithError(err).Panic("can not get hostname")
		}
		if len(hostname) > 39 {
			hostname = hostname[:39]
		}
		identity = fmt.Sprintf("%s:%d", hostname, config.Conf.Port)
	}
	systemStatus := make(chan monitor.SysStatus)
	go func() {
		for status := range systemStatus {
			if status.CpuError == nil {
				cpuPercent.Set(status.CpuPercent)
			}
			if status.MemError == nil {
				memPercent.Set(status.MemPercent)
				if status.MemPercent >= config.Conf.Monitor.PauseAllMemoryThreshold {
					if atomic.CompareAndSwapUint32(&pauseStatus, NormalStatus, PauseStatus) {
						logger.Warn("pause all")
					}
				} else {
					if atomic.CompareAndSwapUint32(&pauseStatus, PauseStatus, NormalStatus) {
						logger.Warn("resume all")
					}
				}
				if status.MemPercent >= config.Conf.Monitor.PauseQueryMemoryThreshold {
					if atomic.CompareAndSwapUint32(&queryStatus, NormalStatus, PauseStatus) {
						logger.Warn("pause query")
					}
				} else {
					if atomic.CompareAndSwapUint32(&queryStatus, PauseStatus, NormalStatus) {
						logger.Warn("resume query")
					}
				}
			}
		}
	}()
	monitor.SysMonitor.Register(systemStatus)
	monitor.Start(config.Conf.Monitor.CollectDuration, config.Conf.Monitor.InCGroup)
	InitKeeper()
	StartUpload()
}

func QueryPaused() bool {
	return atomic.LoadUint32(&queryStatus) == PauseStatus || atomic.LoadUint32(&pauseStatus) == PauseStatus
}

func AllPaused() bool {
	return atomic.LoadUint32(&pauseStatus) == PauseStatus
}
