package monitor

import (
	"runtime"
	"sync"
	"time"

	"github.com/taosdata/taosadapter/v3/log"
)

var logger = log.GetLogger("MON")

type SysStatus struct {
	CollectTime     time.Time
	CpuPercent      float64
	CpuError        error
	MemPercent      float64
	MemError        error
	GoroutineCounts int
	ThreadCounts    int
}

type sysMonitor struct {
	sync.Mutex
	collectDuration time.Duration
	collector       SysCollector
	status          *SysStatus
	outputs         map[chan<- SysStatus]struct{}
	ticker          *time.Ticker
}

func (s *sysMonitor) collect() {
	s.status.CollectTime = time.Now()
	s.status.CpuPercent, s.status.CpuError = s.collector.CpuPercent()
	s.status.MemPercent, s.status.MemError = s.collector.MemPercent()
	s.status.GoroutineCounts = runtime.NumGoroutine()
	s.status.ThreadCounts, _ = runtime.ThreadCreateProfile(nil)
	s.Lock()
	for output := range s.outputs {
		select {
		case output <- *s.status:
		default:
		}
	}
	s.Unlock()
}

func (s *sysMonitor) Register(c chan<- SysStatus) {
	s.Lock()
	if s.outputs == nil {
		s.outputs = map[chan<- SysStatus]struct{}{
			c: {},
		}
	} else {
		s.outputs[c] = struct{}{}
	}
	s.Unlock()
}

func (s *sysMonitor) Deregister(c chan<- SysStatus) {
	s.Lock()
	if s.outputs != nil {
		delete(s.outputs, c)
	}
	s.Unlock()
}

var SysMonitor = &sysMonitor{status: &SysStatus{}}

func Start(collectDuration time.Duration, inCGroup bool) {
	SysMonitor.collectDuration = collectDuration
	if inCGroup {
		collector, err := NewCGroupCollector(readUint)
		if err != nil {
			logger.WithError(err).Fatal("new normal controller")
		}
		SysMonitor.collector = collector
	} else {
		collector, err := NewNormalCollector()
		if err != nil {
			logger.WithError(err).Fatal("new normal controller")
		}
		SysMonitor.collector = collector
	}
	SysMonitor.collect()
	SysMonitor.ticker = time.NewTicker(SysMonitor.collectDuration)
	go func() {
		for range SysMonitor.ticker.C {
			SysMonitor.collect()
		}
	}()
}
