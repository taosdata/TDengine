package monitor

import (
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/taosdata/taoskeeper/util/pool"
)

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
	// skip when inf or nan
	if math.IsInf(s.status.CpuPercent, 0) || math.IsNaN(s.status.CpuPercent) ||
		math.IsInf(s.status.MemPercent, 0) || math.IsNaN(s.status.MemPercent) {
		return
	}

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
		collector, err := NewCGroupCollector()
		if err != nil {
			logger.Errorf("new normal group controller error, msg:%s", err)
		}
		SysMonitor.collector = collector
	} else {
		collector, err := NewNormalCollector()
		if err != nil {
			logger.Errorf("new normal controller error, msg:%s", err)
		}
		SysMonitor.collector = collector
	}
	SysMonitor.collect()
	SysMonitor.ticker = time.NewTicker(SysMonitor.collectDuration)
	pool.GoroutinePool.Submit(func() {
		for range SysMonitor.ticker.C {
			SysMonitor.collect()
		}
	})
}
