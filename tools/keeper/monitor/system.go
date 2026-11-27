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
	defer s.Unlock()

	for output := range s.outputs {
		select {
		case output <- *s.status:
		default:
		}
	}
}

func (s *sysMonitor) Register(c chan<- SysStatus) {
	s.Lock()
	defer s.Unlock()
	if s.outputs == nil {
		s.outputs = map[chan<- SysStatus]struct{}{}
	}
	s.outputs[c] = struct{}{}
}

func (s *sysMonitor) Deregister(c chan<- SysStatus) {
	s.Lock()
	defer s.Unlock()
	if s.outputs != nil {
		delete(s.outputs, c)
	}
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
