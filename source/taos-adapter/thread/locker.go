package thread

import "github.com/taosdata/taosadapter/v3/monitor/metrics"

type Semaphore struct {
	c     chan struct{}
	gauge *metrics.Gauge
}

var SyncSemaphore *Semaphore
var AsyncSemaphore *Semaphore

func NewSemaphore(count int) *Semaphore {
	return &Semaphore{c: make(chan struct{}, count)}
}
func (l *Semaphore) SetGauge(gauge *metrics.Gauge) {
	l.gauge = gauge
}

func (l *Semaphore) Acquire() {
	l.c <- struct{}{}
	if l.gauge != nil {
		l.gauge.Inc()
	}
}

func (l *Semaphore) Release() {
	<-l.c
	if l.gauge != nil {
		l.gauge.Dec()
	}
}
