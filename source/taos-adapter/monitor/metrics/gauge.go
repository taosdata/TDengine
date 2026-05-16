package metrics

import (
	"math"
	"sync/atomic"
)

type Gauge struct {
	valBits    uint64
	metricName string
}

func NewGauge(metricName string) *Gauge {
	return &Gauge{metricName: metricName}
}

func (g *Gauge) Set(val float64) {
	atomic.StoreUint64(&g.valBits, math.Float64bits(val))
}

func (g *Gauge) Inc() {
	g.Add(1)
}

func (g *Gauge) Dec() {
	g.Add(-1)
}

func (g *Gauge) Add(val float64) {
	for {
		oldBits := atomic.LoadUint64(&g.valBits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + val)
		if atomic.CompareAndSwapUint64(&g.valBits, oldBits, newBits) {
			return
		}
	}
}

func (g *Gauge) Sub(val float64) {
	g.Add(val * -1)
}

func (g *Gauge) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.valBits))
}

func (g *Gauge) MetricName() string {
	return g.metricName
}
