package metrics

import (
	"testing"
)

func TestGaugeSetAndGet(t *testing.T) {
	g := &Gauge{}
	val := 42.0
	g.Set(val)
	result := g.Value()
	if result != val {
		t.Errorf("Expected %f, got %f", val, result)
	}
}

func TestGaugeIncAndDec(t *testing.T) {
	g := &Gauge{}
	g.Inc()
	result := g.Value()
	if result != 1.0 {
		t.Errorf("Expected 1.0, got %f", result)
	}

	g.Dec()
	result = g.Value()
	if result != 0.0 {
		t.Errorf("Expected 0.0, got %f", result)
	}
}

func TestGaugeAddAndSub(t *testing.T) {
	g := &Gauge{}
	g.Add(10.0)
	result := g.Value()
	if result != 10.0 {
		t.Errorf("Expected 10.0, got %f", result)
	}

	g.Sub(5.0)
	result = g.Value()
	if result != 5.0 {
		t.Errorf("Expected 5.0, got %f", result)
	}
}

func TestGaugeMetricName(t *testing.T) {
	g := NewGauge("test")
	result := g.MetricName()
	if result != "test" {
		t.Errorf("Expected test, got %s", result)
	}
}

func BenchmarkGauge_Inc(b *testing.B) {
	g := NewGauge("test")
	for i := 0; i < b.N; i++ {
		if g != nil {
			g.Inc()
		}
	}
}

func BenchmarkGauge_Dec(b *testing.B) {
	g := NewGauge("test")
	for i := 0; i < b.N; i++ {
		if g != nil {
			g.Dec()
		}
	}
}
