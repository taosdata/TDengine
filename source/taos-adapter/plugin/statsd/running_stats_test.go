package statsd

import (
	"math"
	"testing"
)

// Test that a single metric is handled correctly
func TestRunningStats_Single(t *testing.T) {
	rs := RunningStats{}
	values := []float64{10.1}

	for _, v := range values {
		rs.AddValue(v)
	}

	if rs.Mean() != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Mean())
	}
	if rs.Upper() != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Upper())
	}
	if rs.Lower() != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Lower())
	}
	if rs.Percentile(100) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(100))
	}
	if rs.Percentile(99.95) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(99.95))
	}
	if rs.Percentile(90) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(90))
	}
	if rs.Percentile(50) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(50))
	}
	if rs.Percentile(0) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(0))
	}
	if rs.Count() != 1 {
		t.Errorf("Expected %v, got %v", 1, rs.Count())
	}
	if rs.Variance() != 0 {
		t.Errorf("Expected %v, got %v", 0, rs.Variance())
	}
	if rs.Stddev() != 0 {
		t.Errorf("Expected %v, got %v", 0, rs.Stddev())
	}
}

// Test that duplicate values are handled correctly
func TestRunningStats_Duplicate(t *testing.T) {
	rs := RunningStats{}
	values := []float64{10.1, 10.1, 10.1, 10.1}

	for _, v := range values {
		rs.AddValue(v)
	}

	if rs.Mean() != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Mean())
	}
	if rs.Upper() != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Upper())
	}
	if rs.Lower() != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Lower())
	}
	if rs.Percentile(100) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(100))
	}
	if rs.Percentile(99.95) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(99.95))
	}
	if rs.Percentile(90) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(90))
	}
	if rs.Percentile(50) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(50))
	}
	if rs.Percentile(0) != 10.1 {
		t.Errorf("Expected %v, got %v", 10.1, rs.Percentile(0))
	}
	if rs.Count() != 4 {
		t.Errorf("Expected %v, got %v", 4, rs.Count())
	}
	if rs.Variance() != 0 {
		t.Errorf("Expected %v, got %v", 0, rs.Variance())
	}
	if rs.Stddev() != 0 {
		t.Errorf("Expected %v, got %v", 0, rs.Stddev())
	}
}

// Test a list of sample values, returns all correct values
func TestRunningStats(t *testing.T) {
	rs := RunningStats{}
	values := []float64{10, 20, 10, 30, 20, 11, 12, 32, 45, 9, 5, 5, 5, 10, 23, 8}

	for _, v := range values {
		rs.AddValue(v)
	}

	if rs.Mean() != 15.9375 {
		t.Errorf("Expected %v, got %v", 15.9375, rs.Mean())
	}
	if rs.Upper() != 45 {
		t.Errorf("Expected %v, got %v", 45, rs.Upper())
	}
	if rs.Lower() != 5 {
		t.Errorf("Expected %v, got %v", 5, rs.Lower())
	}
	if rs.Percentile(100) != 45 {
		t.Errorf("Expected %v, got %v", 45, rs.Percentile(100))
	}
	if rs.Percentile(99.98) != 45 {
		t.Errorf("Expected %v, got %v", 45, rs.Percentile(99.98))
	}
	if rs.Percentile(90) != 32 {
		t.Errorf("Expected %v, got %v", 32, rs.Percentile(90))
	}
	if rs.Percentile(50.1) != 11 {
		t.Errorf("Expected %v, got %v", 11, rs.Percentile(50.1))
	}
	if rs.Percentile(50) != 11 {
		t.Errorf("Expected %v, got %v", 11, rs.Percentile(50))
	}
	if rs.Percentile(49.9) != 10 {
		t.Errorf("Expected %v, got %v", 10, rs.Percentile(49.9))
	}
	if rs.Percentile(0) != 5 {
		t.Errorf("Expected %v, got %v", 5, rs.Percentile(0))
	}
	if rs.Count() != 16 {
		t.Errorf("Expected %v, got %v", 4, rs.Count())
	}
	if !fuzzyEqual(rs.Variance(), 124.93359, .00001) {
		t.Errorf("Expected %v, got %v", 124.93359, rs.Variance())
	}
	if !fuzzyEqual(rs.Stddev(), 11.17736, .00001) {
		t.Errorf("Expected %v, got %v", 11.17736, rs.Stddev())
	}
}

// Test that the percentile limit is respected.
func TestRunningStats_PercentileLimit(t *testing.T) {
	rs := RunningStats{}
	rs.PercLimit = 10
	values := []float64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	for _, v := range values {
		rs.AddValue(v)
	}

	if rs.Count() != 11 {
		t.Errorf("Expected %v, got %v", 11, rs.Count())
	}
	if len(rs.perc) != 10 {
		t.Errorf("Expected %v, got %v", 10, len(rs.perc))
	}
}

func fuzzyEqual(a, b, epsilon float64) bool {
	return math.Abs(a-b) <= epsilon
}
