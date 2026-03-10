package process

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func Test_i2string(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected string
	}{
		{"abc", "abc"},
		{"abcdef", "abcdef"},
		{[]byte{97, 98, 99, 100, 101, 102}, "abcdef"},
	}

	for _, tt := range tests {
		res := i2string(tt.value)
		assert.Equal(t, tt.expected, res)
	}
}

func Test_i2string_panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for unexpected type, but did not panic")
		}
	}()

	i2string(12345)
}

func Test_i2float(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected float64
	}{
		{int8(1), 1.0},
		{int16(1), 1.0},
		{int32(1), 1.0},
		{int64(1), 1.0},
		{uint8(1), 1.0},
		{uint16(1), 1.0},
		{uint32(1), 1.0},
		{uint64(1), 1.0},
		{float32(1.5), 1.5},
		{float64(1.5), 1.5},
		{true, 1.0},
		{false, 0.0},
	}

	for _, tt := range tests {
		res := i2float(tt.value)
		assert.Equal(t, tt.expected, res)
	}
}

func Test_i2float_panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for unexpected type, but did not panic")
		}
	}()

	i2float("unexpected type")
}

func Test_getRoleStr(t *testing.T) {
	tests := []struct {
		value    float64
		expected string
	}{
		{0, "offline"},
		{99.5, "follower"},
		{100, "follower"},
		{100.4, "follower"},
		{100.5, "candidate"},
		{101, "candidate"},
		{101.4, "candidate"},
		{101.5, "leader"},
		{102, "leader"},
		{102.4, "leader"},
		{102.5, "error"},
		{103, "error"},
		{104, "learner"},
		{99.4, "unknown"},
		{105, "unknown"},
		{-1, "unknown"},
		{150, "unknown"},
	}

	for _, tt := range tests {
		res := getRoleStr(tt.value)
		assert.Equal(t, tt.expected, res)
	}
}

func Test_getStatusStr(t *testing.T) {
	tests := []struct {
		value    float64
		expected string
	}{
		{-0.4, "offline"},
		{0, "offline"},
		{0.4, "offline"},
		{0.5, "ready"},
		{1, "ready"},
		{1.4, "ready"},
		{1.5, "unknown"},
		{2, "unknown"},
		{-0.5, "unknown"},
		{-1, "unknown"},
	}

	for _, tt := range tests {
		res := getStatusStr(tt.value)
		assert.Equal(t, tt.expected, res)
	}
}

func Test_withDBName(t *testing.T) {
	processor := &Processor{db: "db"}
	res := processor.withDBName("test")
	assert.Equal(t, res, "`db`.`test`")
}

func Test_exchangeDBType_UnsupportedType_Panics(t *testing.T) {
	assert.PanicsWithValue(t, "unsupported type", func() { _ = exchangeDBType("DECIMAL_UNSUPPORTED") })
}

func TestProcessorGetMetric(t *testing.T) {
	processor := &Processor{}
	metric := processor.GetMetric()
	assert.Nil(t, metric)
}

func TestProcessor_buildFQName_UsesMappedName(t *testing.T) {
	p := &Processor{prefix: "pref"}

	got := p.buildFQName("taosd_cluster_basic", "first_ep")
	want := "pref_cluster_info_first_ep"

	if got != want {
		t.Fatalf("buildFQName mapped name mismatch: got %q, want %q", got, want)
	}
}

func TestProcessorCollect_Gauge_SkipsNilAndEmitsMetric(t *testing.T) {
	p := &Processor{
		metricMap: map[string]*Metric{},
	}
	m := &Metric{
		FQName:    "test_gauge",
		Type:      Gauge,
		Variables: []string{"a"},
	}
	m.SetValue([]*Value{
		{Label: map[string]string{"a": "1"}, Value: nil},
		{Label: map[string]string{"a": "2"}, Value: float64(3.14)},
	})
	p.metricMap["test_gauge"] = m

	ch := make(chan prometheus.Metric, 4)
	p.Collect(ch)

	if len(ch) != 1 {
		t.Fatalf("expected 1 metric emitted, got %d", len(ch))
	}
}

func TestProcessorCollect_Counter_SkipNilAndNegative_EmitPositive(t *testing.T) {
	p := &Processor{
		metricMap: map[string]*Metric{},
	}
	m := &Metric{
		FQName:    "test_counter",
		Type:      Counter,
		Variables: []string{"a"},
	}
	m.SetValue([]*Value{
		{Label: map[string]string{"a": "x"}, Value: nil},
		{Label: map[string]string{"a": "y"}, Value: float64(-2)},
		{Label: map[string]string{"a": "z"}, Value: int64(3)},
		{Label: map[string]string{"a": "b"}, Value: true},
	})
	p.metricMap["test_counter"] = m

	ch := make(chan prometheus.Metric, 4)
	p.Collect(ch)

	if len(ch) != 2 {
		t.Fatalf("expected 2 metrics emitted, got %d", len(ch))
	}
}

func TestProcessorCollect_Info_SkipNilAndEmitWithValueLabel(t *testing.T) {
	p := &Processor{
		metricMap: map[string]*Metric{},
	}

	m := &Metric{
		FQName:    "test_info_metric",
		Type:      Info,
		Variables: []string{"tag"},
	}
	m.SetValue([]*Value{
		nil,
		{Label: map[string]string{"tag": "x"}, Value: "ready"},
	})
	p.metricMap["test_info_metric"] = m

	ch := make(chan prometheus.Metric, 2)
	p.Collect(ch)

	if len(ch) != 1 {
		t.Fatalf("expected 1 metric emitted, got %d", len(ch))
	}
}
