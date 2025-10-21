package process

import (
	"fmt"
	"strings"
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
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic, got none")
		} else if s, ok := r.(string); !ok || s != "unsupported type" {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	_ = exchangeDBType("DECIMAL_UNSUPPORTED")
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

func TestProcessorProcess_SelectedBlock_ValueTransformations(t *testing.T) {
	p := &Processor{
		prefix:    "pref",
		metricMap: map[string]*Metric{},
	}

	tableName := "t"
	table := &Table{
		Variables:  []string{"zone"},
		ColumnList: []string{"mrole", "mstatus", "cluster_uptime", "name"},
	}
	columns := table.ColumnList

	p.metricMap[p.buildFQName(tableName, "mrole")] = &Metric{Type: Info}
	p.metricMap[p.buildFQName(tableName, "mstatus")] = &Metric{Type: Info}
	p.metricMap[p.buildFQName(tableName, "cluster_uptime")] = &Metric{Type: Gauge}
	p.metricMap[p.buildFQName(tableName, "name")] = &Metric{Type: Info}

	head := append(append([]string{}, columns...), "zone")
	row := []interface{}{float64(102), float64(1), float64(172800), "node-1", "z1"}

	hasTag := len(table.Variables) > 0
	tagIndex := 0
	if hasTag {
		tagIndex = len(columns)
	}

	values := make([][]*Value, len(table.ColumnList))
	label := map[string]string{}
	valuesMap := make(map[string]interface{})
	colEndIndex := len(columns)

	if hasTag {
		for i := tagIndex; i < len(head); i++ {
			if row[i] != nil {
				label[head[i]] = fmt.Sprintf("%v", row[i])
			}
		}
	}

	for i := 0; i < colEndIndex; i++ {
		valuesMap[columns[i]] = row[i]
	}

	for i, column := range table.ColumnList {
		var v interface{}
		metric := p.metricMap[p.buildFQName(tableName, column)]
		switch metric.Type {
		case Info:
			_, isFloat := valuesMap[column].(float64)
			if strings.HasSuffix(column, "role") && valuesMap[column] != nil && isFloat {
				v = getRoleStr(valuesMap[column].(float64))
				break
			}
			if strings.HasSuffix(column, "status") && valuesMap[column] != nil && isFloat {
				v = getStatusStr(valuesMap[column].(float64))
				break
			}
			if valuesMap[column] != nil {
				v = i2string(valuesMap[column])
			} else {
				v = nil
			}
		case Counter, Gauge, Summary:
			if valuesMap[column] != nil {
				v = i2float(valuesMap[column])
				if column == "cluster_uptime" {
					v = i2float(valuesMap[column]) / 86400
				}
			} else {
				v = nil
			}
		}
		values[i] = append(values[i], &Value{
			Label: label,
			Value: v,
		})
	}

	if got := values[0][0].Value.(string); got != "leader" {
		t.Fatalf("mrole mapped = %q, want %q", got, "leader")
	}
	if got := values[1][0].Value.(string); got != "ready" {
		t.Fatalf("mstatus mapped = %q, want %q", got, "ready")
	}
	if got := values[2][0].Value.(float64); got != 2.0 {
		t.Fatalf("cluster_uptime days = %v, want 2.0", got)
	}
	if got := values[3][0].Value.(string); got != "node-1" {
		t.Fatalf("name = %q, want %q", got, "node-1")
	}
	if values[0][0].Label["zone"] != "z1" {
		t.Fatalf("label zone = %q, want %q", values[0][0].Label["zone"], "z1")
	}
}

func TestProcessorProcess_SelectedSetMetricValues_AppendsOldAndSetsNew(t *testing.T) {
	p := &Processor{
		prefix:    "pref",
		metricMap: map[string]*Metric{},
	}

	tableName := "t"
	table := &Table{
		ColumnList: []string{"c1", "c2"},
	}

	fq1 := p.buildFQName(tableName, "c1")
	fq2 := p.buildFQName(tableName, "c2")
	m1 := &Metric{}
	m2 := &Metric{}
	m2.SetValue([]*Value{
		{Label: map[string]string{"tag": "a"}, Value: "old"},
	})
	p.metricMap[fq1] = m1
	p.metricMap[fq2] = m2

	values := make([][]*Value, len(table.ColumnList))
	values[0] = []*Value{{Label: map[string]string{"t": "x"}, Value: 1.0}}
	values[1] = []*Value{{Label: map[string]string{"t": "y"}, Value: "new"}}

	for i, column := range table.ColumnList {
		metric := p.metricMap[p.buildFQName(tableName, column)]
		for _, value := range values[i] {
			logger.Tracef("set metric:%s, Label:%v, Value:%v", column, value.Label, value.Value)
		}
		if metric.GetValue() != nil {
			values[i] = append(values[i], metric.GetValue()...)
		}
		metric.SetValue(values[i])
	}

	got1 := p.metricMap[fq1].GetValue()
	if len(got1) != 1 || got1[0].Value.(float64) != 1.0 || got1[0].Label["t"] != "x" {
		t.Fatalf("metric c1 values unexpected: %+v", got1)
	}

	got2 := p.metricMap[fq2].GetValue()
	if len(got2) != 2 {
		t.Fatalf("metric c2 values length = %d, want 2", len(got2))
	}
	if got2[0].Label["t"] != "y" || got2[0].Value.(string) != "new" {
		t.Fatalf("metric c2 first value unexpected: %+v", got2[0])
	}
	if got2[1].Label["tag"] != "a" || got2[1].Value.(string) != "old" {
		t.Fatalf("metric c2 appended old value unexpected: %+v", got2[1])
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
