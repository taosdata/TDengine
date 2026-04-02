package nodeexporter

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

const validUniqueGauge = `# HELP cadvisor_version_info A metric with a constant '1' value labeled by kernel version, OS version, docker version, cadvisor version & cadvisor revision.
# TYPE cadvisor_version_info gauge
cadvisor_version_info{cadvisorRevision="",cadvisorVersion="",dockerVersion="1.8.2",kernelVersion="3.10.0-229.20.1.el7.x86_64",osVersion="CentOS Linux 7 (Core)"} 1
`

const validUniqueCounter = `# HELP get_token_fail_count Counter of failed Token() requests to the alternate token source
# TYPE get_token_fail_count counter
get_token_fail_count 0
`

const validUniqueSummary = `# HELP http_request_duration_microseconds The HTTP request latencies in microseconds.
# TYPE http_request_duration_microseconds summary
http_request_duration_microseconds{handler="prometheus",quantile="0.5"} 552048.506
http_request_duration_microseconds{handler="prometheus",quantile="0.9"} 5.876804288e+06
http_request_duration_microseconds{handler="prometheus",quantile="0.99"} 5.876804288e+06
http_request_duration_microseconds_sum{handler="prometheus"} 1.8909097205e+07
http_request_duration_microseconds_count{handler="prometheus"} 9
`

const validUniqueHistogram = `# HELP apiserver_request_latencies Response latency distribution in microseconds for each verb, resource and client.
# TYPE apiserver_request_latencies histogram
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="125000"} 1994
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="250000"} 1997
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="500000"} 2000
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="1e+06"} 2005
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="2e+06"} 2012
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="4e+06"} 2017
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="8e+06"} 2024
apiserver_request_latencies_bucket{resource="bindings",verb="POST",le="+Inf"} 2025
apiserver_request_latencies_sum{resource="bindings",verb="POST"} 1.02726334e+08
apiserver_request_latencies_count{resource="bindings",verb="POST"} 2025
`

func TestParseValidPrometheus(t *testing.T) {
	// Gauge value
	metrics, err := Parse([]byte(validUniqueGauge), http.Header{}, false)
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, "cadvisor_version_info", metrics[0].Name())
	require.Equal(t, map[string]interface{}{
		"gauge": float64(1),
	}, metrics[0].Fields())
	require.Equal(t, map[string]string{
		"osVersion":        "CentOS Linux 7 (Core)",
		"cadvisorRevision": "",
		"cadvisorVersion":  "",
		"dockerVersion":    "1.8.2",
		"kernelVersion":    "3.10.0-229.20.1.el7.x86_64",
	}, metrics[0].Tags())

	// Counter value
	metrics, err = Parse([]byte(validUniqueCounter), http.Header{}, false)
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, "get_token_fail_count", metrics[0].Name())
	require.Equal(t, map[string]interface{}{
		"counter": float64(0),
	}, metrics[0].Fields())
	require.Equal(t, map[string]string{}, metrics[0].Tags())

	// Summary data
	//SetDefaultTags(map[string]string{})
	metrics, err = Parse([]byte(validUniqueSummary), http.Header{}, false)
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, "http_request_duration_microseconds", metrics[0].Name())
	require.Equal(t, map[string]interface{}{
		"0.5":   552048.506,
		"0.9":   5.876804288e+06,
		"0.99":  5.876804288e+06,
		"count": 9.0,
		"sum":   1.8909097205e+07,
	}, metrics[0].Fields())
	require.Equal(t, map[string]string{"handler": "prometheus"}, metrics[0].Tags())

	// histogram data
	metrics, err = Parse([]byte(validUniqueHistogram), http.Header{}, false)
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, "apiserver_request_latencies", metrics[0].Name())
	require.Equal(t, map[string]interface{}{
		"500000": 2000.0,
		"count":  2025.0,
		"sum":    1.02726334e+08,
		"250000": 1997.0,
		"2e+06":  2012.0,
		"4e+06":  2017.0,
		"8e+06":  2024.0,
		"+Inf":   2025.0,
		"125000": 1994.0,
		"1e+06":  2005.0,
	}, metrics[0].Fields())
	require.Equal(t,
		map[string]string{"verb": "POST", "resource": "bindings"},
		metrics[0].Tags())
}

func TestMetricsWithTimestamp(t *testing.T) {
	testTime := time.Date(2020, time.October, 4, 17, 0, 0, 0, time.UTC)
	testTimeUnix := testTime.UnixNano() / int64(time.Millisecond)
	metricsWithTimestamps := fmt.Sprintf(`
# TYPE test_counter counter
test_counter{label="test"} 1 %d
`, testTimeUnix)

	// IgnoreTimestamp is false
	metrics, err := Parse([]byte(metricsWithTimestamps), http.Header{}, false)
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, "test_counter", metrics[0].Name())
	require.Equal(t, map[string]interface{}{
		"counter": float64(1),
	}, metrics[0].Fields())
	require.Equal(t, map[string]string{
		"label": "test",
	}, metrics[0].Tags())
	require.Equal(t, testTime, metrics[0].Time().UTC())

	// IgnoreTimestamp is true
	metrics, err = Parse([]byte(metricsWithTimestamps), http.Header{}, true)
	require.NoError(t, err)
	require.Len(t, metrics, 1)
	require.Equal(t, "test_counter", metrics[0].Name())
	require.Equal(t, map[string]interface{}{
		"counter": float64(1),
	}, metrics[0].Fields())
	require.Equal(t, map[string]string{
		"label": "test",
	}, metrics[0].Tags())
	require.WithinDuration(t, time.Now(), metrics[0].Time().UTC(), 5*time.Second)
}

func TestValueType_AllCases(t *testing.T) {
	require.Equal(t, telegraf.Counter, ValueType(dto.MetricType_COUNTER))
	require.Equal(t, telegraf.Gauge, ValueType(dto.MetricType_GAUGE))
	require.Equal(t, telegraf.Summary, ValueType(dto.MetricType_SUMMARY))
	require.Equal(t, telegraf.Histogram, ValueType(dto.MetricType_HISTOGRAM))
	require.Equal(t, telegraf.Untyped, ValueType(dto.MetricType_UNTYPED))
	require.Equal(t, telegraf.Untyped, ValueType(dto.MetricType(100)))
}

func TestMakeLabels(t *testing.T) {
	m := &dto.Metric{
		Label: []*dto.LabelPair{
			{Name: strPtr("foo"), Value: strPtr("bar")},
			{Name: strPtr("baz"), Value: strPtr("qux")},
		},
	}
	labels := MakeLabels(m)
	require.Equal(t, map[string]string{
		"foo": "bar",
		"baz": "qux",
	}, labels)

	m = &dto.Metric{
		Label: []*dto.LabelPair{
			{Name: strPtr("empty"), Value: strPtr("")},
		},
	}
	labels = MakeLabels(m)
	require.Equal(t, map[string]string{"empty": ""}, labels)

	m = &dto.Metric{}
	labels = MakeLabels(m)
	require.Empty(t, labels)
}

func strPtr(s string) *string {
	return &s
}
