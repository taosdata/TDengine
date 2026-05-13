package statsd

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	telegrafMetric "github.com/influxdata/telegraf/metric"
)

const (
	testMsg         = "test.tcp.msg:100|c"
	producerThreads = 10
)

func NewTestStatsd() *Statsd {
	s := Statsd{
		User:     "root",
		Password: "taosdata",
		Log:      logrus.New().WithField("test", "TestStatsd"),
	}

	// Make data structures
	s.done = make(chan struct{})
	s.in = make(chan input, s.AllowedPendingMessages)
	s.gauges = make(map[string]cachedgauge)
	s.counters = make(map[string]cachedcounter)
	s.sets = make(map[string]cachedset)
	s.timings = make(map[string]cachedtimings)
	s.distributions = make([]cacheddistributions, 0)

	s.MetricSeparator = "_"

	return &s
}

// Test that MaxTCPConnections is respected
func TestConcurrentConns(t *testing.T) {
	listener := Statsd{
		User:                   "root",
		Password:               "taosdata",
		Log:                    logrus.New().WithField("test", "TestConcurrentConns"),
		Protocol:               "tcp",
		ServiceAddress:         "localhost:8125",
		AllowedPendingMessages: 10000,
		MaxTCPConnections:      2,
	}

	acc := &Accumulator{}
	require.NoError(t, listener.Start(acc))
	defer listener.Stop()

	time.Sleep(time.Millisecond * 250)
	_, err := net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)
	_, err = net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)

	// Connection over the limit:
	conn, err := net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)
	_, err = net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)
	_, err = conn.Write([]byte(testMsg))
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 100)
	require.Zero(t, acc.NFields())
}

// Test that MaxTCPConnections is respected when max==1
func TestConcurrentConns1(t *testing.T) {
	listener := Statsd{
		User:                   "root",
		Password:               "taosdata",
		Log:                    logrus.New().WithField("test", "TestConcurrentConns1"),
		Protocol:               "tcp",
		ServiceAddress:         "localhost:8125",
		AllowedPendingMessages: 10000,
		MaxTCPConnections:      1,
	}

	acc := &Accumulator{}
	require.NoError(t, listener.Start(acc))
	defer listener.Stop()

	time.Sleep(time.Millisecond * 250)
	_, err := net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)

	// Connection over the limit:
	conn, err := net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)
	_, err = net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)
	_, err = conn.Write([]byte(testMsg))
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 100)
	require.Zero(t, acc.NFields())
}

// Test that MaxTCPConnections is respected
func TestCloseConcurrentConns(t *testing.T) {
	listener := Statsd{
		User:                   "root",
		Password:               "taosdata",
		Log:                    logrus.New().WithField("test", "TestCloseConcurrentConns"),
		Protocol:               "tcp",
		ServiceAddress:         "localhost:8125",
		AllowedPendingMessages: 10000,
		MaxTCPConnections:      2,
	}

	acc := &Accumulator{}
	require.NoError(t, listener.Start(acc))

	time.Sleep(time.Millisecond * 250)
	_, err := net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)
	_, err = net.Dial("tcp", "127.0.0.1:8125")
	require.NoError(t, err)

	listener.Stop()
}

// benchmark how long it takes to accept & process 100,000 metrics:
func BenchmarkUDP(b *testing.B) {
	listener := Statsd{
		User:                   "root",
		Password:               "taosdata",
		Log:                    logrus.New().WithField("test", "BenchmarkUDP"),
		Protocol:               "udp",
		ServiceAddress:         "localhost:8125",
		AllowedPendingMessages: 250000,
	}
	acc := &Accumulator{Discard: true}

	// send multiple messages to socket
	for n := 0; n < b.N; n++ {
		require.NoError(b, listener.Start(acc))

		time.Sleep(time.Millisecond * 250)
		conn, err := net.Dial("udp", "127.0.0.1:8125")
		require.NoError(b, err)

		var wg sync.WaitGroup
		for i := 1; i <= producerThreads; i++ {
			wg.Add(1)
			go sendRequests(conn, &wg)
		}
		wg.Wait()

		// wait for 250,000 metrics to get added to accumulator
		for len(listener.in) > 0 {
			time.Sleep(time.Millisecond)
		}
		listener.Stop()
	}
}

func sendRequests(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 25000; i++ {
		//nolint:errcheck,revive
		fmt.Fprint(conn, testMsg)
	}
}

// benchmark how long it takes to accept & process 100,000 metrics:
func BenchmarkTCP(b *testing.B) {
	listener := Statsd{
		User:                   "root",
		Password:               "taosdata",
		Log:                    logrus.New().WithField("test", "BenchmarkTCP"),
		Protocol:               "tcp",
		ServiceAddress:         "localhost:8125",
		AllowedPendingMessages: 250000,
		MaxTCPConnections:      250,
	}
	acc := &Accumulator{Discard: true}

	// send multiple messages to socket
	for n := 0; n < b.N; n++ {
		require.NoError(b, listener.Start(acc))

		time.Sleep(time.Millisecond * 250)
		conn, err := net.Dial("tcp", "127.0.0.1:8125")
		require.NoError(b, err)

		var wg sync.WaitGroup
		for i := 1; i <= producerThreads; i++ {
			wg.Add(1)
			go sendRequests(conn, &wg)
		}
		wg.Wait()
		// wait for 250,000 metrics to get added to accumulator
		for len(listener.in) > 0 {
			time.Sleep(time.Millisecond)
		}
		listener.Stop()
	}
}

// Valid lines should be parsed and their values should be cached
func TestParse_ValidLines(t *testing.T) {
	s := NewTestStatsd()
	validLines := []string{
		"valid:45|c",
		"valid:45|s",
		"valid:45|g",
		"valid.timer:45|ms",
		"valid.timer:45|h",
	}

	for _, line := range validLines {
		require.NoError(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}
}

// Tests low-level functionality of gauges
func TestParse_Gauges(t *testing.T) {
	s := NewTestStatsd()

	// Test that gauge +- values work
	validLines := []string{
		"plus.minus:100|g",
		"plus.minus:-10|g",
		"plus.minus:+30|g",
		"plus.plus:100|g",
		"plus.plus:+100|g",
		"plus.plus:+100|g",
		"minus.minus:100|g",
		"minus.minus:-100|g",
		"minus.minus:-100|g",
		"lone.plus:+100|g",
		"lone.minus:-100|g",
		"overwrite:100|g",
		"overwrite:300|g",
		"scientific.notation:4.696E+5|g",
		"scientific.notation.minus:4.7E-5|g",
	}

	for _, line := range validLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	validations := []struct {
		name  string
		value float64
	}{
		{
			"scientific_notation",
			469600,
		},
		{
			"scientific_notation_minus",
			0.000047,
		},
		{
			"plus_minus",
			120,
		},
		{
			"plus_plus",
			300,
		},
		{
			"minus_minus",
			-100,
		},
		{
			"lone_plus",
			100,
		},
		{
			"lone_minus",
			-100,
		},
		{
			"overwrite",
			300,
		},
	}

	for _, test := range validations {
		require.NoError(t, testValidateGauge(test.name, test.value, s.gauges))
	}
}

// Tests low-level functionality of sets
func TestParse_Sets(t *testing.T) {
	s := NewTestStatsd()

	// Test that sets work
	validLines := []string{
		"unique.user.ids:100|s",
		"unique.user.ids:100|s",
		"unique.user.ids:100|s",
		"unique.user.ids:100|s",
		"unique.user.ids:100|s",
		"unique.user.ids:101|s",
		"unique.user.ids:102|s",
		"unique.user.ids:102|s",
		"unique.user.ids:123456789|s",
		"oneuser.id:100|s",
		"oneuser.id:100|s",
		"scientific.notation.sets:4.696E+5|s",
		"scientific.notation.sets:4.696E+5|s",
		"scientific.notation.sets:4.697E+5|s",
		"string.sets:foobar|s",
		"string.sets:foobar|s",
		"string.sets:bar|s",
	}

	for _, line := range validLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"scientific_notation_sets",
			2,
		},
		{
			"unique_user_ids",
			4,
		},
		{
			"oneuser_id",
			1,
		},
		{
			"string_sets",
			2,
		},
	}

	for _, test := range validations {
		require.NoError(t, testValidateSet(test.name, test.value, s.sets))
	}
}

// Tests low-level functionality of counters
func TestParse_Counters(t *testing.T) {
	s := NewTestStatsd()

	// Test that counters work
	validLines := []string{
		"small.inc:1|c",
		"big.inc:100|c",
		"big.inc:1|c",
		"big.inc:100000|c",
		"big.inc:1000000|c",
		"small.inc:1|c",
		"zero.init:0|c",
		"sample.rate:1|c|@0.1",
		"sample.rate:1|c",
		"scientific.notation:4.696E+5|c",
		"negative.test:100|c",
		"negative.test:-5|c",
	}

	for _, line := range validLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"scientific_notation",
			469600,
		},
		{
			"small_inc",
			2,
		},
		{
			"big_inc",
			1100101,
		},
		{
			"zero_init",
			0,
		},
		{
			"sample_rate",
			11,
		},
		{
			"negative_test",
			95,
		},
	}

	for _, test := range validations {
		require.NoError(t, testValidateCounter(test.name, test.value, s.counters))
	}
}

// Tests low-level functionality of timings
func TestParse_Timings(t *testing.T) {
	s := NewTestStatsd()
	s.Percentiles = []Number{90.0}
	acc := &Accumulator{}

	// Test that timings work
	validLines := []string{
		"test.timing:1|ms",
		"test.timing:11|ms",
		"test.timing:1|ms",
		"test.timing:1|ms",
		"test.timing:1|ms",
	}

	for _, line := range validLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	require.NoError(t, s.Gather(acc))

	valid := map[string]interface{}{
		"90_percentile": float64(11),
		"count":         int64(5),
		"lower":         float64(1),
		"mean":          float64(3),
		"stddev":        float64(4),
		"sum":           float64(15),
		"upper":         float64(11),
	}

	acc.AssertContainsFields(t, "test_timing", valid)
}

// Tests low-level functionality of distributions
func TestParse_Distributions(t *testing.T) {
	s := NewTestStatsd()
	acc := &Accumulator{}

	parseMetrics := func() {
		// Test that distributions work
		validLines := []string{
			"test.distribution:1|d",
			"test.distribution2:2|d",
			"test.distribution3:3|d",
			"test.distribution4:1|d",
			"test.distribution5:1|d",
		}

		for _, line := range validLines {
			require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
		}

		require.NoError(t, s.Gather(acc))
	}

	validMeasurementMap := map[string]float64{
		"test_distribution":  1,
		"test_distribution2": 2,
		"test_distribution3": 3,
		"test_distribution4": 1,
		"test_distribution5": 1,
	}

	// Test parsing when DataDogExtensions and DataDogDistributions aren't enabled
	parseMetrics()
	for key := range validMeasurementMap {
		acc.AssertDoesNotContainMeasurement(t, key)
	}

	// Test parsing when DataDogDistributions is enabled but not DataDogExtensions
	s.DataDogDistributions = true
	parseMetrics()
	for key := range validMeasurementMap {
		acc.AssertDoesNotContainMeasurement(t, key)
	}

	// Test parsing when DataDogExtensions and DataDogDistributions are enabled
	s.DataDogExtensions = true
	parseMetrics()
	for key, value := range validMeasurementMap {
		field := map[string]interface{}{
			"value": value,
		}
		acc.AssertContainsFields(t, key, field)
	}
}

func TestParseScientificNotation(t *testing.T) {
	s := NewTestStatsd()
	sciNotationLines := []string{
		"scientific.notation:4.6968460083008E-5|ms",
		"scientific.notation:4.6968460083008E-5|g",
		"scientific.notation:4.6968460083008E-5|c",
		"scientific.notation:4.6968460083008E-5|h",
	}
	for _, line := range sciNotationLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line [%s] should not have resulted in error", line)
	}
}

// Invalid lines should return an error
func TestParse_InvalidLines(t *testing.T) {
	s := NewTestStatsd()
	invalidLines := []string{
		"i.dont.have.a.pipe:45g",
		"i.dont.have.a.colon45|c",
		"invalid.metric.type:45|e",
		"invalid.plus.minus.non.gauge:+10|s",
		"invalid.plus.minus.non.gauge:+10|ms",
		"invalid.plus.minus.non.gauge:+10|h",
		"invalid.value:foobar|c",
		"invalid.value:d11|c",
		"invalid.value:1d1|c",
	}
	for _, line := range invalidLines {
		require.Errorf(t, s.parseStatsdLine(line), "Parsing line %s should have resulted in an error", line)
	}
}

// Invalid sample rates should be ignored and not applied
func TestParse_InvalidSampleRate(t *testing.T) {
	s := NewTestStatsd()
	invalidLines := []string{
		"invalid.sample.rate:45|c|0.1",
		"invalid.sample.rate.2:45|c|@foo",
		"invalid.sample.rate:45|g|@0.1",
		"invalid.sample.rate:45|s|@0.1",
	}

	for _, line := range invalidLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	counterValidations := []struct {
		name  string
		value int64
		cache map[string]cachedcounter
	}{
		{
			"invalid_sample_rate",
			45,
			s.counters,
		},
		{
			"invalid_sample_rate_2",
			45,
			s.counters,
		},
	}

	for _, test := range counterValidations {
		require.NoError(t, testValidateCounter(test.name, test.value, test.cache))
	}

	require.NoError(t, testValidateGauge("invalid_sample_rate", 45, s.gauges))

	require.NoError(t, testValidateSet("invalid_sample_rate", 1, s.sets))
}

// Names should be parsed like . -> _
func TestParse_DefaultNameParsing(t *testing.T) {
	s := NewTestStatsd()
	validLines := []string{
		"valid:1|c",
		"valid.foo-bar:11|c",
	}

	for _, line := range validLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"valid",
			1,
		},
		{
			"valid_foo-bar",
			11,
		},
	}

	for _, test := range validations {
		require.NoError(t, testValidateCounter(test.name, test.value, s.counters))
	}
}

// Test that template name transformation works
func TestParse_Template(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{
		"measurement.measurement.host.service",
	}

	lines := []string{
		"cpu.idle.localhost:1|c",
		"cpu.busy.host01.myservice:11|c",
	}

	for _, line := range lines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"cpu_idle",
			1,
		},
		{
			"cpu_busy",
			11,
		},
	}

	// Validate counters
	for _, test := range validations {
		require.NoError(t, testValidateCounter(test.name, test.value, s.counters))
	}
}

// Test that template filters properly
func TestParse_TemplateFilter(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{
		"cpu.idle.* measurement.measurement.host",
	}

	lines := []string{
		"cpu.idle.localhost:1|c",
		"cpu.busy.host01.myservice:11|c",
	}

	for _, line := range lines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"cpu_idle",
			1,
		},
		{
			"cpu_busy_host01_myservice",
			11,
		},
	}

	// Validate counters
	for _, test := range validations {
		require.NoError(t, testValidateCounter(test.name, test.value, s.counters))
	}
}

// Test that most specific template is chosen
func TestParse_TemplateSpecificity(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{
		"cpu.* measurement.foo.host",
		"cpu.idle.* measurement.measurement.host",
	}

	lines := []string{
		"cpu.idle.localhost:1|c",
	}

	for _, line := range lines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"cpu_idle",
			1,
		},
	}

	// Validate counters
	for _, test := range validations {
		require.NoError(t, testValidateCounter(test.name, test.value, s.counters))
	}
}

// Test that most specific template is chosen
func TestParse_TemplateFields(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{
		"* measurement.measurement.field",
	}

	lines := []string{
		"my.counter.f1:1|c",
		"my.counter.f1:1|c",
		"my.counter.f2:1|c",
		"my.counter.f3:10|c",
		"my.counter.f3:100|c",
		"my.gauge.f1:10.1|g",
		"my.gauge.f2:10.1|g",
		"my.gauge.f1:0.9|g",
		"my.set.f1:1|s",
		"my.set.f1:2|s",
		"my.set.f1:1|s",
		"my.set.f2:100|s",
	}

	for _, line := range lines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	counterTests := []struct {
		name  string
		value int64
		field string
	}{
		{
			"my_counter",
			2,
			"f1",
		},
		{
			"my_counter",
			1,
			"f2",
		},
		{
			"my_counter",
			110,
			"f3",
		},
	}
	// Validate counters
	for _, test := range counterTests {
		require.NoError(t, testValidateCounter(test.name, test.value, s.counters, test.field))
	}

	gaugeTests := []struct {
		name  string
		value float64
		field string
	}{
		{
			"my_gauge",
			0.9,
			"f1",
		},
		{
			"my_gauge",
			10.1,
			"f2",
		},
	}
	// Validate gauges
	for _, test := range gaugeTests {
		require.NoError(t, testValidateGauge(test.name, test.value, s.gauges, test.field))
	}

	setTests := []struct {
		name  string
		value int64
		field string
	}{
		{
			"my_set",
			2,
			"f1",
		},
		{
			"my_set",
			1,
			"f2",
		},
	}
	// Validate sets
	for _, test := range setTests {
		require.NoError(t, testValidateSet(test.name, test.value, s.sets, test.field))
	}
}

// Test that fields are parsed correctly
func TestParse_Fields(t *testing.T) {
	if false {
		t.Errorf("TODO")
	}
}

// Test that tags within the bucket are parsed correctly
func TestParse_Tags(t *testing.T) {
	s := NewTestStatsd()

	tests := []struct {
		bucket string
		name   string
		tags   map[string]string
	}{
		{
			"cpu.idle,host=localhost",
			"cpu_idle",
			map[string]string{
				"host": "localhost",
			},
		},
		{
			"cpu.idle,host=localhost,region=west",
			"cpu_idle",
			map[string]string{
				"host":   "localhost",
				"region": "west",
			},
		},
		{
			"cpu.idle,host=localhost,color=red,region=west",
			"cpu_idle",
			map[string]string{
				"host":   "localhost",
				"region": "west",
				"color":  "red",
			},
		},
	}

	for _, test := range tests {
		name, _, tags := s.parseName(test.bucket)
		require.Equalf(t, name, test.name, "Expected: %s, got %s", test.name, name)

		for k, v := range test.tags {
			actual, ok := tags[k]
			require.Truef(t, ok, "Expected key: %s not found", k)
			require.Equalf(t, actual, v, "Expected %s, got %s", v, actual)
		}
	}
}

func TestParse_DataDogTags(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		expected []telegraf.Metric
	}{
		{
			name: "counter",
			line: "my_counter:1|c|#host:localhost,environment:prod,endpoint:/:tenant?/oauth/ro",
			expected: []telegraf.Metric{
				telegrafMetric.New(
					"my_counter",
					map[string]string{
						"endpoint":    "/:tenant?/oauth/ro",
						"environment": "prod",
						"host":        "localhost",
						"metric_type": "counter",
					},
					map[string]interface{}{
						"value": 1,
					},
					time.Now(),
					telegraf.Counter,
				),
			},
		},
		{
			name: "gauge",
			line: "my_gauge:10.1|g|#live",
			expected: []telegraf.Metric{
				telegrafMetric.New(
					"my_gauge",
					map[string]string{
						"live":        "true",
						"metric_type": "gauge",
					},
					map[string]interface{}{
						"value": 10.1,
					},
					time.Now(),
					telegraf.Gauge,
				),
			},
		},
		{
			name: "set",
			line: "my_set:1|s|#host:localhost",
			expected: []telegraf.Metric{
				telegrafMetric.New(
					"my_set",
					map[string]string{
						"host":        "localhost",
						"metric_type": "set",
					},
					map[string]interface{}{
						"value": 1,
					},
					time.Now(),
				),
			},
		},
		{
			name: "timer",
			line: "my_timer:3|ms|@0.1|#live,host:localhost",
			expected: []telegraf.Metric{
				telegrafMetric.New(
					"my_timer",
					map[string]string{
						"host":        "localhost",
						"live":        "true",
						"metric_type": "timing",
					},
					map[string]interface{}{
						"count":  10,
						"lower":  float64(3),
						"mean":   float64(3),
						"stddev": float64(0),
						"sum":    float64(30),
						"upper":  float64(3),
					},
					time.Now(),
				),
			},
		},
		{
			name: "empty tag set",
			line: "cpu:42|c|#",
			expected: []telegraf.Metric{
				telegrafMetric.New(
					"cpu",
					map[string]string{
						"metric_type": "counter",
					},
					map[string]interface{}{
						"value": 42,
					},
					time.Now(),
					telegraf.Counter,
				),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var acc Accumulator

			s := NewTestStatsd()
			s.DataDogExtensions = true

			require.NoError(t, s.parseStatsdLine(tt.line))
			require.NoError(t, s.Gather(&acc))

			RequireMetricsEqual(t, tt.expected, acc.GetTelegrafMetrics(),
				SortMetrics(), IgnoreTime())
		})
	}
}

// Test that statsd buckets are parsed to measurement names properly
func TestParseName(t *testing.T) {
	s := NewTestStatsd()

	tests := []struct {
		inName  string
		outName string
	}{
		{
			"foobar",
			"foobar",
		},
		{
			"foo.bar",
			"foo_bar",
		},
		{
			"foo.bar-baz",
			"foo_bar-baz",
		},
	}

	for _, test := range tests {
		name, _, _ := s.parseName(test.inName)
		require.Equalf(t, name, test.outName, "Expected: %s, got %s", test.outName, name)
	}

	// Test with separator == "."
	s.MetricSeparator = "."

	tests = []struct {
		inName  string
		outName string
	}{
		{
			"foobar",
			"foobar",
		},
		{
			"foo.bar",
			"foo.bar",
		},
		{
			"foo.bar-baz",
			"foo.bar-baz",
		},
	}

	for _, test := range tests {
		name, _, _ := s.parseName(test.inName)
		require.Equalf(t, name, test.outName, "Expected: %s, got %s", test.outName, name)
	}
}

// Test that measurements with the same name, but different tags, are treated
// as different outputs
func TestParse_MeasurementsWithSameName(t *testing.T) {
	s := NewTestStatsd()

	// Test that counters work
	validLines := []string{
		"test.counter,host=localhost:1|c",
		"test.counter,host=localhost,region=west:1|c",
	}

	for _, line := range validLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	require.Lenf(t, s.counters, 2, "Expected 2 separate measurements, found %d", len(s.counters))
}

// Test that the metric caches expire (clear) an entry after the entry hasn't been updated for the configurable MaxTTL duration.
func TestCachesExpireAfterMaxTTL(t *testing.T) {
	s := NewTestStatsd()
	s.MaxTTL = 100 * time.Microsecond

	acc := &Accumulator{}
	require.NoError(t, s.parseStatsdLine("valid:45|c"))
	require.NoError(t, s.parseStatsdLine("valid:45|c"))
	require.NoError(t, s.Gather(acc))

	// Max TTL goes by, our 'valid' entry is cleared.
	time.Sleep(100 * time.Microsecond)
	require.NoError(t, s.Gather(acc))

	// Now when we gather, we should have a counter that is reset to zero.
	require.NoError(t, s.parseStatsdLine("valid:45|c"))
	require.NoError(t, s.Gather(acc))

	// Wait for the metrics to arrive
	acc.Wait(3)

	RequireMetricsEqual(t,
		[]telegraf.Metric{
			telegrafMetric.New(
				"valid",
				map[string]string{
					"metric_type": "counter",
				},
				map[string]interface{}{
					"value": 90,
				},
				time.Now(),
				telegraf.Counter,
			),
			telegrafMetric.New(
				"valid",
				map[string]string{
					"metric_type": "counter",
				},
				map[string]interface{}{
					"value": 90,
				},
				time.Now(),
				telegraf.Counter,
			),
			telegrafMetric.New(
				"valid",
				map[string]string{
					"metric_type": "counter",
				},
				map[string]interface{}{
					"value": 45,
				},
				time.Now(),
				telegraf.Counter,
			),
		},
		acc.GetTelegrafMetrics(),
		IgnoreTime(),
	)
}

// Test that measurements with multiple bits, are treated as different outputs
// but are equal to their single-measurement representation
func TestParse_MeasurementsWithMultipleValues(t *testing.T) {
	singleLines := []string{
		"valid.multiple:0|ms|@0.1",
		"valid.multiple:0|ms|",
		"valid.multiple:1|ms",
		"valid.multiple.duplicate:1|c",
		"valid.multiple.duplicate:1|c",
		"valid.multiple.duplicate:2|c",
		"valid.multiple.duplicate:1|c",
		"valid.multiple.duplicate:1|h",
		"valid.multiple.duplicate:1|h",
		"valid.multiple.duplicate:2|h",
		"valid.multiple.duplicate:1|h",
		"valid.multiple.duplicate:1|s",
		"valid.multiple.duplicate:1|s",
		"valid.multiple.duplicate:2|s",
		"valid.multiple.duplicate:1|s",
		"valid.multiple.duplicate:1|g",
		"valid.multiple.duplicate:1|g",
		"valid.multiple.duplicate:2|g",
		"valid.multiple.duplicate:1|g",
		"valid.multiple.mixed:1|c",
		"valid.multiple.mixed:1|ms",
		"valid.multiple.mixed:2|s",
		"valid.multiple.mixed:1|g",
	}

	multipleLines := []string{
		"valid.multiple:0|ms|@0.1:0|ms|:1|ms",
		"valid.multiple.duplicate:1|c:1|c:2|c:1|c",
		"valid.multiple.duplicate:1|h:1|h:2|h:1|h",
		"valid.multiple.duplicate:1|s:1|s:2|s:1|s",
		"valid.multiple.duplicate:1|g:1|g:2|g:1|g",
		"valid.multiple.mixed:1|c:1|ms:2|s:1|g",
	}

	sSingle := NewTestStatsd()
	sMultiple := NewTestStatsd()

	for _, line := range singleLines {
		require.NoErrorf(t, sSingle.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	for _, line := range multipleLines {
		require.NoErrorf(t, sMultiple.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}

	require.Lenf(t, sSingle.timings, 3, "Expected 3 measurement, found %d", len(sSingle.timings))

	cachedtiming, ok := sSingle.timings["metric_type=timingvalid_multiple"]
	require.Truef(t, ok, "Expected cached measurement with hash 'metric_type=timingvalid_multiple' not found")
	require.Equalf(t, cachedtiming.name, "valid_multiple", "Expected the name to be 'valid_multiple', got %s", cachedtiming.name)

	// A 0 at samplerate 0.1 will add 10 values of 0,
	// A 0 with invalid samplerate will add a single 0,
	// plus the last bit of value 1
	// which adds up to 12 individual datapoints to be cached
	require.EqualValuesf(t, cachedtiming.fields[defaultFieldName].n, 12, "Expected 12 additions, got %d", cachedtiming.fields[defaultFieldName].n)

	require.EqualValuesf(t, cachedtiming.fields[defaultFieldName].upper, 1, "Expected max input to be 1, got %f", cachedtiming.fields[defaultFieldName].upper)

	// test if sSingle and sMultiple did compute the same stats for valid.multiple.duplicate
	require.NoError(t, testValidateSet("valid_multiple_duplicate", 2, sSingle.sets))

	require.NoError(t, testValidateSet("valid_multiple_duplicate", 2, sMultiple.sets))

	require.NoError(t, testValidateCounter("valid_multiple_duplicate", 5, sSingle.counters))

	require.NoError(t, testValidateCounter("valid_multiple_duplicate", 5, sMultiple.counters))

	require.NoError(t, testValidateGauge("valid_multiple_duplicate", 1, sSingle.gauges))

	require.NoError(t, testValidateGauge("valid_multiple_duplicate", 1, sMultiple.gauges))

	// test if sSingle and sMultiple did compute the same stats for valid.multiple.mixed
	require.NoError(t, testValidateSet("valid_multiple_mixed", 1, sSingle.sets))

	require.NoError(t, testValidateSet("valid_multiple_mixed", 1, sMultiple.sets))

	require.NoError(t, testValidateCounter("valid_multiple_mixed", 1, sSingle.counters))

	require.NoError(t, testValidateCounter("valid_multiple_mixed", 1, sMultiple.counters))

	require.NoError(t, testValidateGauge("valid_multiple_mixed", 1, sSingle.gauges))

	require.NoError(t, testValidateGauge("valid_multiple_mixed", 1, sMultiple.gauges))
}

// Tests low-level functionality of timings when multiple fields is enabled
// and a measurement template has been defined which can parse field names
func TestParse_TimingsMultipleFieldsWithTemplate(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{"measurement.field"}
	s.Percentiles = []Number{90.0}
	acc := &Accumulator{}

	validLines := []string{
		"test_timing.success:1|ms",
		"test_timing.success:11|ms",
		"test_timing.success:1|ms",
		"test_timing.success:1|ms",
		"test_timing.success:1|ms",
		"test_timing.error:2|ms",
		"test_timing.error:22|ms",
		"test_timing.error:2|ms",
		"test_timing.error:2|ms",
		"test_timing.error:2|ms",
	}

	for _, line := range validLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}
	require.NoError(t, s.Gather(acc))

	valid := map[string]interface{}{
		"success_90_percentile": float64(11),
		"success_count":         int64(5),
		"success_lower":         float64(1),
		"success_mean":          float64(3),
		"success_stddev":        float64(4),
		"success_sum":           float64(15),
		"success_upper":         float64(11),

		"error_90_percentile": float64(22),
		"error_count":         int64(5),
		"error_lower":         float64(2),
		"error_mean":          float64(6),
		"error_stddev":        float64(8),
		"error_sum":           float64(30),
		"error_upper":         float64(22),
	}

	acc.AssertContainsFields(t, "test_timing", valid)
}

// Tests low-level functionality of timings when multiple fields is enabled
// but a measurement template hasn't been defined so we can't parse field names
// In this case the behaviour should be the same as normal behaviour
func TestParse_TimingsMultipleFieldsWithoutTemplate(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{}
	s.Percentiles = []Number{90.0}
	acc := &Accumulator{}

	validLines := []string{
		"test_timing.success:1|ms",
		"test_timing.success:11|ms",
		"test_timing.success:1|ms",
		"test_timing.success:1|ms",
		"test_timing.success:1|ms",
		"test_timing.error:2|ms",
		"test_timing.error:22|ms",
		"test_timing.error:2|ms",
		"test_timing.error:2|ms",
		"test_timing.error:2|ms",
	}

	for _, line := range validLines {
		require.NoErrorf(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)
	}
	require.NoError(t, s.Gather(acc))

	expectedSuccess := map[string]interface{}{
		"90_percentile": float64(11),
		"count":         int64(5),
		"lower":         float64(1),
		"mean":          float64(3),
		"stddev":        float64(4),
		"sum":           float64(15),
		"upper":         float64(11),
	}
	expectedError := map[string]interface{}{
		"90_percentile": float64(22),
		"count":         int64(5),
		"lower":         float64(2),
		"mean":          float64(6),
		"stddev":        float64(8),
		"sum":           float64(30),
		"upper":         float64(22),
	}

	acc.AssertContainsFields(t, "test_timing_success", expectedSuccess)
	acc.AssertContainsFields(t, "test_timing_error", expectedError)
}

func BenchmarkParse(b *testing.B) {
	s := NewTestStatsd()
	validLines := []string{
		"test.timing.success:1|ms",
		"test.timing.success:11|ms",
		"test.timing.success:1|ms",
		"test.timing.success:1|ms",
		"test.timing.success:1|ms",
		"test.timing.error:2|ms",
		"test.timing.error:22|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func BenchmarkParseWithTemplate(b *testing.B) {
	s := NewTestStatsd()
	s.Templates = []string{"measurement.measurement.field"}
	validLines := []string{
		"test.timing.success:1|ms",
		"test.timing.success:11|ms",
		"test.timing.success:1|ms",
		"test.timing.success:1|ms",
		"test.timing.success:1|ms",
		"test.timing.error:2|ms",
		"test.timing.error:22|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func BenchmarkParseWithTemplateAndFilter(b *testing.B) {
	s := NewTestStatsd()
	s.Templates = []string{"cpu* measurement.measurement.field"}
	validLines := []string{
		"test.timing.success:1|ms",
		"test.timing.success:11|ms",
		"test.timing.success:1|ms",
		"cpu.timing.success:1|ms",
		"cpu.timing.success:1|ms",
		"cpu.timing.error:2|ms",
		"cpu.timing.error:22|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func BenchmarkParseWith2TemplatesAndFilter(b *testing.B) {
	s := NewTestStatsd()
	s.Templates = []string{
		"cpu1* measurement.measurement.field",
		"cpu2* measurement.measurement.field",
	}
	validLines := []string{
		"test.timing.success:1|ms",
		"test.timing.success:11|ms",
		"test.timing.success:1|ms",
		"cpu1.timing.success:1|ms",
		"cpu1.timing.success:1|ms",
		"cpu2.timing.error:2|ms",
		"cpu2.timing.error:22|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func BenchmarkParseWith2Templates3TagsAndFilter(b *testing.B) {
	s := NewTestStatsd()
	s.Templates = []string{
		"cpu1* measurement.measurement.region.city.rack.field",
		"cpu2* measurement.measurement.region.city.rack.field",
	}
	validLines := []string{
		"test.timing.us-east.nyc.rack01.success:1|ms",
		"test.timing.us-east.nyc.rack01.success:11|ms",
		"test.timing.us-west.sf.rack01.success:1|ms",
		"cpu1.timing.us-west.sf.rack01.success:1|ms",
		"cpu1.timing.us-east.nyc.rack01.success:1|ms",
		"cpu2.timing.us-east.nyc.rack01.error:2|ms",
		"cpu2.timing.us-west.sf.rack01.error:22|ms",
		"test.timing.us-west.sf.rack01.error:2|ms",
		"test.timing.us-west.sf.rack01.error:2|ms",
		"test.timing.us-east.nyc.rack01.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func TestParse_Timings_Delete(t *testing.T) {
	s := NewTestStatsd()
	s.DeleteTimings = true
	fakeacc := &Accumulator{}

	line := "timing:100|ms"
	require.NoError(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)

	require.Lenf(t, s.timings, 1, "Should be 1 timing, found %d", len(s.timings))

	require.NoError(t, s.Gather(fakeacc))

	require.Lenf(t, s.timings, 0, "All timings should have been deleted, found %d", len(s.timings))
}

// Tests the delete_gauges option
func TestParse_Gauges_Delete(t *testing.T) {
	s := NewTestStatsd()
	s.DeleteGauges = true
	fakeacc := &Accumulator{}

	line := "current.users:100|g"
	require.NoError(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)

	require.NoError(t, testValidateGauge("current_users", 100, s.gauges))

	require.NoError(t, s.Gather(fakeacc))

	require.Error(t, testValidateGauge("current_users", 100, s.gauges), "current_users_gauge metric should have been deleted")
}

// Tests the delete_sets option
func TestParse_Sets_Delete(t *testing.T) {
	s := NewTestStatsd()
	s.DeleteSets = true
	fakeacc := &Accumulator{}

	line := "unique.user.ids:100|s"
	require.NoError(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error", line)

	require.NoError(t, testValidateSet("unique_user_ids", 1, s.sets))

	require.NoError(t, s.Gather(fakeacc))

	require.Error(t, testValidateSet("unique_user_ids", 1, s.sets), "unique_user_ids_set metric should have been deleted")
}

// Tests the delete_counters option
func TestParse_Counters_Delete(t *testing.T) {
	s := NewTestStatsd()
	s.DeleteCounters = true
	fakeacc := &Accumulator{}

	line := "total.users:100|c"
	require.NoError(t, s.parseStatsdLine(line), "Parsing line %s should not have resulted in an error\n", line)

	require.NoError(t, testValidateCounter("total_users", 100, s.counters))

	require.NoError(t, s.Gather(fakeacc))

	require.Error(t, testValidateCounter("total_users", 100, s.counters), "total_users_counter metric should have been deleted")
}

func TestParseKeyValue(t *testing.T) {
	k, v := parseKeyValue("foo=bar")
	require.Equalf(t, k, "foo", "Expected %s, got %s", "foo", k)
	require.Equalf(t, v, "bar", "Expected %s, got %s", "bar", v)

	k2, v2 := parseKeyValue("baz")
	require.Equalf(t, k2, "", "Expected %s, got %s", "", k2)
	require.Equalf(t, v2, "baz", "Expected %s, got %s", "baz", v2)
}

// Test utility functions
func testValidateSet(
	name string,
	value int64,
	cache map[string]cachedset,
	field ...string,
) error {
	var f string
	if len(field) > 0 {
		f = field[0]
	} else {
		f = "value"
	}
	var metric cachedset
	var found bool
	for _, v := range cache {
		if v.name == name {
			metric = v
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("test Error: Metric name %s not found", name)
	}

	if value != int64(len(metric.fields[f])) {
		return fmt.Errorf("measurement: %s, expected %d, actual %d", name, value, len(metric.fields[f]))
	}
	return nil
}

func testValidateCounter(
	name string,
	valueExpected int64,
	cache map[string]cachedcounter,
	field ...string,
) error {
	var f string
	if len(field) > 0 {
		f = field[0]
	} else {
		f = "value"
	}
	var valueActual int64
	var found bool
	for _, v := range cache {
		if v.name == name {
			valueActual = v.fields[f].(int64)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("test Error: Metric name %s not found", name)
	}

	if valueExpected != valueActual {
		return fmt.Errorf("measurement: %s, expected %d, actual %d", name, valueExpected, valueActual)
	}
	return nil
}

func testValidateGauge(
	name string,
	valueExpected float64,
	cache map[string]cachedgauge,
	field ...string,
) error {
	var f string
	if len(field) > 0 {
		f = field[0]
	} else {
		f = "value"
	}
	var valueActual float64
	var found bool
	for _, v := range cache {
		if v.name == name {
			valueActual = v.fields[f].(float64)
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("test Error: Metric name %s not found", name)
	}

	if valueExpected != valueActual {
		return fmt.Errorf("measurement: %s, expected %f, actual %f", name, valueExpected, valueActual)
	}
	return nil
}

func TestTCP(t *testing.T) {
	statsd := Statsd{
		User:                   "root",
		Password:               "taosdata",
		Log:                    logger.WithField("test", "TestTCP"),
		Protocol:               "tcp",
		ServiceAddress:         "localhost:0",
		AllowedPendingMessages: 10000,
		MaxTCPConnections:      2,
	}
	var acc Accumulator
	require.NoError(t, statsd.Start(&acc))
	defer statsd.Stop()

	addr := statsd.TCPlistener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	_, err = conn.Write([]byte("cpu.time_idle:42|c\n"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	start := time.Now()
	for {
		require.NoError(t, statsd.Gather(&acc))

		if len(acc.Metrics) > 0 {
			break
		}
		if time.Since(start) > time.Second*5 {
			t.Fatal("timeout waiting for metrics")
		}
	}

	RequireMetricsEqual(t,
		[]telegraf.Metric{
			telegrafMetric.New(
				"cpu_time_idle",
				map[string]string{
					"metric_type": "counter",
				},
				map[string]interface{}{
					"value": 42,
				},
				time.Now(),
				telegraf.Counter,
			),
		},
		acc.GetTelegrafMetrics(),
		IgnoreTime(),
	)
}

func TestUdp(t *testing.T) {
	statsd := Statsd{
		User:                   "root",
		Password:               "taosdata",
		Log:                    logrus.New().WithField("test", "TestUdp"),
		Protocol:               "udp",
		ServiceAddress:         "localhost:14223",
		AllowedPendingMessages: 250000,
	}
	var acc Accumulator
	require.NoError(t, statsd.Start(&acc))
	defer statsd.Stop()

	conn, err := net.Dial("udp", "127.0.0.1:14223")
	require.NoError(t, err)
	_, err = conn.Write([]byte("cpu.time_idle:42|c\n"))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	start := time.Now()
	for {
		require.NoError(t, statsd.Gather(&acc))

		if len(acc.Metrics) > 0 {
			break
		}
		if time.Since(start) > time.Second*5 {
			t.Fatal("timeout waiting for metrics")
		}
	}

	RequireMetricsEqual(t,
		[]telegraf.Metric{
			telegrafMetric.New(
				"cpu_time_idle",
				map[string]string{
					"metric_type": "counter",
				},
				map[string]interface{}{
					"value": 42,
				},
				time.Now(),
				telegraf.Counter,
			),
		},
		acc.GetTelegrafMetrics(),
		IgnoreTime(),
	)
}

func TestParse_Ints(t *testing.T) {
	s := NewTestStatsd()
	s.Percentiles = []Number{90}
	acc := &Accumulator{}

	require.NoError(t, s.Gather(acc))
	require.Equal(t, s.Percentiles, []Number{90.0})
}

func TestParse_KeyValue(t *testing.T) {
	type output struct {
		key string
		val string
	}

	validLines := []struct {
		input  string
		output output
	}{
		{"", output{"", ""}},
		{"only value", output{"", "only value"}},
		{"key=value", output{"key", "value"}},
		{"url=/api/querystring?key1=val1&key2=value", output{"url", "/api/querystring?key1=val1&key2=value"}},
	}

	for _, line := range validLines {
		key, val := parseKeyValue(line.input)
		if key != line.output.key {
			t.Errorf("line: %s,  key expected %s, actual %s", line, line.output.key, key)
		}
		if val != line.output.val {
			t.Errorf("line: %s,  val expected %s, actual %s", line, line.output.val, val)
		}
	}
}

func TestParseSanitize(t *testing.T) {
	s := NewTestStatsd()
	s.SanitizeNamesMethod = "upstream"

	tests := []struct {
		inName  string
		outName string
	}{
		{
			"regex.ARP flood stats",
			"regex_ARP_flood_stats",
		},
		{
			"regex./dev/null",
			"regex_-dev-null",
		},
		{
			"regex.wow!!!",
			"regex_wow",
		},
		{
			"regex.all*things",
			"regex_allthings",
		},
	}

	for _, test := range tests {
		name, _, _ := s.parseName(test.inName)
		require.Equalf(t, name, test.outName, "Expected: %s, got %s", test.outName, name)
	}
}

func TestParseNoSanitize(t *testing.T) {
	s := NewTestStatsd()
	s.SanitizeNamesMethod = ""

	tests := []struct {
		inName  string
		outName string
	}{
		{
			"regex.ARP flood stats",
			"regex_ARP",
		},
		{
			"regex./dev/null",
			"regex_/dev/null",
		},
		{
			"regex.wow!!!",
			"regex_wow!!!",
		},
		{
			"regex.all*things",
			"regex_all*things",
		},
	}

	for _, test := range tests {
		name, _, _ := s.parseName(test.inName)
		require.Equalf(t, name, test.outName, "Expected: %s, got %s", test.outName, name)
	}
}
