package statsd

// copied from github.com/influxdata/telegraf@v1.23.4/accumulator_test.go
import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	telegrafLogger "github.com/influxdata/telegraf/logger"
	"github.com/stretchr/testify/require"
)

func TestAddFields(t *testing.T) {
	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	tags := map[string]string{"foo": "bar"}
	fields := map[string]interface{}{
		"usage": float64(99),
	}
	now := time.Now()
	a.AddCounter("acctest", fields, tags, now)

	testm := <-metrics

	require.Equal(t, "acctest", testm.Name())
	actual, ok := testm.GetField("usage")

	require.True(t, ok)
	require.Equal(t, float64(99), actual)

	actual, ok = testm.GetTag("foo")
	require.True(t, ok)
	require.Equal(t, "bar", actual)

	tm := testm.Time()
	// okay if monotonic clock differs
	require.True(t, now.Equal(tm))

	tp := testm.Type()
	require.Equal(t, telegraf.Counter, tp)
}

func TestAccAddError(t *testing.T) {
	errBuf := bytes.NewBuffer(nil)
	telegrafLogger.RedirectLogging(errBuf)
	defer telegrafLogger.RedirectLogging(os.Stderr)

	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	a.AddError(errors.New("foo"))
	a.AddError(errors.New("bar"))
	a.AddError(errors.New("baz"))

	errs := bytes.Split(errBuf.Bytes(), []byte{'\n'})
	require.Len(t, errs, 4) // 4 because of trailing newline
	require.Contains(t, string(errs[0]), "TestPlugin")
	require.Contains(t, string(errs[0]), "foo")
	require.Contains(t, string(errs[1]), "TestPlugin")
	require.Contains(t, string(errs[1]), "bar")
	require.Contains(t, string(errs[2]), "TestPlugin")
	require.Contains(t, string(errs[2]), "baz")
}

func TestSetPrecision(t *testing.T) {
	tests := []struct {
		name      string
		unset     bool
		precision time.Duration
		timestamp time.Time
		expected  time.Time
	}{
		{
			name:      "default precision is nanosecond",
			unset:     true,
			timestamp: time.Date(2006, time.February, 10, 12, 0, 0, 82912748, time.UTC),
			expected:  time.Date(2006, time.February, 10, 12, 0, 0, 82912748, time.UTC),
		},
		{
			name:      "second interval",
			precision: time.Second,
			timestamp: time.Date(2006, time.February, 10, 12, 0, 0, 82912748, time.UTC),
			expected:  time.Date(2006, time.February, 10, 12, 0, 0, 0, time.UTC),
		},
		{
			name:      "microsecond interval",
			precision: time.Microsecond,
			timestamp: time.Date(2006, time.February, 10, 12, 0, 0, 82912748, time.UTC),
			expected:  time.Date(2006, time.February, 10, 12, 0, 0, 82913000, time.UTC),
		},
		{
			name:      "2 second precision",
			precision: 2 * time.Second,
			timestamp: time.Date(2006, time.February, 10, 12, 0, 2, 4, time.UTC),
			expected:  time.Date(2006, time.February, 10, 12, 0, 2, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := make(chan telegraf.Metric, 10)

			a := NewAccumulator(&TestMetricMaker{}, metrics)
			if !tt.unset {
				a.SetPrecision(tt.precision)
			}

			a.AddFields("acctest",
				map[string]interface{}{"value": float64(101)},
				map[string]string{},
				tt.timestamp,
			)

			testm := <-metrics
			require.Equal(t, tt.expected, testm.Time())

			close(metrics)
		})
	}
}

func TestAddTrackingMetricGroupEmpty(t *testing.T) {
	ch := make(chan telegraf.Metric, 10)
	metrics := []telegraf.Metric{}
	acc := NewAccumulator(&TestMetricMaker{}, ch).WithTracking(1)

	id := acc.AddTrackingMetricGroup(metrics)

	select {
	case tracking := <-acc.Delivered():
		require.Equal(t, tracking.ID(), id)
	default:
		t.Fatal("empty group should be delivered immediately")
	}
}

type TestMetricMaker struct {
}

func (tm *TestMetricMaker) Name() string {
	return "TestPlugin"
}

func (tm *TestMetricMaker) LogName() string {
	return tm.Name()
}

func (tm *TestMetricMaker) MakeMetric(metric telegraf.Metric) telegraf.Metric {
	return metric
}

func (tm *TestMetricMaker) Log() telegraf.Logger {
	return telegrafLogger.New("TestPlugin", "test", "")
}

func TestAddGauge(t *testing.T) {
	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	tags := map[string]string{"foo": "bar"}
	fields := map[string]interface{}{
		"usage": float64(99),
	}
	now := time.Now()
	a.AddGauge("acctest", fields, tags, now)

	testm := <-metrics

	require.Equal(t, "acctest", testm.Name())
	actual, ok := testm.GetField("usage")

	require.True(t, ok)
	require.Equal(t, float64(99), actual)

	actual, ok = testm.GetTag("foo")
	require.True(t, ok)
	require.Equal(t, "bar", actual)

	tm := testm.Time()
	// okay if monotonic clock differs
	require.True(t, now.Equal(tm))

	tp := testm.Type()
	require.Equal(t, telegraf.Gauge, tp)
}

func TestAddSummary(t *testing.T) {
	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	tags := map[string]string{"foo": "bar"}
	fields := map[string]interface{}{
		"usage": float64(99),
	}
	now := time.Now()
	a.AddSummary("acctest", fields, tags, now)

	testm := <-metrics

	require.Equal(t, "acctest", testm.Name())
	actual, ok := testm.GetField("usage")

	require.True(t, ok)
	require.Equal(t, float64(99), actual)

	actual, ok = testm.GetTag("foo")
	require.True(t, ok)
	require.Equal(t, "bar", actual)

	tm := testm.Time()
	// okay if monotonic clock differs
	require.True(t, now.Equal(tm))

	tp := testm.Type()
	require.Equal(t, telegraf.Summary, tp)
}

func TestAddHistogram(t *testing.T) {
	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	tags := map[string]string{"foo": "bar"}
	fields := map[string]interface{}{
		"usage": float64(99),
	}
	now := time.Now()
	a.AddHistogram("acctest", fields, tags, now)

	testm := <-metrics

	require.Equal(t, "acctest", testm.Name())
	actual, ok := testm.GetField("usage")

	require.True(t, ok)
	require.Equal(t, float64(99), actual)

	actual, ok = testm.GetTag("foo")
	require.True(t, ok)
	require.Equal(t, "bar", actual)

	tm := testm.Time()
	// okay if monotonic clock differs
	require.True(t, now.Equal(tm))

	tp := testm.Type()
	require.Equal(t, telegraf.Histogram, tp)
}

func TestAddTrackingMetric(t *testing.T) {
	ch := make(chan telegraf.Metric, 10)
	acc := NewAccumulator(&TestMetricMaker{}, ch).WithTracking(1)
	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	tags := map[string]string{"foo": "bar"}
	fields := map[string]interface{}{
		"usage": float64(99),
	}
	now := time.Now()
	a.AddHistogram("acctest", fields, tags, now)

	testm := <-metrics
	require.Equal(t, "acctest", testm.Name())
	actual, ok := testm.GetField("usage")
	require.True(t, ok)
	require.Equal(t, float64(99), actual)
	id := acc.AddTrackingMetricGroup([]telegraf.Metric{testm})
	m := <-ch
	m.Accept()
	select {
	case tracking := <-acc.Delivered():
		require.Equal(t, tracking.ID(), id)
	case <-time.Tick(time.Second * 10):
		t.Fatal("timeout")
	}

	id2 := acc.AddTrackingMetric(testm)
	m = <-ch
	m.Accept()
	select {
	case tracking := <-acc.Delivered():
		require.Equal(t, tracking.ID(), id2)
	case <-time.Tick(time.Second * 10):
		t.Fatal("timeout")
	}
}
