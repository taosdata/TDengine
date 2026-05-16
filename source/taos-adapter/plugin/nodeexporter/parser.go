package nodeexporter

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"time"

	"github.com/matttproud/golang_protobuf_extensions/v2/pbutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

func Parse(buf []byte, header http.Header, ignoreTimestamp bool) ([]telegraf.Metric, error) {
	var parser expfmt.TextParser
	var metrics []telegraf.Metric
	var err error
	// parse even if the buffer begins with a newline
	buf = bytes.TrimPrefix(buf, []byte("\n"))
	// Read raw data
	buffer := bytes.NewBuffer(buf)
	reader := bufio.NewReader(buffer)

	// Prepare output
	metricFamilies := make(map[string]*dto.MetricFamily)

	if isProtobuf(header) {
		for {
			mf := &dto.MetricFamily{}
			if _, ierr := pbutil.ReadDelimited(reader, mf); ierr != nil {
				if ierr == io.EOF {
					break
				}
				return nil, fmt.Errorf("reading metric family protocol buffer failed: %s", ierr)
			}
			metricFamilies[mf.GetName()] = mf
		}
	} else {
		metricFamilies, err = parser.TextToMetricFamilies(reader)
		if err != nil {
			return nil, fmt.Errorf("reading text format failed: %s", err)
		}
	}

	now := time.Now()
	// read metrics
	for metricName, mf := range metricFamilies {
		for _, m := range mf.Metric {
			// reading tags
			tags := MakeLabels(m)

			// reading fields
			var fields map[string]interface{}
			if mf.GetType() == dto.MetricType_SUMMARY {
				// summary metric
				fields = makeQuantiles(m)
				fields["count"] = float64(m.GetSummary().GetSampleCount())
				//nolint:unconvert // Conversion may be needed for float64 https://github.com/mdempsky/unconvert/issues/40
				fields["sum"] = float64(m.GetSummary().GetSampleSum())
			} else if mf.GetType() == dto.MetricType_HISTOGRAM {
				// histogram metric
				fields = makeBuckets(m)
				fields["count"] = float64(m.GetHistogram().GetSampleCount())
				//nolint:unconvert // Conversion may be needed for float64 https://github.com/mdempsky/unconvert/issues/40
				fields["sum"] = float64(m.GetHistogram().GetSampleSum())
			} else {
				// standard metric
				fields = getNameAndValue(m)
			}
			// converting to telegraf metric
			if len(fields) > 0 {
				var t time.Time
				if !ignoreTimestamp && m.TimestampMs != nil && *m.TimestampMs > 0 {
					msec := *m.TimestampMs
					t = time.Unix(msec/1e3, (msec%1e3)*1e6)
				} else {
					t = now
				}
				m := metric.New(metricName, tags, fields, t, ValueType(mf.GetType()))
				metrics = append(metrics, m)
			}
		}
	}

	return metrics, err
}

func isProtobuf(header http.Header) bool {
	mediatype, params, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil {
		return false
	}

	return mediatype == "application/vnd.google.protobuf" &&
		params["encoding"] == "delimited" &&
		params["proto"] == "io.prometheus.client.MetricFamily"
}

// Get Quantiles from summary metric
func makeQuantiles(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	for _, q := range m.GetSummary().Quantile {
		if !math.IsNaN(q.GetValue()) {
			//nolint:unconvert // Conversion may be needed for float64 https://github.com/mdempsky/unconvert/issues/40
			fields[fmt.Sprint(q.GetQuantile())] = float64(q.GetValue())
		}
	}
	return fields
}

// Get Buckets  from histogram metric
func makeBuckets(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	for _, b := range m.GetHistogram().Bucket {
		fields[fmt.Sprint(b.GetUpperBound())] = float64(b.GetCumulativeCount())
	}
	return fields
}

// Get name and value from metric
func getNameAndValue(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	if m.Gauge != nil {
		if !math.IsNaN(m.GetGauge().GetValue()) {
			//nolint:unconvert // Conversion may be needed for float64 https://github.com/mdempsky/unconvert/issues/40
			fields["gauge"] = float64(m.GetGauge().GetValue())
		}
	} else if m.Counter != nil {
		if !math.IsNaN(m.GetCounter().GetValue()) {
			//nolint:unconvert // Conversion may be needed for float64 https://github.com/mdempsky/unconvert/issues/40
			fields["counter"] = float64(m.GetCounter().GetValue())
		}
	} else if m.Untyped != nil {
		if !math.IsNaN(m.GetUntyped().GetValue()) {
			//nolint:unconvert // Conversion may be needed for float64 https://github.com/mdempsky/unconvert/issues/40
			fields["value"] = float64(m.GetUntyped().GetValue())
		}
	}
	return fields
}

func ValueType(mt dto.MetricType) telegraf.ValueType {
	switch mt {
	case dto.MetricType_COUNTER:
		return telegraf.Counter
	case dto.MetricType_GAUGE:
		return telegraf.Gauge
	case dto.MetricType_SUMMARY:
		return telegraf.Summary
	case dto.MetricType_HISTOGRAM:
		return telegraf.Histogram
	default:
		return telegraf.Untyped
	}
}

// Get labels from metric
func MakeLabels(m *dto.Metric) map[string]string {
	result := map[string]string{}

	for _, lp := range m.Label {
		result[lp.GetName()] = lp.GetValue()
	}

	return result
}
