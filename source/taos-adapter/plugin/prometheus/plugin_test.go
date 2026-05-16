package prometheus

import (
	"bytes"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/plugin/prometheus/prompb"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestMain(m *testing.M) {
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())
	config.Init()
	viper.Set("prometheus.enable", true)
	log.ConfigLog()
	db.PrepareConnection()
	logger := log.GetLogger("test")
	isDebug := log.IsDebug()
	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, isDebug)
	if err != nil {
		panic(err)
	}
	defer func() {
		syncinterface.TaosClose(conn, logger, isDebug)
	}()
	r := syncinterface.TaosQuery(conn, "create database if not exists test_plugin_prometheus", logger, isDebug)
	syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	err = testtools.EnsureDBCreated("test_plugin_prometheus")
	if err != nil {
		panic(err)
	}
	code := m.Run()
	r = syncinterface.TaosQuery(conn, "drop database if exists test_plugin_prometheus", logger, isDebug)
	syncinterface.TaosSyncQueryFree(r, logger, isDebug)
	os.Exit(code)
}

// @author: xftan
// @date: 2021/12/20 14:47
// @description: test prometheus remote_write and remote_read
func TestPrometheus(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	p := Plugin{}
	router := gin.Default()
	err := p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Float64()
	defer func() {
		_ = p.Stop()
	}()
	w := httptest.NewRecorder()
	now := time.Now().UnixNano() / 1e6
	var wReq = prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name:  "testK",
						Value: "testV",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     number,
						Timestamp: now,
					},
				},
			},
		},
	}
	data, err := proto.Marshal(&wReq)
	assert.NoError(t, err)
	compressed := snappy.Encode(nil, data)
	req, _ := http.NewRequest("POST", "/remote_write/test_plugin_prometheus", bytes.NewBuffer(compressed))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)

	var rReq = prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				StartTimestampMs: now,
				EndTimestampMs:   now,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "testK",
						Value: "testV",
					},
				},
			},
		},
	}

	rdata, err := proto.Marshal(&rReq)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	compressedR := snappy.Encode(nil, rdata)
	req, _ = http.NewRequest("POST", "/remote_read/test_plugin_prometheus", bytes.NewBuffer(compressedR))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)
	buf, err := snappy.Decode(nil, w.Body.Bytes())
	assert.NoError(t, err)
	var rr prompb.ReadResponse
	err = proto.Unmarshal(buf, &rr)
	assert.NoError(t, err)
	result := rr.GetResults()
	assert.Equal(t, 1, len(result))
	series := result[0].GetTimeseries()
	assert.Equal(t, 1, len(series))
	labels := series[0].GetLabels()
	assert.Equal(t, 1, len(labels))
	assert.Equal(t, "testK", labels[0].GetName())
	assert.Equal(t, "testV", labels[0].GetValue())
	samples := series[0].GetSamples()
	assert.Equal(t, 1, len(samples))
	assert.Equal(t, number, samples[0].GetValue())
	assert.Equal(t, now, samples[0].GetTimestamp())
}

// @author: xftan
// @date: 2022/1/18 18:05
// @description: test prometheus remote_write and remote_read with char `'`
func TestPrometheusEscapeString(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	p := Plugin{}
	router := gin.Default()
	err := p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Float64()
	defer func() {
		_ = p.Stop()
	}()
	w := httptest.NewRecorder()
	now := time.Now().UnixNano() / 1e6
	var wReq = prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name:  "test'K",
						Value: "test'V",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     number,
						Timestamp: now,
					},
				},
			},
		},
	}
	data, err := proto.Marshal(&wReq)
	assert.NoError(t, err)
	compressed := snappy.Encode(nil, data)
	req, _ := http.NewRequest("POST", "/remote_write/test_plugin_prometheus", bytes.NewBuffer(compressed))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)

	var rReq = prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				StartTimestampMs: now,
				EndTimestampMs:   now,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "test'K",
						Value: "test'V",
					},
				},
			},
		},
	}

	rdata, err := proto.Marshal(&rReq)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	compressedR := snappy.Encode(nil, rdata)
	req, _ = http.NewRequest("POST", "/remote_read/test_plugin_prometheus", bytes.NewBuffer(compressedR))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)
	buf, err := snappy.Decode(nil, w.Body.Bytes())
	assert.NoError(t, err)
	var rr prompb.ReadResponse
	err = proto.Unmarshal(buf, &rr)
	assert.NoError(t, err)
	result := rr.GetResults()
	assert.Equal(t, 1, len(result))
	series := result[0].GetTimeseries()
	assert.Equal(t, 1, len(series))
	labels := series[0].GetLabels()
	assert.Equal(t, 1, len(labels))
	assert.Equal(t, "test'K", labels[0].GetName())
	assert.Equal(t, "test'V", labels[0].GetValue())
	samples := series[0].GetSamples()
	assert.Equal(t, 1, len(samples))
	assert.Equal(t, number, samples[0].GetValue())
	assert.Equal(t, now, samples[0].GetTimestamp())
}

func TestPrometheusWithTTL(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	p := Plugin{}
	router := gin.Default()
	err := p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Float64()
	defer func() {
		_ = p.Stop()
	}()
	w := httptest.NewRecorder()
	now := time.Now().UnixNano() / 1e6
	var wReq = prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name:  "test_K",
						Value: "test_V",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     number,
						Timestamp: now,
					},
				},
			},
		},
	}
	data, err := proto.Marshal(&wReq)
	assert.NoError(t, err)
	compressed := snappy.Encode(nil, data)
	req, _ := http.NewRequest("POST", "/remote_write/test_plugin_prometheus?ttl=14", bytes.NewBuffer(compressed))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)

	var rReq = prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				StartTimestampMs: now,
				EndTimestampMs:   now,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "test_K",
						Value: "test_V",
					},
				},
			},
		},
	}

	rdata, err := proto.Marshal(&rReq)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	compressedR := snappy.Encode(nil, rdata)
	req, _ = http.NewRequest("POST", "/remote_read/test_plugin_prometheus", bytes.NewBuffer(compressedR))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)
	buf, err := snappy.Decode(nil, w.Body.Bytes())
	assert.NoError(t, err)
	var rr prompb.ReadResponse
	err = proto.Unmarshal(buf, &rr)
	assert.NoError(t, err)
	result := rr.GetResults()
	assert.Equal(t, 1, len(result))
	series := result[0].GetTimeseries()
	assert.Equal(t, 1, len(series))
	labels := series[0].GetLabels()
	assert.Equal(t, 1, len(labels))
	assert.Equal(t, "test_K", labels[0].GetName())
	assert.Equal(t, "test_V", labels[0].GetValue())
	samples := series[0].GetSamples()
	assert.Equal(t, 1, len(samples))
	assert.Equal(t, number, samples[0].GetValue())
	assert.Equal(t, now, samples[0].GetTimestamp())
}

// @author: xftan
// @date: 2022/1/18 18:05
// @description: test prometheus remote_write and remote_read with `'` and '\'
func TestPrometheusEscape(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	p := Plugin{}
	router := gin.Default()
	err := p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Float64()
	defer func() {
		_ = p.Stop()
	}()
	w := httptest.NewRecorder()
	now := time.Now().UnixNano() / 1e6
	var wReq = prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name:  `k\'e'y\`,
						Value: `k\'v's`,
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     number,
						Timestamp: now,
					},
				},
			},
		},
	}
	data, err := proto.Marshal(&wReq)
	assert.NoError(t, err)
	compressed := snappy.Encode(nil, data)
	req, _ := http.NewRequest("POST", "/remote_write/test_plugin_prometheus", bytes.NewBuffer(compressed))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)

	var rReq = prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				StartTimestampMs: now,
				EndTimestampMs:   now,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  `k\'e'y\`,
						Value: `k\'v's`,
					},
				},
			},
		},
	}

	rdata, err := proto.Marshal(&rReq)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	compressedR := snappy.Encode(nil, rdata)
	req, _ = http.NewRequest("POST", "/remote_read/test_plugin_prometheus", bytes.NewBuffer(compressedR))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)
	buf, err := snappy.Decode(nil, w.Body.Bytes())
	assert.NoError(t, err)
	var rr prompb.ReadResponse
	err = proto.Unmarshal(buf, &rr)
	assert.NoError(t, err)
	result := rr.GetResults()
	assert.Equal(t, 1, len(result))
	series := result[0].GetTimeseries()
	assert.Equal(t, 1, len(series))
	labels := series[0].GetLabels()
	assert.Equal(t, 1, len(labels))
	assert.Equal(t, `k\'e'y\`, labels[0].GetName())
	assert.Equal(t, `k\'v's`, labels[0].GetValue())
	samples := series[0].GetSamples()
	assert.Equal(t, 1, len(samples))
	assert.Equal(t, number, samples[0].GetValue())
	assert.Equal(t, now, samples[0].GetTimestamp())
}

// @author: xftan
// @date: 2022/1/18 17:59
// @description: test prometheus remote_write and remote_read with row limit
func TestPrometheusWithLimit(t *testing.T) {
	p := Plugin{}
	router := gin.Default()
	err := p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Float64()
	defer func() {
		_ = p.Stop()
	}()
	w := httptest.NewRecorder()
	now := time.Now().UnixNano() / 1e6
	var wReq = prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name:  "testLimitK",
						Value: "testLimitV",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     number,
						Timestamp: now,
					}, {
						Value:     number + 1,
						Timestamp: now + 1,
					},
				},
			},
		},
	}
	data, err := proto.Marshal(&wReq)
	assert.NoError(t, err)
	compressed := snappy.Encode(nil, data)
	req, _ := http.NewRequest("POST", "/remote_write/test_plugin_prometheus", bytes.NewBuffer(compressed))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)

	var rReq = prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				StartTimestampMs: now,
				EndTimestampMs:   now,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "testLimitK",
						Value: "testLimitV",
					},
				},
			},
		},
	}
	config.Conf.RestfulRowLimit = 1
	rdata, err := proto.Marshal(&rReq)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	compressedR := snappy.Encode(nil, rdata)
	req, _ = http.NewRequest("POST", "/remote_read/test_plugin_prometheus", bytes.NewBuffer(compressedR))
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)
	buf, err := snappy.Decode(nil, w.Body.Bytes())
	assert.NoError(t, err)
	var rr prompb.ReadResponse
	err = proto.Unmarshal(buf, &rr)
	assert.NoError(t, err)
	result := rr.GetResults()
	assert.Equal(t, 1, len(result))
	series := result[0].GetTimeseries()
	assert.Equal(t, 1, len(series))
	labels := series[0].GetLabels()
	assert.Equal(t, 1, len(labels))
	assert.Equal(t, "testLimitK", labels[0].GetName())
	assert.Equal(t, "testLimitV", labels[0].GetValue())
	samples := series[0].GetSamples()
	assert.Equal(t, 1, len(samples))
	assert.Equal(t, number, samples[0].GetValue())
	assert.Equal(t, now, samples[0].GetTimestamp())
	config.Conf.RestfulRowLimit = -1
}
