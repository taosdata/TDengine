package ws

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestWsSchemaless(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_schemaless", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_schemaless", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_schemaless"))
	defer doRestful("drop database if exists test_ws_schemaless", "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	cases := []struct {
		name         string
		protocol     int
		precision    string
		data         string
		ttl          int
		totalRows    int32
		affectedRows int
		tableNameKey string
	}{
		{
			name:      "influxdb",
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl:          1000,
			totalRows:    4,
			affectedRows: 4,
		},
		{
			name:      "opentsdb_telnet",
			protocol:  wrapper.OpenTSDBTelnetLineProtocol,
			precision: "ms",
			data: "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611249 10.8 location=California.LosAngeles group=3\n" +
				"meters.current 1648432611250 11.3 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611249 219 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611250 218 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611249 221 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
			ttl:          1000,
			totalRows:    8,
			affectedRows: 8,
		},
		{
			name:      "opentsdb_json",
			protocol:  wrapper.OpenTSDBJsonFormatProtocol,
			precision: "ms",
			data: `[
    {
        "metric": "meters2.current",
        "timestamp": 1648432611249,
        "value": 10.3,
        "tags": {
            "location": "California.SanFrancisco",
            "groupid": 2
        }
    },
    {
        "metric": "meters2.voltage",
        "timestamp": 1648432611249,
        "value": 219,
        "tags": {
            "location": "California.LosAngeles",
            "groupid": 1
        }
    },
    {
        "metric": "meters2.current",
        "timestamp": 1648432611250,
        "value": 12.6,
        "tags": {
            "location": "California.SanFrancisco",
            "groupid": 2
        }
    },
    {
        "metric": "meters2.voltage",
        "timestamp": 1648432611250,
        "value": 221,
        "tags": {
            "location": "California.LosAngeles",
            "groupid": 1
        }
    }
]`,
			ttl:          100,
			affectedRows: 4,
		},
		{
			name:      "influxdb_tbnamekey",
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl:          1000,
			totalRows:    4,
			affectedRows: 4,
			tableNameKey: "host",
		},
	}

	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_schemaless"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	for _, c := range cases {
		reqID := uint64(1)
		t.Run(c.name, func(t *testing.T) {
			reqID += 1
			req := schemalessWriteRequest{
				ReqID:        reqID,
				Protocol:     c.protocol,
				Precision:    c.precision,
				TTL:          c.ttl,
				Data:         c.data,
				TableNameKey: c.tableNameKey,
			}
			resp, err = doWebSocket(ws, SchemalessWrite, &req)
			assert.NoError(t, err)
			var schemalessResp schemalessWriteResponse
			err = json.Unmarshal(resp, &schemalessResp)
			assert.NoError(t, err, string(resp))
			assert.Equal(t, reqID, schemalessResp.ReqID)
			assert.Equal(t, 0, schemalessResp.Code, schemalessResp.Message)
			if c.protocol != wrapper.OpenTSDBJsonFormatProtocol {
				assert.Equal(t, c.totalRows, schemalessResp.TotalRows)
			}
			assert.Equal(t, c.affectedRows, schemalessResp.AffectedRows)
		})
	}
}

func TestWsSchemalessError(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_schemaless_error", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_schemaless_error", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_schemaless_error"))

	defer doRestful("drop database if exists test_ws_schemaless_error", "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	cases := []struct {
		name      string
		protocol  int
		precision string
		data      string
	}{
		{
			name:      "wrong protocol",
			protocol:  0,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
		},
		{
			name:      "wrong timestamp",
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "ms",
			data:      "measurement,host=host1 field1=2i,field2=2.0 10",
		},
	}

	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_schemaless_error"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	for _, c := range cases {
		reqID := uint64(1)
		t.Run(c.name, func(t *testing.T) {
			reqID += 1
			req := schemalessWriteRequest{
				ReqID:     reqID,
				Protocol:  c.protocol,
				Precision: c.precision,
				Data:      c.data,
			}
			resp, err = doWebSocket(ws, SchemalessWrite, &req)
			assert.NoError(t, err)
			var schemalessResp schemalessWriteResponse
			err = json.Unmarshal(resp, &schemalessResp)
			assert.NoError(t, err, string(resp))
			assert.Equal(t, reqID, schemalessResp.ReqID)
			assert.NotEqual(t, 0, schemalessResp.Code)
		})
	}
}
