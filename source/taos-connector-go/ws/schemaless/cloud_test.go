package schemaless

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCloudSchemaless(t *testing.T) {
	db := "go_test"
	endPoint := os.Getenv("TDENGINE_CLOUD_ENDPOINT")
	token := os.Getenv("TDENGINE_CLOUD_TOKEN")
	if endPoint == "" || token == "" {
		t.Skip("TDENGINE_CLOUD_TOKEN or TDENGINE_CLOUD_ENDPOINT is not set, skip cloud test")
		return
	}
	cases := []struct {
		name      string
		protocol  int
		precision string
		data      string
		ttl       int
		code      int
	}{
		{
			name:      "influxdb",
			protocol:  InfluxDBLineProtocol,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl: 1000,
		},
		{
			name:      "opentsdb_telnet",
			protocol:  OpenTSDBTelnetLineProtocol,
			precision: "ms",
			data: "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611251 10.8 location=California.LosAngeles group=3\n" +
				"meters.current 1648432611252 11.3 location=California.LosAngeles group=3\n",
			ttl: 1000,
		},
		{
			name:      "opentsdb_json",
			protocol:  OpenTSDBJsonFormatProtocol,
			precision: "ms",
			data: "[{\"metric\": \"meters.voltage\", \"timestamp\": 1648432611249, \"value\": 219, \"tags\": " +
				"{\"location\": \"California.LosAngeles\", \"groupid\": 1 } }, {\"metric\": \"meters.voltage\", " +
				"\"timestamp\": 1648432611250, \"value\": 221, \"tags\": {\"location\": \"California.LosAngeles\", " +
				"\"groupid\": 1 } }]",
			ttl: 100,
		},
	}
	url := fmt.Sprintf("wss://%s?token=%s", endPoint, token)
	s, err := NewSchemaless(NewConfig(url, 1,
		SetDb(db),
		SetReadTimeout(10*time.Second),
		SetWriteTimeout(10*time.Second),
		SetUser("root"),
		SetPassword("taosdata"),
		SetEnableCompression(true),
	))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := s.Insert(c.data, c.protocol, c.precision, c.ttl, 0); err != nil {
				t.Fatal(err)
			}
		})
	}
}
