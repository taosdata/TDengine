package inputjson

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/blues/jsonata-go"
)

func BenchmarkTransform(b *testing.B) {
	transformation := `
    $sort(
    (
        $ts := time;
        $each($, function($value, $key) {
            $key = "time" ? [] : (
                $each($value, function($groupValue, $groupKey) {
                    $each($groupValue, function($deviceValue, $deviceKey) {
                        {
                            "db": "test_input_json",
                            "time": $ts,
                            "location": $key,
                            "groupid": $number($split($groupKey, "_")[1]),
                            "stb": "meters",
                            "table": $deviceKey,
                            "current": $deviceValue.current,
                            "voltage": $deviceValue.voltage,
                            "phase": $deviceValue.phase
                        }
                    })[]
                })[]
            )
        })
    ).[*][*],
    function($l, $r) {
        $l.table > $r.table
    }
)`
	inputData := `{
    "time": "2025-11-04 09:24:13.123456",
    "Los Angeles": {
        "group_1": {
            "d_001": {
                "current": 10.5,
                "voltage": 220,
                "phase": 30
            },
            "d_002": {
                "current": 15.2,
                "voltage": 230,
                "phase": 45
            },
            "d_003": {
                "current": 8.7,
                "voltage": 210,
                "phase": 60
            }
        },
        "group_2": {
            "d_004": {
                "current": 12.3,
                "voltage": 225,
                "phase": 15
            },
            "d_005": {
                "current": 9.8,
                "voltage": 215,
                "phase": 75
            }
        }
    },
    "New York": {
        "group_1": {
            "d_006": {
                "current": 11.0,
                "voltage": 240,
                "phase": 20
            },
            "d_007": {
                "current": 14.5,
                "voltage": 235,
                "phase": 50
            }
        },
        "group_2": {
            "d_008": {
                "current": 13.2,
                "voltage": 245,
                "phase": 10
            },
            "d_009": {
                "current": 7.9,
                "voltage": 220,
                "phase": 80
            }
        }
    }
}`
	e, err := jsonata.Compile(transformation)
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := e.EvalBytes([]byte(inputData))
		if err != nil {
			b.Error(err)
			return
		}
		_ = result
	}
}

func BenchmarkRequest(b *testing.B) {
	// create db and table beforehand, SQL commands:
	/*
		create database test_input_json;
		create table test_input_json.meters (ts timestamp, current float, voltage int, phase float) tags (location nchar(64), `groupid` int);
	*/

	// start taosadapter server with input_json plugin enabled
	/*
		[input_json]
		enable = true
		[[input_json.rules]]
		endpoint = "rule1"
		dbKey = "db"
		superTableKey = "stb"
		subTableKey = "table"
		timeKey = "time"
		timeFormat = "datetime"
		timezone = "UTC"
		transformation = '''
		$sort(
		    (
		        $ts := time;
		        $each($, function($value, $key) {
		            $key = "time" ? [] : (
		                $each($value, function($groupValue, $groupKey) {
		                    $each($groupValue, function($deviceValue, $deviceKey) {
		                        {
		                            "db": "test_input_json",
		                            "time": $ts,
		                            "location": $key,
		                            "groupid": $number($split($groupKey, "_")[1]),
		                            "stb": "meters",
		                            "table": $deviceKey,
		                            "current": $deviceValue.current,
		                            "voltage": $deviceValue.voltage,
		                            "phase": $deviceValue.phase
		                        }
		                    })[]
		                })[]
		            )
		        })
		    ).[*][*],
		    function($l, $r) {
		        $l.table > $r.table
		    }
		)
		'''
		fields = [
		    {key = "current", optional = false},
		    {key = "voltage", optional = false},
		    {key = "phase", optional = false},
		    {key = "location", optional = false},
		    {key = "groupid", optional = false},
	*/

	for i := 0; i < b.N; i++ {
		err := sendRequest()
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func sendRequest() error {
	url := "http://localhost:6041/input_json/v1/rule1"
	method := "POST"

	payload := strings.NewReader(`{"time":"2025-11-04 09:24:13.123456","Los Angeles":{"group_1":{"d_001":{"current":10.5,"voltage":220,"phase":30},"d_002":{"current":15.2,"voltage":230,"phase":45},"d_003":{"current":8.7,"voltage":210,"phase":60}},"group_2":{"d_004":{"current":12.3,"voltage":225,"phase":15},"d_005":{"current":9.8,"voltage":215,"phase":75}}},"New York":{"group_1":{"d_006":{"current":11.0,"voltage":240,"phase":20},"d_007":{"current":14.5,"voltage":235,"phase":50}},"group_2":{"d_008":{"current":13.2,"voltage":245,"phase":10},"d_009":{"current":7.9,"voltage":220,"phase":80}}}}`)

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		return err
	}
	req.Header.Add("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = res.Body.Close()
	}()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	_ = body
	//fmt.Println(string(body))
	return nil
}
