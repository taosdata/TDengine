package main

import (
	"fmt"

	"github.com/taosdata/driver-go/v3/af"
)

const LineDemo = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639000000"

const TelnetDemo = "stb0_0 1707095283260 4 host=host0 interface=eth0"

const JsonDemo = "{\"metric\": \"meter_current\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}"

func main() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	if err != nil {
		fmt.Println("fail to connect, err:", err)
	}
	defer conn.Close()
	_, err = conn.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("use power")
	if err != nil {
		panic(err)
	}
	err = conn.InfluxDBInsertLines([]string{LineDemo}, "ns")
	if err != nil {
		panic(err)
	}
	err = conn.OpenTSDBInsertTelnetLines([]string{TelnetDemo})
	if err != nil {
		panic(err)
	}
	err = conn.OpenTSDBInsertJsonPayload(JsonDemo)
	if err != nil {
		panic(err)
	}
}
