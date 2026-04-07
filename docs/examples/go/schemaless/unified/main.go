package main

import (
	"fmt"
	"log"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

func main() {
	// ANCHOR: main
	host := "127.0.0.1"
	lineDemo := "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
	telnetDemo := "metric_telnet 1707095283260 4 host=host0 interface=eth0"
	jsonDemo := "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}"

	taosDSN := fmt.Sprintf("root:taosdata@ws(%s:6041)/", host)
	client, err := unified.Open(taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	defer client.Close()

	_, err = client.Exec(common.GetReqID(), "CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
	}
	_, err = client.Exec(common.GetReqID(), "USE power")
	if err != nil {
		log.Fatalln("Failed to use database power, ErrMessage: " + err.Error())
	}

	// insert influxdb line protocol
	err = client.SchemalessInsert(common.GetReqID(), lineDemo, unified.InfluxDBLineProtocol, "ms", 0, "")
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data:" + lineDemo + ", ErrMessage: " + err.Error())
	}
	// insert opentsdb telnet line protocol
	err = client.SchemalessInsert(common.GetReqID(), telnetDemo, unified.OpenTSDBTelnetLineProtocol, "ms", 0, "")
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data:" + telnetDemo + ", ErrMessage: " + err.Error())
	}
	// insert opentsdb json format protocol
	err = client.SchemalessInsert(common.GetReqID(), jsonDemo, unified.OpenTSDBJsonFormatProtocol, "s", 0, "")
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data:" + jsonDemo + ", ErrMessage: " + err.Error())
	}
	fmt.Println("Inserted data with schemaless successfully.")
	// ANCHOR_END: main
}
