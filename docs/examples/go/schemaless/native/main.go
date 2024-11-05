package main

import (
	"fmt"
	"log"

	"github.com/taosdata/driver-go/v3/af"
)

func main() {
	host := "127.0.0.1"
	lineDemo := "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
	telnetDemo := "metric_telnet 1707095283260 4 host=host0 interface=eth0"
	jsonDemo := "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}"

	conn, err := af.Open(host, "root", "taosdata", "", 0)
	if err != nil {
		log.Fatalln("Failed to connect to host: " + host + "; ErrMessage: " + err.Error())
	}
	defer conn.Close()
	_, err = conn.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
	}
	_, err = conn.Exec("USE power")
	if err != nil {
		log.Fatalln("Failed to use database power, ErrMessage: " + err.Error())
	}
	// insert influxdb line protocol
	err = conn.InfluxDBInsertLines([]string{lineDemo}, "ms")
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data:" + lineDemo + ", ErrMessage: " + err.Error())
	}
	// insert opentsdb telnet protocol
	err = conn.OpenTSDBInsertTelnetLines([]string{telnetDemo})
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data:" + telnetDemo + ", ErrMessage: " + err.Error())
	}
	// insert opentsdb json protocol
	err = conn.OpenTSDBInsertJsonPayload(jsonDemo)
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data:" + jsonDemo + ", ErrMessage: " + err.Error())
	}
	fmt.Println("Inserted data with schemaless successfully.")
}
