package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	_ "github.com/taosdata/driver-go/v3/taosWS"
	"github.com/taosdata/driver-go/v3/ws/schemaless"
)

func main() {
	host := "127.0.0.1"
	lineDemo := "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
	telnetDemo := "metric_telnet 1707095283260 4 host=host0 interface=eth0"
	jsonDemo := "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}"

	taosDSN := fmt.Sprintf("root:taosdata@ws(%s:6041)/", host)
	db, err := sql.Open("taosWS", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to host: " + host + "; ErrMessage: " + err.Error())
	}
	defer db.Close()
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
	}
	s, err := schemaless.NewSchemaless(schemaless.NewConfig("ws://localhost:6041", 1,
		schemaless.SetDb("power"),
		schemaless.SetReadTimeout(10*time.Second),
		schemaless.SetWriteTimeout(10*time.Second),
		schemaless.SetUser("root"),
		schemaless.SetPassword("taosdata"),
	))
	if err != nil {
		log.Fatalln("Failed to connect to host: " + host + "; ErrMessage: " + err.Error())
	}
	// insert influxdb line protocol
	err = s.Insert(lineDemo, schemaless.InfluxDBLineProtocol, "ms", 0, common.GetReqID())
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data:" + lineDemo + ", ErrMessage: " + err.Error())
	}
	// insert opentsdb telnet line protocol
	err = s.Insert(telnetDemo, schemaless.OpenTSDBTelnetLineProtocol, "ms", 0, common.GetReqID())
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data: " + telnetDemo + ", ErrMessage: " + err.Error())
	}
	// insert opentsdb json format protocol
	err = s.Insert(jsonDemo, schemaless.OpenTSDBJsonFormatProtocol, "s", 0, common.GetReqID())
	if err != nil {
		log.Fatalln("Failed to insert data with schemaless, data: " + jsonDemo + ", ErrMessage: " + err.Error())
	}
	fmt.Println("Inserted data with schemaless successfully.")
}
