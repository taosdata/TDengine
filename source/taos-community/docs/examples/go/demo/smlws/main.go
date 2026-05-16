package main

import (
	"database/sql"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	_ "github.com/taosdata/driver-go/v3/taosWS"
	"github.com/taosdata/driver-go/v3/ws/schemaless"
)

const LineDemo = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639000000"

const TelnetDemo = "stb0_0 1707095283260 4 host=host0 interface=eth0"

const JsonDemo = "{\"metric\": \"meter_current\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}"

func main() {
	db, err := sql.Open("taosWS", "root:taosdata@ws(localhost:6041)/")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists power")
	if err != nil {
		log.Fatal(err)
	}
	s, err := schemaless.NewSchemaless(schemaless.NewConfig("ws://localhost:6041", 1,
		schemaless.SetDb("power"),
		schemaless.SetReadTimeout(10*time.Second),
		schemaless.SetWriteTimeout(10*time.Second),
		schemaless.SetUser("root"),
		schemaless.SetPassword("taosdata"),
		schemaless.SetErrorHandler(func(err error) {
			log.Fatal(err)
		}),
	))
	if err != nil {
		panic(err)
	}
	err = s.Insert(LineDemo, schemaless.InfluxDBLineProtocol, "ns", 0, common.GetReqID())
	if err != nil {
		panic(err)
	}
	err = s.Insert(TelnetDemo, schemaless.OpenTSDBTelnetLineProtocol, "ms", 0, common.GetReqID())
	if err != nil {
		panic(err)
	}
	err = s.Insert(JsonDemo, schemaless.OpenTSDBJsonFormatProtocol, "ms", 0, common.GetReqID())
	if err != nil {
		panic(err)
	}
}
