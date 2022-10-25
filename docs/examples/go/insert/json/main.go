package main

import (
	"log"

	"github.com/taosdata/driver-go/v3/af"
)

func prepareDatabase(conn *af.Connector) {
	_, err := conn.Exec("CREATE DATABASE test")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("USE test")
	if err != nil {
		panic(err)
	}
}

func main() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	if err != nil {
		log.Fatalln("fail to connect, err:", err)
	}
	defer conn.Close()
	prepareDatabase(conn)

	payload := `[{"metric": "meters.current", "timestamp": 1648432611249, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
				{"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "California.LosAngeles", "groupid": 1}},
				{"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}},
				{"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]`

	err = conn.OpenTSDBInsertJsonPayload(payload)
	if err != nil {
		log.Fatalln("insert error:", err)
	}
}
