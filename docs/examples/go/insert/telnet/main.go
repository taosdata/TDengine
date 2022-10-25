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
	var lines = []string{
		"meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
		"meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
		"meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
		"meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
		"meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
		"meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
		"meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
		"meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
	}

	err = conn.OpenTSDBInsertTelnetLines(lines)
	if err != nil {
		log.Fatalln("insert error:", err)
	}
}
