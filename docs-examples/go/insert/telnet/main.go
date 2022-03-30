package main

import (
	"fmt"

	"github.com/taosdata/driver-go/v2/af"
)

func prepareDatabase(conn *af.Connector) {
	_, err := conn.Exec("CREATE DATABASE test")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("use test")
	if err != nil {
		panic(err)
	}
}

func main() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	if err != nil {
		fmt.Println("fail to connect, err:", err)
	}
	defer conn.Close()
	prepareDatabase(conn)
	var lines = []string{
		"meters.current 1648432611249 10.8 location=Beijing.Haidian groupid=3",
		"meters.current 1648432611250 11.3 location=Beijing.Haidian groupid=3",
		"meters.voltage 1648432611249 219 location=Beijing.Chaoyang groupid=2",
		"meters.voltage 1648432611250 218 location=Beijing.Chaoyang groupid=2",
	}

	err = conn.OpenTSDBInsertTelnetLines(lines)
	if err != nil {
		fmt.Println("insert error:", err)
	}
}

// check data in database:
// taos> select * from test.`meters.current`;
//            ts            |           value           | groupid |            location            |
// =================================================================================================
//  2022-03-28 09:56:51.249 |              10.800000000 | 3       | Beijing.Haidian                |
//  2022-03-28 09:56:51.250 |              11.300000000 | 3       | Beijing.Haidian                |
