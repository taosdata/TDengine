package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
	var taosDSN = "root:taosdata@http(localhost:6041)/"
	taos, err := sql.Open("taosRestful", taosDSN)
	if err != nil {
		log.Fatalln("failed to connect TDengine, err:", err)
		return
	}
	fmt.Println("Connected")
	defer taos.Close()
}

// use
// var taosDSN = "root:taosdata@http(localhost:6041)/dbName"
// if you want to connect a specified database named "dbName".
