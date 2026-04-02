package main

import (
	"fmt"
	"log"

	"github.com/taosdata/driver-go/v3/ws/unified"
)

func main() {
	// ANCHOR: main
	// use
	// var taosDSN = "root:taosdata@ws(localhost:6041,localhost:6042)/dbName?autoReconnect=true"
	// if you want to connect a specified database named "dbName" with failover.
	var taosDSN = "root:taosdata@ws(localhost:6041)/"
	taos, err := unified.Open(taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	fmt.Println("Connected to " + taosDSN + " successfully.")
	defer taos.Close()
	// ANCHOR_END: main
}
