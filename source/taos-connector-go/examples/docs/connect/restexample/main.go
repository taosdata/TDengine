package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
	// use
	// var taosDSN = "root:taosdata@http(localhost:6041)/dbName"
	// if you want to connect a specified database named "dbName".
	var taosDSN = "root:taosdata@http(localhost:6041)/"
	taos, err := sql.Open("taosRestful", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	fmt.Println("Connected to " + taosDSN + " successfully.")
	defer taos.Close()
}
