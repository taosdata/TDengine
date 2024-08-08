package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
	// use
	// var taosDSN = "root:taosdata@tcp(localhost:6030)/dbName"
	// if you want to connect a specified database named "dbName".
	var taosDSN = "root:taosdata@tcp(localhost:6030)/"
	taos, err := sql.Open("taosSql", taosDSN)
	if err != nil {
		log.Fatalln("failed to connect TDengine, err:", err)
	}
	fmt.Println("Connected to " + taosDSN + " successfully.")
	defer taos.Close()
}
