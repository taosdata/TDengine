package main

import (
	"database/sql"
	"fmt"

	_ "github.com/taosdata/driver-go/v2/taosSql"
)

func main() {
	var taosDSN = "root:taosdata@tcp(localhost:6030)/"
	taos, err := sql.Open("taosSql", taosDSN)
	if err != nil {
		fmt.Println("failed to connect TDengine, err:", err)
		return
	}
	fmt.Println("Connected")
	defer taos.Close()
}

// use
// var taosDSN = "root:taosdata@tcp(localhost:6030)/dbName"
// if you want to connect a specified database named "dbName".
