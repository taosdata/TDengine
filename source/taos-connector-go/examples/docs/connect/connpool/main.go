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
	fmt.Println("Connected")
	defer taos.Close()
	// ANCHOR: pool
	// SetMaxOpenConns sets the maximum number of open connections to the database. 0 means unlimited.
	taos.SetMaxOpenConns(0)
	// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
	taos.SetMaxIdleConns(2)
	// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
	taos.SetConnMaxLifetime(0)
	// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
	taos.SetConnMaxIdleTime(0)
	// ANCHOR_END: pool
}
