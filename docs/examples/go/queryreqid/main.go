package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
	taosDSN := "root:taosdata@tcp(localhost:6030)/"
	db, err := sql.Open("taosSql", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	defer db.Close()
	initEnv(db)
	// ANCHOR: query_id
	// use context to set request id
	reqId := int64(3)
	ctx := context.WithValue(context.Background(), "taos_req_id", reqId)
	// execute query with context
	rows, err := db.QueryContext(ctx, "SELECT ts, current, location FROM power.meters limit 1")
	if err != nil {
		log.Fatalf("Failed to execute sql with reqId: %d, url: %s; ErrMessage: %s\n", reqId, taosDSN, err.Error())
	}
	for rows.Next() {
		var (
			ts       time.Time
			current  float32
			location string
		)
		err = rows.Scan(&ts, &current, &location)
		if err != nil {
			log.Fatal("Scan error: ", err)
		}
		fmt.Printf("ts: %s, current: %f, location: %s\n", ts, current, location)
	}
	// ANCHOR_END: query_id
}

func initEnv(conn *sql.DB) {
	_, err := conn.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatal("Create database error: ", err)
	}
	_, err = conn.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatal("Create table error: ", err)
	}
	_, err = conn.Exec("INSERT INTO power.d1001 USING power.meters TAGS (2, 'California.SanFrancisco') VALUES (NOW , 10.2, 219, 0.32)")
	if err != nil {
		log.Fatal("Insert data error: ", err)
	}
}
