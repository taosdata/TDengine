package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
	db, err := sql.Open("taosSql", "root:taosdata@tcp(localhost:6030)/")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	initEnv(db)
	// ANCHOR: query_id
	// use context to set request id
	ctx := context.WithValue(context.Background(), "taos_req_id", int64(3))
	// execute query with context
	rows, err := db.QueryContext(ctx, "SELECT ts, current, location FROM power.meters limit 1")
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var (
			ts       time.Time
			current  float32
			location string
		)
		err = rows.Scan(&ts, &current, &location)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ts: %s, current: %f, location: %s\n", ts, current, location)
	}
	// ANCHOR_END: query_id
}

func initEnv(conn *sql.DB) {
	_, err := conn.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("INSERT INTO power.d1001 USING power.meters TAGS (2, 'California.SanFrancisco') VALUES (NOW , 10.2, 219, 0.32)")
	if err != nil {
		panic(err)
	}
}
