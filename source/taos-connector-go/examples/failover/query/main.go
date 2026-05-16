package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosWS"
)

func main() {
	// Multi-endpoint DSN with auto-reconnect for mid-stream failover.
	taosDSN := "root:taosdata@ws(127.0.0.1:6042,127.0.0.1:6041)/?autoReconnect=true"
	db, err := sql.Open("taosWS", taosDSN)
	if err != nil {
		log.Fatalln("open taosWS failed:", err)
	}
	defer db.Close()

	// Use fully qualified table names (db.table) instead of "USE db",
	// because the database context is lost after reconnection.
	mustExec(db, "create database if not exists example_failover_query")
	mustExec(db, "create table if not exists example_failover_query.meters(ts timestamp, v int)")

	baseTs := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 3; i++ {
		sqlText := fmt.Sprintf("insert into example_failover_query.meters values(%d,%d)", baseTs+int64(i), i+1)
		mustExec(db, sqlText)
	}

	rows, err := db.Query("select ts, v from example_failover_query.meters order by ts desc limit 3")
	if err != nil {
		log.Fatalln("query failed:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ts time.Time
		var v int32
		if err = rows.Scan(&ts, &v); err != nil {
			log.Fatalln("scan failed:", err)
		}
		fmt.Printf("ts=%s v=%d\n", ts.Format(time.RFC3339Nano), v)
	}
	if err = rows.Err(); err != nil {
		log.Fatalln("rows err:", err)
	}

	fmt.Println("failover query example done")
}

func mustExec(db *sql.DB, sqlText string) {
	if _, err := db.Exec(sqlText); err != nil {
		log.Fatalf("exec failed, sql=%s, err=%v\n", sqlText, err)
	}
}
