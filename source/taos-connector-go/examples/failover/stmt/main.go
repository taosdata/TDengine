package main

import (
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"time"

	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

func main() {
	// Multi-endpoint DSN with auto-reconnect for mid-stream failover.
	taosDSN := "root:taosdata@ws(127.0.0.1:6042,127.0.0.1:6041)/?autoReconnect=true"
	client, err := unified.Open(taosDSN)
	if err != nil {
		log.Fatalln("open unified client failed:", err)
	}
	defer client.Close()

	// Use fully qualified table names (db.table) instead of "USE db",
	// because the database context is lost after reconnection.
	mustExec(client, "create database if not exists example_failover_stmt")
	mustExec(client, "create table if not exists example_failover_stmt.d0(ts timestamp, v int)")

	stmt, err := client.InitStmt(0)
	if err != nil {
		log.Fatalln("init stmt2 failed:", err)
	}
	defer func() {
		_ = stmt.Close(0)
	}()

	if err = stmt.Prepare(0, "insert into example_failover_stmt.d0 values(?,?)"); err != nil {
		log.Fatalln("prepare failed:", err)
	}

	ts1 := time.Now().UTC().Round(time.Millisecond)
	ts2 := ts1.Add(time.Second)
	ts3 := ts1.Add(2 * time.Second)
	bindData := []*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{ts1, ts2, ts3},
				{int32(10), nil, int32(30)},
			},
		},
	}
	if err = stmt.Bind(bindData); err != nil {
		log.Fatalln("bind failed:", err)
	}

	affected, err := stmt.Exec(0)
	if err != nil {
		log.Fatalln("exec failed:", err)
	}
	fmt.Printf("stmt2 inserted rows=%d\n", affected)

	rows, err := client.Query(0, "select ts, v from example_failover_stmt.d0 order by ts")
	if err != nil {
		log.Fatalln("query failed:", err)
	}
	defer rows.Close()

	for {
		values := make([]driver.Value, 2)
		err = rows.Next(values)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln("next failed:", err)
		}
		fmt.Printf("ts=%v v=%v\n", values[0], values[1])
	}

	fmt.Println("failover stmt example done")
}

func mustExec(client *unified.Client, sqlText string) {
	if _, err := client.Exec(0, sqlText); err != nil {
		log.Fatalf("exec failed, sql=%s, err=%v\n", sqlText, err)
	}
}
