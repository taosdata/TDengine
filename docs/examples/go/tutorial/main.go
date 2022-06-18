package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/taosdata/driver-go/v2/taosRestful"
)

func main() {
	dsn := os.Getenv("TDENGINE_GO_DSN")
	taos, err := sql.Open("taosRestful", dsn)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer taos.Close()
	// ANCHOR: insert
	_, err = taos.Exec("DROP DATABASE IF EXISTS power")
	if err != nil {
		fmt.Println("failed to drop database, err:", err)
		return
	}
	_, err = taos.Exec("CREATE DATABASE power")
	if err != nil {
		fmt.Println("failed to create database, err:", err)
		return
	}
	_, err = taos.Exec("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
	if err != nil {
		fmt.Println("failed to create stable, err:", err)
		return
	}
	result, err := taos.Exec("INSERT INTO power.d1001 USING power.meters TAGS(California.SanFrancisco, 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000) power.d1002 USING power.meters TAGS(California.SanFrancisco, 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)")
	if err != nil {
		fmt.Println("failed to insert, err:", err)
		return
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		fmt.Println("failed to get affected rows, err:", err)
		return
	}
	fmt.Println("RowsAffected", rowsAffected) // RowsAffected 4
	// ANCHOR_END: insert
	// ANCHOR: query
	rows, err := taos.Query("SELECT ts, current FROM power.meters LIMIT 2")
	if err != nil {
		fmt.Println("failed to select from table, err:", err)
		return
	}
	defer rows.Close()
	// ANCHOR_END: query
	// ANCHOR: meta
	// print column names
	colNames, _ := rows.Columns()
	fmt.Println(colNames)
	// ANCHOR_END: meta
	// ANCHOR: iter
	for rows.Next() {
		var r struct {
			ts      time.Time
			current float32
		}
		err := rows.Scan(&r.ts, &r.current)
		if err != nil {
			fmt.Println("scan error:\n", err)
			return
		}
		fmt.Println(r.ts, r.current)
	}
	// 2018-10-03 14:38:05 +0000 UTC 10.3
	// 2018-10-03 14:38:15 +0000 UTC 12.6
	// ANCHOR_END: iter
}
