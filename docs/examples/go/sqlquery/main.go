package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
	db, err := sql.Open("taosSql", "root:taosdata@tcp(localhost:6030)/")
	if err != nil {
		log.Fatal("open database failed:", err)
	}
	defer db.Close()
	// ANCHOR: create_db_and_table
	// create database
	res, err := db.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatal("create database failed:", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		log.Fatal("get affected rows failed:", err)
	}
	fmt.Println("create database affected:", affected)
	// use database
	res, err = db.Exec("USE power")
	if err != nil {
		log.Fatal("use database failed:", err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		log.Fatal("get affected rows failed:", err)
	}
	fmt.Println("use database affected:", affected)
	// create table
	res, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	affected, err = res.RowsAffected()
	if err != nil {
		log.Fatal("create table failed:", err)
	}
	fmt.Println("create table affected:", affected)
	// ANCHOR_END: create_db_and_table
	// ANCHOR: insert_data
	// insert data, please make sure the database and table are created before
	insertQuery := "INSERT INTO " +
		"power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
		"VALUES " +
		"(NOW + 1a, 10.30000, 219, 0.31000) " +
		"(NOW + 2a, 12.60000, 218, 0.33000) " +
		"(NOW + 3a, 12.30000, 221, 0.31000) " +
		"power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
		"VALUES " +
		"(NOW + 1a, 10.30000, 218, 0.25000) "
	res, err = db.Exec(insertQuery)
	if err != nil {
		log.Fatal("insert data failed:", err)
	}
	affected, err = res.RowsAffected()
	if err != nil {
		log.Fatal("get affected rows failed:", err)
	}
	// you can check affectedRows here
	fmt.Println("insert data affected:", affected)
	// ANCHOR_END: insert_data
	// ANCHOR: select_data
	// query data, make sure the database and table are created before
	rows, err := db.Query("SELECT ts, current, location FROM power.meters limit 100")
	if err != nil {
		log.Fatal("query data failed:", err)
	}
	for rows.Next() {
		var (
			ts       time.Time
			current  float32
			location string
		)
		err = rows.Scan(&ts, &current, &location)
		if err != nil {
			log.Fatal("scan data failed:", err)
		}
		// you can check data here
		fmt.Printf("ts: %s, current: %f, location: %s\n", ts, current, location)
	}
	// ANCHOR_END: select_data
}
