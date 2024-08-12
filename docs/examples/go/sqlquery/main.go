package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
	var taosDSN = "root:taosdata@tcp(localhost:6030)/"
	db, err := sql.Open("taosSql", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	defer db.Close()
	// ANCHOR: create_db_and_table
	// create database
	res, err := db.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatalln("Failed to create db, url:" + taosDSN + "; ErrMessage: " + err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create db rowsAffected, url:" + taosDSN + "; ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create database power successfully, rowsAffected: ", rowsAffected)
	// create table
	res, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatalln("Failed to create db and table, url:" + taosDSN + "; ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create create rowsAffected, url:" + taosDSN + "; ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create stable power.meters successfully, rowsAffected:", rowsAffected)
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
		log.Fatal("Failed to insert data to power.meters, url:" + taosDSN + "; ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatal("Failed to get insert rowsAffected, url:" + taosDSN + "; ErrMessage: " + err.Error())
	}
	// you can check affectedRows here
	fmt.Printf("Successfully inserted %d rows to power.meters.\n", rowsAffected)
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
