package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
	var taosDSN = "root:taosdata@tcp(localhost:6030)/"
	taos, err := sql.Open("taosSql", taosDSN)
	if err != nil {
		log.Fatalln("failed to connect TDengine, err:", err)
	}
	defer taos.Close()
	// ANCHOR: create_db_and_table
	_, err = taos.Exec("CREATE DATABASE if not exists power")
	if err != nil {
		log.Fatalln("failed to create database, err:", err)
	}
	_, err = taos.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatalln("failed to create stable, err:", err)
	}
	// ANCHOR_END: create_db_and_table
	// ANCHOR: insert_data
	affected, err := taos.Exec("INSERT INTO " +
		"power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
		"VALUES " +
		"(NOW + 1a, 10.30000, 219, 0.31000) " +
		"(NOW + 2a, 12.60000, 218, 0.33000) " +
		"(NOW + 3a, 12.30000, 221, 0.31000) " +
		"power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
		"VALUES " +
		"(NOW + 1a, 10.30000, 218, 0.25000) ")
	if err != nil {
		log.Fatalln("failed to insert data, err:", err)
	}
	log.Println("affected rows:", affected)
	// ANCHOR_END: insert_data
	// ANCHOR: query_data
	rows, err := taos.Query("SELECT * FROM power.meters")
	if err != nil {
		log.Fatalln("failed to select from table, err:", err)
	}

	defer rows.Close()
	for rows.Next() {
		var (
			ts       time.Time
			current  float32
			voltage  int
			phase    float32
			groupId  int
			location string
		)
		err := rows.Scan(&ts, &current, &voltage, &phase, &groupId, &location)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		log.Println(ts, current, voltage, phase, groupId, location)
	}
	// ANCHOR_END: query_data
	// ANCHOR: with_reqid
	ctx := context.WithValue(context.Background(), common.ReqIDKey, common.GetReqID())
	_, err = taos.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatalln("failed to create database, err:", err)
	}
	// ANCHOR_END: with_reqid
}
