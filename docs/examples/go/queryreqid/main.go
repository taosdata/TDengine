// Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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
		log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
	}
	defer db.Close()
	initEnv(db)
	// ANCHOR: query_id
	// use context to set request id
	reqId := int64(3)
	ctx := context.WithValue(context.Background(), "taos_req_id", reqId)
	// execute query with context
	querySql := "SELECT ts, current, location FROM power.meters limit 1"
	rows, err := db.QueryContext(ctx, querySql)
	if err != nil {
		log.Fatalf("Failed to execute sql with reqId: %d, url: %s, sql: %s, ErrMessage: %s\n", reqId, taosDSN, querySql, err.Error())
	}
	for rows.Next() {
		// Add your data processing logic here
		var (
			ts       time.Time
			current  float32
			location string
		)
		err = rows.Scan(&ts, &current, &location)
		if err != nil {
			log.Fatalf("Failed to scan data, reqId: %d, url:%s, sql: %s, ErrMessage: %s\n", reqId, taosDSN, querySql, err)
		}
		fmt.Printf("ts: %s, current: %f, location: %s\n", ts, current, location)
	}
	// ANCHOR_END: query_id
}

func initEnv(conn *sql.DB) {
	_, err := conn.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatal("Create database power error: ", err)
	}
	_, err = conn.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatal("Create stable meters error: ", err)
	}
	_, err = conn.Exec("INSERT INTO power.d1001 USING power.meters TAGS (2, 'California.SanFrancisco') VALUES (NOW , 10.2, 219, 0.32)")
	if err != nil {
		log.Fatal("Insert data to power.meters error: ", err)
	}
}
