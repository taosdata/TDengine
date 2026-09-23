// Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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
	"database/sql"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
	var taosDSN = "root:taosdata@http(localhost:6041)/power"
	taos, err := sql.Open("taosRestful", taosDSN)
	if err != nil {
		log.Fatalln("failed to connect TDengine, err:", err)
	}
	defer taos.Close()
	rows, err := taos.Query("SELECT ts, current FROM meters LIMIT 2")
	if err != nil {
		log.Fatalln("failed to select from table, err:", err)
	}

	defer rows.Close()
	for rows.Next() {
		var r struct {
			ts      time.Time
			current float32
		}
		err := rows.Scan(&r.ts, &r.current)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		log.Println(r.ts, r.current)
	}
}
