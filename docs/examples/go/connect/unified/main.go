// Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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
	"fmt"
	"log"

	"github.com/taosdata/driver-go/v3/ws/unified"
)

func main() {
	// ANCHOR: main
	// use
	// var taosDSN = "root:taosdata@ws(localhost:6041,localhost:6042)/dbName?autoReconnect=true"
	// if you want to connect a specified database named "dbName" with failover.
	var taosDSN = "root:taosdata@ws(localhost:6041)/"
	taos, err := unified.Open(taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	fmt.Println("Connected to " + taosDSN + " successfully.")
	defer taos.Close()
	// ANCHOR_END: main
}
