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
	"log"

	"github.com/taosdata/driver-go/v3/af"
)

func prepareDatabase(conn *af.Connector) {
	_, err := conn.Exec("CREATE DATABASE test")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("USE test")
	if err != nil {
		panic(err)
	}
}

func main() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	if err != nil {
		log.Fatalln("fail to connect, err:", err)
	}
	defer conn.Close()
	prepareDatabase(conn)
	var lines = []string{
		"meters.current 1648432611249 10.3 location=California.SanFrancisco groupid=2",
		"meters.current 1648432611250 12.6 location=California.SanFrancisco groupid=2",
		"meters.current 1648432611249 10.8 location=California.LosAngeles groupid=3",
		"meters.current 1648432611250 11.3 location=California.LosAngeles groupid=3",
		"meters.voltage 1648432611249 219 location=California.SanFrancisco groupid=2",
		"meters.voltage 1648432611250 218 location=California.SanFrancisco groupid=2",
		"meters.voltage 1648432611249 221 location=California.LosAngeles groupid=3",
		"meters.voltage 1648432611250 217 location=California.LosAngeles groupid=3",
	}

	err = conn.OpenTSDBInsertTelnetLines(lines)
	if err != nil {
		log.Fatalln("insert error:", err)
	}
}
