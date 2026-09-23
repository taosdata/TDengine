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
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
)

func main() {
	host := "127.0.0.1"
	numOfSubTable := 10
	numOfRow := 10
	db, err := af.Open(host, "root", "taosdata", "", 0)
	if err != nil {
		log.Fatalln("Failed to connect to " + host + "; ErrMessage: " + err.Error())
	}
	defer db.Close()
	// prepare database and table
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("USE power")
	if err != nil {
		log.Fatalln("Failed to use database power, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
	}
	// prepare statement
	sql := "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
	stmt := db.Stmt()
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	for i := 1; i <= numOfSubTable; i++ {
		tableName := fmt.Sprintf("d_bind_%d", i)
		tags := param.NewParam(2).AddInt(i).AddBinary([]byte(fmt.Sprintf("location_%d", i)))
		// set tableName and tags
		err = stmt.SetTableNameWithTags(tableName, tags)
		if err != nil {
			log.Fatalln("Failed to set table name and tags, tableName: " + tableName + "; ErrMessage: " + err.Error())
		}
		// bind column data
		current := time.Now()
		for j := 0; j < numOfRow; j++ {
			row := param.NewParam(4).
				AddTimestamp(current.Add(time.Millisecond*time.Duration(j)), common.PrecisionMilliSecond).
				AddFloat(rand.Float32() * 30).
				AddInt(rand.Intn(300)).
				AddFloat(rand.Float32())
			err = stmt.BindRow(row)
			if err != nil {
				log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
			}
		}
		// add batch
		err = stmt.AddBatch()
		if err != nil {
			log.Fatalln("Failed to add batch, ErrMessage: " + err.Error())
		}
		// execute batch
		err = stmt.Execute()
		if err != nil {
			log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
		}
		// get affected rows
		affected := stmt.GetAffectedRows()
		// you can check exeResult here
		fmt.Printf("Successfully inserted %d rows to %s.\n", affected, tableName)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal("failed to close statement, err:", err)
	}
}
