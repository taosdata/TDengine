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
	"strconv"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
)

const (
	NumOfSubTable = 10
	NumOfRow      = 10
)

func main() {
	prepare()
	db, err := af.Open("", "root", "taosdata", "power", 0)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	stmt := db.InsertStmt()
	defer stmt.Close()
	err = stmt.Prepare("INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)")
	if err != nil {
		panic(err)
	}
	for i := 1; i <= NumOfSubTable; i++ {
		tags := param.NewParam(2).AddInt(i).AddBinary([]byte("location"))
		err = stmt.SetTableNameWithTags("d_bind_"+strconv.Itoa(i), tags)
		if err != nil {
			panic(err)
		}
		now := time.Now()
		params := make([]*param.Param, 4)
		params[0] = param.NewParam(NumOfRow)
		params[1] = param.NewParam(NumOfRow)
		params[2] = param.NewParam(NumOfRow)
		params[3] = param.NewParam(NumOfRow)
		for i := 0; i < NumOfRow; i++ {
			params[0].SetTimestamp(i, now.Add(time.Duration(i)*time.Second), common.PrecisionMilliSecond)
			params[1].SetFloat(i, float32(i))
			params[2].SetInt(i, i)
			params[3].SetFloat(i, float32(i))
		}
		paramTypes := param.NewColumnType(4).AddTimestamp().AddFloat().AddInt().AddFloat()
		err = stmt.BindParam(params, paramTypes)
		if err != nil {
			panic(err)
		}
		err = stmt.AddBatch()
		if err != nil {
			panic(err)
		}
		err = stmt.Execute()
		if err != nil {
			panic(err)
		}
		affected := stmt.GetAffectedRows()
		fmt.Println("affected rows:", affected)
	}
}

func prepare() {
	db, err := af.Open("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		panic(err)
	}
}
