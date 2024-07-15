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
