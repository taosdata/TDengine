package main

import (
	"fmt"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
)

func checkErr(err error, prompt string) {
	if err != nil {
		fmt.Printf("%s\n", prompt)
		panic(err)
	}
}

func prepareStable(conn *af.Connector) {
	_, err := conn.Exec("CREATE DATABASE power")
	checkErr(err, "failed to create database")
	_, err = conn.Exec("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
	checkErr(err, "failed to create stable")
	_, err = conn.Exec("USE power")
	checkErr(err, "failed to change database")
}

func main() {
	conn, err := af.Open("localhost", "root", "taosdata", "", 6030)
	checkErr(err, "fail to connect")
	defer conn.Close()
	prepareStable(conn)
	// create stmt
	stmt := conn.InsertStmt()
	defer stmt.Close()
	err = stmt.Prepare("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
	checkErr(err, "failed to create prepare statement")

	// bind table name and tags
	tagParams := param.NewParam(2).AddBinary([]byte("California.SanFrancisco")).AddInt(2)
	err = stmt.SetTableNameWithTags("d1001", tagParams)
	checkErr(err, "failed to execute SetTableNameWithTags")

	// specify ColumnType
	var bindType *param.ColumnType = param.NewColumnType(4).AddTimestamp().AddFloat().AddInt().AddFloat()

	// bind values. note: can only bind one row each time.
	valueParams := []*param.Param{
		param.NewParam(1).AddTimestamp(time.Unix(1648432611, 249300000), common.PrecisionMilliSecond),
		param.NewParam(1).AddFloat(10.3),
		param.NewParam(1).AddInt(219),
		param.NewParam(1).AddFloat(0.31),
	}
	err = stmt.BindParam(valueParams, bindType)
	checkErr(err, "BindParam error")
	err = stmt.AddBatch()
	checkErr(err, "AddBatch error")

	// bind one more row
	valueParams = []*param.Param{
		param.NewParam(1).AddTimestamp(time.Unix(1648432611, 749300000), common.PrecisionMilliSecond),
		param.NewParam(1).AddFloat(12.6),
		param.NewParam(1).AddInt(218),
		param.NewParam(1).AddFloat(0.33),
	}
	err = stmt.BindParam(valueParams, bindType)
	checkErr(err, "BindParam error")
	err = stmt.AddBatch()
	checkErr(err, "AddBatch error")
	// execute
	err = stmt.Execute()
	checkErr(err, "Execute batch error")
}
