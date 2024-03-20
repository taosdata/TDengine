package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"github.com/taosdata/driver-go/v3/ws/stmt"
)

const (
	NumOfSubTable = 10
	NumOfRow      = 10
)

func main() {
	prepare()
	config := stmt.NewConfig("ws://127.0.0.1:6041", 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetConnectDB("power")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)
	config.SetErrorHandler(func(connector *stmt.Connector, err error) {
		panic(err)
	})
	config.SetCloseHandler(func() {
		fmt.Println("stmt connector closed")
	})

	connector, err := stmt.NewConnector(config)
	if err != nil {
		panic(err)
	}
	stmt, err := connector.Init()
	err = stmt.Prepare("INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)")
	if err != nil {
		panic(err)
	}
	for i := 1; i <= NumOfSubTable; i++ {
		tags := param.NewParam(2).AddInt(i).AddBinary([]byte("location"))
		err = stmt.SetTableName("d_bind_" + strconv.Itoa(i))
		if err != nil {
			panic(err)
		}
		err = stmt.SetTags(tags, param.NewColumnType(2).AddInt().AddBinary(8))
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
		err = stmt.Exec()
		if err != nil {
			panic(err)
		}
		affected := stmt.GetAffectedRows()
		fmt.Println("affected rows:", affected)
	}
}

func prepare() {
	db, err := sql.Open("taosRestful", "root:taosdata@http(localhost:6041)/")
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
