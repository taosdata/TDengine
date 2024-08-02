package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"github.com/taosdata/driver-go/v3/ws/stmt"
)

func main() {
	host := "127.0.0.1"
	numOfSubTable := 10
	numOfRow := 10
	db, err := sql.Open("taosRestful", fmt.Sprintf("root:taosdata@http(%s:6041)/", host))
	if err != nil {
		log.Fatal("failed to connect TDengine, err:", err)
	}
	defer db.Close()
	// prepare database and table
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatal("failed to create database, err:", err)
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatal("failed to create table, err:", err)
	}

	config := stmt.NewConfig(fmt.Sprintf("ws://%s:6041", host), 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetConnectDB("power")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)

	connector, err := stmt.NewConnector(config)
	if err != nil {
		log.Fatal("failed to create stmt connector, err:", err)
	}
	// prepare statement
	sql := "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
	stmt, err := connector.Init()
	if err != nil {
		log.Fatal("failed to init stmt, err:", err)
	}
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatal("failed to prepare stmt, err:", err)
	}
	for i := 1; i <= numOfSubTable; i++ {
		tableName := fmt.Sprintf("d_bind_%d", i)
		tags := param.NewParam(2).AddInt(i).AddBinary([]byte(fmt.Sprintf("location_%d", i)))
		tagsType := param.NewColumnType(2).AddInt().AddBinary(24)
		columnType := param.NewColumnType(4).AddTimestamp().AddFloat().AddInt().AddFloat()
		// set tableName
		err = stmt.SetTableName(tableName)
		if err != nil {
			log.Fatal("failed to set table name, err:", err)
		}
		// set tags
		err = stmt.SetTags(tags, tagsType)
		if err != nil {
			log.Fatal("failed to set tags, err:", err)
		}
		// bind column data
		current := time.Now()
		for j := 0; j < numOfRow; j++ {
			columnData := make([]*param.Param, 4)
			columnData[0] = param.NewParam(1).AddTimestamp(current.Add(time.Millisecond*time.Duration(j)), common.PrecisionMilliSecond)
			columnData[1] = param.NewParam(1).AddFloat(rand.Float32() * 30)
			columnData[2] = param.NewParam(1).AddInt(rand.Intn(300))
			columnData[3] = param.NewParam(1).AddFloat(rand.Float32())
			err = stmt.BindParam(columnData, columnType)
			if err != nil {
				log.Fatal("failed to bind param, err:", err)
			}
		}
		// add batch
		err = stmt.AddBatch()
		if err != nil {
			log.Fatal("failed to add batch, err:", err)
		}
		// execute batch
		err = stmt.Exec()
		if err != nil {
			log.Fatal("failed to exec stmt, err:", err)
		}
		// get affected rows
		affected := stmt.GetAffectedRows()
		// you can check exeResult here
		fmt.Printf("table %s insert %d rows.\n", tableName, affected)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal("failed to close stmt, err:", err)
	}
}
