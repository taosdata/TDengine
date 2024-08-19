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

	taosDSN := fmt.Sprintf("root:taosdata@http(%s:6041)/", host)
	db, err := sql.Open("taosRestful", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	defer db.Close()
	// prepare database and table
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatalln("Failed to create stable power.meters, ErrMessage: " + err.Error())
	}

	config := stmt.NewConfig(fmt.Sprintf("ws://%s:6041", host), 0)
	config.SetConnectUser("root")
	config.SetConnectPass("taosdata")
	config.SetConnectDB("power")
	config.SetMessageTimeout(common.DefaultMessageTimeout)
	config.SetWriteWait(common.DefaultWriteWait)

	connector, err := stmt.NewConnector(config)
	if err != nil {
		log.Fatalln("Failed to create stmt connector,url: " + taosDSN + "; ErrMessage: " + err.Error())
	}
	// prepare statement
	sql := "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
	stmt, err := connector.Init()
	if err != nil {
		log.Fatalln("Failed to init stmt, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	err = stmt.Prepare(sql)
	if err != nil {
		log.Fatal("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	for i := 1; i <= numOfSubTable; i++ {
		tableName := fmt.Sprintf("d_bind_%d", i)
		tags := param.NewParam(2).AddInt(i).AddBinary([]byte(fmt.Sprintf("location_%d", i)))
		tagsType := param.NewColumnType(2).AddInt().AddBinary(24)
		columnType := param.NewColumnType(4).AddTimestamp().AddFloat().AddInt().AddFloat()
		// set tableName
		err = stmt.SetTableName(tableName)
		if err != nil {
			log.Fatal("Failed to set table name, tableName: " + tableName + "; ErrMessage: " + err.Error())
		}
		// set tags
		err = stmt.SetTags(tags, tagsType)
		if err != nil {
			log.Fatal("Failed to set tags, ErrMessage: " + err.Error())
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
				log.Fatal("Failed to bind params, ErrMessage: " + err.Error())
			}
		}
		// add batch
		err = stmt.AddBatch()
		if err != nil {
			log.Fatal("Failed to add batch, ErrMessage: " + err.Error())
		}
		// execute batch
		err = stmt.Exec()
		if err != nil {
			log.Fatal("Failed to exec, ErrMessage: " + err.Error())
		}
		// get affected rows
		affected := stmt.GetAffectedRows()
		// you can check exeResult here
		fmt.Printf("Successfully inserted %d rows to %s.\n", affected, tableName)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal("Failed to close stmt, ErrMessage: " + err.Error())
	}
}
