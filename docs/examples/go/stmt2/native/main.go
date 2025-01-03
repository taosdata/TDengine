package main

import (
	"database/sql/driver"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/stmt"
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
	reqID := common.GetReqID()
	stmt2 := db.Stmt2(reqID, false)
	err = stmt2.Prepare(sql)
	if err != nil {
		log.Fatalln("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	for i := 1; i <= numOfSubTable; i++ {
		// generate column data
		current := time.Now()
		columns := make([][]driver.Value, 4)
		for j := 0; j < numOfRow; j++ {
			columns[0] = append(columns[0], current.Add(time.Millisecond*time.Duration(j)))
			columns[1] = append(columns[1], rand.Float32()*30)
			columns[2] = append(columns[2], rand.Int31n(300))
			columns[3] = append(columns[3], rand.Float32())
		}
		// generate bind data
		tableName := fmt.Sprintf("d_bind_%d", i)
		tags := []driver.Value{int32(i), []byte(fmt.Sprintf("location_%d", i))}
		bindData := []*stmt.TaosStmt2BindData{
			{
				TableName: tableName,
				Tags:      tags,
				Cols:      columns,
			},
		}
		// bind params
		err = stmt2.Bind(bindData)
		if err != nil {
			log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
		}
		// execute batch
		err = stmt2.Execute()
		if err != nil {
			log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
		}
		// get affected rows
		affected := stmt2.GetAffectedRows()
		// you can check exeResult here
		fmt.Printf("Successfully inserted %d rows to %s.\n", affected, tableName)
	}
	err = stmt2.Close()
	if err != nil {
		log.Fatal("failed to close statement, err:", err)
	}
}
