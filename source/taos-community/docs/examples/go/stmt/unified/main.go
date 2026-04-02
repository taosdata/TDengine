package main

import (
	"database/sql/driver"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

func main() {
	// ANCHOR: main
	host := "127.0.0.1"
	numOfSubTable := 10
	numOfRow := 10

	taosDSN := fmt.Sprintf("root:taosdata@ws(%s:6041)/", host)
	client, err := unified.Open(taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	defer client.Close()

	// prepare database and table
	_, err = client.Exec(common.GetReqID(), "CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatalln("Failed to create database power, ErrMessage: " + err.Error())
	}
	_, err = client.Exec(common.GetReqID(), "USE power")
	if err != nil {
		log.Fatalln("Failed to use database power, ErrMessage: " + err.Error())
	}
	_, err = client.Exec(common.GetReqID(), "CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatalln("Failed to create stable meters, ErrMessage: " + err.Error())
	}

	// prepare statement
	sql := "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
	stmt, err := client.InitStmt(common.GetReqID())
	if err != nil {
		log.Fatalln("Failed to init stmt, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	defer stmt.Close(common.GetReqID())

	err = stmt.Prepare(common.GetReqID(), sql)
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

		tableName := fmt.Sprintf("d_bind_%d", i)
		bindData := []*commonstmt.TaosStmt2BindData{
			{
				TableName: tableName,
				Tags:      []driver.Value{int32(i), []byte(fmt.Sprintf("location_%d", i))},
				Cols:      columns,
			},
		}

		err = stmt.Bind(bindData)
		if err != nil {
			log.Fatalln("Failed to bind params, ErrMessage: " + err.Error())
		}

		affected, err := stmt.Exec(common.GetReqID())
		if err != nil {
			log.Fatalln("Failed to exec, ErrMessage: " + err.Error())
		}

		fmt.Printf("Successfully inserted %d rows to %s.\n", affected, tableName)
	}
	// ANCHOR_END: main
}
