package main

import (
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

func main() {
	var taosDSN = "root:taosdata@ws(localhost:6041)/"
	client, err := unified.Open(taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
	}
	defer client.Close()
	// create database
	rowsAffected, err := client.Exec(0, "CREATE DATABASE IF NOT EXISTS example_stmt2")
	if err != nil {
		log.Fatalln("Failed to create database example_stmt2, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create database example_stmt2 successfully, rowsAffected: ", rowsAffected)
	// create table with json
	rowsAffected, err = client.Exec(0, "CREATE STABLE IF NOT EXISTS example_stmt2.stb_json ("+
		"ts TIMESTAMP, "+
		"int_col INT) "+
		"tags (json_tag json)")
	if err != nil {
		log.Fatalln("Failed to create table stb_json, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create table stb_json successfully, rowsAffected:", rowsAffected)
	// create table without json
	rowsAffected, err = client.Exec(0, "CREATE STABLE IF NOT EXISTS example_stmt2.stb ("+
		"ts TIMESTAMP, "+
		"bool_col BOOL, "+
		"tinyint_col TINYINT, "+
		"smallint_col SMALLINT, "+
		"int_col INT, "+
		"bigint_col BIGINT, "+
		"utinyint_col TINYINT UNSIGNED, "+
		"usmallint_col SMALLINT UNSIGNED, "+
		"uint_col INT UNSIGNED, "+
		"ubigint_col BIGINT UNSIGNED, "+
		"float_col FLOAT, "+
		"double_col DOUBLE, "+
		"binary_col BINARY(100), "+
		"nchar_col NCHAR(100), "+
		"varbinary_col VARBINARY(100), "+
		"geometry_col GEOMETRY(100)) "+
		"tags ("+
		"bool_tag BOOL, "+
		"tinyint_tag TINYINT, "+
		"smallint_tag SMALLINT, "+
		"int_tag INT, "+
		"bigint_tag BIGINT, "+
		"utinyint_tag TINYINT UNSIGNED, "+
		"usmallint_tag SMALLINT UNSIGNED, "+
		"uint_tag INT UNSIGNED, "+
		"ubigint_tag BIGINT UNSIGNED, "+
		"float_tag FLOAT, "+
		"double_tag DOUBLE, "+
		"binary_tag BINARY(100), "+
		"nchar_tag NCHAR(100), "+
		"varbinary_tag VARBINARY(100), "+
		"geometry_tag GEOMETRY(100))")
	if err != nil {
		log.Fatalln("Failed to create table stb, ErrMessage: " + err.Error())
	}
	_, err = client.Exec(0, "USE example_stmt2")
	if err != nil {
		log.Fatalln("Failed to use database example_stmt2, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create table stb successfully, rowsAffected:", rowsAffected)

	// stmt bind with json tag
	stmtWithJson()
	// stmt bind without json tag
	stmtWithoutJson()
}

func stmtWithJson() {
	client, err := unified.Open("root:taosdata@ws(localhost:6041)/example_stmt2")
	if err != nil {
		log.Fatalln("Failed to connect to 127.0.0.1, ErrMessage: " + err.Error())
	}
	defer client.Close()
	reqID := common.GetReqID()
	fmt.Printf("reqID: 0x%x\n", reqID)
	stmt2, err := client.InitStmt(reqID)
	if err != nil {
		log.Fatalln("Failed to init stmt2, ErrMessage: " + err.Error())
	}
	defer func() {
		if closeErr := stmt2.Close(0); closeErr != nil {
			log.Printf("Failed to close stmt2, ErrMessage: %s", closeErr.Error())
		}
	}()
	// prepare statement with json
	sql := "INSERT INTO ntb_json using stb_json tags(?) VALUES (?,?)"
	err = stmt2.Prepare(0, sql)
	if err != nil {
		log.Fatal("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	current := time.Now()
	bindData := &commonstmt.TaosStmt2BindData{
		Cols: [][]driver.Value{
			{
				current,
			},
			{
				int32(1),
			},
		},
		Tags: []driver.Value{
			[]byte("{\"device\":\"device_1\"}"),
		},
	}
	params := []*commonstmt.TaosStmt2BindData{bindData}
	err = stmt2.Bind(params)
	if err != nil {
		log.Fatal("Failed to bind params, ErrMessage: " + err.Error())
	}
	// execute batch
	affected, err := stmt2.Exec(0)
	if err != nil {
		log.Fatal("Failed to exec, ErrMessage: " + err.Error())
	}
	fmt.Printf("Successfully inserted %d rows.\n", affected)
}

func stmtWithoutJson() {
	client, err := unified.Open("root:taosdata@ws(localhost:6041)/example_stmt2")
	if err != nil {
		log.Fatalln("Failed to connect to 127.0.0.1, ErrMessage: " + err.Error())
	}
	defer client.Close()
	reqID := common.GetReqID()
	fmt.Printf("reqID: 0x%x\n", reqID)
	stmt2, err := client.InitStmt(reqID)
	if err != nil {
		log.Fatalln("Failed to init stmt2, ErrMessage: " + err.Error())
	}
	defer func() {
		if closeErr := stmt2.Close(0); closeErr != nil {
			log.Printf("Failed to close stmt2, ErrMessage: %s", closeErr.Error())
		}
	}()
	// prepare statement without json
	sql := "INSERT INTO ntb using stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	err = stmt2.Prepare(0, sql)
	if err != nil {
		log.Fatal("Failed to prepare sql, sql: " + sql + ", ErrMessage: " + err.Error())
	}
	current := time.Now()
	bindData := &commonstmt.TaosStmt2BindData{
		Cols: [][]driver.Value{
			{
				current,
			},
			{
				true,
			},
			{
				int8(1),
			},
			{
				int16(1),
			},
			{
				int32(1),
			},
			{
				int64(1),
			},
			{
				uint8(1),
			},
			{
				uint16(1),
			},
			{
				uint32(1),
			},
			{
				uint64(1),
			},
			{
				float32(1.1),
			},
			{
				float64(1.1),
			},
			{
				[]byte("binary_value"),
			},
			{
				"nchar_value",
			},
			{
				[]byte{0x98, 0xf4, 0x6e},
			},
			{
				[]byte{
					0x01, 0x01, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x59,
					0x40, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x59, 0x40,
				},
			},
		},
		Tags: []driver.Value{
			true,
			int8(1),
			int16(1),
			int32(1),
			int64(1),
			uint8(1),
			uint16(1),
			uint32(1),
			uint64(1),
			float32(1.1),
			float64(1.1),
			[]byte("binary_value"),
			"nchar_value",
			[]byte{0x98, 0xf4, 0x6e},
			[]byte{
				0x01, 0x01, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x59,
				0x40, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x59, 0x40,
			},
		},
	}
	params := []*commonstmt.TaosStmt2BindData{bindData}
	err = stmt2.Bind(params)
	if err != nil {
		log.Fatal("Failed to bind params, ErrMessage: " + err.Error())
	}
	// execute batch
	affected, err := stmt2.Exec(0)
	if err != nil {
		log.Fatal("Failed to exec, ErrMessage: " + err.Error())
	}
	fmt.Printf("Successfully inserted %d rows.\n", affected)
}
