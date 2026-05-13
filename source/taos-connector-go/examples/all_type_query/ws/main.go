package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosWS"
)

func main() {
	var taosDSN = "root:taosdata@ws(localhost:6041)/"
	db, err := sql.Open("taosWS", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
	}
	defer db.Close()
	// create database
	res, err := db.Exec("CREATE DATABASE IF NOT EXISTS example_all_type_query")
	if err != nil {
		log.Fatalln("Failed to create database example_all_type_query, ErrMessage: " + err.Error())
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create database rowsAffected, ErrMessage: " + err.Error())
	}
	// you can check rowsAffected here
	fmt.Println("Create database example_all_type_query successfully, rowsAffected: ", rowsAffected)
	// create table with json
	res, err = db.Exec("CREATE STABLE IF NOT EXISTS example_all_type_query.stb_json (" +
		"ts TIMESTAMP, " +
		"int_col INT) " +
		"tags (json_tag json)")
	if err != nil {
		log.Fatalln("Failed to create table example_all_type_query.stb_json, ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
	}
	fmt.Println("Create table example_query_varbinary_native.stb_json successfully, rowsAffected:", rowsAffected)
	// insert data
	var insertQuery = "INSERT INTO example_all_type_query.ntb_json using example_all_type_query.stb_json tags('{\"device\":\"device_1\"}') " +
		"values(now, 1)"
	res, err = db.Exec(insertQuery)
	if err != nil {
		log.Fatalf("Failed to insert data to example_all_type_query.ntb_json, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	// you can check affectedRows here
	fmt.Printf("Successfully inserted %d rows to example_all_type_query.ntb_json.\n", rowsAffected)
	// query data
	sql := "SELECT * FROM example_all_type_query.stb_json"
	rows, err := db.Query(sql)
	if err != nil {
		log.Fatalf("Failed to query data from example_all_type_query.stb_json, sql: %s, ErrMessage: %s\n", sql, err.Error())
	}
	for rows.Next() {
		// Add your data processing logic here
		var (
			ts      time.Time
			intVal  int32
			jsonVal []byte
		)
		err = rows.Scan(&ts, &intVal, &jsonVal)
		if err != nil {
			log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", sql, err)
		}
		fmt.Printf(
			"ts: %s, "+
				"int_col: %d, "+
				"json_tag: %s\n",
			ts,
			intVal,
			jsonVal,
		)
	}

	// create table without json
	res, err = db.Exec("CREATE STABLE IF NOT EXISTS example_all_type_query.stb (" +
		"ts TIMESTAMP, " +
		"bool_col BOOL, " +
		"tinyint_col TINYINT, " +
		"smallint_col SMALLINT, " +
		"int_col INT, " +
		"bigint_col BIGINT, " +
		"utinyint_col TINYINT UNSIGNED, " +
		"usmallint_col SMALLINT UNSIGNED, " +
		"uint_col INT UNSIGNED, " +
		"ubigint_col BIGINT UNSIGNED, " +
		"float_col FLOAT, " +
		"double_col DOUBLE, " +
		"binary_col BINARY(100), " +
		"nchar_col NCHAR(100), " +
		"varbinary_col VARBINARY(100), " +
		"geometry_col GEOMETRY(100), " +
		"decimal_col DECIMAL(20,4), " +
		"blob_col BLOB) " +
		"tags (" +
		"bool_tag BOOL, " +
		"tinyint_tag TINYINT, " +
		"smallint_tag SMALLINT, " +
		"int_tag INT, " +
		"bigint_tag BIGINT, " +
		"utinyint_tag TINYINT UNSIGNED, " +
		"usmallint_tag SMALLINT UNSIGNED, " +
		"uint_tag INT UNSIGNED, " +
		"ubigint_tag BIGINT UNSIGNED, " +
		"float_tag FLOAT, " +
		"double_tag DOUBLE, " +
		"binary_tag BINARY(100), " +
		"nchar_tag NCHAR(100), " +
		"varbinary_tag VARBINARY(100), " +
		"geometry_tag GEOMETRY(100)) ")
	if err != nil {
		log.Fatalln("Failed to create table example_all_type_query.stb, ErrMessage: " + err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalln("Failed to get create table rowsAffected, ErrMessage: " + err.Error())
	}
	fmt.Println("Create table example_query_varbinary_native.stb successfully, rowsAffected:", rowsAffected)
	// insert data
	insertQuery = "INSERT INTO example_all_type_query.ntb using example_all_type_query.stb " +
		"tags(true, 1, 2, 3, 4, 5, 6, 7, 8, 1.5, 2.5, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)') " +
		"values(now, true, 1, 2, 3, 4, 5, 6, 7, 8, 1.5, 2.5, 'binary_value', 'nchar_value', '\\x98f46e', 'POINT(100 100)', 12.3400, 'blob_value')"
	res, err = db.Exec(insertQuery)
	if err != nil {
		log.Fatalf("Failed to insert data to example_all_type_query.ntb_json, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	rowsAffected, err = res.RowsAffected()
	if err != nil {
		log.Fatalf("Failed to get insert rowsAffected, sql: %s, ErrMessage: %s\n", insertQuery, err.Error())
	}
	// you can check affectedRows here
	fmt.Printf("Successfully inserted %d rows to example_all_type_query.ntb_json.\n", rowsAffected)
	// query data
	sql = "SELECT * FROM example_all_type_query.stb"
	rows, err = db.Query(sql)
	if err != nil {
		log.Fatalf("Failed to query data from example_all_type_query.stb, sql: %s, ErrMessage: %s\n", sql, err.Error())
	}
	for rows.Next() {
		// Add your data processing logic here
		var (
			ts           time.Time
			boolVal      bool
			tinyintVal   int8
			smallintVal  int16
			intVal       int32
			bigintVal    int64
			utinyintVal  uint8
			usmallintVal uint16
			uintVal      uint32
			ubigintVal   uint64
			floatVal     float32
			doubleVal    float64
			binaryVal    []byte
			ncharVal     string
			varbinaryVal []byte
			geometryVal  []byte
			decimalVal   string
			blobVal      []byte
			boolTag      bool
			tinyintTag   int8
			smallintTag  int16
			intTag       int32
			bigintTag    int64
			utinyintTag  uint8
			usmallintTag uint16
			uintTag      uint32
			ubigintTag   uint64
			floatTag     float32
			doubleTag    float64
			binaryTag    []byte
			ncharTag     string
			varbinaryTag []byte
			geometryTag  []byte
		)
		err = rows.Scan(
			&ts,
			&boolVal,
			&tinyintVal,
			&smallintVal,
			&intVal,
			&bigintVal,
			&utinyintVal,
			&usmallintVal,
			&uintVal,
			&ubigintVal,
			&floatVal,
			&doubleVal,
			&binaryVal,
			&ncharVal,
			&varbinaryVal,
			&geometryVal,
			&decimalVal,
			&blobVal,
			&boolTag,
			&tinyintTag,
			&smallintTag,
			&intTag,
			&bigintTag,
			&utinyintTag,
			&usmallintTag,
			&uintTag,
			&ubigintTag,
			&floatTag,
			&doubleTag,
			&binaryTag,
			&ncharTag,
			&varbinaryTag,
			&geometryTag,
		)
		if err != nil {
			log.Fatalf("Failed to scan data, sql: %s, ErrMessage: %s\n", sql, err)
		}
		fmt.Printf(
			"ts: %s, "+
				"bool_col: %t, "+
				"tinyint_col: %d, "+
				"smallint_col: %d, "+
				"int_col: %d, "+
				"bigint_col: %d, "+
				"utinyint_col: %d, "+
				"usmallint_col: %d, "+
				"uint_col: %d, "+
				"ubigint_col: %d, "+
				"float_col: %f, "+
				"double_col: %f, "+
				"binary_col: %s, "+
				"nchar_col: %s, "+
				"varbinary_col: %v, "+
				"geometry_col: %v, "+
				"decimal_col: %s, "+
				"blob_col: %s, "+
				"bool_tag: %t, "+
				"tinyint_tag: %d, "+
				"smallint_tag: %d, "+
				"int_tag: %d, "+
				"bigint_tag: %d, "+
				"utinyint_tag: %d, "+
				"usmallint_tag: %d, "+
				"uint_tag: %d, "+
				"ubigint_tag: %d, "+
				"float_tag: %f, "+
				"double_tag: %f, "+
				"binary_tag: %s, "+
				"nchar_tag: %s, "+
				"varbinary_tag: %v, "+
				"geometry_tag: %v\n",
			ts,
			boolVal,
			tinyintVal,
			smallintVal,
			intVal,
			bigintVal,
			utinyintVal,
			usmallintVal,
			uintVal,
			ubigintVal,
			floatVal,
			doubleVal,
			binaryVal,
			ncharVal,
			varbinaryVal,
			geometryVal,
			decimalVal,
			blobVal,
			boolTag,
			tinyintTag,
			smallintTag,
			intTag,
			bigintTag,
			utinyintTag,
			usmallintTag,
			uintTag,
			ubigintTag,
			floatTag,
			doubleTag,
			binaryTag,
			ncharTag,
			varbinaryTag,
			geometryTag,
		)
	}
}
