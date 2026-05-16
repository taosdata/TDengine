package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

var cdb *sql.DB
var restfulDB *sql.DB
var dataC []string
var dataRestful []string

func main() {
	var err error
	cdb, err = sql.Open("taosSql", "root:taosdata@tcp(localhost:6030)/")
	if err != nil {
		panic(err)
	}
	restfulDB, err = sql.Open("taosRestful", "root:taosdata@http(127.0.0.1:6041)/?readBufferSize=52428800")
	if err != nil {
		panic(err)
	}
	_, err = cdb.Exec("drop database if exists benchmark_driver")
	if err != nil {
		panic(err)
	}
	_, err = cdb.Exec("create database if not exists benchmark_driver")
	if err != nil {
		panic(err)
	}
	testInsert()
	testQuery()
}
func testQuery() {
	var err error
	_, err = cdb.Exec("create table benchmark_driver.alltype_query(ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")")
	if err != nil {
		panic(err)
	}
	now := time.Now()
	for i := 0; i < 3000; i++ {
		_, err := cdb.Exec(fmt.Sprintf(`insert into benchmark_driver.alltype_query values('%s',1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')`, now.Add(time.Second*time.Duration(i)).Format(time.RFC3339Nano)))
		if err != nil {
			panic(err)
		}
	}
	testQueryC()
	testQueryRestful()
}

func testQueryC() {
	s := time.Now()
	for i := 0; i < 1000; i++ {
		result, err := cdb.Query(`select * from benchmark_driver.alltype_query limit 3000`)
		if err != nil {
			panic(err)
		}
		for result.Next() {
			var (
				ts  time.Time
				c1  bool
				c2  int8
				c3  int16
				c4  int32
				c5  int64
				c6  uint8
				c7  uint16
				c8  uint32
				c9  uint64
				c10 float32
				c11 float64
				c12 string
				c13 string
			)
			err := result.Scan(
				&ts,
				&c1,
				&c2,
				&c3,
				&c4,
				&c5,
				&c6,
				&c7,
				&c8,
				&c9,
				&c10,
				&c11,
				&c12,
				&c13,
			)
			if err != nil {
				panic(err)
			}
		}
	}
	delta := time.Since(s).Nanoseconds()
	fmt.Println("cgo query", float64(delta)/1000)
}

func testQueryRestful() {
	s := time.Now()
	var (
		ts  time.Time
		c1  bool
		c2  int8
		c3  int16
		c4  int32
		c5  int64
		c6  uint8
		c7  uint16
		c8  uint32
		c9  uint64
		c10 float32
		c11 float64
		c12 string
		c13 string
	)
	for i := 0; i < 1000; i++ {
		result, err := restfulDB.Query(`select * from benchmark_driver.alltype_query limit 3000`)
		if err != nil {
			panic(err)
		}
		for result.Next() {
			err := result.Scan(
				&ts,
				&c1,
				&c2,
				&c3,
				&c4,
				&c5,
				&c6,
				&c7,
				&c8,
				&c9,
				&c10,
				&c11,
				&c12,
				&c13,
			)
			if err != nil {
				panic(err)
			}
		}
	}
	delta := time.Since(s).Nanoseconds()
	fmt.Println("restful query", float64(delta)/1000)
}

func testInsert() {
	var err error
	_, err = cdb.Exec("create table benchmark_driver.alltype_insert_c(ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")")
	if err != nil {
		panic(err)
	}
	_, err = cdb.Exec("create table benchmark_driver.alltype_insert_restful(ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")")
	if err != nil {
		panic(err)
	}
	ts := time.Now().UnixNano() / 1e6
	for i := 0; i < 50000; i++ {
		dataC = append(dataC, fmt.Sprintf("insert into benchmark_driver.alltype_insert_c values(%d,1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')", int64(i)+ts))
		dataRestful = append(dataRestful, fmt.Sprintf("insert into benchmark_driver.alltype_insert_restful values(%d,1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')", int64(i)+ts))
	}
	testCGO()
	testRestful()
}
func testCGO() {
	s := time.Now()
	for i := 0; i < 50000; i++ {
		_, err := cdb.Exec(dataC[i])
		if err != nil {
			panic(err)
		}
	}
	delta := time.Since(s).Nanoseconds()
	fmt.Println("cgo", float64(delta)/50000)
}

func testRestful() {
	s := time.Now()
	for i := 0; i < 50000; i++ {
		_, err := restfulDB.Exec(dataRestful[i])
		if err != nil {
			panic(err)
		}
	}
	delta := time.Since(s).Nanoseconds()
	fmt.Println("restful", float64(delta)/50000)
}
