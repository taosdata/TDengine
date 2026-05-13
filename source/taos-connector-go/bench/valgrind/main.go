package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

var (
	driverName     = "taosSql"
	user           = "root"
	password       = "taosdata"
	host           = ""
	port           = 6030
	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s?interpolateParams=true", user, password, host, port, "log")
	db             *sql.DB
)

func main() {
	var err error
	db, err = sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Fatalf("error on:  sql.open %s", err.Error())
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatalf("close db error:%s", err.Error())
		}
	}(db)
	_, err = db.Exec("create database if not exists bench_test")
	if err != nil {
		log.Fatalf("create database banch_test error %s", err.Error())
	}
	_, err = db.Exec("drop table IF EXISTS bench_test.valgrind")
	if err != nil {
		log.Fatalf("drop table bench_test.valgrind error %s", err.Error())
	}
	_, err = db.Exec("create table  bench_test.valgrind (ts timestamp ,value double)")
	if err != nil {
		log.Fatalf("create table bench_test.valgrind error %s", err.Error())
	}
	insert()
	query()
}

func insert() {
	var err error
	for i := 0; i < 100; i++ {
		_, err = db.Exec(fmt.Sprintf("insert into bench_test.valgrind values ( now + %ds ,123.456)", i))
		if err != nil {
			log.Fatalf("insert data error %s", err)
		}
	}
}

func query() {
	for i := 0; i < 100; i++ {
		rows, err := db.Query("select * from bench_test.test_select")
		if err != nil {
			log.Fatalf("select data error %s", err.Error())
		}
		var t time.Time
		var s float64
		for rows.Next() {
			err := rows.Scan(&t, &s)
			if err != nil {
				log.Fatalf("scan error %s", err.Error())
			}
			if s != 123.456 {
				log.Fatalf("result error expect 123.456 got %f", s)
			}
		}
	}
}
