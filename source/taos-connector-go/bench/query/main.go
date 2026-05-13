package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

var (
	driverName     = "taosSql"
	user           = "root"
	password       = "taosdata"
	host           = ""
	port           = 6030
	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s?interpolateParams=true", user, password, host, port, "")
)

func main() {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			panic(err)
		}
	}()
	_, err = db.Exec("create database if not exists test_json")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("drop table if exists test_json.tjson")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create stable if not exists test_json.tjson(ts timestamp,v int )tags(t json)")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`insert into test_json.tj_1 using test_json.tjson tags('{"a":1,"b":"b"}')values (now,1)`)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`insert into test_json.tj_2 using test_json.tjson tags('{"a":1,"c":"c"}')values (now,1)`)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`insert into test_json.tj_3 using test_json.tjson tags('null')values (now,1)`)
	if err != nil {
		panic(err)
	}
	rows, err := db.Query("select t from test_json.tjson")
	if err != nil {
		panic(err)
	}
	counter := 0
	for rows.Next() {
		var info []byte
		err := rows.Scan(&info)
		if err != nil {
			panic(err)
		}
		if info != nil && !json.Valid(info) {
			fmt.Println("invalid json ", string(info))
			return
		}
		if info == nil {
			fmt.Println("null")
		} else {
			fmt.Printf("%s", string(info))
		}
		counter += 1
	}
	fmt.Println(counter)
	//assert.Equal(t, 3, counter)
}
