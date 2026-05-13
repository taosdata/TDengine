package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"github.com/taosdata/driver-go/v3/types"
)

func main() {
	db, err := sql.Open("taosRestful", "root:taosdata@http(192.168.1.163:8085)/?token=xxxxxx")
	if err != nil {
		panic(err)
	}
	rand.Seed(time.Now().UnixNano())
	defer db.Close()
	_, err = db.Exec("create database if not exists restful_demo")
	if err != nil {
		panic(err)
	}
	var (
		v1  = true
		v2  = int8(rand.Int())
		v3  = int16(rand.Int())
		v4  = rand.Int31()
		v5  = int64(rand.Int31())
		v6  = uint8(rand.Uint32())
		v7  = uint16(rand.Uint32())
		v8  = rand.Uint32()
		v9  = uint64(rand.Uint32())
		v10 = rand.Float32()
		v11 = rand.Float64()
	)

	_, err = db.Exec("create table if not exists restful_demo.all_type(ts timestamp," +
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
		")" +
		"tags(t json)",
	)
	if err != nil {
		panic(err)
	}
	now := time.Now().Round(time.Millisecond)
	_, err = db.Exec(fmt.Sprintf(`insert into restful_demo.t1 using restful_demo.all_type tags('{"a":"b"}') values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar')`, now.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11))
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from restful_demo.all_type where ts = '%s'", now.Format(time.RFC3339Nano)))
	if err != nil {
		panic(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	fmt.Printf("column names: %s\n", strings.Join(columns, ","))
	for rows.Next() {
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
			tt  types.RawMessage
		)
		err := rows.Scan(
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
			&tt,
		)
		if err != nil {
			panic(err)
		}
		fmt.Println("ts", "insert", now.Local(), "result", ts.Local())
		fmt.Println("c1", "insert", v1, "result", c1)
		fmt.Println("c2", "insert", v2, "result", c2)
		fmt.Println("c3", "insert", v3, "result", c3)
		fmt.Println("c4", "insert", v4, "result", c4)
		fmt.Println("c5", "insert", v5, "result", c5)
		fmt.Println("c6", "insert", v6, "result", c6)
		fmt.Println("c7", "insert", v7, "result", c7)
		fmt.Println("c8", "insert", v8, "result", c8)
		fmt.Println("c9", "insert", v9, "result", c9)
		fmt.Println("c10", "insert", v10, "result", c10)
		fmt.Println("c11", "insert", v11, "result", c11)
		fmt.Println("c12", "insert", "test_binary", "result", c12)
		fmt.Println("c13", "insert", "test_nchar", "result", c13)
		fmt.Println("tag", "insert", "{\"a\":\"b\"}", "result", string(tt))
	}
}
