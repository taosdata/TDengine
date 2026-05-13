package taosSql

import (
	"database/sql"
	"testing"
	"time"
)

// @author: xftan
// @date: 2022/1/27 16:19
// @description: test nano second timestamp
func TestNanosecond(t *testing.T) {
	db, err := sql.Open("taosSql", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, "drop database if exists test_go_ns_")
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, "create database if not exists test_go_ns_ precision 'ns'")
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, "create table if not exists test_go_ns_.tb1 (ts timestamp, n int)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, "insert into test_go_ns_.tb1 values(1629363529469478001, 1)(1629363529469478002,2)")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select ts from test_go_ns_.tb1")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var ts time.Time
		err := rows.Scan(&ts)
		if err != nil {
			t.Fatal(err)
		}
		if ts.Nanosecond()%1000 == 0 {
			t.Log(ts.UnixNano())
			t.Fatal("nanosecond is not correct")
		}
	}
}
