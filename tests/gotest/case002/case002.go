package main

import (
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"time"

	taos "github.com/taosdata/driver-go/v2/af"
)

func Subscribe_check(topic taos.Subscriber, check int) bool {
	count := 0
	rows, err := topic.Consume()
	defer func() { rows.Close(); time.Sleep(time.Second) }()
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}
	for {
		values := make([]driver.Value, 2)
		err := rows.Next(values)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(4)
		}
		count++
	}
	if count == check {
		return false
	} else {
		return true
	}
}
func main() {
	ts := 1630461600000
	db, err := taos.Open("127.0.0.1", "", "", "", 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	db.Exec("drop database if exists test")
	db.Exec("create database if not exists test ")
	db.Exec("use test")
	db.Exec("create table test (ts timestamp ,level int)")
	for i := 0; i < 10; i++ {
		sqlcmd := fmt.Sprintf("insert into test values(%d,%d)", ts+i, i)
		db.Exec(sqlcmd)
	}

	fmt.Println("consumption 01.")
	topic, err := db.Subscribe(false, "test", "select ts, level from test", time.Second)
	if Subscribe_check(topic, 10) {
		os.Exit(3)
	}

	fmt.Println("consumption 02: no new rows inserted")
	if Subscribe_check(topic, 0) {
		os.Exit(3)
	}

	fmt.Println("consumption 03: after one new rows inserted")
	sqlcmd := fmt.Sprintf("insert into test values(%d,%d)", ts+10, 10)
	db.Exec(sqlcmd)
	if Subscribe_check(topic, 1) {
		os.Exit(3)
	}

	fmt.Println("consumption 04: keep progress and continue previous subscription")
	topic.Unsubscribe(true)
	topic, err = db.Subscribe(false, "test", "select ts, level from test", time.Second)
	if Subscribe_check(topic, 0) {
		os.Exit(3)
	}

}
