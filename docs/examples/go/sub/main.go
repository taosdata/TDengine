package main

import (
	"fmt"
	"os"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/af/tmq"
	tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
)

func main() {
	db, err := af.Open("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists example_tmq wal_retention_period 3600")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create topic if not exists example_tmq_topic as DATABASE example_tmq")
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
	consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
		"group.id":            "test",
		"auto.offset.reset":   "latest",
		"td.connect.ip":       "127.0.0.1",
		"td.connect.user":     "root",
		"td.connect.pass":     "taosdata",
		"td.connect.port":     "6030",
		"client.id":           "test_tmq_client",
		"enable.auto.commit":  "false",
		"msg.with.table.name": "true",
	})
	if err != nil {
		panic(err)
	}
	err = consumer.Subscribe("example_tmq_topic", nil)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table example_tmq.t1 (ts timestamp,v int)")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			_, err = db.Exec("insert into example_tmq.t1 values(now,1)")
			if err != nil {
				panic(err)
			}
			time.Sleep(time.Microsecond * 100)
		}
	}()
	for i := 0; i < 5; i++ {
		ev := consumer.Poll(500)
		if ev != nil {
			switch e := ev.(type) {
			case *tmqcommon.DataMessage:
				fmt.Println(e.String())
			case tmqcommon.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				panic(e)
			}
			consumer.Commit()
		}
	}
	err = consumer.Unsubscribe()
	if err != nil {
		panic(err)
	}
	err = consumer.Close()
	if err != nil {
		panic(err)
	}
}
