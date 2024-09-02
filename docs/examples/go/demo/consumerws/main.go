package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
	"github.com/taosdata/driver-go/v3/ws/tmq"
)

func main() {
	db, err := sql.Open("taosRestful", "root:taosdata@http(localhost:6041)/")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists power WAL_RETENTION_PERIOD 86400")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table if not exists power.d001 using power.meters tags(1,'location')")
	if err != nil {
		panic(err)
	}
	// ANCHOR: create_topic
	_, err = db.Exec("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM power.meters")
	if err != nil {
		panic(err)
	}
	// ANCHOR_END: create_topic
	// ANCHOR: create_consumer
	consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
		"ws.url":                "ws://127.0.0.1:6041",
		"ws.message.channelLen": uint(0),
		"ws.message.timeout":    common.DefaultMessageTimeout,
		"ws.message.writeWait":  common.DefaultWriteWait,
		"td.connect.user":       "root",
		"td.connect.pass":       "taosdata",
		"group.id":              "example",
		"client.id":             "example_consumer",
		"auto.offset.reset":     "latest",
	})
	if err != nil {
		panic(err)
	}
	// ANCHOR_END: create_consumer
	// ANCHOR: poll_data
	go func() {
		for {
			_, err = db.Exec("insert into power.d001 values (now, 1.1, 220, 0.1)")
			if err != nil {
				panic(err)
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()

	err = consumer.Subscribe("topic_meters", nil)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 5; i++ {
		ev := consumer.Poll(500)
		if ev != nil {
			switch e := ev.(type) {
			case *tmqcommon.DataMessage:
				fmt.Printf("get message:%v\n", e)
			case tmqcommon.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				panic(e)
			}
			consumer.Commit()
		}
	}
	// ANCHOR_END: poll_data
	// ANCHOR: consumer_seek
	partitions, err := consumer.Assignment()
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(partitions); i++ {
		fmt.Println(partitions[i])
		err = consumer.Seek(tmqcommon.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    0,
		}, 0)
		if err != nil {
			panic(err)
		}
	}
	partitions, err = consumer.Assignment()
	if err != nil {
		panic(err)
	}
	// ANCHOR_END: consumer_seek
	for i := 0; i < len(partitions); i++ {
		fmt.Println(partitions[i])
	}
	// ANCHOR: consumer_close
	err = consumer.Close()
	if err != nil {
		panic(err)
	}
	// ANCHOR_END: consumer_close
}
