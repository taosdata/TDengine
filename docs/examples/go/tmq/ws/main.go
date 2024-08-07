package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
	_ "github.com/taosdata/driver-go/v3/taosWS"
	"github.com/taosdata/driver-go/v3/ws/tmq"
)

var done = make(chan struct{})

func main() {
	// init env
	conn, err := sql.Open("taosWS", "root:taosdata@ws(127.0.0.1:6041)/")
	if err != nil {
		log.Fatal("failed to connect TDengine, err:", err)
	}
	defer func() {
		conn.Close()
	}()
	initEnv(conn)
	// ANCHOR: create_consumer
	// create consumer
	consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:6041",
		"ws.message.channelLen":   uint(0),
		"ws.message.timeout":      common.DefaultMessageTimeout,
		"ws.message.writeWait":    common.DefaultWriteWait,
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"auto.offset.reset":       "latest",
		"msg.with.table.name":     "true",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "1000",
		"group.id":                "group1",
		"client.id":               "client1",
	})
	if err != nil {
		log.Fatal("failed to create consumer, err:", err)
	}
	// ANCHOR_END: create_consumer
	// ANCHOR: subscribe
	err = consumer.Subscribe("topic_meters", nil)
	if err != nil {
		log.Fatal("failed to subscribe, err:", err)
	}
	for i := 0; i < 50; i++ {
		ev := consumer.Poll(100)
		if ev != nil {
			switch e := ev.(type) {
			case *tmqcommon.DataMessage:
				// process your data here
				fmt.Printf("data:%v\n", e)
				// ANCHOR: commit_offset
				// commit offset
				topicPartition, err := consumer.CommitOffsets([]tmqcommon.TopicPartition{e.TopicPartition})
				if err != nil {
					log.Fatal("failed to commit offset, err:", err)
				}
				fmt.Println(topicPartition)
				// ANCHOR_END: commit_offset
			case tmqcommon.Error:
				fmt.Printf("%% Error: %v: %v\n", e.Code(), e)
				log.Fatal("failed to get message, err:", e)
			}
			// commit all offsets
			topicPartition, err := consumer.Commit()
			if err != nil {
				log.Fatal("failed to commit, err:", err)
			}
			fmt.Println(topicPartition)

		}
	}
	// ANCHOR_END: subscribe
	// ANCHOR: seek
	// get assignment
	partitions, err := consumer.Assignment()
	if err != nil {
		log.Fatal("failed to get assignment, err:", err)
	}
	for i := 0; i < len(partitions); i++ {
		fmt.Println(partitions[i])
		// seek to the beginning
		err = consumer.Seek(tmqcommon.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    0,
		}, 0)
		if err != nil {
			log.Fatal("failed to seek, err:", err)
		}
	}
	fmt.Println("assignment seek to beginning successfully")
	// poll data again
	gotData := false
	for i := 0; i < 50; i++ {
		if gotData {
			break
		}
		ev := consumer.Poll(100)
		if ev != nil {
			switch e := ev.(type) {
			case *tmqcommon.DataMessage:
				// process your data here
				fmt.Printf("second data polled:%v\n", e)
				gotData = true
			case tmqcommon.Error:
				fmt.Printf("%% Error: %v: %v\n", e.Code(), e)
				log.Fatal("failed to get message, err:", e)
			}
		}
	}
	// ANCHOR_END: seek
	// ANCHOR: close
	// unsubscribe
	err = consumer.Unsubscribe()
	if err != nil {
		log.Fatal("failed to unsubscribe, err:", err)
	}
	// close consumer
	err = consumer.Close()
	if err != nil {
		log.Fatal("failed to close consumer, err:", err)
	}
	// ANCHOR_END: close
	<-done
}

func initEnv(conn *sql.DB) {
	_, err := conn.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatal("failed to create database, err:", err)
	}
	_, err = conn.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatal("failed to create stable, err:", err)
	}
	_, err = conn.Exec("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM power.meters")
	if err != nil {
		log.Fatal("failed to create topic, err:", err)
	}
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second)
			_, err = conn.Exec("INSERT INTO power.d1001 USING power.meters TAGS (2, 'California.SanFrancisco') VALUES (NOW , 10.2, 219, 0.32)")
			if err != nil {
				log.Fatal("failed to insert data, err:", err)
			}
		}
		done <- struct{}{}
	}()
}
