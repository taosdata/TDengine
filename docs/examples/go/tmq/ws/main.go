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
var groupID string
var clientID string
var host string
var topic string

func main() {
	// init env
	taosDSN := "root:taosdata@ws(127.0.0.1:6041)/"
	conn, err := sql.Open("taosWS", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + ", ErrMessage: " + err.Error())
	}
	defer func() {
		conn.Close()
	}()
	initEnv(conn)
	// ANCHOR: create_consumer
	// create consumer
	wsUrl := "ws://127.0.0.1:6041"
	groupID = "group1"
	clientID = "client1"
	host = "127.0.0.1"
	consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
		"ws.url":                  wsUrl,
		"ws.message.channelLen":   uint(0),
		"ws.message.timeout":      common.DefaultMessageTimeout,
		"ws.message.writeWait":    common.DefaultWriteWait,
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"auto.offset.reset":       "latest",
		"msg.with.table.name":     "true",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "1000",
		"group.id":                groupID,
		"client.id":               clientID,
	})
	if err != nil {
		log.Fatalf(
			"Failed to create websocket consumer, host: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
			host,
			groupID,
			clientID,
			err.Error(),
		)
	}
	log.Printf("Create consumer successfully, host: %s, groupId: %s, clientId: %s\n", host, groupID, clientID)

	// ANCHOR_END: create_consumer
	// ANCHOR: subscribe
	topic = "topic_meters"
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf(
			"Failed to subscribe topic_meters, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
			topic,
			groupID,
			clientID,
			err.Error(),
		)
	}
	log.Println("Subscribe topics successfully")
	for i := 0; i < 50; i++ {
		ev := consumer.Poll(100)
		if ev != nil {
			switch e := ev.(type) {
			case *tmqcommon.DataMessage:
				// process your data here
				fmt.Printf("data:%v\n", e)
				// ANCHOR: commit_offset
				// commit offset
				_, err = consumer.CommitOffsets([]tmqcommon.TopicPartition{e.TopicPartition})
				if err != nil {
					log.Fatalf(
						"Failed to commit offset, topic: %s, groupId: %s, clientId: %s, offset %s, ErrMessage: %s\n",
						topic,
						groupID,
						clientID,
						e.TopicPartition,
						err.Error(),
					)
				}
				log.Println("Commit offset manually successfully.")
				// ANCHOR_END: commit_offset
			case tmqcommon.Error:
				log.Fatalf(
					"Failed to poll data, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
					topic,
					groupID,
					clientID,
					e.Error(),
				)
			}
		}
	}
	// ANCHOR_END: subscribe
	// ANCHOR: seek
	// get assignment
	partitions, err := consumer.Assignment()
	if err != nil {
		log.Fatalf(
			"Failed to get assignment, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
			topic,
			groupID,
			clientID,
			err.Error(),
		)
	}
	fmt.Println("Now assignment:", partitions)
	for i := 0; i < len(partitions); i++ {
		// seek to the beginning
		err = consumer.Seek(tmqcommon.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    0,
		}, 0)
		if err != nil {
			log.Fatalf(
				"Failed to seek offset, topic: %s, groupId: %s, clientId: %s, partition: %d, offset: %d, ErrMessage: %s\n",
				topic,
				groupID,
				clientID,
				partitions[i].Partition,
				0,
				err.Error(),
			)
		}
	}
	fmt.Println("Assignment seek to beginning successfully")
	// ANCHOR_END: seek
	// ANCHOR: close
	// unsubscribe
	err = consumer.Unsubscribe()
	if err != nil {
		log.Fatalf(
			"Failed to unsubscribe consumer, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
			topic,
			groupID,
			clientID,
			err.Error(),
		)
	}
	fmt.Println("Consumer unsubscribed successfully.")
	// close consumer
	err = consumer.Close()
	if err != nil {
		log.Fatalf(
			"Failed to close consumer, topic: %s, groupId: %s, clientId: %s, ErrMessage: %s\n",
			topic,
			groupID,
			clientID,
			err.Error(),
		)
	}
	fmt.Println("Consumer closed successfully.")
	// ANCHOR_END: close
	<-done
}

func initEnv(conn *sql.DB) {
	_, err := conn.Exec("CREATE DATABASE IF NOT EXISTS power")
	if err != nil {
		log.Fatal("Failed to create database, ErrMessage: " + err.Error())
	}
	_, err = conn.Exec("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
	if err != nil {
		log.Fatal("Failed to create stable, ErrMessage: " + err.Error())
	}
	_, err = conn.Exec("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM power.meters")
	if err != nil {
		log.Fatal("Failed to create topic, ErrMessage: " + err.Error())
	}
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second)
			_, err = conn.Exec("INSERT INTO power.d1001 USING power.meters TAGS (2, 'California.SanFrancisco') VALUES (NOW , 10.2, 219, 0.32)")
			if err != nil {
				log.Fatal("Failed to insert data, ErrMessage: " + err.Error())
			}
		}
		done <- struct{}{}
	}()
}
