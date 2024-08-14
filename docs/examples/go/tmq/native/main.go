package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/af/tmq"
	tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

var done = make(chan struct{})

func main() {
	// init env
	taosDSN := "root:taosdata@tcp(127.0.0.1:6030)/"
	conn, err := sql.Open("taosSql", taosDSN)
	if err != nil {
		log.Fatalln("Failed to connect to " + taosDSN + "; ErrMessage: " + err.Error())
	}
	defer func() {
		conn.Close()
	}()
	initEnv(conn)
	// ANCHOR: create_consumer
	// create consumer
	groupID := "group1"
	clientID := "client1"
	host := "127.0.0.1"
	consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
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
		log.Fatalln("Failed to create native consumer, host : " + host + "; ErrMessage: " + err.Error())
	}
	log.Println("Create consumer successfully, host: " + host + ", groupId: " + groupID + ", clientId: " + clientID)

	// ANCHOR_END: create_consumer
	// ANCHOR: subscribe
	err = consumer.Subscribe("topic_meters", nil)
	if err != nil {
		log.Fatalln("Failed to subscribe topic_meters, ErrMessage: " + err.Error())
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
					log.Fatalln("Failed to commit offset, ErrMessage: " + err.Error())
				}
				log.Println("Commit offset manually successfully.")
				// ANCHOR_END: commit_offset
			case tmqcommon.Error:
				fmt.Printf("%% Error: %v: %v\n", e.Code(), e)
				log.Fatalln("Failed to poll data, ErrMessage: " + err.Error())
			}
		}
	}
	// ANCHOR_END: subscribe
	// ANCHOR: seek
	// get assignment
	partitions, err := consumer.Assignment()
	if err != nil {
		log.Fatal("Failed to get assignment, ErrMessage: " + err.Error())
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
			log.Fatalln("Failed to execute seek example, ErrMessage: " + err.Error())
		}
	}
	fmt.Println("Assignment seek to beginning successfully")
	// ANCHOR_END: seek
	// ANCHOR: close
	// unsubscribe
	err = consumer.Unsubscribe()
	if err != nil {
		log.Fatal("Failed to unsubscribe consumer, ErrMessage: " + err.Error())
	}
	fmt.Println("Consumer unsubscribed successfully.")
	// close consumer
	err = consumer.Close()
	if err != nil {
		log.Fatal("Failed to close consumer, ErrMessage: " + err.Error())
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
