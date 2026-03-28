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

func main() {
	setupDSN := "root:taosdata@ws(127.0.0.1:6041)/"
	db, err := sql.Open("taosWS", setupDSN)
	if err != nil {
		log.Fatalln("open taosWS failed:", err)
	}
	defer db.Close()

	initTMQEnv(db)

	// ws.url uses multiple endpoints for failover; this example intentionally puts
	// a standby endpoint first to demonstrate endpoint fallback.
	consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:6042,ws://127.0.0.1:6041",
		"ws.autoReconnect":        true,
		"ws.message.channelLen":   uint(0),
		"ws.message.timeout":      common.DefaultMessageTimeout,
		"ws.message.writeWait":    common.DefaultWriteWait,
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"group.id":                "failover_group",
		"client.id":               "failover_client",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      "false",
		"msg.with.table.name":     "true",
		"auto.commit.interval.ms": "1000",
	})
	if err != nil {
		log.Fatalln("new consumer failed:", err)
	}
	defer func() {
		_ = consumer.Unsubscribe()
		_ = consumer.Close()
	}()

	if err = consumer.Subscribe("topic_failover_meters", nil); err != nil {
		log.Fatalln("subscribe failed:", err)
	}

	received := false
	for i := 0; i < 20; i++ {
		ev := consumer.Poll(500)
		if ev == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		switch e := ev.(type) {
		case *tmqcommon.DataMessage:
			received = true
			fmt.Printf("received topic partition: %s\n", e.TopicPartition)
			if _, err = consumer.CommitOffsets([]tmqcommon.TopicPartition{e.TopicPartition}); err != nil {
				log.Fatalln("commit offsets failed:", err)
			}
			fmt.Println("commit offset success")
		case tmqcommon.Error:
			log.Printf("poll error: %s", e.Error())
		}
		if received {
			break
		}
	}

	if !received {
		log.Println("no tmq message received within timeout")
	} else {
		fmt.Println("failover tmq example done")
	}
}

func initTMQEnv(db *sql.DB) {
	mustExec(db, "create database if not exists example_failover_tmq")
	mustExec(db, "create stable if not exists example_failover_tmq.meters(ts timestamp, current float, voltage int) tags(location binary(64))")
	mustExec(db, "create topic if not exists topic_failover_meters as select ts,current,voltage,location from example_failover_tmq.meters")

	for i := 0; i < 3; i++ {
		sqlText := fmt.Sprintf(
			"insert into example_failover_tmq.d1001 using example_failover_tmq.meters tags('beijing') values(now + %da, %f, %d)",
			i,
			10.1+float64(i),
			220+i,
		)
		mustExec(db, sqlText)
	}
}

func mustExec(db *sql.DB, sqlText string) {
	if _, err := db.Exec(sqlText); err != nil {
		log.Fatalf("exec failed, sql=%s, err=%v\n", sqlText, err)
	}
}
