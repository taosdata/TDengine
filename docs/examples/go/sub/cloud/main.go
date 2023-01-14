package main

import (
	"fmt"
	"github.com/taosdata/driver-go/v3/common"
	tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/ws/tmq"
	"os"
)

func main() {
	tmqStr := os.Getenv("TDENGINE_CLOUD_TMQ")
	consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
		"ws.url":                tmqStr,
		"ws.message.channelLen": uint(0),
		"ws.message.timeout":    common.DefaultMessageTimeout,
		"ws.message.writeWait":  common.DefaultWriteWait,
		"group.id":              "test_group",
		"client.id":             "test_consumer_ws",
		"auto.offset.reset":     "earliest",
	})
	if err != nil {
		panic(err)
	}
	err = consumer.Subscribe("test", nil)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	for {
		ev := consumer.Poll(0)
		if ev != nil {
			switch e := ev.(type) {
			case *tmqcommon.DataMessage:
				fmt.Printf("get message:%v\n", e.String())
				consumer.Commit()
			case tmqcommon.Error:
				fmt.Printf("%% Error: %v: %v\n", e.Code(), e)
				return
			default:
				fmt.Printf("unexpected event:%v\n", e)
				return
			}
		}
	}
}
