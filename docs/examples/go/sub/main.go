package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	"github.com/taosdata/driver-go/v3/af/tmq"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func main() {
	db, err := af.Open("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("create database if not exists example_tmq")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create topic if not exists example_tmq_topic with meta as DATABASE example_tmq")
	if err != nil {
		panic(err)
	}
	config := tmq.NewConfig()
	defer config.Destroy()
	err = config.SetGroupID("test")
	if err != nil {
		panic(err)
	}
	err = config.SetAutoOffsetReset("earliest")
	if err != nil {
		panic(err)
	}
	err = config.SetConnectIP("127.0.0.1")
	if err != nil {
		panic(err)
	}
	err = config.SetConnectUser("root")
	if err != nil {
		panic(err)
	}
	err = config.SetConnectPass("taosdata")
	if err != nil {
		panic(err)
	}
	err = config.SetConnectPort("6030")
	if err != nil {
		panic(err)
	}
	err = config.SetMsgWithTableName(true)
	if err != nil {
		panic(err)
	}
	err = config.EnableHeartBeat()
	if err != nil {
		panic(err)
	}
	err = config.EnableAutoCommit(func(result *wrapper.TMQCommitCallbackResult) {
		if result.ErrCode != 0 {
			errStr := wrapper.TMQErr2Str(result.ErrCode)
			err := errors.NewError(int(result.ErrCode), errStr)
			panic(err)
		}
	})
	if err != nil {
		panic(err)
	}
	consumer, err := tmq.NewConsumer(config)
	if err != nil {
		panic(err)
	}
	err = consumer.Subscribe([]string{"example_tmq_topic"})
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("create table example_tmq.t1 (ts timestamp,v int)")
	if err != nil {
		panic(err)
	}
	for {
		result, err := consumer.Poll(time.Second)
		if err != nil {
			panic(err)
		}
		if result.Type != common.TMQ_RES_TABLE_META {
			panic("want message type 2 got " + strconv.Itoa(int(result.Type)))
		}
		data, _ := json.Marshal(result.Meta)
		fmt.Println(string(data))
		consumer.Commit(context.Background(), result.Message)
		consumer.FreeMessage(result.Message)
		break
	}
	_, err = db.Exec("insert into example_tmq.t1 values(now,1)")
	if err != nil {
		panic(err)
	}
	for {
		result, err := consumer.Poll(time.Second)
		if err != nil {
			panic(err)
		}
		if result.Type != common.TMQ_RES_DATA {
			panic("want message type 1 got " + strconv.Itoa(int(result.Type)))
		}
		data, _ := json.Marshal(result.Data)
		fmt.Println(string(data))
		consumer.Commit(context.Background(), result.Message)
		consumer.FreeMessage(result.Message)
		break
	}
	consumer.Close()
}
