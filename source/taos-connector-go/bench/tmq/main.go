package main

import (
	"fmt"
	"log"
	"time"

	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

func main() {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}

	result := wrapper.TaosQuery(conn, "create database if not exists abc1 vgroups 2")
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "use abc1")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct0 using st1 tags(1000)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct1 using st1 tags(2000)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct3 using st1 tags(3000)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	//create topic
	result = wrapper.TaosQuery(conn, "create topic if not exists topic_ctb_column as select ts, c1 from ct1")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)
	go func() {
		for {
			log.Println("start insert")
			result = wrapper.TaosQuery(conn, "insert into ct1 values(now,1,2,'1')")
			log.Println("finish insert")
			code = wrapper.TaosError(result)
			log.Println("get error", code)
			if code != 0 {
				errStr := wrapper.TaosErrorStr(result)
				wrapper.TaosFreeResult(result)
				panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
			}
			log.Println("start free result")
			wrapper.TaosFreeResult(result)
			log.Println("finish free result")
			time.Sleep(time.Millisecond)
		}
	}()
	//time.Sleep(time.Hour)

	//build consumer
	conf := wrapper.TMQConfNew()
	wrapper.TMQConfSet(conf, "group.id", "tg2")
	wrapper.TMQConfSet(conf, "enable.auto.commit", "false")
	c := make(chan *wrapper.TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	wrapper.TMQConfSetAutoCommitCB(conf, h)
	tmq, err := wrapper.TMQConsumerNew(conf)
	if err != nil {
		panic(err)
	}
	wrapper.TMQConfDestroy(conf)
	//build_topic_list
	topicList := wrapper.TMQListNew()
	wrapper.TMQListAppend(topicList, "topic_ctb_column")

	//sync_consume_loop
	errCode := wrapper.TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		panic(errors.NewError(int(errCode), errStr))
	}
	c2 := make(chan *wrapper.TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for {
		message := wrapper.TMQConsumerPoll(tmq, 500)
		if message != nil {
			log.Println(message)
			fileCount := wrapper.TaosNumFields(message)
			rh, err := wrapper.ReadColumn(message, fileCount)
			if err != nil {
				panic(err)
			}
			precision := wrapper.TaosResultPrecision(message)
			for {
				blockSize, errCode, block := wrapper.TaosFetchRawBlock(message)
				if errCode != int(errors.SUCCESS) {
					errStr := wrapper.TaosErrorStr(message)
					err := errors.NewError(errCode, errStr)
					wrapper.TaosFreeResult(message)
					panic(err)
				}
				if blockSize == 0 {
					break
				}
				data, err := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
				if err != nil {
					panic(err)
				}
				fmt.Println(data)
			}
			wrapper.TaosFreeResult(message)

			wrapper.TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c:
				if d.ErrCode != 0 {
					panic("error on commit")
				}
				wrapper.PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				panic("wait tmq commit callback timeout")
			}
		}
	}
}
