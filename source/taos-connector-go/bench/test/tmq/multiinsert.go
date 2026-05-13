package main

import (
	"fmt"
	"net/http"
	"time"

	_ "net/http/pprof"

	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

func main() {
	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			panic(err)
		}
	}()
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}

	result := wrapper.TaosQuery(conn, "create database if not exists tmq_test_db_multi_insert vgroups 2")
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "use tmq_test_db_multi_insert")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct0 (ts timestamp, c1 int)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	result = wrapper.TaosQuery(conn, "create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)

	//create topic
	result = wrapper.TaosQuery(conn, "create topic if not exists tmq_test_db_multi_insert_topic as tmq_test_db_multi_insert")
	code = wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		wrapper.TaosFreeResult(result)
		panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
	}
	wrapper.TaosFreeResult(result)
	go func() {
		for {
			result = wrapper.TaosQuery(conn, "insert into ct0 values(now,1) ct1 values(now,1,2) ct2 values(now,1,2,'3')")
			code = wrapper.TaosError(result)
			if code != 0 {
				errStr := wrapper.TaosErrorStr(result)
				wrapper.TaosFreeResult(result)
				panic(errors.TaosError{Code: int32(code), ErrStr: errStr})
			}
			wrapper.TaosFreeResult(result)
		}
	}()
	//build consumer
	conf := wrapper.TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	wrapper.TMQConfSet(conf, "enable.auto.commit", "true")
	wrapper.TMQConfSet(conf, "group.id", "tg2")
	wrapper.TMQConfSet(conf, "msg.with.table.name", "true")
	c := make(chan *wrapper.TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	wrapper.TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			wrapper.PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := wrapper.TMQConsumerNew(conf)
	if err != nil {
		panic(err)
	}
	wrapper.TMQConfDestroy(conf)
	//build_topic_list
	topicList := wrapper.TMQListNew()
	wrapper.TMQListAppend(topicList, "tmq_test_db_multi_insert_topic")

	//sync_consume_loop
	errCode := wrapper.TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := wrapper.TMQErr2Str(errCode)
		panic(errors.NewError(int(errCode), errStr))
	}
	totalCount := 0
	c2 := make(chan *wrapper.TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	var table []string
	for {
		message := wrapper.TMQConsumerPoll(tmq, 500)
		if message != nil {
			fmt.Println(message)
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
				tableName := wrapper.TMQGetTableName(message)
				table = append(table, tableName)
				filedCount := wrapper.TaosNumFields(message)
				rh, err := wrapper.ReadColumn(message, filedCount)
				if err != nil {
					panic(err)
				}
				precision := wrapper.TaosResultPrecision(message)
				totalCount += blockSize
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
			case d := <-c2:
				wrapper.PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				panic("wait tmq commit callback timeout")
			}
		}
		table = table[:0]
	}

	//errCode = wrapper.TMQConsumerClose(tmq)
	//if errCode != 0 {
	//	errStr := wrapper.TMQErr2Str(errCode)
	//	panic(errors.NewError(int(errCode), errStr))
	//	return
	//}
}
