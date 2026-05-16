package wrapper

import (
	"database/sql/driver"
	"testing"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	tmqcommon "github.com/taosdata/taosadapter/v3/driver/common/tmq"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

// @author: xftan
// @date: 2023/10/13 11:32
// @description: test tmq
func TestTMQ(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		result := TaosQuery(conn, "drop database if exists abc1")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result := TaosQuery(conn, "create database if not exists abc1 vgroups 2 WAL_RETENTION_PERIOD 86400")
	code := TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	assert.NoError(t, ensureDBCreated(conn, "abc1"))

	result = TaosQuery(conn, "use abc1")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct0 using st1 tags(1000)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct1 using st1 tags(2000)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	result = TaosQuery(conn, "create table if not exists ct3 using st1 tags(3000)")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)

	//create topic
	defer func() {
		result = TaosQuery(conn, "drop topic if exists topic_ctb_column")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	result = TaosQuery(conn, "create topic if not exists topic_ctb_column as select ts, c1 from ct1")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
		return
	}
	TaosFreeResult(result)
	defer func() {
		result = TaosQuery(conn, "drop topic if exists topic_ctb_column")
		code = TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
			return
		}
		TaosFreeResult(result)
	}()
	go func() {
		for i := 0; i < 5; i++ {
			result = TaosQuery(conn, "insert into ct1 values(now,1,2,'1')")
			code = TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				TaosFreeResult(result)
				t.Error(errors.TaosError{Code: int32(code), ErrStr: errStr})
				return
			}
			TaosFreeResult(result)
			time.Sleep(time.Millisecond)
		}
	}()
	//build consumer
	conf := TMQConfNew()
	TMQConfSet(conf, "msg.with.table.name", "true")
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "auto.offset.reset", "earliest")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "topic_ctb_column")

	//sync_consume_loop
	s := time.Now()
	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	t.Log("sub", time.Since(s))
	errCode, list := TMQSubscription(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	defer TMQListDestroy(list)
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"topic_ctb_column"}, r)
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {

		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "topic_ctb_column", topic)
			vgroupID := TMQGetVgroupID(message)
			t.Log("vgroupID", vgroupID)

			for {
				blockSize, errCode, block := TaosFetchRawBlock(message)
				if errCode != int(errors.SUCCESS) {
					errStr := TaosErrorStr(message)
					err := errors.NewError(errCode, errStr)
					t.Error(err)
					TaosFreeResult(message)
					return
				}
				if blockSize == 0 {
					break
				}
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				//tableName := TMQGetTableName(message)
				//assert.Equal(t, "ct1", tableName)
				dbName := TMQGetDBName(message)
				assert.Equal(t, "abc1", dbName)
				data, err := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
				assert.NoError(t, err)
				t.Log(data)
			}
			TaosFreeResult(message)
			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
				assert.Equal(t, int32(0), d.ErrCode)
				PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				t.Error("wait tmq commit callback timeout")
				return
			}
		}
	}

	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
}

// @author: xftan
// @date: 2023/10/13 11:33
// @description: test TMQList
func TestTMQList(t *testing.T) {
	list := TMQListNew()
	defer TMQListDestroy(list)
	TMQListAppend(list, "1")
	TMQListAppend(list, "2")
	TMQListAppend(list, "3")
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"1", "2", "3"}, r)
}

// @author: xftan
// @date: 2023/10/13 11:33
// @description: test tmq subscribe db
func TestTMQDB(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists tmq_test_db")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists tmq_test_db vgroups 2 WAL_RETENTION_PERIOD 86400")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "tmq_test_db"))
	err = exec(conn, "use tmq_test_db")
	require.NoError(t, err)

	err = exec(conn, "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct0 using st1 tags(1000)")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct1 using st1 tags(2000)")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct3 using st1 tags(3000)")
	require.NoError(t, err)

	//create topic
	err = exec(conn, "create topic if not exists test_tmq_db_topic as DATABASE tmq_test_db")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop topic if exists test_tmq_db_topic")
	}()
	go func() {
		for i := 0; i < 5; i++ {
			err = exec(conn, "insert into ct1 values(now,1,2,'1')")
			require.NoError(t, err)
			time.Sleep(time.Millisecond)
		}
	}()
	//build consumer
	conf := TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	TMQConfSet(conf, "auto.offset.reset", "earliest")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "test_tmq_db_topic")

	//sync_consume_loop
	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	errCode, list := TMQSubscription(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	defer TMQListDestroy(list)
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"test_tmq_db_topic"}, r)
	totalCount := 0
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "test_tmq_db_topic", topic)
			for {
				blockSize, errCode, block := TaosFetchRawBlock(message)
				if errCode != int(errors.SUCCESS) {
					errStr := TaosErrorStr(message)
					err := errors.NewError(errCode, errStr)
					t.Error(err)
					TaosFreeResult(message)
					return
				}
				if blockSize == 0 {
					break
				}
				tableName := TMQGetTableName(message)
				assert.Equal(t, "ct1", tableName)
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				totalCount += blockSize
				data, err := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
				assert.NoError(t, err)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
				assert.Nil(t, d.GetError())
				assert.Equal(t, int32(0), d.ErrCode)
				PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				t.Error("wait tmq commit callback timeout")
				return
			}
		}
	}

	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	assert.GreaterOrEqual(t, totalCount, 5)
}

// @author: xftan
// @date: 2023/10/13 11:33
// @description: test tmq subscribe multi tables
func TestTMQDBMultiTable(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists tmq_test_db_multi")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists tmq_test_db_multi vgroups 2 WAL_RETENTION_PERIOD 86400")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "tmq_test_db_multi"))
	err = exec(conn, "use tmq_test_db_multi")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct0 (ts timestamp, c1 int)")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	require.NoError(t, err)

	//create topic
	err = exec(conn, "create topic if not exists test_tmq_db_multi_topic as DATABASE tmq_test_db_multi")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop topic if exists test_tmq_db_multi_topic")
		require.NoError(t, err)
	}()
	{
		err = exec(conn, "insert into ct0 values(now,1)")
		require.NoError(t, err)
	}
	{
		err = exec(conn, "insert into ct1 values(now,1,2)")
		require.NoError(t, err)
	}
	{
		err = exec(conn, "insert into ct2 values(now,1,2,'3')")
		require.NoError(t, err)
	}
	//build consumer
	conf := TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	TMQConfSet(conf, "auto.offset.reset", "earliest")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "test_tmq_db_multi_topic")

	//sync_consume_loop
	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	errCode, list := TMQSubscription(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	defer TMQListDestroy(list)
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"test_tmq_db_multi_topic"}, r)
	totalCount := 0
	tables := map[string]struct{}{
		"ct0": {},
		"ct1": {},
		"ct2": {},
	}
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "test_tmq_db_multi_topic", topic)
			for {
				blockSize, errCode, block := TaosFetchRawBlock(message)
				if errCode != int(errors.SUCCESS) {
					errStr := TaosErrorStr(message)
					err := errors.NewError(errCode, errStr)
					t.Error(err)
					TaosFreeResult(message)
					return
				}
				if blockSize == 0 {
					break
				}
				tableName := TMQGetTableName(message)
				delete(tables, tableName)
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				totalCount += blockSize
				data, err := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
				assert.NoError(t, err)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
				assert.Equal(t, int32(0), d.ErrCode)
				PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				t.Error("wait tmq commit callback timeout")
				return
			}
		}
	}
	errCode = TMQUnsubscribe(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	assert.GreaterOrEqual(t, totalCount, 3)
	assert.Emptyf(t, tables, "tables name not empty", tables)
}

// @author: xftan
// @date: 2023/10/13 11:33
// @description: test tmq subscribe db with multi table insert
func TestTMQDBMultiInsert(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists tmq_test_db_multi_insert")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists tmq_test_db_multi_insert vgroups 2 wal_retention_period 3600")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "tmq_test_db_multi_insert"))
	err = exec(conn, "use tmq_test_db_multi_insert")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct0 (ts timestamp, c1 int)")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct1 (ts timestamp, c1 int, c2 float)")
	require.NoError(t, err)

	err = exec(conn, "create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))")
	require.NoError(t, err)

	//create topic
	err = exec(conn, "create topic if not exists tmq_test_db_multi_insert_topic as DATABASE tmq_test_db_multi_insert")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop topic if exists tmq_test_db_multi_insert_topic")
		require.NoError(t, err)
	}()
	{
		err = exec(conn, "insert into ct0 values(now,1) ct1 values(now,1,2) ct2 values(now,1,2,'3')")
		require.NoError(t, err)
	}
	//build consumer
	conf := TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	TMQConfSet(conf, "auto.offset.reset", "earliest")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "tmq_test_db_multi_insert_topic")

	//sync_consume_loop
	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	errCode, list := TMQSubscription(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	defer TMQListDestroy(list)
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"tmq_test_db_multi_insert_topic"}, r)
	totalCount := 0
	var tables [][]string
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "tmq_test_db_multi_insert_topic", topic)
			var table []string
			for {
				blockSize, errCode, block := TaosFetchRawBlock(message)
				if errCode != int(errors.SUCCESS) {
					errStr := TaosErrorStr(message)
					err := errors.NewError(errCode, errStr)
					t.Error(err)
					TaosFreeResult(message)
					return
				}
				if blockSize == 0 {
					break
				}
				tableName := TMQGetTableName(message)
				table = append(table, tableName)
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				totalCount += blockSize
				data, err := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
				assert.NoError(t, err)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
				assert.Equal(t, int32(0), d.ErrCode)
				PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				t.Error("wait tmq commit callback timeout")
				return
			}
			tables = append(tables, table)
		}
	}

	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	assert.GreaterOrEqual(t, totalCount, 3)
	t.Log(tables)
}

// @author: xftan
// @date: 2023/10/13 11:34
// @description: tmq test modify meta
func TestTMQModify(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists tmq_test_db_modify")
		require.NoError(t, err)
		err = exec(conn, "drop database if exists tmq_test_db_modify_target")
		require.NoError(t, err)
	}()

	err = exec(conn, "drop database if exists tmq_test_db_modify_target")
	require.NoError(t, err)

	err = exec(conn, "drop database if exists tmq_test_db_modify")
	require.NoError(t, err)
	err = exec(conn, "create database if not exists tmq_test_db_modify_target vgroups 2 WAL_RETENTION_PERIOD 86400")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "tmq_test_db_modify_target"))

	err = exec(conn, "create database if not exists tmq_test_db_modify vgroups 5 WAL_RETENTION_PERIOD 86400")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "tmq_test_db_modify"))
	err = exec(conn, "use tmq_test_db_modify")
	require.NoError(t, err)

	//create topic
	err = exec(conn, "create topic if not exists tmq_test_db_modify_topic with meta as DATABASE tmq_test_db_modify")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop topic if exists tmq_test_db_modify_topic")
		require.NoError(t, err)
	}()
	//build consumer
	conf := TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	TMQConfSet(conf, "auto.offset.reset", "earliest")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "tmq_test_db_modify_topic")

	//sync_consume_loop
	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	errCode, list := TMQSubscription(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	defer TMQListDestroy(list)
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"tmq_test_db_modify_topic"}, r)
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	targetConn, err := TaosConnect("", "root", "taosdata", "tmq_test_db_modify_target", 0)
	assert.NoError(t, err)
	defer TaosClose(targetConn)
	err = exec(conn, "create table stb (ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20)"+
		")"+
		"tags(tts timestamp,"+
		"tc1 bool,"+
		"tc2 tinyint,"+
		"tc3 smallint,"+
		"tc4 int,"+
		"tc5 bigint,"+
		"tc6 tinyint unsigned,"+
		"tc7 smallint unsigned,"+
		"tc8 int unsigned,"+
		"tc9 bigint unsigned,"+
		"tc10 float,"+
		"tc11 double,"+
		"tc12 binary(20),"+
		"tc13 nchar(20)"+
		")")
	require.NoError(t, err)
	pool := func(cb func(*tmqcommon.Meta, unsafe.Pointer)) {
		message := TMQConsumerPoll(tmq, 500)
		assert.NotNil(t, message)
		topic := TMQGetTopicName(message)
		assert.Equal(t, "tmq_test_db_modify_topic", topic)
		messageType := TMQGetResType(message)
		assert.Equal(t, int32(common.TMQ_RES_TABLE_META), messageType)
		pointer := TMQGetJsonMeta(message)
		assert.NotNil(t, pointer)
		data := ParseJsonMeta(pointer)
		TMQFreeJsonMeta(pointer)
		var meta tmqcommon.Meta
		err = jsoniter.Unmarshal(data, &meta)
		assert.NoError(t, err)

		defer TaosFreeResult(message)

		TMQCommitAsync(tmq, nil, h2)
		timer := time.NewTimer(time.Minute)
		select {
		case d := <-c2:
			assert.Equal(t, int32(0), d.ErrCode)
			PutTMQCommitCallbackResult(d)
			timer.Stop()
			break
		case <-timer.C:
			timer.Stop()
			t.Error("wait tmq commit callback timeout")
			cb(nil, nil)
			return
		}
		errCode, rawMeta := TMQGetRaw(message)
		if errCode != errors.SUCCESS {
			errStr := TaosErrorStr(message)
			t.Error(errors.NewError(int(errCode), errStr))
			return
		}
		cb(&meta, rawMeta)
		TMQFreeRaw(rawMeta)
	}

	pool(func(meta *tmqcommon.Meta, rawMeta unsafe.Pointer) {
		assert.Equal(t, "create", meta.Type)
		assert.Equal(t, "stb", meta.TableName)
		assert.Equal(t, "super", meta.TableType)
		assert.NoError(t, err)
		length, metaType, data := ParseRawMeta(rawMeta)
		errCode = TMQWriteRaw(targetConn, length, metaType, data)
		if errCode != 0 {
			errStr := TMQErr2Str(errCode)
			t.Error(errors.NewError(int(errCode), errStr))
			return
		}
		d, err := query(targetConn, "describe stb")
		assert.NoError(t, err)
		expect := [][]driver.Value{
			{"ts", "TIMESTAMP", int32(8), ""},
			{"c1", "BOOL", int32(1), ""},
			{"c2", "TINYINT", int32(1), ""},
			{"c3", "SMALLINT", int32(2), ""},
			{"c4", "INT", int32(4), ""},
			{"c5", "BIGINT", int32(8), ""},
			{"c6", "TINYINT UNSIGNED", int32(1), ""},
			{"c7", "SMALLINT UNSIGNED", int32(2), ""},
			{"c8", "INT UNSIGNED", int32(4), ""},
			{"c9", "BIGINT UNSIGNED", int32(8), ""},
			{"c10", "FLOAT", int32(4), ""},
			{"c11", "DOUBLE", int32(8), ""},
			{"c12", "VARCHAR", int32(20), ""},
			{"c13", "NCHAR", int32(20), ""},
			{"tts", "TIMESTAMP", int32(8), "TAG"},
			{"tc1", "BOOL", int32(1), "TAG"},
			{"tc2", "TINYINT", int32(1), "TAG"},
			{"tc3", "SMALLINT", int32(2), "TAG"},
			{"tc4", "INT", int32(4), "TAG"},
			{"tc5", "BIGINT", int32(8), "TAG"},
			{"tc6", "TINYINT UNSIGNED", int32(1), "TAG"},
			{"tc7", "SMALLINT UNSIGNED", int32(2), "TAG"},
			{"tc8", "INT UNSIGNED", int32(4), "TAG"},
			{"tc9", "BIGINT UNSIGNED", int32(8), "TAG"},
			{"tc10", "FLOAT", int32(4), "TAG"},
			{"tc11", "DOUBLE", int32(8), "TAG"},
			{"tc12", "VARCHAR", int32(20), "TAG"},
			{"tc13", "NCHAR", int32(20), "TAG"},
		}
		for rowIndex, values := range d {
			for i := 0; i < 4; i++ {
				assert.Equal(t, expect[rowIndex][i], values[i])
			}
		}
	})

	TMQUnsubscribe(tmq)
	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
}

// @author: xftan
// @date: 2023/10/13 11:34
// @description: test tmq subscribe with auto create table
func TestTMQAutoCreateTable(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists tmq_test_auto_create")
		require.NoError(t, err)
	}()
	err = exec(conn, "create database if not exists tmq_test_auto_create vgroups 2 WAL_RETENTION_PERIOD 86400")
	require.NoError(t, err)
	assert.NoError(t, ensureDBCreated(conn, "tmq_test_auto_create"))
	err = exec(conn, "use tmq_test_auto_create")
	require.NoError(t, err)

	err = exec(conn, "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
	require.NoError(t, err)

	//create topic
	err = exec(conn, "create topic if not exists test_tmq_auto_topic with meta as DATABASE tmq_test_auto_create")
	require.NoError(t, err)
	defer func() {
		err = exec(conn, "drop topic if exists test_tmq_auto_topic")
		require.NoError(t, err)
	}()
	err = exec(conn, "insert into ct1 using st1 tags(2000) values(now,1,2,'1')")
	require.NoError(t, err)
	//build consumer
	conf := TMQConfNew()
	// auto commit default is true then the commitCallback function will be called after 5 seconds
	TMQConfSet(conf, "enable.auto.commit", "true")
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "msg.with.table.name", "true")
	TMQConfSet(conf, "auto.offset.reset", "earliest")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Error(err)
	}
	TMQConfDestroy(conf)
	//build_topic_list
	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "test_tmq_auto_topic")

	//sync_consume_loop
	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	errCode, list := TMQSubscription(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	defer TMQListDestroy(list)
	size := TMQListGetSize(list)
	r := TMQListToCArray(list, int(size))
	assert.Equal(t, []string{"test_tmq_auto_topic"}, r)
	totalCount := 0
	c2 := make(chan *TMQCommitCallbackResult, 1)
	h2 := cgo.NewHandle(c2)
	for i := 0; i < 5; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			t.Log(message)
			topic := TMQGetTopicName(message)
			assert.Equal(t, "test_tmq_auto_topic", topic)
			messageType := TMQGetResType(message)
			if messageType != common.TMQ_RES_METADATA {
				continue
			}
			pointer := TMQGetJsonMeta(message)
			data := ParseJsonMeta(pointer)
			TMQFreeJsonMeta(pointer)
			t.Log(string(data))
			var meta tmqcommon.Meta
			err = jsoniter.Unmarshal(data, &meta)
			assert.NoError(t, err)
			assert.Equal(t, "create", meta.Type)
			for {
				blockSize, errCode, block := TaosFetchRawBlock(message)
				if errCode != int(errors.SUCCESS) {
					errStr := TaosErrorStr(message)
					err := errors.NewError(errCode, errStr)
					t.Error(err)
					TaosFreeResult(message)
					return
				}
				if blockSize == 0 {
					break
				}
				tableName := TMQGetTableName(message)
				assert.Equal(t, "ct1", tableName)
				filedCount := TaosNumFields(message)
				rh, err := ReadColumn(message, filedCount)
				if err != nil {
					t.Error(err)
					return
				}
				precision := TaosResultPrecision(message)
				totalCount += blockSize
				data, err := parser.ReadBlock(block, blockSize, rh.ColTypes, precision)
				assert.NoError(t, err)
				t.Log(data)
			}
			TaosFreeResult(message)

			TMQCommitAsync(tmq, nil, h2)
			timer := time.NewTimer(time.Minute)
			select {
			case d := <-c2:
				assert.Nil(t, d.GetError())
				assert.Equal(t, int32(0), d.ErrCode)
				PutTMQCommitCallbackResult(d)
				timer.Stop()
				break
			case <-timer.C:
				timer.Stop()
				t.Error("wait tmq commit callback timeout")
				return
			}
		}
	}

	errCode = TMQConsumerClose(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
	assert.GreaterOrEqual(t, totalCount, 1)
}

// @author: xftan
// @date: 2023/10/13 11:35
// @description: test tmq get assignment
func TestTMQGetTopicAssignment(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer TaosClose(conn)

	defer func() {
		if err = exec(conn, "drop database if exists test_tmq_get_topic_assignment"); err != nil {
			t.Error(err)
		}
	}()

	if err = exec(conn, "create database if not exists test_tmq_get_topic_assignment vgroups 1 WAL_RETENTION_PERIOD 86400"); err != nil {
		t.Fatal(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_tmq_get_topic_assignment"))
	if err = exec(conn, "use test_tmq_get_topic_assignment"); err != nil {
		t.Fatal(err)
		return
	}
	if err = exec(conn, "create table if not exists t (ts timestamp,v int)"); err != nil {
		t.Fatal(err)
		return
	}

	// create topic
	if err = exec(conn, "create topic if not exists test_tmq_assignment as select * from t"); err != nil {
		t.Fatal(err)
		return
	}

	defer func() {
		if err = exec(conn, "drop topic if exists test_tmq_assignment"); err != nil {
			t.Error(err)
		}
	}()

	conf := TMQConfNew()
	defer TMQConfDestroy(conf)
	TMQConfSet(conf, "group.id", "tg2")
	TMQConfSet(conf, "auto.offset.reset", "earliest")
	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer TMQConsumerClose(tmq)

	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "test_tmq_assignment")

	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Fatal(errors.NewError(int(errCode), errStr))
		return
	}

	code, assignment := TMQGetTopicAssignment(tmq, "test_tmq_assignment")
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	assert.Equal(t, 1, len(assignment))
	assert.Equal(t, int64(0), assignment[0].Begin)
	assert.Equal(t, int64(0), assignment[0].Offset)
	assert.GreaterOrEqual(t, assignment[0].End, assignment[0].Offset)
	end := assignment[0].End
	vgID, vgCode := TaosGetTableVgID(conn, "test_tmq_get_topic_assignment", "t")
	if vgCode != 0 {
		t.Fatal(errors.NewError(int(vgCode), TMQErr2Str(code)))
	}
	assert.Equal(t, int32(vgID), assignment[0].VGroupID)

	_ = exec(conn, "insert into t values(now,1)")
	haveMessage := false
	for i := 0; i < 3; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			haveMessage = true
			TMQCommitSync(tmq, message)
			TaosFreeResult(message)
			break
		}
	}
	assert.True(t, haveMessage, "expect have message")
	code, assignment = TMQGetTopicAssignment(tmq, "test_tmq_assignment")
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	assert.Equal(t, 1, len(assignment))
	assert.Equal(t, int64(0), assignment[0].Begin)
	assert.GreaterOrEqual(t, assignment[0].End, end)
	end = assignment[0].End
	assert.Equal(t, int32(vgID), assignment[0].VGroupID)

	//seek
	code = TMQOffsetSeek(tmq, "test_tmq_assignment", int32(vgID), 0)
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	code, assignment = TMQGetTopicAssignment(tmq, "test_tmq_assignment")
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	assert.Equal(t, 1, len(assignment))
	assert.Equal(t, int64(0), assignment[0].Begin)
	assert.Equal(t, int64(0), assignment[0].Offset)
	assert.GreaterOrEqual(t, assignment[0].End, end)
	end = assignment[0].End
	assert.Equal(t, int32(vgID), assignment[0].VGroupID)

	haveMessage = false
	for i := 0; i < 3; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			haveMessage = true
			TMQCommitSync(tmq, message)
			TaosFreeResult(message)
			break
		}
	}
	assert.True(t, haveMessage, "expect have message")
	code, assignment = TMQGetTopicAssignment(tmq, "test_tmq_assignment")
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	assert.Equal(t, 1, len(assignment))
	assert.Equal(t, int64(0), assignment[0].Begin)
	assert.GreaterOrEqual(t, assignment[0].End, end)
	end = assignment[0].End
	assert.Equal(t, int32(vgID), assignment[0].VGroupID)

	// seek twice
	code = TMQOffsetSeek(tmq, "test_tmq_assignment", int32(vgID), 1)
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	code, assignment = TMQGetTopicAssignment(tmq, "test_tmq_assignment")
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	assert.Equal(t, 1, len(assignment))
	assert.Equal(t, int64(0), assignment[0].Begin)
	assert.GreaterOrEqual(t, assignment[0].End, end)
	end = assignment[0].End
	assert.Equal(t, int32(vgID), assignment[0].VGroupID)

	haveMessage = false
	for i := 0; i < 3; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			haveMessage = true
			offset := TMQGetVgroupOffset(message)
			assert.Greater(t, offset, int64(0))
			TMQCommitSync(tmq, message)
			TaosFreeResult(message)
			break
		}
	}
	assert.True(t, haveMessage, "expect have message")
	code, assignment = TMQGetTopicAssignment(tmq, "test_tmq_assignment")
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	assert.Equal(t, 1, len(assignment))
	assert.Equal(t, int64(0), assignment[0].Begin)
	assert.GreaterOrEqual(t, assignment[0].End, end)
	assert.Equal(t, int32(vgID), assignment[0].VGroupID)
}

func TestTMQPosition(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer TaosClose(conn)

	defer func() {
		if err = exec(conn, "drop database if exists test_tmq_position"); err != nil {
			t.Error(err)
		}
	}()

	if err = exec(conn, "create database if not exists test_tmq_position vgroups 1 WAL_RETENTION_PERIOD 86400"); err != nil {
		t.Fatal(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_tmq_position"))
	if err = exec(conn, "use test_tmq_position"); err != nil {
		t.Fatal(err)
		return
	}
	if err = exec(conn, "create table if not exists t (ts timestamp,v int)"); err != nil {
		t.Fatal(err)
		return
	}

	// create topic
	if err = exec(conn, "create topic if not exists test_tmq_position_topic as select * from t"); err != nil {
		t.Fatal(err)
		return
	}

	defer func() {
		if err = exec(conn, "drop topic if exists test_tmq_position_topic"); err != nil {
			t.Error(err)
		}
	}()

	conf := TMQConfNew()
	defer TMQConfDestroy(conf)
	TMQConfSet(conf, "group.id", "position")
	TMQConfSet(conf, "auto.offset.reset", "earliest")

	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer TMQConsumerClose(tmq)

	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "test_tmq_position_topic")

	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Fatal(errors.NewError(int(errCode), errStr))
		return
	}
	_ = exec(conn, "insert into t values(now,1)")
	code, assignment := TMQGetTopicAssignment(tmq, "test_tmq_position_topic")
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	vgID := assignment[0].VGroupID
	position := TMQPosition(tmq, "test_tmq_position_topic", vgID)
	assert.Equal(t, position, int64(0))
	haveMessage := false
	for i := 0; i < 3; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			haveMessage = true
			position := TMQPosition(tmq, "test_tmq_position_topic", vgID)
			assert.Greater(t, position, int64(0))
			committed := TMQCommitted(tmq, "test_tmq_position_topic", vgID)
			assert.Less(t, committed, int64(0))
			TMQCommitSync(tmq, message)
			position = TMQPosition(tmq, "test_tmq_position_topic", vgID)
			committed = TMQCommitted(tmq, "test_tmq_position_topic", vgID)
			assert.Equal(t, position, committed)
			TaosFreeResult(message)
			break
		}
	}
	assert.True(t, haveMessage, "expect have message")
	errCode = TMQUnsubscribe(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
}

func TestTMQCommitOffset(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer TaosClose(conn)

	defer func() {
		if err = exec(conn, "drop database if exists test_tmq_commit_offset"); err != nil {
			t.Error(err)
		}
	}()

	if err = exec(conn, "create database if not exists test_tmq_commit_offset vgroups 1 WAL_RETENTION_PERIOD 86400"); err != nil {
		t.Fatal(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_tmq_commit_offset"))
	if err = exec(conn, "use test_tmq_commit_offset"); err != nil {
		t.Fatal(err)
		return
	}
	if err = exec(conn, "create table if not exists t (ts timestamp,v int)"); err != nil {
		t.Fatal(err)
		return
	}

	// create topic
	if err = exec(conn, "create topic if not exists test_tmq_commit_offset_topic as select * from t"); err != nil {
		t.Fatal(err)
		return
	}

	defer func() {
		if err = exec(conn, "drop topic if exists test_tmq_commit_offset_topic"); err != nil {
			t.Error(err)
		}
	}()

	conf := TMQConfNew()
	defer TMQConfDestroy(conf)
	TMQConfSet(conf, "group.id", "commit")
	TMQConfSet(conf, "auto.offset.reset", "earliest")

	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer TMQConsumerClose(tmq)

	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, "test_tmq_commit_offset_topic")

	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Fatal(errors.NewError(int(errCode), errStr))
		return
	}
	_ = exec(conn, "insert into t values(now,1)")
	code, assignment := TMQGetTopicAssignment(tmq, "test_tmq_commit_offset_topic")
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	vgID := assignment[0].VGroupID
	haveMessage := false
	for i := 0; i < 3; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			haveMessage = true
			position := TMQPosition(tmq, "test_tmq_commit_offset_topic", vgID)
			assert.Greater(t, position, int64(0))
			committed := TMQCommitted(tmq, "test_tmq_commit_offset_topic", vgID)
			assert.Less(t, committed, int64(0))
			offset := TMQGetVgroupOffset(message)
			code = TMQCommitOffsetSync(tmq, "test_tmq_commit_offset_topic", vgID, offset)
			if code != 0 {
				t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
			}
			committed = TMQCommitted(tmq, "test_tmq_commit_offset_topic", vgID)
			assert.Equal(t, int64(offset), committed)
			TaosFreeResult(message)
			break
		}
	}
	assert.True(t, haveMessage, "expect have message")
	errCode = TMQUnsubscribe(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
}

func TestTMQCommitOffsetAsync(t *testing.T) {
	topic := "test_tmq_commit_offset_a_topic"
	databaseName := "test_tmq_commit_offset_a"
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer TaosClose(conn)

	defer func() {
		if err = exec(conn, "drop database if exists "+databaseName); err != nil {
			t.Error(err)
		}
	}()

	if err = exec(conn, "create database if not exists "+databaseName+" vgroups 1 WAL_RETENTION_PERIOD 86400"); err != nil {
		t.Fatal(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, databaseName))
	if err = exec(conn, "use "+databaseName); err != nil {
		t.Fatal(err)
		return
	}
	if err = exec(conn, "create table if not exists t (ts timestamp,v int)"); err != nil {
		t.Fatal(err)
		return
	}

	// create topic

	if err = exec(conn, "create topic if not exists "+topic+" as select * from t"); err != nil {
		t.Fatal(err)
		return
	}

	defer func() {
		if err = exec(conn, "drop topic if exists "+topic); err != nil {
			t.Error(err)
		}
	}()

	conf := TMQConfNew()
	defer TMQConfDestroy(conf)
	TMQConfSet(conf, "group.id", "commit_a")
	TMQConfSet(conf, "auto.offset.reset", "earliest")

	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer TMQConsumerClose(tmq)

	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, topic)

	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Fatal(errors.NewError(int(errCode), errStr))
		return
	}
	_ = exec(conn, "insert into t values(now,1)")
	code, assignment := TMQGetTopicAssignment(tmq, topic)
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	vgID := assignment[0].VGroupID
	haveMessage := false
	for i := 0; i < 3; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			haveMessage = true
			position := TMQPosition(tmq, topic, vgID)
			assert.Greater(t, position, int64(0))
			committed := TMQCommitted(tmq, topic, vgID)
			assert.Less(t, committed, int64(0))
			offset := TMQGetVgroupOffset(message)
			c := make(chan *TMQCommitCallbackResult, 1)
			handler := cgo.NewHandle(c)
			TMQCommitOffsetAsync(tmq, topic, vgID, offset, handler)
			timer := time.NewTimer(time.Second * 5)
			select {
			case r := <-c:
				code = r.ErrCode
				if code != 0 {
					t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
				}
				timer.Stop()
			case <-timer.C:
				t.Fatal("commit async timeout")
				timer.Stop()
			}
			committed = TMQCommitted(tmq, topic, vgID)
			assert.Equal(t, int64(offset), committed)
			TaosFreeResult(message)
			break
		}
	}
	assert.True(t, haveMessage, "expect have message")
	errCode = TMQUnsubscribe(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
}

func TestTMQCommitAsyncCallback(t *testing.T) {
	topic := "test_tmq_commit_a_cb_topic"
	databaseName := "test_tmq_commit_a_cb"
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer TaosClose(conn)

	defer func() {
		if err = exec(conn, "drop database if exists "+databaseName); err != nil {
			t.Error(err)
		}
	}()

	if err = exec(conn, "create database if not exists "+databaseName+" vgroups 1 WAL_RETENTION_PERIOD 86400"); err != nil {
		t.Fatal(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, databaseName))
	if err = exec(conn, "use "+databaseName); err != nil {
		t.Fatal(err)
		return
	}
	if err = exec(conn, "create table if not exists t (ts timestamp,v int)"); err != nil {
		t.Fatal(err)
		return
	}

	// create topic

	if err = exec(conn, "create topic if not exists "+topic+" as select * from t"); err != nil {
		t.Fatal(err)
		return
	}

	defer func() {
		if err = exec(conn, "drop topic if exists "+topic); err != nil {
			t.Error(err)
		}
	}()

	conf := TMQConfNew()
	defer TMQConfDestroy(conf)
	TMQConfSet(conf, "group.id", "commit_a")
	TMQConfSet(conf, "enable.auto.commit", "false")
	TMQConfSet(conf, "auto.offset.reset", "earliest")
	TMQConfSet(conf, "auto.commit.interval.ms", "100")
	c := make(chan *TMQCommitCallbackResult, 1)
	h := cgo.NewHandle(c)
	TMQConfSetAutoCommitCB(conf, h)
	go func() {
		for r := range c {
			t.Log("auto commit", r)
			PutTMQCommitCallbackResult(r)
		}
	}()

	tmq, err := TMQConsumerNew(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer TMQConsumerClose(tmq)

	topicList := TMQListNew()
	defer TMQListDestroy(topicList)
	TMQListAppend(topicList, topic)

	errCode := TMQSubscribe(tmq, topicList)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Fatal(errors.NewError(int(errCode), errStr))
		return
	}
	_ = exec(conn, "insert into t values(now,1)")
	code, assignment := TMQGetTopicAssignment(tmq, topic)
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	vgID := assignment[0].VGroupID
	haveMessage := false
	for i := 0; i < 3; i++ {
		message := TMQConsumerPoll(tmq, 500)
		if message != nil {
			haveMessage = true
			position := TMQPosition(tmq, topic, vgID)
			assert.Greater(t, position, int64(0))
			committed := TMQCommitted(tmq, topic, vgID)
			assert.Less(t, committed, int64(0))
			offset := TMQGetVgroupOffset(message)
			TMQCommitOffsetSync(tmq, topic, vgID, offset)
			committed = TMQCommitted(tmq, topic, vgID)
			assert.Equal(t, offset, committed)
			TaosFreeResult(message)
		}
	}
	assert.True(t, haveMessage, "expect have message")
	committed := TMQCommitted(tmq, topic, vgID)
	t.Log(committed)
	code, assignment = TMQGetTopicAssignment(tmq, topic)
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	t.Log(assignment[0].Offset)
	TMQCommitOffsetSync(tmq, topic, vgID, 1)
	committed = TMQCommitted(tmq, topic, vgID)
	assert.Equal(t, int64(1), committed)
	code, assignment = TMQGetTopicAssignment(tmq, topic)
	if code != 0 {
		t.Fatal(errors.NewError(int(code), TMQErr2Str(code)))
	}
	t.Log(assignment[0].Offset)
	position := TMQPosition(tmq, topic, vgID)
	t.Log(position)
	errCode = TMQUnsubscribe(tmq)
	if errCode != 0 {
		errStr := TMQErr2Str(errCode)
		t.Error(errors.NewError(int(errCode), errStr))
		return
	}
}
