package tmq

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

// @author: xftan
// @date: 2023/10/13 11:11
// @description: test tmq
func TestTmq(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		wrapper.TaosClose(conn)
	}()
	sqls := []string{
		"drop topic if exists test_tmq_common",
		"drop database if exists af_test_tmq",
		"create database if not exists af_test_tmq vgroups 2  WAL_RETENTION_PERIOD 86400",
		"use af_test_tmq",
		"create stable if not exists all_type (ts timestamp," +
			"c1 bool," +
			"c2 tinyint," +
			"c3 smallint," +
			"c4 int," +
			"c5 bigint," +
			"c6 tinyint unsigned," +
			"c7 smallint unsigned," +
			"c8 int unsigned," +
			"c9 bigint unsigned," +
			"c10 float," +
			"c11 double," +
			"c12 binary(20)," +
			"c13 nchar(20)," +
			"c14 varbinary(20)," +
			"c15 geometry(100)," +
			"c16 decimal(20,4)," +
			"c17 blob" +
			") tags(t1 int)",
		"create table if not exists ct0 using all_type tags(1000)",
		"create table if not exists ct1 using all_type tags(2000)",
		"create table if not exists ct2 using all_type tags(3000)",
		"create topic if not exists test_tmq_common as select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17 from all_type",
	}

	defer func() {
		err = execWithoutResult(conn, "drop database if exists af_test_tmq")
		assert.NoError(t, err)
	}()
	for _, sql := range sqls {
		err = execWithoutResult(conn, sql)
		assert.NoError(t, err)
	}
	defer func() {
		err = execWithoutResult(conn, "drop topic if exists test_tmq_common")
		assert.NoError(t, err)
	}()
	now := time.Now()
	err = execWithoutResult(conn, fmt.Sprintf("insert into ct0 values('%s',true,2,3,4,5,6,7,8,9,10,11,'1','2','varbinary','POINT(100 100)',123456789.123,'blob')", now.Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	err = execWithoutResult(conn, fmt.Sprintf("insert into ct1 values('%s',true,2,3,4,5,6,7,8,9,10,11,'1','2','varbinary','POINT(100 100)',123456789.123,'blob')", now.Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	err = execWithoutResult(conn, fmt.Sprintf("insert into ct2 values('%s',true,2,3,4,5,6,7,8,9,10,11,'1','2','varbinary','POINT(100 100)',123456789.123,'blob')", now.Format(time.RFC3339Nano)))
	assert.NoError(t, err)

	consumer, err := NewConsumer(&tmq.ConfigMap{
		"group.id":            "test",
		"auto.offset.reset":   "earliest",
		"td.connect.ip":       "127.0.0.1",
		"td.connect.user":     "root",
		"td.connect.pass":     "taosdata",
		"td.connect.port":     "6030",
		"client.id":           "test_tmq_c",
		"enable.auto.commit":  "false",
		"msg.with.table.name": "true",
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = consumer.Subscribe("test_tmq_common", nil)
	if err != nil {
		t.Error(err)
		return
	}
	ass, err := consumer.Assignment()
	t.Log(ass)
	position, _ := consumer.Position(ass)
	t.Log(position)
	haveMessage := false
	for i := 0; i < 5; i++ {
		if haveMessage {
			break
		}
		ev := consumer.Poll(500)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *tmq.DataMessage:
			haveMessage = true
			row1 := e.Value().([]*tmq.Data)[0].Data[0]
			assert.Equal(t, "af_test_tmq", e.DBName())
			assert.Equal(t, now.UnixNano()/1e6, row1[0].(time.Time).UnixNano()/1e6)
			assert.Equal(t, true, row1[1].(bool))
			assert.Equal(t, int8(2), row1[2].(int8))
			assert.Equal(t, int16(3), row1[3].(int16))
			assert.Equal(t, int32(4), row1[4].(int32))
			assert.Equal(t, int64(5), row1[5].(int64))
			assert.Equal(t, uint8(6), row1[6].(uint8))
			assert.Equal(t, uint16(7), row1[7].(uint16))
			assert.Equal(t, uint32(8), row1[8].(uint32))
			assert.Equal(t, uint64(9), row1[9].(uint64))
			assert.Equal(t, float32(10), row1[10].(float32))
			assert.Equal(t, float64(11), row1[11].(float64))
			assert.Equal(t, "1", row1[12].(string))
			assert.Equal(t, "2", row1[13].(string))
			assert.Equal(t, []byte("varbinary"), row1[14].([]byte))
			assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, row1[15].([]byte))
			assert.Equal(t, "123456789.1230", row1[16].(string))
			assert.Equal(t, []byte("blob"), row1[17].([]byte))

			t.Log(e.Offset())
			ass, err := consumer.Assignment()
			assert.NoError(t, err)
			t.Log(ass)
			committed, err := consumer.Committed(ass, 0)
			assert.NoError(t, err)
			t.Log(committed)
			position, _ := consumer.Position(ass)
			t.Log(position)
			offsets, err := consumer.Position([]tmq.TopicPartition{e.TopicPartition})
			assert.NoError(t, err)
			_, err = consumer.CommitOffsets(offsets)
			assert.NoError(t, err)
			ass, err = consumer.Assignment()
			assert.NoError(t, err)
			t.Log(ass)
			committed, err = consumer.Committed(ass, 0)
			assert.NoError(t, err)
			t.Log(committed)
			position, _ = consumer.Position(ass)
			t.Log(position)
			err = consumer.Unsubscribe()
			assert.NoError(t, err)
			err = consumer.Close()
			assert.NoError(t, err)
		case tmq.Error:
			t.Error(e)
			return
		default:
			t.Error("unexpected", e)
			return
		}
	}
	assert.True(t, haveMessage)
}

// @author: xftan
// @date: 2023/10/13 11:11
// @description: test seek
func TestSeek(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	db := "af_test_tmq_seek"
	topic := "af_test_tmq_seek_topic"
	vgroups := 2
	record := 3
	sqls := []string{
		"drop topic if exists " + topic,
		"drop database if exists " + db,
		"create database if not exists " + db + " vgroups " + strconv.Itoa(vgroups) + "  WAL_RETENTION_PERIOD 86400",
		"use " + db,
		"create table stb(ts timestamp,v int) tags (n binary(10))",
		"create table ct0 using stb tags ('ct0')",
		"create table ct1 using stb tags ('ct1')",
		"create table ct3 using stb tags ('ct3')",
		"insert into ct0 values (now,0)",
		"insert into ct1 values (now,1)",
		"insert into ct3 values (now,2)",
		//"create topic " + topic + " as database " + db,
		//"create topic " + topic + " as select * from ct0 ",
		"create topic " + topic + " as select * from stb ",
	}

	defer func() {
		err = execWithoutResult(conn, "drop database if exists "+db)
		assert.NoError(t, err)
	}()
	for _, sql := range sqls {
		err = execWithoutResult(conn, sql)
		assert.NoError(t, err, sql)
	}
	defer func() {
		err = execWithoutResult(conn, "drop topic if exists "+topic)
		assert.NoError(t, err)
	}()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"group.id":            "test",
		"td.connect.ip":       "127.0.0.1",
		"td.connect.user":     "root",
		"td.connect.pass":     "taosdata",
		"td.connect.port":     "6030",
		"auto.offset.reset":   "earliest",
		"client.id":           "test_tmq_seek",
		"enable.auto.commit":  "false",
		"msg.with.table.name": "true",
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	assignment, err := consumer.Assignment()
	assert.NoError(t, err)
	assert.Equal(t, vgroups, len(assignment))
	for i := 0; i < len(assignment); i++ {
		assert.Equal(t, tmq.Offset(0), assignment[i].Offset)
		assert.Equal(t, topic, *assignment[i].Topic)
	}

	// poll
	dataCount := 0
	for i := 0; i < 20; i++ {
		if dataCount >= record {
			break
		}
		event := consumer.Poll(500)
		if event != nil {
			t.Log(event)
			data := event.(*tmq.DataMessage).Value().([]*tmq.Data)
			for _, datum := range data {
				dataCount += len(datum.Data)
			}
			time.Sleep(time.Second * 2)
			_, err = consumer.Commit()
			assert.NoError(t, err)
		}
	}
	assert.Equal(t, record, dataCount)

	//assignment after poll
	assignment, err = consumer.Assignment()
	t.Log(assignment)
	assert.NoError(t, err)
	assert.Equal(t, vgroups, len(assignment))
	for i := 0; i < len(assignment); i++ {
		assert.Equal(t, topic, *assignment[i].Topic)
	}

	// seek
	for i := 0; i < len(assignment); i++ {
		err = consumer.Seek(tmq.TopicPartition{
			Topic:     &topic,
			Partition: assignment[i].Partition,
			Offset:    0,
		}, 0)
		assert.NoError(t, err)
	}

	//assignment after seek
	assignment, err = consumer.Assignment()
	t.Log(assignment)
	assert.NoError(t, err)
	assert.Equal(t, vgroups, len(assignment))
	for i := 0; i < len(assignment); i++ {
		assert.Equal(t, tmq.Offset(0), assignment[i].Offset)
		assert.Equal(t, topic, *assignment[i].Topic)
	}

	//poll after seek
	dataCount = 0
	for i := 0; i < 20; i++ {
		if dataCount >= record {
			break
		}
		event := consumer.Poll(500)
		if event != nil {
			t.Log(event)
			data := event.(*tmq.DataMessage).Value().([]*tmq.Data)
			for _, datum := range data {
				dataCount += len(datum.Data)
			}
		}
		_, err = consumer.Commit()
		assert.NoError(t, err)
	}
	assert.Equal(t, record, dataCount)

	//assignment after poll
	assignment, err = consumer.Assignment()
	t.Log(assignment)
	assert.NoError(t, err)
	assert.Equal(t, vgroups, len(assignment))
	for i := 0; i < len(assignment); i++ {
		assert.Equal(t, topic, *assignment[i].Topic)
	}
	err = consumer.Close()
	assert.NoError(t, err)
}

func execWithoutResult(conn unsafe.Pointer, sql string) error {
	res := wrapper.TaosQuery(conn, sql)
	if code := wrapper.TaosError(res); code != 0 {
		if (code & 0xffff) == 0x3d3 {
			//Conflict transaction not completed, retry in 100ms
			wrapper.TaosFreeResult(res)
			time.Sleep(100 * time.Millisecond)
			return execWithoutResult(conn, sql)
		}
		errStr := wrapper.TaosErrorStr(res)
		wrapper.TaosFreeResult(res)
		return errors.NewError(code, errStr)
	}
	wrapper.TaosFreeResult(res)
	return nil
}

func prepareMultiBlockEnv(conn unsafe.Pointer) error {
	var err error
	steps := []string{
		"drop topic if exists test_tmq_multi_block_topic",
		"drop database if exists test_tmq_multi_block",
		"create database test_tmq_multi_block vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_tmq_multi_block_topic as database test_tmq_multi_block",
		"create table test_tmq_multi_block.t1(ts timestamp,v int)",
		"create table test_tmq_multi_block.t2(ts timestamp,v int)",
		"create table test_tmq_multi_block.t3(ts timestamp,v int)",
		"create table test_tmq_multi_block.t4(ts timestamp,v int)",
		"create table test_tmq_multi_block.t5(ts timestamp,v int)",
		"create table test_tmq_multi_block.t6(ts timestamp,v int)",
		"create table test_tmq_multi_block.t7(ts timestamp,v int)",
		"create table test_tmq_multi_block.t8(ts timestamp,v int)",
		"create table test_tmq_multi_block.t9(ts timestamp,v int)",
		"create table test_tmq_multi_block.t10(ts timestamp,v int)",
		"insert into test_tmq_multi_block.t1 values (now,1) test_tmq_multi_block.t2 values (now,2) " +
			"test_tmq_multi_block.t3 values (now,3) test_tmq_multi_block.t4 values (now,4)" +
			"test_tmq_multi_block.t5 values (now,5) test_tmq_multi_block.t6 values (now,6)" +
			"test_tmq_multi_block.t7 values (now,7) test_tmq_multi_block.t8 values (now,8)" +
			"test_tmq_multi_block.t9 values (now,9) test_tmq_multi_block.t10 values (now,10)",
	}
	for _, step := range steps {
		err = execWithoutResult(conn, step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanMultiBlockEnv(conn unsafe.Pointer) error {
	var err error
	time.Sleep(2 * time.Second)
	steps := []string{
		"drop topic if exists test_tmq_multi_block_topic",
		"drop database if exists test_tmq_multi_block",
	}
	for _, step := range steps {
		err = execWithoutResult(conn, step)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestMultiBlock(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	err = prepareMultiBlockEnv(conn)
	assert.NoError(t, err)
	defer func() {
		err = cleanMultiBlockEnv(conn)
		assert.NoError(t, err)
	}()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"group.id":            "test",
		"td.connect.ip":       "127.0.0.1",
		"td.connect.user":     "root",
		"td.connect.pass":     "taosdata",
		"td.connect.port":     "6030",
		"auto.offset.reset":   "earliest",
		"client.id":           "test_tmq_multi_block_topic",
		"enable.auto.commit":  "false",
		"msg.with.table.name": "true",
	})
	assert.NoError(t, err)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = consumer.Unsubscribe()
		assert.NoError(t, err)
		err = consumer.Close()
		assert.NoError(t, err)
	}()
	topic := []string{"test_tmq_multi_block_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		event := consumer.Poll(500)
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case *tmq.DataMessage:
			data := e.Value().([]*tmq.Data)
			assert.Equal(t, "test_tmq_multi_block", e.DBName())
			assert.Equal(t, 10, len(data))
			return
		}
	}
}

func prepareMetaEnv(conn unsafe.Pointer) error {
	var err error
	steps := []string{
		"drop topic if exists test_tmq_meta_topic",
		"drop database if exists test_tmq_meta",
		"create database test_tmq_meta vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_tmq_meta_topic with meta as database test_tmq_meta",
	}
	for _, step := range steps {
		err = execWithoutResult(conn, step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanMetaEnv(conn unsafe.Pointer) error {
	var err error
	time.Sleep(2 * time.Second)
	steps := []string{
		"drop topic if exists test_tmq_meta_topic",
		"drop database if exists test_tmq_meta",
	}
	for _, step := range steps {
		err = execWithoutResult(conn, step)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestMeta(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer wrapper.TaosClose(conn)
	err = prepareMetaEnv(conn)
	assert.NoError(t, err)
	defer func() {
		err = cleanMetaEnv(conn)
		assert.NoError(t, err)
	}()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"group.id":            "test",
		"td.connect.ip":       "127.0.0.1",
		"td.connect.user":     "root",
		"td.connect.pass":     "taosdata",
		"td.connect.port":     "6030",
		"auto.offset.reset":   "earliest",
		"client.id":           "test_tmq_multi_block_topic",
		"enable.auto.commit":  "false",
		"msg.with.table.name": "true",
	})
	err = consumer.Subscribe("test_tmq_meta_topic", nil)
	assert.NoError(t, err)
	defer func() {
		err = consumer.Unsubscribe()
		assert.NoError(t, err)
		err = consumer.Close()
		assert.NoError(t, err)
	}()
	go func() {
		err := execWithoutResult(conn, "create table test_tmq_meta.st(ts timestamp,v int) tags (cn binary(20))")
		assert.NoError(t, err)
		err = execWithoutResult(conn, "create table test_tmq_meta.t1 using test_tmq_meta.st tags ('t1')")
		assert.NoError(t, err)
		err = execWithoutResult(conn, "insert into test_tmq_meta.t1 values (now,1)")
		assert.NoError(t, err)
		err = execWithoutResult(conn, "insert into test_tmq_meta.t2 using test_tmq_meta.st tags ('t1') values (now,2)")
		assert.NoError(t, err)
		time.Sleep(time.Second)
		err = execWithoutResult(conn, "insert into test_tmq_meta.t1 values (now,1)")
		assert.NoError(t, err)
		err = execWithoutResult(conn, "insert into test_tmq_meta.t1 values (now,1)")
		assert.NoError(t, err)
	}()
	for i := 0; i < 10; i++ {
		event := consumer.Poll(500)
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case *tmq.DataMessage:
			t.Log(e)
			assert.Equal(t, "test_tmq_meta", e.DBName())
		case *tmq.MetaDataMessage:
			assert.Equal(t, "test_tmq_meta", e.DBName())
			assert.Equal(t, "test_tmq_meta_topic", e.Topic())
			t.Log(e)
		case *tmq.MetaMessage:
			assert.Equal(t, "test_tmq_meta", e.DBName())
			t.Log(e)
		}
	}
}

func Test_tmqError(t *testing.T) {
	err := tmqError(-1)
	expectError := &errors.TaosError{Code: 65535, ErrStr: "fail"}
	assert.Equal(t, expectError, err)
}

func prepareTimezoneEnv(conn unsafe.Pointer) error {
	var err error
	steps := []string{
		"drop topic if exists test_native_tmq_timezone_topic",
		"drop database if exists test_native_tmq_timezone",
		"create database test_native_tmq_timezone vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_native_tmq_timezone_topic as database test_native_tmq_timezone",
		"create table test_native_tmq_timezone.t1(ts timestamp,v int)",
	}
	for _, step := range steps {
		err = execWithoutResult(conn, step)
		if err != nil {
			return err
		}
	}
	return nil
}

func doClean(conn unsafe.Pointer, steps []string) error {
	for _, step := range steps {
		err := execWithoutResult(conn, step)
		if err != nil {
			return err
		}
	}
	return nil
}
func cleanTimezoneEnv(conn unsafe.Pointer) error {
	steps := []string{
		"drop topic if exists test_native_tmq_timezone_topic",
		"drop database if exists test_native_tmq_timezone",
	}
	var err error
	for i := 0; i < 10; i++ {
		err = doClean(conn, steps)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		} else {
			return nil
		}
	}
	return err
}

func TestTimezone(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer wrapper.TaosClose(conn)
	err = prepareTimezoneEnv(conn)
	assert.NoError(t, err)
	defer func() {
		err = cleanTimezoneEnv(conn)
		assert.NoError(t, err)
	}()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"group.id":                "test",
		"client.id":               "test_consumer",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "1000",
		"msg.with.table.name":     "true",
		"timezone":                "Europe/Paris",
	})
	assert.NoError(t, err)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = consumer.Unsubscribe()
		assert.NoError(t, err)
		err = consumer.Close()
		assert.NoError(t, err)
	}()
	topic := []string{"test_native_tmq_timezone_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	nowStr := now.Format(time.RFC3339Nano)
	err = execWithoutResult(conn, fmt.Sprintf("insert into test_native_tmq_timezone.t1 values('%s',1)", nowStr))
	require.NoError(t, err)
	parisTimezone, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		event := consumer.Poll(500)
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case *tmq.DataMessage:
			data := e.Value().([]*tmq.Data)
			assert.Equal(t, "test_native_tmq_timezone", e.DBName())
			assert.Equal(t, 1, len(data))
			assert.Equal(t, "t1", data[0].TableName)
			assert.Equal(t, 1, len(data[0].Data))
			assert.Equal(t, 2, len(data[0].Data[0]))
			recordTime := data[0].Data[0][0].(time.Time)
			assert.Equal(t, parisTimezone, recordTime.Location())
			assert.Equal(t, now.UnixNano()/1e6, recordTime.UnixNano()/1e6)
			assert.Equal(t, int32(1), data[0].Data[0][1].(int32))
			return
		}
	}
	t.Error("no message got")
}
