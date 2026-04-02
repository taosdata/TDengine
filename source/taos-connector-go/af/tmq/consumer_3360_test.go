package tmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func TestTmq_3360(t *testing.T) {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		wrapper.TaosClose(conn)
	}()
	topic := "test_tmq_common_3360"
	database := "af_test_tmq_3360"
	sqls := []string{
		fmt.Sprintf("drop topic if exists %s", topic),
		fmt.Sprintf("drop database if exists %s", database),
		fmt.Sprintf("create database if not exists %s vgroups 2  WAL_RETENTION_PERIOD 86400", database),
		fmt.Sprintf("use %s", database),
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
			"c16 decimal(20,4)" +
			") tags(t1 int)",
		"create table if not exists ct0 using all_type tags(1000)",
		"create table if not exists ct1 using all_type tags(2000)",
		"create table if not exists ct2 using all_type tags(3000)",
		fmt.Sprintf("create topic if not exists %s "+
			"as select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16 from all_type", topic),
	}

	defer func() {
		err = execWithoutResult(conn, fmt.Sprintf("drop database if exists %s", database))
		assert.NoError(t, err)
	}()
	for _, sql := range sqls {
		err = execWithoutResult(conn, sql)
		assert.NoError(t, err)
	}
	defer func() {
		err = execWithoutResult(conn, fmt.Sprintf("drop topic if exists %s", topic))
		assert.NoError(t, err)
	}()
	now := time.Now()
	err = execWithoutResult(conn, fmt.Sprintf("insert into ct0 values('%s',true,2,3,4,5,6,7,8,9,10,11,'1','2','varbinary','POINT(100 100)',123456789.123)", now.Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	err = execWithoutResult(conn, fmt.Sprintf("insert into ct1 values('%s',true,2,3,4,5,6,7,8,9,10,11,'1','2','varbinary','POINT(100 100)',123456789.123)", now.Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	err = execWithoutResult(conn, fmt.Sprintf("insert into ct2 values('%s',true,2,3,4,5,6,7,8,9,10,11,'1','2','varbinary','POINT(100 100)',123456789.123)", now.Format(time.RFC3339Nano)))
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
	err = consumer.Subscribe(topic, nil)
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
			assert.Equal(t, database, e.DBName())
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
