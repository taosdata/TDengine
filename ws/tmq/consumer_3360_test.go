package tmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/tmq"
)

func prepare3360Env() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_3360_topic",
		"drop database if exists test_ws_tmq_3360",
		"create database test_ws_tmq_3360 WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_3360_topic as database test_ws_tmq_3360",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func clean3360Env() error {
	var err error
	time.Sleep(2 * time.Second)
	steps := []string{
		"drop topic if exists test_ws_tmq_3360_topic",
		"drop database if exists test_ws_tmq_3360",
	}
	for i := 0; i < 10; i++ {
		time.Sleep(2 * time.Second)
		err = doClean(steps)
		if err != nil {
			continue
		} else {
			return nil
		}
	}
	return err
}

// @author: xftan
// @date: 2023/10/13 11:36
// @description: test tmq subscribe over websocket
func TestConsumer_3360(t *testing.T) {
	err := prepare3360Env()
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = clean3360Env()
		if err != nil {
			t.Error(err)
		}
	}()
	now := time.Now()

	err = doRequest("create table test_ws_tmq_3360.t_all(ts timestamp," +
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
		")")
	if err != nil {
		t.Error(err)
		return
	}
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:6041",
		"ws.message.channelLen":   uint(0),
		"ws.message.timeout":      common.DefaultMessageTimeout,
		"ws.message.writeWait":    common.DefaultWriteWait,
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"group.id":                "test",
		"client.id":               "test_consumer",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "5000",
		"msg.with.table.name":     "true",
		"session.timeout.ms":      "12000",
		"max.poll.interval.ms":    "300000",
		"min.poll.rows":           "1024",
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = consumer.Close()
		if err != nil {
			t.Error(err)
		}
	}()
	topic := []string{"test_ws_tmq_3360_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	gotData := false
	for i := 0; i < 5; i++ {
		if gotData {
			return
		}
		ev := consumer.Poll(100)
		if ev != nil {
			switch e := ev.(type) {
			case *tmq.DataMessage:
				gotData = true
				data := e.Value().([]*tmq.Data)
				assert.Equal(t, "test_ws_tmq_3360", e.DBName())
				assert.Equal(t, 1, len(data))
				assert.Equal(t, "t_all", data[0].TableName)
				assert.Equal(t, 1, len(data[0].Data))
				assert.Equal(t, now.Unix(), data[0].Data[0][0].(time.Time).Unix())
				var v = data[0].Data[0]
				assert.Equal(t, true, v[1].(bool))
				assert.Equal(t, int8(2), v[2].(int8))
				assert.Equal(t, int16(3), v[3].(int16))
				assert.Equal(t, int32(4), v[4].(int32))
				assert.Equal(t, int64(5), v[5].(int64))
				assert.Equal(t, uint8(6), v[6].(uint8))
				assert.Equal(t, uint16(7), v[7].(uint16))
				assert.Equal(t, uint32(8), v[8].(uint32))
				assert.Equal(t, uint64(9), v[9].(uint64))
				assert.Equal(t, float32(10.123), v[10].(float32))
				assert.Equal(t, float64(11.123), v[11].(float64))
				assert.Equal(t, "binary", v[12].(string))
				assert.Equal(t, "nchar", v[13].(string))
				assert.Equal(t, []byte("varbinary"), v[14].([]byte))
				assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, v[15].([]byte))
				assert.Equal(t, "123456789.1230", v[16].(string))
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
			case tmq.Error:
				t.Error(e)
				return
			default:
				t.Error("unexpected", e)
				return
			}

		} else {
			if i == 0 {
				err = doRequest(fmt.Sprintf("insert into test_ws_tmq_3360.t_all values('%s',true,2,3,4,5,6,7,8,9,10.123,11.123,'binary','nchar','varbinary','POINT(100 100)',123456789.123)", now.Format(time.RFC3339Nano)))
				if err != nil {
					t.Error(err)
					return
				}
			}
		}
	}
	if !gotData {
		t.Error("no data got")
	}
	err = consumer.Unsubscribe()
	if err != nil {
		t.Error(err)
		return
	}
}
