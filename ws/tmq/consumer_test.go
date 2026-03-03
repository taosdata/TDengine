package tmq

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/testtool"
	"github.com/taosdata/driver-go/v3/common/tmq"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func prepareEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_topic",
		"drop database if exists test_ws_tmq",
		"create database test_ws_tmq WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_topic as database test_ws_tmq",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanEnv() error {
	var err error
	time.Sleep(2 * time.Second)
	steps := []string{
		"drop topic if exists test_ws_tmq_topic",
		"drop database if exists test_ws_tmq",
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

func doRequest(payload string) error {
	body := strings.NewReader(payload)
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:6041/rest/sql", body)
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http code: %d", resp.StatusCode)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	iter := client.JsonI.BorrowIterator(data)
	code := int32(0)
	desc := ""
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, s string) bool {
		switch s {
		case "code":
			code = iter.ReadInt32()
		case "desc":
			desc = iter.ReadString()
		default:
			iter.Skip()
		}
		return iter.Error == nil
	})
	client.JsonI.ReturnIterator(iter)
	if code != 0 {
		if code == 0x3d3 {
			time.Sleep(100 * time.Millisecond)
			return doRequest(payload)
		}
		return taosErrors.NewError(int(code), desc)
	}
	return nil
}

// @author: xftan
// @date: 2023/10/13 11:36
// @description: test tmq subscribe over websocket
func TestConsumer(t *testing.T) {
	_, ok := os.LookupEnv("TD_3360_TEST")
	if ok {
		t.Skip("Skip 3.3.6.0 test")
	}
	err := prepareEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanEnv()
		if err != nil {
			t.Error(err)
		}
	}()
	now := time.Now()
	err = doRequest("create table test_ws_tmq.t_all(ts timestamp," +
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
	topic := []string{"test_ws_tmq_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	app := common.GetProcessName()
	if len(app) > 23 {
		app = app[:23]
	}
	connectorInfo := common.GetConnectorInfo("ws")
	checkSql := fmt.Sprintf("select count(*) from performance_schema.perf_connections where user_app = '%s'  and connector_info = '%s'", app, connectorInfo)
	t.Log(checkSql)
	require.Eventually(t, func() bool {
		resp, err := testtool.HTTPQuery(checkSql)
		if err != nil {
			return false
		}
		if len(resp.Data) == 0 || len(resp.Data[0]) == 0 {
			return false
		}
		count, ok := resp.Data[0][0].(int64)
		if !ok {
			return false
		}
		return count > 0
	}, 10*time.Second, 500*time.Millisecond)
	gotData := false
	for i := 0; i < 5; i++ {
		if gotData {
			return
		}
		ev := consumer.Poll(10)
		if ev != nil {
			switch e := ev.(type) {
			case *tmq.DataMessage:
				gotData = true
				data := e.Value().([]*tmq.Data)
				assert.Equal(t, "test_ws_tmq", e.DBName())
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
				assert.Equal(t, []byte("blob"), v[17].([]byte))
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
				err = doRequest(fmt.Sprintf("insert into test_ws_tmq.t_all values('%s',true,2,3,4,5,6,7,8,9,10.123,11.123,'binary','nchar','varbinary','POINT(100 100)',123456789.123,'blob')", now.Format(time.RFC3339Nano)))
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

func prepareSeekEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_seek_topic",
		"drop database if exists test_ws_tmq_seek",
		"create database test_ws_tmq_seek vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_seek_topic as database test_ws_tmq_seek",
		"create table test_ws_tmq_seek.t1(ts timestamp,v int)",
		"insert into test_ws_tmq_seek.t1 values (now,1)",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanSeekEnv() error {
	steps := []string{
		"drop topic if exists test_ws_tmq_seek_topic",
		"drop database if exists test_ws_tmq_seek",
	}
	var err error
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
// @description: test tmq seek over websocket
func TestSeek(t *testing.T) {
	err := prepareSeekEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanSeekEnv()
		if err != nil {
			t.Error(err)
		}
	}()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                       "ws://127.0.0.1:6041",
		"ws.message.channelLen":        uint(0),
		"ws.message.timeout":           common.DefaultMessageTimeout,
		"ws.message.writeWait":         common.DefaultWriteWait,
		"td.connect.user":              "root",
		"td.connect.pass":              "taosdata",
		"group.id":                     "test",
		"client.id":                    "test_consumer",
		"auto.offset.reset":            "earliest",
		"enable.auto.commit":           "false",
		"msg.with.table.name":          "true",
		"ws.message.enableCompression": true,
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
	topic := []string{"test_ws_tmq_seek_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	partitions, err := consumer.Assignment()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_seek_topic", *partitions[0].Topic)
	assert.Equal(t, tmq.Offset(0), partitions[0].Offset)

	//poll
	messageOffset := tmq.Offset(0)
	haveMessage := false
	for i := 0; i < 5; i++ {
		event := consumer.Poll(500)
		if event != nil {
			haveMessage = true
			_, err = consumer.Commit()
			assert.NoError(t, err)
			messageOffset = event.(*tmq.DataMessage).Offset()
		}
	}
	assert.True(t, haveMessage)
	partitions, err = consumer.Assignment()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_seek_topic", *partitions[0].Topic)
	assert.GreaterOrEqual(t, partitions[0].Offset, messageOffset)

	//seek
	tmpTopic := "test_ws_tmq_seek_topic"
	err = consumer.Seek(tmq.TopicPartition{
		Topic:     &tmpTopic,
		Partition: partitions[0].Partition,
		Offset:    0,
	}, 0)
	assert.NoError(t, err)

	//assignment
	partitions, err = consumer.Assignment()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_seek_topic", *partitions[0].Topic)
	assert.Equal(t, tmq.Offset(0), partitions[0].Offset)

	//poll
	messageOffset = tmq.Offset(0)
	haveMessage = false
	for i := 0; i < 5; i++ {
		event := consumer.Poll(500)
		if event != nil {
			haveMessage = true
			messageOffset = event.(*tmq.DataMessage).Offset()
			_, err = consumer.Commit()
			assert.NoError(t, err)
		}
	}
	partitions, err = consumer.Assignment()
	assert.True(t, haveMessage)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_seek_topic", *partitions[0].Topic)
	assert.GreaterOrEqual(t, partitions[0].Offset, messageOffset)
}

func prepareAutocommitEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_autocommit_topic",
		"drop database if exists test_ws_tmq_autocommit",
		"create database test_ws_tmq_autocommit vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_autocommit_topic as database test_ws_tmq_autocommit",
		"create table test_ws_tmq_autocommit.t1(ts timestamp,v int)",
		"insert into test_ws_tmq_autocommit.t1 values (now,1)",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanAutocommitEnv() error {
	steps := []string{
		"drop topic if exists test_ws_tmq_autocommit_topic",
		"drop database if exists test_ws_tmq_autocommit",
	}
	var err error
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

func TestAutoCommit(t *testing.T) {
	err := prepareAutocommitEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanAutocommitEnv()
		if err != nil {
			t.Error(err)
		}
	}()
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
		"auto.commit.interval.ms": "1000",
		"msg.with.table.name":     "true",
	})
	assert.NoError(t, err)
	defer func() {
		err = consumer.Unsubscribe()
		assert.NoError(t, err)
		err = consumer.Close()
		assert.NoError(t, err)
	}()
	topic := []string{"test_ws_tmq_autocommit_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	partitions, err := consumer.Assignment()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))
	assert.Equal(t, "test_ws_tmq_autocommit_topic", *partitions[0].Topic)
	assert.Equal(t, tmq.Offset(0), partitions[0].Offset)

	offset, err := consumer.Committed(partitions, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(offset))
	assert.Equal(t, tmq.OffsetInvalid, offset[0].Offset)

	//poll
	messageOffset := tmq.Offset(0)
	haveMessage := false
	for i := 0; i < 5; i++ {
		event := consumer.Poll(500)
		if event != nil {
			haveMessage = true
			messageOffset = event.(*tmq.DataMessage).Offset()
		}
	}
	assert.True(t, haveMessage)

	offset, err = consumer.Committed(partitions, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(offset))
	assert.GreaterOrEqual(t, offset[0].Offset, messageOffset)
}

func prepareMultiBlockEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_multi_block_topic",
		"drop database if exists test_ws_tmq_multi_block",
		"create database test_ws_tmq_multi_block vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_multi_block_topic as database test_ws_tmq_multi_block",
		"create table test_ws_tmq_multi_block.t1(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t2(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t3(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t4(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t5(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t6(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t7(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t8(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t9(ts timestamp,v int)",
		"create table test_ws_tmq_multi_block.t10(ts timestamp,v int)",
		"insert into test_ws_tmq_multi_block.t1 values (now,1) test_ws_tmq_multi_block.t2 values (now,2) " +
			"test_ws_tmq_multi_block.t3 values (now,3) test_ws_tmq_multi_block.t4 values (now,4)" +
			"test_ws_tmq_multi_block.t5 values (now,5) test_ws_tmq_multi_block.t6 values (now,6)" +
			"test_ws_tmq_multi_block.t7 values (now,7) test_ws_tmq_multi_block.t8 values (now,8)" +
			"test_ws_tmq_multi_block.t9 values (now,9) test_ws_tmq_multi_block.t10 values (now,10)",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanMultiBlockEnv() error {
	steps := []string{
		"drop topic if exists test_ws_tmq_multi_block_topic",
		"drop database if exists test_ws_tmq_multi_block",
	}
	var err error
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

func TestMultiBlock(t *testing.T) {
	err := prepareMultiBlockEnv()
	assert.NoError(t, err)
	defer func() {
		err = cleanMultiBlockEnv()
		assert.NoError(t, err)
	}()
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
		"auto.commit.interval.ms": "1000",
		"msg.with.table.name":     "true",
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
	topic := []string{"test_ws_tmq_multi_block_topic"}
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
			assert.Equal(t, "test_ws_tmq_multi_block", e.DBName())
			assert.Equal(t, 10, len(data))
			return
		}
	}
}

func Test_configMapToConfigWrong(t *testing.T) {
	type args struct {
		m tmq.ConfigMap
	}
	tests := []struct {
		name    string
		args    args
		wantErr string
	}{
		{
			name: "url",
			args: args{
				m: tmq.ConfigMap{
					"ws.url": 123,
				},
			},
			wantErr: "ws.url expects type string, not int",
		},
		{
			name: "empty url",
			args: args{
				m: tmq.ConfigMap{
					"ws.url": "",
				},
			},
			wantErr: "ws.url required",
		},
		{
			name: "channelLen",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":                "ws://127.0.0.1:6041",
					"ws.message.channelLen": "not a uint",
				},
			},
			wantErr: "ws.message.channelLen expects type uint, not string",
		},
		{
			name: "ws.message.timeout",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":             "ws://127.0.0.1:6041",
					"ws.message.timeout": "xx",
				},
			},
			wantErr: "ws.message.timeout expects type time.Duration, not string",
		},
		{
			name: "ws.message.writeWait",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":               "ws://127.0.0.1:6041",
					"ws.message.writeWait": "xx",
				},
			},
			wantErr: "ws.message.writeWait expects type time.Duration, not string",
		},
		{
			name: "td.connect.user",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":          "ws://127.0.0.1:6041",
					"td.connect.user": 123,
				},
			},
			wantErr: "td.connect.user expects type string, not int",
		},
		{
			name: "td.connect.pass",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":          "ws://127.0.0.1:6041",
					"td.connect.pass": 123,
				},
			},
			wantErr: "td.connect.pass expects type string, not int",
		},
		{
			name: "group.id",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":   "ws://127.0.0.1:6041",
					"group.id": 123,
				},
			},
			wantErr: "group.id expects type string, not int",
		},
		{
			name: "client.id",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":    "ws://127.0.0.1:6041",
					"client.id": 123,
				},
			},
			wantErr: "client.id expects type string, not int",
		},
		{
			name: "auto.offset.reset",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":            "ws://127.0.0.1:6041",
					"auto.offset.reset": 123,
				},
			},
			wantErr: "auto.offset.reset expects type string, not int",
		},
		{
			name: "enable.auto.commit",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":             "ws://127.0.0.1:6041",
					"enable.auto.commit": 123,
				},
			},
			wantErr: "enable.auto.commit expects type string, not int",
		},
		{
			name: "auto.commit.interval.ms",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":                  "ws://127.0.0.1:6041",
					"auto.commit.interval.ms": 123,
				},
			},
			wantErr: "auto.commit.interval.ms expects type string, not int",
		},
		{
			name: "experimental.snapshot.enable",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":                       "ws://127.0.0.1:6041",
					"experimental.snapshot.enable": 123,
				},
			},
			wantErr: "experimental.snapshot.enable expects type string, not int",
		},
		{
			name: "msg.with.table.name",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":              "ws://127.0.0.1:6041",
					"msg.with.table.name": 123,
				},
			},
			wantErr: "msg.with.table.name expects type string, not int",
		},
		{
			name: "ws.message.enableCompression",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":                       "ws://127.0.0.1:6041",
					"ws.message.enableCompression": 123,
				},
			},
			wantErr: "ws.message.enableCompression expects type bool, not int",
		},
		{
			name: "ws.message.timeout < 1s",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":             "ws://127.0.0.1:6041",
					"ws.message.timeout": time.Millisecond,
				},
			},
			wantErr: "ws.message.timeout cannot be less than 1 second",
		},
		{
			name: "ws.message.writeWait < 1s",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":               "ws://127.0.0.1:6041",
					"ws.message.writeWait": time.Millisecond,
				},
			},
			wantErr: "ws.message.writeWait cannot be less than 1 second",
		},
		{
			name: "ws.autoReconnect",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":           "ws://127.0.0.1:6041",
					"ws.autoReconnect": 123,
				},
			},
			wantErr: "ws.autoReconnect expects type bool, not int",
		},
		//ws.reconnectIntervalMs
		{
			name: "ws.reconnectIntervalMs",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":                 "ws://127.0.0.1:6041",
					"ws.reconnectIntervalMs": "not int",
				},
			},
			wantErr: "ws.reconnectIntervalMs expects type int, not string",
		},
		//ws.reconnectRetryCount
		{
			name: "ws.reconnectRetryCount",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":                 "ws://127.0.0.1:6041",
					"ws.reconnectRetryCount": "not int",
				},
			},
			wantErr: "ws.reconnectRetryCount expects type int, not string",
		},
		{
			name: "session.timeout.ms",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":             "ws://127.0.0.1:6041",
					"session.timeout.ms": 123,
				},
			},
			wantErr: "session.timeout.ms expects type string, not int",
		},
		{
			name: "max.poll.interval.ms",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":               "ws://127.0.0.1:6041",
					"max.poll.interval.ms": 123,
				},
			},
			wantErr: "max.poll.interval.ms expects type string, not int",
		},
		{
			name: "expect string value",
			args: args{
				m: tmq.ConfigMap{
					"ws.url":        "ws://127.0.0.1:6041",
					"min.poll.rows": 123,
				},
			},
			wantErr: "config min.poll.rows value must be string",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := configMapToConfig(tt.args.m)
			assert.Nil(t, got)
			assert.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func prepareMetaEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_meta_topic",
		"drop database if exists test_ws_tmq_meta",
		"create database test_ws_tmq_meta vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_meta_topic with meta as database test_ws_tmq_meta",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanMetaEnv() error {
	steps := []string{
		"drop topic if exists test_ws_tmq_meta_topic",
		"drop database if exists test_ws_tmq_meta",
	}
	var err error
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

func TestMeta(t *testing.T) {
	err := prepareMetaEnv()
	assert.NoError(t, err)
	defer func() {
		err = cleanMetaEnv()
		assert.NoError(t, err)
	}()
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
		"auto.commit.interval.ms": "1000",
		"msg.with.table.name":     "true",
	})
	err = consumer.Subscribe("test_ws_tmq_meta_topic", nil)
	assert.NoError(t, err)
	defer func() {
		err = consumer.Unsubscribe()
		assert.NoError(t, err)
		err = consumer.Close()
		assert.NoError(t, err)
	}()
	go func() {
		err = doRequest("create table test_ws_tmq_meta.st(ts timestamp,v int) tags (cn binary(20))")
		assert.NoError(t, err)
		err = doRequest("create table test_ws_tmq_meta.t1 using test_ws_tmq_meta.st tags ('t1')")
		assert.NoError(t, err)
		err = doRequest("insert into test_ws_tmq_meta.t1 values (now,1)")
		assert.NoError(t, err)
		err = doRequest("insert into test_ws_tmq_meta.t2 using test_ws_tmq_meta.st tags ('t1') values (now,2)")
		assert.NoError(t, err)
		time.Sleep(time.Second)
		err = doRequest("insert into test_ws_tmq_meta.t1 values (now,1)")
		assert.NoError(t, err)
		err = doRequest("insert into test_ws_tmq_meta.t1 values (now,1)")
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
			assert.Equal(t, "test_ws_tmq_meta", e.DBName())
		case *tmq.MetaDataMessage:
			assert.Equal(t, "test_ws_tmq_meta", e.DBName())
			assert.Equal(t, "test_ws_tmq_meta_topic", e.Topic())
			t.Log(e)
		case *tmq.MetaMessage:
			assert.Equal(t, "test_ws_tmq_meta", e.DBName())
			t.Log(e)
		}
	}
}

func newTaosadapter(port string) *exec.Cmd {
	command := "taosadapter"
	if runtime.GOOS == "windows" {
		command = "C:\\TDengine\\taosadapter.exe"

	}
	return exec.Command(command, "--port", port)
}

func startTaosadapter(cmd *exec.Cmd, port string) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		time.Sleep(time.Second)
		return nil
	}
	return errors.New("taosadapter start failed")
}

func stopTaosadapter(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGINT)
	_, _ = cmd.Process.Wait()
	cmd.Process = nil
}

func prepareSubReconnectEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_sub_reconnect_topic",
		"drop database if exists test_ws_tmq_sub_reconnect",
		"create database test_ws_tmq_sub_reconnect vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_sub_reconnect_topic as database test_ws_tmq_sub_reconnect",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanSubReconnectEnv() error {
	steps := []string{
		"drop topic if exists test_ws_tmq_sub_reconnect_topic",
		"drop database if exists test_ws_tmq_sub_reconnect",
	}
	var err error
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

func doClean(steps []string) error {
	for _, step := range steps {
		err := doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestSubscribeReconnect(t *testing.T) {
	port := "36043"
	cmd := newTaosadapter(port)
	err := startTaosadapter(cmd, port)
	assert.NoError(t, err)
	defer func() {
		stopTaosadapter(cmd)
	}()
	err = prepareSubReconnectEnv()
	assert.NoError(t, err)
	defer func() {
		err = cleanSubReconnectEnv()
		assert.NoError(t, err)
	}()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:" + port,
		"ws.message.channelLen":   uint(0),
		"ws.message.timeout":      time.Second * 5,
		"ws.message.writeWait":    common.DefaultWriteWait,
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"group.id":                "test",
		"client.id":               "test_consumer",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "1000",
		"msg.with.table.name":     "true",
		"ws.autoReconnect":        true,
		"ws.reconnectIntervalMs":  3000,
		"ws.reconnectRetryCount":  3,
	})
	assert.NoError(t, err)
	stopTaosadapter(cmd)
	time.Sleep(time.Second)
	startChan := make(chan struct{})
	go func() {
		time.Sleep(time.Second * 3)
		cmd = newTaosadapter(port)
		err = startTaosadapter(cmd, port)
		if err != nil {
			t.Error(err)
			return
		}
		startChan <- struct{}{}
	}()
	err = consumer.Subscribe("test_ws_tmq_sub_reconnect_topic", nil)
	assert.Error(t, err)
	<-startChan
	time.Sleep(time.Second)
	err = consumer.Subscribe("test_ws_tmq_sub_reconnect_topic", nil)
	assert.NoError(t, err)
	defer func() {
		err = consumer.Unsubscribe()
		assert.NoError(t, err)
		err = consumer.Close()
		assert.NoError(t, err)
	}()
	err = doRequest("create table test_ws_tmq_sub_reconnect.st(ts timestamp,v int) tags (cn binary(20))")
	assert.NoError(t, err)
	err = doRequest("create table test_ws_tmq_sub_reconnect.t1 using test_ws_tmq_sub_reconnect.st tags ('t1')")
	assert.NoError(t, err)
	err = doRequest("insert into test_ws_tmq_sub_reconnect.t1 values (now,1)")
	assert.NoError(t, err)
	stopTaosadapter(cmd)
	go func() {
		defer func() {
			startChan <- struct{}{}
		}()
		time.Sleep(time.Second * 3)
		cmd = newTaosadapter(port)
		err = startTaosadapter(cmd, port)
		if err != nil {
			t.Errorf("start taosadapter failed: %v", err)
			return
		}
	}()
	time.Sleep(time.Second)
	event := consumer.Poll(500)
	assert.NotNil(t, event)
	_, ok := event.(tmq.Error)
	assert.True(t, ok)
	<-startChan
	haveMessage := false
	for i := 0; i < 10; i++ {
		event := consumer.Poll(500)
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case *tmq.DataMessage:
			t.Log(e)
			assert.Equal(t, "test_ws_tmq_sub_reconnect", e.DBName())
			haveMessage = true
		default:
			t.Log(e)
		}
	}
	assert.True(t, haveMessage)
}

func TestWSError_Error(t *testing.T) {
	// Test scenario where an error is provided
	expectedErr := errors.New("connection lost")
	wsErr := &WSError{err: expectedErr}

	// Call the Error() method and check if the format is correct
	actualError := wsErr.Error()

	// The expected error string format
	expectedError := "websocket close with error connection lost"

	// Assert that the error string is formatted correctly
	assert.Equal(t, expectedError, actualError, "Error string should match the expected format")

	// Test scenario where no error is provided (nil error)
	wsErrNil := &WSError{}
	actualErrorNil := wsErrNil.Error()

	// Expected format when error is nil (shouldn't panic)
	expectedErrorNil := "websocket close with error <nil>"

	// Assert that the error string handles nil properly
	assert.Equal(t, expectedErrorNil, actualErrorNil, "Error string should handle nil error correctly")
}

func TestWrongConsumer(t *testing.T) {
	consumer, err := NewConsumer(&tmq.ConfigMap{})
	assert.Error(t, err)
	assert.Nil(t, consumer)

	consumer, err = NewConsumer(&tmq.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:6041",
		"auto.commit.interval.ms": "abc",
	})
	assert.Error(t, err)
	assert.Nil(t, consumer)

	consumer, err = NewConsumer(&tmq.ConfigMap{
		"ws.url": ":xxx",
	})
	assert.Error(t, err)
	assert.Nil(t, consumer)

	consumer, err = NewConsumer(&tmq.ConfigMap{
		"ws.url": "ws://127.0.0.1:9999",
	})
	assert.Error(t, err)
	assert.Nil(t, consumer)

}

func preparePollMultiTimesEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_pool_multi_tmq_topic",
		"drop database if exists test_ws_pool_multi_tmq",
		"create database test_ws_pool_multi_tmq WAL_RETENTION_PERIOD 86400",
		"create table test_ws_pool_multi_tmq.t(ts timestamp,v int)",
		"create topic test_ws_pool_multi_tmq_topic as select * from test_ws_pool_multi_tmq.t",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanPollMultiTimesEnv() error {
	var err error
	time.Sleep(2 * time.Second)
	steps := []string{
		"drop topic if exists test_ws_pool_multi_tmq_topic",
		"drop database if exists test_ws_pool_multi_tmq",
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
// @date: 2025/6/19 9:28
// @description: TestPollMultiTimes tests the consumer's ability to poll multiple times without duplicating messages.
func TestPollMultiTimes(t *testing.T) {
	err := preparePollMultiTimesEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = cleanPollMultiTimesEnv()
		if err != nil {
			t.Error(err)
		}
	}()
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                  "ws://127.0.0.1:6041",
		"ws.message.channelLen":   uint(0),
		"ws.message.timeout":      common.DefaultMessageTimeout,
		"ws.message.writeWait":    common.DefaultWriteWait,
		"td.connect.user":         "root",
		"td.connect.pass":         "taosdata",
		"group.id":                "test",
		"client.id":               "test_consumer",
		"auto.offset.reset":       "latest",
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
	topic := []string{"test_ws_pool_multi_tmq_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	messageCount := 0
	now := time.Now().Unix() * 1000
	insertIdx := int64(0)
	for i := 0; i < 5; i++ {
		ev := consumer.Poll(500)
		if ev != nil {
			switch e := ev.(type) {
			case *tmq.DataMessage:
				messageCount += 1
				data := e.Value().([]*tmq.Data)
				assert.Equal(t, "test_ws_pool_multi_tmq", e.DBName())
				assert.Equal(t, 1, len(data))
				assert.Equal(t, 1, len(data[0].Data))
				// Check if the timestamp is correct
				assert.Equal(t, (now+insertIdx*1000)/1000, data[0].Data[0][0].(time.Time).Unix())
				// Check if the value is correct
				var v = data[0].Data[0]
				assert.Equal(t, int32(insertIdx), v[1].(int32))
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
				position, err = consumer.Position(ass)
				assert.NoError(t, err)
				t.Log(position)
				// insert next data
				insertIdx += 1
				err = doRequest(fmt.Sprintf("insert into test_ws_pool_multi_tmq.t values(%d,%d)", now+insertIdx*1000, insertIdx))
				assert.NoError(t, err)
			case tmq.Error:
				t.Error(e)
				return
			default:
				t.Error("unexpected", e)
				return
			}
		} else {
			if i == 0 {
				// insert first data
				err = doRequest(fmt.Sprintf("insert into test_ws_pool_multi_tmq.t values(%d,%d)", now+insertIdx*1000, insertIdx))
				assert.NoError(t, err)
			} else {
				t.Error("expect got data")
				return
			}
		}
	}
	assert.Equal(t, messageCount, 4)
	err = consumer.Unsubscribe()
	if err != nil {
		t.Error(err)
		return
	}
}

func prepareTimezoneEnv() error {
	var err error
	steps := []string{
		"drop topic if exists test_ws_tmq_timezone_topic",
		"drop database if exists test_ws_tmq_timezone",
		"create database test_ws_tmq_timezone vgroups 1 WAL_RETENTION_PERIOD 86400",
		"create topic test_ws_tmq_timezone_topic as database test_ws_tmq_timezone",
		"create table test_ws_tmq_timezone.t1(ts timestamp,v int)",
	}
	for _, step := range steps {
		err = doRequest(step)
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanTimezoneEnv() error {
	steps := []string{
		"drop topic if exists test_ws_tmq_timezone_topic",
		"drop database if exists test_ws_tmq_timezone",
	}
	var err error
	for i := 0; i < 10; i++ {
		err = doClean(steps)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		} else {
			return nil
		}
	}
	return err
}

func TestTimezone(t *testing.T) {
	err := prepareTimezoneEnv()
	assert.NoError(t, err)
	defer func() {
		err = cleanTimezoneEnv()
		assert.NoError(t, err)
	}()
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
	topic := []string{"test_ws_tmq_timezone_topic"}
	err = consumer.SubscribeTopics(topic, nil)
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	nowStr := now.Format(time.RFC3339Nano)
	err = doRequest(fmt.Sprintf("insert into test_ws_tmq_timezone.t1 values('%s',1)", nowStr))
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
			assert.Equal(t, "test_ws_tmq_timezone", e.DBName())
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
