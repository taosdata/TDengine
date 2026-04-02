package tmq

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/tmq"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func TestCloudTmq(t *testing.T) {
	topic := "go_tmq_test_topic"
	db := "go_test"
	endPoint := os.Getenv("TDENGINE_CLOUD_ENDPOINT")
	token := os.Getenv("TDENGINE_CLOUD_TOKEN")
	if endPoint == "" || token == "" {
		t.Skip("TDENGINE_CLOUD_TOKEN or TDENGINE_CLOUD_ENDPOINT is not set, skip cloud test")
		return
	}
	now := time.Now()
	url := fmt.Sprintf("wss://%s?token=%s", endPoint, token)
	groupId := fmt.Sprintf("test_%d", now.UnixNano())
	clientId := fmt.Sprintf("test_consumer_%d", now.UnixNano())
	consumer, err := NewConsumer(&tmq.ConfigMap{
		"ws.url":                  url,
		"ws.message.channelLen":   uint(0),
		"ws.message.timeout":      common.DefaultMessageTimeout,
		"ws.message.writeWait":    common.DefaultWriteWait,
		"group.id":                groupId,
		"client.id":               clientId,
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
		for i := 0; i < 20; i++ {
			time.Sleep(time.Second * 1)
			err = cloudDoRequest(endPoint, token, db, fmt.Sprintf("DROP CONSUMER GROUP IF EXISTS FORCE %s on %s", groupId, topic))
			if err != nil {
				t.Log(err)
				continue
			}
			break
		}
		if err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		err = consumer.Close()
		if err != nil {
			t.Error(err)
		}
	}()
	topics := []string{topic}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		t.Error(err)
		return
	}
	gotData := false
	for i := 0; i < 5; i++ {
		if gotData {
			return
		}
		ev := consumer.Poll(500)
		if ev != nil {
			switch e := ev.(type) {
			case *tmq.DataMessage:
				gotData = true
				data := e.Value().([]*tmq.Data)
				assert.Equal(t, db, e.DBName())
				assert.Equal(t, "tmq_sub_table", data[0].TableName)
				t.Log(e.Value())
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

		}
		t.Log(i)
		err = cloudDoRequest(endPoint, token, db, "insert into tmq_sub_table using tmq_stb tags(1) values(now,1)")
		assert.NoError(t, err)
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

func cloudDoRequest(endpoint, token, db string, payload string) error {
	body := strings.NewReader(payload)
	url := fmt.Sprintf("https://%s/rest/sql/%s?token=%s", endpoint, db, token)
	req, _ := http.NewRequest(http.MethodPost, url, body)
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
		return taosErrors.NewError(int(code), desc)
	}
	return nil
}
