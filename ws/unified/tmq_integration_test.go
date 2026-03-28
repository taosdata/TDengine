package unified

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
)

type restSQLResp struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

func tmqIntegrationSQL(t *testing.T, sql string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:6041/rest/sql", strings.NewReader(sql))
	require.NoError(t, err)
	req.SetBasicAuth("root", "taosdata")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("integration test requires taosadapter/taosd: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	var parsed restSQLResp
	require.NoError(t, json.Unmarshal(body, &parsed))
	if parsed.Code != 0 {
		t.Fatalf("sql failed: %s (code=%d desc=%s body=%s)", sql, parsed.Code, parsed.Desc, string(body))
	}
}

// TestTMQConsumerRealAdapterSmoke verifies the expected behavior for this scenario.
func TestTMQConsumerRealAdapterSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	db := fmt.Sprintf("unified_tmq_it_%d", time.Now().UnixNano())
	topic := fmt.Sprintf("%s_topic", db)
	groupID := fmt.Sprintf("%s_group", db)
	clientID := fmt.Sprintf("%s_client", db)

	tmqIntegrationSQL(t, "select 1")
	tmqIntegrationSQL(t, fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", db))
	tmqIntegrationSQL(t, fmt.Sprintf("create table if not exists %s.t(ts timestamp, v int)", db))
	tmqIntegrationSQL(t, fmt.Sprintf("create topic if not exists %s as select * from %s.t", topic, db))
	t.Cleanup(func() {
		tmqIntegrationSQL(t, fmt.Sprintf("drop topic if exists %s", topic))
		tmqIntegrationSQL(t, fmt.Sprintf("drop database if exists %s", db))
	})

	cfg := commontmq.ConfigMap{
		"ws.url":              "ws://127.0.0.1:6041",
		"td.connect.user":     "root",
		"td.connect.pass":     "taosdata",
		"group.id":            groupID,
		"client.id":           clientID,
		"auto.offset.reset":   "earliest",
		"enable.auto.commit":  "false",
		"msg.with.table.name": "true",
		"ws.autoReconnect":    true,
	}
	consumer, err := NewTMQConsumer(&cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = consumer.Unsubscribe()
		_ = consumer.Close()
	})
	require.NoError(t, consumer.Subscribe(topic, nil))

	tmqIntegrationSQL(t, fmt.Sprintf("insert into %s.t values(now, 1)", db))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		event := consumer.Poll(500)
		if event == nil {
			continue
		}
		if te, ok := event.(commontmq.Error); ok {
			t.Fatalf("unexpected tmq error event: %v", te)
		}
		if msg, ok := event.(*commontmq.DataMessage); ok {
			require.Equal(t, db, msg.DBName())
			require.Equal(t, topic, msg.Topic())
			return
		}
	}
	t.Fatal("did not receive data message from real taosadapter")
}

// TestTMQConsumerRealAdapterCommitAndErrorBranches verifies tmq e2e commit/offset and error branches.
func TestTMQConsumerRealAdapterCommitAndErrorBranches(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	db := fmt.Sprintf("unified_tmq_it_commit_%d", time.Now().UnixNano())
	topic := fmt.Sprintf("%s_topic", db)
	groupID := fmt.Sprintf("%s_group", db)
	clientID := fmt.Sprintf("%s_client", db)

	tmqIntegrationSQL(t, "select 1")
	tmqIntegrationSQL(t, fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", db))
	tmqIntegrationSQL(t, fmt.Sprintf("create table if not exists %s.t(ts timestamp, v int)", db))
	tmqIntegrationSQL(t, fmt.Sprintf("create topic if not exists %s as select * from %s.t", topic, db))
	t.Cleanup(func() {
		tmqIntegrationSQL(t, fmt.Sprintf("drop topic if exists %s", topic))
		tmqIntegrationSQL(t, fmt.Sprintf("drop database if exists %s", db))
	})

	cfg := commontmq.ConfigMap{
		"ws.url":                       "ws://127.0.0.1:6041",
		"td.connect.user":              "root",
		"td.connect.pass":              "taosdata",
		"group.id":                     groupID,
		"client.id":                    clientID,
		"auto.offset.reset":            "earliest",
		"enable.auto.commit":           "true",
		"auto.commit.interval.ms":      "1",
		"msg.with.table.name":          "true",
		"ws.autoReconnect":             false,
		"timezone":                     "Asia/Shanghai",
		"ws.reconnectRetryCount":       1,
		"ws.reconnectIntervalMs":       1,
		"session.timeout.ms":           "60000",
		"max.poll.interval.ms":         "300000",
		"experimental.snapshot.enable": "false",
	}
	consumer, err := NewTMQConsumer(&cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = consumer.Unsubscribe()
		_ = consumer.Close()
	})
	require.NoError(t, consumer.Subscribe(topic, nil))

	// Trigger auto-commit scheduler path at least once.
	_ = consumer.Poll(1)
	time.Sleep(2 * time.Millisecond)
	_ = consumer.Poll(1)

	tmqIntegrationSQL(t, fmt.Sprintf("insert into %s.t values(now, 100)", db))
	deadline := time.Now().Add(10 * time.Second)
	gotMessage := false
	for time.Now().Before(deadline) {
		event := consumer.Poll(200)
		if event == nil {
			continue
		}
		if te, ok := event.(commontmq.Error); ok {
			t.Fatalf("unexpected tmq error event: %v", te)
		}
		if msg, ok := event.(*commontmq.DataMessage); ok {
			require.Equal(t, db, msg.DBName())
			require.Equal(t, topic, msg.Topic())
			gotMessage = true
			break
		}
	}
	require.True(t, gotMessage, "did not receive data message")

	partitions, err := consumer.Assignment()
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	positions, err := consumer.Position(partitions)
	require.NoError(t, err)
	require.Len(t, positions, len(partitions))

	committed, err := consumer.Committed(partitions, 0)
	require.NoError(t, err)
	require.Len(t, committed, len(partitions))

	_, err = consumer.CommitOffsets(positions)
	require.NoError(t, err)

	err = consumer.Seek(partitions[0], 0)
	require.NoError(t, err)

	_, err = consumer.Commit()
	require.NoError(t, err)

	require.NoError(t, consumer.Unsubscribe())

	// Error branches: methods should return stored error.
	forcedErr := errors.New("forced tmq error")
	consumer.setErr(forcedErr)
	_, err = consumer.Commit()
	require.ErrorIs(t, err, forcedErr)
	_, err = consumer.CommitOffsets(partitions)
	require.ErrorIs(t, err, forcedErr)
	err = consumer.Seek(partitions[0], 0)
	require.ErrorIs(t, err, forcedErr)
	_, err = consumer.Assignment()
	require.ErrorIs(t, err, forcedErr)
	err = consumer.Unsubscribe()
	require.ErrorIs(t, err, forcedErr)

	event := consumer.Poll(1)
	te, ok := event.(commontmq.Error)
	require.True(t, ok, "poll should return tmq.Error when consumer has stored err")
	require.Equal(t, commontmq.ErrorOther, te.Code())
}
