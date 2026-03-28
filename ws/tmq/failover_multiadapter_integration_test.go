package tmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
)

func TestConsumerUnifiedMultiEndpointFailoverRealAdapter(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTMQTaosadapterBinary(t)

	ports, running := startTMQAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			stopTMQAdapterByPort(ports[i], running)
		}
	})

	db, table, topic := setupTMQFailoverEnv(t, ports)

	consumer := newTMQFailoverConsumer(t, ports)
	t.Cleanup(func() {
		_ = consumer.Unsubscribe()
		_ = consumer.Close()
	})
	require.NoError(t, consumer.Subscribe(topic, nil))

	require.NoError(t, execTMQSQLOnAnyPort(ports, fmt.Sprintf("insert into %s.%s values(now, 1)", db, table)))
	_, err := waitForTMQDataValue(consumer, 1, 10*time.Second)
	require.NoError(t, err)
	_, err = consumer.Commit()
	require.NoError(t, err)

	// Stop the first endpoint and rely on unified failover for continued consumption.
	stopTMQAdapterByPort(ports[0], running)

	require.NoError(t, execTMQSQLOnAnyPort(ports, fmt.Sprintf("insert into %s.%s values(now, 2)", db, table)))
	msg, err := waitForTMQDataValue(consumer, 2, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, db, msg.DBName())
	require.Equal(t, topic, msg.Topic())
}

func ensureTMQTaosadapterBinary(t *testing.T) {
	t.Helper()
	command := "taosadapter"
	if runtime.GOOS == "windows" {
		command = "C:\\TDengine\\taosadapter.exe"
	}
	if _, err := exec.LookPath(command); err != nil {
		t.Skipf("taosadapter not found: %v", err)
	}
}

func startTMQAdapters(t *testing.T, n int) ([]string, map[string]*exec.Cmd) {
	t.Helper()
	ports := make([]string, 0, n)
	running := make(map[string]*exec.Cmd, n)
	for i := 0; i < n; i++ {
		port, cmd := startTaosadapterOnFreePort(t)
		ports = append(ports, port)
		running[port] = cmd
	}
	return ports, running
}

func stopTMQAdapterByPort(port string, running map[string]*exec.Cmd) {
	cmd, ok := running[port]
	if !ok {
		return
	}
	stopTaosadapter(cmd)
	delete(running, port)
}

func setupTMQFailoverEnv(t *testing.T, ports []string) (db string, table string, topic string) {
	t.Helper()
	db = "test_ws_tmq_wrapper_cross"
	table = fmt.Sprintf("tmq_wrapper_t_%d", time.Now().UnixNano())
	topic = fmt.Sprintf("tmq_wrapper_topic_%d", time.Now().UnixNano())

	err := execTMQSQLOnAnyPort(ports, fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", db))
	require.NoError(t, err)
	require.NoError(t, execTMQSQLOnAnyPort(ports, fmt.Sprintf("create table if not exists %s.%s(ts timestamp, v int)", db, table)))
	createTopicSQL := fmt.Sprintf("create topic if not exists %s as select * from %s.%s", topic, db, table)
	err = execTMQSQLOnAnyPort(ports, createTopicSQL)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "topic num out of range") {
		cleanupTMQWrapperStaleTopics(ports)
		err = execTMQSQLOnAnyPort(ports, createTopicSQL)
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = execTMQSQLOnAnyPort(ports, fmt.Sprintf("drop topic if exists %s", topic))
		_ = execTMQSQLOnAnyPort(ports, fmt.Sprintf("drop table if exists %s.%s", db, table))
		_ = execTMQSQLOnAnyPort(ports, fmt.Sprintf("drop database if exists %s", db))
	})
	return db, table, topic
}

func newTMQFailoverConsumer(t *testing.T, ports []string) *Consumer {
	t.Helper()
	endpoints := make([]string, 0, len(ports))
	for i := 0; i < len(ports); i++ {
		endpoints = append(endpoints, fmt.Sprintf("ws://127.0.0.1:%s", ports[i]))
	}
	cfg := commontmq.ConfigMap{
		"ws.url":                 strings.Join(endpoints, ","),
		"td.connect.user":        "root",
		"td.connect.pass":        "taosdata",
		"group.id":               fmt.Sprintf("ws_tmq_wrapper_group_%d", time.Now().UnixNano()),
		"client.id":              fmt.Sprintf("ws_tmq_wrapper_client_%d", time.Now().UnixNano()),
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     "false",
		"msg.with.table.name":    "true",
		"ws.message.timeout":     3 * time.Second,
		"ws.message.writeWait":   3 * time.Second,
		"ws.autoReconnect":       true,
		"ws.reconnectIntervalMs": 50,
		"ws.reconnectRetryCount": 60,
	}
	consumer, err := NewConsumer(&cfg)
	require.NoError(t, err)
	return consumer
}

type tmqSQLResp struct {
	Code int             `json:"code"`
	Desc string          `json:"desc"`
	Data [][]interface{} `json:"data"`
}

func execTMQSQLOnAnyPort(ports []string, sql string) error {
	var lastErr error
	for i := 0; i < len(ports); i++ {
		err := execTMQSQLOnPort(ports[i], sql)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return errors.New("no available taosadapter port")
	}
	return lastErr
}

func execTMQSQLOnPort(port string, sql string) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/rest/sql", port), strings.NewReader(sql))
	if err != nil {
		return err
	}
	req.SetBasicAuth("root", "taosdata")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http status %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var parsed tmqSQLResp
	if err = json.Unmarshal(body, &parsed); err != nil {
		return err
	}
	if parsed.Code != 0 {
		return fmt.Errorf("sql failed: code=%d desc=%s sql=%s", parsed.Code, parsed.Desc, sql)
	}
	return nil
}

type tmqEventPoller interface {
	Poll(timeoutMs int) commontmq.Event
}

func waitForTMQDataValue(poller tmqEventPoller, want int32, timeout time.Duration) (*commontmq.DataMessage, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		event := poller.Poll(500)
		if event == nil {
			continue
		}
		switch ev := event.(type) {
		case commontmq.Error:
			lastErr = ev
		case *commontmq.DataMessage:
			if containsTMQValue(ev.Value().([]*commontmq.Data), want) {
				return ev, nil
			}
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("timeout waiting tmq value %d", want)
}

func containsTMQValue(data []*commontmq.Data, want int32) bool {
	for i := 0; i < len(data); i++ {
		for j := 0; j < len(data[i].Data); j++ {
			row := data[i].Data[j]
			if len(row) < 2 {
				continue
			}
			v, ok := row[1].(int32)
			if ok && v == want {
				return true
			}
		}
	}
	return false
}

func cleanupTMQWrapperStaleTopics(ports []string) {
	resp, err := queryTMQSQLOnAnyPort(ports, "show topics")
	if err != nil || resp == nil || resp.Code != 0 {
		return
	}
	for i := 0; i < len(resp.Data); i++ {
		if len(resp.Data[i]) == 0 || resp.Data[i][0] == nil {
			continue
		}
		name, ok := resp.Data[i][0].(string)
		if !ok {
			continue
		}
		if (strings.HasPrefix(name, "tmq_cross_") && strings.HasSuffix(name, "_topic")) ||
			strings.HasPrefix(name, "tmq_cross_topic_") ||
			strings.HasPrefix(name, "tmq_wrapper_topic_") {
			_ = execTMQSQLOnAnyPort(ports, fmt.Sprintf("drop topic if exists %s", name))
		}
	}
}

func queryTMQSQLOnAnyPort(ports []string, sql string) (*tmqSQLResp, error) {
	var lastErr error
	for i := 0; i < len(ports); i++ {
		resp, err := queryTMQSQLOnPort(ports[i], sql)
		if err == nil {
			return resp, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return nil, errors.New("no available taosadapter port")
	}
	return nil, lastErr
}

func queryTMQSQLOnPort(port, sql string) (*tmqSQLResp, error) {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/rest/sql", port), strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("root", "taosdata")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var parsed tmqSQLResp
	if err = json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	if parsed.Code != 0 {
		return nil, fmt.Errorf("sql failed: code=%d desc=%s sql=%s", parsed.Code, parsed.Desc, sql)
	}
	return &parsed, nil
}
