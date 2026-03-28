package tmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
	wsunified "github.com/taosdata/driver-go/v3/ws/unified"
)

func TestConsumerContractParityWithUnifiedRealAdapter(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTMQTaosadapterBinary(t)

	port := getAvailablePort(t)
	cmd := newTaosadapter(port)
	require.NoError(t, startTaosadapter(cmd, port))
	t.Cleanup(func() {
		stopTaosadapter(cmd)
	})

	ports := []string{port}
	db, table, topic := setupTMQFailoverEnv(t, ports)

	wrapperCfg := newTMQRealAdapterConfig(port, "wrapper", "wrapper")
	wrapper, err := NewConsumer(&wrapperCfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = wrapper.Unsubscribe()
		_ = wrapper.Close()
	})

	unifiedCfg := newTMQRealAdapterConfig(port, "unified", "unified")
	unifiedConsumer, err := wsunified.NewTMQConsumer(&unifiedCfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = unifiedConsumer.Unsubscribe()
		_ = unifiedConsumer.Close()
	})

	require.NoError(t, wrapper.Subscribe(topic, nil))
	require.NoError(t, unifiedConsumer.Subscribe(topic, nil))

	require.NoError(t, execTMQSQLOnAnyPort(ports, fmt.Sprintf("insert into %s.%s values(now, 11)", db, table)))

	wrapperMsg, err := waitForTMQDataValue(wrapper, 11, 10*time.Second)
	require.NoError(t, err)
	unifiedMsg, err := waitForTMQDataValue(unifiedConsumer, 11, 10*time.Second)
	require.NoError(t, err)

	require.Equal(t, db, wrapperMsg.DBName())
	require.Equal(t, wrapperMsg.DBName(), unifiedMsg.DBName())
	require.Equal(t, topic, wrapperMsg.Topic())
	require.Equal(t, wrapperMsg.Topic(), unifiedMsg.Topic())
	require.True(t, containsTMQValue(wrapperMsg.Value().([]*commontmq.Data), 11))
	require.True(t, containsTMQValue(unifiedMsg.Value().([]*commontmq.Data), 11))
	require.Equal(t, len(wrapperMsg.Value().([]*commontmq.Data)), len(unifiedMsg.Value().([]*commontmq.Data)))

	assertTMQStateAPIs(t, wrapper)
	assertTMQStateAPIs(t, unifiedConsumer)
}

type tmqStateAPI interface {
	Assignment() ([]commontmq.TopicPartition, error)
	Committed([]commontmq.TopicPartition, int) ([]commontmq.TopicPartition, error)
	Position([]commontmq.TopicPartition) ([]commontmq.TopicPartition, error)
	Commit() ([]commontmq.TopicPartition, error)
	CommitOffsets([]commontmq.TopicPartition) ([]commontmq.TopicPartition, error)
}

func assertTMQStateAPIs(t *testing.T, consumer tmqStateAPI) {
	t.Helper()
	partitions, err := consumer.Assignment()
	require.NoError(t, err)
	require.NotEmpty(t, partitions)

	position, err := consumer.Position(partitions)
	require.NoError(t, err)
	require.Len(t, position, len(partitions))

	committed, err := consumer.Committed(partitions, 0)
	require.NoError(t, err)
	require.Len(t, committed, len(partitions))

	commitResult, err := consumer.Commit()
	require.NoError(t, err)
	require.Len(t, commitResult, len(partitions))

	offsetResult, err := consumer.CommitOffsets(position)
	require.NoError(t, err)
	require.Len(t, offsetResult, len(partitions))
}

func newTMQRealAdapterConfig(port string, groupPrefix string, clientPrefix string) commontmq.ConfigMap {
	now := time.Now().UnixNano()
	return commontmq.ConfigMap{
		"ws.url":                 fmt.Sprintf("ws://127.0.0.1:%s", port),
		"td.connect.user":        "root",
		"td.connect.pass":        "taosdata",
		"group.id":               fmt.Sprintf("ws_tmq_%s_group_%d", groupPrefix, now),
		"client.id":              fmt.Sprintf("ws_tmq_%s_client_%d", clientPrefix, now),
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     "false",
		"msg.with.table.name":    "true",
		"ws.message.timeout":     30 * time.Second,
		"ws.message.writeWait":   10 * time.Second,
		"ws.autoReconnect":       true,
		"ws.reconnectIntervalMs": 50,
		"ws.reconnectRetryCount": 60,
	}
}
