package unified

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
)

// TestUnifiedIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect verifies failover recovery over IPv6 endpoints.
func TestUnifiedIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})
	requireIPv6AdapterReachable(t, ports)

	db := createTestDatabase(t, ports)
	c := newIPv6IntegrationUnifiedClient(t, ports, db)
	defer c.Close()

	activeBefore := activeAdapterPort(t, c)
	standby := otherAdapterPort(activeBefore, ports)

	stopByPort(t, activeBefore, stops)

	recoverCost, lastErr := waitForSuccessfulInsert(t, c, "ipv6_cross_reconnect", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, recoverCost, 2500*time.Millisecond, "disconnect should be detected and recovered quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should switch to standby")
}

// TestUnifiedIPv6CrossConcurrentSendFailoverAndSwitchBack verifies concurrent schemaless failover and switch-back over IPv6 endpoints.
func TestUnifiedIPv6CrossConcurrentSendFailoverAndSwitchBack(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})
	requireIPv6AdapterReachable(t, ports)

	db := createTestDatabase(t, ports)
	c := newIPv6IntegrationUnifiedClient(t, ports, db)
	defer c.Close()

	initialActive := activeAdapterPort(t, c)
	standby := otherAdapterPort(initialActive, ports)

	phase1Success, phase1Fail, err := runConcurrentInsertsWithFault(
		t, c, "ipv6_phase1", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, initialActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase1Success, int32(0), "should have successful inserts during failover")
	t.Logf("ipv6_phase1 success=%d fail=%d", phase1Success, phase1Fail)
	phase1RecoverCost, phase1LastErr := waitForSuccessfulInsert(t, c, "ipv6_phase1_after_fault", 4*time.Second)
	require.NoError(t, phase1LastErr)
	assert.Less(t, phase1RecoverCost, 2500*time.Millisecond, "phase1 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should fail over to standby")

	stops[initialActive] = restartAdapterOnPort(t, initialActive)

	phase2Success, phase2Fail, err := runConcurrentInsertsWithFault(
		t, c, "ipv6_phase2", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, standby, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase2Success, int32(0), "should keep handling concurrent inserts during switch-back")
	t.Logf("ipv6_phase2 success=%d fail=%d", phase2Success, phase2Fail)
	phase2RecoverCost, phase2LastErr := waitForSuccessfulInsert(t, c, "ipv6_phase2_after_fault", 4*time.Second)
	require.NoError(t, phase2LastErr)
	assert.Less(t, phase2RecoverCost, 2500*time.Millisecond, "phase2 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == initialActive
	}, 5*time.Second, 50*time.Millisecond, "active endpoint should switch back after recovery")
}

// TestUnifiedQueryIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect verifies query path failover recovery over IPv6 endpoints.
func TestUnifiedQueryIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})
	requireIPv6AdapterReachable(t, ports)

	db := createTestDatabase(t, ports)
	c := newIPv6IntegrationUnifiedClient(t, ports, db)
	defer c.Close()
	createQueryCrossTable(t, c, db, unifiedCrossQueryTable)

	activeBefore := activeAdapterPort(t, c)
	standby := otherAdapterPort(activeBefore, ports)
	stopByPort(t, activeBefore, stops)

	recoverCost, lastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_ipv6_reconnect", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, recoverCost, 2500*time.Millisecond, "disconnect should be detected and recovered quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should switch to standby")
}

// TestUnifiedQueryIPv6CrossConcurrentExecFailoverAndSwitchBack verifies concurrent query exec failover and switch-back over IPv6 endpoints.
func TestUnifiedQueryIPv6CrossConcurrentExecFailoverAndSwitchBack(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})
	requireIPv6AdapterReachable(t, ports)

	db := createTestDatabase(t, ports)
	c := newIPv6IntegrationUnifiedClient(t, ports, db)
	defer c.Close()
	createQueryCrossTable(t, c, db, unifiedCrossQueryTable)

	initialActive := activeAdapterPort(t, c)
	standby := otherAdapterPort(initialActive, ports)

	phase1Success, phase1Fail, err := runConcurrentExecWithFault(
		t, c, db, unifiedCrossQueryTable, "q_ipv6_phase1", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, initialActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase1Success, int32(0), "should have successful execs during failover")
	t.Logf("q_ipv6_phase1 success=%d fail=%d", phase1Success, phase1Fail)
	phase1RecoverCost, phase1LastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_ipv6_phase1_after_fault", 4*time.Second)
	require.NoError(t, phase1LastErr)
	assert.Less(t, phase1RecoverCost, 2500*time.Millisecond, "phase1 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should fail over to standby")

	stops[initialActive] = restartAdapterOnPort(t, initialActive)

	phase2Success, phase2Fail, err := runConcurrentExecWithFault(
		t, c, db, unifiedCrossQueryTable, "q_ipv6_phase2", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, standby, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase2Success, int32(0), "should keep handling concurrent execs during switch-back")
	t.Logf("q_ipv6_phase2 success=%d fail=%d", phase2Success, phase2Fail)
	phase2RecoverCost, phase2LastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_ipv6_phase2_after_fault", 4*time.Second)
	require.NoError(t, phase2LastErr)
	assert.Less(t, phase2RecoverCost, 2500*time.Millisecond, "phase2 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == initialActive
	}, 5*time.Second, 50*time.Millisecond, "active endpoint should switch back after recovery")
}

// TestUnifiedStmtIPv6CrossConcurrentExecFailoverAndSwitchBack verifies concurrent stmt exec failover and switch-back over IPv6 endpoints.
func TestUnifiedStmtIPv6CrossConcurrentExecFailoverAndSwitchBack(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})
	requireIPv6AdapterReachable(t, ports)

	db := createTestDatabase(t, ports)
	c := newIPv6IntegrationUnifiedClient(t, ports, db)
	defer c.Close()
	createStmtCrossTable(t, c, db, unifiedCrossStmtTable)

	probeStmt, err := newPreparedStmtInsert(c, db, unifiedCrossStmtTable)
	require.NoError(t, err)
	defer func() {
		_ = probeStmt.Close(0)
	}()

	initialActive := activeAdapterPort(t, c)
	standby := otherAdapterPort(initialActive, ports)

	phase1Success, phase1Fail, err := runConcurrentStmtExecWithFault(
		c, db, unifiedCrossStmtTable, "stmt_ipv6_phase1", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, initialActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase1Success, int32(0), "should have successful stmt execs during failover")
	t.Logf("stmt_ipv6_phase1 success=%d fail=%d", phase1Success, phase1Fail)
	phase1RecoverCost, phase1LastErr := waitForSuccessfulStmtExec(probeStmt, "stmt_ipv6_phase1_after_fault", 4*time.Second)
	require.NoError(t, phase1LastErr)
	assert.Less(t, phase1RecoverCost, 2500*time.Millisecond, "phase1 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should fail over to standby")

	stops[initialActive] = restartAdapterOnPort(t, initialActive)

	phase2Success, phase2Fail, err := runConcurrentStmtExecWithFault(
		c, db, unifiedCrossStmtTable, "stmt_ipv6_phase2", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, standby, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase2Success, int32(0), "should keep handling concurrent stmt execs during switch-back")
	t.Logf("stmt_ipv6_phase2 success=%d fail=%d", phase2Success, phase2Fail)
	phase2RecoverCost, phase2LastErr := waitForSuccessfulStmtExec(probeStmt, "stmt_ipv6_phase2_after_fault", 4*time.Second)
	require.NoError(t, phase2LastErr)
	assert.Less(t, phase2RecoverCost, 2500*time.Millisecond, "phase2 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == initialActive
	}, 5*time.Second, 50*time.Millisecond, "active endpoint should switch back after recovery")
}

// TestUnifiedTMQIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect verifies tmq failover recovery over IPv6 endpoints.
func TestUnifiedTMQIPv6CrossFailoverDisconnectDetectionAndImmediateReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})
	requireIPv6AdapterReachable(t, ports)

	db, table, topic := setupTMQCrossEnv(t, ports)
	consumer := newIPv6IntegrationTMQConsumer(t, ports)
	defer func() {
		_ = consumer.Unsubscribe()
		_ = consumer.Close()
	}()
	require.NoError(t, consumer.Subscribe(topic, nil))
	require.NoError(t, waitForTMQAssignment(consumer, ports, 10*time.Second))

	seed := nextTMQCrossValue()
	tmqInsertValue(t, ports, db, table, seed)
	_, err := waitForTMQDataMessage(consumer, 12*time.Second)
	require.NoError(t, err)

	activeBefore := activeAdapterPort(t, consumer.client)
	standby := otherAdapterPort(activeBefore, ports)
	require.NotEmpty(t, standby)
	stopByPort(t, activeBefore, stops)
	require.NoError(t, forceTMQFailover(consumer, func() bool {
		return activeAdapterPort(t, consumer.client) == standby
	}, 8*time.Second))
	require.NoError(t, waitForTMQAssignment(consumer, ports, 10*time.Second))

	start := time.Now()
	probe := nextTMQCrossValue()
	tmqInsertValue(t, ports, db, table, probe)
	msg, err := waitForTMQDataValue(consumer, probe, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, db, msg.DBName())
	assert.Less(t, time.Since(start), 10*time.Second, "tmq failover recovery should complete quickly")
}

// TestUnifiedTMQIPv6CrossConcurrentPollFailoverAndSwitchBack verifies concurrent tmq polling failover and switch-back over IPv6 endpoints.
func TestUnifiedTMQIPv6CrossConcurrentPollFailoverAndSwitchBack(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})
	requireIPv6AdapterReachable(t, ports)

	db, table, topic := setupTMQCrossEnv(t, ports)
	consumer := newIPv6IntegrationTMQConsumer(t, ports)
	defer func() {
		_ = consumer.Unsubscribe()
		_ = consumer.Close()
	}()
	require.NoError(t, consumer.Subscribe(topic, nil))
	require.NoError(t, waitForTMQAssignment(consumer, ports, 10*time.Second))

	initialActive := activeAdapterPort(t, consumer.client)
	standby := otherAdapterPort(initialActive, ports)
	require.NotEmpty(t, standby)

	phase1Success, phase1Fail, phase1Polled, err := runConcurrentTMQConsumeWithFault(
		consumer, ports, db, table, 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, initialActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase1Success, int32(0), "should have successful inserts during failover")
	t.Logf("tmq_ipv6_phase1 insert_success=%d insert_fail=%d polled=%d", phase1Success, phase1Fail, phase1Polled)
	phase1RecoverCost, phase1LastErr := waitForSuccessfulTMQInsert(consumer, ports, db, table, 20*time.Second)
	require.NoError(t, phase1LastErr)
	t.Logf("tmq_ipv6_phase1 recover=%s", phase1RecoverCost)

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, consumer.client) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should fail over to standby")

	stops[initialActive] = restartAdapterOnPort(t, initialActive)

	phase2Success, phase2Fail, phase2Polled, err := runConcurrentTMQConsumeWithFault(
		consumer, ports, db, table, 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, standby, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase2Success, int32(0), "should keep handling inserts during switch-back")
	t.Logf("tmq_ipv6_phase2 insert_success=%d insert_fail=%d polled=%d", phase2Success, phase2Fail, phase2Polled)
	phase2RecoverCost, phase2LastErr := waitForSuccessfulTMQInsert(consumer, ports, db, table, 20*time.Second)
	require.NoError(t, phase2LastErr)
	t.Logf("tmq_ipv6_phase2 recover=%s", phase2RecoverCost)

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, consumer.client) == initialActive
	}, 5*time.Second, 50*time.Millisecond, "active endpoint should switch back after recovery")
}

func newIPv6IntegrationUnifiedClient(t *testing.T, ports []string, db string) *Client {
	t.Helper()
	cfg := NewConfig(ipv6WSEndpoints(ports))
	cfg.User = "root"
	cfg.Passwd = "taosdata"
	cfg.DbName = db
	cfg.ReadTimeout = 2 * time.Second
	cfg.WriteTimeout = 2 * time.Second
	cfg.AutoReconnect = true
	cfg.ReconnectIntervalMs = 50
	cfg.ReconnectRetryCount = 40

	c, err := NewClient(cfg, "/ws")
	require.NoError(t, err)
	require.NoError(t, c.Connect())
	return c
}

func newIPv6IntegrationTMQConsumer(t *testing.T, ports []string) *TMQConsumer {
	t.Helper()
	cfg := commontmq.ConfigMap{
		"ws.url":                 strings.Join(ipv6WSEndpoints(ports), ","),
		"td.connect.user":        "root",
		"td.connect.pass":        "taosdata",
		"group.id":               fmt.Sprintf("tmq_ipv6_group_%d", time.Now().UnixNano()),
		"client.id":              fmt.Sprintf("tmq_ipv6_client_%d", time.Now().UnixNano()),
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     "false",
		"msg.with.table.name":    "true",
		"session.timeout.ms":     "10000",
		"max.poll.interval.ms":   "30000",
		"ws.message.timeout":     3 * time.Second,
		"ws.message.writeWait":   3 * time.Second,
		"ws.autoReconnect":       true,
		"ws.reconnectIntervalMs": 50,
		"ws.reconnectRetryCount": 60,
	}
	consumer, err := NewTMQConsumer(&cfg)
	require.NoError(t, err)
	return consumer
}

func ipv6WSEndpoints(ports []string) []string {
	endpoints := make([]string, 0, len(ports))
	for i := 0; i < len(ports); i++ {
		endpoints = append(endpoints, fmt.Sprintf("ws://%s", net.JoinHostPort("::1", ports[i])))
	}
	return endpoints
}

func requireIPv6AdapterReachable(t *testing.T, ports []string) {
	t.Helper()
	httpClient := &http.Client{Timeout: 1500 * time.Millisecond}
	for i := 0; i < len(ports); i++ {
		pingURL := fmt.Sprintf("http://%s/-/ping", net.JoinHostPort("::1", ports[i]))
		resp, err := httpClient.Get(pingURL)
		if err != nil {
			t.Skipf("skip ipv6 integration test: adapter port %s is not reachable via ::1: %v", ports[i], err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Skipf("skip ipv6 integration test: adapter port %s ping via ::1 status=%d", ports[i], resp.StatusCode)
		}
	}
}
