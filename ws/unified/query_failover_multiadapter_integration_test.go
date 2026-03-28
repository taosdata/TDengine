package unified

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const unifiedCrossQueryTable = "unified_query_cross"

// TestUnifiedQueryCrossFailoverDisconnectDetectionAndImmediateReconnect verifies the expected behavior for this scenario.
func TestUnifiedQueryCrossFailoverDisconnectDetectionAndImmediateReconnect(t *testing.T) {
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

	db := createTestDatabase(t, ports)
	c := newIntegrationUnifiedClient(t, ports, db)
	defer c.Close()
	createQueryCrossTable(t, c, db, unifiedCrossQueryTable)

	activeBefore := activeAdapterPort(t, c)
	standby := otherAdapterPort(activeBefore, ports)
	stopByPort(t, activeBefore, stops)

	recoverCost, lastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_cross_reconnect", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, recoverCost, 2500*time.Millisecond, "disconnect should be detected and recovered quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should switch to standby")
}

// TestUnifiedQueryResultStatefulFetchNoReconnectOnDisconnect verifies the expected behavior for this scenario.
func TestUnifiedQueryResultStatefulFetchNoReconnectOnDisconnect(t *testing.T) {
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

	db := createTestDatabase(t, ports)
	c := newIntegrationUnifiedClient(t, ports, db)
	defer c.Close()
	createQueryCrossTable(t, c, db, unifiedCrossQueryTable)

	_, err := c.Exec(0, buildQueryInsertSQL(db, unifiedCrossQueryTable, "seed", 0, 0))
	require.NoError(t, err)

	queryResult, err := c.Query(0, fmt.Sprintf("select * from %s.%s limit 10", db, unifiedCrossQueryTable))
	require.NoError(t, err)
	require.NotNil(t, queryResult)
	defer func() {
		_ = queryResult.Close()
	}()

	activeBefore := activeAdapterPort(t, c)
	standby := otherAdapterPort(activeBefore, ports)

	stopByPort(t, activeBefore, stops)

	start := time.Now()
	_, _, err = queryResult.fetchRawBlock(0)
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.True(t, IsConnectionDisconnectedError(err), "stateful result fetch should report disconnected without reconnect")
	assert.Less(t, elapsed, 2500*time.Millisecond, "disconnect should be sensed quickly")

	// Stateful fetch must not trigger reconnect/failover by itself.
	assert.Equal(t, activeBefore, activeAdapterPort(t, c), "active endpoint should not switch on stateful fetch failure")

	// A new stateless request should trigger reconnect/failover and recover.
	recoverCost, lastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "fetch_after_disconnect", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, recoverCost, 2500*time.Millisecond)
	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond)
}

// TestUnifiedQueryCrossConcurrentExecFailoverAndSwitchBack verifies the expected behavior for this scenario.
func TestUnifiedQueryCrossConcurrentExecFailoverAndSwitchBack(t *testing.T) {
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

	db := createTestDatabase(t, ports)
	c := newIntegrationUnifiedClient(t, ports, db)
	defer c.Close()
	createQueryCrossTable(t, c, db, unifiedCrossQueryTable)

	initialActive := activeAdapterPort(t, c)
	standby := otherAdapterPort(initialActive, ports)

	phase1Success, phase1Fail, err := runConcurrentExecWithFault(
		t, c, db, unifiedCrossQueryTable, "q_phase1", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, initialActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase1Success, int32(0), "should have successful execs during failover")
	t.Logf("q_phase1 success=%d fail=%d", phase1Success, phase1Fail)
	phase1RecoverCost, phase1LastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_phase1_after_fault", 4*time.Second)
	require.NoError(t, phase1LastErr)
	assert.Less(t, phase1RecoverCost, 2500*time.Millisecond, "phase1 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should fail over to standby")

	stops[initialActive] = restartAdapterOnPort(t, initialActive)

	phase2Success, phase2Fail, err := runConcurrentExecWithFault(
		t, c, db, unifiedCrossQueryTable, "q_phase2", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, standby, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase2Success, int32(0), "should keep handling concurrent execs during switch-back")
	t.Logf("q_phase2 success=%d fail=%d", phase2Success, phase2Fail)
	phase2RecoverCost, phase2LastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_phase2_after_fault", 4*time.Second)
	require.NoError(t, phase2LastErr)
	assert.Less(t, phase2RecoverCost, 2500*time.Millisecond, "phase2 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == initialActive
	}, 5*time.Second, 50*time.Millisecond, "active endpoint should switch back after recovery")
}

// TestUnifiedQueryCrossMultiNodeFailoverChainUnderConcurrency verifies the expected behavior for this scenario.
func TestUnifiedQueryCrossMultiNodeFailoverChainUnderConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 3)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})

	db := createTestDatabase(t, ports)
	c := newIntegrationUnifiedClient(t, ports, db)
	defer c.Close()
	createQueryCrossTable(t, c, db, unifiedCrossQueryTable)

	firstActive := activeAdapterPort(t, c)
	success1, fail1, err := runConcurrentExecWithFault(
		t, c, db, unifiedCrossQueryTable, "q_chain1", 8, 18, 150*time.Millisecond, func() {
			stopByPort(t, firstActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success1, int32(0))
	t.Logf("q_chain1 success=%d fail=%d", success1, fail1)
	chain1RecoverCost, chain1LastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_chain1_after_fault", 4*time.Second)
	require.NoError(t, chain1LastErr)
	assert.Less(t, chain1RecoverCost, 2500*time.Millisecond)

	require.Eventually(t, func() bool {
		current := activeAdapterPort(t, c)
		return current != firstActive
	}, 4*time.Second, 50*time.Millisecond)

	secondActive := activeAdapterPort(t, c)
	success2, fail2, err := runConcurrentExecWithFault(
		t, c, db, unifiedCrossQueryTable, "q_chain2", 8, 18, 150*time.Millisecond, func() {
			stopByPort(t, secondActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success2, int32(0))
	t.Logf("q_chain2 success=%d fail=%d", success2, fail2)
	chain2RecoverCost, chain2LastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_chain2_after_fault", 4*time.Second)
	require.NoError(t, chain2LastErr)
	assert.Less(t, chain2RecoverCost, 2500*time.Millisecond)

	require.Eventually(t, func() bool {
		current := activeAdapterPort(t, c)
		return current != firstActive && current != secondActive
	}, 5*time.Second, 50*time.Millisecond, "should fail over to the third available endpoint")
}

// TestUnifiedQueryCrossDualNodeJitterWithConcurrentExec verifies the expected behavior for this scenario.
func TestUnifiedQueryCrossDualNodeJitterWithConcurrentExec(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	runDualNodeQueryJitterScenario(t, 12, 25, 150*time.Millisecond, 100*time.Millisecond)
}

// TestUnifiedQueryCrossDualNodeJitterLoop verifies the expected behavior for this scenario.
func TestUnifiedQueryCrossDualNodeJitterLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	loopCount := loopCountFromEnvQuery("LOOP_COUNT", 8)
	for i := 0; i < loopCount; i++ {
		round := i + 1
		t.Run(fmt.Sprintf("round_%02d", round), func(t *testing.T) {
			runDualNodeQueryJitterScenario(t, 10, 20, 150*time.Millisecond, 100*time.Millisecond)
		})
	}
}

func runDualNodeQueryJitterScenario(t *testing.T, workers, perWorker int, firstFaultDelay, secondFaultGap time.Duration) {
	t.Helper()
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 3)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})

	db := createTestDatabase(t, ports)
	c := newIntegrationUnifiedClient(t, ports, db)
	defer c.Close()
	createQueryCrossTable(t, c, db, unifiedCrossQueryTable)

	firstActive := activeAdapterPort(t, c)
	secondTarget := ""
	for _, p := range ports {
		if p != firstActive {
			secondTarget = p
			break
		}
	}
	require.NotEmpty(t, secondTarget)

	success, failed, err := runConcurrentExecWithFault(
		t, c, db, unifiedCrossQueryTable, "q_dual_jitter", workers, perWorker, firstFaultDelay, func() {
			stopByPort(t, firstActive, stops)
			time.Sleep(secondFaultGap)
			if _, ok := stops[secondTarget]; ok {
				stopByPort(t, secondTarget, stops)
			}
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success, int32(0), "concurrent query execs should keep succeeding during dual jitter")
	t.Logf("q_dual_jitter success=%d fail=%d", success, failed)
	jitterRecoverCost, jitterLastErr := waitForSuccessfulExec(t, c, db, unifiedCrossQueryTable, "q_dual_jitter_after_fault", 5*time.Second)
	require.NoError(t, jitterLastErr)
	assert.Less(t, jitterRecoverCost, 3500*time.Millisecond, "dual jitter reconnect should finish quickly")

	require.Equal(t, 1, len(stops), "only one adapter should remain after dual jitter")
	survivor := ""
	for p := range stops {
		survivor = p
		break
	}
	require.NotEmpty(t, survivor)

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == survivor
	}, 6*time.Second, 50*time.Millisecond, "active endpoint should converge to surviving adapter")
}

func createQueryCrossTable(t *testing.T, c *Client, db, table string) {
	t.Helper()
	_, err := c.Exec(0, fmt.Sprintf("create table if not exists %s.%s(ts timestamp, v int)", db, table))
	require.NoError(t, err)
}

func waitForSuccessfulExec(t *testing.T, c *Client, db, table, phase string, timeout time.Duration) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	var lastErr error
	i := 0
	for time.Since(start) < timeout {
		_, err := c.Exec(0, buildQueryInsertSQL(db, table, phase, 0, i))
		if err == nil {
			return time.Since(start), nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
		i++
	}
	return time.Since(start), lastErr
}

func runConcurrentExecWithFault(t *testing.T, c *Client, db, table, phase string, workers, perWorker int, faultDelay time.Duration, faultFn func()) (int32, int32, error) {
	t.Helper()
	var successCount int32
	var failCount int32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				_, err := c.Exec(0, buildQueryInsertSQL(db, table, phase, workerID, i))
				if err != nil {
					atomic.AddInt32(&failCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}()
	}

	time.Sleep(faultDelay)
	faultFn()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return atomic.LoadInt32(&successCount), atomic.LoadInt32(&failCount), nil
	case <-time.After(20 * time.Second):
		return atomic.LoadInt32(&successCount), atomic.LoadInt32(&failCount), newInvalidStateErrorf("concurrent exec blocked during phase %s", phase)
	}
}

func buildQueryInsertSQL(db, table, phase string, workerID, i int) string {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	value := workerID*100000 + i
	return fmt.Sprintf("insert into %s.%s values ('%s', %d)", db, table, ts, value)
}

func loopCountFromEnvQuery(envName string, defaultCount int) int {
	raw := strings.TrimSpace(os.Getenv(envName))
	if raw == "" {
		return defaultCount
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultCount
	}
	return n
}
