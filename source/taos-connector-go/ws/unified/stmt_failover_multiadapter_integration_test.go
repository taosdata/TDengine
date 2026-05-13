package unified

import (
	"database/sql/driver"
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
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

const unifiedCrossStmtTable = "unified_stmt_cross"

var stmtCrossTimestampSeq int64

// TestUnifiedStmtCrossFailoverDisconnectDetectionAndImmediateReconnect verifies the expected behavior for this scenario.
func TestUnifiedStmtCrossFailoverDisconnectDetectionAndImmediateReconnect(t *testing.T) {
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
	createStmtCrossTable(t, c, db, unifiedCrossStmtTable)

	probeStmt, err := newPreparedStmtInsert(c, db, unifiedCrossStmtTable)
	require.NoError(t, err)
	defer func() {
		_ = probeStmt.Close(0)
	}()

	activeBefore := activeAdapterPort(t, c)
	standby := otherAdapterPort(activeBefore, ports)
	stopByPort(t, activeBefore, stops)

	recoverCost, lastErr := waitForSuccessfulStmtExec(probeStmt, "stmt_cross_reconnect", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, recoverCost, 2500*time.Millisecond, "disconnect should be detected and recovered quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should switch to standby")
}

// TestUnifiedStmtRawBindCrossFailoverDisconnectDetectionAndImmediateReconnect verifies raw bind failover.
func TestUnifiedStmtRawBindCrossFailoverDisconnectDetectionAndImmediateReconnect(t *testing.T) {
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
	createStmtCrossTable(t, c, db, unifiedCrossStmtTable)

	probeStmt, err := newPreparedStmtInsert(c, db, unifiedCrossStmtTable)
	require.NoError(t, err)
	defer func() {
		_ = probeStmt.Close(0)
	}()

	activeBefore := activeAdapterPort(t, c)
	standby := otherAdapterPort(activeBefore, ports)
	stopByPort(t, activeBefore, stops)

	recoverCost, lastErr := waitForSuccessfulRawStmtExec(probeStmt, "stmt_raw_cross_reconnect", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, recoverCost, 2500*time.Millisecond, "raw stmt bind should recover quickly after disconnect")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should switch to standby")
}

// TestUnifiedStmtCrossConcurrentExecFailoverAndSwitchBack verifies the expected behavior for this scenario.
func TestUnifiedStmtCrossConcurrentExecFailoverAndSwitchBack(t *testing.T) {
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
	createStmtCrossTable(t, c, db, unifiedCrossStmtTable)

	probeStmt, err := newPreparedStmtInsert(c, db, unifiedCrossStmtTable)
	require.NoError(t, err)
	defer func() {
		_ = probeStmt.Close(0)
	}()

	initialActive := activeAdapterPort(t, c)
	standby := otherAdapterPort(initialActive, ports)

	phase1Success, phase1Fail, err := runConcurrentStmtExecWithFault(
		c, db, unifiedCrossStmtTable, "stmt_phase1", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, initialActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase1Success, int32(0), "should have successful stmt execs during failover")
	t.Logf("stmt_phase1 success=%d fail=%d", phase1Success, phase1Fail)
	phase1RecoverCost, phase1LastErr := waitForSuccessfulStmtExec(probeStmt, "stmt_phase1_after_fault", 4*time.Second)
	require.NoError(t, phase1LastErr)
	assert.Less(t, phase1RecoverCost, 2500*time.Millisecond, "phase1 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should fail over to standby")

	stops[initialActive] = restartAdapterOnPort(t, initialActive)

	phase2Success, phase2Fail, err := runConcurrentStmtExecWithFault(
		c, db, unifiedCrossStmtTable, "stmt_phase2", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, standby, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase2Success, int32(0), "should keep handling concurrent stmt execs during switch-back")
	t.Logf("stmt_phase2 success=%d fail=%d", phase2Success, phase2Fail)
	phase2RecoverCost, phase2LastErr := waitForSuccessfulStmtExec(probeStmt, "stmt_phase2_after_fault", 4*time.Second)
	require.NoError(t, phase2LastErr)
	assert.Less(t, phase2RecoverCost, 2500*time.Millisecond, "phase2 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == initialActive
	}, 5*time.Second, 50*time.Millisecond, "active endpoint should switch back after recovery")
}

// TestUnifiedStmtCrossMultiNodeFailoverChainUnderConcurrency verifies the expected behavior for this scenario.
func TestUnifiedStmtCrossMultiNodeFailoverChainUnderConcurrency(t *testing.T) {
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
	createStmtCrossTable(t, c, db, unifiedCrossStmtTable)

	probeStmt, err := newPreparedStmtInsert(c, db, unifiedCrossStmtTable)
	require.NoError(t, err)
	defer func() {
		_ = probeStmt.Close(0)
	}()

	firstActive := activeAdapterPort(t, c)
	success1, fail1, err := runConcurrentStmtExecWithFault(
		c, db, unifiedCrossStmtTable, "stmt_chain1", 8, 18, 150*time.Millisecond, func() {
			stopByPort(t, firstActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success1, int32(0))
	t.Logf("stmt_chain1 success=%d fail=%d", success1, fail1)
	chain1RecoverCost, chain1LastErr := waitForSuccessfulStmtExec(probeStmt, "stmt_chain1_after_fault", 4*time.Second)
	require.NoError(t, chain1LastErr)
	assert.Less(t, chain1RecoverCost, 2500*time.Millisecond)

	require.Eventually(t, func() bool {
		current := activeAdapterPort(t, c)
		return current != firstActive
	}, 4*time.Second, 50*time.Millisecond)

	secondActive := activeAdapterPort(t, c)
	success2, fail2, err := runConcurrentStmtExecWithFault(
		c, db, unifiedCrossStmtTable, "stmt_chain2", 8, 18, 150*time.Millisecond, func() {
			stopByPort(t, secondActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success2, int32(0))
	t.Logf("stmt_chain2 success=%d fail=%d", success2, fail2)
	chain2RecoverCost, chain2LastErr := waitForSuccessfulStmtExec(probeStmt, "stmt_chain2_after_fault", 4*time.Second)
	require.NoError(t, chain2LastErr)
	assert.Less(t, chain2RecoverCost, 2500*time.Millisecond)

	require.Eventually(t, func() bool {
		current := activeAdapterPort(t, c)
		return current != firstActive && current != secondActive
	}, 5*time.Second, 50*time.Millisecond, "should fail over to the third available endpoint")
}

// TestUnifiedStmtCrossDualNodeJitterWithConcurrentExec verifies the expected behavior for this scenario.
func TestUnifiedStmtCrossDualNodeJitterWithConcurrentExec(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	runDualNodeStmtJitterScenario(t, 12, 25, 150*time.Millisecond, 100*time.Millisecond)
}

// TestUnifiedStmtCrossDualNodeJitterLoop verifies the expected behavior for this scenario.
func TestUnifiedStmtCrossDualNodeJitterLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	loopCount := loopCountFromEnvStmt("LOOP_COUNT", 8)
	for i := 0; i < loopCount; i++ {
		round := i + 1
		t.Run(fmt.Sprintf("round_%02d", round), func(t *testing.T) {
			runDualNodeStmtJitterScenario(t, 10, 20, 150*time.Millisecond, 100*time.Millisecond)
		})
	}
}

func runDualNodeStmtJitterScenario(t *testing.T, workers, perWorker int, firstFaultDelay, secondFaultGap time.Duration) {
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
	createStmtCrossTable(t, c, db, unifiedCrossStmtTable)

	probeStmt, err := newPreparedStmtInsert(c, db, unifiedCrossStmtTable)
	require.NoError(t, err)
	defer func() {
		_ = probeStmt.Close(0)
	}()

	firstActive := activeAdapterPort(t, c)
	secondTarget := ""
	for _, p := range ports {
		if p != firstActive {
			secondTarget = p
			break
		}
	}
	require.NotEmpty(t, secondTarget)

	success, failed, err := runConcurrentStmtExecWithFault(
		c, db, unifiedCrossStmtTable, "stmt_dual_jitter", workers, perWorker, firstFaultDelay, func() {
			stopByPort(t, firstActive, stops)
			time.Sleep(secondFaultGap)
			if _, ok := stops[secondTarget]; ok {
				stopByPort(t, secondTarget, stops)
			}
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success, int32(0), "concurrent stmt execs should keep succeeding during dual jitter")
	t.Logf("stmt_dual_jitter success=%d fail=%d", success, failed)
	jitterRecoverCost, jitterLastErr := waitForSuccessfulStmtExec(probeStmt, "stmt_dual_jitter_after_fault", 5*time.Second)
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

func createStmtCrossTable(t *testing.T, c *Client, db, table string) {
	t.Helper()
	_, err := c.Exec(0, fmt.Sprintf("create table if not exists %s.%s(ts timestamp, v int)", db, table))
	require.NoError(t, err)
}

func newPreparedStmtInsert(c *Client, db, table string) (*Stmt, error) {
	stmt, err := c.InitStmt(0)
	if err != nil {
		return nil, err
	}
	if err = stmt.Prepare(0, fmt.Sprintf("insert into %s.%s values(?, ?)", db, table)); err != nil {
		_ = stmt.Close(0)
		return nil, err
	}
	return stmt, nil
}

func execPreparedStmtInsert(stmt *Stmt, value int) error {
	params := []*param.Param{
		param.NewParam(1).AddTimestamp(nextStmtCrossTimestamp(), 0),
		param.NewParam(1).AddInt(value),
	}
	bindType := param.NewColumnType(2).AddTimestamp().AddInt()
	if err := stmt.BindParam(params, bindType); err != nil {
		return err
	}
	if err := stmt.AddBatch(); err != nil {
		return err
	}
	_, err := stmt.Exec(0)
	return err
}

func execPreparedRawStmtInsert(stmt *Stmt, value int) error {
	if err := stmt.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{nextStmtCrossTimestamp()},
				{int32(value)},
			},
		},
	}); err != nil {
		return err
	}
	_, err := stmt.Exec(0)
	return err
}

func waitForSuccessfulStmtExec(stmt *Stmt, phase string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	var lastErr error
	i := 0
	for time.Since(start) < timeout {
		err := execPreparedStmtInsert(stmt, buildStmtInsertValue(phase, 0, i))
		if err == nil {
			return time.Since(start), nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
		i++
	}
	return time.Since(start), lastErr
}

func waitForSuccessfulRawStmtExec(stmt *Stmt, phase string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	var lastErr error
	i := 0
	for time.Since(start) < timeout {
		err := execPreparedRawStmtInsert(stmt, buildStmtInsertValue(phase, 0, i))
		if err == nil {
			return time.Since(start), nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
		i++
	}
	return time.Since(start), lastErr
}

func runConcurrentStmtExecWithFault(c *Client, db, table, phase string, workers, perWorker int, faultDelay time.Duration, faultFn func()) (int32, int32, error) {
	var successCount int32
	var failCount int32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()

			stmt, err := newPreparedStmtInsert(c, db, table)
			if err != nil {
				atomic.AddInt32(&failCount, int32(perWorker))
				return
			}
			defer func() {
				_ = stmt.Close(0)
			}()

			for i := 0; i < perWorker; i++ {
				err = execPreparedStmtInsert(stmt, buildStmtInsertValue(phase, workerID, i))
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
		return atomic.LoadInt32(&successCount), atomic.LoadInt32(&failCount), newInvalidStateErrorf("concurrent stmt exec blocked during phase %s", phase)
	}
}

func buildStmtInsertValue(phase string, workerID, i int) int {
	return len(phase)*1000000 + workerID*100000 + i
}

func nextStmtCrossTimestamp() time.Time {
	ms := int64(1700000000000) + atomic.AddInt64(&stmtCrossTimestampSeq, 1)
	return time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond))
}

func loopCountFromEnvStmt(envName string, defaultCount int) int {
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
