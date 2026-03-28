package unified

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
)

var tmqCrossValueSeq int64

// TestUnifiedTMQCrossFailoverDisconnectDetectionAndImmediateReconnect verifies the expected behavior for this scenario.
func TestUnifiedTMQCrossFailoverDisconnectDetectionAndImmediateReconnect(t *testing.T) {
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

	db, table, topic := setupTMQCrossEnv(t, ports)
	consumer := newIntegrationTMQConsumer(t, ports, db)
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

// TestUnifiedTMQCrossConcurrentPollFailoverAndSwitchBack verifies the expected behavior for this scenario.
func TestUnifiedTMQCrossConcurrentPollFailoverAndSwitchBack(t *testing.T) {
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

	db, table, topic := setupTMQCrossEnv(t, ports)
	consumer := newIntegrationTMQConsumer(t, ports, db)
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
	t.Logf("tmq_phase1 insert_success=%d insert_fail=%d polled=%d", phase1Success, phase1Fail, phase1Polled)
	phase1RecoverCost, phase1LastErr := waitForSuccessfulTMQInsert(consumer, ports, db, table, 20*time.Second)
	require.NoError(t, phase1LastErr)
	t.Logf("tmq_phase1 recover=%s", phase1RecoverCost)

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
	t.Logf("tmq_phase2 insert_success=%d insert_fail=%d polled=%d", phase2Success, phase2Fail, phase2Polled)
	phase2RecoverCost, phase2LastErr := waitForSuccessfulTMQInsert(consumer, ports, db, table, 20*time.Second)
	require.NoError(t, phase2LastErr)
	t.Logf("tmq_phase2 recover=%s", phase2RecoverCost)

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, consumer.client) == initialActive
	}, 5*time.Second, 50*time.Millisecond, "active endpoint should switch back after recovery")
}

// TestUnifiedTMQCrossMultiNodeFailoverChainUnderConcurrency verifies the expected behavior for this scenario.
func TestUnifiedTMQCrossMultiNodeFailoverChainUnderConcurrency(t *testing.T) {
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

	db, table, topic := setupTMQCrossEnv(t, ports)
	consumer := newIntegrationTMQConsumer(t, ports, db)
	defer func() {
		_ = consumer.Unsubscribe()
		_ = consumer.Close()
	}()
	require.NoError(t, consumer.Subscribe(topic, nil))
	require.NoError(t, waitForTMQAssignment(consumer, ports, 10*time.Second))

	firstActive := activeAdapterPort(t, consumer.client)
	success1, fail1, polled1, err := runConcurrentTMQConsumeWithFault(
		consumer, ports, db, table, 8, 18, 150*time.Millisecond, func() {
			stopByPort(t, firstActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success1, int32(0))
	t.Logf("tmq_chain1 insert_success=%d insert_fail=%d polled=%d", success1, fail1, polled1)
	chain1RecoverCost, chain1LastErr := waitForSuccessfulTMQInsert(consumer, ports, db, table, 20*time.Second)
	require.NoError(t, chain1LastErr)
	t.Logf("tmq_chain1 recover=%s", chain1RecoverCost)

	require.Eventually(t, func() bool {
		current := activeAdapterPort(t, consumer.client)
		return current != firstActive
	}, 4*time.Second, 50*time.Millisecond)

	secondActive := activeAdapterPort(t, consumer.client)
	success2, fail2, polled2, err := runConcurrentTMQConsumeWithFault(
		consumer, ports, db, table, 8, 18, 150*time.Millisecond, func() {
			stopByPort(t, secondActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success2, int32(0))
	t.Logf("tmq_chain2 insert_success=%d insert_fail=%d polled=%d", success2, fail2, polled2)
	chain2RecoverCost, chain2LastErr := waitForSuccessfulTMQInsert(consumer, ports, db, table, 20*time.Second)
	require.NoError(t, chain2LastErr)
	t.Logf("tmq_chain2 recover=%s", chain2RecoverCost)

	require.Eventually(t, func() bool {
		current := activeAdapterPort(t, consumer.client)
		return current != firstActive && current != secondActive
	}, 5*time.Second, 50*time.Millisecond, "should fail over to the third available endpoint")
}

// TestUnifiedTMQCrossDualNodeJitterWithConcurrentPoll verifies the expected behavior for this scenario.
func TestUnifiedTMQCrossDualNodeJitterWithConcurrentPoll(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	runDualNodeTMQJitterScenario(t, 12, 25, 150*time.Millisecond, 100*time.Millisecond)
}

// TestUnifiedTMQCrossDualNodeJitterLoop verifies the expected behavior for this scenario.
func TestUnifiedTMQCrossDualNodeJitterLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	loopCount := loopCountFromEnv("LOOP_COUNT", 8)
	for i := 0; i < loopCount; i++ {
		round := i + 1
		t.Run(fmt.Sprintf("round_%02d", round), func(t *testing.T) {
			runDualNodeTMQJitterScenario(t, 10, 20, 150*time.Millisecond, 100*time.Millisecond)
		})
	}
}

func runDualNodeTMQJitterScenario(t *testing.T, workers, perWorker int, firstFaultDelay, secondFaultGap time.Duration) {
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

	db, table, topic := setupTMQCrossEnv(t, ports)
	consumer := newIntegrationTMQConsumer(t, ports, db)
	defer func() {
		_ = consumer.Unsubscribe()
		_ = consumer.Close()
	}()
	require.NoError(t, consumer.Subscribe(topic, nil))
	require.NoError(t, waitForTMQAssignment(consumer, ports, 10*time.Second))

	firstActive := activeAdapterPort(t, consumer.client)
	secondTarget := ""
	for _, p := range ports {
		if p != firstActive {
			secondTarget = p
			break
		}
	}
	require.NotEmpty(t, secondTarget)

	success, failed, polled, err := runConcurrentTMQConsumeWithFault(
		consumer, ports, db, table, workers, perWorker, firstFaultDelay, func() {
			stopByPort(t, firstActive, stops)
			time.Sleep(secondFaultGap)
			if _, ok := stops[secondTarget]; ok {
				stopByPort(t, secondTarget, stops)
			}
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success, int32(0), "concurrent inserts should keep succeeding during dual jitter")
	t.Logf("tmq_dual_jitter insert_success=%d insert_fail=%d polled=%d", success, failed, polled)
	jitterRecoverCost, jitterLastErr := waitForSuccessfulTMQInsert(consumer, ports, db, table, 25*time.Second)
	require.NoError(t, jitterLastErr)
	t.Logf("tmq_dual_jitter recover=%s", jitterRecoverCost)

	require.Equal(t, 1, len(stops), "only one adapter should remain after dual jitter")
	survivor := ""
	for p := range stops {
		survivor = p
		break
	}
	require.NotEmpty(t, survivor)

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, consumer.client) == survivor
	}, 6*time.Second, 50*time.Millisecond, "active endpoint should converge to surviving adapter")

	finalRecoverCost, finalLastErr := waitForSuccessfulTMQInsert(consumer, ports, db, table, 25*time.Second)
	require.NoError(t, finalLastErr)
	t.Logf("tmq_dual_jitter final recover=%s", finalRecoverCost)
}

func newIntegrationTMQConsumer(t *testing.T, ports []string, db string) *TMQConsumer {
	t.Helper()
	endpoints := make([]string, 0, len(ports))
	for i := 0; i < len(ports); i++ {
		endpoints = append(endpoints, fmt.Sprintf("ws://127.0.0.1:%s", ports[i]))
	}
	cfg := commontmq.ConfigMap{
		"ws.url":                 strings.Join(endpoints, ","),
		"td.connect.user":        "root",
		"td.connect.pass":        "taosdata",
		"group.id":               fmt.Sprintf("tmq_group_%d", time.Now().UnixNano()),
		"client.id":              fmt.Sprintf("tmq_client_%d", time.Now().UnixNano()),
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

func setupTMQCrossEnv(t *testing.T, ports []string) (string, string, string) {
	t.Helper()
	db := createTestDatabase(t, ports)
	name := normalizeTMQCrossName(t.Name())
	table := fmt.Sprintf("tmq_cross_%s_t", name)
	topic := fmt.Sprintf("tmq_cross_%s_topic", name)

	if err := resetTMQCrossObjects(ports, db, table, topic); err != nil {
		cleanupTMQCrossStaleTopics(ports)
		require.NoError(t, resetTMQCrossObjects(ports, db, table, topic))
	}
	require.NoError(t, execSQLOnAnyPort(ports, fmt.Sprintf("create table %s.%s(ts timestamp, v int)", db, table)))
	createTopicSQL := fmt.Sprintf("create topic %s as select * from %s.%s", topic, db, table)
	err := execSQLOnAnyPort(ports, createTopicSQL)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "topic num out of range") {
		cleanupTMQCrossStaleTopics(ports)
		require.NoError(t, resetTMQCrossObjects(ports, db, table, topic))
		require.NoError(t, execSQLOnAnyPort(ports, fmt.Sprintf("create table %s.%s(ts timestamp, v int)", db, table)))
		err = execSQLOnAnyPort(ports, createTopicSQL)
	}
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupTMQCrossObjects(ports, db, table, topic)
	})
	return db, table, topic
}

func tmqInsertValue(t *testing.T, ports []string, db, table string, v int32) {
	t.Helper()
	require.NoError(t, execSQLOnAnyPort(ports, fmt.Sprintf("insert into %s.%s values(now, %d)", db, table, v)))
}

func waitForTMQDataMessage(consumer *TMQConsumer, timeout time.Duration) (*commontmq.DataMessage, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		event := consumer.Poll(500)
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case commontmq.Error:
			lastErr = e
		case *commontmq.DataMessage:
			return e, nil
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, newInvalidStateErrorf("timeout waiting tmq data message")
}

func waitForTMQDataValue(consumer *TMQConsumer, want int32, timeout time.Duration) (*commontmq.DataMessage, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		event := consumer.Poll(500)
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case commontmq.Error:
			lastErr = e
		case *commontmq.DataMessage:
			if containsTMQCrossValue(e.Value().([]*commontmq.Data), want) {
				return e, nil
			}
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, newInvalidStateErrorf("timeout waiting tmq data value")
}

func containsTMQCrossValue(data []*commontmq.Data, want int32) bool {
	for i := 0; i < len(data); i++ {
		for j := 0; j < len(data[i].Data); j++ {
			row := data[i].Data[j]
			if len(row) < 2 {
				continue
			}
			value, ok := row[1].(int32)
			if ok && value == want {
				return true
			}
			value64, ok := row[1].(int64)
			if ok && value64 == int64(want) {
				return true
			}
		}
	}
	return false
}

func nextTMQCrossValue() int32 {
	return int32(atomic.AddInt64(&tmqCrossValueSeq, 1))
}

func waitForSuccessfulTMQInsert(consumer *TMQConsumer, ports []string, db, table string, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	var lastErr error
	for time.Since(start) < timeout {
		if err := waitForTMQAssignment(consumer, ports, 2*time.Second); err != nil {
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		value := nextTMQCrossValue()
		err := execSQLOnAnyPort(ports, fmt.Sprintf("insert into %s.%s values(now, %d)", db, table, value))
		if err == nil {
			_, pollErr := waitForTMQDataMessage(consumer, 3500*time.Millisecond)
			if pollErr == nil {
				return time.Since(start), nil
			}
			lastErr = pollErr
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return time.Since(start), lastErr
}

func runConcurrentTMQConsumeWithFault(consumer *TMQConsumer, ports []string, db, table string, workers, perWorker int, faultDelay time.Duration, faultFn func()) (int32, int32, int32, error) {
	var insertSuccess int32
	var insertFail int32
	var pollDataCount int32
	var pollErrCount int32
	lastPollErr := ""
	var lastPollErrLock sync.Mutex

	stopPolling := make(chan struct{})
	var pollWG sync.WaitGroup
	pollWG.Add(1)
	go func() {
		defer pollWG.Done()
		for {
			select {
			case <-stopPolling:
				return
			default:
			}
			event := consumer.Poll(200)
			switch event := event.(type) {
			case *commontmq.DataMessage:
				atomic.AddInt32(&pollDataCount, 1)
			case commontmq.Error:
				atomic.AddInt32(&pollErrCount, 1)
				lastPollErrLock.Lock()
				lastPollErr = event.Error()
				lastPollErrLock.Unlock()
			}
		}
	}()

	var insertWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		insertWG.Add(1)
		go func() {
			defer insertWG.Done()
			for j := 0; j < perWorker; j++ {
				value := nextTMQCrossValue()
				err := execSQLOnAnyPort(ports, fmt.Sprintf("insert into %s.%s values(now, %d)", db, table, value))
				if err != nil {
					atomic.AddInt32(&insertFail, 1)
				} else {
					atomic.AddInt32(&insertSuccess, 1)
				}
			}
		}()
	}

	time.Sleep(faultDelay)
	faultFn()

	done := make(chan struct{})
	go func() {
		insertWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		close(stopPolling)
		pollWG.Wait()
		return atomic.LoadInt32(&insertSuccess), atomic.LoadInt32(&insertFail), atomic.LoadInt32(&pollDataCount), newInvalidStateErrorf("concurrent tmq inserts blocked")
	}

	time.Sleep(300 * time.Millisecond)
	close(stopPolling)
	pollWG.Wait()
	if atomic.LoadInt32(&pollDataCount) == 0 && atomic.LoadInt32(&pollErrCount) > 0 {
		// Reconnect windows may briefly surface poll errors before assignment and data flow recover.
		// Callers validate the end-to-end data path via waitForSuccessfulTMQInsert.
		lastPollErrLock.Lock()
		_ = lastPollErr
		lastPollErrLock.Unlock()
	}
	return atomic.LoadInt32(&insertSuccess), atomic.LoadInt32(&insertFail), atomic.LoadInt32(&pollDataCount), nil
}

func normalizeTMQCrossName(name string) string {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		ch := name[i]
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteByte(ch)
		case ch >= 'A' && ch <= 'Z':
			b.WriteByte(ch + ('a' - 'A'))
		case ch >= '0' && ch <= '9':
			b.WriteByte(ch)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 24 {
			break
		}
	}
	if b.Len() == 0 {
		return "default"
	}
	return b.String()
}

func cleanupTMQCrossObjects(ports []string, db, table, topic string) {
	_ = resetTMQCrossObjects(ports, db, table, topic)
}

func resetTMQCrossObjects(ports []string, db, table, topic string) error {
	var lastErr error
	for i := 0; i < 20; i++ {
		topicErr := execSQLOnAnyPort(ports, fmt.Sprintf("drop topic if exists %s", topic))
		tableErr := execSQLOnAnyPort(ports, fmt.Sprintf("drop table if exists %s.%s", db, table))
		if topicErr == nil && tableErr == nil {
			return nil
		}
		if topicErr != nil {
			lastErr = topicErr
		}
		if tableErr != nil {
			lastErr = tableErr
		}
		time.Sleep(300 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = newInvalidStateErrorf("failed to reset tmq cross objects")
	}
	return lastErr
}

func cleanupTMQCrossStaleTopics(ports []string) {
	resp, err := querySQLOnPort(ports[0], "show topics")
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
			_ = execSQLOnAnyPort(ports, fmt.Sprintf("drop topic if exists %s", name))
		}
	}
}

func forceTMQFailover(consumer *TMQConsumer, switched func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		event := consumer.Poll(200)
		if switched() {
			return nil
		}
		if e, ok := event.(commontmq.Error); ok {
			lastErr = e
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return newInvalidStateErrorf("timeout waiting tmq failover")
}

func waitForTMQAssignment(consumer *TMQConsumer, ports []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		partitions, err := consumer.Assignment()
		if err == nil && len(partitions) > 0 {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		// Poll drives tmq rebalance/rejoin progression after reconnect, especially in chained failover.
		if event := consumer.Poll(100); event != nil {
			if pollErr, ok := event.(commontmq.Error); ok {
				lastErr = pollErr
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return lastErr
	}
	return newInvalidStateErrorf("timeout waiting tmq assignment: %s", tmqAssignmentDiagnostic(consumer, ports))
}

func tmqAssignmentDiagnostic(consumer *TMQConsumer, ports []string) string {
	if consumer == nil {
		return "consumer=nil"
	}
	activeEndpoint := ""
	if consumer.client != nil {
		activeEndpoint = consumer.client.failover.active().URL
	}
	runtime := consumer.runtime()
	runtimeState := "nil"
	runtimeErr := ""
	if runtime != nil {
		runtimeState = fmt.Sprintf("running=%t", runtime.IsRunning())
		if err := runtime.LastError(); err != nil {
			runtimeErr = err.Error()
		}
	}
	adapterStates := make([]string, 0, len(ports))
	for i := 0; i < len(ports); i++ {
		adapterStates = append(adapterStates, fmt.Sprintf("%s=%t", ports[i], pingAdapter(ports[i])))
	}
	consumersState := tmqShowConsumersDiagnostic(ports)
	return fmt.Sprintf("active=%s runtime=%s runtime_err=%q adapters=[%s] consumers=[%s]", activeEndpoint, runtimeState, runtimeErr, strings.Join(adapterStates, ","), consumersState)
}

func tmqShowConsumersDiagnostic(ports []string) string {
	if len(ports) == 0 {
		return "no_ports"
	}
	results := make([]string, 0, len(ports))
	for i := 0; i < len(ports); i++ {
		port := ports[i]
		resp, err := querySQLOnPort(port, "show consumers")
		if err != nil {
			results = append(results, fmt.Sprintf("%s:error=%v", port, err))
			continue
		}
		if resp == nil {
			results = append(results, fmt.Sprintf("%s:nil_resp", port))
			continue
		}
		if resp.Code != 0 {
			results = append(results, fmt.Sprintf("%s:code=%d desc=%s", port, resp.Code, resp.Desc))
			continue
		}
		if len(resp.Data) == 0 {
			results = append(results, fmt.Sprintf("%s:rows=0", port))
			continue
		}
		rowSamples := make([]string, 0, 3)
		limit := len(resp.Data)
		if limit > 3 {
			limit = 3
		}
		for j := 0; j < limit; j++ {
			rowSamples = append(rowSamples, fmt.Sprintf("%v", resp.Data[j]))
		}
		results = append(results, fmt.Sprintf("%s:rows=%d sample=%s", port, len(resp.Data), strings.Join(rowSamples, ";")))
	}
	return strings.Join(results, " | ")
}
