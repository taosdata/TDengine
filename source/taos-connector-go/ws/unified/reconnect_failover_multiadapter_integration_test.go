package unified

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
)

const taosAuthHeader = "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"
const unifiedCrossTestDB = "test_unified_cross"

const (
	testDatabaseCleanupRetries  = 100
	testDatabaseCleanupInterval = 200 * time.Millisecond
)

// TestUnifiedCrossFailoverDisconnectDetectionAndImmediateReconnect verifies the expected behavior for this scenario.
func TestUnifiedCrossFailoverDisconnectDetectionAndImmediateReconnect(t *testing.T) {
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

	activeBefore := activeAdapterPort(t, c)
	standby := otherAdapterPort(activeBefore, ports)

	stopByPort(t, activeBefore, stops)

	recoverCost, lastErr := waitForSuccessfulInsert(t, c, "cross_reconnect", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, recoverCost, 2500*time.Millisecond, "disconnect should be detected and recovered quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should switch to standby")
}

// TestUnifiedCrossConcurrentSendFailoverAndSwitchBack verifies the expected behavior for this scenario.
func TestUnifiedCrossConcurrentSendFailoverAndSwitchBack(t *testing.T) {
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

	initialActive := activeAdapterPort(t, c)
	standby := otherAdapterPort(initialActive, ports)

	phase1Success, phase1Fail, err := runConcurrentInsertsWithFault(
		t, c, "phase1", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, initialActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase1Success, int32(0), "should have successful inserts during failover")
	t.Logf("phase1 success=%d fail=%d", phase1Success, phase1Fail)
	phase1RecoverCost, phase1LastErr := waitForSuccessfulInsert(t, c, "phase1_after_fault", 4*time.Second)
	require.NoError(t, phase1LastErr)
	assert.Less(t, phase1RecoverCost, 2500*time.Millisecond, "phase1 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == standby
	}, 4*time.Second, 50*time.Millisecond, "active endpoint should fail over to standby")

	stops[initialActive] = restartAdapterOnPort(t, initialActive)

	phase2Success, phase2Fail, err := runConcurrentInsertsWithFault(
		t, c, "phase2", 10, 20, 200*time.Millisecond, func() {
			stopByPort(t, standby, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, phase2Success, int32(0), "should keep handling concurrent inserts during switch-back")
	t.Logf("phase2 success=%d fail=%d", phase2Success, phase2Fail)
	phase2RecoverCost, phase2LastErr := waitForSuccessfulInsert(t, c, "phase2_after_fault", 4*time.Second)
	require.NoError(t, phase2LastErr)
	assert.Less(t, phase2RecoverCost, 2500*time.Millisecond, "phase2 reconnect should finish quickly")

	require.Eventually(t, func() bool {
		return activeAdapterPort(t, c) == initialActive
	}, 5*time.Second, 50*time.Millisecond, "active endpoint should switch back after recovery")

	finalRecoverCost, lastErr := waitForSuccessfulInsert(t, c, "phase2_final", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, finalRecoverCost, 2500*time.Millisecond, "switch-back reconnect should finish quickly")
}

// TestUnifiedCrossMultiNodeFailoverChainUnderConcurrency verifies the expected behavior for this scenario.
func TestUnifiedCrossMultiNodeFailoverChainUnderConcurrency(t *testing.T) {
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

	firstActive := activeAdapterPort(t, c)
	secondActive := ""
	for _, p := range ports {
		if p != firstActive {
			secondActive = p
			break
		}
	}
	require.NotEmpty(t, secondActive)

	success1, fail1, err := runConcurrentInsertsWithFault(
		t, c, "chain1", 8, 18, 150*time.Millisecond, func() {
			stopByPort(t, firstActive, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success1, int32(0))
	t.Logf("chain1 success=%d fail=%d", success1, fail1)
	chain1RecoverCost, chain1LastErr := waitForSuccessfulInsert(t, c, "chain1_after_fault", 4*time.Second)
	require.NoError(t, chain1LastErr)
	assert.Less(t, chain1RecoverCost, 2500*time.Millisecond)

	require.Eventually(t, func() bool {
		current := activeAdapterPort(t, c)
		return current != firstActive
	}, 4*time.Second, 50*time.Millisecond)

	currentAfterFirst := activeAdapterPort(t, c)
	success2, fail2, err := runConcurrentInsertsWithFault(
		t, c, "chain2", 8, 18, 150*time.Millisecond, func() {
			stopByPort(t, currentAfterFirst, stops)
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success2, int32(0))
	t.Logf("chain2 success=%d fail=%d", success2, fail2)
	chain2RecoverCost, chain2LastErr := waitForSuccessfulInsert(t, c, "chain2_after_fault", 4*time.Second)
	require.NoError(t, chain2LastErr)
	assert.Less(t, chain2RecoverCost, 2500*time.Millisecond)

	require.Eventually(t, func() bool {
		current := activeAdapterPort(t, c)
		return current != firstActive && current != currentAfterFirst
	}, 5*time.Second, 50*time.Millisecond, "should fail over to the third available endpoint")

	finalRecoverCost, lastErr := waitForSuccessfulInsert(t, c, "chain_final", 4*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, finalRecoverCost, 2500*time.Millisecond)
}

// TestUnifiedCrossDualNodeJitterWithConcurrentSchemalessWrites verifies the expected behavior for this scenario.
func TestUnifiedCrossDualNodeJitterWithConcurrentSchemalessWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	runDualNodeJitterScenario(t, 12, 25, 150*time.Millisecond, 100*time.Millisecond)
}

// TestUnifiedCrossDualNodeJitterLoop verifies the expected behavior for this scenario.
func TestUnifiedCrossDualNodeJitterLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	loopCount := loopCountFromEnv("LOOP_COUNT", 8)
	for i := 0; i < loopCount; i++ {
		round := i + 1
		t.Run(fmt.Sprintf("round_%02d", round), func(t *testing.T) {
			runDualNodeJitterScenario(t, 10, 20, 150*time.Millisecond, 100*time.Millisecond)
		})
	}
}

func runDualNodeJitterScenario(t *testing.T, workers, perWorker int, firstFaultDelay time.Duration, secondFaultGap time.Duration) {
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

	firstActive := activeAdapterPort(t, c)
	secondTarget := ""
	for _, p := range ports {
		if p != firstActive {
			secondTarget = p
			break
		}
	}
	require.NotEmpty(t, secondTarget)

	success, failed, err := runConcurrentInsertsWithFault(
		t, c, "dual_jitter", workers, perWorker, firstFaultDelay, func() {
			stopByPort(t, firstActive, stops)
			time.Sleep(secondFaultGap)
			if _, ok := stops[secondTarget]; ok {
				stopByPort(t, secondTarget, stops)
			}
		},
	)
	require.NoError(t, err)
	assert.Greater(t, success, int32(0), "concurrent schemaless writes should keep succeeding during dual jitter")
	t.Logf("dual_jitter success=%d fail=%d", success, failed)
	jitterRecoverCost, jitterLastErr := waitForSuccessfulInsert(t, c, "dual_jitter_after_fault", 5*time.Second)
	require.NoError(t, jitterLastErr)
	assert.Less(t, jitterRecoverCost, 3500*time.Millisecond, "dual jitter recovery should complete quickly")

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

	recoverCost, lastErr := waitForSuccessfulInsert(t, c, "dual_jitter_final", 5*time.Second)
	require.NoError(t, lastErr)
	assert.Less(t, recoverCost, 3500*time.Millisecond, "dual jitter recovery should complete quickly")
}

func loopCountFromEnv(envName string, defaultCount int) int {
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

func ensureTaosadapterBinary(t *testing.T) {
	t.Helper()
	cmd := "taosadapter"
	if runtime.GOOS == "windows" {
		cmd = "C:\\TDengine\\taosadapter.exe"
	}
	if _, err := exec.LookPath(cmd); err != nil {
		t.Skipf("taosadapter not found: %v", err)
	}
}

func startAdapters(t *testing.T, n int) ([]string, map[string]func()) {
	t.Helper()
	ports := make([]string, 0, n)
	stops := make(map[string]func(), n)
	for i := 0; i < n; i++ {
		var (
			port string
			stop func()
			err  error
		)
		for attempt := 0; attempt < 8; attempt++ {
			port = getFreePort(t)
			stop, err = startAdapterOnPort(t, port)
			if err == nil {
				break
			}
		}
		require.NoError(t, err)
		ports = append(ports, port)
		stops[port] = stop
	}
	return ports, stops
}

func startAdapterOnPort(t *testing.T, port string) (func(), error) {
	t.Helper()
	command := "taosadapter"
	if runtime.GOOS == "windows" {
		command = "C:\\TDengine\\taosadapter.exe"
	}
	cmd := exec.Command(command, "--port", port, "--logLevel", "debug")
	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !isCmdAlive(cmd) {
			_ = stopCmdWithTimeout(cmd)
			return nil, fmt.Errorf("taosadapter exited before ready on port %s, logs: %s", port, logs.String())
		}
		if pingAdapter(port) {
			if !isCmdAlive(cmd) {
				_ = stopCmdWithTimeout(cmd)
				return nil, fmt.Errorf("taosadapter exited before ready on port %s, logs: %s", port, logs.String())
			}
			return func() {
				_ = stopCmdWithTimeout(cmd)
			}, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = stopCmdWithTimeout(cmd)
	return nil, fmt.Errorf("taosadapter start timeout on port %s, logs: %s", port, logs.String())
}

func restartAdapterOnPort(t *testing.T, port string) func() {
	t.Helper()
	stop, err := startAdapterOnPort(t, port)
	require.NoError(t, err)
	return stop
}

func stopByPort(t *testing.T, port string, stops map[string]func()) {
	t.Helper()
	stop, ok := stops[port]
	require.Truef(t, ok, "no running adapter on port %s", port)
	if stop != nil {
		stop()
		delete(stops, port)
		require.Eventuallyf(t, func() bool {
			return !pingAdapter(port)
		}, 3*time.Second, 100*time.Millisecond, "taosadapter on port %s should be down after stop", port)
	}
}

func pingAdapter(port string) bool {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func stopCmdWithTimeout(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_, _ = cmd.Process.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
	return nil
}

func isCmdAlive(cmd *exec.Cmd) bool {
	if cmd == nil || cmd.Process == nil {
		return false
	}
	if runtime.GOOS == "windows" {
		return cmd.ProcessState == nil
	}
	return cmd.Process.Signal(syscall.Signal(0)) == nil
}

func getFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		// Some sandboxed environments disallow opening local listen sockets.
		if strings.Contains(strings.ToLower(err.Error()), "operation not permitted") {
			t.Skipf("skip integration test: local tcp listen is not permitted: %v", err)
		}
		require.NoError(t, err)
	}
	defer func() {
		_ = l.Close()
	}()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func createTestDatabase(t *testing.T, ports []string) string {
	t.Helper()

	// Best-effort cleanup for historical fixed-name residue from previous runs.
	cleanupTMQCrossStaleTopics(ports)
	_ = dropDatabaseWithRetry(ports, unifiedCrossTestDB, testDatabaseCleanupRetries, testDatabaseCleanupInterval)

	dbName := fmt.Sprintf("%s_%d", unifiedCrossTestDB, time.Now().UnixNano())
	createSQL := fmt.Sprintf("create database if not exists %s vgroups 1 buffer 64 pages 64", dbName)
	if err := execSQLOnAnyPort(ports, createSQL); err != nil {
		t.Skipf("failed to create integration database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		if err := dropDatabaseWithRetry(ports, dbName, testDatabaseCleanupRetries, testDatabaseCleanupInterval); err != nil {
			t.Logf("cleanup drop database %s failed: %v", dbName, err)
		}
	})
	return dbName
}

func dropDatabaseWithRetry(ports []string, db string, retries int, interval time.Duration) error {
	if retries <= 0 {
		retries = 1
	}
	var lastErr error
	for i := 0; i < retries; i++ {
		lastErr = execSQLOnAnyPort(ports, fmt.Sprintf("drop database if exists %s", db))
		if lastErr == nil {
			return nil
		}
		if strings.Contains(strings.ToLower(lastErr.Error()), "topic must be dropped first") {
			cleanupTMQCrossStaleTopics(ports)
		}
		if i+1 < retries && interval > 0 {
			time.Sleep(interval)
		}
	}
	return lastErr
}

func execSQLOnAnyPort(ports []string, sql string) error {
	var lastErr error
	for i := 0; i < len(ports); i++ {
		err := execSQLOnPort(ports[i], sql)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return newInvalidStateErrorf("no port available")
	}
	return lastErr
}

func execSQLOnPort(port string, sql string) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/rest/sql", port), strings.NewReader(sql))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", taosAuthHeader)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return newInvalidStateErrorf("http status %d for sql %q", resp.StatusCode, sql)
	}
	restResp, err := common.UnmarshalRestfulBody(resp.Body, 1024)
	if err != nil {
		return err
	}
	if restResp.Code != 0 {
		return newInvalidStateErrorf("sql failed code=%d desc=%s sql=%q", restResp.Code, restResp.Desc, sql)
	}
	return nil
}

func querySQLOnPort(port string, sql string) (*common.TDEngineRestfulResp, error) {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%s/rest/sql", port), strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", taosAuthHeader)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, newInvalidStateErrorf("http status %d for sql %q", resp.StatusCode, sql)
	}
	restResp, err := common.UnmarshalRestfulBody(resp.Body, 1024)
	if err != nil {
		return nil, err
	}
	return restResp, nil
}

func newIntegrationUnifiedClient(t *testing.T, ports []string, db string) *Client {
	t.Helper()
	endpoints := make([]string, 0, len(ports))
	for _, p := range ports {
		endpoints = append(endpoints, fmt.Sprintf("ws://127.0.0.1:%s", p))
	}
	cfg := NewConfig(endpoints)
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

func activeAdapterPort(t *testing.T, c *Client) string {
	t.Helper()
	active := c.failover.active().URL
	u, err := url.Parse(active)
	require.NoError(t, err)
	_, port, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)
	return port
}

func otherAdapterPort(active string, ports []string) string {
	for _, p := range ports {
		if p != active {
			return p
		}
	}
	return ""
}

func waitForSuccessfulInsert(t *testing.T, c *Client, phase string, timeout time.Duration) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	var lastErr error
	i := 0
	for time.Since(start) < timeout {
		err := c.SchemalessInsert(0, buildLine(phase, 0, i), 1, "ns", 0, "")
		if err == nil {
			return time.Since(start), nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
		i++
	}
	return time.Since(start), lastErr
}

func runConcurrentInsertsWithFault(t *testing.T, c *Client, phase string, workers, perWorker int, faultDelay time.Duration, faultFn func()) (int32, int32, error) {
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
				err := c.SchemalessInsert(0, buildLine(phase, workerID, i), 1, "ns", 0, "")
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
		return atomic.LoadInt32(&successCount), atomic.LoadInt32(&failCount), newInvalidStateErrorf("concurrent inserts blocked during phase %s", phase)
	}
}

func buildLine(phase string, workerID, i int) string {
	ts := time.Now().UnixNano()
	return fmt.Sprintf("unified_cross_failover,phase=%s,worker=%d value=%di %d", phase, workerID, i, ts)
}
