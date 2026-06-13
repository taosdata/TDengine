package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"tdsqlsmith/internal/catalog"
	"tdsqlsmith/internal/config"
	"tdsqlsmith/internal/crashguard"
	"tdsqlsmith/internal/executor"
	"tdsqlsmith/internal/report"
	"tdsqlsmith/internal/taosdwatch"
)

func TestIsCrashSignalName(t *testing.T) {
	cases := []struct {
		signal string
		want   bool
	}{
		{signal: "segmentation fault", want: true},
		{signal: "aborted", want: true},
		{signal: "bus error", want: true},
		{signal: "illegal instruction", want: true},
		{signal: "floating point exception", want: true},
		{signal: "trace/breakpoint trap", want: true},
		{signal: "terminated", want: false},
		{signal: "killed", want: false},
		{signal: "", want: false},
	}
	for _, tc := range cases {
		got := isCrashSignalName(tc.signal)
		if got != tc.want {
			t.Fatalf("isCrashSignalName(%q)=%v want %v", tc.signal, got, tc.want)
		}
	}
}

func TestResolveSupervisorRunIdentity_Default(t *testing.T) {
	t.Setenv(envRunIDOverride, "")
	t.Setenv(envRunDirOverride, "")

	start := time.Date(2026, 2, 25, 1, 2, 3, 0, time.UTC)
	outDir := t.TempDir()
	runID, runDir, err := resolveSupervisorRunIdentity(start, outDir, 42)
	if err != nil {
		t.Fatalf("resolveSupervisorRunIdentity returned error: %v", err)
	}
	wantID := report.MakeRunID(start, 42)
	if runID != wantID {
		t.Fatalf("runID=%q want %q", runID, wantID)
	}
	wantDir, _ := filepath.Abs(filepath.Join(outDir, wantID))
	if runDir != wantDir {
		t.Fatalf("runDir=%q want %q", runDir, wantDir)
	}
}

func TestResolveSupervisorRunIdentity_EnvOverride(t *testing.T) {
	start := time.Date(2026, 2, 25, 1, 2, 3, 0, time.UTC)
	outDir := t.TempDir()
	customDir := filepath.Join(t.TempDir(), "custom-run")
	t.Setenv(envRunIDOverride, "pc_fixed")
	t.Setenv(envRunDirOverride, customDir)

	runID, runDir, err := resolveSupervisorRunIdentity(start, outDir, 99)
	if err != nil {
		t.Fatalf("resolveSupervisorRunIdentity returned error: %v", err)
	}
	if runID != "pc_fixed" {
		t.Fatalf("runID=%q want %q", runID, "pc_fixed")
	}
	wantDir, _ := filepath.Abs(customDir)
	if runDir != wantDir {
		t.Fatalf("runDir=%q want %q", runDir, wantDir)
	}
}

func TestWriteSupervisorMinimalReport_GroupedByTDsqlsmith(t *testing.T) {
	runID := "run_test_01"
	runDir := t.TempDir()
	reportPath := filepath.Join(runDir, "run_report.json")

	if err := ensureMinimalRunReport(reportPath, runID); err != nil {
		t.Fatalf("ensureMinimalRunReport failed: %v", err)
	}

	crash := report.ProcessCrashReport{
		RunID:      runID,
		OccurredAt: time.Now().UTC(),
		Snapshot: &report.CrashSnapshotReport{
			ExecutedTotal: 7,
			Pending: &report.CrashPendingStatement{
				SQL: "select * from t1 where v > 1",
			},
		},
	}
	if err := writeSupervisorMinimalReport(reportPath, runID, crash); err != nil {
		t.Fatalf("writeSupervisorMinimalReport failed: %v", err)
	}

	mini, err := report.ReadMinimalRunReport(reportPath)
	if err != nil {
		t.Fatalf("ReadMinimalRunReport failed: %v", err)
	}
	if mini.RunID != runID {
		t.Fatalf("unexpected run id: %s", mini.RunID)
	}
	if len(mini.SetupSQL) == 0 {
		t.Fatalf("expected shared setup sql to be present")
	}
	if len(mini.TaosdIncidents) != 0 {
		t.Fatalf("unexpected taosd incidents: %d", len(mini.TaosdIncidents))
	}
	if len(mini.TDsqlsmithIncidents) != 1 {
		t.Fatalf("expected one tdsqlsmith incident, got %d", len(mini.TDsqlsmithIncidents))
	}
	if mini.TDsqlsmithIncidents[0].CrashSQL != "select * from t1 where v > 1" {
		t.Fatalf("unexpected crash sql: %q", mini.TDsqlsmithIncidents[0].CrashSQL)
	}
	if mini.TotalExecuted != 7 {
		t.Fatalf("expected total_executed=7, got %d", mini.TotalExecuted)
	}
}

func TestWriteSupervisorMinimalReport_AccumulatesExecutedTotal(t *testing.T) {
	runID := "run_test_02"
	runDir := t.TempDir()
	reportPath := filepath.Join(runDir, "run_report.json")

	if err := ensureMinimalRunReport(reportPath, runID); err != nil {
		t.Fatalf("ensureMinimalRunReport failed: %v", err)
	}

	first := report.ProcessCrashReport{
		RunID:      runID,
		OccurredAt: time.Now().UTC(),
		Snapshot: &report.CrashSnapshotReport{
			ExecutedTotal: 5,
			Pending: &report.CrashPendingStatement{
				SQL: "select 1",
			},
		},
	}
	if err := writeSupervisorMinimalReport(reportPath, runID, first); err != nil {
		t.Fatalf("writeSupervisorMinimalReport(first) failed: %v", err)
	}

	second := report.ProcessCrashReport{
		RunID:      runID,
		OccurredAt: time.Now().UTC(),
		Snapshot: &report.CrashSnapshotReport{
			ExecutedTotal: 3,
			Pending: &report.CrashPendingStatement{
				SQL: "select 2",
			},
		},
	}
	if err := writeSupervisorMinimalReport(reportPath, runID, second); err != nil {
		t.Fatalf("writeSupervisorMinimalReport(second) failed: %v", err)
	}

	mini, err := report.ReadMinimalRunReport(reportPath)
	if err != nil {
		t.Fatalf("ReadMinimalRunReport failed: %v", err)
	}
	if mini.TotalExecuted != 8 {
		t.Fatalf("expected total_executed=8, got %d", mini.TotalExecuted)
	}
	if len(mini.TDsqlsmithIncidents) != 2 {
		t.Fatalf("expected two tdsqlsmith incidents, got %d", len(mini.TDsqlsmithIncidents))
	}
}

func TestCrashSQLAndExecutedTotalFallbackToLatestPath(t *testing.T) {
	runDir := t.TempDir()
	rec, err := crashguard.New("run_test_latest", runDir, 8)
	if err != nil {
		t.Fatalf("init crash recorder failed: %v", err)
	}
	if err := rec.Before(crashguard.PendingStatement{
		OccurredAt: time.Now(),
		RunID:      "run_test_latest",
		QueryNo:    9,
		CaseID:     "QGEN",
		Rule:       "query_random",
		Phase:      string(crashguard.PhaseExec),
		RNGState:   "state_9",
		SQL:        "select 9;",
	}); err != nil {
		t.Fatalf("write pending snapshot failed: %v", err)
	}

	sqlText, executedTotal := crashSQLAndExecutedTotal(report.ProcessCrashReport{
		RunID:      "run_test_latest",
		LatestPath: rec.LatestPath(),
	})
	if sqlText != "select 9;" {
		t.Fatalf("unexpected crash sql fallback: %q", sqlText)
	}
	if executedTotal != 1 {
		t.Fatalf("unexpected executed_total fallback: %d", executedTotal)
	}
}

func TestDetectSupervisorTaosdCrash(t *testing.T) {
	prevShould := supervisorTaosdShouldHandleFn
	prevHandle := supervisorTaosdHandleFn
	t.Cleanup(func() {
		supervisorTaosdShouldHandleFn = prevShould
		supervisorTaosdHandleFn = prevHandle
	})

	gotShouldClass := "unset"
	gotHandleClass := "unset"
	supervisorTaosdShouldHandleFn = func(class string, _ error) bool {
		gotShouldClass = class
		return true
	}
	supervisorTaosdHandleFn = func(_ context.Context, class, _ string, _ error) taosdwatch.Incident {
		gotHandleClass = class
		return taosdwatch.Incident{
			Checked:           true,
			CoredumpDetected:  true,
			CoredumpEvidence:  "managed taosd exited by signal segmentation fault (core_dump=true)",
			ExitReason:        "managed_taosd_exit signal=segmentation fault core_dump=true",
			ProcessExists:     true,
			RestartAttempted:  false,
			RestartSucceeded:  false,
			ReconnectRequired: true,
		}
	}

	inc, ok := detectSupervisorTaosdCrash(errors.New("unable to establish connection"), nil)
	if !ok {
		t.Fatalf("expected supervisor taosd crash to be detected")
	}
	if !inc.CoredumpDetected {
		t.Fatalf("expected coredump-detected incident")
	}
	if gotShouldClass != "" {
		t.Fatalf("expected should-handle class to be empty, got %q", gotShouldClass)
	}
	if gotHandleClass != "" {
		t.Fatalf("expected handle class to be empty, got %q", gotHandleClass)
	}
}

func TestShouldRecordSupervisorTaosdCrashIgnoresGenericCoreFile(t *testing.T) {
	inc := taosdwatch.Incident{
		Checked:          true,
		ProcessExists:    true,
		CoredumpDetected: true,
		CoredumpEvidence: "recent core file: /tmp/core.12345",
	}
	if shouldRecordSupervisorTaosdCrash(inc) {
		t.Fatalf("expected generic core file to be ignored for taosd crash classification")
	}
}

func TestShouldRecordSupervisorTaosdCrashAcceptsTaosdCoreEvidence(t *testing.T) {
	inc := taosdwatch.Incident{
		Checked:          true,
		ProcessExists:    true,
		CoredumpDetected: true,
		CoredumpEvidence: "recent core file: /tmp/core.taosd.12345",
	}
	if !shouldRecordSupervisorTaosdCrash(inc) {
		t.Fatalf("expected taosd core evidence to be accepted")
	}
}

func TestDetectSupervisorTaosdCrashSkipsWorkerSignalCrash(t *testing.T) {
	prevShould := supervisorTaosdShouldHandleFn
	prevHandle := supervisorTaosdHandleFn
	t.Cleanup(func() {
		supervisorTaosdShouldHandleFn = prevShould
		supervisorTaosdHandleFn = prevHandle
	})

	supervisorTaosdShouldHandleFn = taosdwatch.ShouldHandle
	handleCalled := false
	supervisorTaosdHandleFn = func(context.Context, string, string, error) taosdwatch.Incident {
		handleCalled = true
		return taosdwatch.Incident{Checked: true}
	}

	_, ok := detectSupervisorTaosdCrash(errors.New("signal: segmentation fault (core dumped)"), nil)
	if ok {
		t.Fatalf("expected worker signal crash not to be classified as taosd crash")
	}
	if handleCalled {
		t.Fatalf("expected taosd handle not to be called for worker signal crash")
	}
}

func TestDetectSupervisorTaosdCrashSkipsWorkerSignalCrashWithPendingSQL(t *testing.T) {
	prevShould := supervisorTaosdShouldHandleFn
	prevHandle := supervisorTaosdHandleFn
	t.Cleanup(func() {
		supervisorTaosdShouldHandleFn = prevShould
		supervisorTaosdHandleFn = prevHandle
	})

	supervisorTaosdShouldHandleFn = taosdwatch.ShouldHandle
	handleCalled := false
	supervisorTaosdHandleFn = func(context.Context, string, string, error) taosdwatch.Incident {
		handleCalled = true
		return taosdwatch.Incident{Checked: true}
	}

	crash := &report.ProcessCrashReport{
		Signal:   "segmentation fault",
		CoreDump: true,
		Snapshot: &report.CrashSnapshotReport{
			Pending: &report.CrashPendingStatement{
				SQL: "select distinct vb, si as x, *, trim('s_504') not between false and true or not v between ceil(f) and usi from t2 order by +(d) limit 33;",
			},
		},
	}

	_, ok := detectSupervisorTaosdCrash(errors.New("signal: segmentation fault (core dumped)"), crash)
	if ok {
		t.Fatalf("expected worker signal crash with pending SQL not to be classified as taosd crash")
	}
	if handleCalled {
		t.Fatalf("expected taosd handle not to be called for worker signal crash")
	}
}

func TestAppendSupervisorTaosdIncident(t *testing.T) {
	runID := "run_test_taosd_01"
	runDir := t.TempDir()
	reportPath := filepath.Join(runDir, "run_report.json")
	if err := ensureMinimalRunReport(reportPath, runID); err != nil {
		t.Fatalf("ensureMinimalRunReport failed: %v", err)
	}

	crash := report.ProcessCrashReport{
		RunID:      runID,
		OccurredAt: time.Now().UTC(),
		Snapshot: &report.CrashSnapshotReport{
			Pending: &report.CrashPendingStatement{
				SQL: "select 42",
			},
		},
	}
	if err := appendSupervisorTaosdIncident(reportPath, runID, crash, taosdwatch.Incident{}); err != nil {
		t.Fatalf("appendSupervisorTaosdIncident failed: %v", err)
	}

	mini, err := report.ReadMinimalRunReport(reportPath)
	if err != nil {
		t.Fatalf("ReadMinimalRunReport failed: %v", err)
	}
	if len(mini.TaosdIncidents) != 1 {
		t.Fatalf("expected one taosd incident, got %d", len(mini.TaosdIncidents))
	}
	if mini.TaosdIncidents[0].CrashSQL != "select 42" {
		t.Fatalf("unexpected taosd crash sql: %q", mini.TaosdIncidents[0].CrashSQL)
	}
	if len(mini.TDsqlsmithIncidents) != 0 {
		t.Fatalf("unexpected tdsqlsmith incidents: %d", len(mini.TDsqlsmithIncidents))
	}
}

func TestBuildRunConfigSkipBootstrapFromWorker(t *testing.T) {
	t.Setenv(envRunWorker, "1")
	parsed := &config.Parsed{
		Run: config.RunConfig{
			DSN:             "root:taosdata@tcp(127.0.0.1:6030)/",
			Seed:            1,
			Cases:           10,
			StmtTimeout:     time.Second,
			OutDir:          t.TempDir(),
			MutationLevel:   1,
			StopWhenCovered: false,
		},
	}
	cfg := buildRunConfig(parsed)
	if !cfg.SkipBootstrap {
		t.Fatalf("expected worker run config SkipBootstrap=true")
	}
}

func TestWorkerCommandContextUsesHardDeadlineGrace(t *testing.T) {
	prevGrace := supervisorWorkerDeadlineGrace
	supervisorWorkerDeadlineGrace = 2 * time.Second
	t.Cleanup(func() {
		supervisorWorkerDeadlineGrace = prevGrace
	})

	softDeadline := time.Now().Add(3 * time.Second)
	ctx, cancel := workerCommandContext(context.Background(), softDeadline)
	defer cancel()

	gotDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("expected worker command context to carry deadline")
	}
	want := softDeadline.Add(2 * time.Second)
	diff := gotDeadline.Sub(want)
	if diff < 0 {
		diff = -diff
	}
	if diff > 200*time.Millisecond {
		t.Fatalf("unexpected hard deadline: got=%s want≈%s diff=%s", gotDeadline, want, diff)
	}
}

func TestWorkerCommandContextNoDeadline(t *testing.T) {
	parent := context.Background()
	ctx, cancel := workerCommandContext(parent, time.Time{})
	defer cancel()

	if ctx != parent {
		t.Fatalf("expected parent context passthrough without deadline")
	}
	if dl, ok := ctx.Deadline(); ok {
		t.Fatalf("did not expect deadline, got %s", dl)
	}
}

func TestInitializeSharedCatalogWithRetryRecovers(t *testing.T) {
	prevNew := supervisorExecutorNewFn
	prevBootstrap := supervisorCatalogBootstrapFn
	prevShould := supervisorTaosdShouldHandleFn
	prevHandle := supervisorTaosdHandleFn
	prevProbe := supervisorProbeSQLFn
	prevBackoff := supervisorBootstrapRetryBackoff
	t.Cleanup(func() {
		supervisorExecutorNewFn = prevNew
		supervisorCatalogBootstrapFn = prevBootstrap
		supervisorTaosdShouldHandleFn = prevShould
		supervisorTaosdHandleFn = prevHandle
		supervisorProbeSQLFn = prevProbe
		supervisorBootstrapRetryBackoff = prevBackoff
	})

	supervisorBootstrapRetryBackoff = 2 * time.Millisecond
	newCalls := 0
	supervisorExecutorNewFn = func(context.Context, string) (*executor.Executor, error) {
		newCalls++
		if newCalls == 1 {
			return nil, errors.New("dial tcp: connection refused")
		}
		return &executor.Executor{}, nil
	}
	bootstrapCalls := 0
	supervisorCatalogBootstrapFn = func(context.Context, *executor.Executor, int64, string) (*catalog.Prepared, catalog.CleanupFunc, error) {
		bootstrapCalls++
		return &catalog.Prepared{}, func(context.Context) {}, nil
	}
	supervisorProbeSQLFn = func(context.Context, *executor.Executor) error { return nil }
	supervisorTaosdShouldHandleFn = func(string, error) bool { return true }
	handleCalls := 0
	supervisorTaosdHandleFn = func(context.Context, string, string, error) taosdwatch.Incident {
		handleCalls++
		return taosdwatch.Incident{Checked: true, RestartAttempted: true, RestartSucceeded: true}
	}

	err := initializeSharedCatalogWithRetry(context.Background(), "root:taosdata@tcp(127.0.0.1:6030)/", 1, time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("initializeSharedCatalogWithRetry returned error: %v", err)
	}
	if bootstrapCalls != 1 {
		t.Fatalf("expected bootstrap once, got %d", bootstrapCalls)
	}
	if handleCalls == 0 {
		t.Fatalf("expected taosd handle called on initial failure")
	}
}

func TestInitializeSharedCatalogWithRetryDeadline(t *testing.T) {
	prevNew := supervisorExecutorNewFn
	prevBootstrap := supervisorCatalogBootstrapFn
	prevShould := supervisorTaosdShouldHandleFn
	prevHandle := supervisorTaosdHandleFn
	prevProbe := supervisorProbeSQLFn
	prevBackoff := supervisorBootstrapRetryBackoff
	t.Cleanup(func() {
		supervisorExecutorNewFn = prevNew
		supervisorCatalogBootstrapFn = prevBootstrap
		supervisorTaosdShouldHandleFn = prevShould
		supervisorTaosdHandleFn = prevHandle
		supervisorProbeSQLFn = prevProbe
		supervisorBootstrapRetryBackoff = prevBackoff
	})

	supervisorBootstrapRetryBackoff = 2 * time.Millisecond
	supervisorExecutorNewFn = func(context.Context, string) (*executor.Executor, error) {
		return nil, errors.New("still down")
	}
	supervisorCatalogBootstrapFn = func(context.Context, *executor.Executor, int64, string) (*catalog.Prepared, catalog.CleanupFunc, error) {
		t.Fatalf("bootstrap should not be called")
		return nil, nil, nil
	}
	supervisorTaosdShouldHandleFn = func(string, error) bool { return false }
	supervisorTaosdHandleFn = func(context.Context, string, string, error) taosdwatch.Incident {
		t.Fatalf("taosd handle should not be called")
		return taosdwatch.Incident{}
	}

	err := initializeSharedCatalogWithRetry(context.Background(), "root:taosdata@tcp(127.0.0.1:6030)/", 1, time.Now().Add(15*time.Millisecond))
	if err == nil {
		t.Fatalf("expected deadline error")
	}
}

func TestWaitSupervisorWorkerRestartHonorsDeadlineCap(t *testing.T) {
	prevBackoff := supervisorWorkerRestartBackoff
	supervisorWorkerRestartBackoff = 200 * time.Millisecond
	t.Cleanup(func() {
		supervisorWorkerRestartBackoff = prevBackoff
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := waitSupervisorWorkerRestart(ctx, time.Now().Add(20*time.Millisecond))
	if err != nil {
		t.Fatalf("waitSupervisorWorkerRestart returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 120*time.Millisecond {
		t.Fatalf("waitSupervisorWorkerRestart did not cap by deadline, elapsed=%s", elapsed)
	}
}

func TestWaitSupervisorWorkerRestartCanceledContext(t *testing.T) {
	prevBackoff := supervisorWorkerRestartBackoff
	supervisorWorkerRestartBackoff = 200 * time.Millisecond
	t.Cleanup(func() {
		supervisorWorkerRestartBackoff = prevBackoff
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitSupervisorWorkerRestart(ctx, time.Time{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}
