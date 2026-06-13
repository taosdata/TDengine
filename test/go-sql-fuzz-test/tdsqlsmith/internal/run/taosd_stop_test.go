package run

import (
	"context"
	"testing"
	"time"

	"tdsqlsmith/internal/catalog"
	"tdsqlsmith/internal/executor"
)

func TestExecuteStopsManagedTaosdOnExit(t *testing.T) {
	prevNew := executorNewFn
	prevBootstrap := catalogBootstrapFn
	prevPrepare := catalogPrepareFn
	prevEnsure := taosdEnsureRunning
	prevStop := taosdStopManaged
	t.Cleanup(func() {
		executorNewFn = prevNew
		catalogBootstrapFn = prevBootstrap
		catalogPrepareFn = prevPrepare
		taosdEnsureRunning = prevEnsure
		taosdStopManaged = prevStop
	})

	taosdEnsureRunning = func(context.Context) (string, string, error) { return "", "", nil }
	executorNewFn = func(context.Context, string) (*executor.Executor, error) {
		return &executor.Executor{}, nil
	}
	catalogBootstrapFn = func(context.Context, *executor.Executor, int64, string) (*catalog.Prepared, catalog.CleanupFunc, error) {
		return &catalog.Prepared{
			Database: "tdsqlsmith_shared",
			SetupSQL: catalog.BootstrapSetupSQL("tdsqlsmith_shared"),
		}, func(context.Context) {}, nil
	}
	catalogPrepareFn = func(context.Context, *executor.Executor, string) (*catalog.Prepared, catalog.CleanupFunc, error) {
		return &catalog.Prepared{
			Database: "tdsqlsmith_shared",
			SetupSQL: catalog.BootstrapSetupSQL("tdsqlsmith_shared"),
		}, func(context.Context) {}, nil
	}

	stopCalls := 0
	taosdStopManaged = func(context.Context) error {
		stopCalls++
		return nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Execute(runCtx, Config{
		Version:       "test",
		DSN:           "root:taosdata@tcp(127.0.0.1:6030)/",
		Seed:          20260227,
		Cases:         1,
		StmtTimeout:   time.Second,
		OutDir:        t.TempDir(),
		MutationLevel: 0,
		CrashGuard:    false,
		SkipBootstrap: false,
	})
	if err != nil {
		t.Fatalf("execute returned error: %v", err)
	}
	if stopCalls != 1 {
		t.Fatalf("expected taosd stop hook called once, got %d", stopCalls)
	}
}
