package run

import (
	"context"
	"testing"
	"time"

	"tdsqlsmith/internal/catalog"
	"tdsqlsmith/internal/executor"
)

func TestExecuteUsesPrepareWhenSkipBootstrap(t *testing.T) {
	prevNew := executorNewFn
	prevBootstrap := catalogBootstrapFn
	prevPrepare := catalogPrepareFn
	prevEnsure := taosdEnsureRunning
	t.Cleanup(func() {
		executorNewFn = prevNew
		catalogBootstrapFn = prevBootstrap
		catalogPrepareFn = prevPrepare
		taosdEnsureRunning = prevEnsure
	})

	taosdEnsureRunning = func(context.Context) (string, string, error) { return "", "", nil }
	executorNewFn = func(context.Context, string) (*executor.Executor, error) {
		return &executor.Executor{}, nil
	}
	catalogBootstrapFn = func(context.Context, *executor.Executor, int64, string) (*catalog.Prepared, catalog.CleanupFunc, error) {
		t.Fatalf("catalogBootstrapFn should not be called when SkipBootstrap=true")
		return nil, nil, nil
	}
	prepareCalled := 0
	catalogPrepareFn = func(context.Context, *executor.Executor, string) (*catalog.Prepared, catalog.CleanupFunc, error) {
		prepareCalled++
		return &catalog.Prepared{
			Database: "tdsqlsmith_shared",
			SetupSQL: catalog.BootstrapSetupSQL("tdsqlsmith_shared"),
		}, func(context.Context) {}, nil
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
		SkipBootstrap: true,
	})
	if err != nil {
		t.Fatalf("execute returned error: %v", err)
	}
	if prepareCalled != 1 {
		t.Fatalf("expected prepare called once, got %d", prepareCalled)
	}
}

func TestExecuteUsesBootstrapByDefault(t *testing.T) {
	prevNew := executorNewFn
	prevBootstrap := catalogBootstrapFn
	prevPrepare := catalogPrepareFn
	prevEnsure := taosdEnsureRunning
	t.Cleanup(func() {
		executorNewFn = prevNew
		catalogBootstrapFn = prevBootstrap
		catalogPrepareFn = prevPrepare
		taosdEnsureRunning = prevEnsure
	})

	taosdEnsureRunning = func(context.Context) (string, string, error) { return "", "", nil }
	executorNewFn = func(context.Context, string) (*executor.Executor, error) {
		return &executor.Executor{}, nil
	}
	bootstrapCalled := 0
	catalogBootstrapFn = func(context.Context, *executor.Executor, int64, string) (*catalog.Prepared, catalog.CleanupFunc, error) {
		bootstrapCalled++
		return &catalog.Prepared{
			Database: "tdsqlsmith_shared",
			SetupSQL: catalog.BootstrapSetupSQL("tdsqlsmith_shared"),
		}, func(context.Context) {}, nil
	}
	catalogPrepareFn = func(context.Context, *executor.Executor, string) (*catalog.Prepared, catalog.CleanupFunc, error) {
		t.Fatalf("catalogPrepareFn should not be called when SkipBootstrap=false")
		return nil, nil, nil
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
	if bootstrapCalled != 1 {
		t.Fatalf("expected bootstrap called once, got %d", bootstrapCalled)
	}
}
