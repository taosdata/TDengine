package run

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"tdsqlsmith/internal/executor"
	"tdsqlsmith/internal/report"
	"tdsqlsmith/internal/taosdwatch"
)

func withInitFailureStubs(
	t *testing.T,
	newFn func(context.Context, string) (*executor.Executor, error),
	shouldFn func(string, error) bool,
	handleFn func(context.Context, string, string, error) taosdwatch.Incident,
) {
	t.Helper()
	prevNew := executorNewFn
	prevEnsure := taosdEnsureRunning
	prevShould := taosdShouldHandle
	prevHandle := taosdHandleIncident
	executorNewFn = newFn
	taosdEnsureRunning = func(context.Context) (string, string, error) { return "", "", nil }
	taosdShouldHandle = shouldFn
	taosdHandleIncident = handleFn
	t.Cleanup(func() {
		executorNewFn = prevNew
		taosdEnsureRunning = prevEnsure
		taosdShouldHandle = prevShould
		taosdHandleIncident = prevHandle
	})
}

func TestHandleInitConnectionFailureNotHandled(t *testing.T) {
	withInitFailureStubs(
		t,
		func(context.Context, string) (*executor.Executor, error) {
			t.Fatalf("unexpected executor retry")
			return nil, nil
		},
		func(string, error) bool { return false },
		func(context.Context, string, string, error) taosdwatch.Incident {
			t.Fatalf("unexpected taosd handle call")
			return taosdwatch.Incident{}
		},
	)

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 1)
	crashIncidents := make([]report.CrashIncident, 0, 1)
	var seq int64
	errIn := errors.New("dial tcp failed")
	gotExec, gotErr := handleInitConnectionFailure(context.Background(), "dsn", errIn, &stats, func(string, string) {}, &taosdIncidents, &crashIncidents, &seq)
	if gotExec != nil {
		t.Fatalf("expected nil executor")
	}
	if !errors.Is(gotErr, errIn) {
		t.Fatalf("expected original error, got %v", gotErr)
	}
}

func TestHandleInitConnectionFailureRestartSuccess(t *testing.T) {
	withInitFailureStubs(
		t,
		func(context.Context, string) (*executor.Executor, error) { return &executor.Executor{}, nil },
		func(string, error) bool { return true },
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       time.Now(),
				Checked:          true,
				ExecClass:        "conn_lost",
				RestartAttempted: true,
				RestartSucceeded: true,
			}
		},
	)

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 1)
	crashIncidents := make([]report.CrashIncident, 0, 1)
	var seq int64
	gotExec, gotErr := handleInitConnectionFailure(context.Background(), "dsn", errors.New("unable to establish connection"), &stats, func(string, string) {}, &taosdIncidents, &crashIncidents, &seq)
	if gotErr != nil {
		t.Fatalf("unexpected error: %v", gotErr)
	}
	if gotExec == nil {
		t.Fatalf("expected retried executor")
	}
	if stats.ConnLost != 1 {
		t.Fatalf("expected conn_lost=1, got %d", stats.ConnLost)
	}
	if stats.TaosdRestart != 1 {
		t.Fatalf("expected taosd_restart=1, got %d", stats.TaosdRestart)
	}
	if len(taosdIncidents) != 1 {
		t.Fatalf("expected taosd incident recorded")
	}
	if taosdIncidents[0].CaseID != "init_ping" {
		t.Fatalf("unexpected case id: %s", taosdIncidents[0].CaseID)
	}
	if len(crashIncidents) != 0 {
		t.Fatalf("unexpected crash incident without crash signal/core evidence: %d", len(crashIncidents))
	}
	if seq != 0 {
		t.Fatalf("unexpected incident seq: %d", seq)
	}
}

func TestHandleInitConnectionFailureRecordsCrashWhenCoredumpDetected(t *testing.T) {
	withInitFailureStubs(
		t,
		func(context.Context, string) (*executor.Executor, error) { return &executor.Executor{}, nil },
		func(string, error) bool { return true },
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       time.Now(),
				Checked:          true,
				ExecClass:        "conn_lost",
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal SIGSEGV (core_dump=true)",
				RestartAttempted: true,
				RestartSucceeded: true,
			}
		},
	)

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 1)
	crashIncidents := make([]report.CrashIncident, 0, 1)
	var seq int64
	gotExec, gotErr := handleInitConnectionFailure(context.Background(), "dsn", errors.New("unable to establish connection"), &stats, func(string, string) {}, &taosdIncidents, &crashIncidents, &seq)
	if gotErr != nil {
		t.Fatalf("unexpected error: %v", gotErr)
	}
	if gotExec == nil {
		t.Fatalf("expected retried executor")
	}
	if stats.TaosdCoredump != 1 {
		t.Fatalf("expected taosd coredump stats, got %d", stats.TaosdCoredump)
	}
	if len(crashIncidents) != 1 {
		t.Fatalf("expected crash incident recorded, got %d", len(crashIncidents))
	}
	if crashIncidents[0].CrashSQL != "" {
		t.Fatalf("unexpected init crash sql: %q", crashIncidents[0].CrashSQL)
	}
	if seq != 1 {
		t.Fatalf("unexpected incident seq: %d", seq)
	}
}

func TestHandleInitConnectionFailureRecordsCrashWhenCoredumpDetectedAfterAutoRestart(t *testing.T) {
	withInitFailureStubs(
		t,
		func(context.Context, string) (*executor.Executor, error) { return nil, errors.New("retry failed") },
		func(string, error) bool { return true },
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       time.Now(),
				Checked:          true,
				ExecClass:        "conn_lost",
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal SIGABRT (core_dump=true)",
				RestartAttempted: true,
				RestartSucceeded: true,
			}
		},
	)

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 1)
	crashIncidents := make([]report.CrashIncident, 0, 1)
	var seq int64
	// Use a short timeout context to avoid hanging in waitExecutorReady loop
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	gotExec, gotErr := handleInitConnectionFailure(ctx, "dsn", errors.New("unable to establish connection"), &stats, func(string, string) {}, &taosdIncidents, &crashIncidents, &seq)
	if gotExec != nil {
		t.Fatalf("expected nil executor")
	}
	if gotErr == nil {
		t.Fatalf("expected wrapped error")
	}
	if stats.TaosdCoredump != 1 {
		t.Fatalf("expected taosd coredump stats, got %d", stats.TaosdCoredump)
	}
	if len(crashIncidents) != 1 {
		t.Fatalf("expected crash incident recorded, got %d", len(crashIncidents))
	}
	if seq != 1 {
		t.Fatalf("unexpected incident seq: %d", seq)
	}
}

func TestHandleInitConnectionFailureIncludesStatusDetails(t *testing.T) {
	withInitFailureStubs(
		t,
		func(context.Context, string) (*executor.Executor, error) { return nil, errors.New("retry failed") },
		func(string, error) bool { return true },
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       time.Now(),
				Checked:          true,
				ExecClass:        "conn_lost",
				ExitReason:       "managed_taosd_exit exit_code=0",
				RestartAttempted: false,
				RestartSucceeded: false,
			}
		},
	)

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 1)
	crashIncidents := make([]report.CrashIncident, 0, 1)
	var seq int64
	// Use a short timeout context to avoid hanging in waitExecutorReady loop
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	gotExec, gotErr := handleInitConnectionFailure(ctx, "dsn", errors.New("unable to establish connection"), &stats, func(string, string) {}, &taosdIncidents, &crashIncidents, &seq)
	if gotExec != nil {
		t.Fatalf("expected nil executor")
	}
	if gotErr == nil {
		t.Fatalf("expected wrapped error")
	}
	msg := gotErr.Error()
	if !strings.Contains(msg, "exit_reason") {
		t.Fatalf("missing exit reason detail: %s", msg)
	}
	if len(taosdIncidents) != 1 {
		t.Fatalf("expected taosd incident recorded")
	}
	if len(crashIncidents) != 0 {
		t.Fatalf("unexpected crash incidents when taosd process still exists: %d", len(crashIncidents))
	}
}
