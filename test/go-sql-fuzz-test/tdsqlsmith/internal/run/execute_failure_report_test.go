package run

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tdsqlsmith/internal/executor"
	"tdsqlsmith/internal/report"
	"tdsqlsmith/internal/taosdwatch"
)

func TestExecuteWritesIncidentReportOnInitFailure(t *testing.T) {
	occurredAt := time.Now().UTC().Truncate(time.Second)
	withInitFailureStubs(
		t,
		func(context.Context, string) (*executor.Executor, error) {
			return nil, errors.New("ping TDengine: [0xb] Unable to establish connection")
		},
		func(string, error) bool { return true },
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       occurredAt,
				Checked:          true,
				ExecClass:        "conn_lost",
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal SIGSEGV (core_dump=true)",
				RestartAttempted: true,
				RestartSucceeded: false,
			}
		},
	)

	outDir := t.TempDir()
	runCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := Execute(runCtx, Config{
		Version:         "test",
		DSN:             "root:taosdata@tcp(127.0.0.1:6030)/",
		Seed:            20260226,
		Cases:           5,
		StmtTimeout:     time.Second,
		OutDir:          outDir,
		MutationLevel:   0,
		StopWhenCovered: false,
		DryRun:          false,
		Verbose:         false,
	})
	if err == nil {
		t.Fatalf("expected execute to return init failure")
	}
	if !strings.Contains(err.Error(), "taosd recovery failed") {
		t.Fatalf("unexpected error text: %v", err)
	}

	entries, readErr := os.ReadDir(outDir)
	if readErr != nil {
		t.Fatalf("read out dir failed: %v", readErr)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one run dir, got %d", len(entries))
	}
	reportPath := filepath.Join(outDir, entries[0].Name(), "run_report.json")
	mini, readReportErr := report.ReadMinimalRunReport(reportPath)
	if readReportErr != nil {
		t.Fatalf("read minimal run report failed: %v", readReportErr)
	}
	if len(mini.TaosdIncidents) != 1 {
		t.Fatalf("expected one taosd incident, got %d", len(mini.TaosdIncidents))
	}
	if len(mini.TDsqlsmithIncidents) != 0 {
		t.Fatalf("unexpected tdsqlsmith incidents: %d", len(mini.TDsqlsmithIncidents))
	}
	incident := mini.TaosdIncidents[0]
	if incident.CrashSQL != "" {
		t.Fatalf("unexpected init crash sql: %q", incident.CrashSQL)
	}
	if len(mini.SetupSQL) == 0 {
		t.Fatalf("expected setup sql to be present")
	}
	if mini.TotalExecuted != 0 {
		t.Fatalf("expected total_executed=0 on init failure, got %d", mini.TotalExecuted)
	}
}
