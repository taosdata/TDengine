package crashguard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRecorderBeforeAfterAndLoadLatest(t *testing.T) {
	runDir := t.TempDir()
	rec, err := New("run_001", runDir, 2)
	if err != nil {
		t.Fatalf("new recorder failed: %v", err)
	}

	if err := rec.Before(PendingStatement{
		OccurredAt: time.Unix(1710000000, 0),
		RunID:      "run_001",
		QueryNo:    100,
		CaseID:     "QGEN",
		Rule:       "query_random",
		Phase:      string(PhaseExec),
		RNGState:   "rng_state_100",
		SQL:        "select 1;",
	}); err != nil {
		t.Fatalf("before failed: %v", err)
	}
	pendingSnap, err := LoadLatest(rec.LatestPath())
	if err != nil {
		t.Fatalf("load latest pending failed: %v", err)
	}
	if pendingSnap.Pending == nil || pendingSnap.Pending.RNGState != "rng_state_100" {
		t.Fatalf("unexpected pending rng state: %+v", pendingSnap.Pending)
	}
	if err := rec.After(&ExecutedStmt{
		QueryNo:    100,
		OccurredAt: time.Unix(1710000001, 0),
		CaseID:     "QGEN",
		Rule:       "query_random",
		ExecClass:  "ok",
		SQL:        "select 1;",
	}); err != nil {
		t.Fatalf("after failed: %v", err)
	}

	got, err := LoadLatest(rec.LatestPath())
	if err != nil {
		t.Fatalf("load latest failed: %v", err)
	}
	if got.Pending != nil {
		t.Fatalf("pending should be cleared after after(), got=%+v", got.Pending)
	}
	if len(got.Window) != 1 {
		t.Fatalf("unexpected window length: %d", len(got.Window))
	}
	if got.Window[0].SQL != "select 1;" {
		t.Fatalf("unexpected window sql: %q", got.Window[0].SQL)
	}
	if got.ExecutedTotal != 1 {
		t.Fatalf("expected executed_total=1, got %d", got.ExecutedTotal)
	}
	if got.CleanExit {
		t.Fatalf("clean_exit should be false before mark")
	}

	// Load by directory path should also work.
	byDir, err := LoadLatest(rec.Dir())
	if err != nil {
		t.Fatalf("load latest by dir failed: %v", err)
	}
	if byDir.RunID != "run_001" {
		t.Fatalf("unexpected run id: %q", byDir.RunID)
	}
}

func TestRecorderWindowTrimAndCleanExit(t *testing.T) {
	runDir := t.TempDir()
	rec, err := New("run_002", runDir, 2)
	if err != nil {
		t.Fatalf("new recorder failed: %v", err)
	}

	for i := 1; i <= 3; i++ {
		if err := rec.Before(PendingStatement{
			RunID:   "run_002",
			QueryNo: int64(i),
			Phase:   string(PhaseExec),
			SQL:     fmt.Sprintf("select %d;", i),
		}); err != nil {
			t.Fatalf("before %d failed: %v", i, err)
		}
		if err := rec.After(&ExecutedStmt{
			QueryNo:    int64(i),
			OccurredAt: time.Unix(1710000000+int64(i), 0),
			ExecClass:  "ok",
			SQL:        "select x;",
		}); err != nil {
			t.Fatalf("after %d failed: %v", i, err)
		}
	}

	got, err := LoadLatest(rec.LatestPath())
	if err != nil {
		t.Fatalf("load latest failed: %v", err)
	}
	if len(got.Window) != 2 {
		t.Fatalf("expected trimmed window=2, got %d", len(got.Window))
	}
	if got.Window[0].QueryNo != 2 || got.Window[1].QueryNo != 3 {
		t.Fatalf("unexpected window tail: %+v", got.Window)
	}
	if got.ExecutedTotal != 3 {
		t.Fatalf("expected executed_total=3, got %d", got.ExecutedTotal)
	}

	if err := rec.MarkCleanExit(); err != nil {
		t.Fatalf("mark clean exit failed: %v", err)
	}
	got, err = LoadLatest(rec.LatestPath())
	if err != nil {
		t.Fatalf("load latest after clean exit failed: %v", err)
	}
	if !got.CleanExit {
		t.Fatalf("clean_exit should be true")
	}
	if got.ExecutedTotal != 3 {
		t.Fatalf("expected executed_total to be kept on clean exit, got %d", got.ExecutedTotal)
	}
	statusPath := filepath.Join(runDir, "crash_guard", "status.json")
	body, err := os.ReadFile(statusPath)
	if err != nil {
		t.Fatalf("read status failed: %v", err)
	}
	if !strings.Contains(string(body), "\"clean_exit\": true") {
		t.Fatalf("status should contain clean_exit=true, got: %s", string(body))
	}
}
