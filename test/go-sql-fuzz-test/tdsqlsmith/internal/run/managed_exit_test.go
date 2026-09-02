package run

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"tdsqlsmith/internal/crashguard"
	"tdsqlsmith/internal/report"
	"tdsqlsmith/internal/taosdwatch"
)

func withManagedExitStubs(
	t *testing.T,
	lastFn func(time.Time) (time.Time, bool),
	handleFn func(context.Context, string, string, error) taosdwatch.Incident,
) {
	t.Helper()
	prevLast := taosdLastManagedAt
	prevHandle := taosdHandleIncident
	taosdLastManagedAt = lastFn
	taosdHandleIncident = handleFn
	t.Cleanup(func() {
		taosdLastManagedAt = prevLast
		taosdHandleIncident = prevHandle
	})
}

func TestCaptureManagedExitIncidentRecordsCrashWithoutConnLostSignal(t *testing.T) {
	exitAt := time.Now().Add(-2 * time.Second).UTC().Truncate(time.Second)
	incidentAt := exitAt.Add(30 * time.Millisecond)
	withManagedExitStubs(
		t,
		func(since time.Time) (time.Time, bool) {
			if since.IsZero() || !exitAt.Before(since) {
				return exitAt, true
			}
			return time.Time{}, false
		},
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       incidentAt,
				Checked:          true,
				ExecClass:        "conn_lost",
				ProcessExists:    true,
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal segmentation fault (core_dump=true)",
				ExitReason:       "managed_taosd_exit signal=segmentation fault core_dump=true",
			}
		},
	)

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 1)
	crashIncidents := make([]report.CrashIncident, 0, 1)
	var seq int64
	var lastSeen time.Time
	errMap := map[string]int64{}
	recordErr := func(kind, msg string) {
		errMap[kind+":"+msg]++
	}

	forceFlush := captureManagedExitIncident(
		context.Background(),
		nil,
		"",
		&stats,
		recordErr,
		&taosdIncidents,
		&crashIncidents,
		&seq,
		&lastSeen,
		"",
		"",
	)
	if !forceFlush {
		t.Fatalf("expected force flush for managed crash incident")
	}
	if len(taosdIncidents) != 1 {
		t.Fatalf("expected 1 taosd incident, got %d", len(taosdIncidents))
	}
	if taosdIncidents[0].CaseID != "managed_exit" {
		t.Fatalf("unexpected case id: %q", taosdIncidents[0].CaseID)
	}
	if len(crashIncidents) != 1 {
		t.Fatalf("expected 1 crash incident, got %d", len(crashIncidents))
	}
	if crashIncidents[0].CrashSQL != "" {
		t.Fatalf("unexpected crash sql: %q", crashIncidents[0].CrashSQL)
	}
	if seq != 1 {
		t.Fatalf("unexpected incident seq: %d", seq)
	}
	if stats.TaosdCoredump != 1 {
		t.Fatalf("expected taosd coredump=1, got %d", stats.TaosdCoredump)
	}
	if !lastSeen.Equal(incidentAt) {
		t.Fatalf("unexpected last seen: got=%s want=%s", lastSeen, incidentAt)
	}
	if len(errMap) == 0 {
		t.Fatalf("expected taosd coredump error to be recorded")
	}
}

func TestCaptureManagedExitIncidentSkipsAlreadySeenExit(t *testing.T) {
	exitAt := time.Now().Add(-2 * time.Second).UTC().Truncate(time.Second)
	incidentAt := exitAt.Add(10 * time.Millisecond)
	withManagedExitStubs(
		t,
		func(since time.Time) (time.Time, bool) {
			if since.IsZero() || !exitAt.Before(since) {
				return exitAt, true
			}
			return time.Time{}, false
		},
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       incidentAt,
				Checked:          true,
				ExecClass:        "conn_lost",
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal segmentation fault (core_dump=true)",
				ExitReason:       "managed_taosd_exit signal=segmentation fault core_dump=true",
			}
		},
	)

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 2)
	crashIncidents := make([]report.CrashIncident, 0, 2)
	var seq int64
	var lastSeen time.Time

	_ = captureManagedExitIncident(context.Background(), nil, "", &stats, func(string, string) {}, &taosdIncidents, &crashIncidents, &seq, &lastSeen, "", "")
	_ = captureManagedExitIncident(context.Background(), nil, "", &stats, func(string, string) {}, &taosdIncidents, &crashIncidents, &seq, &lastSeen, "", "")

	if len(taosdIncidents) != 1 {
		t.Fatalf("expected deduped taosd incidents, got %d", len(taosdIncidents))
	}
	if len(crashIncidents) != 1 {
		t.Fatalf("expected deduped crash incidents, got %d", len(crashIncidents))
	}
	if seq != 1 {
		t.Fatalf("unexpected incident seq: %d", seq)
	}
}

func TestShouldRecordTaosdCrashIgnoresFilesystemCoreFile(t *testing.T) {
	// With parent-child model, filesystem-based core file detection is not used.
	// Only evidence from direct process monitoring ("managed taosd exited...") is accepted.
	inc := taosdwatch.Incident{
		CoredumpDetected: true,
		CoredumpEvidence: "recent core file: /tmp/core.taosd.12345",
	}
	if shouldRecordTaosdCrash(inc) {
		t.Fatalf("expected filesystem core file evidence to be ignored with parent-child model")
	}
}

func TestShouldRecordTaosdCrashAcceptsManagedExitEvidence(t *testing.T) {
	// Parent-child model requires evidence from managed exit metadata
	inc := taosdwatch.Incident{
		CoredumpDetected: true,
		CoredumpEvidence: "managed taosd exited by signal SIGSEGV (core_dump=true)",
	}
	if !shouldRecordTaosdCrash(inc) {
		t.Fatalf("expected managed exit evidence to be accepted")
	}
}

func TestCaptureManagedExitIncidentUsesCrashPendingSQL(t *testing.T) {
	exitAt := time.Now().Add(-2 * time.Second).UTC().Truncate(time.Second)
	incidentAt := exitAt.Add(15 * time.Millisecond)
	withManagedExitStubs(
		t,
		func(since time.Time) (time.Time, bool) {
			if since.IsZero() || !exitAt.Before(since) {
				return exitAt, true
			}
			return time.Time{}, false
		},
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       incidentAt,
				Checked:          true,
				ExecClass:        "conn_lost",
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal segmentation fault (core_dump=true)",
				ExitReason:       "managed_taosd_exit signal=segmentation fault core_dump=true",
			}
		},
	)

	tmpDir := t.TempDir()
	latestPath := filepath.Join(tmpDir, "report.latest.json")
	snap := crashguard.Snapshot{
		Pending: &crashguard.PendingStatement{
			SQL: "select 1",
		},
	}
	body, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if err := os.WriteFile(latestPath, append(body, '\n'), 0o644); err != nil {
		t.Fatalf("write latest snapshot: %v", err)
	}

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 1)
	crashIncidents := make([]report.CrashIncident, 0, 1)
	var seq int64
	var lastSeen time.Time

	forceFlush := captureManagedExitIncident(
		context.Background(),
		nil,
		"",
		&stats,
		func(string, string) {},
		&taosdIncidents,
		&crashIncidents,
		&seq,
		&lastSeen,
		"",
		latestPath,
	)
	if !forceFlush {
		t.Fatalf("expected force flush for managed crash incident")
	}
	if len(crashIncidents) != 1 {
		t.Fatalf("expected 1 crash incident, got %d", len(crashIncidents))
	}
	if crashIncidents[0].CrashSQL != "select 1" {
		t.Fatalf("unexpected crash sql: %q", crashIncidents[0].CrashSQL)
	}
}

func TestLatestCrashCandidateSQLUsesWindowFallback(t *testing.T) {
	tmpDir := t.TempDir()
	latestPath := filepath.Join(tmpDir, "report.latest.json")
	snap := crashguard.Snapshot{
		Window: []crashguard.ExecutedStmt{
			{SQL: ""},
			{SQL: "select 3"},
		},
	}
	body, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if err := os.WriteFile(latestPath, append(body, '\n'), 0o644); err != nil {
		t.Fatalf("write latest snapshot: %v", err)
	}
	if got := latestCrashCandidateSQL(latestPath); got != "select 3" {
		t.Fatalf("unexpected crash sql fallback: %q", got)
	}
}

func TestCaptureManagedExitIncidentUsesLastCrashSQLWhenPendingCleared(t *testing.T) {
	exitAt := time.Now().Add(-2 * time.Second).UTC().Truncate(time.Second)
	incidentAt := exitAt.Add(20 * time.Millisecond)
	withManagedExitStubs(
		t,
		func(since time.Time) (time.Time, bool) {
			if since.IsZero() || !exitAt.Before(since) {
				return exitAt, true
			}
			return time.Time{}, false
		},
		func(context.Context, string, string, error) taosdwatch.Incident {
			return taosdwatch.Incident{
				OccurredAt:       incidentAt,
				Checked:          true,
				ExecClass:        "conn_lost",
				CoredumpDetected: true,
				CoredumpEvidence: "managed taosd exited by signal segmentation fault (core_dump=true)",
				ExitReason:       "managed_taosd_exit signal=segmentation fault core_dump=true",
			}
		},
	)

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 1)
	crashIncidents := make([]report.CrashIncident, 0, 1)
	var seq int64
	var lastSeen time.Time

	forceFlush := captureManagedExitIncident(
		context.Background(),
		nil,
		"",
		&stats,
		func(string, string) {},
		&taosdIncidents,
		&crashIncidents,
		&seq,
		&lastSeen,
		"select 2",
		"",
	)
	if !forceFlush {
		t.Fatalf("expected force flush for managed crash incident")
	}
	if len(crashIncidents) != 1 {
		t.Fatalf("expected 1 crash incident, got %d", len(crashIncidents))
	}
	if crashIncidents[0].CrashSQL != "select 2" {
		t.Fatalf("unexpected crash sql: %q", crashIncidents[0].CrashSQL)
	}
	if len(taosdIncidents) != 1 {
		t.Fatalf("expected 1 taosd incident, got %d", len(taosdIncidents))
	}
	if taosdIncidents[0].SQL != "select 2" {
		t.Fatalf("unexpected taosd incident sql: %q", taosdIncidents[0].SQL)
	}
}
