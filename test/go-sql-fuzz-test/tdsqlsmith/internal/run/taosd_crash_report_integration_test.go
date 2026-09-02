//go:build integration

package run

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tdsqlsmith/internal/report"
	"tdsqlsmith/internal/taosdwatch"
)

// TestTaosdCrashRecordedInReport verifies taosd crashes are recorded in reports.
func TestTaosdCrashRecordedInReport(t *testing.T) {
	// Create a temporary output directory.
	outDir, err := os.MkdirTemp("", "taosd_crash_report_test_*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(outDir)

	// Simulate a crash event.
	crashTime := time.Now().UTC()

	// Build a mock crash incident list.
	taosdCrashIncidents := []report.CrashIncident{
		{
			IncidentID: "incident_000001",
			OccurredAt: crashTime,
			CrashSQL:   "select /*+ hash_join() */ * from t1",
		},
	}

	// Build the report.
	reportPath := filepath.Join(outDir, "run_report.json")
	minimalReport := &report.MinimalRunReport{
		RunID:               "test_crash_run",
		GeneratedAt:         time.Now(),
		TotalExecuted:       1,
		TaosdIncidents:      taosdCrashIncidents,
		TDsqlsmithIncidents: []report.CrashIncident{},
	}
	minimalReport.Normalize()

	// Write report to disk.
	if err := report.WriteJSON(reportPath, minimalReport); err != nil {
		t.Fatalf("failed to write report: %v", err)
	}

	// Read report back and validate.
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("failed to read report: %v", err)
	}

	var loadedReport report.MinimalRunReport
	if err := json.Unmarshal(data, &loadedReport); err != nil {
		t.Fatalf("failed to parse report: %v", err)
	}

	// Ensure taosd_incidents exists.
	if len(loadedReport.TaosdIncidents) == 0 {
		t.Errorf("report has no taosd_incidents")
	}

	if len(loadedReport.TaosdIncidents) != 1 {
		t.Errorf("expected 1 taosd incident, got %d", len(loadedReport.TaosdIncidents))
	}

	incident := loadedReport.TaosdIncidents[0]
	if incident.IncidentID != "incident_000001" {
		t.Errorf("expected IncidentID incident_000001, got %s", incident.IncidentID)
	}

	if !strings.Contains(incident.CrashSQL, "hash_join") {
		t.Errorf("expected CrashSQL to contain hash_join, got %s", incident.CrashSQL)
	}

	t.Logf("report verification passed:")
	t.Logf("  - Taosd incidents: %d", len(loadedReport.TaosdIncidents))
	t.Logf("  - Incident ID: %s", incident.IncidentID)
	t.Logf("  - Crash SQL: %s", incident.CrashSQL)
}

// TestExitReasonHasCrashSignalWithSegmentationFault verifies segmentation fault detection.
func TestExitReasonHasCrashSignalWithSegmentationFault(t *testing.T) {
	testCases := []struct {
		exitReason string
		expected   bool
	}{
		{
			exitReason: "managed_taosd_exit signal=segmentation fault core_dump=true",
			expected:   true,
		},
		{
			exitReason: "managed_taosd_exit signal=aborted core_dump=true",
			expected:   true,
		},
		{
			exitReason: "signal 11",
			expected:   true,
		},
		{
			exitReason: "signal 6",
			expected:   true,
		},
		{
			exitReason: "managed_taosd_exit exit_code=0",
			expected:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.exitReason, func(t *testing.T) {
			result := exitReasonHasCrashSignal(tc.exitReason)
			if result != tc.expected {
				t.Errorf("exitReasonHasCrashSignal(%q) = %v, want %v",
					tc.exitReason, result, tc.expected)
			}
		})
	}
}

// TestShouldRecordTaosdCrashWithRealSignal verifies real crash signal recording.
func TestShouldRecordTaosdCrashWithRealSignal(t *testing.T) {
	// Simulate an incident converted from taosdwatch.Incident.
	inc := taosdwatch.Incident{
		OccurredAt:       time.Now(),
		ExecClass:        "conn_lost",
		SQL:              "select /*+ hash_join() */ * from t1",
		Error:            "connection lost",
		Checked:          true,
		ProcessExists:    false,
		ProcessCheck:     "pgrep reports taosd missing",
		ExitReason:       "managed_taosd_exit signal=segmentation fault core_dump=true",
		CoredumpDetected: true,
		CoredumpEvidence: "managed taosd exited by signal segmentation fault (core_dump=true)",
		RestartAttempted: true,
		RestartSucceeded: true,
	}

	if !shouldRecordTaosdCrash(inc) {
		t.Errorf("shouldRecordTaosdCrash should return true, got false")
		t.Errorf("Incident: ExitReason=%q, CoredumpDetected=%v",
			inc.ExitReason, inc.CoredumpDetected)
	}
}
