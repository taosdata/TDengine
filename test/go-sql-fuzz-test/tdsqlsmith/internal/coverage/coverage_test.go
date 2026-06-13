package coverage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tdsqlsmith/internal/branchmodel"
	"tdsqlsmith/internal/report"
)

func TestWriteMarkdown(t *testing.T) {
	dir := t.TempDir()
	r := &report.RunReport{
		RunID:  "r1",
		Seed:   1,
		OutDir: dir,
		Coverage: branchmodel.CoverageSummary{
			Required:    10,
			Hit:         10,
			RequiredNeg: 5,
			HitNeg:      5,
		},
		CrashGuardDir:         "/tmp/run/crash_guard",
		CrashLatestReport:     "/tmp/run/crash_guard/report.latest.json",
		SupervisorCrashReport: "/tmp/run/crash_guard/coredump_report.json",
		CoredumpStatements: []report.CoredumpStatement{
			{
				OccurredAt:       time.Unix(1710000000, 0),
				IncidentID:       "incident_000001",
				QueryNo:          156,
				CaseID:           "SEL_001",
				Rule:             "query_expression",
				ExecClass:        "conn_lost",
				SQL:              "select v from t1;",
				CandidateSQL:     "select v from t1;",
				Error:            "unable to establish connection",
				CoredumpEvidence: "systemctl indicates core-dump",
				FailureID:        "failure_00000156.json",
				PrecedingWindow: []report.ExecutedStmtRef{
					{
						QueryNo:    155,
						OccurredAt: time.Unix(1710000000, 0),
						ExecClass:  "ok",
						SQL:        "select ts from t1;",
					},
				},
			},
		},
	}
	out := filepath.Join(dir, "coverage.md")
	path, err := WriteMarkdown(r, out)
	if err != nil {
		t.Fatalf("write markdown failed: %v", err)
	}
	if path != out {
		t.Fatalf("unexpected output path: %s", path)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("output file missing: %v", err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output failed: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "## TAOSD Coredump Statements") {
		t.Fatalf("missing coredump section: %s", text)
	}
	if !strings.Contains(text, "select v from t1;") {
		t.Fatalf("missing coredump sql: %s", text)
	}
	if !strings.Contains(text, "incident_000001") {
		t.Fatalf("missing incident id: %s", text)
	}
	if !strings.Contains(text, "failure_00000156.json") {
		t.Fatalf("missing failure id: %s", text)
	}
	if !strings.Contains(text, "## Crash Guard") {
		t.Fatalf("missing crash guard section: %s", text)
	}
	if !strings.Contains(text, "report.latest.json") {
		t.Fatalf("missing crash latest path: %s", text)
	}
}
