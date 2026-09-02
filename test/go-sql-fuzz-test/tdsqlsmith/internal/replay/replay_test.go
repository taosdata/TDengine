package replay

import (
	"strings"
	"testing"
	"time"

	"tdsqlsmith/internal/report"
)

func TestSelectReplayIncidentSQL_PicksLatestNonEmptyCrashSQL(t *testing.T) {
	base := time.Date(2026, 2, 28, 9, 0, 0, 0, time.UTC)
	mini := &report.MinimalRunReport{
		TaosdIncidents: []report.CrashIncident{
			{
				IncidentID: "incident_000001",
				OccurredAt: base.Add(1 * time.Minute),
				CrashSQL:   "select 1;",
			},
		},
		TDsqlsmithIncidents: []report.CrashIncident{
			{
				IncidentID: "incident_000002",
				OccurredAt: base.Add(2 * time.Minute),
				CrashSQL:   "",
			},
			{
				IncidentID: "incident_000003",
				OccurredAt: base.Add(3 * time.Minute),
				CrashSQL:   "select 3;",
			},
		},
	}

	sqlText, incidentID, err := selectReplayIncidentSQL(mini)
	if err != nil {
		t.Fatalf("selectReplayIncidentSQL() error = %v", err)
	}
	if incidentID != "incident_000003" {
		t.Fatalf("unexpected incident id: got=%q want=%q", incidentID, "incident_000003")
	}
	if sqlText != "select 3;" {
		t.Fatalf("unexpected sql: got=%q want=%q", sqlText, "select 3;")
	}
}

func TestSelectReplayIncidentSQL_NoReplayableIncident(t *testing.T) {
	mini := &report.MinimalRunReport{
		TaosdIncidents: []report.CrashIncident{
			{
				IncidentID: "incident_000001",
				CrashSQL:   "   ",
			},
		},
	}

	_, _, err := selectReplayIncidentSQL(mini)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no replayable crash_sql") {
		t.Fatalf("unexpected error: %v", err)
	}
}
