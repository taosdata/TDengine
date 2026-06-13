package report

import (
	"testing"
	"time"

	"tdsqlsmith/internal/queryrules"
)

func TestNormalizeSQLTerminator(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "", want: ""},
		{in: "select 1", want: "select 1;"},
		{in: "select 1;", want: "select 1;"},
		{in: "  use db  ", want: "use db;"},
	}
	for _, tc := range cases {
		got := NormalizeSQLTerminator(tc.in)
		if got != tc.want {
			t.Fatalf("NormalizeSQLTerminator(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestNormalizeSetupSQL(t *testing.T) {
	got := NormalizeSetupSQL([]string{
		"create database if not exists test",
		"  ",
		"use test;",
	})
	if len(got) != 2 {
		t.Fatalf("expected 2 sqls, got %d (%v)", len(got), got)
	}
	if got[0] != "create database if not exists test;" {
		t.Fatalf("unexpected first sql: %q", got[0])
	}
	if got[1] != "use test;" {
		t.Fatalf("unexpected second sql: %q", got[1])
	}
}

func TestMinimalRunReportNormalize(t *testing.T) {
	mini := &MinimalRunReport{
		RunID:       "r1",
		GeneratedAt: zeroTime(),
		SetupSQL: []string{
			"drop database if exists x",
			"use x;",
			"   ",
		},
		TaosdIncidents: []CrashIncident{
			{CrashSQL: " select * from t1 "},
			{IncidentID: "old", CrashSQL: "  "},
		},
		TDsqlsmithIncidents: []CrashIncident{
			{CrashSQL: "select * from t2"},
		},
		QueryRuleCoverage: queryrules.Summary{
			Required:      10,
			Hit:           7,
			Missing:       []string{" joined_table ", "fill_opt", "joined_table"},
			CoverageRatio: 0.1,
		},
		QueryRuleProgress: []QueryRuleProgressPoint{
			{QueryNo: 20, Required: 10, Hit: 6, Missing: 4, TopMissing: []string{" fill_opt "}},
			{QueryNo: 20, Required: 10, Hit: 7, Missing: 3, TopMissing: []string{"joined_table"}},
			{QueryNo: -1, Required: 1, Hit: 1},
		},
		QueryComboCounts: map[string]int64{
			" join ": 3,
			"":       2,
			"where":  0,
		},
	}

	mini.Normalize()

	if len(mini.SetupSQL) != 2 {
		t.Fatalf("expected 2 setup sqls, got %d", len(mini.SetupSQL))
	}
	if mini.SetupSQL[0] != "drop database if exists x;" {
		t.Fatalf("unexpected setup sql[0]: %q", mini.SetupSQL[0])
	}
	if mini.SetupSQL[1] != "use x;" {
		t.Fatalf("unexpected setup sql[1]: %q", mini.SetupSQL[1])
	}
	if mini.TaosdIncidents[0].IncidentID != "incident_000001" {
		t.Fatalf("unexpected taosd incident id[0]: %s", mini.TaosdIncidents[0].IncidentID)
	}
	if mini.TaosdIncidents[1].IncidentID != "incident_000002" {
		t.Fatalf("unexpected taosd incident id[1]: %s", mini.TaosdIncidents[1].IncidentID)
	}
	if mini.TDsqlsmithIncidents[0].IncidentID != "incident_000003" {
		t.Fatalf("unexpected tdsqlsmith incident id: %s", mini.TDsqlsmithIncidents[0].IncidentID)
	}
	if mini.TaosdIncidents[0].CrashSQL != "select * from t1" {
		t.Fatalf("unexpected normalized crash sql: %q", mini.TaosdIncidents[0].CrashSQL)
	}
	if mini.IncidentCount() != 3 {
		t.Fatalf("unexpected incident count: %d", mini.IncidentCount())
	}
	if mini.QueryRuleCoverage.CoverageRatio != 0.7 {
		t.Fatalf("unexpected query rule ratio: %v", mini.QueryRuleCoverage.CoverageRatio)
	}
	if len(mini.QueryRuleCoverage.Missing) != 2 || mini.QueryRuleCoverage.Missing[0] != "fill_opt" {
		t.Fatalf("unexpected query rule missing: %v", mini.QueryRuleCoverage.Missing)
	}
	if len(mini.QueryRuleProgress) != 1 || mini.QueryRuleProgress[0].Hit != 7 {
		t.Fatalf("unexpected rule progress: %+v", mini.QueryRuleProgress)
	}
	if mini.QueryComboCounts["join"] != 3 || len(mini.QueryComboCounts) != 1 {
		t.Fatalf("unexpected query combos: %v", mini.QueryComboCounts)
	}
}

func zeroTime() time.Time {
	return time.Unix(0, 0).UTC()
}
