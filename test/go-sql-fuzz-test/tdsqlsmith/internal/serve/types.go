package serve

import "time"

type Config struct {
	Version     string
	Listen      string
	APIToken    string
	DataDir     string
	OutDir      string
	AllowOrigin string
}

type reportSummary struct {
	RunID                   string    `json:"run_id"`
	StartedAt               time.Time `json:"started_at"`
	GeneratedAt             time.Time `json:"generated_at"`
	ExecutionDurationMS     int64     `json:"execution_duration_ms"`
	Completed               bool      `json:"completed"`
	IncidentCount           int       `json:"incident_count"`
	TaosdIncidentCount      int       `json:"taosd_incident_count"`
	TDsqlsmithIncidentCount int       `json:"tdsqlsmith_incident_count"`
	TotalExecuted           int64     `json:"total_executed"`
	QueryRuleHit            int       `json:"query_rule_hit"`
	QueryRuleRequired       int       `json:"query_rule_required"`
	QueryRuleCoverageRatio  float64   `json:"query_rule_coverage_ratio"`
	QueryRuleMissingCount   int       `json:"query_rule_missing_count"`
}
