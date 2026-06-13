package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"tdsqlsmith/internal/branchmodel"
	"tdsqlsmith/internal/impedance"
	"tdsqlsmith/internal/queryrules"
)

type Stats struct {
	Generated     int64 `json:"generated"`
	Mutated       int64 `json:"mutated"`
	ParseReject   int64 `json:"parse_reject"`
	ParsePanic    int64 `json:"parse_panic"`
	Executed      int64 `json:"executed"`
	OK            int64 `json:"ok"`
	DBError       int64 `json:"db_error"`
	Timeout       int64 `json:"timeout"`
	ConnLost      int64 `json:"conn_lost"`
	Fatal         int64 `json:"fatal"`
	TaosdRestart  int64 `json:"taosd_restart"`
	TaosdCoredump int64 `json:"taosd_coredump"`
}

type ErrorCount struct {
	Message string `json:"message"`
	Count   int64  `json:"count"`
}

type TaosdIncident struct {
	OccurredAt       time.Time `json:"occurred_at"`
	ExecClass        string    `json:"exec_class"`
	CaseID           string    `json:"case_id,omitempty"`
	Rule             string    `json:"rule,omitempty"`
	SQL              string    `json:"sql"`
	Error            string    `json:"error"`
	ProcessExists    bool      `json:"process_exists"`
	ProcessCheck     string    `json:"process_check,omitempty"`
	ExitReason       string    `json:"exit_reason,omitempty"`
	CoredumpDetected bool      `json:"coredump_detected"`
	CoredumpEvidence string    `json:"coredump_evidence,omitempty"`
	RestartAttempted bool      `json:"restart_attempted"`
	RestartCommand   string    `json:"restart_command,omitempty"`
	RestartSucceeded bool      `json:"restart_succeeded"`
	RestartOutput    string    `json:"restart_output,omitempty"`
	RestartError     string    `json:"restart_error,omitempty"`
}

type CoredumpStatement struct {
	OccurredAt       time.Time         `json:"occurred_at"`
	IncidentID       string            `json:"incident_id,omitempty"`
	QueryNo          int64             `json:"query_no,omitempty"`
	CaseID           string            `json:"case_id,omitempty"`
	Rule             string            `json:"rule,omitempty"`
	ExecClass        string            `json:"exec_class"`
	SQL              string            `json:"sql"`
	CandidateSQL     string            `json:"candidate_sql,omitempty"`
	Error            string            `json:"error"`
	CoredumpEvidence string            `json:"coredump_evidence,omitempty"`
	ProcessCheck     string            `json:"process_check,omitempty"`
	ExitReason       string            `json:"exit_reason,omitempty"`
	RestartCommand   string            `json:"restart_command,omitempty"`
	RestartSucceeded bool              `json:"restart_succeeded,omitempty"`
	FailureID        string            `json:"failure_id,omitempty"`
	FailurePath      string            `json:"failure_path,omitempty"`
	PrecedingWindow  []ExecutedStmtRef `json:"preceding_window,omitempty"`
}

type CrashPendingStatement struct {
	OccurredAt time.Time `json:"occurred_at"`
	RunID      string    `json:"run_id,omitempty"`
	QueryNo    int64     `json:"query_no,omitempty"`
	CaseID     string    `json:"case_id,omitempty"`
	Rule       string    `json:"rule,omitempty"`
	Phase      string    `json:"phase,omitempty"`
	RNGState   string    `json:"rng_state,omitempty"`
	SQL        string    `json:"sql"`
}

type CrashSnapshotReport struct {
	RunID         string                 `json:"run_id,omitempty"`
	RunDir        string                 `json:"run_dir,omitempty"`
	UpdatedAt     time.Time              `json:"updated_at"`
	WorkerPID     int                    `json:"worker_pid,omitempty"`
	Pending       *CrashPendingStatement `json:"pending,omitempty"`
	Window        []ExecutedStmtRef      `json:"window,omitempty"`
	ExecutedTotal int64                  `json:"executed_total,omitempty"`
	CleanExit     bool                   `json:"clean_exit,omitempty"`
}

type ProcessCrashReport struct {
	RunID      string               `json:"run_id,omitempty"`
	RunDir     string               `json:"run_dir,omitempty"`
	Seed       int64                `json:"seed,omitempty"`
	OccurredAt time.Time            `json:"occurred_at"`
	Reason     string               `json:"reason,omitempty"`
	Signal     string               `json:"signal,omitempty"`
	ExitCode   int                  `json:"exit_code,omitempty"`
	CoreDump   bool                 `json:"core_dump,omitempty"`
	Error      string               `json:"error,omitempty"`
	LatestPath string               `json:"latest_path,omitempty"`
	Snapshot   *CrashSnapshotReport `json:"snapshot,omitempty"`
}

type ExecutedStmtRef struct {
	QueryNo    int64     `json:"query_no"`
	OccurredAt time.Time `json:"occurred_at"`
	CaseID     string    `json:"case_id,omitempty"`
	Rule       string    `json:"rule,omitempty"`
	ExecClass  string    `json:"exec_class"`
	SQL        string    `json:"sql"`
	Error      string    `json:"error,omitempty"`
	DurationMS int64     `json:"duration_ms,omitempty"`
}

type RunReport struct {
	RunID                 string                      `json:"run_id"`
	Version               string                      `json:"version"`
	StartedAt             time.Time                   `json:"started_at"`
	FinishedAt            time.Time                   `json:"finished_at"`
	DurationMS            int64                       `json:"duration_ms"`
	Seed                  int64                       `json:"seed"`
	DSNSummary            string                      `json:"dsn_summary"`
	OutDir                string                      `json:"out_dir"`
	DryRun                bool                        `json:"dry_run"`
	Cases                 int                         `json:"cases"`
	Duration              string                      `json:"duration"`
	StmtTimeout           string                      `json:"stmt_timeout"`
	MutationLevel         int                         `json:"mutation_level"`
	StopWhenCover         bool                        `json:"stop_when_covered"`
	CorpusDir             string                      `json:"corpus_dir"`
	RNGStateInitial       string                      `json:"rng_state_initial,omitempty"`
	RNGStateFinal         string                      `json:"rng_state_final,omitempty"`
	Coverage              branchmodel.CoverageSummary `json:"query_branch_coverage"`
	QueryRuleCoverage     queryrules.Summary          `json:"query_rule_coverage"`
	PositiveHits          []branchmodel.HitInfo       `json:"positive_hits"`
	NegativeHits          []branchmodel.HitInfo       `json:"negative_hits"`
	Stats                 Stats                       `json:"stats"`
	TopErrors             []ErrorCount                `json:"top_errors"`
	FamilyCounts          map[string]int64            `json:"family_counts,omitempty"`
	QueryComboCounts      map[string]int64            `json:"query_combo_counts,omitempty"`
	FailureArtifacts      []string                    `json:"failure_artifacts"`
	TaosdIncidents        []TaosdIncident             `json:"taosd_incidents,omitempty"`
	CoredumpStatements    []CoredumpStatement         `json:"coredump_statements,omitempty"`
	CoredumpIncidentCount int                         `json:"coredump_incident_count,omitempty"`
	CrashGuardDir         string                      `json:"crash_guard_dir,omitempty"`
	CrashLatestReport     string                      `json:"crash_latest_report,omitempty"`
	SupervisorCrashReport string                      `json:"supervisor_crash_report,omitempty"`
	ImpedanceRows         []impedance.Row             `json:"impedance,omitempty"`
}

type CrashIncident struct {
	IncidentID string    `json:"incident_id"`
	OccurredAt time.Time `json:"occurred_at"`
	CrashSQL   string    `json:"crash_sql"`
}

type QueryRuleProgressPoint struct {
	QueryNo       int64    `json:"query_no"`
	Hit           int      `json:"hit"`
	Required      int      `json:"required"`
	Missing       int      `json:"missing"`
	CoverageRatio float64  `json:"coverage_ratio"`
	TopMissing    []string `json:"top_missing,omitempty"`
}

type MinimalRunReport struct {
	RunID               string                   `json:"run_id"`
	StartedAt           time.Time                `json:"started_at"`
	GeneratedAt         time.Time                `json:"generated_at"`
	ExecutionDurationMS int64                    `json:"execution_duration_ms"`
	Completed           bool                     `json:"completed"`
	SetupSQL            []string                 `json:"setup_sql,omitempty"`
	TotalExecuted       int64                    `json:"total_executed"`
	QueryRuleCoverage   queryrules.Summary       `json:"query_rule_coverage"`
	QueryRuleProgress   []QueryRuleProgressPoint `json:"query_rule_progress,omitempty"`
	QueryComboCounts    map[string]int64         `json:"query_combo_counts,omitempty"`
	TaosdIncidents      []CrashIncident          `json:"taosd_incidents,omitempty"`
	TDsqlsmithIncidents []CrashIncident          `json:"tdsqlsmith_incidents,omitempty"`
}

func MakeRunID(start time.Time, seed int64) string {
	return fmt.Sprintf("%s_seed%d", start.Format("20060102_150405"), seed)
}

func WriteJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func ReadRunReport(path string) (*RunReport, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read run report: %w", err)
	}
	var out RunReport
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("unmarshal run report: %w", err)
	}
	return &out, nil
}

func ReadMinimalRunReport(path string) (*MinimalRunReport, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read run report: %w", err)
	}

	var mini MinimalRunReport
	if err := json.Unmarshal(b, &mini); err != nil {
		return nil, fmt.Errorf("unmarshal minimal run report: %w", err)
	}
	if strings.TrimSpace(mini.RunID) == "" {
		return nil, fmt.Errorf("unmarshal minimal run report: missing run_id")
	}
	mini.Normalize()
	return &mini, nil
}

func (r *MinimalRunReport) Normalize() {
	if r == nil {
		return
	}
	if r.StartedAt.IsZero() {
		r.StartedAt = r.GeneratedAt
	}
	if r.ExecutionDurationMS < 0 {
		r.ExecutionDurationMS = 0
	}
	r.SetupSQL = NormalizeSetupSQL(r.SetupSQL)
	r.QueryRuleCoverage = normalizeQueryRuleSummary(r.QueryRuleCoverage)
	r.QueryRuleProgress = normalizeRuleProgress(r.QueryRuleProgress)
	r.QueryComboCounts = normalizeCountMap(r.QueryComboCounts)
	r.TaosdIncidents = normalizeCrashIncidents(r.TaosdIncidents)
	r.TDsqlsmithIncidents = normalizeCrashIncidents(r.TDsqlsmithIncidents)
	seq := int64(1)
	for i := range r.TaosdIncidents {
		r.TaosdIncidents[i].IncidentID = formatIncidentID(seq)
		seq++
	}
	for i := range r.TDsqlsmithIncidents {
		r.TDsqlsmithIncidents[i].IncidentID = formatIncidentID(seq)
		seq++
	}
}

func (r *MinimalRunReport) IncidentCount() int {
	if r == nil {
		return 0
	}
	return len(r.TaosdIncidents) + len(r.TDsqlsmithIncidents)
}

func formatIncidentID(seq int64) string {
	if seq <= 0 {
		seq = 1
	}
	return fmt.Sprintf("incident_%06d", seq)
}

func normalizeCrashIncidents(items []CrashIncident) []CrashIncident {
	if len(items) == 0 {
		return nil
	}
	out := make([]CrashIncident, 0, len(items))
	for _, item := range items {
		sql := strings.TrimSpace(item.CrashSQL)
		out = append(out, CrashIncident{
			IncidentID: strings.TrimSpace(item.IncidentID),
			OccurredAt: item.OccurredAt,
			CrashSQL:   sql,
		})
	}
	return out
}

func normalizeQueryRuleSummary(in queryrules.Summary) queryrules.Summary {
	out := in
	if out.Required < 0 {
		out.Required = 0
	}
	if out.Hit < 0 {
		out.Hit = 0
	}
	if out.Required < out.Hit {
		out.Required = out.Hit
	}
	out.Missing = normalizeStringSlice(out.Missing)
	if out.Required > 0 {
		out.CoverageRatio = float64(out.Hit) / float64(out.Required)
	} else {
		out.CoverageRatio = 0
	}
	return out
}

func normalizeRuleProgress(items []QueryRuleProgressPoint) []QueryRuleProgressPoint {
	if len(items) == 0 {
		return nil
	}
	tmp := make([]QueryRuleProgressPoint, 0, len(items))
	for _, it := range items {
		if it.QueryNo <= 0 {
			continue
		}
		if it.Required < 0 {
			it.Required = 0
		}
		if it.Hit < 0 {
			it.Hit = 0
		}
		if it.Required < it.Hit {
			it.Required = it.Hit
		}
		if it.Missing < 0 {
			it.Missing = 0
		}
		if it.Required > 0 {
			it.CoverageRatio = float64(it.Hit) / float64(it.Required)
		} else {
			it.CoverageRatio = 0
		}
		it.TopMissing = normalizeStringSlice(it.TopMissing)
		tmp = append(tmp, it)
	}
	if len(tmp) == 0 {
		return nil
	}
	sort.Slice(tmp, func(i, j int) bool {
		if tmp[i].QueryNo == tmp[j].QueryNo {
			if tmp[i].Hit == tmp[j].Hit {
				return tmp[i].Required < tmp[j].Required
			}
			return tmp[i].Hit < tmp[j].Hit
		}
		return tmp[i].QueryNo < tmp[j].QueryNo
	})
	out := make([]QueryRuleProgressPoint, 0, len(tmp))
	for _, it := range tmp {
		if len(out) == 0 || out[len(out)-1].QueryNo != it.QueryNo {
			out = append(out, it)
			continue
		}
		out[len(out)-1] = it
	}
	return out
}

func normalizeCountMap(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for key, value := range in {
		k := strings.TrimSpace(key)
		if k == "" || value <= 0 {
			continue
		}
		out[k] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

func DSNSummary(dsn string) string {
	if dsn == "" {
		return ""
	}
	at := strings.Index(dsn, "@")
	if at == -1 {
		return "***"
	}
	return "***" + dsn[at:]
}

func ToTopErrors(m map[string]int64, limit int) []ErrorCount {
	out := make([]ErrorCount, 0, len(m))
	for k, v := range m {
		out = append(out, ErrorCount{Message: k, Count: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Message < out[j].Message
		}
		return out[i].Count > out[j].Count
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func NormalizeSQLTerminator(sql string) string {
	s := strings.TrimSpace(sql)
	if s == "" {
		return ""
	}
	if strings.HasSuffix(s, ";") {
		return s
	}
	return s + ";"
}

func NormalizeSetupSQL(sqls []string) []string {
	if len(sqls) == 0 {
		return nil
	}
	out := make([]string, 0, len(sqls))
	for _, sql := range sqls {
		s := NormalizeSQLTerminator(sql)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}
