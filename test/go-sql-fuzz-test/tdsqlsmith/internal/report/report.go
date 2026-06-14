// Package report builds and normalizes run reports, including JSON output,
// statistics, error counts, and crash/coredump incident summaries.
//
// report 包构建并规范化运行报告，包括 JSON 输出、统计数据、错误计数
// 以及崩溃/core dump 事件摘要。
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

// Stats aggregates per-run counters for generation and execution outcomes.
//
// Stats 汇总单次运行的生成与执行结果计数。
type Stats struct {
	Generated     int64 `json:"generated"`      // statements generated / 已生成的语句数
	Mutated       int64 `json:"mutated"`        // statements produced by mutation / 由变异产生的语句数
	ParseReject   int64 `json:"parse_reject"`   // statements rejected at the parse gate / 在解析关卡被拒绝的语句数
	ParsePanic    int64 `json:"parse_panic"`    // panics during parsing / 解析期间的 panic 次数
	Executed      int64 `json:"executed"`       // statements executed / 已执行的语句数
	OK            int64 `json:"ok"`             // successful executions / 成功执行的次数
	DBError       int64 `json:"db_error"`       // database errors / 数据库错误次数
	Timeout       int64 `json:"timeout"`        // timed-out executions / 执行超时次数
	ConnLost      int64 `json:"conn_lost"`      // connection-loss outcomes / 连接丢失结果次数
	Fatal         int64 `json:"fatal"`          // fatal executor errors / 执行器致命错误次数
	TaosdRestart  int64 `json:"taosd_restart"`  // taosd restarts triggered / 触发的 taosd 重启次数
	TaosdCoredump int64 `json:"taosd_coredump"` // taosd core dumps observed / 观察到的 taosd core dump 次数
}

// ErrorCount pairs an error message with how often it occurred.
//
// ErrorCount 将错误信息与其出现频次配对。
type ErrorCount struct {
	Message string `json:"message"` // error message / 错误信息
	Count   int64  `json:"count"`   // number of occurrences / 出现次数
}

// TaosdIncident describes a taosd disruption captured in the run report,
// including detection, core-dump, and restart details.
//
// TaosdIncident 描述运行报告中捕获的 taosd 中断，
// 包括检测、core dump 和重启细节。
type TaosdIncident struct {
	OccurredAt       time.Time `json:"occurred_at"`                 // when the incident occurred / 事件发生的时间
	ExecClass        string    `json:"exec_class"`                  // triggering execution class / 触发该事件的执行类别
	CaseID           string    `json:"case_id,omitempty"`           // generated case identifier / 生成用例的标识符
	Rule             string    `json:"rule,omitempty"`              // generation rule / 生成规则
	SQL              string    `json:"sql"`                         // SQL in flight / 正在执行的 SQL
	Error            string    `json:"error"`                       // triggering error message / 触发的错误信息
	ProcessExists    bool      `json:"process_exists"`              // whether the process still existed / 进程是否仍然存在
	ProcessCheck     string    `json:"process_check,omitempty"`     // process check detail / 进程检查细节
	ExitReason       string    `json:"exit_reason,omitempty"`       // formatted exit reason / 格式化的退出原因
	CoredumpDetected bool      `json:"coredump_detected"`           // whether a core dump was detected / 是否检测到 core dump
	CoredumpEvidence string    `json:"coredump_evidence,omitempty"` // core-dump evidence / core dump 证据
	RestartAttempted bool      `json:"restart_attempted"`           // whether a restart was attempted / 是否尝试了重启
	RestartCommand   string    `json:"restart_command,omitempty"`   // restart command / 重启命令
	RestartSucceeded bool      `json:"restart_succeeded"`           // whether the restart succeeded / 重启是否成功
	RestartOutput    string    `json:"restart_output,omitempty"`    // restart output / 重启输出
	RestartError     string    `json:"restart_error,omitempty"`     // restart error message / 重启错误信息
}

// CoredumpStatement records a statement associated with a taosd core dump, plus
// the preceding execution window for reproduction.
//
// CoredumpStatement 记录与 taosd core dump 相关的语句，
// 以及用于复现的先前执行窗口。
type CoredumpStatement struct {
	OccurredAt       time.Time         `json:"occurred_at"`                 // when the statement ran / 语句运行的时间
	IncidentID       string            `json:"incident_id,omitempty"`       // associated incident id / 关联的事件 id
	QueryNo          int64             `json:"query_no,omitempty"`          // sequence number / 序号
	CaseID           string            `json:"case_id,omitempty"`           // generated case id / 生成用例 id
	Rule             string            `json:"rule,omitempty"`              // generation rule / 生成规则
	ExecClass        string            `json:"exec_class"`                  // execution class / 执行类别
	SQL              string            `json:"sql"`                         // executed SQL / 已执行的 SQL
	CandidateSQL     string            `json:"candidate_sql,omitempty"`     // candidate/minimized SQL / 候选/最小化后的 SQL
	Error            string            `json:"error"`                       // error message / 错误信息
	CoredumpEvidence string            `json:"coredump_evidence,omitempty"` // core-dump evidence / core dump 证据
	ProcessCheck     string            `json:"process_check,omitempty"`     // process check detail / 进程检查细节
	ExitReason       string            `json:"exit_reason,omitempty"`       // formatted exit reason / 格式化的退出原因
	RestartCommand   string            `json:"restart_command,omitempty"`   // restart command / 重启命令
	RestartSucceeded bool              `json:"restart_succeeded,omitempty"` // whether restart succeeded / 重启是否成功
	FailureID        string            `json:"failure_id,omitempty"`        // failure artifact id / 失败产物 id
	FailurePath      string            `json:"failure_path,omitempty"`      // failure artifact path / 失败产物路径
	PrecedingWindow  []ExecutedStmtRef `json:"preceding_window,omitempty"`  // statements preceding the crash / 崩溃之前的语句
}

// CrashPendingStatement mirrors the crash-guard pending statement persisted at
// crash time.
//
// CrashPendingStatement 对应崩溃时由 crash-guard 持久化的待处理语句。
type CrashPendingStatement struct {
	OccurredAt time.Time `json:"occurred_at"`         // when it entered the pending state / 进入待处理状态的时间
	RunID      string    `json:"run_id,omitempty"`    // owning run id / 所属运行 id
	QueryNo    int64     `json:"query_no,omitempty"`  // sequence number / 序号
	CaseID     string    `json:"case_id,omitempty"`   // generated case id / 生成用例 id
	Rule       string    `json:"rule,omitempty"`      // generation rule / 生成规则
	Phase      string    `json:"phase,omitempty"`     // processing phase / 处理阶段
	RNGState   string    `json:"rng_state,omitempty"` // serialized RNG state / 序列化的 RNG 状态
	SQL        string    `json:"sql"`                 // the SQL text / SQL 文本
}

// CrashSnapshotReport is the crash-guard snapshot embedded in a process crash report.
//
// CrashSnapshotReport 是嵌入在进程崩溃报告中的 crash-guard 快照。
type CrashSnapshotReport struct {
	RunID         string                 `json:"run_id,omitempty"`         // owning run id / 所属运行 id
	RunDir        string                 `json:"run_dir,omitempty"`        // run directory / 运行目录
	UpdatedAt     time.Time              `json:"updated_at"`               // last update time / 最后更新时间
	WorkerPID     int                    `json:"worker_pid,omitempty"`     // worker PID / 工作进程 PID
	Pending       *CrashPendingStatement `json:"pending,omitempty"`        // statement in flight at crash / 崩溃时正在执行的语句
	Window        []ExecutedStmtRef      `json:"window,omitempty"`         // recent statement window / 最近语句窗口
	ExecutedTotal int64                  `json:"executed_total,omitempty"` // total executed statements / 已执行语句总数
	CleanExit     bool                   `json:"clean_exit,omitempty"`     // whether exit was clean / 是否为干净退出
}

// ProcessCrashReport describes a crash of the worker process as observed by the supervisor.
//
// ProcessCrashReport 描述监督者所观察到的工作进程崩溃。
type ProcessCrashReport struct {
	RunID      string               `json:"run_id,omitempty"`      // owning run id / 所属运行 id
	RunDir     string               `json:"run_dir,omitempty"`     // run directory / 运行目录
	Seed       int64                `json:"seed,omitempty"`        // RNG seed / RNG 种子
	OccurredAt time.Time            `json:"occurred_at"`           // crash time / 崩溃时间
	Reason     string               `json:"reason,omitempty"`      // crash reason / 崩溃原因
	Signal     string               `json:"signal,omitempty"`      // terminating signal, if any / 终止信号（如有）
	ExitCode   int                  `json:"exit_code,omitempty"`   // exit code, if not signaled / 退出码（若非信号终止）
	CoreDump   bool                 `json:"core_dump,omitempty"`   // whether a core dump occurred / 是否产生 core dump
	Error      string               `json:"error,omitempty"`       // associated error message / 关联的错误信息
	LatestPath string               `json:"latest_path,omitempty"` // path to the latest snapshot / 最新快照的路径
	Snapshot   *CrashSnapshotReport `json:"snapshot,omitempty"`    // crash-guard snapshot at crash time / 崩溃时的 crash-guard 快照
}

// ExecutedStmtRef is a compact record of an executed statement used in report windows.
//
// ExecutedStmtRef 是用于报告窗口中已执行语句的紧凑记录。
type ExecutedStmtRef struct {
	QueryNo    int64     `json:"query_no"`              // sequence number / 序号
	OccurredAt time.Time `json:"occurred_at"`           // execution time / 执行时间
	CaseID     string    `json:"case_id,omitempty"`     // generated case id / 生成用例 id
	Rule       string    `json:"rule,omitempty"`        // generation rule / 生成规则
	ExecClass  string    `json:"exec_class"`            // execution class / 执行类别
	SQL        string    `json:"sql"`                   // executed SQL / 已执行的 SQL
	Error      string    `json:"error,omitempty"`       // error message, if any / 错误信息（如有）
	DurationMS int64     `json:"duration_ms,omitempty"` // execution duration in ms / 执行耗时（毫秒）
}

// RunReport is the full report for a completed run: configuration, coverage,
// statistics, top errors, and any crash/coredump artifacts.
//
// RunReport 是已完成运行的完整报告：配置、覆盖率、统计数据、
// 高频错误以及任何崩溃/core dump 产物。
type RunReport struct {
	RunID                 string                      `json:"run_id"`                            // run identifier / 运行标识符
	Version               string                      `json:"version"`                           // tool version / 工具版本
	StartedAt             time.Time                   `json:"started_at"`                        // run start time / 运行开始时间
	FinishedAt            time.Time                   `json:"finished_at"`                       // run finish time / 运行结束时间
	DurationMS            int64                       `json:"duration_ms"`                       // total duration in ms / 总耗时（毫秒）
	Seed                  int64                       `json:"seed"`                              // RNG seed / RNG 种子
	DSNSummary            string                      `json:"dsn_summary"`                       // redacted DSN summary / 脱敏后的 DSN 摘要
	OutDir                string                      `json:"out_dir"`                           // output directory / 输出目录
	DryRun                bool                        `json:"dry_run"`                           // whether execution was skipped / 是否跳过了执行
	Cases                 int                         `json:"cases"`                             // configured case count / 配置的用例数量
	Duration              string                      `json:"duration"`                          // configured duration / 配置的时长
	StmtTimeout           string                      `json:"stmt_timeout"`                      // configured statement timeout / 配置的语句超时
	MutationLevel         int                         `json:"mutation_level"`                    // mutation intensity / 变异强度
	StopWhenCover         bool                        `json:"stop_when_covered"`                 // stop-when-covered setting / 覆盖后停止的设置
	CorpusDir             string                      `json:"corpus_dir"`                        // corpus directory / 语料库目录
	RNGStateInitial       string                      `json:"rng_state_initial,omitempty"`       // serialized RNG state at start / 开始时序列化的 RNG 状态
	RNGStateFinal         string                      `json:"rng_state_final,omitempty"`         // serialized RNG state at end / 结束时序列化的 RNG 状态
	Coverage              branchmodel.CoverageSummary `json:"query_branch_coverage"`             // query branch coverage summary / 查询分支覆盖率摘要
	QueryRuleCoverage     queryrules.Summary          `json:"query_rule_coverage"`               // query rule coverage summary / 查询规则覆盖率摘要
	PositiveHits          []branchmodel.HitInfo       `json:"positive_hits"`                     // covered positive branches / 已覆盖的正向分支
	NegativeHits          []branchmodel.HitInfo       `json:"negative_hits"`                     // covered negative branches / 已覆盖的负向分支
	Stats                 Stats                       `json:"stats"`                             // aggregate counters / 汇总计数器
	TopErrors             []ErrorCount                `json:"top_errors"`                        // most frequent errors / 最高频的错误
	FamilyCounts          map[string]int64            `json:"family_counts,omitempty"`           // counts by statement family / 按语句族分类的计数
	QueryComboCounts      map[string]int64            `json:"query_combo_counts,omitempty"`      // counts by query feature combo / 按查询特征组合分类的计数
	FailureArtifacts      []string                    `json:"failure_artifacts"`                 // paths to failure artifacts / 失败产物的路径
	TaosdIncidents        []TaosdIncident             `json:"taosd_incidents,omitempty"`         // recorded taosd incidents / 记录的 taosd 事件
	CoredumpStatements    []CoredumpStatement         `json:"coredump_statements,omitempty"`     // statements tied to core dumps / 与 core dump 关联的语句
	CoredumpIncidentCount int                         `json:"coredump_incident_count,omitempty"` // number of coredump incidents / core dump 事件数量
	CrashGuardDir         string                      `json:"crash_guard_dir,omitempty"`         // crash-guard directory / crash-guard 目录
	CrashLatestReport     string                      `json:"crash_latest_report,omitempty"`     // path to latest crash snapshot / 最新崩溃快照的路径
	SupervisorCrashReport string                      `json:"supervisor_crash_report,omitempty"` // path to supervisor crash report / 监督者崩溃报告的路径
	ImpedanceRows         []impedance.Row             `json:"impedance,omitempty"`               // impedance-mismatch findings / 阻抗失配的发现项
}

// CrashIncident is a minimal crash record (id, time, crash SQL) for the minimal report.
//
// CrashIncident 是用于精简报告的最小崩溃记录（id、时间、崩溃 SQL）。
type CrashIncident struct {
	IncidentID string    `json:"incident_id"` // incident identifier / 事件标识符
	OccurredAt time.Time `json:"occurred_at"` // when the crash occurred / 崩溃发生的时间
	CrashSQL   string    `json:"crash_sql"`   // SQL associated with the crash / 与崩溃关联的 SQL
}

// QueryRuleProgressPoint captures query-rule coverage progress at a given query number.
//
// QueryRuleProgressPoint 捕获在给定查询序号处的查询规则覆盖率进度。
type QueryRuleProgressPoint struct {
	QueryNo       int64    `json:"query_no"`              // query sequence number / 查询序号
	Hit           int      `json:"hit"`                   // rules hit so far / 至今命中的规则数
	Required      int      `json:"required"`              // rules required / 所需的规则数
	Missing       int      `json:"missing"`               // rules still missing / 仍缺失的规则数
	CoverageRatio float64  `json:"coverage_ratio"`        // hit/required ratio / 命中/所需 的比率
	TopMissing    []string `json:"top_missing,omitempty"` // sample of missing rules / 缺失规则的样本
}

// MinimalRunReport is the compact, replay-oriented run report.
//
// MinimalRunReport 是面向重放的紧凑型运行报告。
type MinimalRunReport struct {
	RunID               string                   `json:"run_id"`                         // run identifier / 运行标识符
	StartedAt           time.Time                `json:"started_at"`                     // run start time / 运行开始时间
	GeneratedAt         time.Time                `json:"generated_at"`                   // report generation time / 报告生成时间
	ExecutionDurationMS int64                    `json:"execution_duration_ms"`          // execution duration in ms / 执行耗时（毫秒）
	Completed           bool                     `json:"completed"`                      // whether the run completed / 运行是否完成
	SetupSQL            []string                 `json:"setup_sql,omitempty"`            // schema setup statements / 模式建立语句
	TotalExecuted       int64                    `json:"total_executed"`                 // total statements executed / 已执行语句总数
	QueryRuleCoverage   queryrules.Summary       `json:"query_rule_coverage"`            // query rule coverage summary / 查询规则覆盖率摘要
	QueryRuleProgress   []QueryRuleProgressPoint `json:"query_rule_progress,omitempty"`  // coverage progress points / 覆盖率进度点
	QueryComboCounts    map[string]int64         `json:"query_combo_counts,omitempty"`   // counts by query feature combo / 按查询特征组合分类的计数
	TaosdIncidents      []CrashIncident          `json:"taosd_incidents,omitempty"`      // taosd crash incidents / taosd 崩溃事件
	TDsqlsmithIncidents []CrashIncident          `json:"tdsqlsmith_incidents,omitempty"` // tool-side crash incidents / 工具侧崩溃事件
}

// MakeRunID builds a run identifier from the start time and seed.
//
// MakeRunID 根据开始时间和种子构建运行标识符。
func MakeRunID(start time.Time, seed int64) string {
	return fmt.Sprintf("%s_seed%d", start.Format("20060102_150405"), seed)
}

// WriteJSON writes v as indented JSON to path, creating parent directories.
//
// WriteJSON 将 v 以缩进 JSON 形式写入 path，并创建父目录。
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

// ReadMinimalRunReport reads and normalizes a MinimalRunReport from path,
// requiring a non-empty run_id.
//
// ReadMinimalRunReport 从 path 读取并规范化一个 MinimalRunReport，
// 要求 run_id 非空。
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

// Normalize fills defaults, sanitizes fields, and assigns sequential incident
// IDs across the taosd and tdsqlsmith incident lists.
//
// Normalize 填充默认值、清洗字段，并为 taosd 与 tdsqlsmith 事件列表
// 分配连续的事件 ID。
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

// IncidentCount returns the total number of taosd and tdsqlsmith incidents.
//
// IncidentCount 返回 taosd 与 tdsqlsmith 事件的总数。
func (r *MinimalRunReport) IncidentCount() int {
	if r == nil {
		return 0
	}
	return len(r.TaosdIncidents) + len(r.TDsqlsmithIncidents)
}

// formatIncidentID formats a 1-based sequence number as an incident id.
//
// formatIncidentID 将从 1 开始的序号格式化为事件 id。
func formatIncidentID(seq int64) string {
	if seq <= 0 {
		seq = 1
	}
	return fmt.Sprintf("incident_%06d", seq)
}

// normalizeCrashIncidents trims and copies the incidents, returning nil if empty.
//
// normalizeCrashIncidents 修剪并复制事件，若为空则返回 nil。
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

// normalizeQueryRuleSummary clamps negative counts, enforces required >= hit, and
// recomputes the coverage ratio.
//
// normalizeQueryRuleSummary 将负计数钳制为非负，强制 required >= hit，
// 并重新计算覆盖率比率。
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

// normalizeRuleProgress sanitizes each progress point, recomputes coverage
// ratios, sorts by query number, and de-duplicates to the last point per query.
//
// normalizeRuleProgress 清洗每个进度点、重新计算覆盖率比率、按查询序号排序，
// 并去重为每个查询序号保留最后一个点。
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

// normalizeCountMap drops blank keys and non-positive values, returning nil if empty.
//
// normalizeCountMap 丢弃空白键和非正值，若为空则返回 nil。
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

// normalizeStringSlice trims, drops blanks, de-duplicates, and sorts the input,
// returning nil if nothing remains.
//
// normalizeStringSlice 修剪、丢弃空白、去重并排序输入，
// 若无剩余内容则返回 nil。
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

// NormalizeSQLTerminator trims sql and ensures it ends with a single semicolon;
// an empty input yields "".
//
// NormalizeSQLTerminator 修剪 sql 并确保其以单个分号结尾；
// 空输入返回 ""。
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

// NormalizeSetupSQL terminates each statement and drops empty ones.
//
// NormalizeSetupSQL 为每条语句添加终止符并丢弃空语句。
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
