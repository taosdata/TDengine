package run

// run.go drives the fuzz loop: it bootstraps the shared catalog, repeatedly
// generates SQL statements (via branch-case/rule-seed/random strategies),
// parses and executes them against TDengine, tracks query-rule coverage,
// detects and records taosd crashes/incidents, and periodically flushes a
// minimal run report. The crash-guard recorder persists pending/executed
// statement state so the supervisor can attribute crashes to specific SQL.
//
// run.go 驱动 fuzz 循环:它引导共享 catalog,反复生成 SQL 语句(通过
// branch-case/rule-seed/random 策略),对其进行解析并在 TDengine 上执行,跟踪
// query-rule 覆盖率,检测并记录 taosd 崩溃/事件,并周期性地刷写一份最小运行报告。
// crash-guard 记录器持久化待执行/已执行语句状态,使 supervisor 能够将崩溃归因到
// 具体的 SQL。

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"sqlparser"
	"tdsqlsmith/internal/branchmodel"
	"tdsqlsmith/internal/catalog"
	"tdsqlsmith/internal/crashguard"
	"tdsqlsmith/internal/executor"
	"tdsqlsmith/internal/impedance"
	"tdsqlsmith/internal/logger"
	"tdsqlsmith/internal/parsergate"
	"tdsqlsmith/internal/querygen"
	"tdsqlsmith/internal/queryrules"
	"tdsqlsmith/internal/random"
	"tdsqlsmith/internal/report"
	"tdsqlsmith/internal/taosdwatch"
)

// Config holds all knobs controlling a single fuzz run.
// Config 持有控制单次 fuzz 运行的所有可调参数。
type Config struct {
	Version         string        // build version, recorded in reports / 构建版本,记录在报告中
	DSN             string        // TDengine data source name (connection string) / TDengine 数据源名称(连接字符串)
	Seed            int64         // RNG seed; 0 means derive from current time / RNG 种子;0 表示从当前时间派生
	RNGState        string        // serialized RNG state to restore (overrides Seed position) / 用于恢复的序列化 RNG 状态(覆盖 Seed 的位置)
	ResumeQueryNo   int64         // query number to resume after (worker restart) / 恢复时从该查询号之后开始(worker 重启)
	ResumeRNGState  string        // serialized RNG state to resume from (worker restart) / 用于恢复的序列化 RNG 状态(worker 重启)
	RunDeadline     time.Time     // absolute deadline for the run; zero means none / 运行的绝对截止时间;零值表示无
	Cases           int           // max number of statements to generate; 0 means unlimited / 最多生成的语句数;0 表示无限制
	Duration        time.Duration // max run duration; 0 means unlimited / 最大运行时长;0 表示无限制
	StmtTimeout     time.Duration // per-statement execution timeout / 单条语句的执行超时
	OutDir          string        // base output directory for run artifacts / 运行产物的基础输出目录
	MutationLevel   int           // mutation aggressiveness level / 变异激进程度等级
	StopWhenCovered bool          // stop early once all coverage targets are hit / 一旦命中所有覆盖目标即提前停止
	DryRun          bool          // generate and parse only; do not execute / 仅生成并解析;不执行
	Verbose         bool          // emit verbose progress to stderr / 向 stderr 输出详细进度
	DumpAllQueries  bool          // log every generated query / 记录每一条生成的查询
	DumpAllGraphs   bool          // dump every statement's AST as graphml / 将每条语句的 AST 导出为 graphml
	ExcludeCatalog  bool          // exclude catalog setup statements from coverage / 将 catalog 建表语句排除在覆盖率之外
	LegacyMode      bool          // legacy behavior toggle / 旧版行为开关
	WorkloadConfig  string        // path/spec for workload-driven generation / workload 驱动生成的路径/规格
	ExecProfile     string        // execution-gating profile: strict/balanced/aggressive / 执行门控档位:strict/balanced/aggressive
	RunIDOverride   string        // forces a specific run ID / 强制使用指定的 run ID
	RunDirOverride  string        // forces a specific run directory / 强制使用指定的运行目录
	CrashGuard      bool          // enable crash-guard snapshot recording / 启用 crash-guard 快照记录
	SkipBootstrap   bool          // attach to an already-bootstrapped shared catalog / 附着到一个已引导的共享 catalog
}

// Result summarizes the outcome of a completed run.
// Result 概括一次已完成运行的结果。
type Result struct {
	RunID      string                      // unique identifier of the run / 运行的唯一标识
	RunDir     string                      // directory holding the run's artifacts / 存放运行产物的目录
	ReportPath string                      // path to the run's JSON report / 运行 JSON 报告的路径
	Coverage   branchmodel.CoverageSummary // branch-model coverage summary / branch-model 覆盖率摘要
	QueryRules queryrules.Summary          // query-rule coverage summary / query-rule 覆盖率摘要
	Stats      report.Stats                // execution statistics counters / 执行统计计数器
}

const (
	executedHistoryLimit        = 64 // max executed statements retained in the rolling history / 滚动历史中保留的最大已执行语句数
	coredumpPrecedingWindowSize = 8  // executed statements captured before a coredump / coredump 之前捕获的已执行语句数
	queryRuleProgressInterval   = 20 // record a query-rule progress point every N generated queries / 每生成 N 条查询记录一个 query-rule 进度点
	minimalReportFlushInterval  = 20 // flush the minimal report every N generated queries / 每生成 N 条查询刷写一次最小报告
)

// Package-level function variables, overridable in tests to stub external deps.
// 包级函数变量,可在测试中覆盖以打桩外部依赖。
var (
	executorNewFn       = executor.New                    // creates an executor connection / 创建 executor 连接
	catalogBootstrapFn  = catalog.Bootstrap               // bootstraps the shared catalog / 引导共享 catalog
	catalogPrepareFn    = catalog.PrepareShared           // attaches to an existing shared catalog / 附着到已有的共享 catalog
	taosdEnsureRunning  = taosdwatch.EnsureRunning        // ensures taosd is running (parent-child mode) / 确保 taosd 正在运行(父子进程模式)
	taosdShouldHandle   = taosdwatch.ShouldHandle         // decides if an error is a taosd incident / 判断某个错误是否为 taosd 事件
	taosdHandleIncident = taosdwatch.Handle               // handles a detected taosd incident / 处理检测到的 taosd 事件
	taosdLastManagedAt  = taosdwatch.LastManagedExitSince // queries the last managed taosd exit time / 查询受管理 taosd 的最近退出时间
	taosdStopManaged    = taosdwatch.StopManaged          // stops the managed taosd child / 停止受管理的 taosd 子进程
)

// executedStmtRecord is one entry in the rolling executed-statement history.
// executedStmtRecord 是滚动已执行语句历史中的一条记录。
type executedStmtRecord struct {
	QueryNo    int64     // sequential query number / 顺序查询号
	OccurredAt time.Time // when execution completed / 执行完成的时间
	CaseID     string    // generation case identifier / 生成用例标识
	Rule       string    // generation rule name / 生成规则名称
	ExecClass  string    // executor result classification / executor 结果分类
	SQL        string    // the executed SQL text / 已执行的 SQL 文本
	Error      string    // error message, if any / 错误信息(若有)
	DurationMS int64     // execution duration in milliseconds / 执行时长(毫秒)
}

// generationStrategy names a SQL generation strategy.
// generationStrategy 命名一种 SQL 生成策略。
type generationStrategy string

const (
	strategyBranchCase  generationStrategy = "branch_case"  // target uncovered branch-model cases / 针对未覆盖的 branch-model 用例
	strategyRuleSeed    generationStrategy = "rule_seed"    // target missing query rules / 针对缺失的 query rule
	strategyQueryRandom generationStrategy = "query_random" // random query generation / 随机查询生成
	strategyWorkload    generationStrategy = "workload"     // workload-driven generation / workload 驱动生成
)

// generatedStatement is a single statement produced by the generator.
// generatedStatement 是生成器产出的单条语句。
type generatedStatement struct {
	CaseID  string   // generation case identifier / 生成用例标识
	Rule    string   // generation rule name / 生成规则名称
	SQL     string   // generated SQL text / 生成的 SQL 文本
	Mutated bool     // true if produced by mutation / 是否由变异产生
	Kind    string   // statement kind, e.g. "query" / 语句类型,如 "query"
	Tags    []string // generation tags used for query-rule mapping / 用于 query-rule 映射的生成标签
}

// crashRecorder records pending and executed statements so a crash can be
// attributed to the SQL in flight when the process died.
//
// crashRecorder 记录待执行和已执行的语句,以便能将崩溃归因于进程死亡时正在执行
// 的 SQL。
type crashRecorder interface {
	Before(meta crashguard.PendingStatement) error // record the statement about to run / 记录即将运行的语句
	After(rec *crashguard.ExecutedStmt) error      // record the statement that just finished / 记录刚刚完成的语句
	MarkCleanExit() error                          // mark that the run exited cleanly / 标记运行已干净退出
	Dir() string                                   // crash-guard directory / crash-guard 目录
	LatestPath() string                            // path to the latest snapshot file / 最新快照文件的路径
}

// noopCrashRecorder is a crashRecorder that records nothing, used when crash
// guarding is disabled.
//
// noopCrashRecorder 是一个什么都不记录的 crashRecorder,在禁用 crash guard 时使用。
type noopCrashRecorder struct{}

// Before is a no-op.
// Before 是空操作。
func (noopCrashRecorder) Before(crashguard.PendingStatement) error { return nil }

// After is a no-op.
// After 是空操作。
func (noopCrashRecorder) After(*crashguard.ExecutedStmt) error { return nil }

// MarkCleanExit is a no-op.
// MarkCleanExit 是空操作。
func (noopCrashRecorder) MarkCleanExit() error { return nil }

// Dir returns an empty directory path.
// Dir 返回空的目录路径。
func (noopCrashRecorder) Dir() string { return "" }

// LatestPath returns an empty snapshot path.
// LatestPath 返回空的快照路径。
func (noopCrashRecorder) LatestPath() string { return "" }

// Execute runs the full fuzz loop for the given configuration and returns a
// Result summarizing coverage and statistics. It bootstraps (or attaches to)
// the shared catalog, generates and executes statements until the deadline,
// case count, or coverage target is reached, recording incidents and flushing
// the minimal run report along the way.
//
// Execute 针对给定配置运行完整的 fuzz 循环,并返回汇总覆盖率与统计信息的 Result。
// 它引导(或附着到)共享 catalog,生成并执行语句,直到到达截止时间、用例数或覆盖
// 目标,期间记录事件并刷写最小运行报告。
func Execute(ctx context.Context, cfg Config) (*Result, error) {
	defer func() {
		_ = taosdStopManaged(context.Background())
	}()

	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	if cfg.StmtTimeout <= 0 {
		cfg.StmtTimeout = 2 * time.Second
	}
	if strings.TrimSpace(cfg.DSN) == "" {
		cfg.DSN = "root:taosdata@tcp(127.0.0.1:6030)/"
	}
	cfg.ExecProfile = normalizeExecProfile(cfg.ExecProfile)
	if strings.TrimSpace(cfg.ResumeRNGState) != "" {
		cfg.RNGState = strings.TrimSpace(cfg.ResumeRNGState)
	}
	if cfg.ResumeQueryNo < 0 {
		cfg.ResumeQueryNo = 0
	}

	queryRuleCatalog := loadRuntimeQueryRuleCatalog()
	trackedQueryRules := defaultTrackedQueryRules()
	if queryRuleCatalog != nil {
		if required := queryRuleCatalog.RequiredRules(); len(required) > 0 {
			trackedQueryRules = required
		}
	}
	queryRuleTracker := queryrules.NewTracker(trackedQueryRules)
	queryGen := querygen.New(querygen.DefaultConfig())
	tracker := branchmodel.NewTracker(nil, nil)

	impedance.Reset()
	rng := random.New(uint64(cfg.Seed))
	if cfg.RNGState != "" {
		if err := rng.Deserialize(cfg.RNGState); err != nil {
			return nil, fmt.Errorf("deserialize --rng-state: %w", err)
		}
	}
	seedStateInitial := rng.Serialize()

	start := time.Now()
	runID := strings.TrimSpace(cfg.RunIDOverride)
	if runID == "" {
		runID = report.MakeRunID(start, cfg.Seed)
	}
	runDir := strings.TrimSpace(cfg.RunDirOverride)
	if runDir == "" {
		runDir = filepath.Join(cfg.OutDir, runID)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	reportPath := filepath.Join(runDir, "run_report.json")
	existingMinimalBase := loadExistingMinimalReport(reportPath)
	baseTDsqlsmithIncidents := cloneCrashIncidents(minimalTDsqlsmithIncidents(existingMinimalBase))
	baseTaosdCrashIncidents := cloneCrashIncidents(minimalTaosdIncidents(existingMinimalBase))
	baseQueryRuleProgress := cloneQueryRuleProgress(minimalQueryRuleProgress(existingMinimalBase))
	baseQueryComboCounts := cloneCountMap(minimalQueryComboCounts(existingMinimalBase))
	baseQueryRuleCoverage := minimalQueryRuleCoverage(existingMinimalBase)
	baseSetupSQL := cloneSetupSQL(minimalSetupSQL(existingMinimalBase))
	baseTotalExecuted := minimalTotalExecuted(existingMinimalBase)
	runStartedAt := minimalStartedAt(existingMinimalBase)
	if runStartedAt.IsZero() {
		runStartedAt = start
	}

	var crashRec crashRecorder = noopCrashRecorder{}
	if cfg.CrashGuard {
		rec, recErr := crashguard.New(runID, runDir, executedHistoryLimit)
		if recErr != nil {
			return nil, fmt.Errorf("init crash guard: %w", recErr)
		}
		crashRec = rec
	}

	loggers, err := buildLoggers(cfg, runID, seedStateInitial, runDir)
	if err != nil {
		return nil, err
	}
	defer closeLoggers(loggers)

	stats := report.Stats{}
	stats.Generated = cfg.ResumeQueryNo
	errorsMap := map[string]int64{}
	familyMap := map[string]int64{}
	queryComboMap := map[string]int64{}
	queryRuleProgress := make([]report.QueryRuleProgressPoint, 0, 64)
	executedHistory := make([]executedStmtRecord, 0, executedHistoryLimit)
	taosdIncidents := make([]report.TaosdIncident, 0, 16)
	coredumpStatements := make([]report.CoredumpStatement, 0, 8)
	taosdCrashIncidents := make([]report.CrashIncident, 0, 8)
	var incidentSeq int64
	var lastManagedExitSeenAt time.Time
	var lastManagedExitCrashSQL string
	if cfg.Verbose && cfg.ResumeQueryNo > 0 {
		fmt.Fprintf(os.Stderr, "resuming worker: next_query_no=%d\n", cfg.ResumeQueryNo+1)
	}

	recordErr := func(kind, msg string) {
		m := normalizeErr(msg)
		if m == "" {
			return
		}
		errorsMap[kind+": "+m]++
	}
	appendRuleProgress := func(force bool) {
		if stats.Generated <= 0 {
			return
		}
		if !force && stats.Generated%queryRuleProgressInterval != 0 {
			return
		}
		summary := queryRuleTracker.Summary()
		queryRuleProgress = append(queryRuleProgress, report.QueryRuleProgressPoint{
			QueryNo:       stats.Generated,
			Hit:           summary.Hit,
			Required:      summary.Required,
			Missing:       len(summary.Missing),
			CoverageRatio: summary.CoverageRatio,
			TopMissing:    topNStrings(summary.Missing, 10),
		})
	}
	var (
		exec     *executor.Executor
		prepared *catalog.Prepared
	)
	setupSQL := report.NormalizeSetupSQL(catalog.BootstrapSetupSQL("tdsqlsmith_shared"))
	var cleanup catalog.CleanupFunc
	var exitErr error

	writeMinimalSnapshot := func(includeCurrentExecuted bool, completed bool) error {
		now := time.Now()
		queryRuleSummary := queryRuleTracker.Summary()
		allTaosdIncidents := append(cloneCrashIncidents(baseTaosdCrashIncidents), taosdCrashIncidents...)
		allQueryRuleProgress := append(cloneQueryRuleProgress(baseQueryRuleProgress), queryRuleProgress...)
		allQueryComboCounts := mergeCountMaps(cloneCountMap(baseQueryComboCounts), queryComboMap)
		mergedRuleSummary := mergeQueryRuleSummary(baseQueryRuleCoverage, queryRuleSummary)
		reportSetupSQL := report.NormalizeSetupSQL(setupSQL)
		if len(reportSetupSQL) == 0 {
			reportSetupSQL = cloneSetupSQL(baseSetupSQL)
		}

		totalExecuted := baseTotalExecuted
		if includeCurrentExecuted {
			totalExecuted += stats.Executed
		}
		executionDurationMS := int64(0)
		if !runStartedAt.IsZero() && now.After(runStartedAt) {
			executionDurationMS = now.Sub(runStartedAt).Milliseconds()
		}
		minimalReport := &report.MinimalRunReport{
			RunID:               runID,
			StartedAt:           runStartedAt,
			GeneratedAt:         now,
			ExecutionDurationMS: executionDurationMS,
			Completed:           completed,
			SetupSQL:            reportSetupSQL,
			TotalExecuted:       totalExecuted,
			QueryRuleCoverage:   mergedRuleSummary,
			QueryRuleProgress:   allQueryRuleProgress,
			QueryComboCounts:    allQueryComboCounts,
			TaosdIncidents:      allTaosdIncidents,
			TDsqlsmithIncidents: cloneCrashIncidents(baseTDsqlsmithIncidents),
		}
		minimalReport.Normalize()
		return report.WriteJSON(reportPath, minimalReport)
	}
	flushMinimalSnapshotPeriodically := func() error {
		if stats.Generated <= 0 {
			return nil
		}
		if stats.Generated%minimalReportFlushInterval != 0 {
			return nil
		}
		return writeMinimalSnapshot(true, false)
	}

	if !cfg.DryRun {
		// Start taosd as child process (parent-child mode)
		// 以子进程方式启动 taosd(父子进程模式)
		if _, _, ensureErr := taosdEnsureRunning(ctx); ensureErr != nil {
			return nil, fmt.Errorf("ensure taosd running: %w", ensureErr)
		}
		exec, err = executorNewFn(ctx, cfg.DSN)
		if err != nil {
			exec, err = handleInitConnectionFailure(ctx, cfg.DSN, err, &stats, recordErr, &taosdIncidents, &taosdCrashIncidents, &incidentSeq)
			if err != nil {
				exitErr = err
			}
		}
		lastManagedExitSeenAt = latestManagedExitSeenAt(lastManagedExitSeenAt)
		if exitErr == nil {
			defer exec.Close()

			bootCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if cfg.SkipBootstrap {
				prepared, cleanup, err = catalogPrepareFn(bootCtx, exec, "tdsqlsmith")
			} else {
				prepared, cleanup, err = catalogBootstrapFn(bootCtx, exec, cfg.Seed, "tdsqlsmith")
			}
			cancel()
			if err != nil {
				return nil, err
			}
			defer cleanup(context.Background())
			queryGen.BindSchema(schemaFromPrepared(prepared))
			if len(prepared.SetupSQL) > 0 {
				setupSQL = report.NormalizeSetupSQL(prepared.SetupSQL)
			}
		}
	}

	runCtx := ctx
	if !cfg.RunDeadline.IsZero() {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithDeadline(runCtx, cfg.RunDeadline)
		defer cancel()
	} else if cfg.Duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(runCtx, cfg.Duration)
		defer cancel()
	}

	for exitErr == nil {
		if err := runCtx.Err(); err != nil {
			break
		}
		if cfg.Cases > 0 && int(stats.Generated) >= cfg.Cases {
			break
		}
		if cfg.StopWhenCovered && tracker.IsPositiveCovered() && tracker.IsNegativeCovered() && queryRuleTracker.IsCovered() {
			break
		}
		if !cfg.DryRun && exec != nil {
			forceFlush := captureManagedExitIncident(
				runCtx,
				exec,
				cfg.DSN,
				&stats,
				recordErr,
				&taosdIncidents,
				&taosdCrashIncidents,
				&incidentSeq,
				&lastManagedExitSeenAt,
				lastManagedExitCrashSQL,
				crashRec.LatestPath(),
			)
			if forceFlush {
				if err := writeMinimalSnapshot(true, false); err != nil {
					return nil, err
				}
			}
		}

		missingRules := queryRuleTracker.MissingRules()
		var (
			generated generatedStatement
			family    string
		)
		seeded := false
		strategies := appendUniqueStrategies(nil, pickGenerationStrategy(rng, 0, missingRules), strategyRuleSeed, strategyQueryRandom)
		for _, strategy := range strategies {
			switch strategy {
			case strategyRuleSeed:
				if len(missingRules) == 0 {
					continue
				}
				if qg, ok, qerr := queryGen.NextForRules(rng, missingRules); qerr == nil && ok {
					generated = generatedStatement{
						CaseID:  "QGEN_RULE",
						Rule:    "query_rule_targeted",
						SQL:     qg.SQL,
						Mutated: false,
						Kind:    "query",
						Tags:    append([]string(nil), qg.Tags...),
					}
					family = "DMLSelect"
					for _, tag := range qg.Tags {
						queryComboMap[tag]++
					}
					seeded = true
				}
			case strategyQueryRandom:
				if qg, qerr := queryGen.Next(rng); qerr == nil {
					generated = generatedStatement{
						CaseID:  "QGEN",
						Rule:    "query_random",
						SQL:     qg.SQL,
						Mutated: false,
						Kind:    "query",
						Tags:    append([]string(nil), qg.Tags...),
					}
					family = "DMLSelect"
					for _, tag := range qg.Tags {
						queryComboMap[tag]++
					}
					seeded = true
				}
			}
			if seeded {
				break
			}
		}
		if !seeded {
			qg, qerr := queryGen.Next(rng)
			if qerr != nil {
				return nil, qerr
			}
			generated = generatedStatement{
				CaseID:  "QGEN",
				Rule:    "query_random",
				SQL:     qg.SQL,
				Mutated: false,
				Kind:    "query",
				Tags:    append([]string(nil), qg.Tags...),
			}
			family = "DMLSelect"
			for _, tag := range qg.Tags {
				queryComboMap[tag]++
			}
		}
		familyMap[family]++

		stats.Generated++
		if generated.Mutated {
			stats.Mutated++
		}

		sqlText := strings.TrimSpace(generated.SQL)
		ev := logger.Event{RunID: runID, QueryNo: stats.Generated, CaseID: generated.CaseID, Rule: generated.Rule, SQL: sqlText}
		notifyGenerated(loggers, ev)

		if err := crashRec.Before(crashguard.PendingStatement{
			OccurredAt: time.Now(),
			RunID:      runID,
			QueryNo:    stats.Generated,
			CaseID:     generated.CaseID,
			Rule:       generated.Rule,
			Phase:      string(crashguard.PhaseParse),
			RNGState:   rng.Serialize(),
			SQL:        sqlText,
		}); err != nil {
			return nil, fmt.Errorf("crash guard before parse: %w", err)
		}
		pg := parsergate.ParseWithRules(sqlText)
		if err := crashRec.After(nil); err != nil {
			return nil, fmt.Errorf("crash guard after parse: %w", err)
		}
		if pg.Err != nil {
			stats.ParseReject++
			if pg.ErrType == "panic" {
				stats.ParsePanic++
			}
			impedance.RecordBad(generated.Rule)
			recordErr("parse_"+pg.ErrType, pg.Err.Error())
			notifyError(loggers, ev, "syntax", pg.Err)
			if err := flushMinimalSnapshotPeriodically(); err != nil {
				return nil, err
			}
			continue
		}
		ev.Stmt = pg.Stmt
		notifyParsed(loggers, ev)
		hitQueryRules := queryRulesFromReductions(queryRuleCatalog, pg.Rules)
		if len(hitQueryRules) == 0 {
			hitQueryRules = queryRulesFromTags(generated.Tags)
		}
		queryRuleTracker.MarkMany(hitQueryRules)
		appendRuleProgress(false)

		var hits []string
		if generated.Kind == "query" {
			hits = tracker.TryMarkPositive(pg.Stmt, sqlText, time.Now())
			if len(hits) == 0 && generated.CaseID != "" && !strings.HasPrefix(generated.CaseID, "QRYRULE_") && !strings.HasPrefix(generated.CaseID, "QGEN") {
				recordErr("coverage", fmt.Sprintf("query did not match any uncovered case (expected=%s)", generated.CaseID))
			}
		}

		if cfg.DryRun {
			impedance.RecordOK(generated.Rule)
			if err := flushMinimalSnapshotPeriodically(); err != nil {
				return nil, err
			}
			continue
		}
		if skipExecutionForCoverageSeed(generated.CaseID) {
			impedance.RecordOK(generated.Rule)
			if err := flushMinimalSnapshotPeriodically(); err != nil {
				return nil, err
			}
			continue
		}
		if !shouldExecuteStatement(pg.Stmt, sqlText, cfg.ExecProfile) {
			impedance.RecordOK(generated.Rule)
			if err := flushMinimalSnapshotPeriodically(); err != nil {
				return nil, err
			}
			continue
		}

		lastManagedExitCrashSQL = strings.TrimSpace(sqlText)
		stats.Executed++
		if err := crashRec.Before(crashguard.PendingStatement{
			OccurredAt: time.Now(),
			RunID:      runID,
			QueryNo:    stats.Generated,
			CaseID:     generated.CaseID,
			Rule:       generated.Rule,
			Phase:      string(crashguard.PhaseExec),
			RNGState:   rng.Serialize(),
			SQL:        sqlText,
		}); err != nil {
			return nil, fmt.Errorf("crash guard before exec: %w", err)
		}
		execCtx, cancel := context.WithTimeout(runCtx, cfg.StmtTimeout)
		out := exec.Exec(execCtx, sqlText)
		cancel()
		execCompletedAt := time.Now()
		execErrText := errString(out.Err)
		precedingWindow := clonePrecedingWindow(executedHistory, coredumpPrecedingWindowSize)

		var pendingCoredump *report.CoredumpStatement
		var taosdIncident *taosdwatch.Incident
		forceFlushMinimalSnapshot := false
		if taosdShouldHandle(string(out.Class), out.Err) {
			inc := taosdHandleIncident(runCtx, string(out.Class), sqlText, out.Err)
			if inc.Checked {
				if inc.OccurredAt.After(lastManagedExitSeenAt) {
					lastManagedExitSeenAt = inc.OccurredAt
				}
				taosdIncident = &inc
				taosdIncidents = append(taosdIncidents, report.TaosdIncident{
					OccurredAt:       inc.OccurredAt,
					ExecClass:        inc.ExecClass,
					CaseID:           generated.CaseID,
					Rule:             generated.Rule,
					SQL:              sqlText,
					Error:            execErrText,
					ProcessExists:    inc.ProcessExists,
					ProcessCheck:     inc.ProcessCheck,
					ExitReason:       inc.ExitReason,
					CoredumpDetected: inc.CoredumpDetected,
					CoredumpEvidence: inc.CoredumpEvidence,
					RestartAttempted: inc.RestartAttempted,
					RestartCommand:   inc.RestartCommand,
					RestartSucceeded: inc.RestartSucceeded,
					RestartOutput:    inc.RestartOutput,
					RestartError:     inc.RestartError,
				})
				if shouldRecordTaosdCrash(inc) {
					incidentID := appendTaosdCrashIncident(&taosdCrashIncidents, &incidentSeq, inc.OccurredAt, sqlText)
					if incidentID != "" {
						forceFlushMinimalSnapshot = true
					}
					if inc.CoredumpDetected {
						stats.TaosdCoredump++
						recordErr("taosd_coredump", inc.CoredumpEvidence)
						pendingCoredump = &report.CoredumpStatement{
							OccurredAt:       inc.OccurredAt,
							IncidentID:       incidentID,
							QueryNo:          stats.Generated,
							CaseID:           generated.CaseID,
							Rule:             generated.Rule,
							ExecClass:        string(out.Class),
							SQL:              sqlText,
							CandidateSQL:     sqlText,
							Error:            execErrText,
							CoredumpEvidence: inc.CoredumpEvidence,
							ProcessCheck:     inc.ProcessCheck,
							ExitReason:       inc.ExitReason,
							RestartCommand:   inc.RestartCommand,
							RestartSucceeded: inc.RestartSucceeded,
							PrecedingWindow:  precedingWindow,
						}
					}
				}
				if inc.RestartSucceeded {
					stats.TaosdRestart++
					_ = recoverConnection(runCtx, exec, cfg.DSN)
				} else if inc.RestartAttempted {
					recordErr("taosd_restart", inc.RestartError)
				}
			}
		}

		switch out.Class {
		case executor.ClassOK:
			stats.OK++
			impedance.RecordOK(generated.Rule)
			notifyExecuted(loggers, ev)
		case executor.ClassDBError:
			stats.DBError++
			impedance.RecordBad(generated.Rule)
			recordErr(string(out.Class), execErrText)
			notifyError(loggers, ev, string(out.Class), out.Err)
		case executor.ClassTimeout:
			stats.Timeout++
			impedance.RecordBad(generated.Rule)
			recordErr(string(out.Class), execErrText)
			notifyError(loggers, ev, string(out.Class), out.Err)
		case executor.ClassConnLost:
			stats.ConnLost++
			impedance.RecordBad(generated.Rule)
			recordErr(string(out.Class), execErrText)
			notifyError(loggers, ev, string(out.Class), out.Err)
			if taosdIncident == nil || !taosdIncident.RestartSucceeded {
				_ = recoverConnection(runCtx, exec, cfg.DSN)
			}
		default:
			stats.Fatal++
			impedance.RecordBad(generated.Rule)
			recordErr(string(out.Class), execErrText)
			notifyError(loggers, ev, string(out.Class), out.Err)
		}

		if out.Class == executor.ClassDBError || out.Class == executor.ClassTimeout || out.Class == executor.ClassConnLost || out.Class == executor.ClassFatal {
		}

		if pendingCoredump != nil {
			coredumpStatements = append(coredumpStatements, *pendingCoredump)
		}

		executedHistory = appendExecutedHistory(executedHistory, executedStmtRecord{
			QueryNo:    stats.Generated,
			OccurredAt: execCompletedAt,
			CaseID:     generated.CaseID,
			Rule:       generated.Rule,
			ExecClass:  string(out.Class),
			SQL:        sqlText,
			Error:      execErrText,
			DurationMS: out.Duration.Milliseconds(),
		}, executedHistoryLimit)
		if err := crashRec.After(&crashguard.ExecutedStmt{
			QueryNo:    stats.Generated,
			OccurredAt: execCompletedAt,
			CaseID:     generated.CaseID,
			Rule:       generated.Rule,
			ExecClass:  string(out.Class),
			SQL:        sqlText,
			Error:      execErrText,
			DurationMS: out.Duration.Milliseconds(),
		}); err != nil {
			return nil, fmt.Errorf("crash guard after exec: %w", err)
		}
		if forceFlushMinimalSnapshot {
			if err := writeMinimalSnapshot(true, false); err != nil {
				return nil, err
			}
		} else if err := flushMinimalSnapshotPeriodically(); err != nil {
			return nil, err
		}
	}

	appendRuleProgress(true)
	if err := writeMinimalSnapshot(true, exitErr == nil); err != nil {
		return nil, err
	}
	covSummary := tracker.Summary()
	queryRuleSummary := queryRuleTracker.Summary()
	if err := crashRec.MarkCleanExit(); err != nil {
		return nil, fmt.Errorf("mark crash guard clean exit: %w", err)
	}

	if cfg.Verbose && !cfg.LegacyMode {
		fmt.Fprintf(os.Stderr, "run complete: report=%s\n", reportPath)
	}
	if exitErr != nil {
		return nil, exitErr
	}

	return &Result{
		RunID:      runID,
		RunDir:     runDir,
		ReportPath: reportPath,
		Coverage:   covSummary,
		QueryRules: queryRuleSummary,
		Stats:      stats,
	}, nil
}

// buildLoggers constructs the set of event loggers enabled by the config
// (query dumper, AST graph logger, stderr logger).
//
// buildLoggers 构建由配置启用的事件 logger 集合(查询导出器、AST 图 logger、
// stderr logger)。
func buildLoggers(cfg Config, runID string, seedState string, runDir string) ([]logger.Logger, error) {
	loggers := make([]logger.Logger, 0, 4)
	if cfg.DumpAllQueries {
		loggers = append(loggers, &logger.QueryDumper{})
	}
	if cfg.DumpAllGraphs {
		prefix := filepath.Join(runDir, "sqlsmith")
		loggers = append(loggers, logger.NewASTLogger(prefix))
	}
	if cfg.Verbose {
		loggers = append(loggers, logger.NewCerrLogger())
	}
	return loggers, nil
}

// closeLoggers closes every logger, ignoring close errors.
// closeLoggers 关闭每一个 logger,忽略关闭错误。
func closeLoggers(loggers []logger.Logger) {
	for _, l := range loggers {
		_ = l.Close()
	}
}

// notifyGenerated dispatches a Generated event to all loggers, recovering from
// any logger panic.
//
// notifyGenerated 向所有 logger 分发一个 Generated 事件,并从任何 logger panic 中
// 恢复。
func notifyGenerated(loggers []logger.Logger, ev logger.Event) {
	for _, l := range loggers {
		func() {
			defer func() { _ = recover() }()
			l.Generated(ev)
		}()
	}
}

// notifyParsed dispatches a Parsed event to all loggers, recovering from any
// logger panic.
//
// notifyParsed 向所有 logger 分发一个 Parsed 事件,并从任何 logger panic 中恢复。
func notifyParsed(loggers []logger.Logger, ev logger.Event) {
	for _, l := range loggers {
		func() {
			defer func() { _ = recover() }()
			l.Parsed(ev)
		}()
	}
}

// notifyExecuted dispatches an Executed event to all loggers, recovering from
// any logger panic.
//
// notifyExecuted 向所有 logger 分发一个 Executed 事件,并从任何 logger panic 中
// 恢复。
func notifyExecuted(loggers []logger.Logger, ev logger.Event) {
	for _, l := range loggers {
		func() {
			defer func() { _ = recover() }()
			l.Executed(ev)
		}()
	}
}

// notifyError dispatches an Error event to all loggers, recovering from any
// logger panic.
//
// notifyError 向所有 logger 分发一个 Error 事件,并从任何 logger panic 中恢复。
func notifyError(loggers []logger.Logger, ev logger.Event, class string, err error) {
	for _, l := range loggers {
		func() {
			defer func() { _ = recover() }()
			l.Error(ev, class, err)
		}()
	}
}

// recoverConnection repeatedly attempts to reconnect the executor until it
// succeeds or the context is cancelled, handling taosd incidents between tries.
//
// recoverConnection 反复尝试重连 executor,直到成功或 context 被取消,并在两次
// 尝试之间处理 taosd 事件。
func recoverConnection(ctx context.Context, exec *executor.Executor, dsn string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		reconnectErr := exec.Reconnect(ctx, dsn)
		if reconnectErr == nil {
			return nil
		}
		if taosdShouldHandle(string(executor.ClassConnLost), reconnectErr) {
			_ = taosdHandleIncident(ctx, string(executor.ClassConnLost), "", reconnectErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// handleInitConnectionFailure handles a failure to establish the initial
// executor connection: it records the error, asks the taosd watcher to handle
// it, and—if taosd was restarted or is recoverable—retries the connection,
// returning a working executor or a descriptive error.
//
// handleInitConnectionFailure 处理建立初始 executor 连接失败的情况:它记录错误,
// 请求 taosd 监视器处理,并在 taosd 被重启或可恢复时重试连接,返回一个可用的
// executor 或一个描述性错误。
func handleInitConnectionFailure(
	ctx context.Context,
	dsn string,
	initErr error,
	stats *report.Stats,
	recordErr func(kind, msg string),
	taosdIncidents *[]report.TaosdIncident,
	taosdCrashIncidents *[]report.CrashIncident,
	incidentSeq *int64,
) (*executor.Executor, error) {
	recordErr(string(executor.ClassConnLost), errString(initErr))
	if !taosdShouldHandle(string(executor.ClassConnLost), initErr) {
		return nil, initErr
	}

	inc := taosdHandleIncident(ctx, string(executor.ClassConnLost), "", initErr)
	if !inc.Checked {
		return nil, initErr
	}

	if stats != nil {
		stats.ConnLost++
	}
	if taosdIncidents != nil {
		*taosdIncidents = append(*taosdIncidents, report.TaosdIncident{
			OccurredAt:       inc.OccurredAt,
			ExecClass:        inc.ExecClass,
			CaseID:           "init_ping",
			Rule:             "init_ping",
			SQL:              "",
			Error:            errString(initErr),
			ProcessExists:    inc.ProcessExists,
			ProcessCheck:     inc.ProcessCheck,
			ExitReason:       inc.ExitReason,
			CoredumpDetected: inc.CoredumpDetected,
			CoredumpEvidence: inc.CoredumpEvidence,
			RestartAttempted: inc.RestartAttempted,
			RestartCommand:   inc.RestartCommand,
			RestartSucceeded: inc.RestartSucceeded,
			RestartOutput:    inc.RestartOutput,
			RestartError:     inc.RestartError,
		})
	}
	if inc.CoredumpDetected {
		if stats != nil {
			stats.TaosdCoredump++
		}
		recordErr("taosd_coredump", inc.CoredumpEvidence)
	}
	if shouldRecordTaosdCrash(inc) {
		_ = appendTaosdCrashIncident(taosdCrashIncidents, incidentSeq, inc.OccurredAt, "")
	}
	if !inc.ProcessExists {
		if inc.RestartSucceeded {
			if stats != nil {
				stats.TaosdRestart++
			}
		} else if inc.RestartAttempted {
			recordErr("taosd_restart", inc.RestartError)
		}
		retried, retryErr := waitExecutorReady(ctx, dsn, stats, recordErr)
		if retryErr == nil {
			return retried, nil
		}
		return nil, fmt.Errorf("initial ping failed (%s), taosd recovery failed: %w", describeTaosdInitIncident(inc), retryErr)
	}

	if inc.RestartSucceeded {
		if stats != nil {
			stats.TaosdRestart++
		}
		retried, retryErr := executorNewFn(ctx, dsn)
		if retryErr == nil {
			return retried, nil
		}
		recordErr(string(executor.ClassConnLost), errString(retryErr))
		return nil, fmt.Errorf("initial ping failed then taosd restarted (%s), but reconnect still failed: %w",
			describeTaosdInitIncident(inc), retryErr)
	}
	if inc.RestartAttempted {
		recordErr("taosd_restart", inc.RestartError)
	}
	return nil, fmt.Errorf("initial ping failed (%s): %w", describeTaosdInitIncident(inc), initErr)
}

// waitExecutorReady polls until a new executor connection succeeds or the
// context is cancelled, handling taosd incidents between attempts.
//
// waitExecutorReady 轮询直到一个新的 executor 连接成功或 context 被取消,并在两次
// 尝试之间处理 taosd 事件。
func waitExecutorReady(
	ctx context.Context,
	dsn string,
	stats *report.Stats,
	recordErr func(kind, msg string),
) (*executor.Executor, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		retried, retryErr := executorNewFn(ctx, dsn)
		if retryErr == nil {
			return retried, nil
		}
		recordErr(string(executor.ClassConnLost), errString(retryErr))
		if taosdShouldHandle(string(executor.ClassConnLost), retryErr) {
			inc := taosdHandleIncident(ctx, string(executor.ClassConnLost), "", retryErr)
			if inc.RestartSucceeded {
				if stats != nil {
					stats.TaosdRestart++
				}
			} else if inc.RestartAttempted {
				recordErr("taosd_restart", inc.RestartError)
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// describeTaosdInitIncident formats a taosd incident into a compact, single-line
// human-readable description for inclusion in error messages.
//
// describeTaosdInitIncident 将一个 taosd 事件格式化为紧凑的单行可读描述,以便包含
// 在错误信息中。
func describeTaosdInitIncident(inc taosdwatch.Incident) string {
	parts := make([]string, 0, 6)
	parts = append(parts, fmt.Sprintf("process_exists=%t", inc.ProcessExists))
	if s := strings.TrimSpace(inc.ProcessCheck); s != "" {
		parts = append(parts, "process_check="+normalizeErr(s))
	}
	parts = append(parts, fmt.Sprintf("restart_attempted=%t", inc.RestartAttempted))
	parts = append(parts, fmt.Sprintf("restart_succeeded=%t", inc.RestartSucceeded))
	parts = append(parts, fmt.Sprintf("coredump_detected=%t", inc.CoredumpDetected))
	if s := strings.TrimSpace(inc.CoredumpEvidence); s != "" {
		parts = append(parts, "coredump_evidence="+normalizeErr(s))
	}
	if s := strings.TrimSpace(inc.ExitReason); s != "" {
		parts = append(parts, "exit_reason="+normalizeErr(s))
	}
	if s := strings.TrimSpace(inc.RestartError); s != "" {
		parts = append(parts, "restart_error="+normalizeErr(s))
	}
	return strings.Join(parts, ", ")
}

// normalizeErr trims and collapses whitespace in an error message and truncates
// it to 240 characters for compact reporting.
//
// normalizeErr 修剪并合并错误信息中的空白字符,并将其截断为 240 个字符以便紧凑
// 报告。
func normalizeErr(msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return ""
	}
	msg = strings.Join(strings.Fields(msg), " ")
	if len(msg) > 240 {
		msg = msg[:240]
	}
	return msg
}

// errString returns the error's message, or an empty string for a nil error.
// errString 返回错误的消息文本,对于 nil 错误返回空字符串。
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// schemaFromPrepared builds a querygen.Schema from the prepared catalog,
// skipping tables and columns with empty names or no usable columns.
//
// schemaFromPrepared 根据已准备的 catalog 构建一个 querygen.Schema,跳过名称为空
// 或无可用列的表和列。
func schemaFromPrepared(p *catalog.Prepared) querygen.Schema {
	if p == nil || len(p.Tables) == 0 {
		return querygen.Schema{}
	}
	out := querygen.Schema{Tables: make([]querygen.Table, 0, len(p.Tables))}
	for _, t := range p.Tables {
		if strings.TrimSpace(t.Name) == "" {
			continue
		}
		qt := querygen.Table{Name: t.Name, Columns: make([]querygen.Column, 0, len(t.Columns))}
		for _, c := range t.Columns {
			if strings.TrimSpace(c.Name) == "" {
				continue
			}
			qt.Columns = append(qt.Columns, querygen.Column{Name: c.Name, Type: c.Type})
		}
		if len(qt.Columns) == 0 {
			continue
		}
		out.Tables = append(out.Tables, qt)
	}
	return out
}

// queryRulesFromTags maps generation tags to grammar query-rule names (via an
// alias table) and returns the sorted, de-duplicated set of rules they imply.
//
// queryRulesFromTags 将生成标签(通过别名表)映射为文法 query-rule 名称,并返回
// 它们所蕴含的、经排序去重的规则集合。
func queryRulesFromTags(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}
	alias := map[string][]string{
		"where":            {"where_clause_opt"},
		"group_by":         {"group_by_clause_opt"},
		"having":           {"having_clause_opt"},
		"order_by":         {"order_by_clause_opt"},
		"limit":            {"limit_clause_opt"},
		"slimit":           {"slimit_clause_opt"},
		"from_clause":      {"from_clause_opt"},
		"partition":        {"partition_by_clause_opt"},
		"window":           {"twindow_clause_opt"},
		"fill":             {"fill_opt", "interp_fill_opt"},
		"function":         {"function_expression"},
		"predicate":        {"predicate"},
		"search_condition": {"search_condition"},
	}
	uniq := make(map[string]struct{}, len(tags)*2)
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		uniq[tag] = struct{}{}
		for _, mapped := range alias[tag] {
			if strings.TrimSpace(mapped) == "" {
				continue
			}
			uniq[mapped] = struct{}{}
		}
	}
	out := make([]string, 0, len(uniq))
	for rule := range uniq {
		out = append(out, rule)
	}
	sort.Strings(out)
	return out
}

// defaultTrackedQueryRules returns the built-in list of grammar query rules to
// track for coverage when no catalog-provided required-rule set is available.
//
// defaultTrackedQueryRules 返回内置的文法 query rule 列表,在没有 catalog 提供的
// 必需规则集时用于跟踪覆盖率。
func defaultTrackedQueryRules() []string {
	return []string{
		"common_expression",
		"expr_or_subquery",
		"insert_query",
		"query_expression",
		"query_or_subquery",
		"query_simple",
		"query_simple_or_subquery",
		"query_specification",
		"subquery",
		"union_query_expression",
		"alias_opt",
		"column_alias",
		"column_name",
		"column_name_list",
		"column_reference",
		"db_name",
		"from_clause_opt",
		"full_table_name",
		"pseudo_column",
		"select_item",
		"select_list",
		"table_alias",
		"table_name",
		"table_primary",
		"table_reference",
		"table_reference_list",
		"anti_joined",
		"asof_joined",
		"inner_joined",
		"join_on_clause",
		"join_on_clause_opt",
		"joined_table",
		"outer_joined",
		"parenthesized_joined_table",
		"semi_joined",
		"win_joined",
		"boolean_primary",
		"boolean_value_expression",
		"compare_op",
		"in_op",
		"in_predicate_value",
		"predicate",
		"search_condition",
		"where_clause_opt",
		"every_opt",
		"group_by_clause_opt",
		"group_by_list",
		"having_clause_opt",
		"jlimit_clause_opt",
		"limit_clause_opt",
		"null_ordering_opt",
		"order_by_clause_opt",
		"ordering_specification_opt",
		"range_opt",
		"sliding_opt",
		"slimit_clause_opt",
		"sort_specification",
		"sort_specification_list",
		"count_window_args",
		"extend_literal",
		"fill_mode",
		"fill_opt",
		"fill_position_mode",
		"fill_position_mode_extension",
		"fill_value",
		"interp_fill_mode",
		"interp_fill_opt",
		"interval_sliding_duration_literal",
		"state_window_opt",
		"true_for_opt",
		"twindow_clause_opt",
		"window_offset_clause",
		"window_offset_literal",
		"zeroth_literal",
		"partition_by_clause_opt",
		"partition_item",
		"partition_list",
		"tag_mode_opt",
		"case_when_else_opt",
		"case_when_expression",
		"cols_func",
		"cols_func_expression",
		"cols_func_expression_list",
		"cols_func_para_list",
		"expression",
		"expression_list",
		"function_expression",
		"function_name",
		"if_expression",
		"literal_func",
		"noarg_func",
		"other_para_list",
		"rand_func",
		"star_func",
		"star_func_para",
		"star_func_para_list",
		"substr_func",
		"trim_specification_type",
		"when_then_expr",
		"when_then_list",
		"duration_literal",
		"literal",
		"literal_list",
		"signed",
		"signed_float",
		"signed_integer",
		"signed_literal",
		"trigger_col_name",
		"type_name",
		"type_name_default_len",
		"unsigned_integer",
		"set_quantifier_opt",
		"hint_list",
	}
}

// pickGenerationStrategy chooses a generation strategy via weighted random
// selection, boosting branch-case and rule-seed strategies when there are
// uncovered branch cases or missing rules and biasing toward complex queries
// when the missing rules involve joins, windows, subqueries, or functions.
//
// pickGenerationStrategy 通过加权随机选择来挑选生成策略:当存在未覆盖的 branch
// 用例或缺失规则时提升 branch-case 和 rule-seed 策略的权重,并在缺失规则涉及
// join、窗口、子查询或函数时偏向生成复杂查询。
func pickGenerationStrategy(rng *random.RNG, missingBranchCases int, missingRules []string) generationStrategy {
	// weightedStrategy pairs a strategy with its selection weight.
	// weightedStrategy 将一个策略与其选择权重配对。
	type weightedStrategy struct {
		name   generationStrategy // the candidate strategy / 候选策略
		weight int                // relative selection weight / 相对选择权重
	}
	missingRuleCount := len(missingRules)
	options := []weightedStrategy{
		{name: strategyBranchCase, weight: 28},
		{name: strategyRuleSeed, weight: 24},
		{name: strategyQueryRandom, weight: 30},
		{name: strategyWorkload, weight: 18},
	}
	if missingBranchCases > 0 {
		boost := 35
		if missingBranchCases < 20 {
			boost = 22
		}
		options[0].weight += boost
		options[2].weight -= 10
		options[3].weight -= 12
	}
	if missingRuleCount > 0 {
		boost := 30
		if missingRuleCount < 20 {
			boost = 20
		}
		options[1].weight += boost
		options[2].weight += 10
		options[3].weight -= 12
	}
	complexGap := queryRuleComplexGapScore(missingRules)
	if complexGap > 0 {
		add := 12
		if complexGap > 16 {
			add = 20
		}
		options[1].weight += add / 2
		options[2].weight += add
		options[0].weight -= 10
		options[3].weight -= 8
	}
	if missingBranchCases == 0 {
		options[0].weight = 0
	}
	if missingRuleCount == 0 {
		options[1].weight = 0
	}
	if missingBranchCases == 0 && missingRuleCount == 0 {
		options[2].weight = 42
		options[3].weight = 58
	}
	total := 0
	for i := range options {
		if options[i].weight < 0 {
			options[i].weight = 0
		}
		total += options[i].weight
	}
	if total <= 0 {
		return strategyQueryRandom
	}
	pick := rng.Intn(total)
	acc := 0
	for _, o := range options {
		if o.weight <= 0 {
			continue
		}
		acc += o.weight
		if pick < acc {
			return o.name
		}
	}
	return strategyQueryRandom
}

// queryRuleComplexGapScore scores the missing rules by how complex the queries
// needed to cover them are (joins/windows weigh most, functions least), used to
// bias strategy selection toward generating more complex statements.
//
// queryRuleComplexGapScore 根据覆盖缺失规则所需查询的复杂度为其打分(join/窗口
// 权重最高,函数最低),用于使策略选择偏向生成更复杂的语句。
func queryRuleComplexGapScore(missingRules []string) int {
	score := 0
	for _, rule := range missingRules {
		r := strings.ToLower(strings.TrimSpace(rule))
		switch {
		case strings.Contains(r, "join"):
			score += 3
		case strings.Contains(r, "window"), strings.Contains(r, "interval"), strings.Contains(r, "fill"):
			score += 3
		case strings.Contains(r, "subquery"), strings.Contains(r, "union"), strings.Contains(r, "query_simple_or_subquery"):
			score += 2
		case strings.Contains(r, "function"), strings.Contains(r, "func"), strings.Contains(r, "expression"):
			score++
		}
	}
	return score
}

// appendUniqueStrategies appends items to base, dropping empty entries and
// duplicates, preserving first-seen order.
//
// appendUniqueStrategies 将 items 追加到 base,丢弃空项和重复项,并保持首次出现
// 的顺序。
func appendUniqueStrategies(base []generationStrategy, items ...generationStrategy) []generationStrategy {
	seen := make(map[generationStrategy]struct{}, len(base)+len(items))
	out := make([]generationStrategy, 0, len(base)+len(items))
	for _, s := range base {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		out = append(out, s)
		seen[s] = struct{}{}
	}
	for _, s := range items {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		out = append(out, s)
		seen[s] = struct{}{}
	}
	return out
}

// skipExecutionForCoverageSeed reports whether a statement is a coverage-only
// seed (SEL_/NEST_/QRYRULE_ cases, but not QGEN) that should be parsed for
// coverage but not actually executed.
//
// skipExecutionForCoverageSeed 报告某条语句是否为仅用于覆盖率的种子
// (SEL_/NEST_/QRYRULE_ 用例,但不含 QGEN),这类语句应为覆盖率而解析但不实际
// 执行。
func skipExecutionForCoverageSeed(caseID string) bool {
	id := strings.TrimSpace(caseID)
	if id == "" {
		return false
	}
	if strings.HasPrefix(id, "QGEN") {
		return false
	}
	return strings.HasPrefix(id, "SEL_") || strings.HasPrefix(id, "NEST_") || strings.HasPrefix(id, "QRYRULE_")
}

// shouldExecuteStatement decides whether a parsed statement should be executed,
// dispatching to the strict/balanced/aggressive gate based on the profile. Only
// SELECT statements are eligible.
//
// shouldExecuteStatement 根据档位将其分发到 strict/balanced/aggressive 门控,以决定
// 一条已解析的语句是否应被执行。只有 SELECT 语句才有资格。
func shouldExecuteStatement(stmt sqlparser.Statement, sqlText string, profile string) bool {
	sel, ok := stmt.(*sqlparser.SelectStmt)
	if !ok || sel == nil {
		return false
	}
	switch normalizeExecProfile(profile) {
	case "aggressive":
		return shouldExecuteAggressive(sel, sqlText)
	case "balanced":
		return shouldExecuteBalanced(sel, sqlText)
	default:
		return shouldExecuteStrict(sel, sqlText)
	}
}

// shouldExecuteStrict gates execution under the strict profile: only simple
// single-table SELECTs without set operations, grouping, windows, partitions,
// fills, or risky heuristic patterns are allowed.
//
// shouldExecuteStrict 在 strict 档位下门控执行:只允许简单的单表 SELECT,不含集合
// 运算、分组、窗口、分区、fill 或有风险的启发式模式。
func shouldExecuteStrict(sel *sqlparser.SelectStmt, sqlText string) bool {
	if sel.Left != nil || sel.Right != nil || sel.SetOp != "" {
		return false
	}
	tbl, ok := sel.From.(*sqlparser.TableNameExpr)
	if !ok || tbl == nil {
		return false
	}
	if strings.TrimSpace(tbl.DBName) != "" {
		return false
	}
	if sel.GroupBy != nil || sel.Having != nil || sel.Partition != nil || sel.Range != nil || sel.InterpFill != nil || sel.SLimit != nil {
		return false
	}
	if len(sel.Every.Val.Bytes) > 0 {
		return false
	}
	if hasWindow(sel) {
		return false
	}
	if sel.IsDistinct && len(sel.OrderBy) > 0 {
		return false
	}
	return !skipExecutionByHeuristic(sqlText, "strict")
}

// shouldExecuteBalanced gates execution under the balanced profile: it allows
// grouping/having but still rejects set operations, partitions, windows, fills,
// slimit, and risky heuristic patterns.
//
// shouldExecuteBalanced 在 balanced 档位下门控执行:允许分组/having,但仍拒绝集合
// 运算、分区、窗口、fill、slimit 以及有风险的启发式模式。
func shouldExecuteBalanced(sel *sqlparser.SelectStmt, sqlText string) bool {
	if sel.Left != nil || sel.Right != nil || sel.SetOp != "" {
		return false
	}
	tbl, ok := sel.From.(*sqlparser.TableNameExpr)
	if !ok || tbl == nil {
		return false
	}
	if strings.TrimSpace(tbl.DBName) != "" {
		return false
	}
	if sel.Partition != nil || sel.Range != nil || sel.InterpFill != nil || sel.SLimit != nil || len(sel.Every.Val.Bytes) > 0 {
		return false
	}
	if hasWindow(sel) {
		return false
	}
	if sel.Having != nil && sel.GroupBy == nil {
		return false
	}
	return !skipExecutionByHeuristic(sqlText, "balanced")
}

// shouldExecuteAggressive gates execution under the aggressive profile: it
// executes any SELECT unless blocked by the heuristic pattern filter.
//
// shouldExecuteAggressive 在 aggressive 档位下门控执行:除非被启发式模式过滤器
// 拦截,否则执行任何 SELECT。
func shouldExecuteAggressive(sel *sqlparser.SelectStmt, sqlText string) bool {
	if sel == nil {
		return false
	}
	return !skipExecutionByHeuristic(sqlText, "aggressive")
}

// hasWindow reports whether the SELECT carries any windowing clause (interval,
// session, state, count, event, or anomaly window).
//
// hasWindow 报告该 SELECT 是否带有任何窗口子句(interval、session、state、count、
// event 或 anomaly 窗口)。
func hasWindow(sel *sqlparser.SelectStmt) bool {
	if sel == nil {
		return false
	}
	return len(sel.Window.Interval.Val.Bytes) > 0 ||
		sel.Window.Session != nil ||
		sel.Window.StateWindow != nil ||
		len(sel.Window.CountWindow.Bytes) > 0 ||
		sel.Window.EventWindowStart != nil ||
		sel.Window.EventWindowEnd != nil ||
		sel.Window.AnomalyWindow != nil
}

// skipExecutionByHeuristic reports whether the raw SQL text contains patterns
// (joins, unions, windows, fills, ranges, and—under strict—aggregates) that the
// given profile considers too risky or non-deterministic to execute.
//
// skipExecutionByHeuristic 报告原始 SQL 文本是否包含给定档位认为过于有风险或不
// 确定而不宜执行的模式(join、union、窗口、fill、range,以及在 strict 档位下的
// 聚合)。
func skipExecutionByHeuristic(sqlText string, profile string) bool {
	s := " " + strings.ToLower(strings.TrimSpace(sqlText)) + " "
	if strings.Contains(s, " join ") || strings.Contains(s, " union ") || strings.Contains(s, " asof ") || strings.Contains(s, " window join ") {
		return true
	}
	if profile == "strict" {
		if strings.Contains(s, " having ") || strings.Contains(s, " group by ") || strings.Contains(s, " partition by ") {
			return true
		}
	} else {
		if strings.Contains(s, " partition by ") {
			return true
		}
	}
	if strings.Contains(s, " interval(") || strings.Contains(s, " session(") || strings.Contains(s, " state_window(") || strings.Contains(s, " event_window") || strings.Contains(s, " count_window(") || strings.Contains(s, " anomaly_window(") {
		return true
	}
	if strings.Contains(s, " slimit ") || strings.Contains(s, " soffset ") || strings.Contains(s, " range(") || strings.Contains(s, " every(") || strings.Contains(s, " fill(") {
		return true
	}
	if profile == "strict" && (strings.Contains(s, " sum(") || strings.Contains(s, " avg(") || strings.Contains(s, " min(") || strings.Contains(s, " max(") || strings.Contains(s, " count(") || strings.Contains(s, " cols(")) {
		return true
	}
	return false
}

// normalizeExecProfile canonicalizes a profile string to "aggressive",
// "balanced", or "strict" (the default for unrecognized input).
//
// normalizeExecProfile 将档位字符串规范化为 "aggressive"、"balanced" 或 "strict"
// (无法识别的输入默认为 "strict")。
func normalizeExecProfile(in string) string {
	switch strings.ToLower(strings.TrimSpace(in)) {
	case "aggressive":
		return "aggressive"
	case "balanced":
		return "balanced"
	default:
		return "strict"
	}
}

// formatIncidentID formats a 1-based sequence number into an incident ID like
// "incident_000001".
//
// formatIncidentID 将一个从 1 开始的序号格式化为类似 "incident_000001" 的事件 ID。
func formatIncidentID(seq int64) string {
	if seq <= 0 {
		seq = 1
	}
	return fmt.Sprintf("incident_%06d", seq)
}

// latestManagedExitSeenAt returns the more recent of lastSeen and the taosd
// watcher's last managed-exit timestamp.
//
// latestManagedExitSeenAt 返回 lastSeen 与 taosd 监视器最近一次受管理退出时间戳
// 中较新的一个。
func latestManagedExitSeenAt(lastSeen time.Time) time.Time {
	at, ok := taosdLastManagedAt(time.Time{})
	if !ok || at.IsZero() {
		return lastSeen
	}
	if at.After(lastSeen) {
		return at
	}
	return lastSeen
}

// captureManagedExitIncident detects a managed taosd child exit that occurred
// since lastSeen, records it as a taosd incident (and, if it qualifies, a crash
// incident), attributes the last in-flight SQL to it, attempts connection
// recovery, and reports whether the minimal snapshot should be force-flushed.
//
// captureManagedExitIncident 检测自 lastSeen 以来发生的受管理 taosd 子进程退出,
// 将其记录为一个 taosd 事件(若符合条件还记录为崩溃事件),将最后正在执行的 SQL
// 归因于它,尝试恢复连接,并报告是否应强制刷写最小快照。
func captureManagedExitIncident(
	runCtx context.Context,
	exec *executor.Executor,
	dsn string,
	stats *report.Stats,
	recordErr func(kind, msg string),
	taosdIncidents *[]report.TaosdIncident,
	taosdCrashIncidents *[]report.CrashIncident,
	incidentSeq *int64,
	lastSeen *time.Time,
	lastCrashSQL string,
	crashLatestPath string,
) bool {
	if taosdIncidents == nil || taosdCrashIncidents == nil || incidentSeq == nil || lastSeen == nil {
		return false
	}

	since := *lastSeen
	if !since.IsZero() {
		since = since.Add(time.Nanosecond)
	}
	at, ok := taosdLastManagedAt(since)
	if !ok || at.IsZero() {
		return false
	}
	if at.After(*lastSeen) {
		*lastSeen = at
	}

	inc := taosdHandleIncident(
		runCtx,
		string(executor.ClassConnLost),
		"",
		errors.New("managed taosd child exited"),
	)
	if !inc.Checked {
		return false
	}
	if inc.OccurredAt.After(*lastSeen) {
		*lastSeen = inc.OccurredAt
	}

	crashSQL := strings.TrimSpace(lastCrashSQL)
	if crashSQL == "" {
		crashSQL = latestCrashCandidateSQL(crashLatestPath)
	}

	*taosdIncidents = append(*taosdIncidents, report.TaosdIncident{
		OccurredAt:       inc.OccurredAt,
		ExecClass:        inc.ExecClass,
		CaseID:           "managed_exit",
		Rule:             "managed_exit",
		SQL:              crashSQL,
		Error:            normalizeErr(inc.Error),
		ProcessExists:    inc.ProcessExists,
		ProcessCheck:     inc.ProcessCheck,
		ExitReason:       inc.ExitReason,
		CoredumpDetected: inc.CoredumpDetected,
		CoredumpEvidence: inc.CoredumpEvidence,
		RestartAttempted: inc.RestartAttempted,
		RestartCommand:   inc.RestartCommand,
		RestartSucceeded: inc.RestartSucceeded,
		RestartOutput:    inc.RestartOutput,
		RestartError:     inc.RestartError,
	})

	forceFlushMinimalSnapshot := false
	if shouldRecordTaosdCrash(inc) {
		incidentID := appendTaosdCrashIncident(taosdCrashIncidents, incidentSeq, inc.OccurredAt, crashSQL)
		if incidentID != "" {
			forceFlushMinimalSnapshot = true
		}
		if inc.CoredumpDetected {
			if stats != nil {
				stats.TaosdCoredump++
			}
			if recordErr != nil {
				recordErr("taosd_coredump", inc.CoredumpEvidence)
			}
		}
	}
	if inc.RestartSucceeded {
		if stats != nil {
			stats.TaosdRestart++
		}
		if exec != nil {
			_ = recoverConnection(runCtx, exec, dsn)
		}
	} else if inc.RestartAttempted {
		if recordErr != nil {
			recordErr("taosd_restart", inc.RestartError)
		}
	}
	return forceFlushMinimalSnapshot
}

// shouldRecordTaosdCrash decides whether a taosd incident should be recorded as
// a crash: either it carries a suspect runtime error, or it detected a coredump
// with taosd-attributable evidence.
//
// shouldRecordTaosdCrash 判断一个 taosd 事件是否应被记录为崩溃:要么它带有可疑的
// 运行时错误,要么它检测到了带有可归因于 taosd 证据的 coredump。
func shouldRecordTaosdCrash(inc taosdwatch.Incident) bool {
	// Some internal taosd crashes bubble up as opaque DB errors instead of conn_lost.
	// Keep recording these as crash incidents so crash_sql is not dropped.
	// 一些 taosd 内部崩溃会以不透明的 DB 错误而非 conn_lost 的形式冒泡上来。继续
	// 将其记录为崩溃事件,以免 crash_sql 被丢弃。
	if isTaosdSuspectRuntimeError(inc.Error) {
		return true
	}
	// With parent-child process model, we get direct signal info from managed exit metadata.
	// Check for crash signals in the coredump evidence (which comes from direct process state).
	// 在父子进程模型下,我们可以从受管理退出元数据中获得直接的信号信息。检查
	// coredump 证据(来自直接的进程状态)中是否包含崩溃信号。
	if inc.CoredumpDetected {
		return isTaosdCoredumpEvidence(inc.CoredumpEvidence)
	}
	return false
}

// isTaosdSuspectRuntimeError reports whether the error text looks like an opaque
// taosd internal failure ("unknown error 65535") that should be treated as a crash.
//
// isTaosdSuspectRuntimeError 报告错误文本是否像是一个不透明的 taosd 内部故障
// ("unknown error 65535"),应当被当作崩溃处理。
func isTaosdSuspectRuntimeError(errText string) bool {
	low := strings.ToLower(strings.TrimSpace(errText))
	if low == "" {
		return false
	}
	return strings.Contains(low, "unknown error 65535")
}

// isTaosdCoredumpEvidence checks if the coredump evidence indicates a taosd crash.
// Evidence from parent-child model contains "managed taosd exited by signal ..."
//
// isTaosdCoredumpEvidence 检查 coredump 证据是否表明发生了 taosd 崩溃。来自父子
// 进程模型的证据包含 "managed taosd exited by signal ..."。
func isTaosdCoredumpEvidence(evidence string) bool {
	low := strings.ToLower(strings.TrimSpace(evidence))
	if low == "" {
		return false
	}
	// With direct child monitoring, evidence comes from managed exit meta
	// 在直接监控子进程的情况下,证据来自受管理退出的元数据
	return strings.Contains(low, "managed taosd exited by signal")
}

// appendTaosdCrashIncident appends a new crash incident with an auto-incremented
// ID and the given crashing SQL, returning the assigned incident ID.
//
// appendTaosdCrashIncident 追加一个新的崩溃事件,使用自增的 ID 和给定的崩溃 SQL,
// 并返回分配的事件 ID。
func appendTaosdCrashIncident(
	crashIncidents *[]report.CrashIncident,
	incidentSeq *int64,
	occurredAt time.Time,
	crashSQL string,
) string {
	if crashIncidents == nil || incidentSeq == nil {
		return ""
	}
	*incidentSeq = *incidentSeq + 1
	incidentID := formatIncidentID(*incidentSeq)
	*crashIncidents = append(*crashIncidents, report.CrashIncident{
		IncidentID: incidentID,
		OccurredAt: occurredAt,
		CrashSQL:   strings.TrimSpace(crashSQL),
	})
	return incidentID
}

// latestCrashCandidateSQL loads the latest crash-guard snapshot at latestPath
// and returns the most relevant SQL (pending statement, else the last non-empty
// window entry), or empty if unavailable.
//
// latestCrashCandidateSQL 加载 latestPath 处最新的 crash-guard 快照,并返回最相关
// 的 SQL(待执行语句,否则是最后一条非空窗口记录),若不可用则返回空。
func latestCrashCandidateSQL(latestPath string) string {
	path := strings.TrimSpace(latestPath)
	if path == "" {
		return ""
	}
	snap, err := crashguard.LoadLatest(path)
	if err != nil || snap == nil {
		return ""
	}
	if snap.Pending != nil {
		if sqlText := strings.TrimSpace(snap.Pending.SQL); sqlText != "" {
			return sqlText
		}
	}
	for i := len(snap.Window) - 1; i >= 0; i-- {
		if sqlText := strings.TrimSpace(snap.Window[i].SQL); sqlText != "" {
			return sqlText
		}
	}
	return ""
}

// queryRulesFromReductions maps a set of grammar reduction IDs to their
// query-rule names using the catalog, returning nil if either is empty.
//
// queryRulesFromReductions 使用 catalog 将一组文法 reduction ID 映射为其
// query-rule 名称,如果两者任一为空则返回 nil。
func queryRulesFromReductions(catalog *queryrules.Catalog, reductions []int) []string {
	if catalog == nil || len(reductions) == 0 {
		return nil
	}
	return catalog.QueryRulesFromReductions(reductions)
}

// loadRuntimeQueryRuleCatalog searches candidate sqlparse roots for a td_sql.y
// grammar and loads the query-rule catalog from the first one that yields a
// non-empty required-rule set, returning nil if none is found.
//
// loadRuntimeQueryRuleCatalog 在候选 sqlparse 根目录中搜索 td_sql.y 文法,并从第
// 一个能产生非空必需规则集的目录加载 query-rule catalog,若都未找到则返回 nil。
func loadRuntimeQueryRuleCatalog() *queryrules.Catalog {
	for _, root := range runtimeSQLParseRootCandidates() {
		if _, err := os.Stat(filepath.Join(root, "td_sql.y")); err != nil {
			continue
		}
		cat, err := queryrules.LoadCatalog(root)
		if err != nil || cat == nil {
			continue
		}
		if len(cat.RequiredRules()) == 0 {
			continue
		}
		return cat
	}
	return nil
}

// runtimeSQLParseRootCandidates returns candidate directories that may contain
// the sqlparse grammar, derived from the SQLPARSE_ROOT env var, the working
// directory, and the executable directory.
//
// runtimeSQLParseRootCandidates 返回可能包含 sqlparse 文法的候选目录,这些目录
// 派生自 SQLPARSE_ROOT 环境变量、工作目录和可执行文件所在目录。
func runtimeSQLParseRootCandidates() []string {
	candidates := make([]string, 0, 20)
	if env := strings.TrimSpace(os.Getenv("SQLPARSE_ROOT")); env != "" {
		candidates = appendUniquePath(candidates, env)
	}
	if wd, err := os.Getwd(); err == nil {
		candidates = appendSQLParseRootCandidates(candidates, wd)
	}
	if exe, err := os.Executable(); err == nil {
		candidates = appendSQLParseRootCandidates(candidates, filepath.Dir(exe))
	}
	return candidates
}

// appendSQLParseRootCandidates walks up from base (up to 8 levels) appending
// "sqlparse" and "../sqlparse" candidate paths at each level.
//
// appendSQLParseRootCandidates 从 base 向上遍历(最多 8 层),在每一层追加
// "sqlparse" 和 "../sqlparse" 候选路径。
func appendSQLParseRootCandidates(out []string, base string) []string {
	cur := filepath.Clean(strings.TrimSpace(base))
	if cur == "" {
		return out
	}
	for depth := 0; depth <= 8; depth++ {
		out = appendUniquePath(out, filepath.Join(cur, "../sqlparse"))
		out = appendUniquePath(out, filepath.Join(cur, "sqlparse"))
		next := filepath.Dir(cur)
		if next == cur {
			break
		}
		cur = next
	}
	return out
}

// appendUniquePath appends the absolute form of raw to paths, skipping empties
// and duplicates.
//
// appendUniquePath 将 raw 的绝对路径形式追加到 paths,跳过空项和重复项。
func appendUniquePath(paths []string, raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return paths
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return paths
	}
	for _, existing := range paths {
		if existing == abs {
			return paths
		}
	}
	return append(paths, abs)
}

// loadExistingMinimalReport reads a previously written minimal run report from
// reportPath to support resuming/merging, returning nil if absent or unreadable.
//
// loadExistingMinimalReport 从 reportPath 读取此前写入的最小运行报告以支持
// 恢复/合并,如果不存在或不可读则返回 nil。
func loadExistingMinimalReport(reportPath string) *report.MinimalRunReport {
	reportPath = strings.TrimSpace(reportPath)
	if reportPath == "" {
		return nil
	}
	prev, err := report.ReadMinimalRunReport(reportPath)
	if err != nil {
		return nil
	}
	return prev
}

// minimalTaosdIncidents returns the taosd incidents from a minimal report, or
// nil if none.
//
// minimalTaosdIncidents 返回最小报告中的 taosd 事件,若没有则返回 nil。
func minimalTaosdIncidents(mini *report.MinimalRunReport) []report.CrashIncident {
	if mini == nil || len(mini.TaosdIncidents) == 0 {
		return nil
	}
	return mini.TaosdIncidents
}

// minimalTDsqlsmithIncidents returns the tdsqlsmith (worker-side) incidents from
// a minimal report, or nil if none.
//
// minimalTDsqlsmithIncidents 返回最小报告中的 tdsqlsmith(worker 侧)事件,若没有
// 则返回 nil。
func minimalTDsqlsmithIncidents(mini *report.MinimalRunReport) []report.CrashIncident {
	if mini == nil || len(mini.TDsqlsmithIncidents) == 0 {
		return nil
	}
	return mini.TDsqlsmithIncidents
}

// minimalSetupSQL returns the setup SQL from a minimal report, or nil if none.
// minimalSetupSQL 返回最小报告中的 setup SQL,若没有则返回 nil。
func minimalSetupSQL(mini *report.MinimalRunReport) []string {
	if mini == nil || len(mini.SetupSQL) == 0 {
		return nil
	}
	return mini.SetupSQL
}

// minimalTotalExecuted returns the cumulative executed-statement count from a
// minimal report, or 0 if nil.
//
// minimalTotalExecuted 返回最小报告中累计的已执行语句数,若为 nil 则返回 0。
func minimalTotalExecuted(mini *report.MinimalRunReport) int64 {
	if mini == nil {
		return 0
	}
	return mini.TotalExecuted
}

// minimalStartedAt returns the run start time from a minimal report, or the zero
// time if nil.
//
// minimalStartedAt 返回最小报告中的运行开始时间,若为 nil 则返回零值时间。
func minimalStartedAt(mini *report.MinimalRunReport) time.Time {
	if mini == nil {
		return time.Time{}
	}
	return mini.StartedAt
}

// minimalQueryRuleCoverage returns the query-rule coverage summary from a
// minimal report, or a zero summary if nil.
//
// minimalQueryRuleCoverage 返回最小报告中的 query-rule 覆盖率摘要,若为 nil 则
// 返回零值摘要。
func minimalQueryRuleCoverage(mini *report.MinimalRunReport) queryrules.Summary {
	if mini == nil {
		return queryrules.Summary{}
	}
	return mini.QueryRuleCoverage
}

// minimalQueryRuleProgress returns the query-rule progress points from a minimal
// report, or nil if none.
//
// minimalQueryRuleProgress 返回最小报告中的 query-rule 进度点,若没有则返回 nil。
func minimalQueryRuleProgress(mini *report.MinimalRunReport) []report.QueryRuleProgressPoint {
	if mini == nil || len(mini.QueryRuleProgress) == 0 {
		return nil
	}
	return mini.QueryRuleProgress
}

// minimalQueryComboCounts returns the per-tag query combination counts from a
// minimal report, or nil if none.
//
// minimalQueryComboCounts 返回最小报告中按标签统计的查询组合计数,若没有则返回
// nil。
func minimalQueryComboCounts(mini *report.MinimalRunReport) map[string]int64 {
	if mini == nil || len(mini.QueryComboCounts) == 0 {
		return nil
	}
	return mini.QueryComboCounts
}

// cloneCrashIncidents returns a deep copy of the crash incidents with trimmed
// IDs and SQL, or nil if empty.
//
// cloneCrashIncidents 返回崩溃事件的深拷贝,其中 ID 和 SQL 已被修剪,若为空则
// 返回 nil。
func cloneCrashIncidents(items []report.CrashIncident) []report.CrashIncident {
	if len(items) == 0 {
		return nil
	}
	out := make([]report.CrashIncident, 0, len(items))
	for _, item := range items {
		out = append(out, report.CrashIncident{
			IncidentID: strings.TrimSpace(item.IncidentID),
			OccurredAt: item.OccurredAt,
			CrashSQL:   strings.TrimSpace(item.CrashSQL),
		})
	}
	return out
}

// cloneSetupSQL returns a copy of the setup SQL with normalized terminators,
// dropping empty entries, or nil if empty.
//
// cloneSetupSQL 返回 setup SQL 的拷贝,其终止符已规范化,丢弃空项,若为空则
// 返回 nil。
func cloneSetupSQL(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		sql := report.NormalizeSQLTerminator(item)
		if sql == "" {
			continue
		}
		out = append(out, sql)
	}
	return out
}

// cloneQueryRuleProgress returns a deep copy of the query-rule progress points,
// or nil if empty.
//
// cloneQueryRuleProgress 返回 query-rule 进度点的深拷贝,若为空则返回 nil。
func cloneQueryRuleProgress(items []report.QueryRuleProgressPoint) []report.QueryRuleProgressPoint {
	if len(items) == 0 {
		return nil
	}
	out := make([]report.QueryRuleProgressPoint, 0, len(items))
	for _, item := range items {
		out = append(out, report.QueryRuleProgressPoint{
			QueryNo:       item.QueryNo,
			Hit:           item.Hit,
			Required:      item.Required,
			Missing:       item.Missing,
			CoverageRatio: item.CoverageRatio,
			TopMissing:    cloneStrings(item.TopMissing),
		})
	}
	return out
}

// cloneCountMap returns a copy of the count map, dropping empty keys and
// non-positive values, or nil if nothing remains.
//
// cloneCountMap 返回计数 map 的拷贝,丢弃空键和非正值,若无剩余则返回 nil。
func cloneCountMap(in map[string]int64) map[string]int64 {
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

// mergeCountMaps adds the extra counts into base (allocating base if needed),
// skipping empty keys and non-positive values, and returns the merged map.
//
// mergeCountMaps 将 extra 中的计数累加到 base(必要时分配 base),跳过空键和非
// 正值,并返回合并后的 map。
func mergeCountMaps(base map[string]int64, extra map[string]int64) map[string]int64 {
	if len(extra) == 0 {
		return base
	}
	if base == nil {
		base = map[string]int64{}
	}
	for key, value := range extra {
		k := strings.TrimSpace(key)
		if k == "" || value <= 0 {
			continue
		}
		base[k] += value
	}
	if len(base) == 0 {
		return nil
	}
	return base
}

// mergeQueryRuleSummary combines a previous and current query-rule summary,
// keeping the larger hit/required counts (and their missing list) and
// recomputing the coverage ratio, so coverage never regresses across resumes.
//
// mergeQueryRuleSummary 合并先前与当前的 query-rule 摘要,保留较大的 hit/required
// 计数(及其 missing 列表)并重新计算覆盖率比例,使覆盖率在多次恢复间永不回退。
func mergeQueryRuleSummary(prev queryrules.Summary, cur queryrules.Summary) queryrules.Summary {
	required := cur.Required
	if prev.Required > required {
		required = prev.Required
	}
	hit := cur.Hit
	missing := cloneStrings(cur.Missing)
	if prev.Hit > hit {
		hit = prev.Hit
		missing = cloneStrings(prev.Missing)
	}
	if required < hit {
		required = hit
	}
	coverage := 0.0
	if required > 0 {
		coverage = float64(hit) / float64(required)
	}
	return queryrules.Summary{
		Required:      required,
		Hit:           hit,
		Missing:       missing,
		CoverageRatio: coverage,
	}
}

// topNStrings returns up to the first n non-empty, trimmed items, or nil if none.
// topNStrings 返回最多前 n 个非空且已修剪的项,若没有则返回 nil。
func topNStrings(items []string, n int) []string {
	if n <= 0 || len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		s := strings.TrimSpace(item)
		if s == "" {
			continue
		}
		out = append(out, s)
		if len(out) >= n {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// cloneStrings returns a sorted copy of the trimmed, non-empty strings, or nil
// if none remain.
//
// cloneStrings 返回经修剪的非空字符串的排序拷贝,若无剩余则返回 nil。
func cloneStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		s := strings.TrimSpace(item)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

// appendExecutedHistory appends rec to the rolling history, trimming it to the
// most recent maxSize entries.
//
// appendExecutedHistory 将 rec 追加到滚动历史,并将其修剪为最近的 maxSize 条记录。
func appendExecutedHistory(history []executedStmtRecord, rec executedStmtRecord, maxSize int) []executedStmtRecord {
	if maxSize <= 0 {
		return history
	}
	history = append(history, rec)
	if len(history) <= maxSize {
		return history
	}
	return append([]executedStmtRecord(nil), history[len(history)-maxSize:]...)
}

// clonePrecedingWindow converts the last `size` executed-history records into
// report.ExecutedStmtRef entries, used to capture context preceding a coredump.
//
// clonePrecedingWindow 将已执行历史中最后 `size` 条记录转换为
// report.ExecutedStmtRef 条目,用于捕获 coredump 之前的上下文。
func clonePrecedingWindow(history []executedStmtRecord, size int) []report.ExecutedStmtRef {
	if size <= 0 || len(history) == 0 {
		return nil
	}
	start := len(history) - size
	if start < 0 {
		start = 0
	}
	out := make([]report.ExecutedStmtRef, 0, len(history)-start)
	for i := start; i < len(history); i++ {
		h := history[i]
		out = append(out, report.ExecutedStmtRef{
			QueryNo:    h.QueryNo,
			OccurredAt: h.OccurredAt,
			CaseID:     h.CaseID,
			Rule:       h.Rule,
			ExecClass:  h.ExecClass,
			SQL:        h.SQL,
			Error:      h.Error,
			DurationMS: h.DurationMS,
		})
	}
	return out
}
