package run

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

type Config struct {
	Version         string
	DSN             string
	Seed            int64
	RNGState        string
	ResumeQueryNo   int64
	ResumeRNGState  string
	RunDeadline     time.Time
	Cases           int
	Duration        time.Duration
	StmtTimeout     time.Duration
	OutDir          string
	MutationLevel   int
	StopWhenCovered bool
	DryRun          bool
	Verbose         bool
	DumpAllQueries  bool
	DumpAllGraphs   bool
	ExcludeCatalog  bool
	LegacyMode      bool
	WorkloadConfig  string
	ExecProfile     string
	RunIDOverride   string
	RunDirOverride  string
	CrashGuard      bool
	SkipBootstrap   bool
}

type Result struct {
	RunID      string
	RunDir     string
	ReportPath string
	Coverage   branchmodel.CoverageSummary
	QueryRules queryrules.Summary
	Stats      report.Stats
}

const (
	executedHistoryLimit        = 64
	coredumpPrecedingWindowSize = 8
	queryRuleProgressInterval   = 20
	minimalReportFlushInterval  = 20
)

var (
	executorNewFn       = executor.New
	catalogBootstrapFn  = catalog.Bootstrap
	catalogPrepareFn    = catalog.PrepareShared
	taosdEnsureRunning  = taosdwatch.EnsureRunning
	taosdShouldHandle   = taosdwatch.ShouldHandle
	taosdHandleIncident = taosdwatch.Handle
	taosdLastManagedAt  = taosdwatch.LastManagedExitSince
	taosdStopManaged    = taosdwatch.StopManaged
)

type executedStmtRecord struct {
	QueryNo    int64
	OccurredAt time.Time
	CaseID     string
	Rule       string
	ExecClass  string
	SQL        string
	Error      string
	DurationMS int64
}

type generationStrategy string

const (
	strategyBranchCase  generationStrategy = "branch_case"
	strategyRuleSeed    generationStrategy = "rule_seed"
	strategyQueryRandom generationStrategy = "query_random"
	strategyWorkload    generationStrategy = "workload"
)

type generatedStatement struct {
	CaseID  string
	Rule    string
	SQL     string
	Mutated bool
	Kind    string
	Tags    []string
}

type crashRecorder interface {
	Before(meta crashguard.PendingStatement) error
	After(rec *crashguard.ExecutedStmt) error
	MarkCleanExit() error
	Dir() string
	LatestPath() string
}

type noopCrashRecorder struct{}

func (noopCrashRecorder) Before(crashguard.PendingStatement) error { return nil }
func (noopCrashRecorder) After(*crashguard.ExecutedStmt) error     { return nil }
func (noopCrashRecorder) MarkCleanExit() error                     { return nil }
func (noopCrashRecorder) Dir() string                              { return "" }
func (noopCrashRecorder) LatestPath() string                       { return "" }

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

func closeLoggers(loggers []logger.Logger) {
	for _, l := range loggers {
		_ = l.Close()
	}
}

func notifyGenerated(loggers []logger.Logger, ev logger.Event) {
	for _, l := range loggers {
		func() {
			defer func() { _ = recover() }()
			l.Generated(ev)
		}()
	}
}

func notifyParsed(loggers []logger.Logger, ev logger.Event) {
	for _, l := range loggers {
		func() {
			defer func() { _ = recover() }()
			l.Parsed(ev)
		}()
	}
}

func notifyExecuted(loggers []logger.Logger, ev logger.Event) {
	for _, l := range loggers {
		func() {
			defer func() { _ = recover() }()
			l.Executed(ev)
		}()
	}
}

func notifyError(loggers []logger.Logger, ev logger.Event, class string, err error) {
	for _, l := range loggers {
		func() {
			defer func() { _ = recover() }()
			l.Error(ev, class, err)
		}()
	}
}

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

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

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

func pickGenerationStrategy(rng *random.RNG, missingBranchCases int, missingRules []string) generationStrategy {
	type weightedStrategy struct {
		name   generationStrategy
		weight int
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

func shouldExecuteAggressive(sel *sqlparser.SelectStmt, sqlText string) bool {
	if sel == nil {
		return false
	}
	return !skipExecutionByHeuristic(sqlText, "aggressive")
}

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

func formatIncidentID(seq int64) string {
	if seq <= 0 {
		seq = 1
	}
	return fmt.Sprintf("incident_%06d", seq)
}

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

func shouldRecordTaosdCrash(inc taosdwatch.Incident) bool {
	// Some internal taosd crashes bubble up as opaque DB errors instead of conn_lost.
	// Keep recording these as crash incidents so crash_sql is not dropped.
	if isTaosdSuspectRuntimeError(inc.Error) {
		return true
	}
	// With parent-child process model, we get direct signal info from managed exit metadata.
	// Check for crash signals in the coredump evidence (which comes from direct process state).
	if inc.CoredumpDetected {
		return isTaosdCoredumpEvidence(inc.CoredumpEvidence)
	}
	return false
}

func isTaosdSuspectRuntimeError(errText string) bool {
	low := strings.ToLower(strings.TrimSpace(errText))
	if low == "" {
		return false
	}
	return strings.Contains(low, "unknown error 65535")
}

// isTaosdCoredumpEvidence checks if the coredump evidence indicates a taosd crash.
// Evidence from parent-child model contains "managed taosd exited by signal ..."
func isTaosdCoredumpEvidence(evidence string) bool {
	low := strings.ToLower(strings.TrimSpace(evidence))
	if low == "" {
		return false
	}
	// With direct child monitoring, evidence comes from managed exit meta
	return strings.Contains(low, "managed taosd exited by signal")
}

// Deprecated: exitReasonHasCrashSignal is no longer used with parent-child process model.
// Kept for backward compatibility during transition.
func exitReasonHasCrashSignal(reason string) bool {
	low := strings.ToLower(strings.TrimSpace(reason))
	if low == "" {
		return false
	}
	signals := []string{
		"result=signal",
		"sigsegv",
		"sigabrt",
		"sigbus",
		"sigill",
		"sigfpe",
		"segfault",
		"signal=segmentation fault",
		"signal segmentation fault",
		"signal=aborted",
		"signal aborted",
		"status=11",
		"status=6",
		"signal=11",
		"signal 11",
		"signal=6",
		"signal 6",
	}
	for _, sig := range signals {
		if strings.Contains(low, sig) {
			return true
		}
	}
	return false
}

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

func queryRulesFromReductions(catalog *queryrules.Catalog, reductions []int) []string {
	if catalog == nil || len(reductions) == 0 {
		return nil
	}
	return catalog.QueryRulesFromReductions(reductions)
}

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

func minimalTaosdIncidents(mini *report.MinimalRunReport) []report.CrashIncident {
	if mini == nil || len(mini.TaosdIncidents) == 0 {
		return nil
	}
	return mini.TaosdIncidents
}

func minimalTDsqlsmithIncidents(mini *report.MinimalRunReport) []report.CrashIncident {
	if mini == nil || len(mini.TDsqlsmithIncidents) == 0 {
		return nil
	}
	return mini.TDsqlsmithIncidents
}

func minimalSetupSQL(mini *report.MinimalRunReport) []string {
	if mini == nil || len(mini.SetupSQL) == 0 {
		return nil
	}
	return mini.SetupSQL
}

func minimalTotalExecuted(mini *report.MinimalRunReport) int64 {
	if mini == nil {
		return 0
	}
	return mini.TotalExecuted
}

func minimalStartedAt(mini *report.MinimalRunReport) time.Time {
	if mini == nil {
		return time.Time{}
	}
	return mini.StartedAt
}

func minimalQueryRuleCoverage(mini *report.MinimalRunReport) queryrules.Summary {
	if mini == nil {
		return queryrules.Summary{}
	}
	return mini.QueryRuleCoverage
}

func minimalQueryRuleProgress(mini *report.MinimalRunReport) []report.QueryRuleProgressPoint {
	if mini == nil || len(mini.QueryRuleProgress) == 0 {
		return nil
	}
	return mini.QueryRuleProgress
}

func minimalQueryComboCounts(mini *report.MinimalRunReport) map[string]int64 {
	if mini == nil || len(mini.QueryComboCounts) == 0 {
		return nil
	}
	return mini.QueryComboCounts
}

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
