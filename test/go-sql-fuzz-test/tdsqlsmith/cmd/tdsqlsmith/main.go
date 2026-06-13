package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"tdsqlsmith/internal/catalog"
	"tdsqlsmith/internal/config"
	"tdsqlsmith/internal/crashguard"
	"tdsqlsmith/internal/executor"
	"tdsqlsmith/internal/replay"
	"tdsqlsmith/internal/report"
	"tdsqlsmith/internal/run"
	"tdsqlsmith/internal/serve"
	"tdsqlsmith/internal/taosdwatch"
)

var version = "dev"

const (
	envRunWorker            = "TDSQLSMITH_RUN_WORKER"
	envRunIDOverride        = "TDSQLSMITH_RUN_ID"
	envRunDirOverride       = "TDSQLSMITH_RUN_DIR"
	envRunResumeQueryNo     = "TDSQLSMITH_RUN_RESUME_QUERY_NO"
	envRunResumeRNGState    = "TDSQLSMITH_RUN_RESUME_RNG_STATE"
	envRunDeadlineUnixNanos = "TDSQLSMITH_RUN_DEADLINE_UNIX_NANOS"
	supervisorReport        = "coredump_report.json"
	supervisorMarkdown      = "crash_summary.md"
)

var (
	supervisorExecutorNewFn         = executor.New
	supervisorCatalogBootstrapFn    = catalog.Bootstrap
	supervisorTaosdShouldHandleFn   = taosdwatch.ShouldHandle
	supervisorTaosdHandleFn         = taosdwatch.Handle
	supervisorTaosdStopFn           = taosdwatch.StopManaged
	supervisorProbeSQLFn            = defaultSupervisorProbeSQL
	supervisorBootstrapRetryBackoff = time.Second
	supervisorWorkerRestartBackoff  = 500 * time.Millisecond
	supervisorWorkerDeadlineGrace   = 15 * time.Second
)

func main() {
	parsed, err := config.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, config.Usage())
		os.Exit(2)
	}
	if parsed.ShowHelp {
		fmt.Fprintln(os.Stderr, config.Usage())
		return
	}
	if parsed.ShowVersion {
		fmt.Printf("tdsqlsmith %s\n", version)
		return
	}

	ctx := context.Background()
	switch parsed.Command {
	case config.CommandRun:
		if !isRunWorker() {
			if err := runWithSupervisor(ctx, parsed); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			return
		}
		res, err := run.Execute(ctx, buildRunConfig(parsed))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("run_id=%s\nreport=%s\n",
			res.RunID,
			res.ReportPath,
		)
	case config.CommandReplay:
		out, err := replay.Run(ctx, replay.Config{
			DSN:         parsed.Replay.DSN,
			File:        parsed.Replay.File,
			Count:       parsed.Replay.Count,
			StmtTimeout: parsed.Replay.StmtTimeout,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
	case config.CommandServe:
		err := serve.Execute(ctx, serve.Config{
			Version:     version,
			Listen:      parsed.Serve.Listen,
			APIToken:    parsed.Serve.APIToken,
			DataDir:     parsed.Serve.DataDir,
			OutDir:      parsed.Serve.OutDir,
			AllowOrigin: parsed.Serve.AllowOrigin,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown command")
		os.Exit(2)
	}
}

func buildRunConfig(parsed *config.Parsed) run.Config {
	resumeQueryNo, _ := parseEnvInt64(envRunResumeQueryNo)
	deadline := parseEnvDeadline(envRunDeadlineUnixNanos)
	return run.Config{
		Version:         version,
		DSN:             parsed.Run.DSN,
		Seed:            parsed.Run.Seed,
		Cases:           parsed.Run.Cases,
		Duration:        parsed.Run.Duration,
		StmtTimeout:     parsed.Run.StmtTimeout,
		OutDir:          parsed.Run.OutDir,
		MutationLevel:   parsed.Run.MutationLevel,
		StopWhenCovered: parsed.Run.StopWhenCovered,
		DryRun:          parsed.Run.DryRun,
		Verbose:         parsed.Run.Verbose,
		RNGState:        parsed.Run.RNGState,
		DumpAllQueries:  parsed.Run.DumpAllQueries,
		DumpAllGraphs:   parsed.Run.DumpAllGraphs,
		ExcludeCatalog:  parsed.Run.ExcludeCatalog,
		LegacyMode:      parsed.Run.LegacyMode,
		WorkloadConfig:  parsed.Run.WorkloadConfig,
		ExecProfile:     parsed.Run.ExecProfile,
		ResumeQueryNo:   resumeQueryNo,
		ResumeRNGState:  strings.TrimSpace(os.Getenv(envRunResumeRNGState)),
		RunDeadline:     deadline,
		RunIDOverride:   strings.TrimSpace(os.Getenv(envRunIDOverride)),
		RunDirOverride:  strings.TrimSpace(os.Getenv(envRunDirOverride)),
		CrashGuard:      true,
		SkipBootstrap:   isRunWorker(),
	}
}

func isRunWorker() bool {
	return strings.TrimSpace(os.Getenv(envRunWorker)) == "1"
}

type workerResume struct {
	QueryNo  int64
	RNGState string
}

func parseEnvInt64(key string) (int64, bool) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func parseEnvDeadline(key string) time.Time {
	nanos, ok := parseEnvInt64(key)
	if !ok || nanos <= 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

func deriveWorkerResume(crash *report.ProcessCrashReport) (workerResume, error) {
	if crash == nil || crash.Snapshot == nil || crash.Snapshot.Pending == nil {
		return workerResume{}, fmt.Errorf("missing crash snapshot pending SQL")
	}
	p := crash.Snapshot.Pending
	rngState := strings.TrimSpace(p.RNGState)
	if rngState == "" {
		return workerResume{}, fmt.Errorf("missing pending rng_state in crash snapshot")
	}
	if p.QueryNo < 0 {
		return workerResume{}, fmt.Errorf("invalid pending query_no=%d", p.QueryNo)
	}
	return workerResume{
		QueryNo:  p.QueryNo,
		RNGState: rngState,
	}, nil
}

func runWithSupervisor(ctx context.Context, parsed *config.Parsed) error {
	defer func() {
		_ = supervisorTaosdStopFn(context.Background())
	}()

	start := time.Now()
	runID, runDir, err := resolveSupervisorRunIdentity(start, parsed.Run.OutDir, parsed.Run.Seed)
	if err != nil {
		return err
	}
	minimalReportPath := filepath.Join(runDir, "run_report.json")
	if err := ensureMinimalRunReport(minimalReportPath, runID); err != nil {
		return fmt.Errorf("init minimal run report: %w", err)
	}
	deadline := time.Time{}
	if parsed.Run.Duration > 0 {
		deadline = start.Add(parsed.Run.Duration)
	}
	if !parsed.Run.DryRun {
		if err := initializeSharedCatalogWithRetry(ctx, parsed.Run.DSN, parsed.Run.Seed, deadline); err != nil {
			return err
		}
	}
	restarts := 0
	crashIncidentSeen := false
	resume := workerResume{}
	for {
		if !deadline.IsZero() {
			if time.Now().After(deadline) {
				return nil
			}
		}
		workerCmdCtx, cancelWorkerCmd := workerCommandContext(ctx, deadline)
		cmd := exec.CommandContext(workerCmdCtx, os.Args[0], os.Args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		env := append(os.Environ(),
			envRunWorker+"=1",
			envRunIDOverride+"="+runID,
			envRunDirOverride+"="+runDir,
		)
		if !deadline.IsZero() {
			env = append(env, envRunDeadlineUnixNanos+"="+strconv.FormatInt(deadline.UnixNano(), 10))
		}
		if resume.QueryNo > 0 {
			env = append(env, envRunResumeQueryNo+"="+strconv.FormatInt(resume.QueryNo, 10))
		}
		if strings.TrimSpace(resume.RNGState) != "" {
			env = append(env, envRunResumeRNGState+"="+resume.RNGState)
		}
		cmd.Env = env

		err = cmd.Run()
		cancelWorkerCmd()
		if err != nil && !deadline.IsZero() && errors.Is(workerCmdCtx.Err(), context.DeadlineExceeded) && time.Now().After(deadline) {
			// Worker missed soft deadline; hard deadline forcibly stopped it.
			return nil
		}
		if err == nil {
			if parsed.Run.CleanupSuccessRunDir && !crashIncidentSeen {
				if cleanupErr := cleanupSuccessfulRunLogs(runDir); cleanupErr != nil {
					fmt.Fprintf(os.Stderr, "warning: cleanup successful run logs failed: %v\n", cleanupErr)
				}
			}
			return nil
		}
		exitMeta := classifyWorkerExit(err)
		if !exitMeta.Signaled {
			return err
		}
		crash, reportPath, reportErr := writeSupervisorCrashReport(runID, runDir, parsed.Run.Seed, err, exitMeta)
		if reportErr != nil {
			return fmt.Errorf("worker terminated by signal %s (core_dump=%t): %w", exitMeta.Signal, exitMeta.CoreDump, err)
		}
		isTaosdCrash := false
		if taosdInc, ok := detectSupervisorTaosdCrash(err, crash); ok {
			isTaosdCrash = true
			if appendErr := appendSupervisorTaosdIncident(minimalReportPath, runID, *crash, taosdInc); appendErr != nil {
				return fmt.Errorf("append supervisor taosd incident: %w", appendErr)
			}
		}
		// Only write to TDsqlsmithIncidents if it's not a taosd crash
		if !isTaosdCrash {
			if writeErr := writeSupervisorMinimalReport(minimalReportPath, runID, *crash); writeErr != nil {
				return fmt.Errorf("write supervisor minimal report: %w", writeErr)
			}
		}

		nextResume, resumeErr := deriveWorkerResume(crash)
		if resumeErr == nil {
			resume = nextResume
		}
		if !(exitMeta.CoreDump || isCrashSignalName(exitMeta.Signal)) {
			if !deadline.IsZero() {
				restarts++
				if resumeErr != nil {
					fmt.Fprintf(os.Stderr, "worker terminated by non-crash signal %s (core_dump=%t), crash report: %s; resume unavailable (%v), restarting worker in %s (restart_count=%d)\n",
						exitMeta.Signal, exitMeta.CoreDump, reportPath, resumeErr, supervisorWorkerRestartBackoff, restarts)
				} else {
					fmt.Fprintf(os.Stderr, "worker terminated by non-crash signal %s (core_dump=%t), crash report: %s; restarting worker from next sql after query_no=%d in %s (restart_count=%d)\n",
						exitMeta.Signal, exitMeta.CoreDump, reportPath, resume.QueryNo, supervisorWorkerRestartBackoff, restarts)
				}
				if waitErr := waitSupervisorWorkerRestart(ctx, deadline); waitErr != nil {
					return waitErr
				}
				continue
			}
			return fmt.Errorf("worker terminated by non-crash signal %s (core_dump=%t), crash report: %s; not restarting", exitMeta.Signal, exitMeta.CoreDump, reportPath)
		}
		crashIncidentSeen = true
		if resumeErr != nil {
			return fmt.Errorf("worker terminated by signal %s (core_dump=%t), crash report: %s, resume failed: %w", exitMeta.Signal, exitMeta.CoreDump, reportPath, resumeErr)
		}
		restarts++
		fmt.Fprintf(os.Stderr, "worker crashed by signal %s (core_dump=%t), restarting worker from next sql after query_no=%d (restart_count=%d)\n",
			exitMeta.Signal, exitMeta.CoreDump, resume.QueryNo, restarts)
		if waitErr := waitSupervisorWorkerRestart(ctx, deadline); waitErr != nil {
			return waitErr
		}
	}
}

func workerCommandContext(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if deadline.IsZero() {
		return parent, func() {}
	}
	hardDeadline := deadline.Add(supervisorWorkerDeadlineGrace)
	return context.WithDeadline(parent, hardDeadline)
}

func initializeSharedCatalogWithRetry(ctx context.Context, dsn string, seed int64, deadline time.Time) error {
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("supervisor bootstrap deadline reached: %w", lastErr)
			}
			return fmt.Errorf("supervisor bootstrap deadline reached")
		}

		execInst, err := supervisorExecutorNewFn(ctx, dsn)
		if err != nil {
			lastErr = err
			supervisorHandleTaosdIncident(err)
			if waitErr := waitSupervisorBootstrapRetry(ctx, deadline); waitErr != nil {
				if lastErr != nil {
					return fmt.Errorf("supervisor bootstrap retry stopped: %w", lastErr)
				}
				return waitErr
			}
			continue
		}

		probeErr := supervisorProbeSQLFn(ctx, execInst)
		if probeErr != nil {
			lastErr = probeErr
			_ = execInst.Close()
			supervisorHandleTaosdIncident(probeErr)
			if waitErr := waitSupervisorBootstrapRetry(ctx, deadline); waitErr != nil {
				return fmt.Errorf("supervisor bootstrap retry stopped: %w", lastErr)
			}
			continue
		}

		bootCtx, bootCancel := context.WithTimeout(ctx, 30*time.Second)
		_, cleanup, bootErr := supervisorCatalogBootstrapFn(bootCtx, execInst, seed, "tdsqlsmith")
		bootCancel()
		if cleanup != nil {
			cleanup(context.Background())
		}
		_ = execInst.Close()
		if bootErr == nil {
			return nil
		}

		lastErr = bootErr
		supervisorHandleTaosdIncident(bootErr)
		if waitErr := waitSupervisorBootstrapRetry(ctx, deadline); waitErr != nil {
			return fmt.Errorf("supervisor bootstrap retry stopped: %w", lastErr)
		}
	}
}

func supervisorHandleTaosdIncident(err error) {
	if !supervisorTaosdShouldHandleFn(string(executor.ClassConnLost), err) {
		return
	}
	_ = supervisorTaosdHandleFn(context.Background(), string(executor.ClassConnLost), "", err)
}

func defaultSupervisorProbeSQL(ctx context.Context, execInst *executor.Executor) error {
	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer probeCancel()
	probeOut := execInst.Exec(probeCtx, "select 1")
	if probeOut.Class == executor.ClassOK {
		return nil
	}
	if probeOut.Err != nil {
		return probeOut.Err
	}
	return fmt.Errorf("probe sql failed: class=%s", probeOut.Class)
}

func waitSupervisorBootstrapRetry(ctx context.Context, deadline time.Time) error {
	waitDur := supervisorBootstrapRetryBackoff
	if waitDur <= 0 {
		waitDur = 100 * time.Millisecond
	}
	if !deadline.IsZero() {
		remain := time.Until(deadline)
		if remain <= 0 {
			return fmt.Errorf("deadline reached")
		}
		if remain < waitDur {
			waitDur = remain
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(waitDur):
		return nil
	}
}

func waitSupervisorWorkerRestart(ctx context.Context, deadline time.Time) error {
	waitDur := supervisorWorkerRestartBackoff
	if waitDur <= 0 {
		return nil
	}
	if !deadline.IsZero() {
		remain := time.Until(deadline)
		if remain <= 0 {
			return nil
		}
		if remain < waitDur {
			waitDur = remain
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(waitDur):
		return nil
	}
}

func resolveSupervisorRunIdentity(start time.Time, outDir string, seed int64) (string, string, error) {
	runID := strings.TrimSpace(os.Getenv(envRunIDOverride))
	if runID == "" {
		runID = report.MakeRunID(start, seed)
	}
	runDir := strings.TrimSpace(os.Getenv(envRunDirOverride))
	if runDir == "" {
		runDir = filepath.Join(outDir, runID)
	}
	absRunDir, err := filepath.Abs(runDir)
	if err != nil {
		return "", "", fmt.Errorf("resolve supervisor run dir: %w", err)
	}
	if err := os.MkdirAll(absRunDir, 0o755); err != nil {
		return "", "", fmt.Errorf("create supervisor run dir: %w", err)
	}
	return runID, absRunDir, nil
}

func ensureMinimalRunReport(path, runID string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("empty run id")
	}
	minimal := loadMinimalForUpdate(path, runID)
	if minimal == nil {
		minimal = &report.MinimalRunReport{RunID: runID}
	}
	minimal.RunID = runID
	applyMinimalRuntimeFields(minimal, time.Now(), false)
	minimal.Normalize()
	if err := report.WriteJSON(path, minimal); err != nil {
		return fmt.Errorf("write minimal run report: %w", err)
	}
	return nil
}

func cleanupSuccessfulRunLogs(runDir string) error {
	paths := []string{
		filepath.Join(runDir, "crash_guard", "pending.json"),
		filepath.Join(runDir, "crash_guard", "window.json"),
		filepath.Join(runDir, "crash_guard", "status.json"),
		filepath.Join(runDir, "crash_guard", "report.latest.json"),
	}
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove temp log %s: %w", p, err)
		}
	}
	graphFiles, err := filepath.Glob(filepath.Join(runDir, "sqlsmith-*.graphml"))
	if err != nil {
		return fmt.Errorf("glob graphml logs: %w", err)
	}
	for _, p := range graphFiles {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove graph log %s: %w", p, err)
		}
	}
	_ = os.Remove(filepath.Join(runDir, "crash_guard"))
	return nil
}

type workerExit struct {
	Signaled bool
	Signal   string
	ExitCode int
	CoreDump bool
}

func classifyWorkerExit(err error) workerExit {
	meta := workerExit{}
	if err == nil {
		return meta
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return meta
	}
	meta.ExitCode = exitErr.ExitCode()
	waitStatus, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		return meta
	}
	if waitStatus.Signaled() {
		meta.Signaled = true
		meta.Signal = waitStatus.Signal().String()
		meta.CoreDump = waitStatus.CoreDump()
	}
	return meta
}

func isCrashSignalName(signal string) bool {
	s := strings.ToLower(strings.TrimSpace(signal))
	switch s {
	case "segmentation fault", "aborted", "bus error", "illegal instruction", "floating point exception", "trace/breakpoint trap":
		return true
	default:
		return false
	}
}

func writeSupervisorCrashReport(runID, runDir string, seed int64, workerErr error, exit workerExit) (*report.ProcessCrashReport, string, error) {
	crashDir := filepath.Join(runDir, "crash_guard")
	if err := os.MkdirAll(crashDir, 0o755); err != nil {
		return nil, "", fmt.Errorf("create crash guard dir: %w", err)
	}
	latestPath := filepath.Join(crashDir, "report.latest.json")
	snapshot, loadErr := crashguard.LoadLatest(latestPath)
	crash := &report.ProcessCrashReport{
		RunID:      runID,
		RunDir:     runDir,
		Seed:       seed,
		OccurredAt: time.Now(),
		Reason:     "worker terminated by signal",
		Signal:     exit.Signal,
		ExitCode:   exit.ExitCode,
		CoreDump:   exit.CoreDump,
		LatestPath: latestPath,
	}
	if workerErr != nil {
		crash.Error = workerErr.Error()
	}
	if loadErr == nil {
		crash.Snapshot = toReportSnapshot(snapshot)
	} else if crash.Error == "" {
		crash.Error = loadErr.Error()
	} else {
		crash.Error = crash.Error + "; load_latest=" + loadErr.Error()
	}

	reportPath := filepath.Join(crashDir, supervisorReport)
	if err := report.WriteJSON(reportPath, crash); err != nil {
		return nil, "", fmt.Errorf("write supervisor crash report: %w", err)
	}
	summaryPath := filepath.Join(crashDir, supervisorMarkdown)
	_ = os.WriteFile(summaryPath, []byte(renderSupervisorSummary(*crash)), 0o644)
	return crash, reportPath, nil
}

func writeSupervisorMinimalReport(path, runID string, crash report.ProcessCrashReport) error {
	crashSQL, executedTotal := crashSQLAndExecutedTotal(crash)

	incident := report.CrashIncident{
		OccurredAt: crash.OccurredAt,
		CrashSQL:   crashSQL,
	}
	minimal := loadMinimalForUpdate(path, runID)
	if minimal == nil {
		minimal = &report.MinimalRunReport{RunID: runID}
	}
	if len(minimal.SetupSQL) == 0 {
		minimal.SetupSQL = report.NormalizeSetupSQL(catalog.BootstrapSetupSQL("tdsqlsmith_shared"))
	}
	minimal.RunID = runID
	applyMinimalRuntimeFields(minimal, time.Now(), false)
	minimal.TotalExecuted += executedTotal
	minimal.TDsqlsmithIncidents = append(minimal.TDsqlsmithIncidents, incident)
	minimal.Normalize()
	if err := report.WriteJSON(path, minimal); err != nil {
		return fmt.Errorf("write minimal run report: %w", err)
	}
	return nil
}

func detectSupervisorTaosdCrash(workerErr error, crash *report.ProcessCrashReport) (taosdwatch.Incident, bool) {
	// First try to detect via workerErr
	if supervisorTaosdShouldHandleFn("", workerErr) {
		inc := supervisorTaosdHandleFn(context.Background(), "", "", workerErr)
		if shouldRecordSupervisorTaosdCrash(inc) {
			return inc, true
		}
	}
	// Do not infer taosd crash from worker signal + pending SQL alone.
	// Worker-side segfault/broken-pipe incidents must stay in tdsqlsmith_incidents
	// unless taosdwatch provides explicit taosd crash evidence.
	_ = crash
	return taosdwatch.Incident{}, false
}

func shouldRecordSupervisorTaosdCrash(inc taosdwatch.Incident) bool {
	if !inc.Checked {
		return false
	}
	if supervisorExitReasonHasCrashSignal(inc.ExitReason) {
		return true
	}
	if !inc.CoredumpDetected {
		return false
	}
	if !inc.ProcessExists {
		return true
	}
	return supervisorCoredumpEvidenceHasTaosd(inc.CoredumpEvidence)
}

func supervisorCoredumpEvidenceHasTaosd(evidence string) bool {
	low := strings.ToLower(strings.TrimSpace(evidence))
	if low == "" {
		return false
	}
	return strings.Contains(low, "taosd")
}

func supervisorExitReasonHasCrashSignal(reason string) bool {
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
		"segmentation fault",
		"aborted",
		"bus error",
		"illegal instruction",
		"floating point exception",
		"trace/breakpoint trap",
		"quit",
		"status=11",
		"status=6",
		"status=7",
		"status=4",
		"status=8",
		"signal=11",
		"signal 11",
		"signal=6",
		"signal 6",
		"signal=7",
		"signal 7",
		"signal=4",
		"signal 4",
		"signal=8",
		"signal 8",
	}
	for _, sig := range signals {
		if strings.Contains(low, sig) {
			return true
		}
	}
	return false
}

func appendSupervisorTaosdIncident(path, runID string, crash report.ProcessCrashReport, _ taosdwatch.Incident) error {
	crashSQL, _ := crashSQLAndExecutedTotal(crash)
	incident := report.CrashIncident{
		OccurredAt: crash.OccurredAt,
		CrashSQL:   crashSQL,
	}
	minimal := loadMinimalForUpdate(path, runID)
	if minimal == nil {
		minimal = &report.MinimalRunReport{RunID: runID}
	}
	if len(minimal.SetupSQL) == 0 {
		minimal.SetupSQL = report.NormalizeSetupSQL(catalog.BootstrapSetupSQL("tdsqlsmith_shared"))
	}
	minimal.RunID = runID
	applyMinimalRuntimeFields(minimal, time.Now(), false)
	minimal.TaosdIncidents = append(minimal.TaosdIncidents, incident)
	minimal.Normalize()
	if err := report.WriteJSON(path, minimal); err != nil {
		return fmt.Errorf("write minimal run report: %w", err)
	}
	return nil
}

func crashSQLAndExecutedTotal(crash report.ProcessCrashReport) (string, int64) {
	crashSQL := ""
	var executedTotal int64
	if crash.Snapshot != nil && crash.Snapshot.Pending != nil {
		crashSQL = strings.TrimSpace(crash.Snapshot.Pending.SQL)
	}
	if crash.Snapshot != nil && crash.Snapshot.ExecutedTotal > 0 {
		executedTotal = crash.Snapshot.ExecutedTotal
	}
	if crashSQL == "" && crash.Snapshot != nil && len(crash.Snapshot.Window) > 0 {
		last := crash.Snapshot.Window[len(crash.Snapshot.Window)-1]
		crashSQL = strings.TrimSpace(last.SQL)
	}
	if (crashSQL == "" || executedTotal == 0) && strings.TrimSpace(crash.LatestPath) != "" {
		if latest, err := crashguard.LoadLatest(crash.LatestPath); err == nil && latest != nil {
			if crashSQL == "" && latest.Pending != nil {
				crashSQL = strings.TrimSpace(latest.Pending.SQL)
			}
			if crashSQL == "" && len(latest.Window) > 0 {
				for i := len(latest.Window) - 1; i >= 0; i-- {
					if sqlText := strings.TrimSpace(latest.Window[i].SQL); sqlText != "" {
						crashSQL = sqlText
						break
					}
				}
			}
			if executedTotal == 0 && latest.ExecutedTotal > 0 {
				executedTotal = latest.ExecutedTotal
			}
		}
	}
	return crashSQL, executedTotal
}

func applyMinimalRuntimeFields(minimal *report.MinimalRunReport, now time.Time, completed bool) {
	if minimal == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	if minimal.StartedAt.IsZero() {
		minimal.StartedAt = now
	}
	if now.Before(minimal.StartedAt) {
		minimal.ExecutionDurationMS = 0
	} else {
		minimal.ExecutionDurationMS = now.Sub(minimal.StartedAt).Milliseconds()
	}
	if minimal.ExecutionDurationMS < 0 {
		minimal.ExecutionDurationMS = 0
	}
	minimal.GeneratedAt = now
	minimal.Completed = completed
}

func loadMinimalForUpdate(path, runID string) *report.MinimalRunReport {
	prev, err := report.ReadMinimalRunReport(path)
	if err != nil {
		return nil
	}
	if strings.TrimSpace(prev.RunID) != "" && strings.TrimSpace(prev.RunID) != strings.TrimSpace(runID) {
		return nil
	}
	return prev
}

func renderSupervisorSummary(crash report.ProcessCrashReport) string {
	b := &strings.Builder{}
	fmt.Fprintf(b, "# tdsqlsmith Worker Crash Summary\n\n")
	fmt.Fprintf(b, "- run_id: `%s`\n", crash.RunID)
	fmt.Fprintf(b, "- run_dir: `%s`\n", crash.RunDir)
	fmt.Fprintf(b, "- occurred_at: `%s`\n", crash.OccurredAt.Format(time.RFC3339Nano))
	fmt.Fprintf(b, "- signal: `%s`\n", crash.Signal)
	fmt.Fprintf(b, "- core_dump: `%t`\n", crash.CoreDump)
	if crash.ExitCode != 0 {
		fmt.Fprintf(b, "- exit_code: `%d`\n", crash.ExitCode)
	}
	if crash.LatestPath != "" {
		fmt.Fprintf(b, "- latest_snapshot: `%s`\n", crash.LatestPath)
	}
	if crash.Snapshot != nil && crash.Snapshot.Pending != nil {
		p := crash.Snapshot.Pending
		fmt.Fprintf(b, "\n## Pending SQL\n")
		fmt.Fprintf(b, "- query_no: `%d`\n", p.QueryNo)
		fmt.Fprintf(b, "- case: `%s`\n", p.CaseID)
		fmt.Fprintf(b, "- rule: `%s`\n", p.Rule)
		fmt.Fprintf(b, "- phase: `%s`\n\n", p.Phase)
		fmt.Fprintf(b, "```sql\n%s\n```\n", p.SQL)
	}
	if crash.Snapshot != nil && len(crash.Snapshot.Window) > 0 {
		fmt.Fprintf(b, "\n## Preceding Window\n")
		for _, w := range crash.Snapshot.Window {
			fmt.Fprintf(b, "- q%d `%s` `%s`\n", w.QueryNo, w.ExecClass, w.OccurredAt.Format(time.RFC3339))
		}
	}
	if crash.Error != "" {
		fmt.Fprintf(b, "\n## Error\n%s\n", crash.Error)
	}
	return b.String()
}

func toReportSnapshot(in *crashguard.Snapshot) *report.CrashSnapshotReport {
	if in == nil {
		return nil
	}
	out := &report.CrashSnapshotReport{
		RunID:         in.RunID,
		RunDir:        in.RunDir,
		UpdatedAt:     in.UpdatedAt,
		WorkerPID:     in.WorkerPID,
		ExecutedTotal: in.ExecutedTotal,
		CleanExit:     in.CleanExit,
	}
	if in.Pending != nil {
		out.Pending = &report.CrashPendingStatement{
			OccurredAt: in.Pending.OccurredAt,
			RunID:      in.Pending.RunID,
			QueryNo:    in.Pending.QueryNo,
			CaseID:     in.Pending.CaseID,
			Rule:       in.Pending.Rule,
			Phase:      in.Pending.Phase,
			RNGState:   in.Pending.RNGState,
			SQL:        in.Pending.SQL,
		}
	}
	if len(in.Window) > 0 {
		out.Window = make([]report.ExecutedStmtRef, 0, len(in.Window))
		for _, w := range in.Window {
			out.Window = append(out.Window, report.ExecutedStmtRef{
				QueryNo:    w.QueryNo,
				OccurredAt: w.OccurredAt,
				CaseID:     w.CaseID,
				Rule:       w.Rule,
				ExecClass:  w.ExecClass,
				SQL:        w.SQL,
				Error:      w.Error,
				DurationMS: w.DurationMS,
			})
		}
	}
	return out
}
