package main

// main.go is the CLI entry point for tdsqlsmith. It parses command-line
// arguments and dispatches to the run/replay/serve commands. For the run
// command it acts as a supervisor that spawns the fuzz worker as a child
// process, restarts it after crashes, and records crash/incident reports.
//
// main.go 是 tdsqlsmith 的 CLI 入口。它解析命令行参数并分发到 run/replay/serve
// 命令。对于 run 命令,它充当 supervisor:将 fuzz worker 作为子进程启动,在崩溃
// 后重启它,并记录崩溃/事件报告。

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

// version is the build version string, overridden at link time via -ldflags.
// version 是构建版本字符串,在链接时通过 -ldflags 覆盖。
var version = "dev"

const (
	envRunWorker            = "TDSQLSMITH_RUN_WORKER"              // set to "1" in the spawned worker process / 在被启动的 worker 进程中设为 "1"
	envRunIDOverride        = "TDSQLSMITH_RUN_ID"                  // forces a specific run ID in the worker / 强制 worker 使用指定的 run ID
	envRunDirOverride       = "TDSQLSMITH_RUN_DIR"                 // forces a specific run output directory / 强制使用指定的运行输出目录
	envRunResumeQueryNo     = "TDSQLSMITH_RUN_RESUME_QUERY_NO"     // query number to resume after on restart / 重启时从该查询号之后恢复
	envRunResumeRNGState    = "TDSQLSMITH_RUN_RESUME_RNG_STATE"    // serialized RNG state to resume from / 用于恢复的序列化 RNG 状态
	envRunDeadlineUnixNanos = "TDSQLSMITH_RUN_DEADLINE_UNIX_NANOS" // absolute run deadline in unix nanoseconds / 以 unix 纳秒表示的绝对运行截止时间
	supervisorReport        = "coredump_report.json"               // filename for the supervisor crash report / supervisor 崩溃报告的文件名
	supervisorMarkdown      = "crash_summary.md"                   // filename for the human-readable crash summary / 人类可读崩溃摘要的文件名
)

// Package-level function variables, overridable in tests to stub out external
// dependencies (executor, catalog bootstrap, taosd watcher) and tune backoffs.
//
// 包级函数变量,可在测试中覆盖以打桩外部依赖(executor、catalog 引导、taosd
// 监视器)并调整退避时间。
var (
	supervisorExecutorNewFn         = executor.New              // creates a new executor connection / 创建新的 executor 连接
	supervisorCatalogBootstrapFn    = catalog.Bootstrap         // bootstraps the shared catalog schema / 引导共享 catalog 模式
	supervisorTaosdShouldHandleFn   = taosdwatch.ShouldHandle   // decides whether an error is a taosd incident / 判断某个错误是否为 taosd 事件
	supervisorTaosdHandleFn         = taosdwatch.Handle         // handles a detected taosd incident / 处理检测到的 taosd 事件
	supervisorTaosdStopFn           = taosdwatch.StopManaged    // stops the managed taosd child process / 停止受管理的 taosd 子进程
	supervisorProbeSQLFn            = defaultSupervisorProbeSQL // probes the connection with a trivial query / 用一个简单查询探测连接
	supervisorBootstrapRetryBackoff = time.Second               // delay between catalog bootstrap retries / catalog 引导重试之间的延迟
	supervisorWorkerRestartBackoff  = 500 * time.Millisecond    // delay between worker restarts / worker 重启之间的延迟
	supervisorWorkerDeadlineGrace   = 15 * time.Second          // grace period added to the worker hard deadline / 加到 worker 硬截止时间上的宽限期
)

// main parses CLI arguments and dispatches to the selected command.
// main 解析 CLI 参数并分发到所选命令。
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

// buildRunConfig assembles a run.Config from parsed CLI flags and the
// worker resume/deadline environment variables.
//
// buildRunConfig 根据解析后的 CLI 标志以及 worker 恢复/截止时间环境变量组装出
// 一个 run.Config。
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

// isRunWorker reports whether this process is the spawned fuzz worker
// rather than the supervisor.
//
// isRunWorker 报告当前进程是被启动的 fuzz worker 还是 supervisor。
func isRunWorker() bool {
	return strings.TrimSpace(os.Getenv(envRunWorker)) == "1"
}

// workerResume carries the position from which a restarted worker should
// resume after a crash.
//
// workerResume 携带重启后的 worker 在崩溃后应从何处恢复的位置信息。
type workerResume struct {
	QueryNo  int64  // last completed query number; resume after this one / 最后完成的查询号;从其后恢复
	RNGState string // serialized RNG state to restore for deterministic resume / 用于确定性恢复的序列化 RNG 状态
}

// parseEnvInt64 reads an int64 from the named environment variable, returning
// ok=false if it is empty or not a valid integer.
//
// parseEnvInt64 从指定的环境变量读取一个 int64,如果为空或不是有效整数则返回
// ok=false。
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

// parseEnvDeadline reads an absolute deadline encoded as unix nanoseconds from
// the named environment variable, returning the zero time if unset or invalid.
//
// parseEnvDeadline 从指定的环境变量读取以 unix 纳秒编码的绝对截止时间,若未设置
// 或无效则返回零值时间。
func parseEnvDeadline(key string) time.Time {
	nanos, ok := parseEnvInt64(key)
	if !ok || nanos <= 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

// deriveWorkerResume extracts the resume position (query number and RNG state)
// from a crash report's pending-statement snapshot, erroring if it is missing
// or invalid.
//
// deriveWorkerResume 从崩溃报告的待执行语句快照中提取恢复位置(查询号和 RNG
// 状态),若缺失或无效则返回错误。
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

// runWithSupervisor runs the fuzz loop under supervision: it bootstraps the
// shared catalog, repeatedly spawns the worker as a child process, and after
// each abnormal (signal) exit writes crash reports and decides whether to
// restart the worker (resuming from the crashing SQL) until the deadline or
// case count is reached.
//
// runWithSupervisor 在受监督的模式下运行 fuzz 循环:它引导共享 catalog,反复将
// worker 作为子进程启动,并在每次异常(信号)退出后写入崩溃报告,决定是否重启
// worker(从崩溃的 SQL 处恢复),直到到达截止时间或用例数上限。
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
			// worker 错过了软截止时间;硬截止时间将其强制停止。
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
		// 仅当不是 taosd 崩溃时才写入 TDsqlsmithIncidents
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

// workerCommandContext derives a context for a single worker invocation. When a
// soft deadline is set it returns a context with a hard deadline (deadline plus
// grace) so a stuck worker is forcibly killed shortly after the soft deadline.
//
// workerCommandContext 为单次 worker 调用派生一个 context。当设置了软截止时间
// 时,它返回带硬截止时间(软截止时间加宽限期)的 context,使卡住的 worker 在软
// 截止时间后不久被强制杀死。
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

// initializeSharedCatalogWithRetry connects, probes, and bootstraps the shared
// catalog once before workers start, retrying with backoff (and handling taosd
// incidents) until it succeeds, the context is cancelled, or the deadline hits.
//
// initializeSharedCatalogWithRetry 在 worker 启动前先连接、探测并引导一次共享
// catalog,带退避地重试(并处理 taosd 事件),直到成功、context 被取消或到达
// 截止时间。
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

// supervisorHandleTaosdIncident routes a connection-loss error to the taosd
// watcher so it can detect and handle a crashed or stopped taosd process.
//
// supervisorHandleTaosdIncident 将连接丢失错误转交给 taosd 监视器,使其能够检测
// 并处理崩溃或已停止的 taosd 进程。
func supervisorHandleTaosdIncident(err error) {
	if !supervisorTaosdShouldHandleFn(string(executor.ClassConnLost), err) {
		return
	}
	_ = supervisorTaosdHandleFn(context.Background(), string(executor.ClassConnLost), "", err)
}

// defaultSupervisorProbeSQL runs "select 1" against the executor to confirm the
// connection is healthy before bootstrapping the catalog.
//
// defaultSupervisorProbeSQL 对 executor 执行 "select 1",在引导 catalog 之前确认
// 连接健康。
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

// waitSupervisorBootstrapRetry sleeps for the bootstrap retry backoff, capped by
// the remaining time to the deadline and interrupted by context cancellation.
//
// waitSupervisorBootstrapRetry 休眠一个引导重试退避时长,以距截止时间的剩余时间
// 为上限,并可被 context 取消中断。
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

// waitSupervisorWorkerRestart sleeps for the worker restart backoff, capped by
// the remaining time to the deadline and interrupted by context cancellation.
//
// waitSupervisorWorkerRestart 休眠一个 worker 重启退避时长,以距截止时间的剩余
// 时间为上限,并可被 context 取消中断。
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

// resolveSupervisorRunIdentity determines the run ID and absolute run directory
// (honoring environment overrides) and creates the directory.
//
// resolveSupervisorRunIdentity 确定 run ID 和绝对运行目录(遵从环境变量覆盖),
// 并创建该目录。
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

// ensureMinimalRunReport writes (or refreshes) a minimal run report at path so a
// report exists even if the run produces no incidents.
//
// ensureMinimalRunReport 在 path 处写入(或刷新)一个最小运行报告,使得即便运行
// 没有产生任何事件也存在一份报告。
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

// cleanupSuccessfulRunLogs removes transient crash-guard files and per-query
// graphml dumps from a run directory after a clean run with no crash incidents.
//
// cleanupSuccessfulRunLogs 在一次没有崩溃事件的干净运行后,从运行目录中删除临时
// crash-guard 文件和逐查询的 graphml dump。
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

// workerExit summarizes how a worker child process terminated.
// workerExit 概括 worker 子进程是如何终止的。
type workerExit struct {
	Signaled bool   // true if the process was killed by a signal / 进程是否被信号杀死
	Signal   string // signal name (when Signaled) / 信号名称(当被信号杀死时)
	ExitCode int    // process exit code / 进程退出码
	CoreDump bool   // true if the kernel produced a core dump / 内核是否产生了 core dump
}

// classifyWorkerExit inspects a *exec.ExitError to determine whether the worker
// was killed by a signal, which signal, its exit code, and whether it dumped core.
//
// classifyWorkerExit 检查 *exec.ExitError,以判断 worker 是否被信号杀死、是哪个
// 信号、退出码是多少,以及是否产生了 core dump。
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

// isCrashSignalName reports whether the signal name denotes a genuine crash
// (segfault, abort, bus error, etc.) as opposed to a benign termination signal.
//
// isCrashSignalName 报告该信号名称是否表示真正的崩溃(段错误、abort、总线错误
// 等),而非良性的终止信号。
func isCrashSignalName(signal string) bool {
	s := strings.ToLower(strings.TrimSpace(signal))
	switch s {
	case "segmentation fault", "aborted", "bus error", "illegal instruction", "floating point exception", "trace/breakpoint trap":
		return true
	default:
		return false
	}
}

// writeSupervisorCrashReport builds a ProcessCrashReport from the worker exit
// metadata and the latest crash-guard snapshot, writes it as JSON plus a
// markdown summary into the run's crash_guard directory, and returns it.
//
// writeSupervisorCrashReport 根据 worker 退出元数据和最新的 crash-guard 快照构建
// 一个 ProcessCrashReport,将其以 JSON 加 markdown 摘要的形式写入运行的
// crash_guard 目录,并返回它。
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

// writeSupervisorMinimalReport appends the crash as a tdsqlsmith incident to the
// minimal run report at path, updating runtime fields and executed totals.
//
// writeSupervisorMinimalReport 将该崩溃作为一个 tdsqlsmith 事件追加到 path 处的
// 最小运行报告中,并更新运行时字段和已执行总数。
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

// detectSupervisorTaosdCrash determines whether the worker failure is actually a
// taosd crash, using only explicit taosd-watcher evidence from the worker error.
// It deliberately does not infer a taosd crash from the worker signal alone.
//
// detectSupervisorTaosdCrash 判断 worker 失败是否实际上是一次 taosd 崩溃,仅依据
// 来自 worker 错误的明确 taosd-watcher 证据。它有意不会仅凭 worker 信号就推断出
// taosd 崩溃。
func detectSupervisorTaosdCrash(workerErr error, crash *report.ProcessCrashReport) (taosdwatch.Incident, bool) {
	// First try to detect via workerErr
	// 首先尝试通过 workerErr 进行检测
	if supervisorTaosdShouldHandleFn("", workerErr) {
		inc := supervisorTaosdHandleFn(context.Background(), "", "", workerErr)
		if shouldRecordSupervisorTaosdCrash(inc) {
			return inc, true
		}
	}
	// Do not infer taosd crash from worker signal + pending SQL alone.
	// Worker-side segfault/broken-pipe incidents must stay in tdsqlsmith_incidents
	// unless taosdwatch provides explicit taosd crash evidence.
	// 不要仅凭 worker 信号 + 待执行 SQL 推断 taosd 崩溃。除非 taosdwatch 提供了明确
	// 的 taosd 崩溃证据,否则 worker 侧的段错误/broken-pipe 事件必须保留在
	// tdsqlsmith_incidents 中。
	_ = crash
	return taosdwatch.Incident{}, false
}

// shouldRecordSupervisorTaosdCrash decides whether a taosd incident has enough
// crash evidence (crash-signal exit reason, or a coredump with a dead process or
// taosd-attributable evidence) to be recorded as a taosd crash.
//
// shouldRecordSupervisorTaosdCrash 判断一个 taosd 事件是否具备足够的崩溃证据
// (崩溃信号退出原因,或伴随进程已死亡或可归因于 taosd 的证据的 coredump),
// 从而被记录为一次 taosd 崩溃。
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

// supervisorCoredumpEvidenceHasTaosd reports whether the coredump evidence text
// mentions taosd, attributing the core dump to the taosd process.
//
// supervisorCoredumpEvidenceHasTaosd 报告 coredump 证据文本是否提及 taosd,从而将
// core dump 归因于 taosd 进程。
func supervisorCoredumpEvidenceHasTaosd(evidence string) bool {
	low := strings.ToLower(strings.TrimSpace(evidence))
	if low == "" {
		return false
	}
	return strings.Contains(low, "taosd")
}

// supervisorExitReasonHasCrashSignal reports whether the exit-reason text
// contains any known crash-signal marker (SIGSEGV, SIGABRT, status codes, etc.).
//
// supervisorExitReasonHasCrashSignal 报告退出原因文本是否包含任何已知的崩溃信号
// 标记(SIGSEGV、SIGABRT、状态码等)。
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

// appendSupervisorTaosdIncident appends the crash as a taosd incident to the
// minimal run report at path, seeding setup SQL and updating runtime fields.
//
// appendSupervisorTaosdIncident 将该崩溃作为一个 taosd 事件追加到 path 处的最小
// 运行报告中,填充 setup SQL 并更新运行时字段。
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

// crashSQLAndExecutedTotal extracts the crashing SQL statement and the total
// executed count from a crash report's snapshot, falling back to the pending
// statement, the last window entry, and finally the latest crash-guard file.
//
// crashSQLAndExecutedTotal 从崩溃报告的快照中提取崩溃的 SQL 语句和已执行总数,
// 依次回退到待执行语句、最后一条窗口记录,最后回退到最新的 crash-guard 文件。
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

// applyMinimalRuntimeFields sets the report's started/generated timestamps,
// recomputes execution duration, and records the completed flag.
//
// applyMinimalRuntimeFields 设置报告的开始/生成时间戳,重新计算执行时长,并记录
// completed 标志。
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

// loadMinimalForUpdate reads an existing minimal run report for in-place update,
// returning nil if it cannot be read or belongs to a different run ID.
//
// loadMinimalForUpdate 读取已有的最小运行报告以供原地更新,如果无法读取或属于
// 不同的 run ID 则返回 nil。
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

// renderSupervisorSummary renders a markdown crash summary from a crash report,
// including the pending SQL, preceding window, and any error text.
//
// renderSupervisorSummary 根据崩溃报告渲染一份 markdown 崩溃摘要,包含待执行
// SQL、之前的窗口以及任何错误文本。
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

// toReportSnapshot converts a crashguard.Snapshot into the report package's
// CrashSnapshotReport representation, copying the pending statement and window.
//
// toReportSnapshot 将 crashguard.Snapshot 转换为 report 包的 CrashSnapshotReport
// 表示,复制待执行语句和窗口。
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
