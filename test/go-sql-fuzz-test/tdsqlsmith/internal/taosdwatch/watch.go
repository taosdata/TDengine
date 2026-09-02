// Package taosdwatch supervises the taosd server process, detecting crashes,
// recording incident details, and restarting the managed process to keep it alive.
//
// taosdwatch 包监督 taosd 服务进程，检测崩溃、记录事件细节，
// 并重启所管理的进程以保持其存活。
package taosdwatch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	managedTaosdCommandEnv = "TDSQLSMITH_TAOSD_COMMAND"          // env var overriding the taosd launch command / 覆盖 taosd 启动命令的环境变量
	managedTaosdLogEnv     = "TDSQLSMITH_TAOSD_LOG"              // env var overriding the managed taosd log path / 覆盖受管 taosd 日志路径的环境变量
	defaultManagedTaosdLog = "/tmp/tdsqlsmith-taosd-restart.log" // default log path for managed taosd output / 受管 taosd 输出的默认日志路径
)

// Incident describes a detected taosd disruption and the response taken.
//
// Incident 描述检测到的 taosd 中断及所采取的响应。
type Incident struct {
	OccurredAt        time.Time `json:"occurred_at"`                 // when the incident was observed / 观察到事件的时间
	ExecClass         string    `json:"exec_class"`                  // execution class that triggered detection / 触发检测的执行类别
	SQL               string    `json:"sql"`                         // SQL in flight at the time / 当时正在执行的 SQL
	Error             string    `json:"error"`                       // triggering error message / 触发的错误信息
	Checked           bool      `json:"checked"`                     // whether crash handling was performed / 是否执行了崩溃处理
	ProcessExists     bool      `json:"process_exists"`              // whether the taosd process still existed / taosd 进程是否仍然存在
	ProcessCheck      string    `json:"process_check,omitempty"`     // details of the process liveness check / 进程存活检查的细节
	ExitReason        string    `json:"exit_reason,omitempty"`       // formatted reason the process exited / 格式化的进程退出原因
	CoredumpDetected  bool      `json:"coredump_detected"`           // whether a core dump was detected / 是否检测到 core dump
	CoredumpEvidence  string    `json:"coredump_evidence,omitempty"` // evidence describing the core dump / 描述 core dump 的证据
	RestartAttempted  bool      `json:"restart_attempted"`           // whether a restart was attempted / 是否尝试了重启
	RestartCommand    string    `json:"restart_command,omitempty"`   // command used to restart taosd / 用于重启 taosd 的命令
	RestartSucceeded  bool      `json:"restart_succeeded"`           // whether the restart succeeded / 重启是否成功
	RestartOutput     string    `json:"restart_output,omitempty"`    // captured restart output / 捕获的重启输出
	RestartError      string    `json:"restart_error,omitempty"`     // restart error message, if any / 重启错误信息（如有）
	ReconnectRequired bool      `json:"reconnect_required"`          // whether the client must reconnect / 客户端是否必须重连
}

var (
	// defaultSupervisor is the process-wide taosd supervisor instance.
	//
	// defaultSupervisor 是进程级的 taosd 监督者实例。
	defaultSupervisor = newTaosdSupervisor()

	// ensureManagedTaosd ensures the managed taosd is running; overridable in tests.
	//
	// ensureManagedTaosd 确保受管 taosd 正在运行；可在测试中覆盖。
	ensureManagedTaosd = func(ctx context.Context) (string, string, error) {
		return defaultSupervisor.EnsureRunning(ctx)
	}
	// lastManagedExit reports the most recent managed exit since a time; overridable in tests.
	//
	// lastManagedExit 报告自某一时刻以来最近一次受管退出；可在测试中覆盖。
	lastManagedExit = func(since time.Time) (managedExitMeta, bool) {
		return defaultSupervisor.LastExitSince(since)
	}
)

// StopManaged stops the managed taosd process via the default supervisor.
//
// StopManaged 通过默认监督者停止受管的 taosd 进程。
func StopManaged(ctx context.Context) error {
	_, err := defaultSupervisor.StopManaged(ctx)
	return err
}

// LastManagedExitSince returns the time of the most recent managed taosd exit at
// or after since, and whether such an exit was recorded.
//
// LastManagedExitSince 返回在 since 当时或之后最近一次受管 taosd 退出的时间，
// 以及是否记录到这样的退出。
func LastManagedExitSince(since time.Time) (time.Time, bool) {
	meta, ok := lastManagedExit(since)
	if !ok || meta.OccurredAt.IsZero() {
		return time.Time{}, false
	}
	return meta.OccurredAt, true
}

// ShouldHandle reports whether an execution outcome signals a possible taosd
// disruption worth handling, based on the exec class or known error substrings.
//
// ShouldHandle 根据执行类别或已知错误子串，报告某个执行结果是否预示着
// 值得处理的潜在 taosd 中断。
func ShouldHandle(execClass string, execErr error) bool {
	class := strings.ToLower(strings.TrimSpace(execClass))
	if class == "conn_lost" {
		return true
	}
	msg := strings.ToLower(errString(execErr))
	if msg == "" {
		return false
	}
	// TDengine has occasionally surfaced internal crashes as opaque "Unknown error 65535".
	// Treat it as a taosd incident trigger so we can capture crash SQL in reports.
	// TDengine 偶尔会将内部崩溃表现为不透明的 "Unknown error 65535"。
	// 将其视为 taosd 事件触发条件，以便在报告中捕获崩溃 SQL。
	if strings.Contains(msg, "unknown error 65535") {
		return true
	}
	if strings.Contains(msg, "unable to establish") {
		return true
	}
	if strings.Contains(msg, "connection refused") {
		return true
	}
	if strings.Contains(msg, "broken pipe") {
		return true
	}
	if strings.Contains(msg, "closed network") {
		return true
	}
	if strings.Contains(msg, "server closed") {
		return true
	}
	return false
}

// Handle inspects a triggering execution outcome, determines the exit reason and
// any core dump, attempts to restart taosd, and returns the resulting Incident.
// If the outcome does not warrant handling it returns an unchecked Incident.
//
// Handle 检查触发的执行结果，确定退出原因和任何 core dump，尝试重启 taosd，
// 并返回得到的 Incident。如果该结果不值得处理，则返回未检查的 Incident。
func Handle(ctx context.Context, execClass, sqlText string, execErr error) Incident {
	inc := Incident{
		OccurredAt:        time.Now(),
		ExecClass:         strings.TrimSpace(execClass),
		SQL:               strings.TrimSpace(sqlText),
		Error:             errString(execErr),
		ReconnectRequired: true,
	}
	if !ShouldHandle(execClass, execErr) {
		return inc
	}
	inc.Checked = true

	exitReason, dumped, dumpEvidence := inspectExitReason(inc.OccurredAt)
	inc.ExitReason = exitReason
	inc.CoredumpDetected = dumped
	inc.CoredumpEvidence = dumpEvidence

	restart := restartTaosd(ctx)
	inc.RestartAttempted = restart.Attempted
	inc.RestartCommand = restart.Command
	inc.RestartOutput = restart.Output
	inc.RestartSucceeded = restart.Succeeded
	inc.RestartError = restart.Error
	return inc
}

// restartResult captures the outcome of a taosd restart attempt.
//
// restartResult 捕获一次 taosd 重启尝试的结果。
type restartResult struct {
	Attempted bool   // whether a restart was attempted / 是否尝试了重启
	Command   string // command used to start taosd / 用于启动 taosd 的命令
	Output    string // captured (truncated) restart output / 捕获的（截断后的）重启输出
	Succeeded bool   // whether the process is considered running afterward / 之后是否认为进程正在运行
	Error     string // error message, if the attempt failed / 错误信息（若尝试失败）
}

// managedExitMeta records how a managed taosd process terminated.
//
// managedExitMeta 记录受管 taosd 进程是如何终止的。
type managedExitMeta struct {
	OccurredAt time.Time // when the process exited / 进程退出的时间
	ExitCode   int       // exit code, when not signaled / 退出码（非信号终止时）
	Signaled   bool      // whether the process was killed by a signal / 进程是否被信号杀死
	Signal     string    // signal name, when signaled / 信号名（信号终止时）
	CoreDump   bool      // whether a core dump was produced / 是否产生了 core dump
	Message    string    // associated error message, if any / 关联的错误信息（如有）
}

// taosdSupervisor owns the lifecycle of a managed taosd child process.
//
// taosdSupervisor 拥有受管 taosd 子进程的生命周期。
type taosdSupervisor struct {
	mu             sync.Mutex      // guards all fields below / 保护下方所有字段
	pid            int             // PID of the running managed process, 0 if none / 正在运行的受管进程 PID，无则为 0
	pidGeneration  uint64          // generation that owns the current pid / 拥有当前 pid 的代次
	generation     uint64          // incremented each time a process is started / 每次启动进程时递增
	stopGeneration uint64          // generations at or below this must not be restarted / 不应重启此代次及以下的进程
	managed        bool            // whether a managed process is currently owned / 当前是否拥有一个受管进程
	starting       bool            // whether a start is in progress / 是否有一次启动正在进行
	lastExit       managedExitMeta // metadata of the most recent exit / 最近一次退出的元数据
}

// newTaosdSupervisor returns an empty taosd supervisor.
//
// newTaosdSupervisor 返回一个空的 taosd 监督者。
func newTaosdSupervisor() *taosdSupervisor {
	return &taosdSupervisor{}
}

// EnsureRunning starts taosd as a child process if not already running.
// This is the exported entry point for parent-child mode.
//
// EnsureRunning 在 taosd 尚未运行时将其作为子进程启动。
// 这是父子进程模式的导出入口点。
func EnsureRunning(ctx context.Context) (string, string, error) {
	return defaultSupervisor.EnsureRunning(ctx)
}

// EnsureRunning starts the managed taosd if it is not already alive, returning
// the launch command string and a status message. It coordinates concurrent
// callers so only one start proceeds at a time and spawns a watcher goroutine.
//
// EnsureRunning 在受管 taosd 尚未存活时将其启动，返回启动命令字符串和一条状态消息。
// 它协调并发调用方，使同一时间只有一次启动进行，并派生一个监视 goroutine。
func (s *taosdSupervisor) EnsureRunning(ctx context.Context) (string, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		s.mu.Lock()
		if pidAlive(s.pid) {
			cmdStr := strings.Join(s.resolveLaunchCommandLocked(), " ")
			pid := s.pid
			s.mu.Unlock()
			return cmdStr, fmt.Sprintf("already running pid=%d", pid), nil
		}
		if s.starting {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return "", "", ctx.Err()
			case <-time.After(120 * time.Millisecond):
			}
			continue
		}

		s.starting = true
		launch := s.resolveLaunchCommandLocked()
		s.mu.Unlock()

		cmdStr := strings.Join(launch, " ")
		if err := ctx.Err(); err != nil {
			s.mu.Lock()
			s.starting = false
			s.mu.Unlock()
			return "", "", err
		}
		cmd, logFile, err := startManagedTaosdProcess(launch, managedTaosdLogPath())
		if err != nil {
			s.mu.Lock()
			s.starting = false
			s.mu.Unlock()
			return cmdStr, "", err
		}

		pid := cmd.Process.Pid
		s.mu.Lock()
		s.starting = false
		s.generation++
		gen := s.generation
		s.pid = pid
		s.pidGeneration = gen
		s.managed = true
		s.mu.Unlock()

		go s.watchProcess(cmd, logFile, gen)
		return cmdStr, fmt.Sprintf("started pid=%d", pid), nil
	}
}

// LastExitSince returns the recorded exit metadata if the last managed exit
// occurred at or after since, and whether such an exit exists.
//
// LastExitSince 在最近一次受管退出发生于 since 当时或之后时返回记录的退出元数据，
// 以及是否存在这样的退出。
func (s *taosdSupervisor) LastExitSince(since time.Time) (managedExitMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastExit.OccurredAt.IsZero() {
		return managedExitMeta{}, false
	}
	if !since.IsZero() && s.lastExit.OccurredAt.Before(since) {
		return managedExitMeta{}, false
	}
	return s.lastExit, true
}

// StopManaged signals the managed taosd to terminate (SIGTERM then SIGKILL),
// raises the stop generation so the watcher will not restart it, and reports
// whether a managed process was found.
//
// StopManaged 向受管 taosd 发出终止信号（先 SIGTERM 后 SIGKILL），
// 提升 stop 代次使监视器不再重启它，并报告是否找到了受管进程。
func (s *taosdSupervisor) StopManaged(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if s.generation > s.stopGeneration {
		s.stopGeneration = s.generation
	}
	pid := s.pid
	managed := s.managed
	s.mu.Unlock()

	if !managed || pid <= 0 {
		return false, nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		s.markStopped(pid)
		return true, nil
	}
	if sigErr := proc.Signal(syscall.SIGTERM); sigErr != nil && !errors.Is(sigErr, syscall.ESRCH) {
		return true, fmt.Errorf("stop managed taosd pid=%d: %w", pid, sigErr)
	}
	if waitPIDExit(ctx, pid, 5*time.Second) {
		s.markStopped(pid)
		return true, nil
	}
	if sigErr := proc.Signal(syscall.SIGKILL); sigErr != nil && !errors.Is(sigErr, syscall.ESRCH) {
		return true, fmt.Errorf("kill managed taosd pid=%d: %w", pid, sigErr)
	}
	if waitPIDExit(ctx, pid, 3*time.Second) {
		s.markStopped(pid)
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	return true, fmt.Errorf("managed taosd pid=%d did not exit after stop", pid)
}

// markStopped clears the supervisor's ownership state if pid is still the
// tracked process.
//
// markStopped 在 pid 仍是所跟踪进程时清除监督者的所有权状态。
func (s *taosdSupervisor) markStopped(pid int) {
	if pid <= 0 {
		return
	}
	s.mu.Lock()
	if s.pid == pid {
		s.pid = 0
		s.pidGeneration = 0
		s.managed = false
	}
	s.mu.Unlock()
}

// watchProcess waits for the managed taosd to exit, records its exit metadata,
// and unless the run was stopped keeps restarting it (clearing the stale lock
// file) so taosd stays alive.
//
// watchProcess 等待受管 taosd 退出，记录其退出元数据，
// 并且除非运行已被停止，否则持续重启它（清除过期的锁文件），使 taosd 保持存活。
func (s *taosdSupervisor) watchProcess(cmd *exec.Cmd, logFile *os.File, gen uint64) {
	err := cmd.Wait()
	if logFile != nil {
		_ = logFile.Close()
	}
	meta := classifyManagedExit(err, cmd.ProcessState)
	s.mu.Lock()
	if s.pid == cmd.Process.Pid {
		s.pid = 0
		s.pidGeneration = 0
		s.managed = false
	}
	s.lastExit = meta
	stopGen := s.stopGeneration
	s.mu.Unlock()

	if gen <= stopGen {
		return
	}

	// Keep taosd alive: once managed process exits, keep trying until it is back.
	// Clean up stale .running lock file before restart attempts.
	// 保持 taosd 存活：一旦受管进程退出，持续尝试直到其恢复。
	// 在重启尝试前清理过期的 .running 锁文件。
	_ = os.Remove("/var/lib/taos/.running")
	for {
		s.mu.Lock()
		stopGen = s.stopGeneration
		s.mu.Unlock()
		if gen <= stopGen {
			return
		}
		_, _, startErr := s.EnsureRunning(context.Background())
		if startErr == nil {
			return
		}
		// Clean up lock file before next retry
		// 在下一次重试前清理锁文件
		_ = os.Remove("/var/lib/taos/.running")
		time.Sleep(time.Second)
	}
}

// waitPIDExit polls until pid is no longer alive, the timeout elapses, or the
// context is cancelled, reporting whether the process exited.
//
// waitPIDExit 轮询直到 pid 不再存活、超时到期或上下文被取消，
// 并报告进程是否已退出。
func waitPIDExit(ctx context.Context, pid int, timeout time.Duration) bool {
	if pid <= 0 {
		return true
	}
	deadline := time.Now().Add(timeout)
	for {
		if !pidAlive(pid) {
			return true
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return false
			default:
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(120 * time.Millisecond)
	}
}

// resolveLaunchCommandLocked returns the taosd launch command, honoring the
// command env override and defaulting to ["taosd"]. Callers must hold s.mu.
//
// resolveLaunchCommandLocked 返回 taosd 启动命令，遵从命令环境变量覆盖，
// 默认为 ["taosd"]。调用方必须持有 s.mu。
func (s *taosdSupervisor) resolveLaunchCommandLocked() []string {
	if raw := strings.TrimSpace(os.Getenv(managedTaosdCommandEnv)); raw != "" {
		parts := strings.Fields(raw)
		if len(parts) > 0 {
			return parts
		}
	}
	return []string{"taosd"}
}

// inspectExitReason checks the last managed exit metadata for crash signals.
// With parent-child process model, we only use direct process state, not filesystem scanning.
//
// inspectExitReason 检查最近一次受管退出的元数据是否存在崩溃信号。
// 在父子进程模型下，我们只使用直接的进程状态，而非文件系统扫描。
func inspectExitReason(now time.Time) (string, bool, string) {
	if meta, ok := lastManagedExit(now.Add(-20 * time.Minute)); ok {
		if meta.Signaled && isCrashSignalName(meta.Signal) {
			return formatManagedExit(meta), true,
				fmt.Sprintf("managed taosd exited by signal %s (core_dump=%t)", meta.Signal, meta.CoreDump)
		}
		return formatManagedExit(meta), false, ""
	}
	return "", false, ""
}

// restartTaosd ensures the managed taosd is running and returns a restartResult
// describing the attempt.
//
// restartTaosd 确保受管 taosd 正在运行，并返回描述本次尝试的 restartResult。
func restartTaosd(ctx context.Context) restartResult {
	cmdStr, out, err := ensureManagedTaosd(ctx)
	res := restartResult{
		Attempted: true,
		Command:   strings.TrimSpace(cmdStr),
		Output:    short(out, 260),
	}
	if err != nil {
		res.Error = short(err.Error(), 220)
		return res
	}
	// With parent-child model, we verify the process is alive via direct PID check.
	// The supervisor's EnsureRunning already confirmed the process started.
	// 在父子模型下，我们通过直接的 PID 检查来验证进程是否存活。
	// 监督者的 EnsureRunning 已确认进程已启动。
	res.Succeeded = true
	return res
}

// startManagedTaosdProcess starts the taosd command, directing stdout/stderr to
// the log at logPath (creating its directory), and returns the running command
// and log file.
//
// startManagedTaosdProcess 启动 taosd 命令，将 stdout/stderr 导向 logPath 处的
// 日志（并创建其目录），并返回正在运行的命令和日志文件。
func startManagedTaosdProcess(command []string, logPath string) (*exec.Cmd, *os.File, error) {
	if len(command) == 0 {
		return nil, nil, fmt.Errorf("empty taosd command")
	}
	dir := filepath.Dir(logPath)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	logFile, logErr := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	cmd := exec.Command(command[0], command[1:]...)
	if logErr == nil {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}
	if err := cmd.Start(); err != nil {
		if logFile != nil {
			_ = logFile.Close()
		}
		return nil, nil, fmt.Errorf("start %s: %w", strings.Join(command, " "), err)
	}
	return cmd, logFile, nil
}

// managedTaosdLogPath returns the log path from the env override or the default.
//
// managedTaosdLogPath 从环境变量覆盖或默认值返回日志路径。
func managedTaosdLogPath() string {
	if path := strings.TrimSpace(os.Getenv(managedTaosdLogEnv)); path != "" {
		return path
	}
	return defaultManagedTaosdLog
}

// pidAlive reports whether a process with the given pid currently exists, using
// a signal-0 liveness probe.
//
// pidAlive 使用 signal-0 存活探测，报告具有给定 pid 的进程当前是否存在。
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = proc.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}

// classifyManagedExit converts a Wait error and process state into managedExitMeta,
// extracting the signal, core-dump flag, or exit code.
//
// classifyManagedExit 将 Wait 错误和进程状态转换为 managedExitMeta，
// 提取信号、core-dump 标志或退出码。
func classifyManagedExit(err error, st *os.ProcessState) managedExitMeta {
	meta := managedExitMeta{
		OccurredAt: time.Now(),
	}
	if st != nil {
		if ws, ok := st.Sys().(syscall.WaitStatus); ok {
			if ws.Signaled() {
				meta.Signaled = true
				meta.Signal = ws.Signal().String()
				meta.CoreDump = ws.CoreDump()
			} else {
				meta.ExitCode = ws.ExitStatus()
			}
		} else {
			meta.ExitCode = st.ExitCode()
		}
	}
	if err != nil {
		meta.Message = short(err.Error(), 220)
	}
	return meta
}

// formatManagedExit renders managedExitMeta as a compact, space-separated string
// summarizing the exit time, signal/exit code, and any error.
//
// formatManagedExit 将 managedExitMeta 渲染为紧凑的、以空格分隔的字符串，
// 概括退出时间、信号/退出码以及任何错误。
func formatManagedExit(meta managedExitMeta) string {
	parts := make([]string, 0, 6)
	parts = append(parts, "managed_taosd_exit")
	if !meta.OccurredAt.IsZero() {
		parts = append(parts, "at="+meta.OccurredAt.Format(time.RFC3339Nano))
	}
	if meta.Signaled {
		parts = append(parts, "signal="+meta.Signal)
		parts = append(parts, fmt.Sprintf("core_dump=%t", meta.CoreDump))
	} else {
		parts = append(parts, fmt.Sprintf("exit_code=%d", meta.ExitCode))
	}
	if meta.Message != "" {
		parts = append(parts, "error="+short(meta.Message, 120))
	}
	return strings.Join(parts, " ")
}

// isCrashSignalName reports whether the signal name denotes a crash (e.g.
// segmentation fault, abort, bus error) rather than an orderly termination.
//
// isCrashSignalName 报告该信号名是否表示崩溃（如段错误、abort、总线错误），
// 而非有序的终止。
func isCrashSignalName(signal string) bool {
	switch strings.ToLower(strings.TrimSpace(signal)) {
	case "segmentation fault", "aborted", "bus error", "illegal instruction", "floating point exception", "trace/breakpoint trap", "quit":
		return true
	default:
		return false
	}
}

// errString returns err.Error(), or "" if err is nil.
//
// errString 返回 err.Error()；若 err 为 nil 则返回 ""。
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// short trims s and truncates it to n characters, appending "..." when cut.
//
// short 修剪 s 并将其截断为 n 个字符，被截断时追加 "..."。
func short(s string, n int) string {
	s = strings.TrimSpace(s)
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
