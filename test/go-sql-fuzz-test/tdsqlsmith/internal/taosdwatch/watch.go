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
	managedTaosdCommandEnv = "TDSQLSMITH_TAOSD_COMMAND"
	managedTaosdLogEnv     = "TDSQLSMITH_TAOSD_LOG"
	defaultManagedTaosdLog = "/tmp/tdsqlsmith-taosd-restart.log"
)

type Incident struct {
	OccurredAt        time.Time `json:"occurred_at"`
	ExecClass         string    `json:"exec_class"`
	SQL               string    `json:"sql"`
	Error             string    `json:"error"`
	Checked           bool      `json:"checked"`
	ProcessExists     bool      `json:"process_exists"`
	ProcessCheck      string    `json:"process_check,omitempty"`
	ExitReason        string    `json:"exit_reason,omitempty"`
	CoredumpDetected  bool      `json:"coredump_detected"`
	CoredumpEvidence  string    `json:"coredump_evidence,omitempty"`
	RestartAttempted  bool      `json:"restart_attempted"`
	RestartCommand    string    `json:"restart_command,omitempty"`
	RestartSucceeded  bool      `json:"restart_succeeded"`
	RestartOutput     string    `json:"restart_output,omitempty"`
	RestartError      string    `json:"restart_error,omitempty"`
	ReconnectRequired bool      `json:"reconnect_required"`
}

var (
	defaultSupervisor = newTaosdSupervisor()

	ensureManagedTaosd = func(ctx context.Context) (string, string, error) {
		return defaultSupervisor.EnsureRunning(ctx)
	}
	lastManagedExit = func(since time.Time) (managedExitMeta, bool) {
		return defaultSupervisor.LastExitSince(since)
	}
)

func StopManaged(ctx context.Context) error {
	_, err := defaultSupervisor.StopManaged(ctx)
	return err
}

func LastManagedExitSince(since time.Time) (time.Time, bool) {
	meta, ok := lastManagedExit(since)
	if !ok || meta.OccurredAt.IsZero() {
		return time.Time{}, false
	}
	return meta.OccurredAt, true
}

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

type restartResult struct {
	Attempted bool
	Command   string
	Output    string
	Succeeded bool
	Error     string
}

type managedExitMeta struct {
	OccurredAt time.Time
	ExitCode   int
	Signaled   bool
	Signal     string
	CoreDump   bool
	Message    string
}

type taosdSupervisor struct {
	mu             sync.Mutex
	pid            int
	pidGeneration  uint64
	generation     uint64
	stopGeneration uint64
	managed        bool
	starting       bool
	lastExit       managedExitMeta
}

func newTaosdSupervisor() *taosdSupervisor {
	return &taosdSupervisor{}
}

// EnsureRunning starts taosd as a child process if not already running.
// This is the exported entry point for parent-child mode.
func EnsureRunning(ctx context.Context) (string, string, error) {
	return defaultSupervisor.EnsureRunning(ctx)
}

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
		_ = os.Remove("/var/lib/taos/.running")
		time.Sleep(time.Second)
	}
}

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
	res.Succeeded = true
	return res
}

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

func managedTaosdLogPath() string {
	if path := strings.TrimSpace(os.Getenv(managedTaosdLogEnv)); path != "" {
		return path
	}
	return defaultManagedTaosdLog
}

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

func isCrashSignalName(signal string) bool {
	switch strings.ToLower(strings.TrimSpace(signal)) {
	case "segmentation fault", "aborted", "bus error", "illegal instruction", "floating point exception", "trace/breakpoint trap", "quit":
		return true
	default:
		return false
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func short(s string, n int) string {
	s = strings.TrimSpace(s)
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
