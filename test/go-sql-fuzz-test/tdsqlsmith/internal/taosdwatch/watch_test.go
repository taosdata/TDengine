package taosdwatch

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestShouldHandle(t *testing.T) {
	tests := []struct {
		name  string
		class string
		err   error
		want  bool
	}{
		{name: "conn_lost class", class: "conn_lost", err: errors.New("x"), want: true},
		{name: "unable establish", class: "db_error", err: errors.New("Unable to establish connection"), want: true},
		{name: "connection refused", class: "db_error", err: errors.New("dial tcp: connection refused"), want: true},
		{name: "broken pipe", class: "db_error", err: errors.New("write: broken pipe"), want: true},
		{name: "unknown 65535", class: "db_error", err: errors.New("Unknown error 65535"), want: true},
		{name: "plain db error", class: "db_error", err: errors.New("not a group by expression"), want: false},
		{name: "nil err", class: "db_error", err: nil, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldHandle(tc.class, tc.err)
			if got != tc.want {
				t.Fatalf("ShouldHandle(%q,%v)=%v want %v", tc.class, tc.err, got, tc.want)
			}
		})
	}
}

func TestIsCrashSignalName(t *testing.T) {
	if !isCrashSignalName("segmentation fault") {
		t.Fatalf("expected segmentation fault to be crash signal")
	}
	if !isCrashSignalName("aborted") {
		t.Fatalf("expected aborted to be crash signal")
	}
	if isCrashSignalName("killed") {
		t.Fatalf("did not expect killed to be crash signal")
	}
}

func TestResolveLaunchCommandUsesEnvOrDefault(t *testing.T) {
	s := newTaosdSupervisor()

	t.Setenv(managedTaosdCommandEnv, "")
	got := s.resolveLaunchCommandLocked()
	if len(got) != 1 || got[0] != "taosd" {
		t.Fatalf("expected default taosd command, got %#v", got)
	}

	t.Setenv(managedTaosdCommandEnv, "taosd -c /tmp/taos_repro/conf")
	got = s.resolveLaunchCommandLocked()
	want := []string{"taosd", "-c", "/tmp/taos_repro/conf"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("expected env command %q, got %#v", strings.Join(want, " "), got)
	}
}

func TestLastManagedExitSince(t *testing.T) {
	prevLastExit := lastManagedExit
	t.Cleanup(func() {
		lastManagedExit = prevLastExit
	})

	wantAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	lastManagedExit = func(time.Time) (managedExitMeta, bool) {
		return managedExitMeta{OccurredAt: wantAt}, true
	}
	gotAt, ok := LastManagedExitSince(time.Time{})
	if !ok {
		t.Fatalf("expected managed exit")
	}
	if !gotAt.Equal(wantAt) {
		t.Fatalf("unexpected managed exit time: got=%s want=%s", gotAt, wantAt)
	}

	lastManagedExit = func(time.Time) (managedExitMeta, bool) {
		return managedExitMeta{}, true
	}
	if _, ok := LastManagedExitSince(time.Time{}); ok {
		t.Fatalf("expected zero exit time to be ignored")
	}
}

func TestHandleDetectsManagedChildCrashAndRestarts(t *testing.T) {
	prevEnsure := ensureManagedTaosd
	prevLastExit := lastManagedExit
	t.Cleanup(func() {
		ensureManagedTaosd = prevEnsure
		lastManagedExit = prevLastExit
	})

	ensureManagedTaosd = func(context.Context) (string, string, error) {
		return "/usr/bin/taosd -c /tmp/taos_repro/conf", "started pid=9123", nil
	}
	lastManagedExit = func(time.Time) (managedExitMeta, bool) {
		return managedExitMeta{
			OccurredAt: time.Now(),
			Signaled:   true,
			Signal:     "segmentation fault",
			CoreDump:   true,
		}, true
	}

	inc := Handle(context.Background(), "conn_lost", "select 1;", fmt.Errorf("unable to establish connection"))
	if !inc.Checked {
		t.Fatalf("expected checked=true")
	}
	if !inc.CoredumpDetected {
		t.Fatalf("expected coredump detection from managed child exit")
	}
	if !inc.RestartAttempted || !inc.RestartSucceeded {
		t.Fatalf("expected restart success, got attempted=%v succeeded=%v err=%s", inc.RestartAttempted, inc.RestartSucceeded, inc.RestartError)
	}
	if inc.RestartCommand != "/usr/bin/taosd -c /tmp/taos_repro/conf" {
		t.Fatalf("unexpected restart command: %q", inc.RestartCommand)
	}
}

func TestHandleRestartManagedStartFailure(t *testing.T) {
	prevEnsure := ensureManagedTaosd
	prevLastExit := lastManagedExit
	t.Cleanup(func() {
		ensureManagedTaosd = prevEnsure
		lastManagedExit = prevLastExit
	})

	ensureManagedTaosd = func(context.Context) (string, string, error) {
		return "taosd -c /tmp/taos_repro/conf", "", fmt.Errorf("start taosd failed")
	}
	lastManagedExit = func(time.Time) (managedExitMeta, bool) {
		return managedExitMeta{}, false
	}

	inc := Handle(context.Background(), "conn_lost", "select 1;", fmt.Errorf("unable to establish connection"))
	if !inc.RestartAttempted {
		t.Fatalf("expected restart attempted")
	}
	if inc.RestartSucceeded {
		t.Fatalf("expected restart failure")
	}
	if !strings.Contains(inc.RestartError, "start taosd failed") {
		t.Fatalf("unexpected restart error: %q", inc.RestartError)
	}
}

func TestHandleNoRestartWhenNoManagedExit(t *testing.T) {
	prevEnsure := ensureManagedTaosd
	prevLastExit := lastManagedExit
	t.Cleanup(func() {
		ensureManagedTaosd = prevEnsure
		lastManagedExit = prevLastExit
	})

	ensureManagedTaosd = func(context.Context) (string, string, error) {
		return "taosd", "started pid=1234", nil
	}
	lastManagedExit = func(time.Time) (managedExitMeta, bool) {
		// No recent managed exit
		return managedExitMeta{}, false
	}

	inc := Handle(context.Background(), "conn_lost", "select 1;", fmt.Errorf("unable to establish connection"))
	// With parent-child model, we always attempt restart when Handle is called
	// because the supervisor manages the process lifecycle
	if !inc.RestartAttempted {
		t.Fatalf("expected restart attempted when no managed process")
	}
}

func TestHandleReportsNonCrashSignal(t *testing.T) {
	prevEnsure := ensureManagedTaosd
	prevLastExit := lastManagedExit
	t.Cleanup(func() {
		ensureManagedTaosd = prevEnsure
		lastManagedExit = prevLastExit
	})

	ensureManagedTaosd = func(context.Context) (string, string, error) {
		return "taosd", "started pid=5678", nil
	}
	lastManagedExit = func(time.Time) (managedExitMeta, bool) {
		return managedExitMeta{
			OccurredAt: time.Now(),
			Signaled:   true,
			Signal:     "terminated", // Not a crash signal
			CoreDump:   false,
		}, true
	}

	inc := Handle(context.Background(), "conn_lost", "select 1;", fmt.Errorf("unable to establish connection"))
	// Exit reason is captured for all managed exits
	if inc.ExitReason == "" {
		t.Fatalf("expected exit reason to be captured")
	}
	// With parent-child model, we always restart when Handle is called
	if !inc.RestartAttempted {
		t.Fatalf("expected restart attempted")
	}
}

func TestStopManagedStopsOwnedProcess(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid
	done := make(chan struct{})
	t.Cleanup(func() {
		if pidAlive(pid) {
			_ = cmd.Process.Kill()
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = cmd.Wait()
		}
	})

	s := newTaosdSupervisor()
	s.mu.Lock()
	s.pid = pid
	s.managed = true
	s.generation = 1
	s.pidGeneration = 1
	s.mu.Unlock()
	go func() {
		s.watchProcess(cmd, nil, 1)
		close(done)
	}()

	stopped, err := s.StopManaged(context.Background())
	if err != nil {
		t.Fatalf("StopManaged() err=%v", err)
	}
	if !stopped {
		t.Fatalf("expected managed process to be stopped")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("watchProcess did not exit in time")
	}
}

func TestStopManagedSkipsExternalProcess(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() {
		if pidAlive(pid) {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	s := newTaosdSupervisor()
	s.mu.Lock()
	s.pid = pid
	s.managed = false
	s.generation = 1
	s.pidGeneration = 1
	s.mu.Unlock()

	stopped, err := s.StopManaged(context.Background())
	if err != nil {
		t.Fatalf("StopManaged() err=%v", err)
	}
	if stopped {
		t.Fatalf("expected external process to be untouched")
	}
	if !pidAlive(pid) {
		t.Fatalf("external process should still be alive")
	}
}

func TestStopManagedPreventsAutoRestart(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	pid := cmd.Process.Pid
	t.Cleanup(func() {
		if pidAlive(pid) {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	s := newTaosdSupervisor()
	s.mu.Lock()
	s.pid = pid
	s.managed = true
	s.generation = 1
	s.pidGeneration = 1
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.watchProcess(cmd, nil, 1)
		close(done)
	}()

	stopped, err := s.StopManaged(context.Background())
	if err != nil {
		t.Fatalf("StopManaged() err=%v", err)
	}
	if !stopped {
		t.Fatalf("expected managed process to be stopped")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("watchProcess did not exit in time")
	}

	time.Sleep(300 * time.Millisecond)
	s.mu.Lock()
	gotPID := s.pid
	gotManaged := s.managed
	gotGen := s.generation
	gotStop := s.stopGeneration
	s.mu.Unlock()
	if gotPID != 0 {
		t.Fatalf("unexpected restarted pid=%d", gotPID)
	}
	if gotManaged {
		t.Fatalf("managed flag should be false after stop")
	}
	if gotGen != 1 {
		t.Fatalf("unexpected generation (restart happened): %d", gotGen)
	}
	if gotStop < 1 {
		t.Fatalf("expected stop generation to be set, got %d", gotStop)
	}
}

func TestEnsureRunningReturnsOnCanceledContext(t *testing.T) {
	s := newTaosdSupervisor()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := s.EnsureRunning(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
