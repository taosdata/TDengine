//go:build integration

package run

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"tdsqlsmith/internal/executor"
	"tdsqlsmith/internal/report"
)

func TestIntegrationInitFailureCrashSignalRecordedAndRestarted(t *testing.T) {
	if strings.TrimSpace(os.Getenv("TDSQLSMITH_E2E_TAOSD_CRASH")) != "1" {
		t.Skip("set TDSQLSMITH_E2E_TAOSD_CRASH=1 to enable taosd crash-signal integration test")
	}
	requireCmd(t, "sudo")
	requireCmd(t, "systemctl")
	requireCmd(t, "pgrep")
	if out, err := runCmd(3*time.Second, "sudo", "-n", "true"); err != nil {
		t.Skipf("sudo -n unavailable: %v (%s)", err, out)
	}

	_, _ = runCmd(10*time.Second, "sudo", "-n", "systemctl", "start", "taosd")
	t.Cleanup(func() {
		_, _ = runCmd(10*time.Second, "sudo", "-n", "systemctl", "start", "taosd")
	})

	dsn := resolveReachableDSN(t)
	sig := probeCoreCrashSignal(t)
	if err := crashAndHoldTaosdDown(sig); err != nil {
		t.Fatalf("failed to send crash signal %s and hold taosd down: %v", sig, err)
	}

	stats := report.Stats{}
	taosdIncidents := make([]report.TaosdIncident, 0, 2)
	crashIncidents := make([]report.CrashIncident, 0, 2)
	var incidentSeq int64
	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	execInst, err := handleInitConnectionFailure(
		runCtx,
		dsn,
		errors.New("Unable to establish connection"),
		&stats,
		func(string, string) {},
		&taosdIncidents,
		&crashIncidents,
		&incidentSeq,
	)
	if err != nil {
		t.Fatalf("handleInitConnectionFailure failed after crash signal %s: %v", sig, err)
	}
	if execInst == nil {
		t.Fatalf("expected recovered executor, got nil")
	}
	defer execInst.Close()

	if stats.TaosdRestart <= 0 {
		t.Fatalf("expected taosd restart count > 0, got %d", stats.TaosdRestart)
	}
	if len(taosdIncidents) == 0 {
		t.Fatalf("expected taosd incidents to be recorded")
	}
	if len(crashIncidents) == 0 {
		t.Fatalf("expected crash incidents to be recorded after crash signal %s", sig)
	}
	if crashIncidents[0].CrashSQL != "" {
		t.Fatalf("unexpected init crash sql: %q", crashIncidents[0].CrashSQL)
	}

	statusOut, statusErr := runCmd(5*time.Second, "sudo", "-n", "systemctl", "is-active", "taosd")
	if statusErr != nil {
		t.Fatalf("systemctl is-active taosd failed: %v (%s)", statusErr, statusOut)
	}
	if strings.TrimSpace(statusOut) != "active" {
		t.Fatalf("taosd not active after recovery: %q", statusOut)
	}
}

func probeCoreCrashSignal(t *testing.T) string {
	t.Helper()
	signals := []string{"ILL", "SEGV", "ABRT", "BUS", "FPE", "QUIT"}
	for _, sig := range signals {
		_, _ = runCmd(10*time.Second, "sudo", "-n", "systemctl", "start", "taosd")
		before, err := firstTaosdPID()
		if err != nil {
			continue
		}
		if sendErr := sendSignalToPID(sig, before); sendErr != nil {
			continue
		}
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			after, afterErr := firstTaosdPID()
			if afterErr == nil && strings.TrimSpace(after) != "" && strings.TrimSpace(after) != strings.TrimSpace(before) {
				return sig
			}
			time.Sleep(120 * time.Millisecond)
		}
	}
	t.Skip("taosd ignores tested core-producing signals (ILL/SEGV/ABRT/BUS/FPE/QUIT) in this environment")
	return ""
}

func crashAndHoldTaosdDown(sig string) error {
	if err := sendSignalToTaosd(sig); err != nil {
		return err
	}
	time.Sleep(80 * time.Millisecond)
	_, stopErr := runCmd(8*time.Second, "sudo", "-n", "systemctl", "stop", "taosd")
	return stopErr
}

func sendSignalToTaosd(sig string) error {
	pid, err := firstTaosdPID()
	if err != nil {
		return err
	}
	return sendSignalToPID(sig, pid)
}

func sendSignalToPID(sig, pid string) error {
	pid = strings.TrimSpace(pid)
	if pid == "" {
		return fmt.Errorf("empty taosd pid")
	}
	_, err := runCmd(3*time.Second, "sudo", "-n", "kill", "-s", sig, pid)
	return err
}

func firstTaosdPID() (string, error) {
	out, err := runCmd(3*time.Second, "pgrep", "-x", "taosd")
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) == 0 {
		return "", fmt.Errorf("empty pgrep output")
	}
	pid := strings.TrimSpace(lines[0])
	if pid == "" {
		return "", fmt.Errorf("empty pid line")
	}
	return pid, nil
}

func requireCmd(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not found: %v", name, err)
	}
}

func resolveReachableDSN(t *testing.T) string {
	t.Helper()
	candidates := make([]string, 0, 4)
	if dsn := strings.TrimSpace(os.Getenv("TDSQLSMITH_E2E_DSN")); dsn != "" {
		candidates = append(candidates, dsn)
	}
	if dsn := strings.TrimSpace(os.Getenv("DSN")); dsn != "" {
		candidates = append(candidates, dsn)
	}
	candidates = append(candidates,
		"root:taosdata@tcp(127.0.0.1:16030)/",
		"root:taosdata@tcp(127.0.0.1:6030)/",
	)

	seen := map[string]struct{}{}
	for _, dsn := range candidates {
		dsn = strings.TrimSpace(dsn)
		if dsn == "" {
			continue
		}
		if _, ok := seen[dsn]; ok {
			continue
		}
		seen[dsn] = struct{}{}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		inst, err := executor.New(ctx, dsn)
		cancel()
		if err == nil {
			_ = inst.Close()
			return dsn
		}
	}
	t.Skip("no reachable TDengine DSN found for integration test")
	return ""
}

func runCmd(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if ctx.Err() != nil {
		return text, fmt.Errorf("%w (%s %s)", ctx.Err(), name, strings.Join(args, " "))
	}
	return text, err
}
