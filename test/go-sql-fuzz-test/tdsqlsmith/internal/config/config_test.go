package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestParseLegacyOptions(t *testing.T) {
	p, err := Parse([]string{"--target=root:taosdata@tcp(127.0.0.1:6030)/", "--max-queries=123", "--dry-run", "--seed=7", "--verbose", "--dump-all-queries", "--rng-state=0x12", "--config=/tmp/x.toml"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if p.Command != CommandRun {
		t.Fatalf("unexpected command: %s", p.Command)
	}
	if !p.Run.LegacyMode {
		t.Fatalf("expected legacy mode")
	}
	if p.Run.Cases != 123 {
		t.Fatalf("unexpected cases: %d", p.Run.Cases)
	}
	if !p.Run.DryRun || !p.Run.Verbose || !p.Run.DumpAllQueries {
		t.Fatalf("legacy booleans not parsed correctly: %+v", p.Run)
	}
	if p.Run.Seed != 7 {
		t.Fatalf("unexpected seed: %d", p.Run.Seed)
	}
	if p.Run.RNGState != "0x12" {
		t.Fatalf("unexpected rng-state: %q", p.Run.RNGState)
	}
	if p.Run.WorkloadConfig != "/tmp/x.toml" {
		t.Fatalf("unexpected config path: %q", p.Run.WorkloadConfig)
	}
}

func TestParseRunSubcommandCompatibilityAliases(t *testing.T) {
	p, err := Parse([]string{"run", "--target=root:taosdata@tcp(127.0.0.1:6030)/", "--max-queries=50", "--dump-all-graphs", "--exclude-catalog"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if p.Run.DSN == "" {
		t.Fatalf("expected DSN from --target")
	}
	if p.Run.Cases != 50 {
		t.Fatalf("expected cases from --max-queries, got %d", p.Run.Cases)
	}
	if !p.Run.DumpAllGraphs || !p.Run.ExcludeCatalog {
		t.Fatalf("flag parse mismatch: %+v", p.Run)
	}
}

func TestParseRunExecProfile(t *testing.T) {
	p, err := Parse([]string{"run", "--exec-profile=balanced"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if p.Run.ExecProfile != "balanced" {
		t.Fatalf("unexpected exec profile: %q", p.Run.ExecProfile)
	}
}

func TestParseRunExecProfileRejectsInvalid(t *testing.T) {
	_, err := Parse([]string{"run", "--exec-profile=bad"})
	if err == nil {
		t.Fatalf("expected parse error for invalid exec profile")
	}
}

func TestParseRunCleanupSuccessRunDir(t *testing.T) {
	p, err := Parse([]string{"run", "--cleanup-success-run-dir=true"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !p.Run.CleanupSuccessRunDir {
		t.Fatalf("expected cleanup-success-run-dir to be true")
	}
}

func TestParseRunCleanupSuccessRunDirDefaultTrue(t *testing.T) {
	p, err := Parse([]string{"run"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !p.Run.CleanupSuccessRunDir {
		t.Fatalf("expected cleanup-success-run-dir default to true")
	}
	if got := filepath.Base(p.Run.OutDir); got != "out" {
		t.Fatalf("expected default out-dir to end with out, got %q", p.Run.OutDir)
	}
}

func TestParseServe(t *testing.T) {
	p, err := Parse([]string{"serve", "--listen=:18080", "--api-token=abc123", "--data-dir=tmp-data", "--out-dir=tmp-out", "--allow-origin=http://localhost:5173"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if p.Command != CommandServe {
		t.Fatalf("unexpected command: %s", p.Command)
	}
	if p.Serve.Listen != ":18080" {
		t.Fatalf("unexpected listen: %q", p.Serve.Listen)
	}
	if p.Serve.APIToken != "abc123" {
		t.Fatalf("unexpected token: %q", p.Serve.APIToken)
	}
	if p.Serve.AllowOrigin != "http://localhost:5173" {
		t.Fatalf("unexpected origin: %q", p.Serve.AllowOrigin)
	}
	if p.Serve.DataDir == "" || p.Serve.OutDir == "" {
		t.Fatalf("expected absolute paths: %+v", p.Serve)
	}
}

func TestParseServeDefaultOutDir(t *testing.T) {
	p, err := Parse([]string{"serve"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if got := filepath.Base(p.Serve.OutDir); got != "out" {
		t.Fatalf("expected default out-dir to end with out, got %q", p.Serve.OutDir)
	}
}

func TestParseCoverageCommandRemoved(t *testing.T) {
	_, err := Parse([]string{"coverage", "--report=/tmp/run_report.json"})
	if err == nil {
		t.Fatalf("expected parse error for removed coverage command")
	}
	if !strings.Contains(err.Error(), "unknown subcommand") {
		t.Fatalf("unexpected error: %v", err)
	}
}
