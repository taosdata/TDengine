package run

import (
	"context"
	"testing"
	"time"
)

func TestDryRunCoversAllQueryBranches(t *testing.T) {
	outDir := t.TempDir()
	res, err := Execute(context.Background(), Config{
		Version:         "test",
		Seed:            20260216,
		Cases:           4000,
		StmtTimeout:     2 * time.Second,
		OutDir:          outDir,
		MutationLevel:   0,
		StopWhenCovered: true,
		DryRun:          true,
		Verbose:         false,
	})
	if err != nil {
		t.Fatalf("run execute failed: %v", err)
	}
	if res.Coverage.Hit != res.Coverage.Required {
		t.Fatalf("positive branch not fully covered: %d/%d missing=%v", res.Coverage.Hit, res.Coverage.Required, res.Coverage.Missing)
	}
	if res.Coverage.HitNeg != res.Coverage.RequiredNeg {
		t.Fatalf("negative branch not fully covered: %d/%d missing=%v", res.Coverage.HitNeg, res.Coverage.RequiredNeg, res.Coverage.MissingNeg)
	}
	if res.QueryRules.Required != 113 {
		t.Fatalf("unexpected query rule set size: %d", res.QueryRules.Required)
	}
	if res.QueryRules.Hit == 0 {
		t.Fatalf("unexpected empty query rule hits: missing=%v", res.QueryRules.Missing)
	}
}
