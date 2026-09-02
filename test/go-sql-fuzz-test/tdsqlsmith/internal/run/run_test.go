package run

import (
	"context"
	"testing"
	"time"
)

func TestDryRunCoversAllQueryRules(t *testing.T) {
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
	if res.QueryRules.Required != 113 {
		t.Fatalf("unexpected query rule set size: %d", res.QueryRules.Required)
	}
	if res.QueryRules.Hit != res.QueryRules.Required {
		t.Fatalf("query rules not fully covered: %d/%d missing=%v", res.QueryRules.Hit, res.QueryRules.Required, res.QueryRules.Missing)
	}
}
