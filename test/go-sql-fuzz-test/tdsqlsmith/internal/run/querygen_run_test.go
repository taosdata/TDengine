package run

import (
	"context"
	"testing"
	"time"
)

func TestDryRunRecordsQueryCombos(t *testing.T) {
	res, err := Execute(context.Background(), Config{
		Version:         "test",
		Seed:            20260219,
		Cases:           250,
		StmtTimeout:     2 * time.Second,
		OutDir:          t.TempDir(),
		MutationLevel:   0,
		StopWhenCovered: false,
		DryRun:          true,
		Verbose:         false,
	})
	if err != nil {
		t.Fatalf("run execute failed: %v", err)
	}
	if len(res.QueryRules.Missing) > 0 && res.QueryRules.Hit == 0 {
		t.Fatalf("unexpected empty query rule hits")
	}
}
