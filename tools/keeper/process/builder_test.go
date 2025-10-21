package process

import (
	"context"
	"testing"

	"github.com/taosdata/taoskeeper/infrastructure/config"
)

func TestExpandMetricsFromConfig_DuplicateTables_ContinueBranch(t *testing.T) {
	ctx := context.Background()
	cfg := &config.MetricsConfig{
		Tables: []string{"t1", "t1", "t2", "t2"},
		Database: config.Database{
			Name: "db",
		},
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic due to nil connector after selected code executed")
		}
	}()

	_, _ = ExpandMetricsFromConfig(ctx, nil, cfg)
}
