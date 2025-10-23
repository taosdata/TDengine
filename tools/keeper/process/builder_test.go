package process

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/infrastructure/config"
)

func TestExpandMetricsFromConfig(t *testing.T) {
	ctx := context.Background()
	cfg := &config.MetricsConfig{
		Tables: []string{"t1", "t1", "t2", "t2"},
		Database: config.Database{
			Name: "db",
		},
	}

	assert.Panics(t, func() {
		_, _ = ExpandMetricsFromConfig(ctx, nil, cfg)
	})
}
