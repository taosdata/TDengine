package connector

import (
	"collector/common"
	"context"
)

type Connector interface {
	Connect(ctx context.Context) error
	Stop(ctx context.Context)
	Collect(ctx context.Context) (<-chan *common.NodeValue, error)
	GetAllPoints(ctx context.Context) ([]common.Point, error)
}
