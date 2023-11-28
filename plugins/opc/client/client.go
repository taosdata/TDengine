package client

import (
	"collector/common"
	"collector/config"
)

type OPCClient interface {
	Connect() error
	Collect() error
	GetAllPoints(conf config.PointsConfig) ([]common.Point, error)
	Close() error
}
type OnMessage func(message []*common.NodeValue)

var UnrecoverableError = make(chan error, 1)
