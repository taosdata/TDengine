package client

import (
	"collector/common"
	"collector/config"
)

type OPCClient interface {
	Connect() error
	Collect(collectConfig config.CollectConfig, onMessage OnMessage) error
	GetAllPoints(conf config.PointsConfig) ([]*common.Point, error)
	ChangeCollectConfig(conf config.CollectConfig)
	Close() error
}
type OnMessage func(message []*common.NodeValue)
