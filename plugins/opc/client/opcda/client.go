//go:build !windows
// +build !windows

package opcda

import (
	"collector/client"
	"collector/common"
	"collector/config"
	"context"

	"github.com/sirupsen/logrus"
)

type DAClient struct {
}

func NewDAClient(ctx context.Context, connectConfig config.DaConnectConfig, index int, logger *logrus.Entry) (*DAClient, error) {
	panic("only support windows")
}
func (c *DAClient) Connect() error {
	panic("only support windows")
}

func (c *DAClient) Collect(collectConfig config.CollectConfig, onMessage client.OnMessage) error {
	panic("only support windows")
}

func (c *DAClient) GetAllPoints(conf config.PointsConfig) ([]*common.Point, error) {
	panic("only support windows")
}

func (c *DAClient) ChangeCollectConfig(conf config.CollectConfig) {
	panic("only support windows")
}

func (c *DAClient) Close() error {
	panic("only support windows")
}
