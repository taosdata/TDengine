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

func NewDAClient(ctx context.Context, connConf config.DaConnectConfig, collectConf config.CollectConfig, index int, logger *logrus.Entry, onMessage client.OnMessage) (*DAClient, error) {
	panic("only support windows")
}
func (c *DAClient) Connect() error {
	panic("only support windows")
}

func (c *DAClient) Collect() error {
	panic("only support windows")
}

func (c *DAClient) GetAllPoints(conf config.PointsConfig) ([]common.Point, error) {
	panic("only support windows")
}

func (c *DAClient) Close() error {
	panic("only support windows")
}
