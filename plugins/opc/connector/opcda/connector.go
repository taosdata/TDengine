//go:build !windows
// +build !windows

package opcda

import (
	"collector/common"
	"collector/connector"
	"context"
	"errors"
)

type DaConnector struct {
}

func NewConnector(_ common.Config) (connector.Connector, error) {
	return nil, errors.New("OPC DA only supports Windows")
}

var _ connector.Connector = (*DaConnector)(nil)

func (d DaConnector) Stop(_ context.Context) {
	panic("implement me")
}

func (d DaConnector) Collect(_ context.Context) (<-chan *common.NodeValue, error) {
	panic("implement me")
}

func (d DaConnector) GetAllPoints(_ context.Context) ([]common.Point, error) {
	panic("implement me")
}
