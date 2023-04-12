package opcua

import (
	"collector/common"
	"collector/connector"
	"context"
	"fmt"
	"log"
)

type UaConnector struct {
	reader *reader
}

var _ connector.Connector = (*UaConnector)(nil)

func NewConnector(config common.Config) (connector.Connector, error) {
	r, err := newReader(config)
	if err != nil {
		log.Println("## create opc ua reader error", err)
		return nil, err
	}

	return &UaConnector{reader: r}, nil
}

func (c *UaConnector) Connect(ctx context.Context) error {
	if err := c.reader.connect(ctx); err != nil {
		return fmt.Errorf("connect fail %w", err)
	}

	return nil
}

func (c *UaConnector) Stop(ctx context.Context) {
	log.Println("## stop opc ua connector")
	if c.reader != nil {
		c.reader.stop(ctx)
	}
	c.reader = nil
}

func (c *UaConnector) Collect(ctx context.Context) (<-chan *common.NodeValue, error) {
	return c.reader.read(ctx)
}

func (c *UaConnector) GetAllPoints(ctx context.Context) ([]common.Point, error) {
	return c.reader.getAllNodes(ctx)
}
