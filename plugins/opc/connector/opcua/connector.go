package opcua

import (
	"collector/common"
	"collector/connector"
	"context"
	"fmt"
	"log"
)

type UaConnector struct {
	reader      *reader
	collectMode string
}

var _ connector.Connector = (*UaConnector)(nil)

func NewConnector(config common.Config) (connector.Connector, error) {
	r, err := newReader(config)
	if err != nil {
		log.Println("## create opc ua reader error", err)
		return nil, err
	}

	return &UaConnector{reader: r, collectMode: config.Collect.Ua.CollectMode}, nil
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
	if c.collectMode == common.OPcUaSubscribeType {
		log.Println("## opc ua connector is in subscribe mode")
		return c.reader.subscribe(ctx)
	}
	log.Println("## opc ua connector collect is in observe mode")
	return c.reader.read(ctx)
}

func (c *UaConnector) GetAllPoints(ctx context.Context) ([]common.Point, error) {
	return c.reader.getAllNodes(ctx)
}
