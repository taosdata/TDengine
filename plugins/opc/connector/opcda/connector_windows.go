//go:build windows
// +build windows

package opcda

import (
	"collector/common"
	"collector/connector"
	"context"
	"log"
)

type DaConnector struct {
	r *reader
}

func NewConnector(config common.Config) (connector.Connector, error) {
	r, err := newReader(config)
	if err != nil {
		return nil, err
	}
	return &DaConnector{r: r}, nil
}

var _ connector.Connector = (*DaConnector)(nil)

func (d *DaConnector) Stop(ctx context.Context) {
	if d.r != nil {
		d.r.stop(ctx)
	}
	d.r = nil
	log.Println("## opc da connector stopped!")
}

func (d *DaConnector) Collect(ctx context.Context) (<-chan *common.NodeValue, error) {
	return d.r.read(ctx)
}

func (d *DaConnector) GetAllPoints(ctx context.Context) ([]common.Point, error) {
	return d.r.getAllTags(ctx)
}
