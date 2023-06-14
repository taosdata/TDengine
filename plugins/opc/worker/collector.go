package worker

import (
	"collector/common"
	"collector/connector"
	"collector/connector/opcda"
	"collector/connector/opcua"
	"collector/reporter"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sunpe/gobox/logger"
)

type Collector interface {
	Collect(ctx context.Context) error
	Stop(ctx context.Context)
}

type OpcCollector struct {
	collector connector.Connector
	reporter  reporter.Reporter
	once      sync.Once
}

func NewCollector(_ context.Context, config common.Config) (*OpcCollector, error) {
	if err := config.Report.Validate(); err != nil {
		return nil, err
	}
	var c connector.Connector
	var err error
	if config.OpcType == common.OpcTypeUA {
		c, err = opcua.NewConnector(config)
	}
	if config.OpcType == common.OpcTypeDA {
		c, err = opcda.NewConnector(config)
	}
	if config.OpcType == common.OpcTypeFake {
		c = connector.NewFakeConnector(config.Collect)
	}
	if err != nil {
		return nil, fmt.Errorf("create connector for worker error %v", err)
	}
	if c == nil {
		return nil, fmt.Errorf("unknown opc type %s", config.OpcType)
	}

	if err != nil {
		logger.Error("## create connector for worker error ", "error", err)
		return nil, fmt.Errorf("create connector for worker error %v", err)
	}
	r, err := reporter.NewDataReporter(config)
	if err != nil {
		logger.Error("## create reporter for worker error ", "error", err)
		return nil, fmt.Errorf("create reporter for worker error %v", err)
	}

	opcCollector := OpcCollector{collector: c, reporter: r}
	return &opcCollector, nil
}

var _ Collector = (*OpcCollector)(nil)

func (c *OpcCollector) Collect(ctx context.Context) error {
	ch, err := c.collector.Collect(ctx)
	if err != nil {
		logger.Error("## collector data error", "error", err)
		return err
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	return c.reporter.Report(cancelCtx, ch)
}

func (c *OpcCollector) Stop(ctx context.Context) {
	c.once.Do(func() {
		time.Sleep(2 * time.Second)

		if c.collector != nil {
			c.collector.Stop(ctx)
		}
		if c.reporter != nil {
			c.reporter.Stop(ctx)
		}

		logger.Warn("## opc collector stopped!")
	})
}
