package worker

import (
	"collector/common"
	"collector/connector"
	"collector/connector/opcda"
	"collector/connector/opcua"
	"collector/reporter"
	"context"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Collector interface {
	Collect(ctx context.Context) error
	Stop(ctx context.Context)
}

type OpcCollector struct {
	collector        connector.Connector
	reporter         reporter.Reporter
	done             chan struct{}
	nodeCount        int
	nodeValueCh      chan []*common.NodeValue
	batchSize        int
	batchDuration    time.Duration
	reportConcurrent int
	once             sync.Once
}

func NewCollector(ctx context.Context, config common.Config) (*OpcCollector, error) {
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
	if c == nil {
		return nil, fmt.Errorf("unknown opc type %s", config.OpcType)
	}

	if err != nil {
		log.Println("## create connector for worker error ", err)
		return nil, fmt.Errorf("create connector for worker error %v", err)
	}
	r, err := reporter.NewArrowReporter(config)
	if err != nil {
		log.Println("## create reporter for worker error ", err)
		return nil, fmt.Errorf("create reporter for worker error %v", err)
	}

	opcCollector := OpcCollector{
		collector:        c,
		reporter:         r,
		done:             make(chan struct{}, 1),
		nodeCount:        len(config.Collect.Ua.Nodes) + len(config.Collect.Da.Tags),
		batchSize:        config.Report.BatchSize,
		batchDuration:    time.Duration(config.Report.BatchTimeout) * time.Second,
		nodeValueCh:      make(chan []*common.NodeValue, 100),
		reportConcurrent: config.Report.Concurrent,
	}

	opcCollector.doReport(ctx)
	return &opcCollector, nil
}

var _ Collector = (*OpcCollector)(nil)

func (c *OpcCollector) Collect(ctx context.Context) error {
	return c.collect(ctx)
}

func (c *OpcCollector) Stop(ctx context.Context) {
	c.once.Do(func() {
		log.Println("## stop worker!")
		if c.collector != nil {
			c.collector.Stop(ctx)
		}
		if c.reporter != nil {
			c.reporter.Close()
		}

		close(c.done)
		time.Sleep(2 * time.Second)
	})
}

func (c *OpcCollector) collect(ctx context.Context) error {
	defer c.reporter.Close()
	// connect to opc
	if err := c.collector.Connect(ctx); err != nil {
		log.Println("## collector connect error", err)
		return err
	}
	defer close(c.nodeValueCh)

	ch, err := c.collector.Collect(ctx)
	if err != nil {
		log.Println("## collector data error", err)
		return err
	}

	notifyCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ticker := time.NewTicker(c.batchDuration)
	defer ticker.Stop()

	values := make(map[string][]*common.NodeValue, c.nodeCount) // key: node identifier, value: node value
	f := func(threshold int) {
		var shouldReport bool
		for _, nodeValues := range values {
			if len(nodeValues) >= threshold {
				shouldReport = true
				break
			}
		}
		if shouldReport {
			for _, nodeValues := range values {
				c.nodeValueCh <- nodeValues
			}
			values = make(map[string][]*common.NodeValue, c.nodeCount) // key: node identifier, value: node value
		}
	}

	for {
		select {
		case value, ok := <-ch:
			if !ok {
				// ch is close. and should exist
				f(1)
				return nil
			}
			if _, exists := values[value.Identifier]; !exists {
				values[value.Identifier] = make([]*common.NodeValue, 0, c.batchSize)
			}
			values[value.Identifier] = append(values[value.Identifier], value)
			f(c.batchSize)
		case <-ticker.C:
			f(1)
		case <-notifyCtx.Done():
			f(1)
			return nil
		case <-c.done:
			f(1)
			return nil
		}
	}
}

func (c *OpcCollector) doReport(ctx context.Context) {
	for i := 0; i < c.reportConcurrent; i++ {
		go func(index int) {
			for nodeValues := range c.nodeValueCh {
				c.report(ctx, index, nodeValues)
			}
		}(i)
	}
}

func (c *OpcCollector) report(ctx context.Context, routineId int, values []*common.NodeValue) {
	if err := c.reporter.Report(ctx, routineId, values); err != nil {
		log.Printf("## report node value error, and exit %v", err)
		// report data error, and exit
		c.Stop(ctx)
	}
}
