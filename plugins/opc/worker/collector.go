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
	"os"
	"os/signal"
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
	batchSize        int
	batchDuration    time.Duration
	nodeValueCh      chan []*common.NodeValue
	reportConcurrent int
}

func NewCollector(config common.Config) (*OpcCollector, error) {
	if err := config.Report.Validate(); err != nil {
		return nil, err
	}
	var c connector.Connector
	var err error
	if config.OpcType == common.OpcTypeUA {
		c, err = opcua.NewConnector(config)
	} else {
		c, err = opcda.NewConnector(config)
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
		batchSize:        config.Report.BatchSize,
		batchDuration:    time.Duration(config.Report.BatchTimeout) * time.Second,
		nodeValueCh:      make(chan []*common.NodeValue, 100),
		reportConcurrent: config.Report.Concurrent,
	}

	opcCollector.doReport()
	return &opcCollector, nil
}

var _ Collector = (*OpcCollector)(nil)

func (c *OpcCollector) Collect(ctx context.Context) error {
	return c.collect(ctx)
}

func (c *OpcCollector) Stop(_ context.Context) {
	log.Println("## stop worker!")
	close(c.done)
}

func (c *OpcCollector) collect(ctx context.Context) error {
	defer c.reporter.Close()
	// connect to opc
	if err := c.collector.Connect(ctx); err != nil {
		log.Println("## collector connect error", err)
		return err
	}
	defer c.collector.Stop(ctx)
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

	values := make(map[string][]*common.NodeValue, c.batchSize)
	f := func(threshold int) {
		if len(values) >= threshold {
			for _, value := range values {
				c.nodeValueCh <- value
			}
			values = make(map[string][]*common.NodeValue, c.batchSize)
		}
	}

	for {
		select {
		case value, ok := <-ch:
			if !ok {
				continue
			}
			if _, exists := values[value.Identifier]; !exists {
				values[value.Identifier] = make([]*common.NodeValue, 0, c.batchSize)
			}
			values[value.Identifier] = append(values[value.Identifier], value)
			f(c.batchSize)
		case <-ticker.C:
			f(0)
		case <-notifyCtx.Done():
			f(0)
			return nil
		case <-c.done:
			f(0)
			return nil
		}
	}
}

func (c *OpcCollector) doReport() {
	for i := 0; i < c.reportConcurrent; i++ {
		go func() {
			for nodeValues := range c.nodeValueCh {
				c.report(context.Background(), nodeValues)
			}
		}()
	}
}

func (c *OpcCollector) report(ctx context.Context, values []*common.NodeValue) {
	if err := c.reporter.Report(ctx, values); err != nil {
		log.Printf("## report node value error %v", err)
		os.Exit(2)
	}
}
