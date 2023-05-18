package opcua

import (
	"collector/common"
	"collector/connector"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type UaConnector struct {
	readers     []*reader
	collectMode string
	ch          chan *common.NodeValue
	wait        sync.WaitGroup
	once        sync.Once
}

var _ connector.Connector = (*UaConnector)(nil)

func NewConnector(config common.Config) (connector.Connector, error) {
	if err := config.Connect.Ua.Validate(); err != nil {
		return nil, fmt.Errorf("validate connection collectConfig fail. %v", err)
	}
	if config.Collect.Interval <= 0 {
		config.Collect.Interval = 1
	}
	readers, err := createReaders(config)
	if err != nil {
		log.Println("## create opc ua reader error", err)
		return nil, err
	}

	nodeValueCh := make(chan *common.NodeValue, len(config.Collect.Ua.Nodes))
	return &UaConnector{readers: readers, collectMode: config.Collect.Ua.CollectMode, ch: nodeValueCh}, nil
}

func createReaders(config common.Config) (readers []*reader, err error) {
	limit := config.Collect.Limit
	nodes := config.Collect.Ua.Nodes
	if limit == 0 || limit >= len(nodes) { // nodes length is zero on get all points case or no limit
		r, err := createReader(config, nodes)
		return []*reader{r}, err
	}
	readers = make([]*reader, 0, len(nodes)/limit+1)
	start := 0
	for start < len(nodes) {
		end := start + limit
		if end > len(nodes) {
			end = len(nodes)
		}
		subNodes := nodes[start:end]
		r, err := createReader(config, subNodes)
		if err != nil {
			return nil, fmt.Errorf("create reader fail %w", err)
		}
		readers = append(readers, r)
		start = end
	}

	return
}

func createReader(config common.Config, nodes []common.NodeConfig) (*reader, error) {
	return newReader(config.Debug, config.Connect.Ua, config.Points, config.Collect.Ua.CollectMode, nodes,
		config.Collect.Interval, config.Collect.ContainsBad)
}

func (c *UaConnector) Stop(ctx context.Context) {
	c.once.Do(func() {
		defer func() {
			c.wait.Wait()
			time.Sleep(time.Second)
			close(c.ch)
		}()

		if c.readers != nil {
			for _, r := range c.readers {
				r.stop(ctx)
			}
		}
		c.readers = nil
		log.Println("## opc ua connector stopped!")
	})
}

func (c *UaConnector) Collect(ctx context.Context) (<-chan *common.NodeValue, error) {
	for _, r := range c.readers {
		c.wait.Add(1)
		go func(r *reader) {
			defer c.wait.Done()
			if err := c.collect(ctx, r); err != nil {
				log.Println("## collect error", err)
			}
		}(r)
	}

	return c.ch, nil
}

func (c *UaConnector) collect(ctx context.Context, r *reader) error {
	if c.collectMode == common.OPcUaSubscribeType {
		log.Println("## opc ua connector is in subscribe mode")
		return r.subscribe(ctx, c.ch)
	}

	if c.collectMode == common.OpcUaObserveType {
		log.Println("## opc ua connector collect is in observe mode")
		return r.observe(ctx, c.ch)
	}

	return fmt.Errorf("collect mode is not supported")
}

func (c *UaConnector) GetAllPoints(ctx context.Context) ([]common.Point, error) {
	return c.readers[0].getAllNodes(ctx)
}
