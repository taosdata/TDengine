package opcua

import (
	"collector/common"
	"collector/connector"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/sunpe/gobox/logger"
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
		config.Collect.Interval = 10
	}
	readers, err := createReaders(config)
	if err != nil {
		logger.Error("## create opc ua reader error", "error", err)
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
			for _, created := range readers {
				created.stop(context.Background())
			}
			return nil, fmt.Errorf("create reader fail %w", err)
		}
		readers = append(readers, r)
		start = end
	}

	return
}

func createReader(config common.Config, nodes []common.NodeConfig) (*reader, error) {
	return newReader(config.Debug, config.Connect.Ua, config.Collect.Dump, config.Points, config.Collect.Ua.CollectMode,
		nodes, config.Collect.Interval, config.Collect.ContainsBad)
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
		logger.Warn("## opc ua connector stopped!")
	})
}

func (c *UaConnector) Collect(ctx context.Context) (<-chan *common.NodeValue, error) {
	for _, r := range c.readers {
		c.wait.Add(1)
		go func(r *reader) {
			if err := c.collect(ctx, r); err != nil {
				logger.Error("## collect error", "error", err)
				c.wait.Done()
				c.Stop(ctx)
				panic(err)
			}
			c.wait.Done()
		}(r)
	}

	return c.ch, nil
}

func (c *UaConnector) collect(ctx context.Context, r *reader) error {
	if c.collectMode == common.OPcUaSubscribeType {
		logger.Info("## opc ua connector is in subscribe mode")
		return r.subscribe(ctx, c.ch)
	}

	if c.collectMode == common.OpcUaObserveType {
		logger.Info("## opc ua connector collect is in observe mode")
		return r.observe(ctx, c.ch)
	}

	return fmt.Errorf("collect mode is not supported")
}

func (c *UaConnector) GetAllPoints(ctx context.Context) ([]common.Point, error) {
	return c.readers[0].getAllNodes(ctx)
}

func CheckConnection(config common.Config) error {
	client, err := createClient(config)
	if err != nil {
		return err
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Connect.Ua.ConnectTimeout)*time.Second)
	defer cancel()
	if err := client.Connect(timeoutCtx); err != nil {
		return fmt.Errorf("error in Client Connection: %w", err)
	}
	client.Close(context.Background())
	return nil
}

func createClient(config common.Config) (*opcua.Client, error) {
	if err := config.Connect.Ua.Validate(); err != nil {
		return nil, fmt.Errorf("validate connection collectConfig fail. %v", err)
	}
	connectConfig := config.Connect.Ua
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(connectConfig.RequestTimeout)*time.Second)
	defer cancel()
	endpoints, err := opcua.GetEndpoints(ctx, connectConfig.Endpoint)
	if err != nil {
		return nil, err
	}
	var opts []opcua.Option
	opts = append(opts, opcua.RequestTimeout(time.Duration(connectConfig.RequestTimeout)*time.Second))
	var cert []byte
	if len(connectConfig.Certificate) != 0 && len(connectConfig.PrivateKey) != 0 {
		certificate, err := tls.LoadX509KeyPair(connectConfig.Certificate, connectConfig.PrivateKey)
		if err != nil {
			return nil, err
		}

		privateKey, ok := certificate.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("invalid private key")
		}
		cert = certificate.Certificate[0]
		opts = append(opts, opcua.PrivateKey(privateKey), opcua.Certificate(cert))
	}

	authMode, authOption := authOptions(strings.ToLower(connectConfig.AuthMethod), cert, connectConfig.Username,
		connectConfig.Password)
	opts = append(opts, authOption)
	securityPolity := ua.SecurityPolicyURIPrefix + connectConfig.SecurityPolicy
	if strings.HasPrefix(connectConfig.SecurityPolicy, ua.SecurityPolicyURIPrefix) {
		securityPolity = connectConfig.SecurityPolicy
	}

	securityMode := ua.MessageSecurityModeFromString(connectConfig.SecurityMode)

	if securityMode == ua.MessageSecurityModeNone || securityPolity == ua.SecurityPolicyURINone {
		securityMode = ua.MessageSecurityModeNone
		securityPolity = ua.SecurityPolicyURINone
	}

	serverEndpoint, err := getServerEndpoint(endpoints, securityPolity, securityMode)
	if err != nil {
		return nil, err
	}

	opts = append(opts, opcua.SecurityFromEndpoint(serverEndpoint, authMode))

	return opcua.NewClient(connectConfig.Endpoint, opts...)
}
