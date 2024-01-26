package opcua

import (
	"collector/client"
	"collector/common"
	"collector/config"
	"collector/log"
	"collector/types"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	"github.com/sirupsen/logrus"
)

type UAClient struct {
	onMessage     client.OnMessage
	conn          *opcua.Client
	ctx           context.Context
	collectMode   string
	nodes         []*ua.NodeID
	dataCache     []*common.NodeValue
	index         int
	logger        *logrus.Entry
	readInterval  time.Duration
	connectConfig config.UaConnectConfig

	maxNodesPerRead          uint64
	maxMonitoredItemsPerCall uint64
	maxNodesPerBrowse        uint64
	containsBad              bool
	closeChan                chan struct{}
	once                     sync.Once
	dumper                   *log.DataDump
}

func NewUAClient(ctx context.Context, connectConfig config.UaConnectConfig, collectConfig config.CollectConfig, index int, logger *logrus.Entry, onMessage client.OnMessage) (*UAClient, error) {
	if err := connectConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validate connection collectConfig fail. %v", err)
	}
	conn, err := createUAConn(connectConfig)
	if err != nil {
		return nil, err
	}
	dataCache := make([]*common.NodeValue, len(collectConfig.Ua.Nodes))
	nodes := make([]*ua.NodeID, 0, len(collectConfig.Ua.Nodes))
	for i, node := range collectConfig.Ua.Nodes {
		nodeID, err := ua.ParseNodeID(node.ID)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, nodeID)
		dataCache[i] = &common.NodeValue{
			Identifier: node.ID,
		}
	}
	interval := collectConfig.Interval
	opcLogger := logger.WithField("id", index)
	var dataDumper *log.DataDump
	if collectConfig.Dump.Enable {
		opcLogger.Info("dump is enabled")
		dataDumper, err = log.NewDataDump(collectConfig.Dump.Path, collectConfig.Dump.Keep, true)
		if err != nil {
			opcLogger.WithError(err).Error("new data dump error")
			return nil, err
		}
	}
	return &UAClient{
		onMessage:                onMessage,
		conn:                     conn,
		ctx:                      ctx,
		collectMode:              collectConfig.Ua.CollectMode,
		nodes:                    nodes,
		index:                    index,
		logger:                   opcLogger,
		readInterval:             time.Duration(interval) * time.Second,
		connectConfig:            connectConfig,
		maxMonitoredItemsPerCall: 0,
		maxNodesPerRead:          0,
		dataCache:                dataCache,
		containsBad:              collectConfig.ContainsBad,
		closeChan:                make(chan struct{}),
		dumper:                   dataDumper,
	}, nil
}

func createUAConn(connectConfig config.UaConnectConfig) (*opcua.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(connectConfig.RequestTimeout)*time.Second)
	defer cancel()
	endpoints, err := opcua.GetEndpoints(ctx, connectConfig.Endpoint)
	if err != nil {
		return nil, err
	}
	var opts []opcua.Option
	opts = append(opts, opcua.RequestTimeout(time.Duration(connectConfig.RequestTimeout)*time.Second))
	if len(connectConfig.Certificate) != 0 && len(connectConfig.PrivateKey) != 0 {
		certOpt, keyOpt, err := tlsOpts(connectConfig.Certificate, connectConfig.PrivateKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, certOpt, keyOpt)
	}
	if len(connectConfig.Certificate) != 0 && len(connectConfig.PrivateKey) != 0 {
		certOpt, keyOpt, err := tlsOpts(connectConfig.Certificate, connectConfig.PrivateKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, certOpt, keyOpt)
	}
	var authType ua.UserTokenType
	switch strings.ToLower(connectConfig.AuthMethod) {
	case "certificate":
		if len(connectConfig.Certificate) == 0 || len(connectConfig.PrivateKey) == 0 {
			return nil, fmt.Errorf("certificate and privateKey is required if auth method is `certificate`")
		}
		certOpt, keyOpt, err := tlsOpts(connectConfig.Certificate, connectConfig.PrivateKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, certOpt, keyOpt)
		authType = ua.UserTokenTypeCertificate
	case "username":
		if len(connectConfig.Username) == 0 || len(connectConfig.Password) == 0 {
			return nil, fmt.Errorf("user name and password is required for `Username` auth method")
		}
		opts = append(opts, opcua.AuthUsername(connectConfig.Username, connectConfig.Password))
		authType = ua.UserTokenTypeUserName
	case "anonymous":
		opts = append(opts, opcua.AuthAnonymous())
		authType = ua.UserTokenTypeAnonymous
	default:
		return nil, fmt.Errorf("invalid auth method %q", connectConfig.AuthMethod)
	}

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

	opts = append(opts, opcua.SecurityFromEndpoint(serverEndpoint, authType))

	return opcua.NewClient(connectConfig.Endpoint, opts...)
}

func tlsOpts(certFile, keyFile string) (opcua.Option, opcua.Option, error) {
	var cert []byte
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, err
	}
	privateKey, ok := certificate.PrivateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("invalid private key")
	}
	cert = certificate.Certificate[0]
	return opcua.Certificate(cert), opcua.PrivateKey(privateKey), nil
}

func getServerEndpoint(endpoints []*ua.EndpointDescription, securityPolicy string, securityMode ua.MessageSecurityMode) (endpoint *ua.EndpointDescription, err error) {
	// Find the best endpoint (highest SecurityMode+SecurityLevel)
	for _, ep := range endpoints {
		if ep.SecurityPolicyURI == securityPolicy && ep.SecurityMode == securityMode &&
			(endpoint == nil || ep.SecurityLevel >= endpoint.SecurityLevel) {
			endpoint = ep
		}
	}

	if endpoint == nil { // Didn't find an endpoint with matching policy and mode.
		return nil, fmt.Errorf("unable to find suitable server endpoint with selected sec-policy and sec-mode")
	}

	return
}

func (c *UAClient) Connect() error {
	c.logger.Debug("connect to opc ua server")
	timeoutCtx, cancel := context.WithTimeout(c.ctx, time.Duration(c.connectConfig.ConnectTimeout)*time.Second)
	defer cancel()
	if err := c.conn.Connect(timeoutCtx); err != nil {
		return fmt.Errorf("error in Client Connection: %w", err)
	}
	return c.getServerLimit()
}

func (c *UAClient) getServerLimit() error {
	maxReadID, _ := ua.ParseNodeID("i=11705")         //MaxNodesPerRead
	maxItemID, _ := ua.ParseNodeID("i=11714")         //MaxMonitoredItemsPerCall
	maxNodesPerBrowse, _ := ua.ParseNodeID("i=11710") //MaxNodesPerBrowse
	req := &ua.ReadRequest{
		MaxAge: 2000,
		NodesToRead: []*ua.ReadValueID{
			{NodeID: maxReadID},
			{NodeID: maxItemID},
			{NodeID: maxNodesPerBrowse},
		},
		TimestampsToReturn: ua.TimestampsToReturnNeither,
	}
	resp, err := c.conn.Read(c.ctx, req)
	if err != nil {
		return err
	}
	if resp.Results[0].Status == ua.StatusOK {
		c.maxNodesPerRead = resp.Results[0].Value.Uint()
		if c.maxNodesPerRead == 0 {
			c.maxNodesPerRead = uint64(resp.Results[0].Value.Int())
		}
		c.logger.Info("get max node per read success ", c.maxNodesPerRead)
	} else {
		c.logger.Warn("get max node per read fail, set to 1")
		c.maxNodesPerRead = 1
	}
	if c.maxNodesPerRead == 0 {
		c.logger.Warn("get max node per read 0, set to 1")
		c.maxNodesPerRead = 1
	}
	if resp.Results[1].Status == ua.StatusOK {
		c.maxMonitoredItemsPerCall = resp.Results[1].Value.Uint()
		if c.maxMonitoredItemsPerCall == 0 {
			c.maxMonitoredItemsPerCall = uint64(resp.Results[1].Value.Int())
		}
		c.logger.Info("get max monitored items per call success, ", c.maxMonitoredItemsPerCall)
	} else {
		c.logger.Warn("get max monitored items per call fail, set to 1")
		c.maxMonitoredItemsPerCall = 1
	}
	if c.maxMonitoredItemsPerCall == 0 {
		c.logger.Warn("get max monitored items per call 0, set to 1")
		c.maxMonitoredItemsPerCall = 1
	}

	if resp.Results[2].Status == ua.StatusOK {
		c.maxNodesPerBrowse = resp.Results[2].Value.Uint()
		if c.maxNodesPerBrowse == 0 {
			c.maxNodesPerBrowse = uint64(resp.Results[2].Value.Int())
		}
		c.logger.Info("get max nodes per browse success, ", c.maxNodesPerBrowse)
	}
	if c.maxNodesPerBrowse == 0 {
		c.logger.Warn("get max nodes per browse fail, set to maxMonitoredItemsPerCall")
		c.maxNodesPerBrowse = c.maxMonitoredItemsPerCall
	}
	return nil
}

func (c *UAClient) Collect() error {
	err := c.checkCollect()
	if err != nil {
		return err
	}
	err = c.initNodeNameAndValue()
	if err != nil {
		return err
	}
	switch c.collectMode {
	case config.OpcUaObserveType:
		return c.observe()
	case config.OPcUaSubscribeType:
		return c.subscribe()
	default:
		return fmt.Errorf("invalid collect mode %q", c.collectMode)
	}
}

func (c *UAClient) checkCollect() error {
	if len(c.nodes) == 0 {
		return fmt.Errorf("no nodes to collect")
	}
	if c.conn == nil {
		return fmt.Errorf("opc ua client is nil")
	}
	if c.conn.State() != opcua.Connected {
		return fmt.Errorf("opc ua client is not connected")
	}
	return nil
}

func (c *UAClient) initNodeNameAndValue() error {
	err := c.checkCollect()
	if err != nil {
		return err
	}
	// read names
	c.readAllNames()
	// read value
	c.readAllValue()
	return nil
}

func (c *UAClient) readAllNames() {
	maxOperations := uint(c.maxNodesPerRead)
	operationTimes := uint(len(c.nodes)) / maxOperations
	for i := uint(0); i < operationTimes; i++ {
		base := i * maxOperations
		nodes := c.nodes[base : base+maxOperations]
		c.readNameBatch(int(base), nodes)
	}
	if len(c.nodes)%int(maxOperations) != 0 {
		base := operationTimes * maxOperations
		nodes := c.nodes[base:]
		c.readNameBatch(int(base), nodes)
	}

}

func (c *UAClient) readAllValue() {
	maxOperations := uint(c.maxNodesPerRead)
	operationTimes := uint(len(c.nodes)) / maxOperations
	for i := uint(0); i < operationTimes; i++ {
		base := i * maxOperations
		nodes := c.nodes[base : base+maxOperations]
		c.readValueBatch(int(base), nodes)
	}
	if len(c.nodes)%int(maxOperations) != 0 {
		base := operationTimes * maxOperations
		nodes := c.nodes[base:]
		c.readValueBatch(int(base), nodes)
	}
}

func (c *UAClient) readNameBatch(base int, nodes []*ua.NodeID) {
	reqs := make([]*ua.ReadValueID, 0, len(nodes))
	for _, node := range nodes {
		reqs = append(reqs, &ua.ReadValueID{NodeID: node, AttributeID: ua.AttributeIDBrowseName})
	}
	resp, err := c.conn.Read(c.ctx, &ua.ReadRequest{NodesToRead: reqs})
	if err != nil {
		c.logger.WithError(err).Error("read names error")
		return
	}
	for i, r := range resp.Results {
		if r.Status != ua.StatusOK {
			c.logger.WithError(err).Error("read names error")
			continue
			//return fmt.Errorf("read names for node %s failed: %w", nodes[uint(i)].String(), r.Status)
		}
		c.dataCache[base+i].Name = r.Value.String()
	}
	return
}

func (c *UAClient) readValueBatch(base int, nodes []*ua.NodeID) {
	valueReqs := make([]*ua.ReadValueID, 0, len(nodes))
	for _, node := range nodes {
		valueReqs = append(valueReqs, &ua.ReadValueID{NodeID: node, AttributeID: ua.AttributeIDValue})
	}
	start := time.Now()
	resp, err := c.conn.Read(c.ctx, &ua.ReadRequest{MaxAge: 2000, TimestampsToReturn: ua.TimestampsToReturnBoth, NodesToRead: valueReqs})
	if err != nil {
		c.logger.WithError(err).Error("read value batch error")
		return
	}
	end := time.Now()
	c.logger.WithField("time", end.Sub(start)).Debug("read value spend")
	for i, r := range resp.Results {
		if r.Status != ua.StatusOK {
			c.logger.WithField("id", c.dataCache[base+i].Identifier).WithError(r.Status).Error("read value batch status error")
			c.dataCache[base+i].Value = nil
		} else {
			if r.Value != nil {
				c.dataCache[base+i].Value = r.Value.Value()
				if r.Value.ArrayLength() > 0 || r.Value.ArrayDimensions() != nil {
					c.logger.WithField("id", c.dataCache[base+i].Identifier).Warn("skip node: read value is array")
					continue
				}
				exists := false
				c.dataCache[base+i].ValueType, exists = convertType[r.Value.Type()]
				if !exists {
					c.logger.WithField("id", c.dataCache[base+i].Identifier).WithField("valueType", r.Value.Type()).Warn("skip node: read value type is not supported")
					continue
				}
			} else {
				c.dataCache[base+i].Value = nil
			}
		}
		var ts time.Time
		if !r.SourceTimestamp.IsZero() {
			ts = r.SourceTimestamp
		} else if !r.ServerTimestamp.IsZero() {
			ts = r.ServerTimestamp
		} else {
			ts = time.Now()
		}
		c.dataCache[base+i].Timestamp = ts
		c.dataCache[base+i].FinishTime = end
		c.dataCache[base+i].StartTime = start
		c.dataCache[base+i].Status = int64(r.Status)
	}
}

func (c *UAClient) observe() error {
	ticker := time.NewTicker(c.readInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Info("context done,observe exit")
				return
			case <-c.closeChan:
				c.logger.Info("close chan,observe exit")
				return
			case <-ticker.C:
				start := time.Now()
				c.readAllValue()
				spent := time.Since(start)
				if spent > c.readInterval {
					c.logger.WithField("spent", spent).WithField("interval", c.readInterval).Warn("read value spend too much time")
				}
				values := make([]*common.NodeValue, 0, len(c.dataCache))
				for _, data := range c.dataCache {
					if data.Name == "" {
						continue
					}
					if ua.StatusCode(data.Status) != ua.StatusOK {
						c.logger.WithField("id", data.Identifier).WithField("status", ua.StatusCode(data.Status)).Warn("read value status is not ok")
						if !c.containsBad {
							continue
						}
					}
					values = append(values, &common.NodeValue{
						Identifier: data.Identifier,
						Name:       data.Name,
						Timestamp:  data.Timestamp,
						StartTime:  data.StartTime,
						FinishTime: data.FinishTime,
						Value:      data.Value,
						ValueType:  data.ValueType,
						Status:     data.Status,
					})
				}
				if len(values) == 0 {
					c.logger.Warn("opcua read no values")
					return
				}
				if c.dumper != nil {
					c.logger.Debug("opcua start to dump")
					c.dumper.Dump(values)
					c.logger.Debug("opcua dump success")
				}
				c.logger.Debug("read value success")
				c.onMessage(values)
				c.logger.Debug("handle message success")
			}
		}
	}()
	return nil
}

func (c *UAClient) subscribe() error {
	err := c.checkCollect()
	if err != nil {
		return err
	}
	needSubTimes := uint64(len(c.nodes)) / c.maxNodesPerBrowse
	for subTimes := uint64(0); subTimes < needSubTimes; subTimes++ {
		err = c.doSubBatch(uint(subTimes*c.maxNodesPerBrowse), c.nodes[subTimes*c.maxNodesPerBrowse:(subTimes+1)*c.maxNodesPerBrowse])
		if err != nil {
			c.logger.WithError(err).Error("subscribe error")
			return err
		}
	}
	if len(c.nodes)%int(c.maxNodesPerBrowse) != 0 {
		err = c.doSubBatch(uint(needSubTimes*c.maxNodesPerBrowse), c.nodes[needSubTimes*c.maxNodesPerBrowse:])
		if err != nil {
			c.logger.WithError(err).Error("subscribe error")
			return err
		}
	}
	c.logger.Info("add monitored items success")
	return err
}

func (c *UAClient) doSubBatch(base uint, nodes []*ua.NodeID) error {
	maxOperations := uint(c.maxMonitoredItemsPerCall)
	ch := make(chan *opcua.PublishNotificationData, 1)
	sub, err := c.conn.Subscribe(c.ctx, &opcua.SubscriptionParameters{}, ch)
	if err != nil {
		c.logger.WithError(err).Error("subscribe error")
		return err
	}
	c.logger.Info("start to add monitored items")
	subItemTimes := uint(len(nodes)) / maxOperations

	for i := uint(0); i < subItemTimes; i++ {
		indexBase := i * maxOperations
		subNodes := nodes[indexBase : indexBase+maxOperations]
		err = c.doSubItems(int(indexBase+base), subNodes, sub)
		if err != nil {
			return err
		}
	}
	if len(nodes)%int(maxOperations) != 0 {
		indexBase := subItemTimes * maxOperations
		subNodes := nodes[indexBase:]
		err = c.doSubItems(int(indexBase+base), subNodes, sub)
		if err != nil {
			return err
		}
	}
	c.handleSubCallback(sub, ch)
	return nil
}

func (c *UAClient) doSubItems(base int, nodes []*ua.NodeID, sub *opcua.Subscription) error {
	reqs := make([]*ua.MonitoredItemCreateRequest, 0, len(nodes))
	for i, node := range nodes {
		reqs = append(reqs, opcua.NewMonitoredItemCreateRequestWithDefaults(node, ua.AttributeIDValue, uint32(base+i)))
	}
	resp, err := sub.Monitor(c.ctx, ua.TimestampsToReturnBoth, reqs...)
	if err != nil {
		c.logger.WithError(err).Error("monitor error")
		return err
	}
	var errs []error
	for index, r := range resp.Results {
		if r.StatusCode != ua.StatusOK {
			c.logger.WithError(err).Error("monitor item error")
			errs = append(errs, fmt.Errorf("subscribe monitor for node %s failed: %w", nodes[uint(index)].String(), r.StatusCode))
		}
	}
	if len(errs) != 0 {
		err = errors.Join(errs...)
		return err
	}
	return nil
}

func (c *UAClient) handleSubCallback(sub *opcua.Subscription, ch chan *opcua.PublishNotificationData) {
	go func() {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			sub.Cancel(ctx)
		}()
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Info("context done,handleSubCallback exit")
				return
			case <-c.closeChan:
				c.logger.Info("close chan,handleSubCallback exit")
				return
			case data := <-ch:
				if data == nil {
					return
				}
				if data.Error != nil {
					c.logger.WithError(data.Error).Error("sub callback error")
					continue
				}
				switch x := data.Value.(type) {
				case *ua.DataChangeNotification:
					c.logger.Debug("receive data change notification")
					now := time.Now()
					values := make([]*common.NodeValue, 0, len(x.MonitoredItems))
					for _, item := range x.MonitoredItems {
						handle := item.ClientHandle
						nodeID := c.nodes[handle]
						identifier := nodeID.String()

						if item == nil || item.Value == nil {
							c.logger.WithField("identifier", identifier).WithField("item", item).Error("observe opc ua item is nil")
							continue
						}
						if item.Value.Value != nil {
							c.dataCache[handle].Value = item.Value.Value.Value()
						} else {
							c.dataCache[handle].Value = nil
						}
						var ts time.Time
						if !item.Value.SourceTimestamp.IsZero() {
							ts = item.Value.SourceTimestamp
						} else if !item.Value.ServerTimestamp.IsZero() {
							ts = item.Value.ServerTimestamp
						} else {
							ts = now
						}
						c.dataCache[handle].Timestamp = ts
						c.dataCache[handle].FinishTime = now
						c.dataCache[handle].StartTime = now
						c.dataCache[handle].Status = int64(item.Value.Status)
						if item.Value.Status != ua.StatusOK {
							c.logger.WithField("status", item.Value.Status).WithField("identifier", identifier).Warn("read value status is not ok")
							if !c.containsBad {
								continue
							}
						}
						values = append(values, &common.NodeValue{
							Identifier: identifier,
							Name:       c.dataCache[handle].Name,
							Timestamp:  c.dataCache[handle].Timestamp,
							StartTime:  c.dataCache[handle].StartTime,
							FinishTime: c.dataCache[handle].FinishTime,
							Value:      c.dataCache[handle].Value,
							ValueType:  c.dataCache[handle].ValueType,
							Status:     c.dataCache[handle].Status,
						})
					}
					if len(values) != 0 {
						if c.dumper != nil {
							c.logger.Debug("opcua start to dump")
							c.dumper.Dump(values)
							c.logger.Debug("opcua dump success")
						}
						c.logger.Debug("read value success")
						c.onMessage(values)
						c.logger.Debug("handle message success")
					}
				default:
					c.logger.WithField("type", fmt.Sprintf("%T", x)).Error("invalid publish result")
				}
			}
		}
	}()
}

func (c *UAClient) Namespaces() []string {
	return c.conn.Namespaces()
}

type childrenResp struct {
	index    int
	children []*opcua.Node
}

func (c *UAClient) GetAllPoints(conf config.PointsConfig) ([]common.Point, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("opc ua client is nil")
	}
	if c.conn.State() != opcua.Connected {
		return nil, fmt.Errorf("opc ua client is not connected")
	}
	reg, err := regexp.Compile(conf.Regex)
	if err != nil {
		return nil, fmt.Errorf("invalid points regex: %w", err)
	}
	rootId, err := ua.ParseNodeID(conf.Ua.Root) // objects node
	if err != nil {
		return nil, err
	}
	nss := conf.Ua.Namespaces
	nsMap := make(map[uint16]struct{}, len(nss))
	for _, ns := range nss {
		nsMap[ns] = struct{}{}
	}
	rootNode := c.conn.Node(rootId)
	ctx := c.ctx
	bfsList := []*opcua.Node{rootNode}
	var result []common.Point
	m := sync.Map{}
	wg := sync.WaitGroup{}
	// bfs
	for {
		if len(bfsList) == 0 {
			break
		}
		nodeCount := len(bfsList)
		childrenChannels := make(chan *childrenResp, nodeCount)
		maxNodePerGetPoints := 1
		if c.maxNodesPerRead > 3 {
			maxNodePerGetPoints = int(c.maxNodesPerRead) / 3
		}
		operation := nodeCount / maxNodePerGetPoints
		getTimes := operation

		more := false
		if nodeCount%maxNodePerGetPoints != 0 {
			more = true
			getTimes += 1
		}
		wg.Add(getTimes)
		availablePoints := make([][]*common.Point, getTimes)
		for i := 0; i < operation; i++ {
			go func(i int) {
				defer wg.Done()
				points := c.getPoints(ctx, c.conn, bfsList[i*maxNodePerGetPoints:(i+1)*maxNodePerGetPoints], reg, nsMap)
				availablePoints[i] = points
			}(i)
		}
		if more {
			go func() {
				defer wg.Done()
				points := c.getPoints(ctx, c.conn, bfsList[operation*maxNodePerGetPoints:], reg, nsMap)
				availablePoints[operation] = points
			}()
		}
		wg.Wait()
		wg.Add(nodeCount)
		for i, node := range bfsList {
			go func(index int, n *opcua.Node) {
				defer wg.Done()
				children, err := getChildren(ctx, n)
				if err != nil {
					c.logger.WithError(err).Error("get children error")
				}
				m.LoadOrStore(n.String(), struct{}{})
				childrenChannels <- &childrenResp{
					index:    index,
					children: children,
				}
			}(i, node)
		}
		wg.Wait()
		for _, points := range availablePoints {
			if points != nil {
				for _, point := range points {
					result = append(result, *point)
					if conf.Limit > 0 && len(result) >= conf.Limit {
						return result, nil
					}
				}
			}
		}
		bfsList = bfsList[:0]
		for i := 0; i < nodeCount; i++ {
			select {
			case resp := <-childrenChannels:
				for _, child := range resp.children {
					//avoid nested loops
					_, ok := m.Load(child.String())
					if !ok {
						bfsList = append(bfsList, child)
						m.Store(child.String(), struct{}{})
					}
				}
			}
		}
	}
	return result, nil
}

var convertType = map[ua.TypeID]types.ValueType{
	ua.TypeIDBoolean:  types.BOOL,
	ua.TypeIDSByte:    types.INT8,
	ua.TypeIDByte:     types.UINT8,
	ua.TypeIDInt16:    types.INT16,
	ua.TypeIDUint16:   types.UINT16,
	ua.TypeIDInt32:    types.INT32,
	ua.TypeIDUint32:   types.UINT32,
	ua.TypeIDInt64:    types.INT64,
	ua.TypeIDUint64:   types.UINT64,
	ua.TypeIDFloat:    types.Float,
	ua.TypeIDDouble:   types.DOUBLE,
	ua.TypeIDString:   types.STRING,
	ua.TypeIDDateTime: types.TIMESTAMP,
}

var attributes = []ua.AttributeID{
	ua.AttributeIDNodeClass,
	ua.AttributeIDBrowseName,
}
var attributeNames = []string{
	"NodeClass",
	"BrowseName",
}

func (c *UAClient) getPoints(ctx context.Context, conn *opcua.Client, ns []*opcua.Node, reg *regexp.Regexp, nsMap map[uint16]struct{}) []*common.Point {
	nodes := make([]*opcua.Node, 0, len(ns))
	for i := 0; i < len(ns); i++ {
		if len(nsMap) == 0 {
			if ns[i].ID.Namespace() == 0 {
				continue
			}
		} else {
			if _, ok := nsMap[ns[i].ID.Namespace()]; !ok {
				continue
			}
		}
		nodes = append(nodes, ns[i])
	}
	req := &ua.ReadRequest{NodesToRead: make([]*ua.ReadValueID, 0, len(nodes)*len(attributes))}
	for _, node := range nodes {
		for i := 0; i < len(attributes); i++ {
			req.NodesToRead = append(req.NodesToRead, &ua.ReadValueID{
				NodeID:      node.ID,
				AttributeID: attributes[i],
			})
		}
	}
	if len(req.NodesToRead) == 0 {
		return nil
	}
	res, err := conn.Read(ctx, req)
	if err != nil {
		c.logger.WithError(err).Error("get points properties error")
		return nil
	}
	var result []*common.Point
	for i := 0; i < len(nodes); i++ {
		index := i * len(attributes)
		err = res.Results[index].Status
		if !errors.Is(err, ua.StatusOK) {
			c.logger.WithError(err).WithField("nodeID", nodes[i].ID.String()).Errorf("get node attribute %s error", attributeNames[0])
			continue
		}
		nodeClass := ua.NodeClass(res.Results[index].Value.Int())
		if nodeClass != ua.NodeClassVariable {
			continue
		}
		err = res.Results[index+1].Status
		if !errors.Is(err, ua.StatusOK) {
			c.logger.WithError(err).WithField("nodeID", nodes[i].ID.String()).Errorf("get node attribute %s error", attributeNames[1])
			continue
		}
		browseName := res.Results[index+1].Value.String()
		point := &common.Point{
			ID:   nodes[i].ID.String(),
			Name: browseName,
		}
		if !regMatched(reg, point) {
			continue
		} else {
			result = append(result, point)
		}
	}
	return result
}

func regMatched(reg *regexp.Regexp, point *common.Point) bool {
	if reg == nil {
		return true
	}
	if reg.MatchString(point.ID) || reg.MatchString(point.Name) {
		return true
	}
	return false
}

func getChildren(ctx context.Context, n *opcua.Node) ([]*opcua.Node, error) {
	children := make([]*opcua.Node, 0)
	refs, err := n.ReferencedNodes(ctx, id.HierarchicalReferences, ua.BrowseDirectionForward, ua.NodeClassAll, true)
	if err != nil {
		return nil, fmt.Errorf("reference: %d: %s", id.HierarchicalReferences, err)
	}
	if len(refs) > 0 {
		children = append(children, refs...)
	}
	return children, nil
}

func (c *UAClient) Close() error {
	c.once.Do(func() {
		close(c.closeChan)
		if c.conn != nil {
			ctx, cancel := context.WithTimeout(c.ctx, time.Second*5)
			defer cancel()
			c.conn.Close(ctx)
		}
		if c.dumper != nil {
			c.dumper.Close()
		}
	})
	return nil
}
