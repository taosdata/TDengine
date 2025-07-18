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
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"collector/regexp"

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
	nodes         []*nodeValue
	index         int
	logger        *logrus.Entry
	readInterval  time.Duration
	connectConfig config.UaConnectConfig

	maxNodesPerRead          uint64
	maxMonitoredItemsPerCall uint64
	maxNodesPerBrowse        uint64
	isKepServer              bool
	containsBad              bool
	closeChan                chan struct{}
	once                     sync.Once
	dumper                   *log.DataDump
	maxAge                   float64

	observeChange chan []*nodeValue
	subList       []*subscription
	subIndex      int

	reconnectMutex sync.Mutex
}

type subscription struct {
	client            *UAClient
	nodes             []*nodeValue
	ch                chan *opcua.PublishNotificationData
	sub               *opcua.Subscription
	clientHandleIndex uint32
	subCount          int
	subIndex          int
}

type nodeValue struct {
	nodeID          *ua.NodeID
	nodeValue       *common.NodeValue
	clientHandle    uint32 //always exists
	subscribed      bool
	subscriptionID  *int
	monitoredItemID uint32
}

func newSubscription(uaClient *UAClient) (*subscription, error) {
	c := uaClient
	ch := make(chan *opcua.PublishNotificationData, 1)
	sub, err := c.conn.Subscribe(c.ctx, &opcua.SubscriptionParameters{}, ch)
	if err != nil {
		c.logger.WithError(err).Error("subscribe error")
		return nil, err
	}
	s := &subscription{
		ch:       ch,
		sub:      sub,
		client:   uaClient,
		subIndex: uaClient.subIndex,
	}
	c.subList = append(c.subList, s)
	uaClient.subIndex += 1
	return s, nil
}

func NewUAClient(ctx context.Context, connectConfig config.UaConnectConfig, index int, logger *logrus.Entry) (*UAClient, error) {
	if err := connectConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validate connection collectConfig fail. %v", err)
	}
	conn, err := createUAConn(connectConfig)
	if err != nil {
		return nil, err
	}

	opcLogger := logger.WithField("id", index)
	maxAge := float64(2000)
	if connectConfig.MaxAge != nil {
		maxAge = *connectConfig.MaxAge
	}
	return &UAClient{
		conn:                     conn,
		ctx:                      ctx,
		index:                    index,
		logger:                   opcLogger,
		connectConfig:            connectConfig,
		maxMonitoredItemsPerCall: 0,
		maxNodesPerRead:          0,
		maxAge:                   maxAge,
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
	opts = append(
		opts,
		opcua.ApplicationURI("urn:taosx-opc:client"),
		opcua.ApplicationName("taosx-opc"),
		opcua.ProductURI("urn:taosx-opc"),
	)
	opts = append(opts, opcua.RequestTimeout(time.Duration(connectConfig.RequestTimeout)*time.Second))
	if len(connectConfig.Certificate) != 0 && len(connectConfig.PrivateKey) != 0 {
		cert, key, err := tlsOpts(connectConfig.Certificate, connectConfig.PrivateKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, opcua.Certificate(cert), opcua.PrivateKey(key))
	}
	var authType ua.UserTokenType
	switch strings.ToLower(connectConfig.AuthMethod) {
	case "certificate":
		if len(connectConfig.AuthCertificate) == 0 || len(connectConfig.AuthPrivateKey) == 0 {
			return nil, fmt.Errorf("certificate and privateKey is required if auth method is `certificate`")
		}
		cert, key, err := tlsOpts(connectConfig.AuthCertificate, connectConfig.AuthPrivateKey)
		if err != nil {
			return nil, err
		}
		opts = append(opts, opcua.AuthCertificate(cert), opcua.AuthPrivateKey(key))
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

func tlsOpts(certFile, keyFile string) ([]byte, *rsa.PrivateKey, error) {
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
	return cert, privateKey, nil
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
	return c.doConnect(c.conn)
}

func (c *UAClient) doConnect(conn *opcua.Client) error {
	c.logger.Debug("connect to opc ua server")
	timeoutCtx, cancel := context.WithTimeout(c.ctx, time.Duration(c.connectConfig.ConnectTimeout)*time.Second)
	defer cancel()
	if err := conn.Connect(timeoutCtx); err != nil {
		return fmt.Errorf("error in Client Connection: %w", err)
	}
	return nil
}

func (c *UAClient) getServerLimit(needMonitorLimit bool) error {
	productNameID, _ := ua.ParseNodeID("i=2261")
	maxReadID, _ := ua.ParseNodeID("i=11705")         //MaxNodesPerRead
	maxItemID, _ := ua.ParseNodeID("i=11714")         //MaxMonitoredItemsPerCall
	maxNodesPerBrowse, _ := ua.ParseNodeID("i=11710") //MaxNodesPerBrowse
	req := &ua.ReadRequest{
		MaxAge: c.maxAge,
		NodesToRead: []*ua.ReadValueID{
			{NodeID: maxReadID},
			{NodeID: maxItemID},
			{NodeID: maxNodesPerBrowse},
			{NodeID: productNameID},
		},
		TimestampsToReturn: ua.TimestampsToReturnNeither,
	}
	resp, err := c.conn.Read(c.ctx, req)
	if err != nil {
		return err
	}
	if errors.Is(resp.Results[3].Status, ua.StatusOK) {
		if resp.Results[3].Value.String() == "KEPServerEX" {
			c.logger.Info("get opc ua server KEPServerEX,set max nodes per read 10000, max monitored items per call 10000, max nodes per browse 10000")
			c.maxNodesPerRead = 10000
			c.maxMonitoredItemsPerCall = 10000
			c.maxNodesPerBrowse = 10000
			c.isKepServer = true
			return nil
		}
	}
	if errors.Is(resp.Results[0].Status, ua.StatusOK) {
		c.maxNodesPerRead = resp.Results[0].Value.Uint()
		if c.maxNodesPerRead == 0 {
			c.maxNodesPerRead = uint64(resp.Results[0].Value.Int())
		}
		c.logger.Info("get max node per read success ", c.maxNodesPerRead)
	} else {
		c.logger.Warn("get max node per read fail")
	}
	if c.maxNodesPerRead == 0 {
		c.maxNodesPerRead = 1000
		c.logger.Warn("get max node per read 0, set maxNodesPerRead 1000")
	}
	if needMonitorLimit {
		if errors.Is(resp.Results[1].Status, ua.StatusOK) {
			c.maxMonitoredItemsPerCall = resp.Results[1].Value.Uint()
			if c.maxMonitoredItemsPerCall == 0 {
				c.maxMonitoredItemsPerCall = uint64(resp.Results[1].Value.Int())
			}
			c.logger.Info("get max monitored items per call success, ", c.maxMonitoredItemsPerCall)
		} else {
			c.logger.Warn("get max monitored items per call fail")
		}
		if c.maxMonitoredItemsPerCall == 0 {
			c.maxMonitoredItemsPerCall = 1000
			c.logger.Warn("get max monitored items per call 0, set maxMonitoredItemsPerCall 1000")
		}

		if errors.Is(resp.Results[2].Status, ua.StatusOK) {
			c.maxNodesPerBrowse = resp.Results[2].Value.Uint()
			if c.maxNodesPerBrowse == 0 {
				c.maxNodesPerBrowse = uint64(resp.Results[2].Value.Int())
			}
			c.logger.Info("get max nodes per browse success, ", c.maxNodesPerBrowse)
		} else {
			c.logger.Warn("get max node per browse fail")
		}
		if c.maxNodesPerBrowse == 0 {
			c.logger.Warn("get max nodes per browse 0, set maxNodesPerBrowse 1000")
			c.maxNodesPerBrowse = 1000
		}
	}
	return nil
}

func (c *UAClient) Collect(collectConfig config.CollectConfig, onMessage client.OnMessage) error {
	nodes := make([]*nodeValue, len(collectConfig.Ua.Nodes))
	for i, node := range collectConfig.Ua.Nodes {
		nodeID, err := ua.ParseNodeID(node.ID)
		if err != nil {
			c.logger.WithField("node", node.ID).WithError(err).Error("parse node id error")
			return err
		}
		nodes[i] = &nodeValue{
			nodeID: nodeID,
			nodeValue: &common.NodeValue{
				IDStr: node.ID,
			},
		}
	}
	interval := collectConfig.Interval
	var dataDumper *log.DataDump
	var err error
	if collectConfig.Dump.Enable {
		c.logger.Info("dump is enabled")
		dataDumper, err = log.NewDataDump(collectConfig.Dump.Path, collectConfig.Dump.Keep, true)
		if err != nil {
			c.logger.WithError(err).Error("new data dump error")
			return err
		}
	}

	c.onMessage = onMessage
	c.readInterval = time.Duration(interval) * time.Second
	c.dumper = dataDumper
	c.closeChan = make(chan struct{})
	c.collectMode = collectConfig.Ua.CollectMode
	c.nodes = nodes
	c.containsBad = collectConfig.ContainsBad
	c.logger.Info("opc ua start to collect")
	err = c.checkCollect()
	if err != nil {
		return err
	}
	err = c.getServerLimit(c.collectMode == config.OpcUaSubscribeType)
	if err != nil {
		return err
	}
	err = c.initNodeName()
	if err != nil {
		return err
	}
	switch c.collectMode {
	case config.OpcUaObserveType:
		c.observeChange = make(chan []*nodeValue, 1)
		return c.observe()
	case config.OpcUaSubscribeType:
		return c.subscribe(c.nodes)
	default:
		return fmt.Errorf("invalid collect mode %q", c.collectMode)
	}
}

func (c *UAClient) checkCollect() error {
	if c.conn == nil {
		return fmt.Errorf("opc ua client is nil")
	}
	if c.conn.State() != opcua.Connected {
		return fmt.Errorf("opc ua client is not connected")
	}
	return nil
}

func (c *UAClient) initNodeName() error {
	err := c.checkCollect()
	if err != nil {
		return err
	}
	// read names
	c.readAllNames(c.nodes)
	return nil
}

func (c *UAClient) readAllNames(nodes []*nodeValue) {
	maxOperations := uint(c.maxNodesPerRead)
	operationTimes := uint(len(nodes)) / maxOperations
	for i := uint(0); i < operationTimes; i++ {
		base := i * maxOperations
		c.readNameBatch(nodes[base : base+maxOperations])
	}
	if len(nodes)%int(maxOperations) != 0 {
		base := operationTimes * maxOperations
		c.readNameBatch(nodes[base:])
	}
}

func (c *UAClient) readAllValue(nodes []*nodeValue) {
	if len(nodes) == 0 {
		c.logger.Errorf("no nodes to collect")
		return
	}
	maxOperations := uint(c.maxNodesPerRead)
	operationTimes := uint(len(nodes)) / maxOperations
	for i := uint(0); i < operationTimes; i++ {
		base := i * maxOperations
		c.readValueBatch(nodes[base : base+maxOperations])
	}
	if len(nodes)%int(maxOperations) != 0 {
		base := operationTimes * maxOperations
		c.readValueBatch(nodes[base:])
	}
}

func (c *UAClient) readNameBatch(nodes []*nodeValue) {
	reqs := make([]*ua.ReadValueID, 0, len(nodes))
	for _, node := range nodes {
		reqs = append(reqs, &ua.ReadValueID{NodeID: node.nodeID, AttributeID: ua.AttributeIDBrowseName})
	}
	resp, err := c.conn.Read(c.ctx, &ua.ReadRequest{NodesToRead: reqs})
	if err != nil {
		c.logger.WithError(err).Error("read names error")
		return
	}
	for i, r := range resp.Results {
		if !errors.Is(r.Status, ua.StatusOK) {
			c.logger.WithError(err).Error("read names error")
			continue
		}
		nodes[i].nodeValue.Name = r.Value.String()
	}
	return
}

func (c *UAClient) readValueBatch(nodes []*nodeValue) {
	valueReqs := make([]*ua.ReadValueID, 0, len(nodes))
	for _, node := range nodes {
		valueReqs = append(valueReqs, &ua.ReadValueID{NodeID: node.nodeID, AttributeID: ua.AttributeIDValue})
	}
	start := time.Now()
	conn := c.conn
	resp, err := conn.Read(c.ctx, &ua.ReadRequest{MaxAge: c.maxAge, TimestampsToReturn: ua.TimestampsToReturnBoth, NodesToRead: valueReqs})
	if err != nil {
		c.logger.WithError(err).Error("read value batch error")
		c.reconnect(conn, err)
		return
	}
	end := time.Now()
	c.logger.WithField("time", end.Sub(start)).Debug("read value spend")
	for i, r := range resp.Results {
		if !errors.Is(r.Status, ua.StatusOK) {
			c.logger.WithField("id", nodes[i].nodeValue.IDStr).WithError(r.Status).Error("read value batch status error")
			nodes[i].nodeValue.Value = nil
		} else {
			if r.Value != nil {
				nodes[i].nodeValue.Value = r.Value.Value()
				if r.Value.ArrayLength() > 0 || r.Value.ArrayDimensions() != nil {
					c.logger.WithField("id", nodes[i].nodeValue.IDStr).Warn("skip node: read value is array")
					continue
				}
				exists := false
				nodes[i].nodeValue.ValueType, exists = convertType[r.Value.Type()]
				if !exists {
					c.logger.WithField("id", nodes[i].nodeValue.IDStr).WithField("valueType", r.Value.Type()).Warn("skip node: read value type is not supported")
					continue
				}
			} else {
				nodes[i].nodeValue.Value = nil
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
		nodes[i].nodeValue.Timestamp = ts
		nodes[i].nodeValue.FinishTime = end
		nodes[i].nodeValue.StartTime = start
		nodes[i].nodeValue.Status = int64(r.Status)
	}
}

func (c *UAClient) reconnect(oldConn *opcua.Client, err error) {
	// reconnect
	if err == nil {
		return
	}
	if _, ok := err.(*net.OpError); !ok {
		return
	}
	c.reconnectMutex.Lock()
	defer c.reconnectMutex.Unlock()
	if c.conn != oldConn {
		return
	}
	for i := 1; i < 61; i++ {
		c.logger.Infof("reconnect to opcua server after 5s, retry %d times", i)
		time.Sleep(time.Second * 5)
		c.logger.Infof("reconnect to opcua server, retry %d times", i)
		conn, err := createUAConn(c.connectConfig)
		if err != nil {
			c.logger.Errorf("reconnect create ua connection error, retry %d times, error: %v", i, err)
			continue
		}
		err = c.doConnect(conn)
		if err != nil {
			c.logger.Errorf("reconnect connect ua server error, retry %d times, error: %v", i, err)
			continue
		}
		c.logger.Debug("close old connection")
		c.doCloseConn(c.conn)
		c.conn = conn
		c.logger.Infof("reconnect to opcua server success, retry %d times", i)
		return
	}
	c.logger.Panic("reconnect to opcua server failed, retry 60 times")
}

func (c *UAClient) observe() error {
	ticker := time.NewTicker(c.readInterval)
	go func() {
		defer ticker.Stop()
		var readNameList []*nodeValue
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Info("context done,observe exit")
				return
			case <-c.closeChan:
				c.logger.Info("close chan,observe exit")
				return
			case data := <-c.observeChange:
				c.nodes = data
			case <-ticker.C:
				readNameList = readNameList[:0]
				start := time.Now()
				c.readAllValue(c.nodes)
				spent := time.Since(start)
				if spent > c.readInterval {
					c.logger.WithField("spent", spent).WithField("interval", c.readInterval).Warn("read value spend too much time")
				}
				values := make([]*common.NodeValue, 0, len(c.nodes))
				for _, node := range c.nodes {
					if !node.nodeValue.ValueType.IsValid() {
						continue
					}
					if node.nodeValue.Name == "" {
						readNameList = append(readNameList, node)
						continue
					}
					if !errors.Is(ua.StatusCode(node.nodeValue.Status), ua.StatusOK) {
						c.logger.WithField("id", node.nodeValue.IDStr).WithField("status", ua.StatusCode(node.nodeValue.Status)).Warn("read value status is not ok")
						if !c.containsBad {
							continue
						}
					}
					values = append(values, node.nodeValue.Copy())
				}
				if len(readNameList) != 0 {
					readNameStart := time.Now()
					c.readAllNames(readNameList)
					readNameEnd := time.Now()
					totalSpent := readNameEnd.Sub(start)
					if spent > c.readInterval {
						c.logger.WithField("total", totalSpent).WithField("interval", c.readInterval).WithField("name", readNameEnd.Sub(readNameStart)).WithField("value", spent).WithField("read_count", len(readNameList)).Warn("after read name spend too much time")
					}
					for _, value := range readNameList {
						if value.nodeValue.Name != "" {
							values = append(values, value.nodeValue.Copy())
						}
					}
				}
				if len(values) == 0 {
					c.logger.Warn("opcua read no values")
					continue
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

func (c *UAClient) subscribe(nodes []*nodeValue) error {
	//return nil
	err := c.checkCollect()
	if err != nil {
		return err
	}
	needSubTimes := uint64(len(nodes)) / c.maxNodesPerBrowse
	for subTimes := uint64(0); subTimes < needSubTimes; subTimes++ {
		subHandle, err := newSubscription(c)
		if err != nil {
			// panic on create subscription error
			c.logger.WithError(err).Fatal("create subscription error")
		}
		subHandle.nodes = nodes[subTimes*c.maxNodesPerBrowse : (subTimes+1)*c.maxNodesPerBrowse]
		err = c.doSubBatch(subHandle, subHandle.nodes)
		if err != nil {
			c.logger.WithError(err).Error("subscribe error")
			return err
		}
	}
	if len(nodes)%int(c.maxNodesPerBrowse) != 0 {
		subHandle, err := newSubscription(c)
		if err != nil {
			// panic on create subscription error
			c.logger.WithError(err).Fatal("create subscription error")
		}
		subHandle.nodes = nodes[needSubTimes*c.maxNodesPerBrowse:]
		err = c.doSubBatch(subHandle, subHandle.nodes)
		if err != nil {
			c.logger.WithError(err).Error("subscribe error")
			return err
		}
	}
	c.logger.Info("add monitored items success")
	return err
}

func (c *UAClient) doSubBatch(subscriptionHandle *subscription, nodes []*nodeValue) error {
	c.logger.Info("start to add monitored items")
	maxOperations := uint(c.maxMonitoredItemsPerCall)
	subItemTimes := uint(len(nodes)) / maxOperations

	for i := uint(0); i < subItemTimes; i++ {
		indexBase := i * maxOperations
		subNodes := nodes[indexBase : indexBase+maxOperations]
		//ignore error
		c.doSubItems(subscriptionHandle, subNodes)
	}
	if len(nodes)%int(maxOperations) != 0 {
		indexBase := subItemTimes * maxOperations
		subNodes := nodes[indexBase:]
		//ignore error
		c.doSubItems(subscriptionHandle, subNodes)
	}
	subscriptionHandle.handleSubCallback()
	return nil
}

func (c *UAClient) doSubItems(sub *subscription, nodes []*nodeValue) error {
	reqs := make([]*ua.MonitoredItemCreateRequest, 0, len(nodes))
	for _, node := range nodes {
		if node.subscriptionID == nil {
			node.subscriptionID = &sub.subIndex
			node.clientHandle = sub.clientHandleIndex
			sub.clientHandleIndex += 1
		}
		reqs = append(reqs, opcua.NewMonitoredItemCreateRequestWithDefaults(node.nodeID, ua.AttributeIDValue, node.clientHandle))
	}
	resp, err := sub.sub.Monitor(c.ctx, ua.TimestampsToReturnBoth, reqs...)
	if err != nil {
		c.logger.WithError(err).Error("monitor error")
		return err
	}
	var errs []error
	for index, r := range resp.Results {
		if !errors.Is(r.StatusCode, ua.StatusOK) {
			errs = append(errs, fmt.Errorf("subscribe monitor for node %s failed: %w", nodes[index].nodeValue.IDStr, r.StatusCode))
		} else {
			nodes[index].subscribed = true
			nodes[index].monitoredItemID = r.MonitoredItemID
			sub.subCount += 1
		}
	}
	if len(errs) != 0 {
		err = errors.Join(errs...)
		c.logger.WithError(err).Error("subscribe monitor error")
		return err
	}
	return nil
}

func (s *subscription) handleSubCallback() {
	go func() {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			s.sub.Cancel(ctx)
		}()
		c := s.client
		var readNameList []*nodeValue
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Info("context done,handleSubCallback exit")
				return
			case <-c.closeChan:
				c.logger.Info("close chan,handleSubCallback exit")
				return
			case data := <-s.ch:
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
						node := s.nodes[handle]
						if item == nil || item.Value == nil {
							c.logger.WithField("identifier", node.nodeValue.IDStr).WithField("item", item).Error("observe opc ua item is nil")
							continue
						}
						if item.Value.Value != nil {
							node.nodeValue.Value = item.Value.Value.Value()
							if !node.nodeValue.ValueType.IsValid() {
								node.nodeValue.ValueType = convertType[item.Value.Value.Type()]
							}
						} else {
							node.nodeValue.Value = nil
						}
						var ts time.Time
						if !item.Value.SourceTimestamp.IsZero() {
							ts = item.Value.SourceTimestamp
						} else if !item.Value.ServerTimestamp.IsZero() {
							ts = item.Value.ServerTimestamp
						} else {
							ts = now
						}
						node.nodeValue.Timestamp = ts
						node.nodeValue.FinishTime = now
						node.nodeValue.StartTime = now
						node.nodeValue.Status = int64(item.Value.Status)
						if !errors.Is(item.Value.Status, ua.StatusOK) {
							c.logger.WithField("status", item.Value.Status).WithField("identifier", node.nodeValue.IDStr).Warn("read value status is not ok")
							if !c.containsBad {
								continue
							}
						}
						if node.nodeValue.Name == "" {
							readNameList = append(readNameList, node)
							continue
						}
						values = append(values, node.nodeValue.Copy())
					}
					if len(readNameList) != 0 {
						readNameStart := time.Now()
						c.readAllNames(readNameList)
						readNameEnd := time.Now()
						totalSpent := readNameEnd.Sub(readNameStart)
						if totalSpent > 3*time.Second {
							c.logger.WithField("spent", totalSpent).WithField("name", readNameEnd.Sub(readNameStart)).WithField("read_count", len(readNameList)).Warn("read name spend over 3 seconds")
						}
						for _, value := range readNameList {
							if value.nodeValue.Name != "" {
								values = append(values, value.nodeValue.Copy())
							}
						}
					}
					for _, value := range readNameList {
						if value.nodeValue.Name != "" {
							values = append(values, value.nodeValue.Copy())
						}
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

type TokenBucket struct {
	token chan struct{}
}

func NewTokenBucket(size int) *TokenBucket {
	token := make(chan struct{}, size)
	for i := 0; i < size; i++ {
		token <- struct{}{}
	}
	return &TokenBucket{
		token: token,
	}
}

func (t *TokenBucket) Get() {
	<-t.token
}

func (t *TokenBucket) Put() {
	t.token <- struct{}{}
}

func (c *UAClient) GetAllPoints(conf config.PointsConfig) ([]common.Point, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("opc ua client is nil")
	}
	if c.conn.State() != opcua.Connected {
		return nil, fmt.Errorf("opc ua client is not connected")
	}
	err := c.getServerLimit(false)
	if err != nil {
		return nil, fmt.Errorf("get server ability fail: %s", err.Error())
	}
	var reg regexp.Regexp
	var regName regexp.Regexp
	var regID regexp.Regexp
	if conf.Regex != "" {
		reg, err = regexp.Compile(conf.Regex)
		if err != nil {
			return nil, fmt.Errorf("invalid points regex: %w", err)
		}
	}
	if len(conf.RegexName) > 0 {
		regName, err = regexp.Compile(conf.RegexName)
		if err != nil {
			return nil, fmt.Errorf("invalid regex_name: %w", err)
		}
	}

	if len(conf.RegexID) > 0 {
		regID, err = regexp.Compile(conf.RegexID)
		if err != nil {
			return nil, fmt.Errorf("invalid regex_id: %w", err)
		}
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
	bucket := NewTokenBucket(runtime.NumCPU() * 2)
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
				bucket.Get()
				defer bucket.Put()
				points := c.getPoints(ctx, c.conn, bfsList[i*maxNodePerGetPoints:(i+1)*maxNodePerGetPoints], reg, regName, regID, nsMap)
				availablePoints[i] = points
			}(i)
		}
		if more {
			go func() {
				defer wg.Done()
				points := c.getPoints(ctx, c.conn, bfsList[operation*maxNodePerGetPoints:], reg, regName, regID, nsMap)
				availablePoints[operation] = points
			}()
		}
		wg.Wait()
		wg.Add(nodeCount)
		for i, node := range bfsList {
			go func(index int, n *opcua.Node) {
				defer wg.Done()
				bucket.Get()
				defer bucket.Put()
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
					childID := child.String()
					if c.isKepServer {
						paths := strings.Split(childID, ".")
						if len(paths) > 1 && strings.HasPrefix(paths[len(paths)-1], "_") {
							continue
						}
					}
					//avoid nested loops
					_, ok := m.Load(childID)
					if !ok {
						bfsList = append(bfsList, child)
						m.Store(childID, struct{}{})
					}
				}
			}
		}
		sort.Slice(bfsList, func(i, j int) bool {
			return bfsList[i].ID.String() < bfsList[j].ID.String()
		})
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
	ua.AttributeIDDescription,
}

var attributeNames = []string{
	"NodeClass",
	"BrowseName",
	"Description",
}

func (c *UAClient) getPoints(ctx context.Context, conn *opcua.Client, ns []*opcua.Node, pointRegex, nameRegex, idRegex regexp.Regexp, nsMap map[uint16]struct{}) []*common.Point {
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
		// node class
		err = res.Results[index].Status
		if !errors.Is(err, ua.StatusOK) {
			c.logger.WithError(err).WithField("nodeID", nodes[i].ID.String()).Errorf("get node attribute %s error", attributeNames[0])
			continue
		}
		nodeClass := ua.NodeClass(res.Results[index].Value.Int())
		if nodeClass != ua.NodeClassVariable {
			continue
		}
		// Browse Name
		err = res.Results[index+1].Status
		if !errors.Is(err, ua.StatusOK) {
			c.logger.WithError(err).WithField("nodeID", nodes[i].ID.String()).Errorf("get node attribute %s error", attributeNames[1])
			continue
		}
		browseName := res.Results[index+1].Value.String()
		// get Description attribute
		var description string
		err = res.Results[index+2].Status
		// ignore get description error, some nodes may not have description
		if errors.Is(err, ua.StatusOK) {
			// success
			description = res.Results[index+2].Value.String()
		} else if !errors.Is(err, ua.StatusBadAttributeIDInvalid) {
			// log error if not BadAttributeIDInvalid
			c.logger.WithError(err).WithField("nodeID", nodes[i].ID.String()).Errorf("get node attribute %s error", attributeNames[2])
		}

		point := &common.Point{
			ID:          nodes[i].ID.String(),
			Name:        browseName,
			Description: description,
		}
		if (pointRegex != nil && !(pointRegex.MatchString(point.Name) || pointRegex.MatchString(point.ID))) ||
			(nameRegex != nil && !nameRegex.MatchString(point.Name)) ||
			(idRegex != nil && !idRegex.MatchString(point.ID)) {
			continue
		}
		result = append(result, point)
	}
	if len(result) > 0 {
		if c.isKepServer {
			// get KepServer point description
			c.getKepServerDescription(ctx, conn, result)
		}
	}
	return result
}

func (c *UAClient) getKepServerDescription(ctx context.Context, conn *opcua.Client, result []*common.Point) {
	// get {point}._Description
	reqIDList := make([]int, len(result))
	valueIDs := make([]*ua.ReadValueID, 0, len(result))
	reqIndex := 0
	for i := 0; i < len(result); i++ {
		nodeID := fmt.Sprintf("%s._Description", result[i].ID)
		descriptionID, err := ua.ParseNodeID(nodeID)
		if err != nil {
			c.logger.WithError(err).WithField("node", nodeID).Error("parse node id error")
			continue
		}
		valueID := &ua.ReadValueID{
			NodeID:      descriptionID,
			AttributeID: ua.AttributeIDValue,
		}
		reqIDList[reqIndex] = i
		reqIndex++
		valueIDs = append(valueIDs, valueID)
	}
	if len(valueIDs) > 0 {
		req := &ua.ReadRequest{NodesToRead: valueIDs}
		res, err := conn.Read(ctx, req)
		if err != nil {
			c.logger.WithError(err).Error("get points _Description error")
			return
		}
		if len(res.Results) != reqIndex {
			c.logger.Error("get points _Description response length not match request length")
			return
		}
		for index := 0; index < len(res.Results); index++ {
			err = res.Results[index].Status
			if !errors.Is(err, ua.StatusOK) {
				c.logger.WithError(err).WithField("nodeID", valueIDs[index].NodeID.String()).Error("get _Description resp status error")
				continue
			}
			descriptionStr := res.Results[index].Value.String()
			if descriptionStr != "" {
				resultID := reqIDList[index]
				result[resultID].Description = descriptionStr
			}
		}
	}
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
		if c.closeChan != nil {
			close(c.closeChan)
		}
		if c.conn != nil {
			c.doCloseConn(c.conn)
		}
		if c.dumper != nil {
			c.dumper.Close()
		}
	})
	return nil
}

func (c *UAClient) doCloseConn(conn *opcua.Client) {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*5)
	defer cancel()
	conn.Close(ctx)
}

func (c *UAClient) ChangeCollectConfig(conf config.CollectConfig) {
	// observe
	c.logger.Info("opcua start to change collect config")
	if c.collectMode != conf.Ua.CollectMode {
		c.logger.Error("collect mode not match")
		return
	}

	if c.collectMode == config.OpcUaObserveType {
		oldNodeMap := make(map[string]*nodeValue, len(c.nodes))
		for _, node := range c.nodes {
			oldNodeMap[node.nodeValue.IDStr] = node
		}

		newCacheNodes := make([]*nodeValue, 0, len(conf.Ua.Nodes))
		var needInitNodeIDs []*nodeValue
		for i := 0; i < len(conf.Ua.Nodes); i++ {
			node, err := ua.ParseNodeID(conf.Ua.Nodes[i].ID)
			if err != nil {
				c.logger.WithError(err).WithField("node", conf.Ua.Nodes[i].ID).Error("parse node id error")
				continue
			}
			oldNode := oldNodeMap[conf.Ua.Nodes[i].ID]
			if oldNode != nil {
				newCacheNodes = append(newCacheNodes, oldNode)
				delete(oldNodeMap, conf.Ua.Nodes[i].ID)
			} else {
				cache := &nodeValue{
					nodeID: node,
					nodeValue: &common.NodeValue{
						IDStr: conf.Ua.Nodes[i].ID,
					},
				}
				newCacheNodes = append(newCacheNodes, cache)
				needInitNodeIDs = append(needInitNodeIDs, cache)
				c.logger.Info("opcua add node:", conf.Ua.Nodes[i].ID)
			}
		}
		for s := range oldNodeMap {
			c.logger.Info("opcua remove node:", s)
		}
		c.readNameBatch(needInitNodeIDs)
		c.readValueBatch(needInitNodeIDs)
		c.observeChange <- newCacheNodes
	} else if c.collectMode == config.OpcUaSubscribeType {
		oldSubMap := make(map[string]*nodeValue, len(c.nodes))
		for _, node := range c.nodes {
			oldSubMap[node.nodeValue.IDStr] = node
		}

		var newSubNode []*nodeValue
		var reSubNode []*nodeValue
		needUnsubNode := map[int][]*nodeValue{}
		for i := 0; i < len(conf.Ua.Nodes); i++ {
			nodeID, err := ua.ParseNodeID(conf.Ua.Nodes[i].ID)
			if err != nil {
				c.logger.WithError(err).WithField("node", conf.Ua.Nodes[i].ID).Error("parse node id error")
				continue
			}
			if node, ok := oldSubMap[conf.Ua.Nodes[i].ID]; ok {
				if !node.subscribed {
					reSubNode = append(reSubNode, node)
				}
				delete(oldSubMap, conf.Ua.Nodes[i].ID)
				continue
			} else {
				newSubNode = append(newSubNode, &nodeValue{
					nodeID: nodeID,
					nodeValue: &common.NodeValue{
						IDStr: conf.Ua.Nodes[i].ID,
					},
				})
			}
		}
		if len(oldSubMap) > 0 {
			for _, v := range oldSubMap {
				subID := *v.subscriptionID
				needUnsubNode[subID] = append(needUnsubNode[subID], v)
			}
		}
		//unsubscribe
		for subscriptionID, monitoredNode := range needUnsubNode {
			var unsubRetryNode []*nodeValue

			var monitoredItemIDs []uint32
			var unsubscribeNode []*nodeValue
			for _, node := range monitoredNode {
				if !node.subscribed {
					continue
				}
				c.logger.Info("opcua unsubscribe node:", node.nodeValue.IDStr)
				unsubscribeNode = append(unsubscribeNode, node)
				monitoredItemIDs = append(monitoredItemIDs, node.monitoredItemID)
			}
			if len(monitoredItemIDs) > 0 {
				subscriber := c.subList[subscriptionID]
				resp, err := subscriber.sub.Unmonitor(c.ctx, monitoredItemIDs...)
				if err != nil {
					c.logger.WithError(err).Error("unsubscribe response error")
				}
				for index, r := range resp.Results {
					if !errors.Is(r, ua.StatusOK) {
						c.logger.WithError(r).WithField("nodeID", unsubscribeNode[index].nodeValue.IDStr).Error("unsubscribe error")
						unsubRetryNode = append(unsubRetryNode, unsubscribeNode[index])
					} else {
						c.logger.Info(unsubscribeNode[index].nodeValue.IDStr, "unsubscribe success")
						unsubscribeNode[index].subscribed = false
						subscriber.subCount -= 1
					}
				}
				for _, value := range unsubRetryNode {
					c.logger.WithField("nodeID", value.nodeValue.IDStr).Info("retry unsubscribe")
					resp, err := subscriber.sub.Unmonitor(c.ctx, value.monitoredItemID)
					if err != nil {
						c.logger.WithError(err).WithField("nodeID", value.nodeValue.IDStr).Error("retry unsubscribe response error")
						continue
					}
					if !errors.Is(resp.Results[0], ua.StatusOK) {
						c.logger.WithError(resp.Results[0]).WithField("nodeID", value.nodeValue.IDStr).Error("retry unsubscribe error")
					} else {
						c.logger.Info(value.nodeValue.IDStr, "unsubscribe success after retry")
						value.subscribed = false
						subscriber.subCount -= 1
					}
				}
			}
		}
		// reSubscribe nodes
		reSubscriptionNodes := make([][]*nodeValue, len(c.subList))
		if len(reSubNode) > 0 {
			for _, value := range reSubNode {
				if value.subscriptionID != nil {
					subscriptionID := *value.subscriptionID

					reSubscriptionNodes[subscriptionID] = append(reSubscriptionNodes[subscriptionID], value)
				}
			}
			for i, nodes := range reSubscriptionNodes {
				if len(nodes) == 0 {
					continue
				}
				subHandle := c.subList[i]
				for _, n := range nodes {
					c.logger.Info("opcua resubscribe node:", n.nodeValue.IDStr)
				}
				err := c.doSubBatch(subHandle, nodes)
				if err != nil {
					c.logger.WithError(err).Error("resubscribe error")
				}
			}
		}
		if len(newSubNode) > 0 {
			for _, n := range newSubNode {
				c.logger.Info("opcua subscribe new node:", n.nodeValue.IDStr)
			}
			// subscribe new nodes
			c.readNameBatch(newSubNode)
			c.nodes = append(c.nodes, newSubNode...)
			lastSubscription := c.subList[len(c.subList)-1]
			delta := int(c.maxNodesPerBrowse) - len(lastSubscription.nodes)
			if delta > 0 {
				if len(newSubNode) > delta {
					lastSubscription.nodes = append(lastSubscription.nodes, newSubNode[delta:]...)
					err := c.doSubBatch(lastSubscription, newSubNode[:delta])
					if err != nil {
						c.logger.WithError(err).Error("subscribe new delta 1 error")
					}
					newSubNode = newSubNode[delta:]
				} else {
					lastSubscription.nodes = append(lastSubscription.nodes, newSubNode...)
					err := c.doSubBatch(lastSubscription, newSubNode)
					if err != nil {
						c.logger.WithError(err).Error("subscribe new delta 2 error")
					}
					newSubNode = nil
				}
			}
			if len(newSubNode) != 0 {
				err := c.subscribe(newSubNode)
				if err != nil {
					c.logger.WithError(err).Error("subscribe new error")
				}
			}
		}
	}
}
