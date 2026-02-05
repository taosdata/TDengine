package opcua

import (
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

	"collector/client"
	"collector/common"
	"collector/config"
	"collector/log"
	"collector/types"

	"collector/regexp"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	"github.com/sirupsen/logrus"
)

const ObjectRootID = "i=85"

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

	autoReconnect bool

	getPointsCache sync.Map
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
		autoReconnect:            connectConfig.GetAutoReconnect(),
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
	opts = append(opts, opcua.AutoReconnect(connectConfig.GetAutoReconnect()))

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
	if !c.autoReconnect {
		c.logger.Fatal("reconnect disabled, skip reconnect, exit")
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
		connectStatusTicker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-connectStatusTicker.C:
				if c.autoReconnect {
					continue
				}
				if c.conn.State() != opcua.Connected {
					c.logger.Fatal("connection not connected, auto reconnect disabled, exit")
				}
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

type bfsListElement struct {
	node        *opcua.Node
	id          string
	browseName  string
	displayName string
	parentID    string
	path        string
	nodeClass   ua.NodeClass
}

func (c *UAClient) GetAllPoints(conf config.PointsConfig) ([]*common.Point, error) {
	points, err := c.getCollectPoints(conf)
	if err != nil {
		return nil, err
	}
	var result []*common.Point
	for _, point := range points {
		result = c.AppendParent(point, result)
	}
	return result, nil
}

func (c *UAClient) AppendParent(point *common.Point, result []*common.Point) []*common.Point {
	for {
		result = append(result, point)
		val, ok := c.getPointsCache.LoadAndDelete(point.ParentID)
		if !ok {
			// parent not exists
			return result
		}
		parentPoint := val.(*common.Point)
		point = parentPoint
	}
}

func escapePathName(name string) string {
	return strings.ReplaceAll(name, ".", `\.`)
}

func (c *UAClient) getCollectPoints(conf config.PointsConfig) ([]*common.Point, error) {
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
	rootIDStr := conf.Ua.Root
	rootId, err := ua.ParseNodeID(rootIDStr) // objects node
	if err != nil {
		return nil, err
	}
	nss := conf.Ua.Namespaces
	nsMap := make(map[uint16]struct{}, len(nss))
	for _, ns := range nss {
		nsMap[ns] = struct{}{}
	}
	rootNode := c.conn.Node(rootId)
	isObjectRoot := rootIDStr == ObjectRootID
	ctx := c.ctx
	// BFS list of nodes to traverse at the current level
	rootElement := &bfsListElement{
		node:     rootNode,
		parentID: "",
		id:       rootIDStr,
	}
	rootAttributes, err := rootNode.Attributes(
		ctx,
		ua.AttributeIDNodeClass,   // to get node class
		ua.AttributeIDBrowseName,  // to get browse name
		ua.AttributeIDDisplayName, // to get display name
		ua.AttributeIDDescription, // to get description
	)
	if err != nil {
		return nil, fmt.Errorf("get root node attributes error: %w", err)
	}
	if !errors.Is(rootAttributes[0].Status, ua.StatusOK) {
		return nil, fmt.Errorf("get root node node class attributes error: %s", rootAttributes[0].Status)
	}
	rootElement.nodeClass = ua.NodeClass(rootAttributes[0].Value.Int())
	if !errors.Is(rootAttributes[1].Status, ua.StatusOK) {
		return nil, fmt.Errorf("get root node browse name attributes error: %s", rootAttributes[1].Status)
	}
	rootElement.browseName = rootAttributes[1].Value.String()
	if !errors.Is(rootAttributes[2].Status, ua.StatusOK) {
		return nil, fmt.Errorf("get root node display name attributes error: %s", rootAttributes[2].Status)
	}
	rootElement.displayName = rootAttributes[2].Value.String()
	description := ""
	if !errors.Is(rootAttributes[3].Status, ua.StatusOK) {
		c.logger.WithField("status", rootAttributes[3].Status).Warn("get root node description attributes error")
	}
	rootElement.path = escapePathName(rootElement.displayName)
	point := &common.Point{
		ID:          rootElement.id,
		Name:        rootElement.browseName,
		Description: description,
		DisplayName: rootElement.displayName,
		NodeType:    nodeClassNames[rootElement.nodeClass],
		ParentID:    "",
		Path:        rootElement.path,
		IsStatic:    true,
	}
	c.getPointsCache.Store(rootIDStr, point)

	bfsList := []*bfsListElement{
		rootElement,
	}
	var result []*common.Point
	m := sync.Map{}
	wg := sync.WaitGroup{}
	bucket := NewTokenBucket(runtime.NumCPU() * 2)

	// each get points operation max points. Each point has len(attributes) attributes
	maxNodePerGetPoints := 1
	if int(c.maxNodesPerRead) > len(attributes) {
		maxNodePerGetPoints = int(c.maxNodesPerRead) / len(attributes)
	}

	// bfs
	for {
		if len(bfsList) == 0 {
			break
		}
		nodeCount := len(bfsList)
		// each goroutine get children for one node
		//childrenChannels := make(chan *childrenResp, nodeCount)
		// calculate get points operation times. total points / max points per get
		operation := nodeCount / maxNodePerGetPoints
		getTimes := operation

		more := false
		// still have remaining nodes to get attributes
		if nodeCount%maxNodePerGetPoints != 0 {
			more = true
			getTimes += 1
		}
		start := time.Now()
		wg.Add(getTimes)
		availablePoints := make([][]*common.Point, getTimes)
		for i := 0; i < operation; i++ {
			go func(i int) {
				defer wg.Done()
				bucket.Get()
				defer bucket.Put()
				points := c.getPointsAttribute(ctx, c.conn, bfsList[i*maxNodePerGetPoints:(i+1)*maxNodePerGetPoints], reg, regName, regID, nsMap)
				availablePoints[i] = points
			}(i)
		}
		// handle remaining nodes
		if more {
			go func() {
				defer wg.Done()
				points := c.getPointsAttribute(ctx, c.conn, bfsList[operation*maxNodePerGetPoints:], reg, regName, regID, nsMap)
				availablePoints[operation] = points
			}()
		}
		wg.Wait()
		c.logger.Debugf("get points attribute spend %d ms", time.Since(start).Milliseconds())
		// append available points to result
		for _, points := range availablePoints {
			if points != nil {
				for _, point := range points {
					result = append(result, point)
					if conf.Limit > 0 && len(result) >= conf.Limit {
						return result, nil
					}
				}
			}
		}
		start = time.Now()
		children, err := c.getChildrenByList(ctx, bfsList)
		c.logger.Debugf("get children by list spend %d ms", time.Since(start).Milliseconds())
		if err != nil {
			break
		}

		// Clear the current level's node list to prepare for storing the next level's nodes
		bfsList = bfsList[:0]
		for _, child := range children {
			childID := child.id
			if isObjectRoot {
				// ignore Server and Aliases
				if childID == "i=2253" || childID == "i=23470" {
					continue
				}
			}
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
		isObjectRoot = false
		sort.Slice(bfsList, func(i, j int) bool {
			return bfsList[i].node.ID.String() < bfsList[j].node.ID.String()
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
	ua.AttributeIDDescription,
}

var attributeNames = []string{
	"Description",
}

var nodeClassNames = []string{
	ua.NodeClassObject:   "Object",
	ua.NodeClassVariable: "Variable",
}

func (c *UAClient) getPointsAttribute(ctx context.Context, conn *opcua.Client, ns []*bfsListElement, pointRegex, nameRegex, idRegex regexp.Regexp, nsMap map[uint16]struct{}) []*common.Point {
	nodes := make([]*bfsListElement, 0, len(ns))
	for i := 0; i < len(ns); i++ {
		if len(nsMap) == 0 {
			if ns[i].node.ID.Namespace() == 0 {
				continue
			}
		} else {
			if _, ok := nsMap[ns[i].node.ID.Namespace()]; !ok {
				continue
			}
		}
		nodes = append(nodes, ns[i])
	}
	req := &ua.ReadRequest{NodesToRead: make([]*ua.ReadValueID, 0, len(nodes)*len(attributes))}
	for _, node := range nodes {
		for i := 0; i < len(attributes); i++ {
			req.NodesToRead = append(req.NodesToRead, &ua.ReadValueID{
				NodeID:      node.node.ID,
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
		parent := nodes[i].parentID
		nodeID := nodes[i].id
		path := nodes[i].path
		displayName := nodes[i].displayName
		browseName := nodes[i].browseName
		// node class
		nodeType := nodes[i].nodeClass
		if nodeType != ua.NodeClassVariable && nodeType != ua.NodeClassObject {
			continue
		}
		// get Description attribute
		description := ""
		err = res.Results[index].Status
		// ignore get description error, some nodes may not have description
		if errors.Is(err, ua.StatusOK) {
			// success
			if res.Results[index].Value != nil {
				description = res.Results[index].Value.String()
			}
		} else if !errors.Is(err, ua.StatusBadAttributeIDInvalid) {
			// log error if not BadAttributeIDInvalid
			c.logger.WithError(err).WithField("nodeID", nodeID).Errorf("get node attribute %s error", attributeNames[0])
		}

		point := &common.Point{
			ID:          nodeID,
			Name:        browseName,
			Description: description,
			DisplayName: displayName,
			NodeType:    nodeClassNames[nodeType],
			ParentID:    parent,
			Path:        path,
			IsStatic:    true,
		}
		c.getPointsCache.Store(nodeID, point)
		if (pointRegex != nil && !(pointRegex.MatchString(point.Name) || pointRegex.MatchString(point.ID))) ||
			(nameRegex != nil && !nameRegex.MatchString(point.Name)) ||
			(idRegex != nil && !idRegex.MatchString(point.ID)) {
			continue
		}
		if nodeType != ua.NodeClassVariable {
			continue
		}
		point.IsStatic = false
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

// Get child nodes for the given parent node
func getChildren(ctx context.Context, n *bfsListElement) ([]*bfsListElement, error) {
	var children []*bfsListElement
	parentID := n.node.ID.String()
	refs, err := n.node.ReferencedNodes(ctx, id.HierarchicalReferences, ua.BrowseDirectionForward, ua.NodeClassAll, true)
	if err != nil {
		return nil, fmt.Errorf("reference: %d: %s", id.HierarchicalReferences, err)
	}
	if len(refs) > 0 {
		for _, ref := range refs {
			children = append(children, &bfsListElement{
				node:     ref,
				parentID: parentID,
			})
		}
	}
	return children, nil
}

const refType = uint32(id.HierarchicalReferences)

var refTypeID = ua.NewNumericNodeID(0, refType)

const dir = ua.BrowseDirectionForward
const mask = uint32(ua.NodeClassObject + ua.NodeClassVariable)
const resultMask = uint32(ua.BrowseResultMaskAll)

func (c *UAClient) getChildrenByList(ctx context.Context, nodes []*bfsListElement) ([]*bfsListElement, error) {
	browseNodes := nodes
	batchTimes := len(browseNodes) / int(c.maxNodesPerBrowse)
	more := false
	if len(browseNodes)%int(c.maxNodesPerBrowse) != 0 {
		batchTimes += 1
		more = true
	}
	var children []*bfsListElement
	for i := 0; i < batchTimes; i++ {
		startIndex := i * int(c.maxNodesPerBrowse)
		endIndex := (i + 1) * int(c.maxNodesPerBrowse)
		if !more && i == batchTimes-1 {
			endIndex = len(browseNodes)
		} else if more && i == batchTimes-1 {
			endIndex = len(browseNodes)
		}
		result, err := c.doBrowse(ctx, browseNodes[startIndex:endIndex])
		if err != nil {
			return nil, err
		}
		refs := c.browseNext(ctx, result)
		for j, ref := range refs {
			if len(ref) > 0 {
				for _, refItem := range ref {
					browseName := ""
					if refItem.BrowseName != nil && refItem.BrowseName.Name != "" {
						browseName = refItem.BrowseName.Name
					}
					displayName := ""
					if refItem.DisplayName != nil && refItem.DisplayName.Text != "" {
						displayName = refItem.DisplayName.Text
					}
					nodeId := c.conn.NodeFromExpandedNodeID(refItem.NodeID)
					browseNode := browseNodes[startIndex+j]
					parentID := browseNode.id
					path := browseNode.path + "." + escapePathName(displayName)

					children = append(children, &bfsListElement{
						node:        nodeId,
						id:          nodeId.String(),
						browseName:  browseName,
						displayName: displayName,
						nodeClass:   refItem.NodeClass,
						parentID:    parentID,
						path:        path,
					})
				}
			}
		}
	}
	return children, nil
}

func (c *UAClient) doBrowse(ctx context.Context, nodes []*bfsListElement) ([]*ua.BrowseResult, error) {
	browseDesc := make([]*ua.BrowseDescription, 0, len(nodes))
	for _, node := range nodes {
		n := node.node
		desc := &ua.BrowseDescription{
			NodeID:          n.ID,
			BrowseDirection: dir,
			ReferenceTypeID: refTypeID,
			IncludeSubtypes: true,
			NodeClassMask:   mask,
			ResultMask:      resultMask,
		}
		browseDesc = append(browseDesc, desc)
	}
	req := &ua.BrowseRequest{
		View: &ua.ViewDescription{
			ViewID: ua.NewTwoByteNodeID(0),
		},
		RequestedMaxReferencesPerNode: 0,
		NodesToBrowse:                 browseDesc,
	}
	resp, err := c.conn.Browse(ctx, req)
	if err != nil {
		c.logger.WithError(err).Error("browse error")
		return nil, fmt.Errorf("browse nodes error: %w", err)
	}
	return resp.Results, nil
}

func (c *UAClient) browseNext(ctx context.Context, results []*ua.BrowseResult) [][]*ua.ReferenceDescription {
	refResults := make([][]*ua.ReferenceDescription, len(results))
	for i, result := range results {
		refResults[i] = result.References
	}
	browseResults := results
	var refs [][]*ua.ReferenceDescription
	for {
		browseResults, refs = c.doBrowseNext(ctx, browseResults)
		if refs == nil {
			break
		}
		for i := 0; i < len(refs); i++ {
			if len(refs[i]) > 0 {
				refResults[i] = append(refResults[i], refs[i]...)
			}
		}
	}
	return refResults
}

func (c *UAClient) doBrowseNext(ctx context.Context, result []*ua.BrowseResult) ([]*ua.BrowseResult, [][]*ua.ReferenceDescription) {
	var continuationPoints [][]byte
	hasContinuationPoint := make([]bool, len(result))
	resultDescriptions := make([][]*ua.ReferenceDescription, len(result))
	resultBrowse := make([]*ua.BrowseResult, len(result))
	for i, res := range result {
		if res != nil && len(res.ContinuationPoint) > 0 {
			hasContinuationPoint[i] = true
			continuationPoints = append(continuationPoints, res.ContinuationPoint)
		}
	}
	if len(continuationPoints) == 0 {
		return nil, nil
	}
	req := &ua.BrowseNextRequest{
		ContinuationPoints:        continuationPoints,
		ReleaseContinuationPoints: false,
	}
	resp, err := c.conn.BrowseNext(ctx, req)
	if err != nil {
		c.logger.WithError(err).Error("browse next error")
		return nil, nil
	}
	resultIndex := 0
	for i := 0; i < len(result); i++ {
		if hasContinuationPoint[i] {
			res := resp.Results[resultIndex]
			resultIndex++
			if !errors.Is(res.StatusCode, ua.StatusOK) {
				c.logger.WithError(err).Error("browse next response status error")
				continue
			}
			resultBrowse[i] = res
			resultDescriptions[i] = res.References
		}
	}
	return resultBrowse, resultDescriptions
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
