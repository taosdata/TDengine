package opcua

import (
	"collector/common"
	"container/list"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/sunpe/gobox/logger"
)

type uaNode struct {
	nodeID    *ua.NodeID
	name      string
	valueType common.ValueType
}

type reader struct {
	connectConfig  common.UaConnectConfig
	collectMode    string
	collectNodes   []common.NodeConfig
	client         *opcua.Client
	state          opcua.ConnState
	nodes          []*uaNode
	nodesToRead    []*ua.ReadValueID
	connectTimeout time.Duration
	requestTimeout time.Duration
	interval       time.Duration
	pointsLimit    int
	pointRegex     *regexp.Regexp
	done           chan struct{}
	debug          bool
	containsBad    bool
	mutex          sync.Mutex
	once           sync.Once
}

func newReader(debug bool, connectConfig common.UaConnectConfig, pointConfig common.PointsConfig, collectMode string, nodes []common.NodeConfig, interval int64, containsBad bool) (*reader, error) {
	r := &reader{
		connectConfig:  connectConfig,
		collectMode:    collectMode,
		collectNodes:   nodes,
		state:          opcua.Disconnected,
		interval:       time.Duration(interval) * time.Second,
		connectTimeout: time.Duration(connectConfig.ConnectTimeout) * time.Second,
		requestTimeout: time.Duration(connectConfig.RequestTimeout) * time.Second,
		pointsLimit:    pointConfig.Limit,
		done:           make(chan struct{}, 1),
		debug:          debug,
		containsBad:    containsBad,
	}
	if len(pointConfig.Regex) > 0 {
		reg, err := regexp.Compile(pointConfig.Regex)
		if err != nil {
			return nil, fmt.Errorf("invalid points regex: %w", err)
		}
		r.pointRegex = reg
	}
	if err := r.connect(context.Background()); err != nil {
		return nil, fmt.Errorf("connect error %v", err)
	}

	if err := r.initNodeMetricMapping(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *reader) connect(ctx context.Context) error {
	logger.Debug("## connect to opc ua server", "endpoint", r.connectConfig.Endpoint)

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.state == opcua.Connected {
		return nil
	}

	r.state = opcua.Connecting
	opts, err := r.setupOptions(ctx)
	if err != nil {
		logger.Error("## setup options error", "error", err)
		return err
	}

	if r.client != nil {
		logger.Warn("## Closing connection due to Connection already instantiated")
		if err = r.client.Close(); err != nil {
			logger.Error("## close connection error", "error", err)
		}
	}

	r.client = opcua.NewClient(r.connectConfig.Endpoint, opts...)

	timeoutCtx, cancel := context.WithTimeout(ctx, r.connectTimeout)
	defer cancel()
	if err := r.client.Connect(timeoutCtx); err != nil {
		r.state = opcua.Disconnected
		return fmt.Errorf("error in Client Connection: %w", err)
	}

	r.state = opcua.Connected
	logger.Info("## create reader %p and connected to opc ua server", "reader", r)
	return nil
}

func (r *reader) ensureConnected(ctx context.Context) error {
	logger.Debug("## ensure connected to opc ua server")
	if r.state == opcua.Disconnected {
		if err := r.connect(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (r *reader) stop(ctx context.Context) {
	r.once.Do(func() {
		defer close(r.done)
		if r.state == opcua.Disconnected {
			return
		}
		if r.client == nil {
			return
		}
		if err := r.client.CloseWithContext(ctx); err != nil {
			logger.Error("## close opc ua connection error", "error", err)
		}
		r.client = nil
		r.state = opcua.Disconnected
	})
}

// initNodeMetricMapping builds nodes from the configuration
func (r *reader) initNodeMetricMapping() error {
	ctx := context.Background()
	if err := r.ensureConnected(ctx); err != nil {
		return fmt.Errorf("init node metric mapping error %v", err)
	}

	existing := make(map[string]struct{}, len(r.collectNodes))
	for _, node := range r.collectNodes {
		if _, ok := existing[node.ID]; ok {
			continue
		}
		existing[node.ID] = struct{}{}

		vt, err := common.ValueTypeFromString(node.ValueType)
		if err != nil {
			return err
		}

		nid, err := ua.ParseNodeID(node.ID)
		if err != nil {
			return err
		}

		r.nodes = append(r.nodes, &uaNode{nodeID: nid, name: r.nodeToPoint(ctx, r.client.Node(nid)).Name, valueType: vt})
	}

	if err := r.registerNodes(); err != nil {
		return fmt.Errorf("register nodes error %v", err)
	}

	return nil
}

func (r *reader) registerNodes() error {
	if len(r.nodes) > 0 {
		nodeToRegister := make([]*ua.NodeID, len(r.nodes))
		for i, v := range r.nodes {
			nodeToRegister[i] = v.nodeID
		}
		regResp, err := r.client.RegisterNodes(&ua.RegisterNodesRequest{NodesToRegister: nodeToRegister})
		if err != nil {
			return fmt.Errorf("register node failed: %w", err)
		}
		nodesToRead := make([]*ua.ReadValueID, len(regResp.RegisteredNodeIDs))
		for i, v := range regResp.RegisteredNodeIDs {
			nodesToRead[i] = &ua.ReadValueID{NodeID: v}
		}
		r.nodesToRead = nodesToRead
	}
	return nil
}

func (r *reader) observe(ctx context.Context, ch chan *common.NodeValue) error {
	if len(r.collectNodes) == 0 {
		return errors.New("nodes to be observe is empty")
	}
	if err := r.ensureConnected(ctx); err != nil {
		return err
	}

	go func() {
		defer r.stop(ctx)
		notifyCtx, _ := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

		ticker := time.NewTicker(r.interval)
		checkConnTicker := time.NewTicker(10 * time.Second)

		defer ticker.Stop()
		defer checkConnTicker.Stop()

		for {
			select {
			case <-checkConnTicker.C:
				if r.state == opcua.Connected && !r.OpcConnected() {
					logger.Panic("## opc ua connection is not alive")
				}
			case <-r.done:
				return
			case <-notifyCtx.Done():
				return
			case <-ticker.C:
				values, err := r.observeValue(ctx)
				if err != nil {
					logger.Error("## observe metric error", "error", err)
					continue
				}

				for _, value := range values {
					ch <- value
				}
			}
		}

	}()

	return nil
}

func (r *reader) observeValue(ctx context.Context) ([]*common.NodeValue, error) {
	res, err := r.client.ReadWithContext(
		ctx,
		&ua.ReadRequest{
			MaxAge:             2000,
			TimestampsToReturn: ua.TimestampsToReturnBoth,
			NodesToRead:        r.nodesToRead,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("observe failed: %w", err)
	}

	values := make([]*common.NodeValue, 0, len(res.Results))
	for i, item := range res.Results {
		node := r.nodes[i]
		identifier := node.nodeID.String()
		logger.DebugF("## observe opc ua identifier [%s] value [%v] type [%v]", identifier, item.Value.Value(),
			item.Value.Type())

		if item.Status != ua.StatusOK && !r.containsBad {
			logger.WarnF("## observe data for identifier [%q] status [%v] is not ok(0x0) ", identifier, item.Status)
			continue
		}

		if err = r.checkValueType(identifier, item, node.valueType); err != nil {
			logger.ErrorF("## check value type for identifier [%q] error [%v]", identifier, err)
			continue
		}

		var ts time.Time
		if !item.SourceTimestamp.IsZero() {
			ts = item.SourceTimestamp
		} else if !item.ServerTimestamp.IsZero() {
			ts = item.ServerTimestamp
		} else {
			ts = time.Now()
		}

		values = append(values, &common.NodeValue{
			Identifier: identifier,
			Name:       node.name,
			Timestamp:  ts,
			Now:        time.Now(),
			Value:      item.Value.Value(),
			ValueType:  node.valueType,
			Status:     int64(item.Status),
		})
	}

	return values, nil
}

func (r *reader) subscribe(ctx context.Context, ch chan *common.NodeValue) error {
	if len(r.collectNodes) == 0 {
		return errors.New("nodes to be subscribed is empty")
	}
	if err := r.ensureConnected(ctx); err != nil {
		return err
	}

	sub, subscribeCh, err := r.subscribeNodes(ctx)
	if err != nil {
		return fmt.Errorf("subscribe nodes failed: %w", err)
	}

	go func() {
		defer r.stop(ctx)
		notifyCtx, _ := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

		defer func() {
			if sub == nil {
				return
			}
			cancelCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_ = sub.Cancel(cancelCtx)

			logger.Warn("## cancel subscription")
		}()

		checkConnTicker := time.NewTicker(10 * time.Second)
		defer checkConnTicker.Stop()

		for {
			select {
			case <-checkConnTicker.C:
				if !r.OpcConnected() {
					logger.Panic("## opc ua connection is not alive")
				}
			case <-r.done:
				return
			case <-notifyCtx.Done():
				return
			case value, ok := <-subscribeCh:
				if !ok {
					continue
				}

				if value.Error != nil {
					logger.Error("## subscribe error", "error", value.Error)
					continue
				}

				v, ok := value.Value.(*ua.DataChangeNotification)
				if !ok {
					logger.WarnF("what's this publish result? %#v", value)
					continue
				}
				if r.debug {
					j, _ := json.Marshal(v)
					logger.DebugF("## subscribe from opc ua %s", string(j))
				}
				for _, item := range v.MonitoredItems {
					var ts time.Time
					if !item.Value.SourceTimestamp.IsZero() {
						ts = item.Value.SourceTimestamp
					} else if !item.Value.ServerTimestamp.IsZero() {
						ts = item.Value.ServerTimestamp
					} else {
						ts = time.Now()
					}

					if uint64(item.ClientHandle) > uint64(len(r.nodes)) {
						continue
					}
					node := r.nodes[item.ClientHandle]
					id := node.nodeID.String()

					status := item.Value.Status
					if status != ua.StatusOK && !r.containsBad {
						logger.WarnF("## observe data for identifier [%q] status [%v] is not ok(0x0) ", id, status)
						continue
					}

					ch <- &common.NodeValue{
						Identifier: id,
						Name:       node.name,
						Timestamp:  ts,
						Now:        time.Now(),
						Value:      item.Value.Value.Value(),
						ValueType:  node.valueType,
						Status:     int64(status),
					}
				}
			}
		}

	}()

	return nil
}

func (r *reader) subscribeNodes(ctx context.Context) (sub *opcua.Subscription, ch chan *opcua.PublishNotificationData, err error) {
	ch = make(chan *opcua.PublishNotificationData, 1)
	sub, err = r.client.SubscribeWithContext(ctx, &opcua.SubscriptionParameters{}, ch)
	if err != nil {
		logger.Error("## subscribe failed", "error", err)
		return nil, nil, fmt.Errorf("subscribe failed: %w", err)
	}

	for i, node := range r.nodes {
		if _, err = sub.Monitor(ua.TimestampsToReturnBoth, opcua.NewMonitoredItemCreateRequestWithDefaults(
			node.nodeID, ua.AttributeIDValue, uint32(i))); err != nil {
			logger.Error("## subscribe monitor failed", "error", err)
			return nil, nil, fmt.Errorf("subscribe monitor failed: %w", err)
		}
	}
	return
}

func (r *reader) OpcConnected() bool {
	return r.client.State() == opcua.Connected
}

var types = map[ua.TypeID][]common.ValueType{
	ua.TypeIDBoolean:  {common.BOOL},
	ua.TypeIDSByte:    {common.TINYINT, common.SMALLINT, common.INT, common.BIGINT},
	ua.TypeIDByte:     {common.TINYINT, common.SMALLINT, common.INT, common.BIGINT},
	ua.TypeIDInt16:    {common.SMALLINT, common.INT, common.BIGINT},
	ua.TypeIDUint16:   {common.SMALLINTUNSIGNED, common.INT, common.INTUNSIGNED, common.BIGINT, common.BIGINTUNSIGNED},
	ua.TypeIDInt32:    {common.INT, common.BIGINT},
	ua.TypeIDUint32:   {common.INTUNSIGNED, common.BIGINT, common.BIGINTUNSIGNED},
	ua.TypeIDInt64:    {common.BIGINT},
	ua.TypeIDUint64:   {common.BIGINTUNSIGNED},
	ua.TypeIDFloat:    {common.FLOAT, common.DOUBLE},
	ua.TypeIDDouble:   {common.DOUBLE},
	ua.TypeIDString:   {common.BINARY, common.NCHAR, common.JSON, common.VARCHAR},
	ua.TypeIDDateTime: {common.TIMESTAMP},
}

func (r *reader) checkValueType(identifier string, value *ua.DataValue, nodeType common.ValueType) error {
	valueTypes, ok := types[value.Value.Type()]
	if !ok {
		return fmt.Errorf("unsupported value type for %s for opcua reader %s", identifier, value.Value.Type().String())
	}

	if !common.InSlice[common.ValueType](nodeType, valueTypes) {
		return fmt.Errorf("%s value type unmatch. expect %s, but get %s", identifier, nodeType.String(),
			value.Value.Type().String())
	}
	return nil
}

func (r *reader) setupOptions(ctx context.Context) (opts []opcua.Option, err error) {
	ctx, cancel := context.WithTimeout(ctx, r.connectTimeout)
	defer cancel()

	// Get a list of the endpoints for target server
	endpoints, err := opcua.GetEndpoints(ctx, r.connectConfig.Endpoint)
	if err != nil {
		return nil, err
	}
	return r.generateOptions(endpoints)
}

func (r *reader) generateOptions(endpoints []*ua.EndpointDescription) (opts []opcua.Option, err error) {
	// ApplicationURI is automatically observe from the cert so is not required if a cert is provided
	opts = append(opts, opcua.ApplicationURI("urn:taosx:gopcua:client"))
	opts = append(opts, opcua.ApplicationName("taosx"))
	opts = append(opts, opcua.RequestTimeout(r.requestTimeout))

	// certificate and private key
	var cert []byte
	if len(r.connectConfig.Certificate) != 0 && len(r.connectConfig.PrivateKey) != 0 {
		certificate, err := tls.LoadX509KeyPair(r.connectConfig.Certificate, r.connectConfig.PrivateKey)
		if err != nil {
			logger.Error("## Failed to load certificate ", "error", err)
			return nil, err
		}

		privateKey, ok := certificate.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("invalid private key")
		}
		cert = certificate.Certificate[0]
		opts = append(opts, opcua.PrivateKey(privateKey), opcua.Certificate(cert))
	}

	// Select the most appropriate authentication mode from server capabilities and user input
	authMode, authOption := r.authOptions(r.connectConfig.AuthMethod, cert, r.connectConfig.Username,
		r.connectConfig.Password)
	opts = append(opts, authOption)

	securityPolity := r.getSecurityPolicy()
	securityMode := r.getSecurityMode()

	// Allow input of only one of sec-mode,sec-policy when choosing 'None'
	if securityMode == ua.MessageSecurityModeNone || securityPolity == ua.SecurityPolicyURINone {
		securityMode = ua.MessageSecurityModeNone
		securityPolity = ua.SecurityPolicyURINone
	}

	serverEndpoint, err := r.getServerEndpoint(endpoints, securityPolity, securityMode)
	if err != nil {
		logger.Error("## get server endpoint error ", "error", err)
		return nil, err
	}

	opts = append(opts, opcua.SecurityFromEndpoint(serverEndpoint, authMode))
	return
}

func (r *reader) getSecurityPolicy() string {
	if strings.HasPrefix(r.connectConfig.SecurityPolicy, ua.SecurityPolicyURIPrefix) {
		return r.connectConfig.SecurityPolicy
	}

	return ua.SecurityPolicyURIPrefix + r.connectConfig.SecurityPolicy
}

func (r *reader) getSecurityMode() ua.MessageSecurityMode {
	return ua.MessageSecurityModeFromString(r.connectConfig.SecurityMode)
}

func (r *reader) getServerEndpoint(endpoints []*ua.EndpointDescription, securityPolicy string, securityMode ua.MessageSecurityMode) (endpoint *ua.EndpointDescription, err error) {
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

func (r *reader) authOptions(authMode string, cert []byte, username, password string) (token ua.UserTokenType, option opcua.Option) {
	if authMode == "anonymous" {
		token = ua.UserTokenTypeAnonymous
		option = opcua.AuthAnonymous()
		return
	}

	if authMode == "username" {
		token = ua.UserTokenTypeUserName
		option = opcua.AuthUsername(username, password)
		return
	}

	if authMode == "certificate" {
		token = ua.UserTokenTypeCertificate
		option = opcua.AuthCertificate(cert)
		return
	}

	token = ua.UserTokenTypeAnonymous
	option = opcua.AuthAnonymous()

	return token, option
}

func (r *reader) getAllNodes(ctx context.Context) (nodes []common.Point, err error) {
	if err = r.ensureConnected(ctx); err != nil {
		return nil, err
	}

	rootId, err := ua.ParseNodeID("i=84") // root node
	if err != nil {
		return nil, err
	}
	rootNode := r.client.Node(rootId)
	nodes, err = r.browse(ctx, rootNode)
	return
}

func (r *reader) browse(ctx context.Context, root *opcua.Node) (points []common.Point, err error) {
	l := list.New()
	l.PushBack(root)

	pointMap := make(map[string]common.Point)

BK:
	for {
		front := l.Front()
		if front == nil { // no more nodes
			break
		}

		node := l.Remove(front).(*opcua.Node)
		leaves, nodes, err := r.browseChildrenNode(ctx, node)
		if err != nil {
			return nil, fmt.Errorf("get child for node %s error %v", root.String(), err)
		}

		for _, n := range leaves {
			point := r.nodeToPoint(ctx, n)
			if r.pointRegex != nil && !r.pointRegex.MatchString(point.Name) {
				continue
			}

			pointMap[point.ID] = point
			if r.pointsLimit > 0 && len(pointMap) >= r.pointsLimit {
				break BK
			}
		}

		for _, n := range nodes {
			l.PushBack(n)
		}
	}

	points = make([]common.Point, 0, len(pointMap))
	for _, point := range pointMap {
		points = append(points, point)
	}

	return
}

func (r *reader) browseChildrenNode(ctx context.Context, node *opcua.Node) (leaves []*opcua.Node, nodes []*opcua.Node, err error) {
	childrenNodes, err := node.ChildrenWithContext(ctx, 0, ua.NodeClassAll)
	if err != nil {
		return nil, nil, fmt.Errorf("get child for node %s error %v", node.String(), err)
	}

	for _, child := range childrenNodes {
		nodeClass, err := child.NodeClassWithContext(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("get node class for node %s error %v", child.String(), err)
		}
		if nodeClass == ua.NodeClassVariable {
			leaves = append(leaves, child)
		}
		nodes = append(nodes, child)
	}

	return
}

func (r *reader) nodeToPoint(ctx context.Context, node *opcua.Node) common.Point {
	var name string
	if browseName, err := node.BrowseNameWithContext(ctx); err == nil {
		name = browseName.Name
	}

	return common.Point{ID: node.String(), Name: name}
}
