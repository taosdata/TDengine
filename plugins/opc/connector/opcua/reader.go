package opcua

import (
	"collector/common"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

type reader struct {
	connectConfig  common.UaConnectConfig
	collectConfig  common.UaCollectConfig
	client         *opcua.Client
	state          opcua.ConnState
	nodes          []*ua.NodeID
	nodeTypes      []common.ValueType
	nodesToRead    []*ua.ReadValueID
	connectTimeout time.Duration
	requestTimeout time.Duration
	interval       time.Duration
	done           chan struct{}
	debug          bool
	mutex          sync.Mutex
}

func newReader(config common.Config) (*reader, error) {
	if err := config.Connect.Ua.Validate(); err != nil {
		return nil, fmt.Errorf("validate connection collectConfig fail. %v", err)
	}

	if config.Collect.Interval <= 0 {
		config.Collect.Interval = 1
	}

	r := &reader{
		connectConfig:  config.Connect.Ua,
		collectConfig:  config.Collect.Ua,
		state:          opcua.Disconnected,
		interval:       time.Duration(config.Collect.Interval) * time.Second,
		connectTimeout: time.Duration(config.Connect.Ua.ConnectTimeout) * time.Second,
		requestTimeout: time.Duration(config.Connect.Ua.RequestTimeout) * time.Second,
		done:           make(chan struct{}, 1),
		debug:          config.Debug,
	}
	if err := r.initNodeMetricMapping(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *reader) connect(ctx context.Context) error {
	if r.debug {
		log.Println("## connect to opc ua server", "endpoint", r.connectConfig.Endpoint)
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.state = opcua.Connecting
	opts, err := r.setupOptions(ctx)
	if err != nil {
		log.Println("## setup options error ", err)
		return err
	}

	if r.client != nil {
		log.Println("## Closing connection due to Connection already instantiated")
		if err := r.client.Close(); err != nil {
			log.Println("## close connection error ", err)
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
	log.Println("## connected to opc ua server")

	if len(r.nodes) > 0 {
		//regResp, err := r.client.RegisterNodes(&ua.RegisterNodesRequest{NodesToRegister: r.nodes})
		//if err != nil {
		//	return fmt.Errorf("register node failed: %w", err)
		//}
		//
		//nodesToRead := make([]*ua.ReadValueID, len(regResp.RegisteredNodeIDs))
		//for i, v := range regResp.RegisteredNodeIDs {
		//	nodesToRead[i] = &ua.ReadValueID{NodeID: v}
		//}
		nodesToRead := make([]*ua.ReadValueID, len(r.nodes))
		for i, v := range r.nodes {
			nodesToRead[i] = &ua.ReadValueID{NodeID: v}
		}
		r.nodesToRead = nodesToRead
	}
	return nil
}

func (r *reader) ensureConnected(ctx context.Context) error {
	if r.debug {
		log.Println("## ensure connected to opc ua server")
	}
	if r.state == opcua.Disconnected {
		if err := r.connect(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (r *reader) stop(ctx context.Context) {
	defer close(r.done)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if err := r.client.CloseWithContext(ctx); err != nil {
		log.Println("## close opc ua connection error", err)
	}
	r.client = nil
	r.state = opcua.Disconnected
}

// initNodeMetricMapping builds nodes from the configuration
func (r *reader) initNodeMetricMapping() error {
	existing := make(map[string]struct{}, len(r.collectConfig.Nodes))
	for _, node := range r.collectConfig.Nodes {
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

		r.nodes = append(r.nodes, nid)
		r.nodeTypes = append(r.nodeTypes, vt)
	}

	return nil
}

func (r *reader) read(ctx context.Context) (<-chan *common.NodeValue, error) {
	if err := r.collectConfig.Validate(); err != nil { // check collect config before collect
		return nil, fmt.Errorf("validate reader collectConfig fail. %v", err)
	}
	if err := r.ensureConnected(ctx); err != nil {
		return nil, err
	}
	ch := make(chan *common.NodeValue, len(r.nodes))

	go func() {
		defer close(ch)
		notifyCtx, _ := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

		ticker := time.NewTicker(r.interval)
		checkConnTicker := time.NewTicker(10 * time.Second)

		defer ticker.Stop()
		defer checkConnTicker.Stop()

		for {
			select {
			case <-checkConnTicker.C:
				if !r.OpcConnected() {
					log.Println("## opc ua connection is not alive")
					return
				}
			case <-r.done:
				return
			case <-notifyCtx.Done():
				return
			case <-ticker.C:
				values, err := r.readValue(ctx)
				if err != nil {
					log.Println("## read metric error ", err)
					continue
				}

				for _, value := range values {
					ch <- value
				}
			}
		}

	}()

	return ch, nil
}

func (r *reader) readValue(ctx context.Context) ([]*common.NodeValue, error) {
	res, err := r.client.ReadWithContext(
		ctx,
		&ua.ReadRequest{
			MaxAge:             2000,
			TimestampsToReturn: ua.TimestampsToReturnBoth,
			NodesToRead:        r.nodesToRead,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	values := make([]*common.NodeValue, 0, len(res.Results))
	for i, value := range res.Results {
		identifier := r.nodes[i].String()
		if value.Status != ua.StatusOK {
			log.Printf("## read data for identifier [%q] status [%v] is not ok(0x0) ", identifier, value.Status)
			continue
		}

		if r.debug {
			log.Printf("## read opc ua identifier [%s] value [%v] type [%v]", identifier,
				value.Value.Value(), value.Value.Type())
		}

		if err = r.checkValueType(identifier, value, r.nodeTypes[i]); err != nil {
			return nil, err
		}

		values = append(values, &common.NodeValue{
			Identifier: identifier,
			Timestamp:  time.Now(), // TD-23826, ts for read mod is time.Now()
			Value:      value.Value.Value(),
			ValueType:  r.nodeTypes[i],
		})
	}

	return values, nil
}

func (r *reader) subscribe(ctx context.Context) (<-chan *common.NodeValue, error) {
	if err := r.collectConfig.Validate(); err != nil { // check collect config before collect
		return nil, fmt.Errorf("validate reader collectConfig fail. %v", err)
	}
	if err := r.ensureConnected(ctx); err != nil {
		return nil, err
	}

	sub, subscribeCh, err := r.subscribeNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("subscribe nodes failed: %w", err)
	}

	ch := make(chan *common.NodeValue, len(r.nodes))
	go func() {
		notifyCtx, _ := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

		defer close(ch)
		defer func() {
			if sub == nil {
				return
			}
			cancelCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_ = sub.Cancel(cancelCtx)

			log.Println("## cancel subscription")
		}()

		checkConnTicker := time.NewTicker(10 * time.Second)
		defer checkConnTicker.Stop()

		for {
			select {
			case <-checkConnTicker.C:
				if !r.OpcConnected() {
					log.Println("## opc ua connection is not alive")
					return
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
					log.Println("## subscribe error", value.Error)
					continue
				}

				v, ok := value.Value.(*ua.DataChangeNotification)
				if !ok {
					log.Printf("what's this publish result? %#v", value)
					continue
				}
				if r.debug {
					j, _ := json.Marshal(v)
					log.Println("## subscribe from opc ua", string(j))
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

					ch <- &common.NodeValue{
						Identifier: r.nodes[item.ClientHandle].String(),
						Timestamp:  ts,
						Value:      item.Value.Value.Value(),
						ValueType:  r.nodeTypes[item.ClientHandle],
					}
				}
			}
		}

	}()

	return ch, nil
}

func (r *reader) subscribeNodes(ctx context.Context) (sub *opcua.Subscription, ch chan *opcua.PublishNotificationData, err error) {
	ch = make(chan *opcua.PublishNotificationData, 1)
	sub, err = r.client.SubscribeWithContext(ctx, &opcua.SubscriptionParameters{}, ch)
	if err != nil {
		log.Println("## subscribe failed ", err)
		return nil, nil, fmt.Errorf("subscribe failed: %w", err)
	}

	for i, node := range r.nodes {
		res, err := sub.Monitor(ua.TimestampsToReturnBoth, opcua.NewMonitoredItemCreateRequestWithDefaults(node,
			ua.AttributeIDValue, uint32(i)))
		if err != nil {
			log.Println("## subscribe monitor failed ", err)
			return nil, nil, fmt.Errorf("subscribe monitor failed: %w", err)
		}

		for _, r := range res.Results {
			if r.StatusCode != ua.StatusOK {
				log.Println("## subscribe monitor status error", r.StatusCode)
				return nil, nil, fmt.Errorf("subscribe monitor status error")
			}
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
	// ApplicationURI is automatically read from the cert so is not required if a cert is provided
	opts = append(opts, opcua.ApplicationURI("urn:taosx:gopcua:client"))
	opts = append(opts, opcua.ApplicationName("taosx"))
	opts = append(opts, opcua.RequestTimeout(r.requestTimeout))

	// certificate and private key
	var cert []byte
	if len(r.connectConfig.Certificate) != 0 && len(r.connectConfig.PrivateKey) != 0 {
		certificate, err := tls.LoadX509KeyPair(r.connectConfig.Certificate, r.connectConfig.PrivateKey)
		if err != nil {
			log.Println("## Failed to load certificate ", err)
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
		log.Println("## get server endpoint error ", err)
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

	log.Println("## unknown auth-mode, defaulting to Anonymous")
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
	return r.browseRecursive(ctx, rootNode, 0)
}

func (r *reader) browseRecursive(ctx context.Context, root *opcua.Node, level int) ([]common.Point, error) {
	if level > 5 {
		return nil, nil
	}
	childrenNodes, err := root.ChildrenWithContext(ctx, 0, ua.NodeClassAll)
	if err != nil {
		return nil, fmt.Errorf("get child for node %s error %v", root.String(), err)
	}

	var nodeMap = make(map[string]common.Point, len(childrenNodes))
	for _, node := range childrenNodes {
		nodeClass, err := node.NodeClassWithContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("get node class for node %s error %v", node.String(), err)
		}

		if nodeClass == ua.NodeClassVariable {
			var name string
			if browseName, err := node.BrowseNameWithContext(ctx); err == nil {
				name = browseName.Name
			}

			nodeMap[node.String()] = common.Point{
				ID:   node.String(),
				Name: name,
			}
		}
		recursiveNodes, err := r.browseRecursive(ctx, node, level+1)
		if err != nil {
			return nil, err
		}
		for _, recursiveNode := range recursiveNodes {
			nodeMap[recursiveNode.ID] = recursiveNode
		}
	}

	nodes := make([]common.Point, 0, len(nodeMap))
	for _, node := range nodeMap {
		nodes = append(nodes, node)
	}

	return nodes, nil
}
