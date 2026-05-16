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
	// propertyMu 保护 getPointsCache 内 Point.Properties map 的并发写入。
	// 收集 Property 阶段，多个 goroutine 可能向同一父 Point 回填 Property → 必须加锁。
	propertyMu sync.Mutex

	// badNodes 记录持续失败的节点，后续轮询跳过这些节点以避免整批失败和日志洪泛。
	// 定期重探以检测节点是否恢复。
	badNodes      map[string]*badNodeInfo
	badNodesMu    sync.RWMutex
	probeInterval uint64 // 每隔多少个轮询周期重探一次
	pollCount     uint64 // 当前轮询周期计数

	// 跨周期失败批次拆分：当批次 RPC 失败时（如 StatusBadEncodingError），
	// 记录失败批次的节点范围，下一周期将其拆成更小的子批次分别读取。
	// 利用周期间的连接自动恢复来绕过"同周期连接断开"的约束。
	failedBatches []failedBatchInfo
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
	nodeID              *ua.NodeID
	nodeValue           *common.NodeValue
	clientHandle        uint32 //always exists
	subscribed          bool
	subscriptionID      *int
	monitoredItemID     uint32
	consecutiveFailures int // 连续 Status 失败次数，用于黑名单判定
}

// badNodeInfo 记录被拉黑节点的信息
type badNodeInfo struct {
	idStr     string
	reason    string // "batch_rpc_error" | "status_error"
	lastError string
	addedAt   time.Time
}

// failedBatchInfo 记录上一周期失败的批次，下一周期跨周期拆分重试。
type failedBatchInfo struct {
	nodes   []*nodeValue // 失败批次中的节点（直接引用，不依赖索引）
	subSize int          // 下一周期用的子批次大小（每次失败减半）
}

// batchReadResult 表示 readValueBatch 的执行结果。
type batchReadResult int

const (
	batchReadOK              batchReadResult = iota // 批次读取成功
	batchReadConnectionError                        // 连接级错误，Server 不可达
	batchReadRPCError                               // 非连接 RPC 错误（如 EncodingError），节点相关
)

const (
	// statusFailThreshold 连续 Status 失败多少次后加入黑名单
	statusFailThreshold = 3
	// defaultProbeInterval 每隔多少个轮询周期重探一次黑名单节点
	defaultProbeInterval = 60
	// postFailureDelay 批次 RPC 错误后等待自动重连的延迟
	postFailureDelay = 500 * time.Millisecond
	// individualTestThreshold 当子批次大小 <= 此值时，逐个读取以精确定位坏节点
	individualTestThreshold = 10
)

// isConnectionError 判断 err 是否为连接级别错误（非节点自身问题）。
// 连接级错误意味着 OPC Server 不可达，任何 Read 都会失败，不应归咎于具体节点。
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	// 优先使用类型化的 ua.StatusCode 检查
	connectionStatuses := []ua.StatusCode{
		ua.StatusBadServerNotConnected,
		ua.StatusBadConnectionClosed,
		ua.StatusBadSessionClosed,
		ua.StatusBadSecureChannelClosed,
		ua.StatusBadCommunicationError,
		ua.StatusBadTimeout,
	}
	for _, code := range connectionStatuses {
		if errors.Is(err, code) {
			return true
		}
	}
	// context 取消 / 超时
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// 网络层错误
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// 最后兜底：字符串匹配
	s := err.Error()
	return strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "use of closed network connection")
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
		badNodes:                 make(map[string]*badNodeInfo),
		probeInterval:            defaultProbeInterval,
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

	// Phase 1: 处理上一周期记录的失败批次（跨周期拆分重试）。
	// 此时连接已自动恢复，子批次 Read 可以正常完成。
	var nextFailedBatches []failedBatchInfo
	if len(c.failedBatches) > 0 {
		c.logger.WithField("count", len(c.failedBatches)).
			Debug("retrying failed batches from previous cycle")

		// 收集失败批次覆盖的节点，用于 Phase 2 中跳过
		failedNodeSet := make(map[*nodeValue]struct{})
		for _, fb := range c.failedBatches {
			for _, n := range fb.nodes {
				failedNodeSet[n] = struct{}{}
			}
		}

		for _, fb := range c.failedBatches {
			newFailed := c.retryFailedBatch(fb)
			nextFailedBatches = append(nextFailedBatches, newFailed...)
		}

		// Phase 2: 正常读取不在失败批次中的节点
		if len(failedNodeSet) < len(nodes) {
			normalNodes := make([]*nodeValue, 0, len(nodes)-len(failedNodeSet))
			for _, n := range nodes {
				if _, inFailed := failedNodeSet[n]; !inFailed {
					normalNodes = append(normalNodes, n)
				}
			}
			newFailed := c.readNormalBatches(normalNodes)
			nextFailedBatches = append(nextFailedBatches, newFailed...)
		}
	} else {
		// 无历史失败批次，所有节点正常分批读取
		nextFailedBatches = c.readNormalBatches(nodes)
	}

	c.failedBatches = nextFailedBatches
}

// readNormalBatches 用 maxNodesPerRead 分批读取节点，返回新产生的失败批次。
func (c *UAClient) readNormalBatches(nodes []*nodeValue) []failedBatchInfo {
	if len(nodes) == 0 {
		return nil
	}
	var failed []failedBatchInfo
	maxOp := int(c.maxNodesPerRead)

	for i := 0; i < len(nodes); i += maxOp {
		end := i + maxOp
		if end > len(nodes) {
			end = len(nodes)
		}
		batch := nodes[i:end]
		result := c.readValueBatch(batch)
		if result == batchReadRPCError {
			subSize := len(batch) / 2
			if subSize < 1 {
				subSize = 1
			}
			failed = append(failed, failedBatchInfo{
				nodes:   batch,
				subSize: subSize,
			})
			c.logger.WithFields(logrus.Fields{
				"batch_size":     len(batch),
				"next_sub_size":  subSize,
				"first":          batch[0].nodeValue.IDStr,
				"last":           batch[len(batch)-1].nodeValue.IDStr,
			}).Warn("batch failed, will retry with smaller sub-batches next cycle")
			time.Sleep(postFailureDelay)
		} else if result == batchReadConnectionError {
			time.Sleep(postFailureDelay)
		}
	}
	return failed
}

// retryFailedBatch 将上一周期的失败批次用更小的子批次重试，返回仍然失败的子批次。
func (c *UAClient) retryFailedBatch(fb failedBatchInfo) []failedBatchInfo {
	nodes := fb.nodes
	subSize := fb.subSize

	c.logger.WithFields(logrus.Fields{
		"total_nodes": len(nodes),
		"sub_size":    subSize,
		"first":       nodes[0].nodeValue.IDStr,
		"last":        nodes[len(nodes)-1].nodeValue.IDStr,
	}).Info("retrying failed batch with sub-batches")

	var stillFailed []failedBatchInfo

	for i := 0; i < len(nodes); i += subSize {
		end := i + subSize
		if end > len(nodes) {
			end = len(nodes)
		}
		sub := nodes[i:end]
		result := c.readValueBatch(sub)
		switch result {
		case batchReadOK:
			// 子批次成功，数据已正常处理
		case batchReadRPCError:
			if len(sub) <= individualTestThreshold {
				// 子批次足够小，逐节点测试精确定位坏节点
				c.testNodesIndividually(sub)
			} else {
				// 继续缩小，下一周期再拆
				nextSub := len(sub) / 2
				if nextSub < 1 {
					nextSub = 1
				}
				stillFailed = append(stillFailed, failedBatchInfo{
					nodes:   sub,
					subSize: nextSub,
				})
				c.logger.WithFields(logrus.Fields{
					"sub_size":      len(sub),
					"next_sub_size": nextSub,
					"first":         sub[0].nodeValue.IDStr,
					"last":          sub[len(sub)-1].nodeValue.IDStr,
				}).Warn("sub-batch still failing, will retry with smaller size next cycle")
			}
			time.Sleep(postFailureDelay)
		case batchReadConnectionError:
			// 连接断开，保留原批次下一周期重试（不缩小，因为不是节点问题）
			stillFailed = append(stillFailed, failedBatchInfo{
				nodes:   sub,
				subSize: len(sub), // 不缩小
			})
			time.Sleep(postFailureDelay)
		}
	}
	return stillFailed
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

func (c *UAClient) readValueBatch(nodes []*nodeValue) batchReadResult {
	valueReqs := make([]*ua.ReadValueID, 0, len(nodes))
	for _, node := range nodes {
		valueReqs = append(valueReqs, &ua.ReadValueID{NodeID: node.nodeID, AttributeID: ua.AttributeIDValue})
	}
	if len(nodes) > 0 {
		c.logger.WithFields(logrus.Fields{
			"size":  len(nodes),
			"first": nodes[0].nodeValue.IDStr,
			"last":  nodes[len(nodes)-1].nodeValue.IDStr,
		}).Debug("readValueBatch start")
	}
	start := time.Now()
	conn := c.conn
	resp, err := conn.Read(c.ctx, &ua.ReadRequest{MaxAge: c.maxAge, TimestampsToReturn: ua.TimestampsToReturnBoth, NodesToRead: valueReqs})
	if err != nil {
		if isConnectionError(err) {
			c.logger.WithError(err).WithFields(logrus.Fields{
				"batch_size": len(nodes),
				"first":      nodes[0].nodeValue.IDStr,
				"last":       nodes[len(nodes)-1].nodeValue.IDStr,
			}).Warn("readValueBatch failed with connection error, skipping batch")
			return batchReadConnectionError
		}
		c.logger.WithError(err).WithFields(logrus.Fields{
			"batch_size": len(nodes),
			"first":      nodes[0].nodeValue.IDStr,
			"last":       nodes[len(nodes)-1].nodeValue.IDStr,
		}).Warn("readValueBatch failed with RPC error")

		return batchReadRPCError
	}
	end := time.Now()
	c.logger.WithField("time", end.Sub(start)).Debug("read value spend")
	for i, r := range resp.Results {
		if !errors.Is(r.Status, ua.StatusOK) {
			nodes[i].nodeValue.Value = nil
			nodes[i].consecutiveFailures++
			if nodes[i].consecutiveFailures == statusFailThreshold {
				c.logger.WithField("id", nodes[i].nodeValue.IDStr).
					WithError(r.Status).
					WithField("consecutive_failures", nodes[i].consecutiveFailures).
					Error("node consistently failing, adding to bad nodes list")
				c.addBadNode(nodes[i].nodeValue.IDStr, "status_error", r.Status.Error())
			} else if nodes[i].consecutiveFailures == 1 {
				c.logger.WithField("id", nodes[i].nodeValue.IDStr).
					WithError(r.Status).
					Debug("read value status error")
			}
		} else {
			nodes[i].consecutiveFailures = 0
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
	return batchReadOK
}

// testNodesIndividually 逐个读取小批次中的节点，精确定位并黑名单化坏节点。
// 仅在批次大小 <= individualTestThreshold 时调用。
// 注意：调用时机是在 RPC 错误之后，连接可能已断开。
// 对连接错误采用容忍策略：跳过并等待下一轮 probe 处理。
func (c *UAClient) testNodesIndividually(nodes []*nodeValue) {
	for _, n := range nodes {
		req := &ua.ReadRequest{
			MaxAge:             c.maxAge,
			TimestampsToReturn: ua.TimestampsToReturnBoth,
			NodesToRead: []*ua.ReadValueID{
				{NodeID: n.nodeID, AttributeID: ua.AttributeIDValue},
			},
		}
		_, err := c.conn.Read(c.ctx, req)
		if err == nil {
			continue // 单节点读取成功，不是坏节点
		}
		if isConnectionError(err) {
			// 连接断开，无法继续逐个测试。
			// 不标记任何节点为坏——等连接恢复后由 adaptive 机制再次处理。
			c.logger.WithField("id", n.nodeValue.IDStr).WithError(err).
				Debug("testNodesIndividually: connection error, deferring remaining nodes")
			return
		}
		// 单节点 RPC 错误 → 确认为坏节点
		c.logger.WithField("id", n.nodeValue.IDStr).WithError(err).
			Warn("testNodesIndividually: single node causes RPC failure, adding to bad nodes list")
		c.addBadNode(n.nodeValue.IDStr, "batch_rpc_error",
			"single node Read RPC failure: "+err.Error())
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

// addBadNode 将节点加入黑名单。
func (c *UAClient) addBadNode(idStr string, reason string, lastError string) {
	c.badNodesMu.Lock()
	defer c.badNodesMu.Unlock()
	if _, exists := c.badNodes[idStr]; exists {
		return
	}
	c.badNodes[idStr] = &badNodeInfo{
		idStr:     idStr,
		reason:    reason,
		lastError: lastError,
		addedAt:   time.Now(),
	}
	c.logger.WithFields(logrus.Fields{
		"id":           idStr,
		"reason":       reason,
		"error":        lastError,
		"total_bad":    len(c.badNodes),
	}).Warn("node added to bad nodes list, will be skipped in subsequent polls")
}

// filterActiveNodes 返回不在黑名单中的节点。
func (c *UAClient) filterActiveNodes(nodes []*nodeValue) []*nodeValue {
	c.badNodesMu.RLock()
	defer c.badNodesMu.RUnlock()
	if len(c.badNodes) == 0 {
		return nodes
	}
	active := make([]*nodeValue, 0, len(nodes))
	for _, n := range nodes {
		if _, bad := c.badNodes[n.nodeValue.IDStr]; !bad {
			active = append(active, n)
		}
	}
	return active
}

// probeBadNodes 逐个重探黑名单节点，恢复正常的移出黑名单。
func (c *UAClient) probeBadNodes() {
	c.badNodesMu.RLock()
	badList := make([]*badNodeInfo, 0, len(c.badNodes))
	for _, info := range c.badNodes {
		badList = append(badList, info)
	}
	c.badNodesMu.RUnlock()

	if len(badList) == 0 {
		return
	}
	c.logger.WithField("count", len(badList)).Info("probing bad nodes for recovery")

	recovered := 0
	for _, info := range badList {
		// 找到对应的 nodeValue（需要从 c.nodes 中查找）
		var target *nodeValue
		for _, n := range c.nodes {
			if n.nodeValue.IDStr == info.idStr {
				target = n
				break
			}
		}
		if target == nil {
			continue
		}

		req := &ua.ReadRequest{
			MaxAge:             c.maxAge,
			TimestampsToReturn: ua.TimestampsToReturnBoth,
			NodesToRead: []*ua.ReadValueID{
				{NodeID: target.nodeID, AttributeID: ua.AttributeIDValue},
			},
		}
		resp, err := c.conn.Read(c.ctx, req)
		if err != nil {
			if isConnectionError(err) {
				c.logger.WithError(err).Warn("probe: connection error, aborting probe cycle")
				break
			}
			c.logger.WithField("id", info.idStr).WithError(err).
				Debug("probe: node still fails RPC")
			continue
		}
		if len(resp.Results) > 0 && errors.Is(resp.Results[0].Status, ua.StatusOK) {
			// 节点恢复
			c.badNodesMu.Lock()
			delete(c.badNodes, info.idStr)
			c.badNodesMu.Unlock()
			target.consecutiveFailures = 0
			recovered++
			c.logger.WithField("id", info.idStr).
				Info("probe: node recovered, removed from bad nodes list")
		} else {
			c.logger.WithField("id", info.idStr).
				Debug("probe: node still has bad status")
		}
	}
	if recovered > 0 {
		c.badNodesMu.RLock()
		remaining := len(c.badNodes)
		c.badNodesMu.RUnlock()
		c.logger.WithFields(logrus.Fields{
			"recovered": recovered,
			"remaining": remaining,
		}).Info("probe completed")
	}
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
				c.failedBatches = nil // 节点列表变更，旧的失败批次引用失效
			case <-ticker.C:
				c.pollCount++

				// 定期重探黑名单节点
				if c.pollCount%c.probeInterval == 0 {
					c.probeBadNodes()
				}

				// 构建活跃节点列表（排除黑名单）
				activeNodes := c.filterActiveNodes(c.nodes)

				readNameList = readNameList[:0]
				start := time.Now()
				c.readAllValue(activeNodes)
				spent := time.Since(start)
				if spent > c.readInterval {
					c.logger.WithField("spent", spent).WithField("interval", c.readInterval).Warn("read value spend too much time")
				}
				values := make([]*common.NodeValue, 0, len(activeNodes))
				for _, node := range activeNodes {
					if !node.nodeValue.ValueType.IsValid() {
						continue
					}
					if node.nodeValue.Name == "" {
						readNameList = append(readNameList, node)
						continue
					}
					if !errors.Is(ua.StatusCode(node.nodeValue.Status), ua.StatusOK) {
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
	// 打印当前批要 monitor 的节点范围
	if len(nodes) > 0 {
		c.logger.WithFields(logrus.Fields{
			"sub_index": sub.subIndex,
			"size":      len(nodes),
			"first":     nodes[0].nodeValue.IDStr,
			"last":      nodes[len(nodes)-1].nodeValue.IDStr,
		}).Debug("doSubItems start")
	}
	resp, err := sub.sub.Monitor(c.ctx, ua.TimestampsToReturnBoth, reqs...)
	if err != nil {
		c.logger.WithError(err).WithField("batch_size", len(nodes)).Error("doSubItems Monitor RPC failed")
		return err
	}
	var errs []error
	var failedIds []string
	var failedStatuses []string
	for index, r := range resp.Results {
		if !errors.Is(r.StatusCode, ua.StatusOK) {
			errs = append(errs, fmt.Errorf("subscribe monitor for node %s failed: %w", nodes[index].nodeValue.IDStr, r.StatusCode))
			if len(failedIds) < 20 {
				failedIds = append(failedIds, nodes[index].nodeValue.IDStr)
				failedStatuses = append(failedStatuses, r.StatusCode.Error())
			}
		} else {
			nodes[index].subscribed = true
			nodes[index].monitoredItemID = r.MonitoredItemID
			sub.subCount += 1
		}
	}
	// 打印本批 monitor 成功/失败统计
	c.logger.WithFields(logrus.Fields{
		"sub_index":  sub.subIndex,
		"batch_size": len(nodes),
		"success":    len(nodes) - len(errs),
		"fail":       len(errs),
	}).Debug("doSubItems result")
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
					readNameList = readNameList[:0]
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
	node            *opcua.Node
	id              string
	browseName      string
	displayName     string
	parentID        string
	path            string
	nodeClass       ua.NodeClass
	parentNodeClass ua.NodeClass
	referenceTypeID *ua.NodeID
	typeDefinition  *ua.NodeID
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
	ua.AttributeIDDataType,
}

var attributeNames = []string{
	"Description",
	"DataType",
}

var nodeClassNames = []string{
	ua.NodeClassObject:   "Object",
	ua.NodeClassVariable: "Variable",
}

// dataTypeNames maps OPC UA built-in type IDs (namespace 0) to their standard names.
// These names match OPC UA specification Part 6, and are recognized by Rust-side
// opc_data_type_to_ipc() for resolving {type} in super_table_expression.
var dataTypeNames = map[uint32]string{
	1:  "Boolean",
	2:  "SByte",
	3:  "Byte",
	4:  "Int16",
	5:  "UInt16",
	6:  "Int32",
	7:  "UInt32",
	8:  "Int64",
	9:  "UInt64",
	10: "Float",
	11: "Double",
	12: "String",
	13: "DateTime",
	15: "ByteString",
	// Uncommon types — include for completeness
	14: "Guid",
	16: "XmlElement",
	17: "NodeId",
	20: "QualifiedName",
	21: "LocalizedText",
	22: "ExtensionObject",
	24: "Variant",
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
	var propertyTasks []propertyReadTask
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
		// get Description attribute (index+0)
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

		// get DataType attribute (index+1) — returns a NodeID identifying the data type
		dataTypeName := ""
		dtStatus := res.Results[index+1].Status
		if errors.Is(dtStatus, ua.StatusOK) && res.Results[index+1].Value != nil {
			dtNodeID := res.Results[index+1].Value.NodeID()
			if dtNodeID != nil && dtNodeID.Namespace() == 0 {
				if name, ok := dataTypeNames[dtNodeID.IntID()]; ok {
					dataTypeName = name
				}
			}
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
			DataType:    dataTypeName,
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
		// 对 Variable 应用 4 级分类规则
		cls, hit := Classify(nodes[i].parentNodeClass, nodes[i].referenceTypeID, nodes[i].typeDefinition)
		if cls == ClassifyProperty {
			// Property 节点：不进 result（→ 不下发到 collect 订阅），
			// 只标记 IsProperty 并加入待 Read Value 队列，由 readAndAttachProperties
			// 把值回填到父 Variable 的 Properties map。
			point.IsProperty = true
			propertyTasks = append(propertyTasks, propertyReadTask{
				nodeID:     nodes[i].node.ID,
				parentID:   parent,
				browseName: browseName,
			})
			continue
		}
		if hit == "rule4-fallback" {
			c.logger.WithFields(logrus.Fields{
				"nodeID":          nodeID,
				"parentNodeClass": nodes[i].parentNodeClass,
				"referenceTypeID": referenceTypeIDString(nodes[i].referenceTypeID),
				"typeDefinition":  typeDefinitionString(nodes[i].typeDefinition),
			}).Warn("classify fell through to fallback rule, treating as dynamic Variable")
		}
		point.IsStatic = false
		result = append(result, point)
	}
	if len(propertyTasks) > 0 {
		c.readAndAttachProperties(ctx, conn, propertyTasks)
	}
	if len(result) > 0 {
		if c.isKepServer {
			// get KepServer point description
			c.getKepServerDescription(ctx, conn, result)
		}
	}
	return result
}

// propertyReadTask 描述一个待 Read Value 的 Property 节点。
type propertyReadTask struct {
	nodeID     *ua.NodeID
	parentID   string
	browseName string
}

// readAndAttachProperties 批量 Read 所有 Property 节点的 Value，
// 序列化后回填到父 Variable Point 的 Properties map。
//
// 调用频次：每批 BFS 调用一次。失败不影响其他 Property，仅 WARN 日志。
func (c *UAClient) readAndAttachProperties(ctx context.Context, conn *opcua.Client, tasks []propertyReadTask) {
	if len(tasks) == 0 {
		return
	}
	valueIDs := make([]*ua.ReadValueID, len(tasks))
	for i, t := range tasks {
		valueIDs[i] = &ua.ReadValueID{
			NodeID:      t.nodeID,
			AttributeID: ua.AttributeIDValue,
		}
	}
	req := &ua.ReadRequest{NodesToRead: valueIDs}
	res, err := conn.Read(ctx, req)
	if err != nil {
		c.logger.WithError(err).Warn("read property values error, properties will be empty")
		return
	}
	if len(res.Results) != len(tasks) {
		c.logger.WithFields(logrus.Fields{
			"want": len(tasks),
			"got":  len(res.Results),
		}).Warn("read property values response length mismatch")
		return
	}
	for i, dv := range res.Results {
		t := tasks[i]
		if dv == nil {
			continue
		}
		if !errors.Is(dv.Status, ua.StatusOK) {
			c.logger.WithFields(logrus.Fields{
				"nodeID": t.nodeID.String(),
				"status": dv.Status,
			}).Warn("read property value failed, skipping")
			continue
		}
		if dv.Value == nil {
			continue
		}
		serialized, serr := serializePropertyValue(dv.Value.Value())
		if serr != nil {
			c.logger.WithFields(logrus.Fields{
				"nodeID": t.nodeID.String(),
				"err":    serr,
			}).Warn("serialize property value failed, skipping")
			continue
		}
		// 找到父 Variable Point，回填 Properties map。
		parentVal, ok := c.getPointsCache.Load(t.parentID)
		if !ok {
			continue
		}
		parent, ok := parentVal.(*common.Point)
		if !ok {
			continue
		}
		// 只回填到 Variable 父；Object 父的 Properties 无意义（opc_object stable 无 Tag 列）。
		if parent.NodeType != "Variable" {
			continue
		}
		c.propertyMu.Lock()
		if parent.Properties == nil {
			parent.Properties = make(map[string]string)
		}
		// 同名冲突直接覆盖；调用方（Rust 侧 generate）会在合并 Tag union 时再做冲突检测。
		// 这里覆盖是因为 OPC UA 规范保证同一父下 Property 的 BrowseName 唯一。
		parent.Properties[t.browseName] = serialized
		c.propertyMu.Unlock()
	}
}

// referenceTypeIDString 返回 NodeID 字符串，nil → "<nil>"。仅用于日志。
func referenceTypeIDString(n *ua.NodeID) string {
	if n == nil {
		return "<nil>"
	}
	return n.String()
}

// typeDefinitionString 返回 NodeID 字符串，nil → "<nil>"。仅用于日志。
func typeDefinitionString(n *ua.NodeID) string {
	if n == nil {
		return "<nil>"
	}
	return n.String()
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

					var typeDef *ua.NodeID
					if refItem.TypeDefinition != nil {
						typeDef = refItem.TypeDefinition.NodeID
					}

					childIDStr := nodeId.String()

					children = append(children, &bfsListElement{
						node:            nodeId,
						id:              childIDStr,
						browseName:      browseName,
						displayName:     displayName,
						nodeClass:       refItem.NodeClass,
						parentID:        parentID,
						path:            path,
						parentNodeClass: browseNode.nodeClass,
						referenceTypeID: refItem.ReferenceTypeID,
						typeDefinition:  typeDef,
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
