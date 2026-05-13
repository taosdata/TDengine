package unified

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	tLog "github.com/taosdata/driver-go/v3/log"
	"github.com/taosdata/driver-go/v3/ws/client"
)

var (
	defaultUnifiedErrHandler = func(error) {}
)

type BootstrapFunc func(conn *websocket.Conn) error

type DialFunc func(endpoint string) (*websocket.Conn, error)

type ClientFactory func(conn *websocket.Conn, chanLength uint) *client.Client

type Option func(c *Client)

// pendingRequest represents a pending request waiting for response.
type pendingRequest struct {
	reqID   uint64
	channel chan []byte
}

type runtimeStateSnapshot struct {
	runtime    *client.Client
	generation uint64
}

// WithDialFunc overrides how websocket connections are created.
func WithDialFunc(dialFunc DialFunc) Option {
	return func(c *Client) {
		c.dial = dialFunc
	}
}

// WithClientFactory overrides runtime client construction after websocket bootstrap.
func WithClientFactory(factory ClientFactory) Option {
	return func(c *Client) {
		c.clientFactory = factory
	}
}

// Client is the shared websocket client holder used by adapters.
type Client struct {
	config        Config
	failover      *failoverState
	dialer        *websocket.Dialer
	dial          DialFunc
	clientFactory ClientFactory

	lock         sync.RWMutex
	runtime      *client.Client
	closedFlag   uint32
	runtimeGen   uint64 // incremented on each runtime swap
	closeChan    chan struct{}
	errorHandler func(error)

	// Atomic runtime snapshot used by hot paths to avoid c.lock read contention.
	runtimeSnapshot      atomic.Value
	runtimeSnapshotReady uint32

	// normal connect support
	normalConnectLock sync.Mutex
	connected         bool

	// Message routing for request-response pattern
	pendingLock     sync.RWMutex
	pendingRequests map[uint64]*pendingRequest

	// Reconnect protection
	reconnectLock sync.Mutex
	reconnecting  bool
	reconnectDone chan struct{}
	reconnectErr  error
}

// NewClient builds a failover-capable unified websocket client from normalized config input.
func NewClient(cfg *Config, defaultPath string, opts ...Option) (*Client, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	config := *cfg
	config.Endpoints = append([]string(nil), cfg.Endpoints...)
	if err := config.Normalize(defaultPath); err != nil {
		return nil, err
	}
	failoverState, err := newFailoverState(config.Endpoints)
	if err != nil {
		return nil, err
	}
	dialer := common.DefaultDialer
	dialer.EnableCompression = config.EnableCompression
	c := &Client{
		config:          config,
		failover:        failoverState,
		dialer:          &dialer,
		clientFactory:   client.NewClient,
		pendingRequests: make(map[uint64]*pendingRequest),
		closeChan:       make(chan struct{}),
		errorHandler:    defaultUnifiedErrHandler,
	}
	c.runtimeSnapshot.Store(runtimeStateSnapshot{})
	atomic.StoreUint32(&c.runtimeSnapshotReady, 1)
	c.dial = c.dialWithDialer
	for i := 0; i < len(opts); i++ {
		opts[i](c)
	}
	return c, nil
}

// NewClientFromDSN builds Config from DSN and returns a unified Client.
func NewClientFromDSN(dsn string, defaultPath string) (*Client, error) {
	cfg, err := NewConfigFromDSN(dsn, defaultPath)
	if err != nil {
		return nil, err
	}
	return NewClient(cfg, defaultPath)
}

// dialWithDialer dials endpoint using configured gorilla dialer options.
func (c *Client) dialWithDialer(endpoint string) (*websocket.Conn, error) {
	conn, _, err := c.dialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}
	conn.EnableWriteCompression(c.dialer.EnableCompression)
	return conn, nil
}

// connectWithBootstrap dials endpoints in initial order and replaces runtime client on success.
func (c *Client) connectWithBootstrap(bootstrap BootstrapFunc) error {
	return c.connectWithCandidates(c.failover.initialCandidates(), bootstrap)
}

// reconnectWithBootstrap performs reconnection with concurrent protection.
// If failedRuntime is provided, reconnect is skipped if current runtime is different and healthy.
func (c *Client) reconnectWithBootstrap(bootstrap BootstrapFunc, failedRuntime *client.Client) error {
	c.reconnectLock.Lock()

	// Check current runtime after acquiring lock
	currentRuntime := c.runtimeClient()
	// Check if current runtime is different from failed one and still healthy
	if failedRuntime != nil && currentRuntime != nil && currentRuntime != failedRuntime && currentRuntime.IsRunning() {
		c.reconnectLock.Unlock()
		tLog.Debug(0, "reconnect skipped, current runtime is healthy")
		return nil
	}

	// If reconnect is already in progress, wait for its completion.
	if c.reconnecting {
		done := c.reconnectDone
		c.reconnectLock.Unlock()
		tLog.Debug(0, "reconnect already in progress, waiting")

		select {
		case <-done:
		case <-c.closeChan:
			return ErrUnifiedClosed
		}

		c.reconnectLock.Lock()
		reconnectErr := c.reconnectErr
		c.reconnectLock.Unlock()

		runtime := c.runtimeClient()
		if runtime != nil && runtime.IsRunning() {
			return nil
		}
		if reconnectErr != nil {
			return reconnectErr
		}
		return ErrUnifiedConnectFailed
	}

	c.reconnecting = true
	c.reconnectDone = make(chan struct{})
	done := c.reconnectDone
	c.reconnectErr = nil
	c.reconnectLock.Unlock()
	tLog.Info(0, "reconnect started")

	err := c.connectWithCandidatesWithRetry(c.failover.reconnectCandidates, bootstrap)
	if err != nil && !errors.Is(err, ErrUnifiedClosed) && !IsReconnectFailedError(err) {
		err = &Error{
			Type:              ErrorTypeReconnectFailed,
			Message:           ErrUnifiedConnectFailed.Message,
			Cause:             err,
			ConnectionRelated: true,
			ReconnectFailed:   true,
		}
	}

	c.reconnectLock.Lock()
	c.reconnecting = false
	c.reconnectErr = err
	close(done)
	c.reconnectLock.Unlock()
	if err != nil {
		tLog.Errorf(0, "reconnect failed, err: %v", err)
	} else {
		tLog.Info(0, "reconnect succeeded")
	}

	return err
}

// connectWithCandidates dials candidates until one succeeds and swaps in a new runtime.
func (c *Client) connectWithCandidates(candidates []endpointCandidate, bootstrap BootstrapFunc) error {
	var lastErr error
	for i := 0; i < len(candidates); i++ {
		if c.IsClosed() {
			return ErrUnifiedClosed
		}
		candidate := candidates[i]
		endpointForLog := sanitizeEndpointForLog(candidate.URL)
		tLog.Infof(0, "connecting to endpoint %s", endpointForLog)
		conn, err := c.dial(candidate.URL)
		if err != nil {
			tLog.Warnf(0, "connect to endpoint %s failed, err: %v", endpointForLog, err)
			lastErr = err
			continue
		}
		if bootstrap != nil {
			if err = bootstrap(conn); err != nil {
				if conn != nil {
					_ = conn.Close()
				}
				tLog.Warnf(0, "connect to endpoint %s failed, err: %v", endpointForLog, err)
				lastErr = err
				continue
			}
		}
		nextRuntime := c.clientFactory(conn, c.config.ChanLength)
		oldRuntime, err := c.swapRuntime(nextRuntime, candidate.Index)
		if err != nil {
			nextRuntime.Close()
			lastErr = err
			continue
		}

		// Initialize the new runtime with handlers and pumps
		c.initializeRuntime(nextRuntime)

		if oldRuntime != nil {
			oldRuntime.Close()
		}
		tLog.Infof(0, "connected to endpoint %s", endpointForLog)
		return nil
	}
	if lastErr != nil {
		tLog.Errorf(0, "all endpoint connection attempts failed, candidates: %d, err: %v", len(candidates), lastErr)
		return lastErr
	}
	tLog.Error(0, "all endpoint connection attempts failed, no candidate available")
	return ErrUnifiedConnectFailed
}

// connectWithCandidatesWithRetry dials candidates with retry logic based on config.
func (c *Client) connectWithCandidatesWithRetry(candidateProvider func() []endpointCandidate, bootstrap BootstrapFunc) error {
	retryCount := c.config.ReconnectRetryCount
	if retryCount <= 0 {
		retryCount = 1
	}

	var lastErr error
	for attempt := 0; attempt < retryCount; attempt++ {
		tLog.Infof(0, "reconnect attempt %d/%d started", attempt+1, retryCount)
		if c.IsClosed() {
			return ErrUnifiedClosed
		}

		// Try all candidates
		candidates := candidateProvider()
		err := c.connectWithCandidates(candidates, bootstrap)
		if err == nil {
			return nil
		}
		lastErr = err
		tLog.Warnf(0, "reconnect attempt %d/%d failed, err: %v", attempt+1, retryCount, err)

		// Don't sleep after last attempt
		if attempt < retryCount-1 {
			interval := time.Duration(c.config.ReconnectIntervalMs) * time.Millisecond
			if interval <= 0 {
				interval = 2000 * time.Millisecond
			}
			tLog.Infof(0, "waiting %d ms before next reconnect attempt", interval/time.Millisecond)
			if err = c.waitReconnectInterval(interval); err != nil {
				return err
			}
		}
	}

	return lastErr
}

// initializeRuntime sets up handlers and starts pumps for a new runtime.
func (c *Client) initializeRuntime(runtime *client.Client) {
	if runtime == nil {
		return
	}

	c.lock.RLock()
	handler := c.errorHandler
	c.lock.RUnlock()

	runtime.AsyncCallbacks = false
	if c.config.WriteTimeout > 0 {
		runtime.WriteWait = c.config.WriteTimeout
	}
	runtime.SetErrorHandler(normalizeErrorHandler(handler))

	// Set unified message handlers for routing responses
	runtime.TextMessageHandler = c.handleTextMessage
	runtime.BinaryMessageHandler = c.handleBinaryMessage

	// Some unit tests build runtimes without an underlying websocket connection.
	// Skip pump startup in that case to avoid nil-pointer panics in ws/client.
	if !runtime.HasConnection() {
		return
	}

	// Start pumps
	go runtime.ReadPump()
	go runtime.WritePump()
}

// swapRuntime marks endpoint active and atomically replaces current runtime client.
// It cleans up pending requests from the old runtime and notifies waiters with nil.
func (c *Client) swapRuntime(next *client.Client, endpointIndex int) (*client.Client, error) {
	c.lock.Lock()
	if c.IsClosed() {
		c.lock.Unlock()
		return nil, ErrUnifiedClosed
	}
	if next == nil {
		c.lock.Unlock()
		return nil, ErrNilRuntime
	}
	oldActive := c.failover.active()
	newHostPort, err := c.failover.hostPortByIndex(endpointIndex)
	if err != nil {
		c.lock.Unlock()
		return nil, err
	}
	oldRuntime := c.runtime
	oldHostPort := ""
	if oldRuntime != nil {
		oldHostPort, err = c.failover.hostPortByIndex(oldActive.Index)
		if err != nil {
			c.lock.Unlock()
			return nil, err
		}
	}
	if err = c.failover.markActive(endpointIndex); err != nil {
		c.lock.Unlock()
		return nil, err
	}

	if oldRuntime == nil {
		globalHostPortConnCounts.inc(newHostPort)
	} else if oldHostPort != newHostPort {
		globalHostPortConnCounts.dec(oldHostPort)
		globalHostPortConnCounts.inc(newHostPort)
	}

	c.runtime = next
	c.runtimeGen++ // Increment generation for new runtime

	// Keep c.lock -> pendingLock order with send path.
	c.pendingLock.Lock()
	oldPending := c.resetPendingRequestsLocked()
	c.publishRuntimeSnapshotLocked()
	c.pendingLock.Unlock()
	c.lock.Unlock()

	notifyPendingRequestsClosed(oldPending)
	tLog.Infof(0, "runtime swapped to endpoint %s, pending_cleared: %d", newHostPort, len(oldPending))

	return oldRuntime, nil
}

func (c *Client) removePendingRequest(reqID uint64, expected *pendingRequest) *pendingRequest {
	c.pendingLock.Lock()
	defer c.pendingLock.Unlock()
	if c.pendingRequests == nil {
		return nil
	}
	req, ok := c.pendingRequests[reqID]
	if !ok {
		return nil
	}
	if expected != nil && expected != req {
		return nil
	}
	delete(c.pendingRequests, reqID)
	return req
}

// Runtime returns the currently active runtime client pointer.
func (c *Client) runtimeClient() *client.Client {
	return c.loadRuntimeSnapshot().runtime
}

func (c *Client) publishRuntimeSnapshotLocked() {
	c.runtimeSnapshot.Store(runtimeStateSnapshot{
		runtime:    c.runtime,
		generation: c.runtimeGen,
	})
	atomic.StoreUint32(&c.runtimeSnapshotReady, 1)
}

func (c *Client) loadRuntimeSnapshotAtomic() (runtimeStateSnapshot, bool) {
	if atomic.LoadUint32(&c.runtimeSnapshotReady) == 0 {
		return runtimeStateSnapshot{}, false
	}
	snapshot, ok := c.runtimeSnapshot.Load().(runtimeStateSnapshot)
	if !ok {
		return runtimeStateSnapshot{}, false
	}
	return snapshot, true
}

func (c *Client) loadRuntimeSnapshot() runtimeStateSnapshot {
	if snapshot, ok := c.loadRuntimeSnapshotAtomic(); ok {
		return snapshot
	}
	c.lock.RLock()
	snapshot := runtimeStateSnapshot{
		runtime:    c.runtime,
		generation: c.runtimeGen,
	}
	c.lock.RUnlock()
	return snapshot
}

func (c *Client) runtimeOrError() (*client.Client, error) {
	runtime := c.runtimeClient()
	if runtime != nil {
		return runtime, nil
	}
	if c.IsClosed() {
		return nil, ErrUnifiedClosed
	}
	return nil, client.ClosedError
}

func (c *Client) reconnectRuntimeForRetry(sendErr error, _ bool, failedRuntime *client.Client) (*client.Client, error) {
	if c.IsClosed() {
		return nil, ErrUnifiedClosed
	}
	if !c.config.AutoReconnect {
		return nil, sendErr
	}
	if !isReconnectableError(sendErr) {
		return nil, sendErr
	}
	if err := c.reconnectWithBootstrap(c.defaultBootstrap, failedRuntime); err != nil {
		return nil, err
	}
	runtime := c.runtimeClient()
	if runtime == nil {
		return nil, client.ClosedError
	}
	return runtime, nil
}

type sendWithRuntimeFunc func(runtime *client.Client) ([]byte, bool, uint64, error)

func (c *Client) sendWithReconnect(runtime *client.Client, send sendWithRuntimeFunc) ([]byte, *client.Client, uint64, error) {
	respBytes, writeAckedToSocket, runtimeGen, err := send(runtime)
	if err == nil {
		return respBytes, runtime, runtimeGen, nil
	}
	tLog.Warnf(0, "request failed, attempting reconnect, write_acked: %t, err: %v", writeAckedToSocket, err)

	runtime, err = c.reconnectRuntimeForRetry(err, writeAckedToSocket, runtime)
	if err != nil {
		tLog.Errorf(0, "reconnect for request retry failed, err: %v", err)
		return nil, nil, 0, err
	}
	tLog.Info(0, "retrying request after reconnect")

	respBytes, _, runtimeGen, err = send(runtime)
	if err != nil {
		tLog.Errorf(0, "request retry after reconnect failed, err: %v", err)
		return nil, nil, 0, err
	}
	return respBytes, runtime, runtimeGen, nil
}

// Close marks client closed and closes active runtime if present.
func (c *Client) Close() {
	c.lock.Lock()
	if c.IsClosed() {
		c.lock.Unlock()
		return
	}
	var activeHostPort string
	if c.runtime != nil && c.failover != nil {
		active := c.failover.active()
		if hostPort, err := c.failover.hostPortByIndex(active.Index); err == nil {
			activeHostPort = hostPort
		}
	}
	atomic.StoreUint32(&c.closedFlag, 1)
	c.connected = false
	runtime := c.runtime
	c.runtime = nil
	c.pendingLock.Lock()
	oldPending := c.resetPendingRequestsLocked()
	c.publishRuntimeSnapshotLocked()
	c.pendingLock.Unlock()
	if c.closeChan != nil {
		close(c.closeChan)
	}
	c.lock.Unlock()
	notifyPendingRequestsClosed(oldPending)
	if activeHostPort != "" {
		globalHostPortConnCounts.dec(activeHostPort)
	}
	if runtime != nil {
		runtime.Close()
	}
}

// IsClosed reports whether client has been closed.
func (c *Client) IsClosed() bool {
	return atomic.LoadUint32(&c.closedFlag) == 1
}

func (c *Client) resetPendingRequestsLocked() map[uint64]*pendingRequest {
	oldPending := c.pendingRequests
	c.pendingRequests = make(map[uint64]*pendingRequest)
	return oldPending
}

func notifyPendingRequestsClosed(requests map[uint64]*pendingRequest) {
	for _, req := range requests {
		notifyPendingRequestClosed(req)
	}
}

func notifyPendingRequestClosed(req *pendingRequest) {
	if req == nil || req.channel == nil {
		return
	}

	// Fast path: queue closed notification immediately.
	select {
	case req.channel <- nil:
		return
	default:
	}

	// Channel is full. Preserve a routed response if one is already queued.
	select {
	case msg := <-req.channel:
		if msg != nil {
			select {
			case req.channel <- msg:
			default:
			}
			return
		}
	default:
		return
	}

	// Channel previously held nil; make sure closed notification remains queued.
	select {
	case req.channel <- nil:
	default:
	}
}

// Config returns current normalized client config by value.
func (c *Client) Config() Config {
	return c.config
}

// SetErrorHandler sets the error handler callback for the runtime client.
func (c *Client) SetErrorHandler(handler func(error)) {
	normalized := normalizeErrorHandler(handler)

	c.lock.Lock()
	c.errorHandler = normalized
	if c.runtime != nil {
		c.runtime.SetErrorHandler(normalized)
	}
	c.lock.Unlock()
}

func (c *Client) waitReconnectInterval(interval time.Duration) error {
	if interval <= 0 {
		return nil
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-c.closeChan:
		return ErrUnifiedClosed
	}
}

func normalizeErrorHandler(handler func(error)) func(error) {
	if handler == nil {
		return defaultUnifiedErrHandler
	}
	return handler
}

// isReconnectableError checks if an error should trigger reconnect.
func isReconnectableError(err error) bool {
	if err == nil {
		return false
	}
	var opError *net.OpError
	var closeError *websocket.CloseError
	if errors.Is(err, client.ClosedError) || errors.As(err, &opError) || errors.As(err, &closeError) {
		return true
	}
	return false
}
