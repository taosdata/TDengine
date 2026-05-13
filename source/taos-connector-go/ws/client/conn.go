package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"

	"github.com/taosdata/driver-go/v3/common"
	errors2 "github.com/taosdata/driver-go/v3/errors"
)

const (
	StatusNormal = uint32(1)
	StatusStop   = uint32(2)
)

var JsonI = jsoniter.ConfigCompatibleWithStandardLibrary

type WSAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

var GlobalEnvelopePool EnvelopePool

type EnvelopePool struct {
	p sync.Pool
}

func (ep *EnvelopePool) Get() *Envelope {
	epv := ep.p.Get()
	if epv == nil {
		return &Envelope{Msg: new(bytes.Buffer), ErrorChan: make(chan error, 1)}
	}
	return epv.(*Envelope)
}

func (ep *EnvelopePool) Put(epv *Envelope) {
	epv.Reset()
	ep.p.Put(epv)
}

type Envelope struct {
	Type      int
	Msg       *bytes.Buffer
	ErrorChan chan error
}

func (e *Envelope) Reset() {
	if e.Msg.Cap() > 64*1024 {
		e.Msg = new(bytes.Buffer)
	} else {
		e.Msg.Reset()
	}
	if len(e.ErrorChan) > 0 {
		e.ErrorChan = make(chan error, 1)
	}
}

func notifyEnvelopeError(envelope *Envelope, err error) {
	if envelope == nil || envelope.ErrorChan == nil {
		return
	}
	select {
	case envelope.ErrorChan <- err:
	default:
		// Channel is full (usually stale unread value). Drop one and retry once.
		select {
		case <-envelope.ErrorChan:
		default:
		}
		select {
		case envelope.ErrorChan <- err:
		default:
		}
	}
}

//revive:disable-next-line
var ClosedError = errors.New("websocket closed")

type Client struct {
	conn                 *websocket.Conn
	status               uint32
	sendChan             chan *Envelope
	done                 chan struct{}
	AsyncCallbacks       bool
	BufferSize           int
	WriteWait            time.Duration
	PingPeriod           time.Duration
	PongWait             time.Duration
	TextMessageHandler   func(message []byte)
	BinaryMessageHandler func(message []byte)
	// ErrorHandler is kept exported for backward compatibility.
	// Prefer SetErrorHandler and avoid direct concurrent assignment after pumps start.
	ErrorHandler func(err error)
	// SendMessageHandler   func(envelope *Envelope)
	once           sync.Once
	doneOnce       sync.Once
	errHandlerOnce sync.Once
	sendLock       sync.RWMutex
	errLock        sync.RWMutex
	handlerLock    sync.RWMutex
	lastErr        error
}

func NewClient(conn *websocket.Conn, sendChanLength uint) *Client {
	return &Client{
		conn:                 conn,
		status:               StatusNormal,
		BufferSize:           common.BufferSize4M,
		sendChan:             make(chan *Envelope, sendChanLength),
		done:                 make(chan struct{}),
		AsyncCallbacks:       false,
		WriteWait:            common.DefaultWriteWait,
		PingPeriod:           common.DefaultPingPeriod,
		PongWait:             common.DefaultPongWait,
		TextMessageHandler:   func(message []byte) {},
		BinaryMessageHandler: func(message []byte) {},
		ErrorHandler:         func(err error) {},
	}
}

func (c *Client) ReadPump() {
	c.conn.SetReadLimit(0)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.PongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.PongWait))
		return nil
	})
	c.conn.SetCloseHandler(nil)
	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if !c.IsRunning() {
				return
			}
			c.handleError(err)
			c.Close()
			return
		}
		switch messageType {
		case websocket.TextMessage:
			if c.AsyncCallbacks {
				go c.TextMessageHandler(message)
			} else {
				c.TextMessageHandler(message)
			}
		case websocket.BinaryMessage:
			if c.AsyncCallbacks {
				go c.BinaryMessageHandler(message)
			} else {
				c.BinaryMessageHandler(message)
			}
		}
	}
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(c.PingPeriod)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case message, ok := <-c.sendChan:
			if !ok {
				return
			}
			if message == nil {
				continue
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.WriteWait))
			err := c.conn.WriteMessage(message.Type, message.Msg.Bytes())
			if err != nil {
				notifyEnvelopeError(message, err)
				c.handleError(err)
				c.Close()
				c.drainSendChan()
				return
			}
			notifyEnvelopeError(message, nil)
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.WriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.handleError(err)
				c.Close()
				c.drainSendChan()
				return
			}
		}
	}
}

func (c *Client) Send(envelope *Envelope) (err error) {
	c.sendLock.RLock()
	defer c.sendLock.RUnlock()
	if !c.IsRunning() {
		return ClosedError
	}
	select {
	case <-c.done:
		return ClosedError
	case c.sendChan <- envelope:
		return nil
	}
}

func (c *Client) GetEnvelope() *Envelope {
	return GlobalEnvelopePool.Get()
}

func (c *Client) PutEnvelope(envelope *Envelope) {
	GlobalEnvelopePool.Put(envelope)
}

func (c *Client) IsRunning() bool {
	return atomic.LoadUint32(&c.status) == StatusNormal
}

func (c *Client) Done() <-chan struct{} {
	return c.done
}

// HasConnection reports whether this runtime has an underlying websocket connection.
func (c *Client) HasConnection() bool {
	return c != nil && c.conn != nil
}

func (c *Client) LastError() error {
	c.errLock.RLock()
	err := c.lastErr
	c.errLock.RUnlock()
	if err != nil {
		return err
	}
	if !c.IsRunning() {
		return ClosedError
	}
	return nil
}

func (c *Client) Close() {
	c.once.Do(func() {
		atomic.StoreUint32(&c.status, StatusStop)
		c.closeDone()
		c.sendLock.Lock()
		defer c.sendLock.Unlock()
		close(c.sendChan)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func (c *Client) handleError(err error) {
	atomic.StoreUint32(&c.status, StatusStop)
	c.errLock.Lock()
	if c.lastErr == nil {
		c.lastErr = err
	}
	c.errLock.Unlock()
	c.closeDone()
	handler := c.getErrorHandler()
	c.errHandlerOnce.Do(func() { handler(err) })
}

func (c *Client) closeDone() {
	c.doneOnce.Do(func() {
		close(c.done)
	})
}

func (c *Client) drainSendChan() {
	for message := range c.sendChan {
		if message == nil {
			continue
		}
		notifyEnvelopeError(message, ClosedError)
	}
}

func (c *Client) SetErrorHandler(handler func(error)) {
	if handler == nil {
		handler = func(error) {}
	}
	c.handlerLock.Lock()
	c.ErrorHandler = handler
	c.handlerLock.Unlock()
}

func (c *Client) getErrorHandler() func(error) {
	c.handlerLock.RLock()
	handler := c.ErrorHandler
	c.handlerLock.RUnlock()
	if handler == nil {
		return func(error) {}
	}
	return handler
}

func HandleResponseError(err error, code int, msg string) error {
	if err != nil {
		return err
	}
	if code != 0 {
		return errors2.NewError(code, msg)
	}
	return nil
}
