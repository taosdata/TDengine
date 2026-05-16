package melody

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

type envelope struct {
	t   int
	msg []byte
}

type handleMessageFunc func(*Session, []byte)
type handleErrorFunc func(*Session, error)
type handleCloseFunc func(*Session, int, string) error
type handleSessionFunc func(*Session)

// Melody implements a websocket manager.
type Melody struct {
	sessionCount         uint32
	Config               *Config
	Upgrader             *websocket.Upgrader
	messageHandler       handleMessageFunc
	messageHandlerBinary handleMessageFunc
	errorHandler         handleErrorFunc
	closeHandler         handleCloseFunc
	connectHandler       handleSessionFunc
	disconnectHandler    handleSessionFunc
	pongHandler          handleSessionFunc
}

// New creates a new melody instance with default Upgrader and Config.
func New() *Melody {
	upGrader := &websocket.Upgrader{
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
		WriteBufferPool:   &sync.Pool{},
		CheckOrigin:       func(r *http.Request) bool { return true },
		EnableCompression: true,
	}

	return &Melody{
		sessionCount:         0,
		Config:               newConfig(),
		Upgrader:             upGrader,
		messageHandler:       func(*Session, []byte) {},
		messageHandlerBinary: func(*Session, []byte) {},
		errorHandler:         func(*Session, error) {},
		closeHandler:         nil,
		connectHandler:       func(*Session) {},
		disconnectHandler:    func(*Session) {},
		pongHandler:          func(*Session) {},
	}
}

// HandleConnect fires fn when a session connects.
func (m *Melody) HandleConnect(fn func(*Session)) {
	m.connectHandler = fn
}

// HandleDisconnect fires fn when a session disconnects.
func (m *Melody) HandleDisconnect(fn func(*Session)) {
	m.disconnectHandler = fn
}

// HandlePong fires fn when a pong is received from a session.
func (m *Melody) HandlePong(fn func(*Session)) {
	m.pongHandler = fn
}

// HandleMessage fires fn when a text message comes in.
func (m *Melody) HandleMessage(fn func(*Session, []byte)) {
	m.messageHandler = fn
}

// HandleMessageBinary fires fn when a binary message comes in.
func (m *Melody) HandleMessageBinary(fn func(*Session, []byte)) {
	m.messageHandlerBinary = fn
}

// HandleError fires fn when a session has an error.
func (m *Melody) HandleError(fn func(*Session, error)) {
	m.errorHandler = fn
}

// HandleClose sets the handler for close messages received from the session.
// The code argument to h is the received close code or CloseNoStatusReceived
// if the close message is empty. The default close handler sends a close frame
// back to the session.
//
// The application must read the connection to process close messages as
// described in the section on Control Frames above.
//
// The connection read methods return a CloseError when a close frame is
// received. Most applications should handle close messages as part of their
// normal error handling. Applications should only set a close handler when the
// application must perform some action before sending a close frame back to
// the session.
func (m *Melody) HandleClose(fn func(*Session, int, string) error) {
	if fn != nil {
		m.closeHandler = fn
	}
}

// HandleRequestWithKeys does the same as HandleRequest but populates session.Keys with keys.
func (m *Melody) HandleRequestWithKeys(w http.ResponseWriter, r *http.Request, keys map[string]interface{}) error {
	conn, err := m.Upgrader.Upgrade(w, r, w.Header())

	if err != nil {
		return err
	}

	session := &Session{
		Request:   r,
		conn:      conn,
		output:    make(chan *envelope, m.Config.MessageBufferSize),
		melody:    m,
		status:    StatusNormal,
		closeOnce: sync.Once{},
	}
	for k, v := range keys {
		session.Keys.Store(k, v)
	}
	atomic.AddUint32(&m.sessionCount, 1)

	m.connectHandler(session)

	go session.writePump()

	session.readPump()

	atomic.AddUint32(&m.sessionCount, ^uint32(0))

	session.close()

	m.disconnectHandler(session)

	return nil
}

// Len return the number of connected sessions.
func (m *Melody) Len() uint32 {
	return atomic.LoadUint32(&m.sessionCount)
}
