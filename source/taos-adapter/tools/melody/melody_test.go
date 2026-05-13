package melody

import (
	"bytes"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNewMelody(t *testing.T) {
	handleConnect := handleSessionFunc(func(*Session) {})
	handleDisconnect := handleSessionFunc(func(*Session) {})
	handlePong := handleSessionFunc(func(*Session) {})
	handleMessage := handleMessageFunc(func(*Session, []byte) {})
	handleMessageBinary := handleMessageFunc(func(*Session, []byte) {})
	handleError := handleErrorFunc(func(*Session, error) {})
	handleClose := handleCloseFunc(func(*Session, int, string) error {
		return nil
	})
	melody := New()
	melody.HandleConnect(handleConnect)
	melody.HandleDisconnect(handleDisconnect)
	melody.HandlePong(handlePong)
	melody.HandleMessage(handleMessage)
	melody.HandleMessageBinary(handleMessageBinary)
	melody.HandleError(handleError)
	melody.HandleClose(handleClose)

	defaultConf := &Config{
		WriteWait:         60 * time.Second,
		PongWait:          60 * time.Second,
		PingPeriod:        (60 * time.Second * 9) / 10,
		MaxMessageSize:    0,
		MessageBufferSize: 1,
	}
	assert.Equal(t, defaultConf, melody.Config)
	assert.Equal(t, uint32(0), melody.sessionCount)
}

var TestMsg = []byte("test")

type TestServer struct {
	m *Melody
}

func NewTestServerHandler(handler handleMessageFunc) *TestServer {
	m := New()
	m.HandleMessage(handler)
	return &TestServer{
		m: m,
	}
}

func NewTestServer() *TestServer {
	m := New()
	return &TestServer{
		m: m,
	}
}

func (s *TestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = s.m.HandleRequestWithKeys(w, r, map[string]interface{}{"logger": logrus.New().WithField("test", "melody")})
}

func NewDialer(url string) (*websocket.Conn, error) {
	dialer := &websocket.Dialer{}
	conn, _, err := dialer.Dial(strings.Replace(url, "http", "ws", 1), nil)
	return conn, err
}

func MustNewDialer(url string) *websocket.Conn {
	conn, err := NewDialer(url)

	if err != nil {
		panic("could not dail websocket")
	}

	return conn
}

func TestEcho(t *testing.T) {
	ws := NewTestServerHandler(func(session *Session, msg []byte) {
		err := session.Write(msg)
		assert.NoError(t, err)
	})
	server := httptest.NewServer(ws)
	defer server.Close()

	fn := func(msg string) bool {
		conn := MustNewDialer(server.URL)
		defer func() {
			err := conn.Close()
			assert.NoError(t, err)
		}()

		err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
		assert.NoError(t, err)
		retType, ret, err := conn.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, retType)

		assert.Equal(t, msg, string(ret))

		return true
	}

	err := quick.Check(fn, nil)

	assert.Nil(t, err)
}

func TestEchoBinary(t *testing.T) {
	ws := NewTestServerHandler(func(session *Session, msg []byte) {
		err := session.WriteBinary(msg)
		assert.NoError(t, err)
	})
	server := httptest.NewServer(ws)
	defer server.Close()

	fn := func(msg string) bool {
		conn := MustNewDialer(server.URL)
		defer func() {
			err := conn.Close()
			assert.NoError(t, err)
		}()

		err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
		assert.NoError(t, err)
		retType, ret, err := conn.ReadMessage()

		assert.NoError(t, err)
		assert.Equal(t, websocket.BinaryMessage, retType)
		assert.True(t, bytes.Equal([]byte(msg), ret))

		return true
	}

	err := quick.Check(fn, nil)

	assert.Nil(t, err)
}

func TestWriteClosedServer(t *testing.T) {
	done := make(chan bool)

	ws := NewTestServer()

	server := httptest.NewServer(ws)
	defer server.Close()

	ws.m.HandleConnect(func(s *Session) {
		err := s.Close()
		assert.NoError(t, err)
	})

	ws.m.HandleDisconnect(func(s *Session) {
		err := s.Write([]byte("msg"))
		assert.NotNil(t, err)
		close(done)
	})

	conn := MustNewDialer(server.URL)
	_, _, err := conn.ReadMessage()
	assert.Error(t, err)
	defer func() {
		err = conn.Close()
		assert.Nil(t, err)
	}()

	<-done
}

func TestWriteClosedClient(t *testing.T) {
	done := make(chan bool)

	ws := NewTestServer()

	server := httptest.NewServer(ws)
	defer server.Close()

	ws.m.HandleDisconnect(func(s *Session) {
		err := s.Write([]byte("msg"))
		assert.Error(t, err)
		close(done)
	})

	conn := MustNewDialer(server.URL)
	err := conn.Close()
	assert.NoError(t, err)
	<-done
}

func TestUpgrader(t *testing.T) {
	ws := NewTestServer()
	ws.m.HandleMessage(func(session *Session, msg []byte) {
		err := session.Write(msg)
		assert.NoError(t, err)
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	ws.m.Upgrader = &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return false },
	}

	_, err := NewDialer(server.URL)

	assert.ErrorIs(t, err, websocket.ErrBadHandshake)
}

func TestLen(t *testing.T) {
	//nolint:staticcheck
	rand.Seed(time.Now().UnixNano())

	connect := int(rand.Int31n(100))
	disconnect := rand.Float32()
	conns := make([]*websocket.Conn, connect)

	defer func() {
		for _, conn := range conns {
			if conn != nil {
				err := conn.Close()
				assert.NoError(t, err)
			}
		}
	}()

	ws := NewTestServer()

	server := httptest.NewServer(ws)
	defer server.Close()

	disconnected := 0
	for i := 0; i < connect; i++ {
		conn := MustNewDialer(server.URL)

		if rand.Float32() < disconnect {
			conns[i] = nil
			disconnected++
			err := conn.Close()
			assert.NoError(t, err)
			continue
		}

		conns[i] = conn
	}

	time.Sleep(time.Millisecond)

	connected := connect - disconnected

	assert.Equal(t, uint32(connected), ws.m.Len())
}

func TestPingPong(t *testing.T) {
	done := make(chan bool)

	ws := NewTestServer()
	ws.m.Config.PingPeriod = time.Millisecond

	ws.m.HandlePong(func(s *Session) {
		close(done)
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()

	go func() {
		_, _, err := conn.NextReader()
		if err != nil {
			return
		}
	}()

	<-done
}

func TestHandleClose(t *testing.T) {
	done := make(chan bool)

	ws := NewTestServer()
	ws.m.Config.PingPeriod = time.Millisecond

	ws.m.HandleClose(func(s *Session, code int, text string) error {
		close(done)
		return nil
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)

	err := conn.WriteMessage(websocket.CloseMessage, nil)
	assert.NoError(t, err)
	<-done
}

func TestHandleError(t *testing.T) {
	done := make(chan bool)

	ws := NewTestServer()

	ws.m.HandleError(func(s *Session, err error) {
		var closeError *websocket.CloseError
		assert.ErrorAs(t, err, &closeError)
		close(done)
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)

	err := conn.Close()
	assert.NoError(t, err)
	<-done
}

func TestHandleErrorWrite(t *testing.T) {
	writeError := make(chan struct{})
	disconnect := make(chan struct{})

	ws := NewTestServer()
	ws.m.Config.WriteWait = 0
	ws.m.Config.PingPeriod = time.Millisecond * 100
	ws.m.Config.PongWait = time.Millisecond * 100
	ws.m.HandleConnect(func(s *Session) {
		err := s.Write(TestMsg)
		assert.Nil(t, err)
	})
	once := sync.Once{}

	ws.m.HandleError(func(s *Session, err error) {
		assert.NotNil(t, err)

		if os.IsTimeout(err) {
			once.Do(func() {
				close(writeError)
			})
		}
	})

	ws.m.HandleDisconnect(func(s *Session) {
		close(disconnect)
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()

	go func() {
		_, _, err := conn.NextReader()
		if err != nil {
			return
		}
	}()

	<-writeError
	<-disconnect
}

func TestErrSessionClosed(t *testing.T) {
	res := make(chan *Session)

	ws := NewTestServer()

	ws.m.HandleConnect(func(s *Session) {
		err := s.Close()
		assert.NoError(t, err)
	})

	ws.m.HandleDisconnect(func(s *Session) {
		res <- s
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()

	go func() {
		_, _, err := conn.ReadMessage()
		if err != nil {
			return
		}
	}()

	s := <-res

	assert.True(t, s.IsClosed())
	assert.ErrorIs(t, s.Write(TestMsg), ErrSessionClosed)
	assert.ErrorIs(t, s.WriteBinary(TestMsg), ErrSessionClosed)
	assert.ErrorIs(t, s.Close(), ErrSessionClosed)

	assert.ErrorIs(t, s.writeRaw(&envelope{}), ErrWriteClosed)
	s.writeMessage(&envelope{})
}

func TestSessionKeys(t *testing.T) {
	ws := NewTestServer()

	ws.m.HandleConnect(func(session *Session) {
		session.Set("stamp", time.Now().UnixNano())
	})
	ws.m.HandleMessage(func(session *Session, msg []byte) {
		stamp := session.MustGet("stamp").(int64)
		err := session.Write([]byte(strconv.Itoa(int(stamp))))
		assert.NoError(t, err)
	})
	server := httptest.NewServer(ws)
	defer server.Close()

	fn := func(msg string) bool {
		conn := MustNewDialer(server.URL)
		defer func() {
			err := conn.Close()
			assert.NoError(t, err)
		}()

		err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
		assert.NoError(t, err)
		_, ret, err := conn.ReadMessage()

		assert.NoError(t, err)

		stamp, err := strconv.Atoi(string(ret))

		assert.Nil(t, err)

		diff := int(time.Now().UnixNano()) - stamp

		assert.Greater(t, diff, 0)

		return true
	}

	assert.Nil(t, quick.Check(fn, nil))
}

func TestSessionKeysConcurrent(t *testing.T) {
	ss := make(chan *Session)

	ws := NewTestServer()

	ws.m.HandleConnect(func(s *Session) {
		ss <- s
	})

	server := httptest.NewServer(ws)
	defer server.Close()

	conn := MustNewDialer(server.URL)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()

	s := <-ss

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			s.Set("test", TestMsg)

			v1, exists := s.Get("test")

			assert.True(t, exists)
			assert.Equal(t, v1, TestMsg)

			v2 := s.MustGet("test")

			assert.Equal(t, v1, v2)

			wg.Done()
		}()
	}

	wg.Wait()

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			s.UnSet("test")

			_, exists := s.Get("test")

			assert.False(t, exists)

			wg.Done()
		}()
	}

	wg.Wait()
}

func TestConcurrentMessageHandling(t *testing.T) {
	testTimeout := func(cmh bool, msgType int) bool {
		base := time.Millisecond * 100
		done := make(chan struct{})

		handler := func(s *Session, msg []byte) {
			if len(msg) == 0 {
				done <- struct{}{}
				return
			}

			time.Sleep(base * 2)
		}

		messageHandler := func(s *Session, msg []byte) {
			if cmh {
				go handler(s, msg)
			} else {
				handler(s, msg)
			}
		}

		ws := NewTestServerHandler(func(session *Session, msg []byte) {})
		if msgType == websocket.TextMessage {
			ws.m.HandleMessage(messageHandler)
		} else {
			ws.m.HandleMessageBinary(messageHandler)
		}
		ws.m.Config.PingPeriod = base / 2
		ws.m.Config.PongWait = base

		var errorSet uint32
		ws.m.HandleError(func(s *Session, err error) {
			atomic.StoreUint32(&errorSet, 1)
			done <- struct{}{}
		})

		server := httptest.NewServer(ws)
		defer server.Close()

		conn := MustNewDialer(server.URL)
		defer func() {
			err := conn.Close()
			assert.NoError(t, err)
		}()

		err := conn.WriteMessage(msgType, TestMsg)
		assert.NoError(t, err)
		err = conn.WriteMessage(msgType, TestMsg)
		assert.NoError(t, err)

		time.Sleep(base / 4)

		err = conn.WriteMessage(msgType, nil)
		assert.NoError(t, err)

		<-done

		return atomic.LoadUint32(&errorSet) != 0
	}

	t.Run("text should error", func(t *testing.T) {
		errorSet := testTimeout(false, websocket.TextMessage)

		if !errorSet {
			t.FailNow()
		}
	})

	t.Run("text should not error", func(t *testing.T) {
		errorSet := testTimeout(true, websocket.TextMessage)

		if errorSet {
			t.FailNow()
		}
	})

	t.Run("binary should error", func(t *testing.T) {
		errorSet := testTimeout(false, websocket.BinaryMessage)

		if !errorSet {
			t.FailNow()
		}
	})

	t.Run("binary should not error", func(t *testing.T) {
		errorSet := testTimeout(true, websocket.BinaryMessage)

		if errorSet {
			t.FailNow()
		}
	})
}
