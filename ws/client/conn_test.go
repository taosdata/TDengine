package client

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"

	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

func TestEnvelopePool(t *testing.T) {
	pool := &EnvelopePool{}

	// Test Get method
	env := pool.Get()
	assert.NotNil(t, env)
	assert.NotNil(t, env.Msg)

	// Test Put method
	env.Msg.WriteString("test")
	pool.Put(env)

	// Test if the envelope is reset after put
	env = pool.Get()
	assert.Equal(t, 0, env.Msg.Len())
}

func TestEnvelope_Reset(t *testing.T) {
	env := &Envelope{
		Type: 1,
		Msg:  bytes.NewBufferString("test"),
	}

	env.Reset()

	assert.Equal(t, 0, env.Msg.Len())
}

func TestNotifyEnvelopeError(t *testing.T) {
	t.Run("replace stale value when channel is full", func(t *testing.T) {
		env := &Envelope{
			ErrorChan: make(chan error, 1),
		}
		stale := errors.New("stale")
		want := errors.New("new")
		env.ErrorChan <- stale

		done := make(chan struct{})
		go func() {
			notifyEnvelopeError(env, want)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("notifyEnvelopeError should not block on full channel")
		}

		select {
		case got := <-env.ErrorChan:
			assert.Equal(t, want, got)
		default:
			t.Fatal("expected error value in channel")
		}
	})

	t.Run("nil safe", func(t *testing.T) {
		assert.NotPanics(t, func() {
			notifyEnvelopeError(nil, errors.New("ignored"))
			notifyEnvelopeError(&Envelope{}, errors.New("ignored"))
		})
	})
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func wsEchoServer(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if err := conn.WriteMessage(messageType, message); err != nil {
			return
		}
	}
}

func wsCloseServer(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	time.Sleep(200 * time.Millisecond)
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"),
		time.Now().Add(time.Second),
	)
	_ = conn.Close()
}

func TestClient(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(wsEchoServer))
	defer s.Close()
	t.Log(s.URL)
	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	assert.NoError(t, err)
	c := NewClient(ws, 1)
	gotMessage := make(chan struct{})
	c.TextMessageHandler = func(message []byte) {
		assert.Equal(t, "test", string(message))
		gotMessage <- struct{}{}
	}
	running := c.IsRunning()
	assert.True(t, running)
	defer c.Close()
	go c.ReadPump()
	go c.WritePump()
	env := c.GetEnvelope()
	env.Type = websocket.TextMessage
	env.Msg.WriteString("test")
	err = c.Send(env)
	assert.NoError(t, err)
	env = c.GetEnvelope()
	c.PutEnvelope(env)
	timeout := time.NewTimer(time.Second * 3)
	select {
	case <-gotMessage:
		t.Log("got message")
	case <-timeout.C:
		t.Error("timeout")
	}
	c.Close()
	env = c.GetEnvelope()
	err = c.Send(env)
	assert.Equal(t, ClosedError, err)
}

func TestClientDoneWithServerClose(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(wsCloseServer))
	defer s.Close()
	ep := "ws" + strings.TrimPrefix(s.URL, "http")
	ws, _, err := websocket.DefaultDialer.Dial(ep, nil)
	assert.NoError(t, err)
	c := NewClient(ws, 1)
	defer c.Close()
	go c.ReadPump()
	go c.WritePump()

	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("client done timeout")
	}
	assert.False(t, c.IsRunning())
	assert.Error(t, c.LastError())
}

func TestClientLastErrorAfterClose(t *testing.T) {
	c := NewClient(nil, 1)
	c.Close()
	assert.Equal(t, ClosedError, c.LastError())
	select {
	case <-c.Done():
	default:
		t.Fatal("done channel should be closed after close")
	}
}

func TestClientCloseUnblocksBlockedSend(t *testing.T) {
	c := NewClient(nil, 1)
	first := c.GetEnvelope()
	second := c.GetEnvelope()
	defer c.PutEnvelope(first)
	defer c.PutEnvelope(second)

	assert.NoError(t, c.Send(first))

	result := make(chan error, 1)
	go func() {
		result <- c.Send(second)
	}()

	c.Close()

	select {
	case err := <-result:
		assert.Equal(t, ClosedError, err)
	case <-time.After(time.Second):
		t.Fatal("blocked send was not released by close")
	}
}

func TestClientHandleErrorRejectsNewSends(t *testing.T) {
	c := NewClient(nil, 1)
	env := c.GetEnvelope()
	defer c.PutEnvelope(env)

	specificErr := errors.New("write failed")
	c.handleError(specificErr)

	assert.False(t, c.IsRunning())
	assert.Equal(t, specificErr, c.LastError())

	err := c.Send(env)
	assert.Equal(t, ClosedError, err)
	assert.Len(t, c.sendChan, 0)

	select {
	case <-c.Done():
	default:
		t.Fatal("done channel should be closed after handleError")
	}
}

func TestClientSetErrorHandler(t *testing.T) {
	c := NewClient(nil, 1)
	specificErr := errors.New("write failed")
	called := false

	c.SetErrorHandler(func(err error) {
		called = true
		assert.Equal(t, specificErr, err)
	})
	c.handleError(specificErr)

	assert.True(t, called, "custom error handler should be called")
}

func TestClientSetErrorHandlerNilSafe(t *testing.T) {
	c := NewClient(nil, 1)
	c.SetErrorHandler(nil)

	assert.NotPanics(t, func() {
		c.handleError(errors.New("write failed"))
	})
}

func TestClientDrainSendChanNonBlockingWithFullErrorChan(t *testing.T) {
	c := NewClient(nil, 1)
	env := c.GetEnvelope()
	defer c.PutEnvelope(env)
	env.ErrorChan <- errors.New("stale")
	c.sendChan <- env
	close(c.sendChan)

	done := make(chan struct{})
	go func() {
		c.drainSendChan()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drainSendChan should not block on full envelope error channel")
	}

	select {
	case err := <-env.ErrorChan:
		assert.Equal(t, ClosedError, err)
	default:
		t.Fatal("expected closed error notification")
	}
}

func TestWrapClosedError(t *testing.T) {
	cause := &net.OpError{Op: "read", Err: io.ErrUnexpectedEOF}

	err := WrapClosedError(ClosedError, cause)

	assert.True(t, errors.Is(err, ClosedError))
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF))
	var opErr *net.OpError
	assert.True(t, errors.As(err, &opErr))
	assert.Same(t, cause, opErr)
}

func TestWrapClosedErrorWithoutCause(t *testing.T) {
	assert.Equal(t, ClosedError, WrapClosedError(ClosedError, nil))
}

func TestHandleResponseError(t *testing.T) {
	t.Run("Error not nil", func(t *testing.T) {
		err := errors.New("some error")
		result := HandleResponseError(err, 0, "ignored message")
		assert.Equal(t, err, result, "Expected the original error to be returned")
	})

	t.Run("Error nil and non-zero code", func(t *testing.T) {
		code := 123
		msg := "some error message"
		expectedErr := taosErrors.NewError(code, msg)

		result := HandleResponseError(nil, code, msg)
		assert.EqualError(t, result, expectedErr.Error(), "Expected a new error to be returned based on code and message")
	})

	t.Run("Error nil and zero code", func(t *testing.T) {
		result := HandleResponseError(nil, 0, "ignored message")
		assert.Nil(t, result, "Expected nil to be returned when there is no error and code is zero")
	})
}
