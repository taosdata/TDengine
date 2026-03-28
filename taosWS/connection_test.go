package taosWS

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	osexec "os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	wsClient "github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

func TestBadConnection(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			// bad connection should not panic
			t.Fatalf("panic: %v", r)
		}
	}()

	cfg, err := ParseDSN(dataSourceName)
	if err != nil {
		t.Fatalf("ParseDSN error: %v", err)
	}
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	rawConn, err := (&connector{cfg: cfg}).Connect(context.Background())
	if err != nil {
		t.Fatalf("connector connect error: %v", err)
	}
	conn, ok := rawConn.(*taosConn)
	if !ok {
		t.Fatalf("unexpected connection type: %T", rawConn)
	}

	// to test bad connection, we manually close the connection
	err = conn.Close()
	if err != nil {
		t.Fatalf("close error: %v", err)
	}

	_, err = conn.QueryContext(context.Background(), "select 1", nil)
	if err == nil {
		t.Fatalf("query should fail")
	}
}

func TestMapUnifiedConnError(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		assert.NoError(t, mapUnifiedConnError(nil))
	})

	t.Run("plain error", func(t *testing.T) {
		in := assert.AnError
		assert.Equal(t, in, mapUnifiedConnError(in))
	})

	t.Run("closed error", func(t *testing.T) {
		err := mapUnifiedConnError(wsClient.ClosedError)
		assert.Error(t, err)
		assert.ErrorIs(t, err, driver.ErrBadConn)
	})

	t.Run("net closed", func(t *testing.T) {
		err := mapUnifiedConnError(&net.OpError{
			Op:  "read",
			Net: "tcp",
			Err: errors.New("use of closed network connection"),
		})
		assert.Error(t, err)
		assert.ErrorIs(t, err, driver.ErrBadConn)
	})

	t.Run("websocket close", func(t *testing.T) {
		err := mapUnifiedConnError(&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "closed"})
		assert.Error(t, err)
		assert.ErrorIs(t, err, driver.ErrBadConn)
	})

	t.Run("unified closed", func(t *testing.T) {
		err := mapUnifiedConnError(unified.ErrUnifiedClosed)
		assert.Error(t, err)
		assert.ErrorIs(t, err, driver.ErrBadConn)
	})
}

func TestIsNetOrWebsocketError(t *testing.T) {
	assert.True(t, isNetOrWebsocketError(&net.OpError{Op: "read"}))
	assert.True(t, isNetOrWebsocketError(&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "bye"}))
	assert.False(t, isNetOrWebsocketError(errors.New("plain")))
}

func TestIllegalSQLReturnsDriverErrorType(t *testing.T) {
	cfg, err := ParseDSN(dataSourceName)
	if err != nil {
		t.Fatalf("ParseDSN error: %v", err)
	}
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	rawConn, err := (&connector{cfg: cfg}).Connect(context.Background())
	if err != nil {
		t.Fatalf("connector connect error: %v", err)
	}
	conn, ok := rawConn.(*taosConn)
	if !ok {
		t.Fatalf("unexpected connection type: %T", rawConn)
	}
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.ExecContext(context.Background(), "xxxxxxx inot", nil)
	if assert.Error(t, err) {
		var terr *taosErrors.TaosError
		assert.ErrorAs(t, err, &terr)
		var unifiedErr *unified.Error
		assert.False(t, errors.As(err, &unifiedErr))
		assert.NotErrorIs(t, err, driver.ErrBadConn)
	}
}

func TestBegin(t *testing.T) {
	cfg, err := ParseDSN(dataSourceName)
	if err != nil {
		t.Fatalf("ParseDSN error: %v", err)
	}
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	rawConn, err := (&connector{cfg: cfg}).Connect(context.Background())
	if err != nil {
		t.Fatalf("connector connect error: %v", err)
	}
	conn, ok := rawConn.(*taosConn)
	if !ok {
		t.Fatalf("unexpected connection type: %T", rawConn)
	}
	defer func() {
		err = conn.Close()
		assert.NoError(t, err)
	}()

	tx, err := conn.Begin()
	assert.Error(t, err)
	assert.Nil(t, tx)
}

func newTaosadapter(port string) *osexec.Cmd {
	command := "taosadapter"
	if runtime.GOOS == "windows" {
		command = "C:\\TDengine\\taosadapter.exe"
	}
	return osexec.Command(command, "--port", port, "--log.level", "debug")
}

func startTaosadapter(cmd *osexec.Cmd, port string) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
		if err != nil {
			continue
		}
		_ = resp.Body.Close()
		time.Sleep(time.Second)
		return nil
	}
	if cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGINT)
		_, _ = cmd.Process.Wait()
		cmd.Process = nil
	}
	return errors.New("taosadapter start failed")
}

func stopTaosadapter(cmd *osexec.Cmd, port string) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGINT)
	_, _ = cmd.Process.Wait()
	cmd.Process = nil
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 100)
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/-/ping", port))
		if err != nil {
			return
		}
		_ = resp.Body.Close()
		time.Sleep(time.Second)
	}
	panic("taosadapter stop failed")
}

func getAvailablePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on ephemeral port: %v", err)
	}
	defer func() {
		_ = listener.Close()
	}()
	return strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
}

func TestDisconnectNoReadTimeout(t *testing.T) {
	port := getAvailablePort(t)
	cmd := newTaosadapter(port)
	err := startTaosadapter(cmd, port)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopTaosadapter(cmd, port)
	}()
	dsn := fmt.Sprintf("%s:%s@ws(%s:%s)/", user, password, host, port)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ReadTimeout = 10 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	rawConn, err := (&connector{cfg: cfg}).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn, ok := rawConn.(*taosConn)
	if !ok {
		t.Fatalf("unexpected connection type: %T", rawConn)
	}
	defer func() {
		_ = conn.Close()
	}()
	stopTaosadapter(cmd, port)
	start := time.Now()
	_, err = conn.QueryContext(context.Background(), "select 1", nil)
	if assert.Error(t, err) {
		assert.NotContains(t, strings.ToLower(err.Error()), "read timeout")
	}
	assert.Less(t, time.Since(start), cfg.ReadTimeout)
}
