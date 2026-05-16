package common

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	BufferSize4M          = 4 * 1024 * 1024
	DefaultMessageTimeout = time.Minute * 5
	DefaultPongWait       = 60 * time.Second
	DefaultPingPeriod     = (60 * time.Second * 9) / 10
	DefaultWriteWait      = 10 * time.Second
)

var DefaultDialer = websocket.Dialer{
	Proxy:            http.ProxyFromEnvironment,
	HandshakeTimeout: 45 * time.Second,
	ReadBufferSize:   BufferSize4M,
	WriteBufferSize:  BufferSize4M,
	WriteBufferPool:  &sync.Pool{},
}
