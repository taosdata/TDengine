package handler

import (
	"container/list"
	"sync"
	"unsafe"

	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

type AsyncResult struct {
	Res unsafe.Pointer
	N   int
}
type Caller struct {
	QueryResult chan *AsyncResult
	FetchResult chan *AsyncResult
}

func NewCaller() *Caller {
	return &Caller{
		QueryResult: make(chan *AsyncResult, 1),
		FetchResult: make(chan *AsyncResult, 1),
	}
}

func (c *Caller) QueryCall(res unsafe.Pointer, code int) {
	c.QueryResult <- &AsyncResult{
		Res: res,
		N:   code,
	}
}

func (c *Caller) FetchCall(res unsafe.Pointer, numOfRows int) {
	c.FetchResult <- &AsyncResult{
		Res: res,
		N:   numOfRows,
	}
}

type poolReq struct {
	idleHandler *Handler
}

type HandlerPool struct {
	mu       sync.RWMutex
	count    int
	handlers chan *Handler
	reqList  *list.List
}

type Handler struct {
	Handler cgo.Handle
	Caller  *Caller
}

func NewHandlerPool(count int) *HandlerPool {
	c := &HandlerPool{
		count:    count,
		handlers: make(chan *Handler, count),
		reqList:  list.New(),
	}
	for i := 0; i < count; i++ {
		caller := NewCaller()
		c.handlers <- &Handler{
			Handler: cgo.NewHandle(caller),
			Caller:  caller,
		}
	}
	return c
}

func (c *HandlerPool) Get() *Handler {
	for {
		select {
		case wrapConn := <-c.handlers:
			return wrapConn
		default:
			c.mu.Lock()
			req := make(chan poolReq, 1)
			c.reqList.PushBack(req)
			c.mu.Unlock()
			ret := <-req
			return ret.idleHandler
		}
	}
}

func (c *HandlerPool) Put(handler *Handler) {
	c.mu.Lock()
	e := c.reqList.Front()
	if e != nil {
		req := e.Value.(chan poolReq)
		c.reqList.Remove(e)
		req <- poolReq{
			idleHandler: handler,
		}
		c.mu.Unlock()
		return
	}
	c.handlers <- handler
	c.mu.Unlock()
}
