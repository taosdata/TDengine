package tmqhandle

import "C"
import (
	"container/list"
	"sync"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type FetchRawBlockResult struct {
	Code      int
	BlockSize int
	Block     unsafe.Pointer
}

type NewConsumerResult struct {
	Consumer unsafe.Pointer
	ErrStr   string
}

type GetTopicAssignmentResult struct {
	Code       int32
	Assignment []*Assignment
}

type ListTopicsResult struct {
	Code   int32
	Topics []string
}

type PollResult struct {
	Code   int32
	Res    unsafe.Pointer
	ErrStr string
}

type TMQCaller struct {
	PollResult               chan *PollResult
	FreeResult               chan struct{}
	CommitResult             chan int32
	SubscribeResult          chan int32
	UnsubscribeResult        chan int32
	ConsumerCloseResult      chan int32
	GetRawResult             chan int32
	OffsetSeekResult         chan int32
	FetchRawBlockResult      chan *FetchRawBlockResult
	NewConsumerResult        chan *NewConsumerResult
	GetJsonMetaResult        chan unsafe.Pointer
	GetTopicAssignmentResult chan *GetTopicAssignmentResult
	CommittedResult          chan int64
	PositionResult           chan int64
	ListTopicsResult         chan *ListTopicsResult
}

func NewTMQCaller() *TMQCaller {
	return &TMQCaller{
		PollResult:               make(chan *PollResult, 1),
		FreeResult:               make(chan struct{}, 1),
		CommitResult:             make(chan int32, 1),
		SubscribeResult:          make(chan int32, 1),
		UnsubscribeResult:        make(chan int32, 1),
		ConsumerCloseResult:      make(chan int32, 1),
		GetRawResult:             make(chan int32, 1),
		OffsetSeekResult:         make(chan int32, 1),
		FetchRawBlockResult:      make(chan *FetchRawBlockResult, 1),
		NewConsumerResult:        make(chan *NewConsumerResult, 1),
		GetJsonMetaResult:        make(chan unsafe.Pointer, 1),
		GetTopicAssignmentResult: make(chan *GetTopicAssignmentResult, 1),
		CommittedResult:          make(chan int64, 1),
		PositionResult:           make(chan int64, 1),
		ListTopicsResult:         make(chan *ListTopicsResult, 1),
	}
}

func (c *TMQCaller) PollCall(res unsafe.Pointer, code int32, errStr string) {
	c.PollResult <- &PollResult{
		Code:   code,
		Res:    res,
		ErrStr: errStr,
	}
}

func (c *TMQCaller) FreeCall() {
	c.FreeResult <- struct{}{}
}

func (c *TMQCaller) CommitCall(code int32) {
	c.CommitResult <- code
}

func (c *TMQCaller) FetchRawBlockCall(code int, blockSize int, block unsafe.Pointer) {
	c.FetchRawBlockResult <- &FetchRawBlockResult{
		Code:      code,
		BlockSize: blockSize,
		Block:     block,
	}
}

func (c *TMQCaller) NewConsumerCall(consumer unsafe.Pointer, errStr string) {
	c.NewConsumerResult <- &NewConsumerResult{
		Consumer: consumer,
		ErrStr:   errStr,
	}
}

func (c *TMQCaller) SubscribeCall(code int32) {
	c.SubscribeResult <- code
}

func (c *TMQCaller) UnsubscribeCall(code int32) {
	c.UnsubscribeResult <- code
}

func (c *TMQCaller) ConsumerCloseCall(code int32) {
	c.ConsumerCloseResult <- code
}

func (c *TMQCaller) GetRawCall(code int32) {
	c.GetRawResult <- code
}

func (c *TMQCaller) GetJsonMetaCall(meta unsafe.Pointer) {
	c.GetJsonMetaResult <- meta
}

func (c *TMQCaller) GetTopicAssignment(code int32, assignment []*Assignment) {
	if code != 0 {
		c.GetTopicAssignmentResult <- &GetTopicAssignmentResult{
			Code: code,
		}
		return
	}
	c.GetTopicAssignmentResult <- &GetTopicAssignmentResult{
		Code:       0,
		Assignment: assignment,
	}
}

func (c *TMQCaller) OffsetSeekCall(code int32) {
	c.OffsetSeekResult <- code
}

func (c *TMQCaller) CommittedCall(code int64) {
	c.CommittedResult <- code
}

func (c *TMQCaller) PositionCall(code int64) {
	c.PositionResult <- code
}

func (c *TMQCaller) ListTopicCall(code int32, topics []string) {
	c.ListTopicsResult <- &ListTopicsResult{
		Code:   code,
		Topics: topics,
	}
}

type poolReq struct {
	idleHandler *TMQHandler
}

type TMQHandlerPool struct {
	mu       sync.RWMutex
	count    int
	handlers chan *TMQHandler
	reqList  *list.List
}

type TMQHandler struct {
	Handler cgo.Handle
	Caller  *TMQCaller
}

func NewHandlerPool(count int) *TMQHandlerPool {
	c := &TMQHandlerPool{
		count:    count,
		handlers: make(chan *TMQHandler, count),
		reqList:  list.New(),
	}
	for i := 0; i < count; i++ {
		caller := NewTMQCaller()
		c.handlers <- &TMQHandler{
			Handler: cgo.NewHandle(caller),
			Caller:  caller,
		}
	}
	return c
}

func (c *TMQHandlerPool) Get() *TMQHandler {
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

func (c *TMQHandlerPool) Put(handler *TMQHandler) {
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

var GlobalTMQHandlerPoll = NewHandlerPool(10000)

type Assignment struct {
	VGroupID int32 `json:"vgroup_id"`
	Offset   int64 `json:"offset"`
	Begin    int64 `json:"begin"`
	End      int64 `json:"end"`
}
