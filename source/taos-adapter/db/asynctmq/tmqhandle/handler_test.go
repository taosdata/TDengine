package tmqhandle

import (
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestTMQHandlerPool(t *testing.T) {
	poolSize := 10
	handlerPool := NewHandlerPool(poolSize)

	// Test Get() and Put()
	for i := 0; i < poolSize; i++ {
		handler := handlerPool.Get()
		if handler == nil {
			t.Error("Get handler from empty pool error")
		} else if handler.Handler == 0 || handler.Caller == nil {
			t.Error("Get handler from pool with invalid caller error")
		}

		caller := handler.Caller
		pollRes := unsafe.Pointer(&struct{}{})
		caller.PollCall(pollRes, 0, "")
		res := <-caller.PollResult
		if res.Res != pollRes {
			t.Errorf("PollCall failed")
		}

		handlerPool.Put(handler)
	}

	// Test Pending requests
	l := make([]*TMQHandler, poolSize)
	for i := 0; i < poolSize; i++ {
		l[i] = handlerPool.Get()
	}
	finish := make(chan struct{})
	go func() {
		h := handlerPool.Get()
		handlerPool.Put(h)
		finish <- struct{}{}
	}()
	time.Sleep(time.Second)
	handlerPool.Put(l[0])
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	select {
	case <-ctx.Done():
		t.Error("wait for get handler timeout")
	case <-finish:
		cancel()
	}
	for i := 1; i < poolSize; i++ {
		handlerPool.Put(l[i])
	}
	handlerPool = NewHandlerPool(1)
	handle := handlerPool.Get()
	go func() {
		time.Sleep(time.Second)
		handlerPool.Put(handle)
	}()
	handle2 := handlerPool.Get()
	assert.NotNil(t, handle2)
}

func TestTMQCaller(t *testing.T) {
	caller := NewTMQCaller()

	// Test PollCall
	a := 1
	res := unsafe.Pointer(&a)
	caller.PollCall(res, 0, "")
	pollRes := <-caller.PollResult
	if pollRes.Res != res {
		t.Error("PollCall failed")
	}

	// Test FreeCall
	go func() { caller.FreeCall() }()
	<-caller.FreeResult

	// Test CommitCall
	code := int32(123)
	caller.CommitCall(code)
	if <-caller.CommitResult != code {
		t.Error("CommitCall failed")
	}

	// Test FetchRawBlockCall
	b := 2
	frbr := &FetchRawBlockResult{
		Code:      456,
		BlockSize: 789,
		Block:     unsafe.Pointer(&b),
	}
	caller.FetchRawBlockCall(frbr.Code, frbr.BlockSize, frbr.Block)
	result := <-caller.FetchRawBlockResult
	if result.Code != frbr.Code || result.BlockSize != frbr.BlockSize || result.Block != frbr.Block {
		t.Error("FetchRawBlockCall failed")
	}

	// Test NewConsumerCall
	c := 3
	consumer := unsafe.Pointer(&c)
	errStr := "some error"
	caller.NewConsumerCall(consumer, errStr)
	result2 := <-caller.NewConsumerResult
	if result2.Consumer != consumer || result2.ErrStr != errStr {
		t.Error("NewConsumerCall failed")
	}

	// Test SubscribeCall
	code2 := int32(456)
	caller.SubscribeCall(code2)
	if <-caller.SubscribeResult != code2 {
		t.Error("SubscribeCall failed")
	}

	// Test UnsubscribeCall
	code3 := int32(789)
	caller.UnsubscribeCall(code3)
	if <-caller.UnsubscribeResult != code3 {
		t.Error("UnsubscribeCall failed")
	}

	// Test ConsumerCloseCall
	code4 := int32(101112)
	caller.ConsumerCloseCall(code4)
	if <-caller.ConsumerCloseResult != code4 {
		t.Error("ConsumerCloseCall failed")
	}

	// Test GetRawCall
	code5 := int32(131415)
	caller.GetRawCall(code5)
	if <-caller.GetRawResult != code5 {
		t.Error("GetRawCall failed")
	}

	// Test GetJsonMetaCall
	d := 4
	meta := unsafe.Pointer(&d)
	caller.GetJsonMetaCall(meta)
	if <-caller.GetJsonMetaResult != meta {
		t.Error("GetJsonMetaCall failed")
	}
}
