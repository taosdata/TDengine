package async

import (
	"testing"
	"time"
)

// @author: xftan
// @date: 2022/1/25 16:33
// @description: test async handler pool available
func TestHandler(t *testing.T) {
	SetHandlerSize(12)
	timer := time.NewTimer(time.Second)
	done := make(chan struct{})
	go func() {
		handler := GetHandler()
		PutHandler(handler)
		done <- struct{}{}
	}()
	select {
	case <-done:
		return
	case <-timer.C:
		timer.Stop()
		timer = nil
		t.Error("dead lock")
		return
	}
}
