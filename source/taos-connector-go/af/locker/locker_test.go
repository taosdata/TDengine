package locker

import (
	"runtime"
	"testing"
	"time"
)

// @author: xftan
// @date: 2022/1/25 16:38
// @description: test thread locker available
func TestLock(t *testing.T) {
	SetMaxThreadSize(runtime.NumCPU())
	timer := time.NewTimer(time.Second)
	done := make(chan struct{})
	go func() {
		Lock()
		Unlock()
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
