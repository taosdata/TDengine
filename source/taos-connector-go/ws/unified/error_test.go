package unified

import (
	"errors"
	"fmt"
	"testing"
)

// TestUnifiedErrorHelpers verifies the expected behavior for this scenario.
func TestUnifiedErrorHelpers(t *testing.T) {
	if !errors.Is(ErrUnifiedClosed, ErrUnifiedClosed) {
		t.Fatal("ErrUnifiedClosed should match itself")
	}
	if !IsErrorType(ErrUnifiedClosed, ErrorTypeClientClosed) {
		t.Fatal("ErrUnifiedClosed should be client_closed type")
	}
	if !IsConnectionRelatedError(ErrUnifiedClosed) {
		t.Fatal("ErrUnifiedClosed should be connection-related")
	}
	if !IsConnectionDisconnectedError(ErrUnifiedClosed) {
		t.Fatal("ErrUnifiedClosed should indicate disconnected")
	}
	if IsReconnectFailedError(ErrUnifiedClosed) {
		t.Fatal("ErrUnifiedClosed should not be reconnect_failed")
	}

	if !IsReconnectFailedError(ErrUnifiedConnectFailed) {
		t.Fatal("ErrUnifiedConnectFailed should indicate reconnect_failed")
	}
	if ErrorTypeOf(fmt.Errorf("x")) != ErrorTypeUnknown {
		t.Fatal("non unified errors should be unknown type")
	}
}
