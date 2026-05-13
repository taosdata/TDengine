package unified

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotifyPendingRequestClosedPreservesBufferedResponse(t *testing.T) {
	ch := make(chan []byte, 1)
	want := []byte(`{"req_id":1}`)
	ch <- want

	notifyPendingRequestClosed(&pendingRequest{reqID: 1, channel: ch})

	select {
	case got := <-ch:
		require.Equal(t, want, got)
	default:
		t.Fatal("expected buffered response to remain available")
	}
}

func TestNotifyPendingRequestClosedKeepsNilMarkerBuffered(t *testing.T) {
	ch := make(chan []byte, 1)
	ch <- nil

	notifyPendingRequestClosed(&pendingRequest{reqID: 2, channel: ch})

	select {
	case got := <-ch:
		require.Nil(t, got)
	default:
		t.Fatal("expected nil marker to remain buffered")
	}
}
