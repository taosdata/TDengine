package unified

import (
	"reflect"
	"sync"
	"testing"
)

// TestFailoverStateInitialCandidates verifies the expected behavior for this scenario.
func TestFailoverStateInitialCandidates(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	state, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	addEndpointConnCountForTest(t, "ws://a:1/ws", 2)
	addEndpointConnCountForTest(t, "ws://b:2/ws", 1)

	got := state.initialCandidates()
	want := []endpointCandidate{
		{Index: 2, URL: "ws://c:3/ws"},
		{Index: 1, URL: "ws://b:2/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestFailoverStateReconnectCandidates verifies the expected behavior for this scenario.
func TestFailoverStateReconnectCandidates(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	state, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.markActive(1); err != nil {
		t.Fatal(err)
	}

	got := state.reconnectCandidates()
	want := []endpointCandidate{
		{Index: 1, URL: "ws://b:2/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
		{Index: 2, URL: "ws://c:3/ws"},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestFailoverStateReconnectCandidatesActiveFirstRegardlessOfConnectionCount verifies the expected behavior for this scenario.
func TestFailoverStateReconnectCandidatesActiveFirstRegardlessOfConnectionCount(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	state, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.markActive(1); err != nil {
		t.Fatal(err)
	}

	addEndpointConnCountForTest(t, "ws://a:1/ws", 1)
	addEndpointConnCountForTest(t, "ws://b:2/ws", 10)
	addEndpointConnCountForTest(t, "ws://c:3/ws", 2)

	got := state.reconnectCandidates()
	want := []endpointCandidate{
		{Index: 1, URL: "ws://b:2/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
		{Index: 2, URL: "ws://c:3/ws"},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// TestFailoverStateDoesNotCrossClientEndpointSet verifies the expected behavior for this scenario.
func TestFailoverStateDoesNotCrossClientEndpointSet(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	state1, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws"})
	if err != nil {
		t.Fatal(err)
	}
	state2, err := newFailoverState([]string{"ws://a:1/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	addEndpointConnCountForTest(t, "ws://a:1/ws", 2)
	addEndpointConnCountForTest(t, "ws://b:2/ws", 1)

	got1 := state1.initialCandidates()
	want1 := []endpointCandidate{
		{Index: 1, URL: "ws://b:2/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
	}
	if !reflect.DeepEqual(want1, got1) {
		t.Fatalf("state1 want %v, got %v", want1, got1)
	}

	got2 := state2.initialCandidates()
	want2 := []endpointCandidate{
		{Index: 1, URL: "ws://c:3/ws"},
		{Index: 0, URL: "ws://a:1/ws"},
	}
	if !reflect.DeepEqual(want2, got2) {
		t.Fatalf("state2 want %v, got %v", want2, got2)
	}
}

// TestGlobalHostPortConnCounterConcurrentIncDec verifies the expected behavior for this scenario.
func TestGlobalHostPortConnCounterConcurrentIncDec(t *testing.T) {
	resetGlobalConnCounterForTest(t)
	key := hostPortKeyForEndpointForTest(t, "ws://a:1/ws")

	var wg sync.WaitGroup
	const workers = 8
	const loops = 1000
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				globalHostPortConnCounts.inc(key)
			}
			for j := 0; j < loops; j++ {
				globalHostPortConnCounts.dec(key)
			}
		}()
	}
	wg.Wait()

	if got := globalHostPortConnCounts.get(key); got != 0 {
		t.Fatalf("want count 0, got %d", got)
	}
	for i := 0; i < 10; i++ {
		globalHostPortConnCounts.dec(key)
	}
	if got := globalHostPortConnCounts.get(key); got != 0 {
		t.Fatalf("count should not go below zero, got %d", got)
	}
}

// TestFailoverStateMarkActiveAndActive verifies the expected behavior for this scenario.
func TestFailoverStateMarkActiveAndActive(t *testing.T) {
	state, err := newFailoverState([]string{"ws://a:1/ws", "ws://b:2/ws", "ws://c:3/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.markActive(2); err != nil {
		t.Fatal(err)
	}
	active := state.active()
	if active.Index != 2 || active.URL != "ws://c:3/ws" {
		t.Fatalf("unexpected active: %+v", active)
	}
}

// TestFailoverStateMarkActiveInvalidIndex verifies the expected behavior for this scenario.
func TestFailoverStateMarkActiveInvalidIndex(t *testing.T) {
	state, err := newFailoverState([]string{"ws://a:1/ws"})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.markActive(2); err == nil {
		t.Fatal("expect invalid endpoint index error")
	}
}
