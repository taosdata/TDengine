package unified

import (
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type endpointCandidate struct {
	Index int
	URL   string
}

type hostPortConnectionCounter struct {
	lock   sync.RWMutex
	counts map[string]*int64
}

func newHostPortConnectionCounter() *hostPortConnectionCounter {
	return &hostPortConnectionCounter{
		counts: make(map[string]*int64),
	}
}

func (c *hostPortConnectionCounter) getOrCreate(key string) *int64 {
	c.lock.RLock()
	ptr, ok := c.counts[key]
	c.lock.RUnlock()
	if ok {
		return ptr
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	ptr, ok = c.counts[key]
	if ok {
		return ptr
	}
	ptr = new(int64)
	c.counts[key] = ptr
	return ptr
}

func (c *hostPortConnectionCounter) inc(key string) int64 {
	ptr := c.getOrCreate(key)
	return atomic.AddInt64(ptr, 1)
}

func (c *hostPortConnectionCounter) dec(key string) int64 {
	ptr := c.getOrCreate(key)
	for {
		current := atomic.LoadInt64(ptr)
		if current <= 0 {
			return 0
		}
		next := current - 1
		if atomic.CompareAndSwapInt64(ptr, current, next) {
			return next
		}
	}
}

func (c *hostPortConnectionCounter) get(key string) int64 {
	c.lock.RLock()
	ptr, ok := c.counts[key]
	c.lock.RUnlock()
	if !ok {
		return 0
	}
	return atomic.LoadInt64(ptr)
}

func (c *hostPortConnectionCounter) reset() {
	c.lock.Lock()
	c.counts = make(map[string]*int64)
	c.lock.Unlock()
}

var globalHostPortConnCounts = newHostPortConnectionCounter()

func endpointHostPortKey(endpointURL string) (string, error) {
	u, err := url.Parse(endpointURL)
	if err != nil || u.Host == "" {
		return "", newInvalidConfigErrorf("invalid websocket endpoint: %s", endpointURL)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "ws" && scheme != "wss" {
		return "", newInvalidConfigErrorf("invalid websocket endpoint scheme: %s", endpointURL)
	}
	host := u.Hostname()
	if host == "" {
		return "", newInvalidConfigErrorf("invalid websocket endpoint: %s", endpointURL)
	}
	port := u.Port()
	if port == "" {
		if scheme == "wss" {
			port = strconv.Itoa(443)
		} else {
			port = strconv.Itoa(80)
		}
	}
	return net.JoinHostPort(host, port), nil
}

// failoverState stores active endpoint and candidate order for initial connect/reconnect.
type failoverState struct {
	endpoints         []string
	endpointHostPorts []string
	activeIndex       int
	lock              sync.RWMutex
}

// newFailoverState initializes failover state with a copied endpoint list.
func newFailoverState(endpoints []string) (*failoverState, error) {
	if len(endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	copyEndpoints := make([]string, len(endpoints))
	copy(copyEndpoints, endpoints)
	hostPorts := make([]string, len(copyEndpoints))
	for i := 0; i < len(copyEndpoints); i++ {
		hostPort, err := endpointHostPortKey(copyEndpoints[i])
		if err != nil {
			return nil, err
		}
		hostPorts[i] = hostPort
	}
	return &failoverState{
		endpoints:         copyEndpoints,
		endpointHostPorts: hostPorts,
		activeIndex:       0,
	}, nil
}

// endpointsCopy returns a copy of configured endpoints.
func (s *failoverState) endpointsCopy() []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	out := make([]string, len(s.endpoints))
	copy(out, s.endpoints)
	return out
}

// active returns the currently selected endpoint candidate.
func (s *failoverState) active() endpointCandidate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return endpointCandidate{
		Index: s.activeIndex,
		URL:   s.endpoints[s.activeIndex],
	}
}

func (s *failoverState) hostPortByIndex(index int) (string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if index < 0 || index >= len(s.endpointHostPorts) {
		return "", ErrInvalidEndpointIndex
	}
	return s.endpointHostPorts[index], nil
}

// markActive updates the active endpoint index.
func (s *failoverState) markActive(index int) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if index < 0 || index >= len(s.endpoints) {
		return ErrInvalidEndpointIndex
	}
	s.activeIndex = index
	return nil
}

// initialCandidates returns endpoints ordered by global least-connections.
func (s *failoverState) initialCandidates() []endpointCandidate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.leastConnectionCandidatesLocked(-1)
}

// reconnectCandidates returns endpoints for reconnect attempts.
// The active endpoint is always tried first to avoid unnecessary switch-away
// during transient network glitches. Remaining endpoints are ordered by
// least-connections.
func (s *failoverState) reconnectCandidates() []endpointCandidate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	size := len(s.endpoints)
	if size == 0 {
		return nil
	}

	activeIndex := s.activeIndex
	candidates := make([]endpointCandidate, 0, size)
	if activeIndex >= 0 && activeIndex < size {
		candidates = append(candidates, endpointCandidate{
			Index: activeIndex,
			URL:   s.endpoints[activeIndex],
		})
	}

	if size == 1 {
		return candidates
	}

	indices := make([]int, 0, size-1)
	counts := make([]int64, size)
	for i := 0; i < size; i++ {
		counts[i] = globalHostPortConnCounts.get(s.endpointHostPorts[i])
		if i == activeIndex {
			continue
		}
		indices = append(indices, i)
	}
	sort.SliceStable(indices, func(i, j int) bool {
		left := indices[i]
		right := indices[j]
		if counts[left] != counts[right] {
			return counts[left] < counts[right]
		}
		return left < right
	})
	for i := 0; i < len(indices); i++ {
		idx := indices[i]
		candidates = append(candidates, endpointCandidate{
			Index: idx,
			URL:   s.endpoints[idx],
		})
	}
	return candidates
}

// leastConnectionCandidatesLocked returns all endpoints sorted by host:port connection count.
// activeLastIndex, when >= 0, is moved to the end regardless of count.
func (s *failoverState) leastConnectionCandidatesLocked(activeLastIndex int) []endpointCandidate {
	size := len(s.endpoints)
	counts := make([]int64, size)
	indices := make([]int, size)
	for i := 0; i < size; i++ {
		indices[i] = i
		counts[i] = globalHostPortConnCounts.get(s.endpointHostPorts[i])
	}
	sort.SliceStable(indices, func(i, j int) bool {
		left := indices[i]
		right := indices[j]
		if activeLastIndex >= 0 {
			if left == activeLastIndex && right != activeLastIndex {
				return false
			}
			if right == activeLastIndex && left != activeLastIndex {
				return true
			}
		}
		if counts[left] != counts[right] {
			return counts[left] < counts[right]
		}
		return left < right
	})
	candidates := make([]endpointCandidate, 0, size)
	for i := 0; i < size; i++ {
		idx := indices[i]
		candidates = append(candidates, endpointCandidate{
			Index: idx,
			URL:   s.endpoints[idx],
		})
	}
	return candidates
}
