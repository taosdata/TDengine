package unified

func registerPendingRequestForTest(c *Client, req *pendingRequest) {
	if c == nil || req == nil {
		return
	}
	c.pendingLock.Lock()
	if c.pendingRequests == nil {
		c.pendingRequests = make(map[uint64]*pendingRequest)
	}
	c.pendingRequests[req.reqID] = req
	c.pendingLock.Unlock()
}

func pendingRequestExistsForTest(c *Client, reqID uint64) bool {
	if c == nil {
		return false
	}
	c.pendingLock.RLock()
	_, ok := c.pendingRequests[reqID]
	c.pendingLock.RUnlock()
	return ok
}

func clearPendingRequestsForTest(c *Client) {
	if c == nil {
		return
	}
	c.pendingLock.Lock()
	oldRequests := c.pendingRequests
	c.pendingRequests = make(map[uint64]*pendingRequest)
	c.pendingLock.Unlock()
	notifyPendingRequestsClosed(oldRequests)
}
