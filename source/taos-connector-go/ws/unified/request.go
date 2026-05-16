package unified

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	tLog "github.com/taosdata/driver-go/v3/log"
	"github.com/taosdata/driver-go/v3/ws/client"
)

var errNilEnvelope = &Error{
	Type:    ErrorTypeInvalidState,
	Message: "nil envelope",
}

// sendEnvelopeWithRuntime sends one request on a specific runtime and waits for one routed response.
// It returns whether the websocket write has been acknowledged and the runtime generation used.
// Timeout only bounds local waiting for a routed response. It does not cancel an in-flight
// websocket write already queued in the runtime send path.
func (c *Client) sendEnvelopeWithRuntime(runtime *client.Client, reqID uint64, envelope *client.Envelope, timeout time.Duration, timeoutErr error) ([]byte, bool, uint64, error) {
	return c.sendEnvelopeWithRuntimeWithSummaryFunc(runtime, reqID, envelope, timeout, timeoutErr, nil)
}

func (c *Client) sendEnvelopeWithRuntimeWithSummaryFunc(runtime *client.Client, reqID uint64, envelope *client.Envelope, timeout time.Duration, timeoutErr error, requestSummaryFunc func() string) ([]byte, bool, uint64, error) {
	if runtime == nil {
		return nil, false, 0, wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
	}
	if timeout <= 0 {
		timeout = c.config.ReadTimeout
	}
	if timeout <= 0 {
		timeout = common.DefaultMessageTimeout
	}
	if timeoutErr == nil {
		timeoutErr = ErrQueryMessageTimeout
	}

	respChan := make(chan []byte, 1)
	pendingReq := &pendingRequest{
		reqID:   reqID,
		channel: respChan,
	}
	var runtimeGen uint64

	// Fast path: first atomic snapshot read is an early reject optimization so
	// stale runtimes can fail without contending on pendingLock.
	// We re-check snapshot again after pendingLock is held to close the TOCTOU
	// window before registering pendingReq.
	if snapshot, ok := c.loadRuntimeSnapshotAtomic(); ok {
		if snapshot.runtime != runtime {
			return nil, false, 0, wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
		}
		runtimeGen = snapshot.generation

		c.pendingLock.Lock()
		currentSnapshot, currentOK := c.loadRuntimeSnapshotAtomic()
		if !currentOK || currentSnapshot.runtime != runtime || currentSnapshot.generation != runtimeGen {
			c.pendingLock.Unlock()
			return nil, false, 0, wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
		}
		if c.pendingRequests == nil {
			c.pendingRequests = make(map[uint64]*pendingRequest)
		}
		c.pendingRequests[reqID] = pendingReq
		c.pendingLock.Unlock()
	} else {
		// Compatibility fallback for tests that create zero-value Client literals.
		// Keep c.lock -> pendingLock order with swapRuntime.
		c.lock.RLock()
		c.pendingLock.Lock()
		if c.runtime != runtime {
			c.pendingLock.Unlock()
			c.lock.RUnlock()
			return nil, false, 0, wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
		}
		runtimeGen = c.runtimeGen
		if c.pendingRequests == nil {
			c.pendingRequests = make(map[uint64]*pendingRequest)
		}
		c.pendingRequests[reqID] = pendingReq
		c.pendingLock.Unlock()
		c.lock.RUnlock()
	}

	defer func() {
		_ = c.removePendingRequest(reqID, pendingReq)
	}()

	if shouldLogPacketInfo() {
		payload := envelope.Msg.Bytes()
		tLog.Infof(reqID, "sending %s packet, size: %d bytes, content: %s", packetTypeName(envelope.Type), len(payload), packetContentForLog(envelope.Type, payload))
	}

	err := runtime.Send(envelope)
	if err != nil {
		return nil, false, runtimeGen, wrapRequestErrorWithSummaryFunc(err, requestSummaryFunc)
	}

	err = <-envelope.ErrorChan
	if err != nil {
		return nil, false, runtimeGen, wrapRequestErrorWithSummaryFunc(err, requestSummaryFunc)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case resp := <-respChan:
		if resp == nil {
			// nil means connection was lost during runtime swap
			return nil, true, runtimeGen, wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
		}
		return resp, true, runtimeGen, nil
	case <-runtime.Done():
		// Prefer an already-routed response over disconnect if both race.
		select {
		case resp := <-respChan:
			if resp == nil {
				return nil, true, runtimeGen, wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
			}
			return resp, true, runtimeGen, nil
		default:
		}
		return nil, true, runtimeGen, wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
	case <-ctx.Done():
		// Prefer an already-routed response over timeout if both race.
		// A timeout here means caller stop-waiting, not guaranteed server-side cancellation.
		select {
		case resp := <-respChan:
			if resp == nil {
				return nil, true, runtimeGen, wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
			}
			return resp, true, runtimeGen, nil
		default:
		}
		return nil, true, runtimeGen, wrapRequestErrorWithSummaryFunc(timeoutErr, requestSummaryFunc)
	}
}

// sendEnvelopeNoResponse sends one request on a specific runtime and only waits for write-ack.
func (c *Client) sendEnvelopeNoResponse(runtime *client.Client, envelope *client.Envelope) error {
	return c.sendEnvelopeNoResponseWithSummaryFunc(runtime, envelope, nil)
}

func (c *Client) sendEnvelopeNoResponseWithSummary(runtime *client.Client, envelope *client.Envelope, requestSummary string) error {
	return c.sendEnvelopeNoResponseWithSummaryFunc(runtime, envelope, fixedSummaryFunc(requestSummary))
}

func (c *Client) sendEnvelopeNoResponseWithSummaryFunc(runtime *client.Client, envelope *client.Envelope, requestSummaryFunc func() string) error {
	if runtime == nil {
		return wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
	}
	if envelope == nil {
		return errNilEnvelope
	}

	if snapshot, ok := c.loadRuntimeSnapshotAtomic(); ok {
		if snapshot.runtime != runtime {
			return wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
		}
	} else {
		c.lock.RLock()
		runtimeMatched := c.runtime == runtime
		c.lock.RUnlock()
		if !runtimeMatched {
			return wrapRequestErrorWithSummaryFunc(client.ClosedError, requestSummaryFunc)
		}
	}

	reqID := uint64(0)
	if shouldLogPacketInfo() {
		payload := envelope.Msg.Bytes()
		tLog.Infof(reqID, "sending %s packet without response, size: %d bytes, content: %s", packetTypeName(envelope.Type), len(payload), packetContentForLog(envelope.Type, payload))
	}

	if err := runtime.Send(envelope); err != nil {
		return wrapRequestErrorWithSummaryFunc(err, requestSummaryFunc)
	}
	err := wrapRequestErrorWithSummaryFunc(<-envelope.ErrorChan, requestSummaryFunc)
	if err == nil && shouldLogPacketInfo() {
		tLog.Infof(reqID, "packet sent without response, type: %s", packetTypeName(envelope.Type))
	}
	return err
}

func wrapRequestError(err error, requestSummary string) error {
	return wrapRequestErrorWithSummaryFunc(err, fixedSummaryFunc(requestSummary))
}

func wrapRequestErrorWithSummaryFunc(err error, requestSummaryFunc func() string) error {
	if err == nil {
		return nil
	}
	if requestSummaryFunc == nil {
		return err
	}
	requestSummary := strings.TrimSpace(requestSummaryFunc())
	if requestSummary == "" {
		return err
	}
	return attachRequestSummary(err, requestSummary)
}

func fixedSummaryFunc(summary string) func() string {
	if strings.TrimSpace(summary) == "" {
		return nil
	}
	return func() string {
		return summary
	}
}

func writeUint64(buffer *bytes.Buffer, v uint64) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
	buffer.WriteByte(byte(v >> 32))
	buffer.WriteByte(byte(v >> 40))
	buffer.WriteByte(byte(v >> 48))
	buffer.WriteByte(byte(v >> 56))
}

func writeUint32(buffer *bytes.Buffer, v uint32) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
}

func writeUint16(buffer *bytes.Buffer, v uint16) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
}
