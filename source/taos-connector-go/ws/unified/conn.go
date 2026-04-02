package unified

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/tdversion"
	tLog "github.com/taosdata/driver-go/v3/log"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// Connect connects and initializes the client for normal connect operations.
func (c *Client) Connect() error {
	c.normalConnectLock.Lock()
	defer c.normalConnectLock.Unlock()

	if c.IsClosed() {
		return ErrUnifiedClosed
	}

	c.lock.RLock()
	connected := c.connected
	runtime := c.runtime
	c.lock.RUnlock()

	if connected && runtime != nil && runtime.IsRunning() {
		return nil
	}
	// connected can become stale when runtime has been closed/replaced.
	c.lock.Lock()
	c.connected = false
	c.lock.Unlock()

	// Connect with schemaless bootstrap
	if err := c.connectWithBootstrap(c.defaultBootstrap); err != nil {
		return err
	}

	// Check if closed during connect
	if c.IsClosed() {
		return ErrUnifiedClosed
	}

	// Initialize runtime (handlers and pumps are set in connectWithCandidates)
	c.lock.Lock()
	c.connected = true
	c.lock.Unlock()
	return nil
}

// defaultBootstrap performs the normal connect handshake on a new websocket connection.
func (c *Client) defaultBootstrap(conn *websocket.Conn) error {
	// Keep legacy behavior: fail fast when server version is incompatible.
	if err := tdversion.WSCheckVersion(conn); err != nil {
		return err
	}

	tz := ""
	if c.config.Timezone != nil {
		tz = c.config.Timezone.String()
	}
	req := &proto.WSConnectReq{
		ReqID:       uint64(common.GetReqID()),
		User:        c.config.User,
		Password:    c.config.Passwd,
		DB:          c.config.DbName,
		TZ:          tz,
		TOTPCode:    c.config.TotpCode,
		BearerToken: c.config.BearerToken,
		App:         common.GetProcessName(),
		Connector:   common.GetConnectorInfo("ws"),
	}

	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}

	var connectAction bytes.Buffer
	err = encodeWSActionToBuffer(&connectAction, proto.Connect, args, false)
	if err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	err = conn.WriteMessage(websocket.TextMessage, connectAction.Bytes())
	if err != nil {
		return err
	}

	readTimeout := c.config.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = common.DefaultMessageTimeout
	}
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	_, respBytes, readErr := conn.ReadMessage()
	if readErr != nil {
		var netErr net.Error
		if errors.As(readErr, &netErr) && netErr.Timeout() {
			return ErrConnectTimeout
		}
		return readErr
	}

	var resp proto.WSConnectResp
	return decodeAndCheckJSONResponse(respBytes, &resp)
}

// handleTextMessage routes incoming text messages to pending requests by req_id.
func (c *Client) handleTextMessage(message []byte) {
	// Extract req_id from message
	reqID, err := extractReqIDFromTextMessage(message)
	if err != nil {
		if shouldLogPacketWarn() {
			tLog.Warnf(0, "received unroutable text packet, size: %d bytes, err: %v, content: %s", len(message), err, packetContentForLog(websocket.TextMessage, message))
		} else {
			tLog.Warnf(0, "received unroutable text packet, size: %d bytes, err: %v", len(message), err)
		}
		return
	}
	if shouldLogPacketInfo() {
		tLog.Infof(reqID, "received text packet, size: %d bytes, content: %s", len(message), packetContentForLog(websocket.TextMessage, message))
	}

	c.handleMessage(message, reqID)
}

func (c *Client) handleBinaryMessage(message []byte) {
	// Extract req_id from message
	reqID, err := extractReqIDFromBinaryMessage(message)
	if err != nil {
		if shouldLogPacketWarn() {
			tLog.Warnf(0, "received unroutable binary packet, size: %d bytes, err: %v, content: %s", len(message), err, packetContentForLog(websocket.BinaryMessage, message))
		} else {
			tLog.Warnf(0, "received unroutable binary packet, size: %d bytes, err: %v", len(message), err)
		}
		return
	}
	if shouldLogPacketInfo() {
		tLog.Infof(reqID, "received binary packet, size: %d bytes, content: %s", len(message), packetContentForLog(websocket.BinaryMessage, message))
	}

	c.handleMessage(message, reqID)
}

func (c *Client) handleMessage(message []byte, reqID uint64) {
	req := c.removePendingRequest(reqID, nil)
	if req == nil {
		if tLog.IsPacketLoggingEnabled() && tLog.IsDebugEnabled() {
			tLog.Debugf(reqID, "dropped response without pending request, size: %d bytes", len(message))
		}
		return
	}

	// Use select to avoid blocking if channel is full or closed.
	select {
	case req.channel <- message:
	default:
	}
}

// extractReqIDFromTextMessage extracts req_id from JSON text protocol message.
func extractReqIDFromTextMessage(message []byte) (uint64, error) {
	var payload struct {
		ReqID uint64 `json:"req_id"`
	}
	if err := json.Unmarshal(message, &payload); err != nil {
		return 0, err
	}
	return payload.ReqID, nil
}

// extractReqIDFromBinaryMessage extracts req_id from unified binary frame header.
func extractReqIDFromBinaryMessage(message []byte) (uint64, error) {
	if len(message) < 16 {
		return 0, ErrBinaryMessageTooShort
	}
	flag := binary.LittleEndian.Uint64(message[0:8])
	if flag == 0xffffffffffffffff {
		if len(message) < 34 {
			return 0, ErrBinaryMessageExtendedHeaderTooShort
		}
		return binary.LittleEndian.Uint64(message[26:34]), nil
	}
	return binary.LittleEndian.Uint64(message[8:16]), nil
}
