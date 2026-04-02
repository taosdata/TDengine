package unified

import (
	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

const (
	InfluxDBLineProtocol       = 1
	OpenTSDBTelnetLineProtocol = 2
	OpenTSDBJsonFormatProtocol = 3
)

// SchemalessInsert sends a schemaless insert request with automatic failover and reconnect.
// tableNameKey is required. Pass "" when the protocol does not use table name key.
func (c *Client) SchemalessInsert(reqID int64, lines string, protocol int, precision string, ttl int, tableNameKey string) error {
	if reqID == 0 {
		reqID = common.GetReqID()
	}

	req := &proto.SchemalessWriteRequest{
		ReqID:        uint64(reqID),
		Protocol:     protocol,
		Precision:    precision,
		TTL:          ttl,
		Data:         lines,
		TableNameKey: tableNameKey,
	}

	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.Reset()
	err = encodeWSActionToBuffer(envelope.Msg, proto.SchemalessWrite, args, true)
	if err != nil {
		return err
	}
	respBytes, err := c.sendSchemalessWithReconnect(uint64(reqID), envelope, proto.SchemalessWrite, args)
	if err != nil {
		return err
	}

	var resp proto.SchemalessWriteResponse
	return decodeAndCheckJSONResponse(respBytes, &resp)
}

// sendSchemalessWithReconnect sends a schemaless message with automatic reconnect on failure.
func (c *Client) sendSchemalessWithReconnect(reqID uint64, envelope *client.Envelope, action string, args []byte) ([]byte, error) {
	runtime, err := c.runtimeOrError()
	if err != nil {
		return nil, err
	}

	envelope.Type = websocket.TextMessage
	send := func(rt *client.Client) ([]byte, bool, uint64, error) {
		return c.sendSchemalessWithRuntime(rt, reqID, envelope, action, args)
	}
	respBytes, _, _, err := c.sendWithReconnect(runtime, send)
	if err != nil {
		return nil, err
	}
	return respBytes, nil
}

// sendSchemalessWithRuntime sends a schemaless message using the provided runtime client.
// The boolean return indicates whether websocket write has been acknowledged by WritePump.
func (c *Client) sendSchemalessWithRuntime(runtime *client.Client, reqID uint64, envelope *client.Envelope, action string, args []byte) ([]byte, bool, uint64, error) {
	respBytes, writeAcked, runtimeGen, err := c.sendEnvelopeWithRuntimeWithSummaryFunc(runtime, reqID, envelope, c.config.ReadTimeout, ErrSchemalessMessageTimeout, func() string {
		return buildTextRequestSummary(action, reqID, args)
	})
	return respBytes, writeAcked, runtimeGen, err
}
