package unified

import (
	"bytes"
	"errors"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

type execQueryResponse struct {
	Code         int    `json:"code"`
	Message      string `json:"message"`
	ID           uint64 `json:"id"`
	IsUpdate     bool   `json:"is_update"`
	AffectedRows int    `json:"affected_rows"`
}

func (r *execQueryResponse) GetCode() int {
	if r == nil {
		return 0
	}
	return r.Code
}

func (r *execQueryResponse) GetMessage() string {
	if r == nil {
		return ""
	}
	return r.Message
}

// Exec executes one SQL query and returns affected rows.
func (c *Client) Exec(reqID int64, sql string) (int, error) {
	if reqID == 0 {
		reqID = common.GetReqID()
	}

	respBytes, runtime, runtimeGen, err := c.sendQueryWithReconnect(uint64(reqID), sql)
	if err != nil {
		return 0, normalizeDisconnectedError(err, "query connection lost")
	}

	resp, err := parseExecQueryResponse(respBytes)
	if err != nil {
		return 0, err
	}

	if !resp.IsUpdate && resp.ID != 0 {
		// Exec-style calls must not leak server-side result handles.
		rs := &ResultSet{
			client:     c,
			runtime:    runtime,
			runtimeGen: runtimeGen,
			resultID:   resp.ID,
			timezone:   c.config.Timezone,
		}
		_ = rs.Close()
	}
	return resp.AffectedRows, nil
}

// Query sends one binary query message with reconnect/failover support.
// It returns nil result for update statements.
func (c *Client) Query(reqID int64, sql string) (*ResultSet, error) {
	queryResp, runtime, runtimeGen, err := c.queryRaw(reqID, sql)
	if err != nil {
		return nil, err
	}
	return buildResultSetFromQueryResp(c, runtime, runtimeGen, queryResp), nil
}

func (c *Client) queryRaw(reqID int64, sql string) (*proto.WSQueryResp, *client.Client, uint64, error) {
	if reqID == 0 {
		reqID = common.GetReqID()
	}

	respBytes, runtime, runtimeGen, err := c.sendQueryWithReconnect(uint64(reqID), sql)
	if err != nil {
		return nil, nil, 0, normalizeDisconnectedError(err, "query connection lost")
	}

	var queryResp proto.WSQueryResp
	if err = decodeAndCheckJSONResponseAsProtocol(respBytes, &queryResp, "invalid query response"); err != nil {
		return nil, nil, 0, err
	}
	return &queryResp, runtime, runtimeGen, nil
}

func buildResultSetFromQueryResp(c *Client, runtime *client.Client, runtimeGen uint64, queryResp *proto.WSQueryResp) *ResultSet {
	if c == nil || queryResp == nil || queryResp.IsUpdate {
		return nil
	}
	return &ResultSet{
		client:      c,
		runtime:     runtime,
		runtimeGen:  runtimeGen,
		resultID:    queryResp.ID,
		timezone:    c.config.Timezone,
		fieldsCount: queryResp.FieldsCount,
		// Query response object is request-scoped; transfer slice ownership to avoid per-query copying.
		fieldsNames:     queryResp.FieldsNames,
		fieldsTypes:     queryResp.FieldsTypes,
		fieldsLengths:   queryResp.FieldsLengths,
		fieldsPrecision: queryResp.FieldsPrecisions,
		fieldsScale:     queryResp.FieldsScales,
		precision:       queryResp.Precision,
	}
}

func parseExecQueryResponse(respBytes []byte) (*execQueryResponse, error) {
	var resp execQueryResponse
	if err := decodeAndCheckJSONResponseAsProtocol(respBytes, &resp, "invalid query response"); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) sendQueryWithReconnect(reqID uint64, sql string) ([]byte, *client.Client, uint64, error) {
	runtime, err := c.runtimeOrError()
	if err != nil {
		return nil, nil, 0, err
	}

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Type = websocket.BinaryMessage

	send := func(rt *client.Client) ([]byte, bool, uint64, error) {
		buildBinaryQueryRequestToBuffer(envelope.Msg, reqID, sql)
		return c.sendEnvelopeWithRuntimeWithSummaryFunc(rt, reqID, envelope, c.config.ReadTimeout, ErrQueryMessageTimeout, func() string {
			return buildBinaryQueryRequestSummary(reqID, sql)
		})
	}
	return c.sendWithReconnect(runtime, send)
}

func buildBinaryQueryRequestToBuffer(buf *bytes.Buffer, reqID uint64, sql string) {
	buf.Reset()
	buf.Grow(30 + len(sql))
	writeUint64(buf, reqID)
	writeUint64(buf, 0)
	writeUint64(buf, proto.BinaryQueryMessage)
	writeUint16(buf, proto.BinaryProtocolVersion1)
	writeUint32(buf, uint32(len(sql)))
	buf.WriteString(sql)
}

func normalizeDisconnectedError(err error, message string) error {
	if err == nil {
		return nil
	}
	if IsConnectionRelatedError(err) {
		return err
	}
	if errors.Is(err, client.ClosedError) || isReconnectableError(err) {
		return &Error{
			Type:                   ErrorTypeClientClosed,
			Message:                message,
			Cause:                  err,
			ConnectionRelated:      true,
			ConnectionDisconnected: true,
		}
	}
	return err
}
