package proto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBaseRespNilReceiver verifies that nil *BaseResp does not panic.
func TestBaseRespNilReceiver(t *testing.T) {
	var b *BaseResp

	assert.Equal(t, 0, b.GetCode(), "nil BaseResp.GetCode() should return 0")
	assert.Equal(t, "", b.GetMessage(), "nil BaseResp.GetMessage() should return empty string")
	assert.Equal(t, uint64(0), b.GetReqID(), "nil BaseResp.GetReqID() should return 0")
}

// TestBaseRespNormalValues verifies accessor methods return correct values.
func TestBaseRespNormalValues(t *testing.T) {
	b := &BaseResp{
		Code:    1234,
		Message: "test error",
		Action:  WSQuery,
		ReqID:   42,
		Timing:  999,
	}

	assert.Equal(t, 1234, b.GetCode())
	assert.Equal(t, "test error", b.GetMessage())
	assert.Equal(t, uint64(42), b.GetReqID())
}

// TestBaseRespZeroValue verifies zero-value BaseResp returns zero values.
func TestBaseRespZeroValue(t *testing.T) {
	b := &BaseResp{}

	assert.Equal(t, 0, b.GetCode())
	assert.Equal(t, "", b.GetMessage())
	assert.Equal(t, uint64(0), b.GetReqID())
}

// TestEmbeddedBaseRespNilSafety verifies that a nil outer struct pointer
// panics due to Go's method promotion dereferencing the nil pointer.
// This is expected — callers must ensure response pointers are non-nil.
// BaseResp's nil receiver check only protects standalone (*BaseResp)(nil).
func TestEmbeddedBaseRespNilSafety(t *testing.T) {
	var connResp *WSConnectResp
	assert.Panics(t, func() { connResp.GetCode() })
}

// TestActionConstants verifies action string constants match the protocol spec.
func TestActionConstants(t *testing.T) {
	// query/connection actions
	assert.Equal(t, "conn", Connect)
	assert.Equal(t, "query", WSQuery)
	assert.Equal(t, "fetch", WSFetch)
	assert.Equal(t, "fetch_block", WSFetchBlock)
	assert.Equal(t, "free_result", WSFreeResult)
	assert.Equal(t, "get_current_db", WSGetCurrentDB)
	assert.Equal(t, "get_server_info", WSGetServerInfo)
	assert.Equal(t, "num_fields", WSNumFields)

	// schemaless
	assert.Equal(t, "insert", SchemalessWrite)

	// stmt (legacy)
	assert.Equal(t, "init", STMTInit)
	assert.Equal(t, "prepare", STMTPrepare)
	assert.Equal(t, "set_table_name", STMTSetTableName)
	assert.Equal(t, "set_tags", STMTSetTags)
	assert.Equal(t, "bind", STMTBind)
	assert.Equal(t, "add_batch", STMTAddBatch)
	assert.Equal(t, "exec", STMTExec)
	assert.Equal(t, "close", STMTClose)
	assert.Equal(t, "get_tag_fields", STMTGetTagFields)
	assert.Equal(t, "get_col_fields", STMTGetColFields)
	assert.Equal(t, "use_result", STMTUseResult)
	assert.Equal(t, "stmt_num_params", STMTNumParams)
	assert.Equal(t, "stmt_get_param", STMTGetParam)

	// stmt2
	assert.Equal(t, "stmt2_init", STMT2Init)
	assert.Equal(t, "stmt2_prepare", STMT2Prepare)
	assert.Equal(t, "stmt2_exec", STMT2Exec)
	assert.Equal(t, "stmt2_result", STMT2Result)
	assert.Equal(t, "stmt2_close", STMT2Close)

	// tmq
	assert.Equal(t, "subscribe", TMQActionSubscribe)
	assert.Equal(t, "poll", TMQActionPoll)
	assert.Equal(t, "fetch_raw", TMQActionFetchRaw)
	assert.Equal(t, "fetch_json_meta", TMQActionFetchJSONMeta)
	assert.Equal(t, "commit", TMQActionCommit)
	assert.Equal(t, "unsubscribe", TMQActionUnsubscribe)
	assert.Equal(t, "assignment", TMQActionAssignment)
	assert.Equal(t, "seek", TMQActionSeek)
	assert.Equal(t, "commit_offset", TMQActionCommitOffset)
	assert.Equal(t, "committed", TMQActionCommitted)
	assert.Equal(t, "position", TMQActionPosition)

	// other
	assert.Equal(t, "options_connection", OptionsConnection)
	assert.Equal(t, "check_server_status", CheckServerStatus)
	assert.Equal(t, "get_connection_info", GetConnectionInfo)
}

// TestBinaryMessageTypeConstants verifies binary message type IDs.
func TestBinaryMessageTypeConstants(t *testing.T) {
	assert.Equal(t, 1, SetTagsMessage)
	assert.Equal(t, 2, BindMessage)
	assert.Equal(t, 3, TMQRawMessage)
	assert.Equal(t, 4, RawBlockMessage)
	assert.Equal(t, 5, RawBlockMessageWithFields)
	assert.Equal(t, 6, BinaryQueryMessage)
	assert.Equal(t, 7, FetchRawBlockMessage)
	assert.Equal(t, 8, TMQFetchRawNewMessage)
	assert.Equal(t, 9, Stmt2BindMessage)
	assert.Equal(t, 10, ValidateSQL)
}

// TestProtocolVersionConstants verifies protocol version values.
func TestProtocolVersionConstants(t *testing.T) {
	assert.Equal(t, uint16(1), BinaryProtocolVersion1)
	assert.Equal(t, uint16(1), Stmt2BindProtocolVersion1)
}

// TestStmt2BindAllColumns verifies the sentinel value for binding all columns.
func TestStmt2BindAllColumns(t *testing.T) {
	assert.Equal(t, int32(-1), Stmt2BindAllColumns)
}

// TestWSActionJSON verifies WSAction marshals/unmarshals correctly.
func TestWSActionJSON(t *testing.T) {
	raw := `{"action":"query","args":{"req_id":1,"sql":"select 1"}}`

	var action WSAction
	err := json.Unmarshal([]byte(raw), &action)
	assert.NoError(t, err)
	assert.Equal(t, "query", action.Action)
	assert.NotNil(t, action.Args)

	// Re-marshal and verify roundtrip
	out, err := json.Marshal(&action)
	assert.NoError(t, err)

	var action2 WSAction
	err = json.Unmarshal(out, &action2)
	assert.NoError(t, err)
	assert.Equal(t, action.Action, action2.Action)
	assert.JSONEq(t, string(action.Args), string(action2.Args))
}

// TestBaseRespJSON verifies BaseResp JSON serialization.
func TestBaseRespJSON(t *testing.T) {
	raw := `{"code":0,"message":"","action":"query","req_id":123,"timing":456}`

	var resp BaseResp
	err := json.Unmarshal([]byte(raw), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "", resp.Message)
	assert.Equal(t, "query", resp.Action)
	assert.Equal(t, uint64(123), resp.ReqID)
	assert.Equal(t, int64(456), resp.Timing)
}

// TestQueryRespJSON verifies WSQueryResp correctly unmarshals with embedded BaseResp.
func TestQueryRespJSON(t *testing.T) {
	raw := `{
		"code": 0,
		"message": "",
		"action": "query",
		"req_id": 1,
		"timing": 100,
		"id": 42,
		"is_update": true,
		"affected_rows": 5,
		"fields_count": 2,
		"fields_names": ["ts", "v"],
		"fields_types": [9, 4],
		"fields_lengths": [8, 4],
		"precision": 0
	}`

	var resp WSQueryResp
	err := json.Unmarshal([]byte(raw), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.GetCode())
	assert.Equal(t, uint64(1), resp.GetReqID())
	assert.Equal(t, uint64(42), resp.ID)
	assert.True(t, resp.IsUpdate)
	assert.Equal(t, 5, resp.AffectedRows)
	assert.Equal(t, 2, resp.FieldsCount)
	assert.Equal(t, []string{"ts", "v"}, resp.FieldsNames)
	assert.Equal(t, []uint8{9, 4}, resp.FieldsTypes)
	assert.Equal(t, []int64{8, 4}, resp.FieldsLengths)
}

// TestSchemalessWriteResponseJSON verifies schemaless response deserialization.
func TestSchemalessWriteResponseJSON(t *testing.T) {
	raw := `{"code":0,"message":"","action":"insert","req_id":7,"timing":50,"affected_rows":3,"total_rows":3}`

	var resp SchemalessWriteResponse
	err := json.Unmarshal([]byte(raw), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.GetCode())
	assert.Equal(t, uint64(7), resp.GetReqID())
	assert.Equal(t, 3, resp.AffectedRows)
	assert.Equal(t, int32(3), resp.TotalRows)
}

// TestStmt2InitResponseJSON verifies stmt2 init response deserialization.
func TestStmt2InitResponseJSON(t *testing.T) {
	raw := `{"code":0,"message":"","action":"stmt2_init","req_id":10,"timing":20,"stmt_id":99}`

	var resp Stmt2InitResponse
	err := json.Unmarshal([]byte(raw), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.GetCode())
	assert.Equal(t, uint64(99), resp.StmtID)
}

// TestRespInterface verifies that all response types implement RespInterface.
func TestRespInterface(t *testing.T) {
	var _ RespInterface = &BaseResp{}
	var _ RespInterface = &WSConnectResp{}
	var _ RespInterface = &WSQueryResp{}
	var _ RespInterface = &WSFetchResp{}
	var _ RespInterface = &SchemalessWriteResponse{}
	var _ RespInterface = &Stmt2InitResponse{}
	var _ RespInterface = &Stmt2PrepareResponse{}
	var _ RespInterface = &Stmt2ExecResponse{}
	var _ RespInterface = &Stmt2UseResultResponse{}
	var _ RespInterface = &Stmt2CloseResponse{}
	var _ RespInterface = &Stmt2BindResponse{}
	var _ RespInterface = &StmtPrepareResponse{}
	var _ RespInterface = &StmtInitResp{}
	var _ RespInterface = &StmtCloseResponse{}
	var _ RespInterface = &StmtGetColFieldsResponse{}
	var _ RespInterface = &StmtBindResponse{}
	var _ RespInterface = &StmtAddBatchResponse{}
	var _ RespInterface = &StmtExecResponse{}
	var _ RespInterface = &StmtUseResultResponse{}
	var _ RespInterface = &SubscribeResp{}
	var _ RespInterface = &PollResp{}
	var _ RespInterface = &FetchJSONMetaResp{}
	var _ RespInterface = &FetchResp{}
	var _ RespInterface = &CommitResp{}
	var _ RespInterface = &UnsubscribeResp{}
	var _ RespInterface = &AssignmentResp{}
	var _ RespInterface = &OffsetSeekResp{}
	var _ RespInterface = &CommittedResp{}
	var _ RespInterface = &CommitOffsetResp{}
	var _ RespInterface = &PositionResp{}
}

// TestBaseRespWithErrorCode verifies that error responses are correctly parsed.
func TestBaseRespWithErrorCode(t *testing.T) {
	raw := `{"code":904,"message":"Database not exist","action":"query","req_id":5,"timing":10}`

	var resp BaseResp
	err := json.Unmarshal([]byte(raw), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 904, resp.GetCode())
	assert.Equal(t, "Database not exist", resp.GetMessage())
	assert.Equal(t, uint64(5), resp.GetReqID())
}
