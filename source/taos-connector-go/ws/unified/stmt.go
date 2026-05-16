package unified

import (
	"errors"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	tLog "github.com/taosdata/driver-go/v3/log"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// Stmt provides stmt2 workflow on top of unified client runtime.
type Stmt struct {
	client  *Client
	runtime *client.Client // runtime bound at creation; updated on reconnect

	mu sync.Mutex

	id           uint64
	lastAffected int
	sql          string
	isInsert     bool
	fieldsCount  int
	fields       []*commonstmt.Stmt2AllField
	needTable    bool
	tagCount     int
	colCount     int

	schemaChanged bool
	closed        bool
	bindMode      stmtBindMode

	state *stmtCompatState
}

type stmtBindMode uint8

const (
	stmtBindModeUnset stmtBindMode = iota
	stmtBindModeCompat
	stmtBindModeRaw
)

// InitStmt initializes one stmt2 handle.
func (c *Client) InitStmt(reqID int64) (*Stmt, error) {
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	stmtID, runtime, err := c.stmt2InitWithReconnect(uint64(reqID))
	if err != nil {
		return nil, normalizeStmtError(err)
	}
	return &Stmt{
		client:  c,
		runtime: runtime,
		id:      stmtID,
		state:   newStmtCompatState(),
	}, nil
}

// Prepare sends stmt2_prepare.
func (s *Stmt) Prepare(reqID int64, sql string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkNotClosedLocked(); err != nil {
		return err
	}

	if err := s.prepareWithReconnectLocked(reqID, sql); err != nil {
		s.resetPrepareLocked()
		return err
	}

	s.sql = sql
	return nil
}

// SetTableName sets current batch table name.
// Deprecated: use Bind with []*commonstmt.TaosStmt2BindData instead.
func (s *Stmt) SetTableName(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkPreparedLocked(); err != nil {
		return err
	}
	if err := s.enterCompatModeLocked(); err != nil {
		return err
	}
	if !s.needTable {
		return ErrStmtTableNameNotRequired
	}
	if name == "" {
		return ErrStmtTableNameEmpty
	}
	s.state.setTableName(name)
	return nil
}

// SetTags sets current batch tags.
// Deprecated: use Bind with []*commonstmt.TaosStmt2BindData instead.
func (s *Stmt) SetTags(tags *param.Param, bindType *param.ColumnType) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkPreparedLocked(); err != nil {
		return err
	}
	if err := s.enterCompatModeLocked(); err != nil {
		return err
	}
	if !s.isInsert || s.tagCount == 0 {
		return ErrStmtTagsNotNeeded
	}
	if tags == nil {
		return ErrStmtTagsNil
	}
	if len(tags.GetValues()) != s.tagCount {
		return newInvalidStateErrorf("expected %d tags, got %d", s.tagCount, len(tags.GetValues()))
	}
	s.state.setTags(tags, bindType)
	return nil
}

// Bind stores stmt2-style bind data directly without value conversion.
// The params type must equal to the DB type.
// DBType               | GoType
// -----------------------------
// BOOL                 | bool
// TINYINT              | int8
// SMALLINT             | int16
// INT                  | int32
// BIGINT               | int64
// TINYINT UNSIGNED     | uint8
// SMALLINT UNSIGNED    | uint16
// INT UNSIGNED         | uint32
// BIGINT UNSIGNED      | uint64
// FLOAT                | float32
// DOUBLE               | float64
// TIMESTAMP            | time.Time
// BINARY               | []byte
// NCHAR                | string/[]byte
// VARBINARY            | []byte
// GEOMETRY             | []byte
// JSON                 | []byte
// DECIMAL/DECIMAL64    | string
// BLOB                 | []byte/string
func (s *Stmt) Bind(params []*commonstmt.TaosStmt2BindData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.bindStmt2DataLocked(params)
}

func (s *Stmt) bindParamLocked(params []*param.Param, bindType *param.ColumnType) error {
	if err := s.checkPreparedLocked(); err != nil {
		return err
	}
	if err := s.enterCompatModeLocked(); err != nil {
		return err
	}
	if len(params) == 0 {
		return ErrStmtParamsEmpty
	}
	if s.isInsert {
		if s.colCount > 0 && len(params) != s.colCount {
			return newInvalidStateErrorf("expected %d columns, got %d", s.colCount, len(params))
		}
	} else if s.fieldsCount > 0 && len(params) != s.fieldsCount {
		return newInvalidStateErrorf("expected %d query params, got %d", s.fieldsCount, len(params))
	}
	s.state.bindParams(params, bindType)
	return nil
}

func (s *Stmt) bindStmt2DataLocked(params []*commonstmt.TaosStmt2BindData) error {
	if err := s.checkPreparedLocked(); err != nil {
		return err
	}
	if err := s.enterRawModeLocked(); err != nil {
		return err
	}
	if len(params) == 0 {
		return ErrStmtParamsEmpty
	}
	if !s.isInsert && len(params) != 1 {
		return newInvalidStateErrorf("query statement supports exactly one batch, got %d", len(params))
	}

	for i := 0; i < len(params); i++ {
		item := params[i]
		if item == nil {
			return newInvalidStateErrorf("bind data at index %d is nil", i)
		}
		if !s.isInsert {
			if item.TableName != "" {
				return newInvalidStateErrorf("query statement does not support table name in bind data")
			}
			if len(item.Tags) != 0 {
				return newInvalidStateErrorf("query statement does not support tags in bind data")
			}
		}
		if err := s.validateBindDataItemLocked(item); err != nil {
			return err
		}
	}
	return s.state.setRawBindData(params, s.isInsert)
}

// BindParam is stmt compatibility alias.
// Deprecated: use Bind with []*commonstmt.TaosStmt2BindData instead.
func (s *Stmt) BindParam(params []*param.Param, bindType *param.ColumnType) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bindParamLocked(params, bindType)
}

// AddBatch caches current batch for exec.
// Deprecated: use Bind with []*commonstmt.TaosStmt2BindData instead.
func (s *Stmt) AddBatch() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkPreparedLocked(); err != nil {
		return err
	}
	if err := s.enterCompatModeLocked(); err != nil {
		return err
	}
	if err := s.validateCurrentBatchLocked(); err != nil {
		return err
	}
	return s.state.addBatch(s.isInsert)
}

// Exec sends cached batches through stmt2_bind and stmt2_exec.
func (s *Stmt) Exec(reqID int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkPreparedLocked(); err != nil {
		return 0, err
	}
	if !s.state.hasBindData(s.isInsert) {
		return 0, ErrStmtNoBatchAdded
	}
	defer s.cleanExecLocked()

	bindPayload, err := s.buildExecPayloadLocked()
	if err != nil {
		return 0, err
	}
	resp, err := s.execWithReconnectLocked(reqID, bindPayload)
	if err != nil {
		return 0, normalizeStmtError(err)
	}
	s.lastAffected = resp.Affected
	return s.lastAffected, nil
}

// AffectedRows returns affected rows from last exec.
func (s *Stmt) AffectedRows() int {
	s.mu.Lock()
	affected := s.lastAffected
	s.mu.Unlock()
	return affected
}

// IsInsert reports whether the prepared statement is insert.
func (s *Stmt) IsInsert() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkPreparedLocked(); err != nil {
		return false, err
	}
	return s.isInsert, nil
}

// ColFields returns insert column fields parsed from prepared stmt2 metadata.
func (s *Stmt) ColFields() ([]*commonstmt.StmtField, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkPreparedLocked(); err != nil {
		return nil, err
	}
	if !s.isInsert {
		return nil, nil
	}

	fields := make([]*commonstmt.StmtField, 0, s.colCount)
	for i := 0; i < len(s.fields); i++ {
		field := s.fields[i]
		if field == nil || field.BindType != commonstmt.TAOS_FIELD_COL {
			continue
		}
		fields = append(fields, &commonstmt.StmtField{
			Name:      field.Name,
			FieldType: field.FieldType,
			Precision: field.Precision,
			Scale:     field.Scale,
			Bytes:     field.Bytes,
		})
	}
	return fields, nil
}

// UseResult gets stmt2 query result and returns unified ResultSet.
func (s *Stmt) UseResult(reqID int64) (*ResultSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkPreparedLocked(); err != nil {
		return nil, err
	}
	if reqID == 0 {
		reqID = common.GetReqID()
	}

	runtime, err := s.runtimeOrError()
	if err != nil {
		return nil, err
	}
	req := &proto.Stmt2UseResultRequest{
		ReqID:  uint64(reqID),
		StmtID: s.id,
	}
	var resp proto.Stmt2UseResultResponse
	_, runtimeGen, err := s.client.sendStmtJSONAndDecode(runtime, uint64(reqID), proto.STMT2Result, req, &resp)
	if err != nil {
		return nil, normalizeStmtError(err)
	}

	return &ResultSet{
		client:          s.client,
		runtime:         runtime,
		runtimeGen:      runtimeGen,
		resultID:        resp.ID,
		timezone:        s.client.config.Timezone,
		fieldsCount:     resp.FieldsCount,
		fieldsNames:     append([]string(nil), resp.FieldsNames...),
		fieldsTypes:     append([]uint8(nil), resp.FieldsTypes...),
		fieldsLengths:   append([]int64(nil), resp.FieldsLengths...),
		fieldsPrecision: append([]int64(nil), resp.FieldsPrecisions...),
		fieldsScale:     append([]int64(nil), resp.FieldsScales...),
		precision:       resp.Precision,
	}, nil
}

// Close closes stmt2 handle.
func (s *Stmt) Close(reqID int64) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	stmtID := s.id
	s.mu.Unlock()
	tLog.Debugf(0, "stmt2 closed, stmt_id: %d", stmtID)

	runtime := s.runtime
	if runtime == nil {
		return nil
	}
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	req := &proto.Stmt2CloseRequest{
		ReqID:  uint64(reqID),
		StmtID: stmtID,
	}
	var resp proto.Stmt2CloseResponse
	_, _, err := s.client.sendStmtJSONAndDecode(runtime, uint64(reqID), proto.STMT2Close, req, &resp)
	if err != nil {
		if errors.Is(err, client.ClosedError) || isReconnectableError(err) || IsConnectionRelatedError(err) {
			return nil
		}
		return err
	}
	return nil
}

func (s *Stmt) prepareWithReconnectLocked(reqID int64, sql string) error {
	resp, runtime, err := s.prepareOnceLocked(reqID, sql)
	if err == nil {
		s.applyPrepareMetadataLocked(resp)
		return nil
	}
	if !s.shouldReconnectLocked(err, runtime) {
		return normalizeStmtError(err)
	}
	tLog.Warnf(0, "stmt2 prepare failed, attempting reconnect, sql_len: %d, err: %v", len(sql), err)
	if err = s.reconnectAndInitLocked(runtime); err != nil {
		return normalizeStmtError(err)
	}
	tLog.Infof(0, "stmt2 prepare retrying after reconnect, sql_len: %d", len(sql))
	resp, _, err = s.prepareOnceLocked(reqID, sql)
	if err != nil {
		return normalizeStmtError(err)
	}
	s.applyPrepareMetadataLocked(resp)
	return nil
}

func (s *Stmt) prepareOnceLocked(reqID int64, sql string) (*proto.Stmt2PrepareResponse, *client.Client, error) {
	runtime, err := s.runtimeOrError()
	if err != nil {
		return nil, nil, err
	}
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	reqIDUint64 := uint64(reqID)
	req := &proto.Stmt2PrepareRequest{
		ReqID:     reqIDUint64,
		StmtID:    s.id,
		SQL:       sql,
		GetFields: true,
	}
	var resp proto.Stmt2PrepareResponse
	if _, _, err = s.client.sendStmtJSONAndDecode(runtime, reqIDUint64, proto.STMT2Prepare, req, &resp); err != nil {
		return nil, runtime, err
	}
	return &resp, runtime, nil
}

func (s *Stmt) applyPrepareMetadataLocked(resp *proto.Stmt2PrepareResponse) {
	s.isInsert = resp.IsInsert
	s.fieldsCount = resp.FieldsCount
	s.fields = cloneStmt2Fields(resp.Fields)
	s.needTable = false
	s.tagCount = 0
	s.colCount = 0
	s.schemaChanged = false
	s.lastAffected = 0
	s.bindMode = stmtBindModeUnset
	s.state.reset()
	s.state.setPreparedFields(s.fields)

	if !s.isInsert {
		return
	}

	for i := 0; i < len(s.fields); i++ {
		field := s.fields[i]
		switch field.BindType {
		case commonstmt.TAOS_FIELD_TBNAME:
			s.needTable = true
		case commonstmt.TAOS_FIELD_TAG:
			s.tagCount += 1
		case commonstmt.TAOS_FIELD_COL:
			s.colCount += 1
		}
	}
}

func (s *Stmt) validateCurrentBatchLocked() error {
	batch := s.state.Current
	if s.needTable && batch.TableName == "" {
		return ErrStmtTableNameNotSet
	}
	if s.tagCount > 0 {
		if batch.Tags == nil {
			return ErrStmtTagsNotSet
		}
		if len(batch.Tags.GetValues()) != s.tagCount {
			return newInvalidStateErrorf("expected %d tags, got %d", s.tagCount, len(batch.Tags.GetValues()))
		}
	}
	if len(batch.Params) == 0 {
		return ErrStmtColumnsNotSet
	}
	if s.isInsert {
		if s.colCount > 0 && len(batch.Params) != s.colCount {
			return newInvalidStateErrorf("expected %d columns, got %d", s.colCount, len(batch.Params))
		}
	}
	rows := len(batch.Params[0].GetValues())
	if rows == 0 {
		return ErrStmtNoRowsToAdd
	}
	for i := 0; i < len(batch.Params); i++ {
		currentRows := len(batch.Params[i].GetValues())
		if currentRows == 0 {
			return newInvalidStateErrorf("column at index %d has no rows to add", i)
		}
		if currentRows != rows {
			return newInvalidStateErrorf("column at index %d has a different row count than the first column. expected %d, got %d", i, rows, currentRows)
		}
	}
	return nil
}

func (s *Stmt) buildExecPayloadLocked() ([]byte, error) {
	bindData := s.state.bindData(s.isInsert)
	if len(bindData) == 0 {
		return nil, ErrStmtNoBatchAdded
	}
	return buildStmt2BindPayload(bindData, s.isInsert, s.fields)
}

func (s *Stmt) execWithReconnectLocked(reqID int64, bindPayload []byte) (*proto.Stmt2ExecResponse, error) {
	resp, runtime, err := s.execOnceLocked(reqID, bindPayload)
	if err == nil {
		return resp, nil
	}
	if !s.shouldReconnectLocked(err, runtime) {
		return nil, err
	}
	if err = s.reconnectAndInitLocked(runtime); err != nil {
		return nil, err
	}
	if err = s.reprepareAfterReconnectLocked(); err != nil {
		return nil, err
	}
	tLog.Infof(0, "stmt2 exec retrying after reconnect, stmt_id: %d", s.id)
	resp, _, err = s.execOnceLocked(reqID, bindPayload)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *Stmt) execOnceLocked(reqID int64, bindPayload []byte) (*proto.Stmt2ExecResponse, *client.Client, error) {
	runtime, err := s.runtimeOrError()
	if err != nil {
		return nil, nil, err
	}

	bindReqID := uint64(common.GetReqID())
	bindReq := buildStmt2BindBinaryRequest(bindReqID, s.id, bindPayload, proto.Stmt2BindAllColumns)
	var bindResp proto.Stmt2BindResponse
	if _, _, err = s.client.sendStmtBinaryAndDecode(runtime, bindReqID, bindReq, &bindResp); err != nil {
		return nil, runtime, err
	}

	if reqID == 0 {
		reqID = common.GetReqID()
	}
	execReqID := uint64(reqID)
	execReq := &proto.Stmt2ExecRequest{
		ReqID:  execReqID,
		StmtID: s.id,
	}
	var execResp proto.Stmt2ExecResponse
	if _, _, err = s.client.sendStmtJSONAndDecode(runtime, execReqID, proto.STMT2Exec, execReq, &execResp); err != nil {
		return nil, runtime, err
	}
	return &execResp, runtime, nil
}

func (s *Stmt) reconnectAndInitLocked(failedRuntime *client.Client) error {
	tLog.Infof(0, "stmt2 reconnecting, old_stmt_id: %d", s.id)
	if err := s.client.reconnectWithBootstrap(s.client.defaultBootstrap, failedRuntime); err != nil {
		return err
	}
	stmtID, runtime, err := s.client.stmt2InitWithReconnect(uint64(common.GetReqID()))
	if err != nil {
		return err
	}
	s.id = stmtID
	s.runtime = runtime
	tLog.Infof(0, "stmt2 re-initialized, new_stmt_id: %d", s.id)
	return nil
}

func (s *Stmt) reprepareAfterReconnectLocked() error {
	resp, _, err := s.prepareOnceLocked(0, s.sql)
	if err != nil {
		return err
	}
	if !samePrepareMetadata(s, resp) {
		s.schemaChanged = true
		tLog.Warnf(0, "stmt2 re-prepare detected schema change, sql_len: %d", len(s.sql))
		return ErrStmtReprepareSchemaChanged
	}
	return nil
}

func (s *Stmt) shouldReconnectLocked(err error, runtime *client.Client) bool {
	if err == nil {
		return false
	}
	if s.client == nil || !s.client.config.AutoReconnect {
		return false
	}
	if runtime == nil || !runtime.IsRunning() {
		return true
	}
	if errors.Is(err, client.ClosedError) {
		return true
	}
	return isReconnectableError(err)
}

func (s *Stmt) cleanExecLocked() {
	s.state.reset()
}

func (s *Stmt) resetPrepareLocked() {
	s.sql = ""
	s.isInsert = false
	s.fieldsCount = 0
	s.fields = nil
	s.needTable = false
	s.tagCount = 0
	s.colCount = 0
	s.schemaChanged = false
	s.lastAffected = 0
	s.bindMode = stmtBindModeUnset
	s.state.reset()
}

func (s *Stmt) checkPreparedLocked() error {
	if err := s.checkNotClosedLocked(); err != nil {
		return err
	}
	if s.sql == "" {
		return ErrStmtNotPrepared
	}
	if s.schemaChanged {
		return ErrStmtSchemaChanged
	}
	return nil
}

func (s *Stmt) checkNotClosedLocked() error {
	if s.closed {
		return ErrUnifiedClosed
	}
	return nil
}

// runtimeOrError returns the runtime bound to this Stmt.
// It returns an error when the bound runtime is nil or the client is closed.
func (s *Stmt) runtimeOrError() (*client.Client, error) {
	rt := s.runtime
	if rt != nil {
		return rt, nil
	}
	if s.client.IsClosed() {
		return nil, ErrUnifiedClosed
	}
	return nil, ErrStmtConnectionLost
}

func (c *Client) stmt2InitWithReconnect(reqID uint64) (uint64, *client.Client, error) {
	runtime, err := c.runtimeOrError()
	if err != nil {
		return 0, nil, err
	}
	stmtID, err := c.stmt2InitOnce(runtime, reqID)
	if err == nil {
		return stmtID, runtime, nil
	}

	runtime, err = c.reconnectRuntimeForRetry(err, false, runtime)
	if err != nil {
		return 0, nil, err
	}
	stmtID, err = c.stmt2InitOnce(runtime, reqID)
	if err != nil {
		return 0, nil, err
	}
	return stmtID, runtime, nil
}

func (c *Client) stmt2InitOnce(runtime *client.Client, reqID uint64) (uint64, error) {
	req := &proto.Stmt2InitRequest{
		ReqID:               reqID,
		SingleStbInsert:     true,
		SingleTableBindOnce: true,
	}
	var resp proto.Stmt2InitResponse
	if _, _, err := c.sendStmtJSONAndDecode(runtime, reqID, proto.STMT2Init, req, &resp); err != nil {
		return 0, err
	}
	tLog.Debugf(reqID, "stmt2 init succeeded, stmt_id: %d", resp.StmtID)
	return resp.StmtID, nil
}

func (c *Client) sendStmtJSONWithRuntime(runtime *client.Client, reqID uint64, actionName string, req interface{}) ([]byte, bool, uint64, error) {
	if runtime == nil {
		return nil, false, 0, client.ClosedError
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, false, 0, err
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Type = websocket.TextMessage
	envelope.Msg.Reset()
	if err = encodeWSActionToBuffer(envelope.Msg, actionName, args, true); err != nil {
		return nil, false, 0, err
	}
	return c.sendEnvelopeWithRuntimeWithSummaryFunc(runtime, reqID, envelope, c.config.ReadTimeout, ErrStmtMessageTimeout, func() string {
		return buildTextRequestSummary(actionName, reqID, args)
	})
}

func (c *Client) sendStmtBinaryWithRuntime(runtime *client.Client, reqID uint64, reqPayload []byte) ([]byte, bool, uint64, error) {
	if runtime == nil {
		return nil, false, 0, client.ClosedError
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Type = websocket.BinaryMessage
	envelope.Msg.Reset()
	envelope.Msg.Grow(len(reqPayload))
	envelope.Msg.Write(reqPayload)
	return c.sendEnvelopeWithRuntimeWithSummaryFunc(runtime, reqID, envelope, c.config.ReadTimeout, ErrStmtMessageTimeout, func() string {
		return buildStmtBinaryRequestSummary(reqID, reqPayload)
	})
}

func (c *Client) sendStmtJSONAndDecode(runtime *client.Client, reqID uint64, actionName string, req interface{}, resp responseWithCodeAndMessage) (bool, uint64, error) {
	respBytes, writeAcked, runtimeGen, err := c.sendStmtJSONWithRuntime(runtime, reqID, actionName, req)
	if err != nil {
		return writeAcked, runtimeGen, err
	}
	if err = decodeAndCheckJSONResponse(respBytes, resp); err != nil {
		return writeAcked, runtimeGen, err
	}
	return writeAcked, runtimeGen, nil
}

func (c *Client) sendStmtBinaryAndDecode(runtime *client.Client, reqID uint64, reqPayload []byte, resp responseWithCodeAndMessage) (bool, uint64, error) {
	respBytes, writeAcked, runtimeGen, err := c.sendStmtBinaryWithRuntime(runtime, reqID, reqPayload)
	if err != nil {
		return writeAcked, runtimeGen, err
	}
	if err = decodeAndCheckJSONResponse(respBytes, resp); err != nil {
		return writeAcked, runtimeGen, err
	}
	return writeAcked, runtimeGen, nil
}

func samePrepareMetadata(current *Stmt, resp *proto.Stmt2PrepareResponse) bool {
	if current.isInsert != resp.IsInsert {
		return false
	}
	if current.fieldsCount != resp.FieldsCount {
		return false
	}
	if !current.isInsert {
		return true
	}
	if len(current.fields) != len(resp.Fields) {
		return false
	}
	for i := 0; i < len(current.fields); i++ {
		left := current.fields[i]
		right := resp.Fields[i]
		if left == nil || right == nil {
			if left != right {
				return false
			}
			continue
		}
		if left.Name != right.Name ||
			left.FieldType != right.FieldType ||
			left.Precision != right.Precision ||
			left.Scale != right.Scale ||
			left.Bytes != right.Bytes ||
			left.BindType != right.BindType {
			return false
		}
	}
	return true
}

func cloneStmt2Fields(fields []*commonstmt.Stmt2AllField) []*commonstmt.Stmt2AllField {
	if len(fields) == 0 {
		return nil
	}
	cloned := make([]*commonstmt.Stmt2AllField, len(fields))
	for i := 0; i < len(fields); i++ {
		if fields[i] == nil {
			continue
		}
		cp := *fields[i]
		cloned[i] = &cp
	}
	return cloned
}

func normalizeStmtError(err error) error {
	if err == nil {
		return nil
	}
	if IsConnectionRelatedError(err) {
		return err
	}
	if errors.Is(err, client.ClosedError) || isReconnectableError(err) {
		return ErrStmtConnectionLost
	}
	return err
}

func (s *Stmt) enterCompatModeLocked() error {
	switch s.bindMode {
	case stmtBindModeUnset:
		s.bindMode = stmtBindModeCompat
		return nil
	case stmtBindModeCompat:
		return nil
	default:
		return ErrStmtCompatAPIAfterBind
	}
}

func (s *Stmt) enterRawModeLocked() error {
	switch s.bindMode {
	case stmtBindModeUnset:
		s.bindMode = stmtBindModeRaw
		return nil
	case stmtBindModeRaw:
		return nil
	default:
		return ErrStmtBindAfterCompatAPI
	}
}

func (s *Stmt) validateBindDataItemLocked(item *commonstmt.TaosStmt2BindData) error {
	if s.needTable && item.TableName == "" {
		return ErrStmtTableNameNotSet
	}
	if s.tagCount > 0 {
		if len(item.Tags) == 0 {
			return ErrStmtTagsNotSet
		}
		if len(item.Tags) != s.tagCount {
			return newInvalidStateErrorf("expected %d tags, got %d", s.tagCount, len(item.Tags))
		}
	}
	if len(item.Cols) == 0 {
		return ErrStmtColumnsNotSet
	}
	if s.isInsert {
		if s.colCount > 0 && len(item.Cols) != s.colCount {
			return newInvalidStateErrorf("expected %d columns, got %d", s.colCount, len(item.Cols))
		}
	} else if s.fieldsCount > 0 && len(item.Cols) != s.fieldsCount {
		return newInvalidStateErrorf("expected %d query params, got %d", s.fieldsCount, len(item.Cols))
	}
	rows := len(item.Cols[0])
	if rows == 0 {
		return ErrStmtNoRowsToAdd
	}
	for i := 0; i < len(item.Cols); i++ {
		currentRows := len(item.Cols[i])
		if currentRows == 0 {
			return newInvalidStateErrorf("column at index %d has no rows to add", i)
		}
		if currentRows != rows {
			return newInvalidStateErrorf("column at index %d has a different row count than the first column. expected %d, got %d", i, rows, currentRows)
		}
	}
	return nil
}
