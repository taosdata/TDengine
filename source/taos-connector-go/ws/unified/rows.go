package unified

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	tLog "github.com/taosdata/driver-go/v3/log"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// ResultSet is a stateful query handle bound to one runtime connection.
// Fetching from this handle never triggers reconnect/failover because result state lives on one server connection.
type ResultSet struct {
	client     *Client
	runtime    *client.Client
	runtimeGen uint64
	resultID   uint64
	timezone   *time.Location

	fieldsCount     int
	fieldsNames     []string
	fieldsTypes     []uint8
	fieldsLengths   []int64
	fieldsPrecision []int64
	fieldsScale     []int64
	precision       int

	block       []byte
	blockPtr    unsafe.Pointer
	blockOffset int
	blockSize   int

	opMu sync.Mutex

	prefetching bool
	prefetchCh  chan fetchRawBlockResult

	completed bool
	closed    uint32
}

type fetchRawBlockResult struct {
	block     []byte
	completed bool
	err       error
}

// resultID returns backend result identifier.
func (r *ResultSet) resultIDValue() uint64 {
	if r == nil {
		return 0
	}
	return r.resultID
}

// Close frees server-side result resources on the bound runtime.
func (r *ResultSet) Close() error {
	return r.freeResult(0)
}

// freeResult frees server-side result resources on the bound runtime.
func (r *ResultSet) freeResult(reqID int64) error {
	if r == nil {
		return ErrQueryResultClosed
	}
	if reqID == 0 {
		reqID = common.GetReqID()
	}

	r.opMu.Lock()
	defer r.opMu.Unlock()

	if atomic.LoadUint32(&r.closed) != 0 {
		return nil
	}
	atomic.StoreUint32(&r.closed, 1)

	r.waitPrefetchLocked()

	// Clear local block state after closing to stop further scanning work.
	r.blockPtr = nil
	r.block = nil
	r.blockSize = 0
	r.blockOffset = 0
	r.prefetchCh = nil
	r.prefetching = false

	// Result stream already drained by fetch_raw_block(completed=true); no explicit free needed.
	if r.completed {
		tLog.Debugf(0, "free_result skipped, result already completed, result_id: %d", r.resultID)
		return nil
	}

	if err := r.ensureBoundRuntime(); err != nil {
		return err
	}

	req := &proto.WSFreeResultReq{
		ReqID: uint64(reqID),
		ID:    r.resultID,
	}

	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Type = websocket.TextMessage
	envelope.Msg.Reset()

	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	if err = encodeWSActionToBuffer(envelope.Msg, proto.WSFreeResult, args, true); err != nil {
		return err
	}

	err = r.client.sendEnvelopeNoResponseWithSummaryFunc(r.runtime, envelope, func() string {
		return buildTextRequestSummary(proto.WSFreeResult, uint64(reqID), args)
	})
	err = normalizeDisconnectedError(err, ErrQueryResultConnectionLost.Message)
	if err == nil {
		tLog.Debugf(uint64(reqID), "sent free_result, result_id: %d", r.resultID)
	} else {
		tLog.Warnf(uint64(reqID), "free_result failed, result_id: %d, err: %v", r.resultID, err)
	}
	return err
}

// fetchRawBlock fetches next raw block for this query result.
func (r *ResultSet) fetchRawBlock(reqID int64) ([]byte, bool, error) {
	if r == nil {
		return nil, false, ErrQueryResultClosed
	}
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	if r.isClosed() {
		return nil, false, ErrQueryResultClosed
	}
	if err := r.ensureBoundRuntime(); err != nil {
		return nil, false, err
	}

	payload := buildFetchRawBlockRequest(uint64(reqID), r.resultID)
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Type = websocket.BinaryMessage
	envelope.Msg.Reset()
	_, _ = envelope.Msg.Write(payload)

	respBytes, _, _, err := r.client.sendEnvelopeWithRuntimeWithSummaryFunc(r.runtime, uint64(reqID), envelope, r.client.config.ReadTimeout, ErrQueryMessageTimeout, func() string {
		return buildFetchRawBlockRequestSummary(uint64(reqID), r.resultID)
	})
	if err != nil {
		return nil, false, normalizeDisconnectedError(err, ErrQueryResultConnectionLost.Message)
	}
	return parseFetchRawBlockResponse(respBytes)
}

// ColumnTypePrecisionScale returns decimal precision and scale for one column.
func (r *ResultSet) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.fieldsTypes) || index >= len(r.fieldsPrecision) || index >= len(r.fieldsScale) {
		return 0, 0, false
	}
	if r.fieldsTypes[index] == common.TSDB_DATA_TYPE_DECIMAL || r.fieldsTypes[index] == common.TSDB_DATA_TYPE_DECIMAL64 {
		return r.fieldsPrecision[index], r.fieldsScale[index], true
	}
	return 0, 0, false
}

// Columns returns column names.
func (r *ResultSet) Columns() []string {
	return r.fieldsNames
}

// ColumnTypeDatabaseTypeName returns TAOS type name for one column.
func (r *ResultSet) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.fieldsTypes) {
		return ""
	}
	return common.GetTypeName(int(r.fieldsTypes[index]))
}

// ColumnTypeLength returns fixed length metadata for one column.
func (r *ResultSet) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.fieldsLengths) {
		return 0, false
	}
	return r.fieldsLengths[index], true
}

// ColumnTypeScanType returns scan target type for one column.
func (r *ResultSet) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.fieldsTypes) {
		return common.UnknownType
	}
	t, exists := common.ColumnTypeMap[int(r.fieldsTypes[index])]
	if !exists {
		return common.UnknownType
	}
	return t
}

// Next parses the next row from raw blocks into dest.
func (r *ResultSet) Next(dest []driver.Value) error {
	if r == nil {
		return ErrQueryResultClosed
	}

	r.opMu.Lock()
	defer r.opMu.Unlock()

	if r.isClosed() {
		return ErrQueryResultClosed
	}
	if r.blockPtr == nil {
		err := r.fetchBlock()
		if err != nil {
			return err
		}
	}
	if r.blockSize == 0 {
		r.blockPtr = nil
		r.block = nil
		return io.EOF
	}
	if r.blockOffset >= r.blockSize {
		err := r.fetchBlock()
		if err != nil {
			return err
		}
	}
	if r.blockSize == 0 {
		r.blockPtr = nil
		r.block = nil
		return io.EOF
	}
	var err error
	if r.timezone != nil {
		err = parser.ReadRowWithTimeFormat(dest, r.blockPtr, r.blockSize, r.blockOffset, r.fieldsTypes, r.precision, r.fieldsScale, r.formatTime)
	} else {
		err = parser.ReadRow(dest, r.blockPtr, r.blockSize, r.blockOffset, r.fieldsTypes, r.precision, r.fieldsScale)
	}
	if err != nil {
		return err
	}
	r.blockOffset += 1
	return nil
}

// formatTime converts timestamp value to configured location.
func (r *ResultSet) formatTime(ts int64, precision int) driver.Value {
	return common.TimestampConvertToTimeWithLocation(ts, precision, r.timezone)
}

func (r *ResultSet) fetchBlock() error {
	block, completed, err := r.nextRawBlock()
	if err != nil {
		return err
	}
	if completed {
		r.completed = true
		r.block = nil
		r.blockPtr = nil
		r.blockSize = 0
		r.blockOffset = 0
		return nil
	}
	if len(block) == 0 {
		return ErrInvalidFetchRawBlockResponse
	}
	r.block = block
	r.blockPtr = unsafe.Pointer(&r.block[0])
	r.blockSize = int(parser.RawBlockGetNumOfRows(r.blockPtr))
	r.blockOffset = 0
	r.startPrefetch()
	return nil
}

func (r *ResultSet) waitPrefetchLocked() {
	// waitPrefetchLocked drains in-flight prefetch while opMu is held.
	// IMPORTANT: fetchRawBlock must never acquire opMu, otherwise this will deadlock.
	if !r.prefetching || r.prefetchCh == nil {
		r.prefetching = false
		r.prefetchCh = nil
		return
	}
	ch := r.prefetchCh
	r.prefetching = false
	r.prefetchCh = nil
	res := <-ch
	if res.completed {
		r.completed = true
	}
}

func (r *ResultSet) startPrefetch() {
	if r.prefetching || r.blockSize == 0 {
		return
	}
	ch := make(chan fetchRawBlockResult, 1)
	r.prefetchCh = ch
	r.prefetching = true
	go func() {
		block, completed, err := r.fetchRawBlock(0)
		ch <- fetchRawBlockResult{
			block:     block,
			completed: completed,
			err:       err,
		}
	}()
}

func (r *ResultSet) nextRawBlock() ([]byte, bool, error) {
	if !r.prefetching || r.prefetchCh == nil {
		return r.fetchRawBlock(0)
	}
	res := <-r.prefetchCh
	r.prefetchCh = nil
	r.prefetching = false
	return res.block, res.completed, res.err
}

func (r *ResultSet) isClosed() bool {
	return atomic.LoadUint32(&r.closed) != 0
}

func (r *ResultSet) ensureBoundRuntime() error {
	if r.client == nil || r.runtime == nil {
		return ErrQueryResultConnectionLost
	}
	if r.client.IsClosed() {
		return ErrUnifiedClosed
	}

	snapshot := r.client.loadRuntimeSnapshot()
	currentRuntime := snapshot.runtime
	currentGen := snapshot.generation

	if currentRuntime != r.runtime || currentGen != r.runtimeGen || !r.runtime.IsRunning() {
		return ErrQueryResultConnectionLost
	}
	return nil
}

// buildFetchRawBlockRequest builds a unified fetch_raw_block request payload.
func buildFetchRawBlockRequest(reqID uint64, resultID uint64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 26))
	writeUint64(buf, reqID)
	writeUint64(buf, resultID)
	writeUint64(buf, proto.FetchRawBlockMessage)
	writeUint16(buf, proto.BinaryProtocolVersion1)
	return buf.Bytes()
}

// parseFetchRawBlockResponse parses a binary fetch_raw_block response.
func parseFetchRawBlockResponse(respBytes []byte) ([]byte, bool, error) {
	if len(respBytes) < 51 {
		return nil, false, ErrInvalidFetchRawBlockResponse
	}
	version := binary.LittleEndian.Uint16(respBytes[16:])
	if version != proto.BinaryProtocolVersion1 {
		return nil, false, &Error{
			Type:    ErrorTypeProtocol,
			Message: "unsupported fetch raw block response version",
		}
	}

	code := binary.LittleEndian.Uint32(respBytes[34:])
	msgLen := int(binary.LittleEndian.Uint32(respBytes[38:]))
	if msgLen < 0 || len(respBytes) < 51+msgLen {
		return nil, false, ErrInvalidFetchRawBlockResponse
	}

	msgStart := 42
	msgEnd := msgStart + msgLen
	if msgEnd > len(respBytes) {
		return nil, false, ErrInvalidFetchRawBlockResponse
	}
	errMsg := string(respBytes[msgStart:msgEnd])
	if code != 0 {
		return nil, false, taosErrors.NewError(int(code), errMsg)
	}

	completedIndex := 50 + msgLen
	if completedIndex >= len(respBytes) {
		return nil, false, ErrInvalidFetchRawBlockResponse
	}
	completed := respBytes[completedIndex] == 1
	if completed {
		return nil, true, nil
	}

	blockLenStart := 51 + msgLen
	blockLenEnd := blockLenStart + 4
	if blockLenEnd > len(respBytes) {
		return nil, false, ErrInvalidFetchRawBlockResponse
	}
	blockLen := int(binary.LittleEndian.Uint32(respBytes[blockLenStart:blockLenEnd]))
	blockStart := 55 + msgLen
	blockEnd := blockStart + blockLen
	if blockLen < 0 || blockEnd > len(respBytes) {
		return nil, false, ErrInvalidFetchRawBlockResponse
	}
	block := append([]byte(nil), respBytes[blockStart:blockEnd]...)
	return block, false, nil
}
