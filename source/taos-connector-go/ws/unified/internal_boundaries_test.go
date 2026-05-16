package unified

import (
	"database/sql/driver"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// TestStmtValidateBindDataItemLockedBoundaries verifies the expected behavior for this scenario.
func TestStmtValidateBindDataItemLockedBoundaries(t *testing.T) {
	s := &Stmt{
		isInsert:  true,
		needTable: true,
		tagCount:  1,
		colCount:  2,
	}

	err := s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		Cols: [][]driver.Value{{int32(1)}, {int32(2)}},
	})
	require.ErrorIs(t, err, ErrStmtTableNameNotSet)

	err = s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		TableName: "tb",
		Cols:      [][]driver.Value{{int32(1)}, {int32(2)}},
	})
	require.ErrorIs(t, err, ErrStmtTagsNotSet)

	err = s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		TableName: "tb",
		Tags:      []driver.Value{"a", "b"},
		Cols:      [][]driver.Value{{int32(1)}, {int32(2)}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 1 tags, got 2")

	err = s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		TableName: "tb",
		Tags:      []driver.Value{"a"},
	})
	require.ErrorIs(t, err, ErrStmtColumnsNotSet)

	err = s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		TableName: "tb",
		Tags:      []driver.Value{"a"},
		Cols:      [][]driver.Value{{int32(1)}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 2 columns, got 1")

	err = s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		TableName: "tb",
		Tags:      []driver.Value{"a"},
		Cols:      [][]driver.Value{{}, {int32(1)}},
	})
	require.ErrorIs(t, err, ErrStmtNoRowsToAdd)

	err = s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		TableName: "tb",
		Tags:      []driver.Value{"a"},
		Cols:      [][]driver.Value{{int32(1)}, {}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "has no rows to add")

	err = s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		TableName: "tb",
		Tags:      []driver.Value{"a"},
		Cols:      [][]driver.Value{{int32(1), int32(2)}, {int32(3)}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "different row count")

	err = s.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		TableName: "tb",
		Tags:      []driver.Value{"a"},
		Cols:      [][]driver.Value{{int32(1), int32(2)}, {int32(3), int32(4)}},
	})
	require.NoError(t, err)

	queryStmt := &Stmt{
		isInsert:    false,
		fieldsCount: 1,
	}
	err = queryStmt.validateBindDataItemLocked(&commonstmt.TaosStmt2BindData{
		Cols: [][]driver.Value{{int32(1)}, {int32(2)}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 1 query params, got 2")
}

// TestStmtValidateCurrentBatchLockedBoundaries verifies the expected behavior for this scenario.
func TestStmtValidateCurrentBatchLockedBoundaries(t *testing.T) {
	s := &Stmt{
		isInsert:  true,
		needTable: true,
		tagCount:  1,
		colCount:  2,
		state:     newStmtCompatState(),
	}

	err := s.validateCurrentBatchLocked()
	require.ErrorIs(t, err, ErrStmtTableNameNotSet)

	s.state.setTableName("tb")
	err = s.validateCurrentBatchLocked()
	require.ErrorIs(t, err, ErrStmtTagsNotSet)

	s.state.setTags(param.NewParam(2).AddNchar("a").AddNchar("b"), param.NewColumnType(2).AddNchar(8).AddNchar(8))
	err = s.validateCurrentBatchLocked()
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 1 tags, got 2")

	s.state.setTags(param.NewParam(1).AddNchar("a"), param.NewColumnType(1).AddNchar(8))
	err = s.validateCurrentBatchLocked()
	require.ErrorIs(t, err, ErrStmtColumnsNotSet)

	s.state.bindParams([]*param.Param{param.NewParam(1).AddInt(1)}, param.NewColumnType(1).AddInt())
	err = s.validateCurrentBatchLocked()
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 2 columns, got 1")

	s.state.bindParams([]*param.Param{
		param.NewParam(0),
		param.NewParam(0),
	}, param.NewColumnType(2).AddInt().AddInt())
	err = s.validateCurrentBatchLocked()
	require.ErrorIs(t, err, ErrStmtNoRowsToAdd)

	s.state.bindParams([]*param.Param{
		param.NewParam(1).AddInt(1),
		param.NewParam(0),
	}, param.NewColumnType(2).AddInt().AddInt())
	err = s.validateCurrentBatchLocked()
	require.Error(t, err)
	require.Contains(t, err.Error(), "has no rows to add")

	s.state.bindParams([]*param.Param{
		param.NewParam(2).AddInt(1).AddInt(2),
		param.NewParam(1).AddInt(3),
	}, param.NewColumnType(2).AddInt().AddInt())
	err = s.validateCurrentBatchLocked()
	require.Error(t, err)
	require.Contains(t, err.Error(), "different row count")

	s.state.bindParams([]*param.Param{
		param.NewParam(2).AddInt(1).AddInt(2),
		param.NewParam(2).AddInt(3).AddInt(4),
	}, param.NewColumnType(2).AddInt().AddInt())
	require.NoError(t, s.validateCurrentBatchLocked())
}

// TestStmtControlFlowHelpers verifies the expected behavior for this scenario.
func TestStmtControlFlowHelpers(t *testing.T) {
	s := &Stmt{}
	require.NoError(t, s.enterCompatModeLocked())
	require.NoError(t, s.enterCompatModeLocked())
	require.ErrorIs(t, s.enterRawModeLocked(), ErrStmtBindAfterCompatAPI)

	s = &Stmt{}
	require.NoError(t, s.enterRawModeLocked())
	require.NoError(t, s.enterRawModeLocked())
	require.ErrorIs(t, s.enterCompatModeLocked(), ErrStmtCompatAPIAfterBind)

	s = &Stmt{closed: true}
	require.ErrorIs(t, s.checkNotClosedLocked(), ErrUnifiedClosed)

	s = &Stmt{}
	require.ErrorIs(t, s.checkPreparedLocked(), ErrStmtNotPrepared)
	s.sql = "select 1"
	s.schemaChanged = true
	require.ErrorIs(t, s.checkPreparedLocked(), ErrStmtSchemaChanged)
	s.schemaChanged = false
	require.NoError(t, s.checkPreparedLocked())
}

// TestStmtShouldReconnectLockedBranches verifies the expected behavior for this scenario.
func TestStmtShouldReconnectLockedBranches(t *testing.T) {
	s := &Stmt{}
	require.False(t, s.shouldReconnectLocked(fmt.Errorf("x"), nil))

	s = &Stmt{client: &Client{config: Config{AutoReconnect: false}}}
	require.False(t, s.shouldReconnectLocked(client.ClosedError, nil))

	s = &Stmt{client: &Client{config: Config{AutoReconnect: true}}}
	require.True(t, s.shouldReconnectLocked(client.ClosedError, nil))

	stoppedRuntime := client.NewClient(nil, 1)
	stoppedRuntime.Close()
	require.True(t, s.shouldReconnectLocked(fmt.Errorf("x"), stoppedRuntime))

	runningRuntime := client.NewClient(nil, 1)
	require.True(t, s.shouldReconnectLocked(&websocket.CloseError{Code: websocket.CloseAbnormalClosure}, runningRuntime))
	require.False(t, s.shouldReconnectLocked(fmt.Errorf("not reconnectable"), runningRuntime))
	require.False(t, s.shouldReconnectLocked(nil, runningRuntime))
}

// TestStmt2InitWithReconnectNoRuntimePaths verifies the expected behavior for this scenario.
func TestStmt2InitWithReconnectNoRuntimePaths(t *testing.T) {
	closedClient := &Client{}
	atomic.StoreUint32(&closedClient.closedFlag, 1)
	_, _, err := closedClient.stmt2InitWithReconnect(1)
	require.ErrorIs(t, err, ErrUnifiedClosed)

	openClient := &Client{config: Config{AutoReconnect: false}}
	_, _, err = openClient.stmt2InitWithReconnect(1)
	require.ErrorIs(t, err, client.ClosedError)
}

// TestQueryHelperBoundaries verifies the expected behavior for this scenario.
func TestQueryHelperBoundaries(t *testing.T) {
	require.Nil(t, buildResultSetFromQueryResp(nil, nil, 0, &proto.WSQueryResp{}))
	require.Nil(t, buildResultSetFromQueryResp(&Client{}, nil, 0, nil))
	require.Nil(t, buildResultSetFromQueryResp(&Client{}, nil, 0, &proto.WSQueryResp{IsUpdate: true}))

	c := &Client{config: Config{Timezone: time.UTC}}
	rs := buildResultSetFromQueryResp(c, nil, 10, &proto.WSQueryResp{
		ID:               12,
		FieldsCount:      1,
		FieldsNames:      []string{"v"},
		FieldsTypes:      []uint8{common.TSDB_DATA_TYPE_INT},
		FieldsLengths:    []int64{4},
		FieldsPrecisions: []int64{0},
		FieldsScales:     []int64{0},
		Precision:        0,
	})
	require.NotNil(t, rs)
	require.Equal(t, uint64(12), rs.resultIDValue())
	require.Equal(t, uint64(10), rs.runtimeGen)

	require.Nil(t, normalizeDisconnectedError(nil, "x"))
	err := normalizeDisconnectedError(client.ClosedError, "query closed")
	require.Error(t, err)
	require.True(t, IsConnectionRelatedError(err))
	require.True(t, IsConnectionDisconnectedError(err))
	sameErr := normalizeDisconnectedError(ErrUnifiedClosed, "ignored")
	require.ErrorIs(t, sameErr, ErrUnifiedClosed)
	plainErr := fmt.Errorf("plain")
	require.ErrorIs(t, normalizeDisconnectedError(plainErr, "x"), plainErr)
}

// TestRequestSendEnvelopeNilRuntimePaths verifies the expected behavior for this scenario.
func TestRequestSendEnvelopeNilRuntimePaths(t *testing.T) {
	c := &Client{config: Config{ReadTimeout: time.Millisecond}}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Type = websocket.TextMessage
	envelope.Msg.Reset()
	_, _ = envelope.Msg.WriteString("{}")

	_, acked, _, err := c.sendEnvelopeWithRuntime(nil, 1, envelope, 0, nil)
	require.ErrorIs(t, err, client.ClosedError)
	require.False(t, acked)

	err = c.sendEnvelopeNoResponse(nil, envelope)
	require.ErrorIs(t, err, client.ClosedError)

	runtime := client.NewClient(nil, 1)
	c.lock.Lock()
	c.runtime = runtime
	c.runtimeGen = 1
	c.publishRuntimeSnapshotLocked()
	c.lock.Unlock()

	err = c.sendEnvelopeNoResponse(runtime, nil)
	require.EqualError(t, err, errNilEnvelope.Error())
}

// TestNormalizeStmt2ValueCoversTypes verifies the expected behavior for this scenario.
func TestNormalizeStmt2ValueCoversTypes(t *testing.T) {
	now := time.Now().UTC().Round(time.Millisecond)
	ts := types.TaosTimestamp{T: now, Precision: common.PrecisionMilliSecond}

	cases := []struct {
		name      string
		value     driver.Value
		queryMode bool
		want      driver.Value
	}{
		{name: "taos_bool", value: types.TaosBool(true), want: true},
		{name: "taos_tinyint", value: types.TaosTinyint(1), want: int8(1)},
		{name: "taos_smallint", value: types.TaosSmallint(2), want: int16(2)},
		{name: "taos_int", value: types.TaosInt(3), want: int32(3)},
		{name: "taos_bigint", value: types.TaosBigint(4), want: int64(4)},
		{name: "taos_utinyint", value: types.TaosUTinyint(5), want: uint8(5)},
		{name: "taos_usmallint", value: types.TaosUSmallint(6), want: uint16(6)},
		{name: "taos_uint", value: types.TaosUInt(7), want: uint32(7)},
		{name: "taos_ubigint", value: types.TaosUBigint(8), want: uint64(8)},
		{name: "taos_float", value: types.TaosFloat(1.5), want: float32(1.5)},
		{name: "taos_double", value: types.TaosDouble(2.5), want: float64(2.5)},
		{name: "taos_binary", value: types.TaosBinary([]byte("b")), want: []byte("b")},
		{name: "taos_varbinary", value: types.TaosVarBinary([]byte("vb")), want: []byte("vb")},
		{name: "taos_nchar", value: types.TaosNchar("n"), want: "n"},
		{name: "taos_json", value: types.TaosJson([]byte(`{"k":1}`)), want: []byte(`{"k":1}`)},
		{name: "taos_geometry", value: types.TaosGeometry([]byte{1, 2}), want: []byte{1, 2}},
		{name: "taos_blob", value: types.TaosBlob([]byte{3, 4}), want: []byte{3, 4}},
		{name: "taos_timestamp_query", value: ts, queryMode: true, want: now.Format(time.RFC3339Nano)},
		{name: "taos_timestamp_insert", value: ts, want: common.TimeToTimestamp(now, common.PrecisionMilliSecond)},
	}

	for i := 0; i < len(cases); i++ {
		tc := cases[i]
		got, err := normalizeStmt2Value(tc.value, tc.queryMode)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}

	_, err := normalizeStmt2Value(struct{}{}, false)
	require.Error(t, err)
}

// TestStmtCompatStateAddBatchAndUpsertBoundaries verifies the expected behavior for this scenario.
func TestStmtCompatStateAddBatchAndUpsertBoundaries(t *testing.T) {
	state := newStmtCompatState()
	state.setTableName("tb")
	state.setTags(param.NewParam(1).AddNchar("tag"), param.NewColumnType(1).AddNchar(8))
	state.bindParams([]*param.Param{
		param.NewParam(1).AddInt(1),
	}, param.NewColumnType(1).AddInt())
	require.NoError(t, state.addBatch(true))
	require.True(t, state.hasBindData(true))

	queryState := newStmtCompatState()
	queryState.bindParams([]*param.Param{
		param.NewParam(1).AddBinary([]byte("x")),
	}, param.NewColumnType(1).AddBinary(1))
	require.NoError(t, queryState.addBatch(false))
	require.True(t, queryState.hasBindData(false))

	err := state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{TableName: "tb", Cols: [][]driver.Value{{int32(1)}}},
	}, true)
	require.NoError(t, err)
	err = state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{TableName: "tb", Cols: [][]driver.Value{{int32(2)}, {int32(3)}}},
	}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "col count not match")
}

// TestReconnectAndDisconnectFlags verifies the expected behavior for this scenario.
func TestReconnectAndDisconnectFlags(t *testing.T) {
	require.False(t, isReconnectableError(nil))
	require.True(t, isReconnectableError(&net.OpError{}))
	require.True(t, isReconnectableError(&websocket.CloseError{Code: websocket.CloseAbnormalClosure}))

	require.False(t, IsConnectionDisconnectedError(fmt.Errorf("x")))
	require.False(t, IsReconnectFailedError(fmt.Errorf("x")))
}
