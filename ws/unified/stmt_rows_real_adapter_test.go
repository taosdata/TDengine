package unified

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/client"
)

// TestUnifiedStmtAndRowsRealAdapterCoverage verifies the expected behavior for this scenario.
func TestUnifiedStmtAndRowsRealAdapterCoverage(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration test in short mode")
	}
	ensureTaosadapterBinary(t)

	ports, stops := startAdapters(t, 2)
	t.Cleanup(func() {
		for i := len(ports) - 1; i >= 0; i-- {
			if stop, ok := stops[ports[i]]; ok && stop != nil {
				stop()
				delete(stops, ports[i])
			}
		}
	})

	db := createTestDatabase(t, ports)
	c := newIntegrationUnifiedClient(t, ports, db)
	defer c.Close()

	table := fmt.Sprintf("unified_stmt_rows_cov_%d", time.Now().UnixNano())
	_, err := c.Exec(0, fmt.Sprintf("create table if not exists %s.%s(ts timestamp, v int, note nchar(16))", db, table))
	require.NoError(t, err)

	insertSQL := fmt.Sprintf("insert into %s.%s values(?,?,?)", db, table)
	querySQL := fmt.Sprintf("select ts,v,note from %s.%s where v >= ? order by ts", db, table)

	t.Run("stmt_bind_exec_use_result_rows", func(t *testing.T) {
		stmt, stmtErr := c.InitStmt(0)
		require.NoError(t, stmtErr)
		defer func() {
			_ = stmt.Close(0)
		}()

		require.NoError(t, stmt.Prepare(0, insertSQL))
		isInsert, stmtErr := stmt.IsInsert()
		require.NoError(t, stmtErr)
		require.True(t, isInsert)
		insertFields, stmtErr := stmt.ColFields()
		require.NoError(t, stmtErr)
		require.Len(t, insertFields, 3)

		baseTS := time.Now().UTC().Round(time.Millisecond)
		require.NoError(t, stmt.Bind([]*commonstmt.TaosStmt2BindData{
			{
				Cols: [][]driver.Value{
					{baseTS, baseTS.Add(time.Second)},
					{int32(11), int32(22)},
					{"note_1", "note_2"},
				},
			},
		}))
		affected, stmtErr := stmt.Exec(0)
		require.NoError(t, stmtErr)
		require.Equal(t, 2, affected)
		require.Equal(t, 2, stmt.AffectedRows())

		require.NoError(t, stmt.Close(0))
		require.NoError(t, stmt.Close(0))

		queryStmt, queryErr := c.InitStmt(0)
		require.NoError(t, queryErr)
		defer func() {
			_ = queryStmt.Close(0)
		}()

		require.NoError(t, queryStmt.Prepare(0, querySQL))
		queryInsert, queryErr := queryStmt.IsInsert()
		require.NoError(t, queryErr)
		require.False(t, queryInsert)
		queryFields, queryErr := queryStmt.ColFields()
		require.NoError(t, queryErr)
		require.Nil(t, queryFields)

		require.NoError(t, queryStmt.Bind([]*commonstmt.TaosStmt2BindData{
			{
				Cols: [][]driver.Value{{int32(10)}},
			},
		}))
		_, queryErr = queryStmt.Exec(0)
		require.NoError(t, queryErr)

		rows, queryErr := queryStmt.UseResult(0)
		require.NoError(t, queryErr)
		require.NotNil(t, rows)
		require.NotZero(t, rows.resultIDValue())
		require.Equal(t, []string{"ts", "v", "note"}, rows.Columns())
		require.NotEmpty(t, rows.ColumnTypeDatabaseTypeName(0))
		_, _ = rows.ColumnTypeLength(1)
		_, _, _ = rows.ColumnTypePrecisionScale(1)
		require.NotNil(t, rows.ColumnTypeScanType(1))

		result := make([][]driver.Value, 0, 4)
		for {
			values := make([]driver.Value, 3)
			nextErr := rows.Next(values)
			if errors.Is(nextErr, io.EOF) {
				break
			}
			require.NoError(t, nextErr)
			result = append(result, append([]driver.Value(nil), values...))
		}
		require.Len(t, result, 2)
		require.Equal(t, int32(11), result[0][1])
		require.Equal(t, int32(22), result[1][1])

		require.NoError(t, rows.freeResult(0))
		require.NoError(t, rows.freeResult(0))
		require.ErrorIs(t, rows.Next(make([]driver.Value, 3)), ErrQueryResultClosed)
		require.NoError(t, rows.Close())
	})

	t.Run("query_exec_and_runtime_mismatch_result", func(t *testing.T) {
		_, err = c.Exec(0, fmt.Sprintf("insert into %s.%s values(now, 33, 'note_3')", db, table))
		require.NoError(t, err)

		affected, execErr := c.Exec(0, fmt.Sprintf("select ts,v,note from %s.%s limit 1", db, table))
		require.NoError(t, execErr)
		require.Equal(t, 0, affected)

		rows, queryErr := c.Query(0, fmt.Sprintf("select ts,v,note from %s.%s order by ts limit 1", db, table))
		require.NoError(t, queryErr)
		require.NotNil(t, rows)
		require.NotZero(t, rows.resultIDValue())

		oldRuntime := c.runtimeClient()
		require.NotNil(t, oldRuntime)
		require.NoError(t, c.reconnectWithBootstrap(c.defaultBootstrap, nil))

		_, _, queryErr = rows.fetchRawBlock(0)
		require.ErrorIs(t, queryErr, ErrQueryResultConnectionLost)
		require.ErrorIs(t, rows.freeResult(0), ErrQueryResultConnectionLost)
		require.NoError(t, rows.freeResult(0))
	})

	t.Run("stmt_reconnect_paths", func(t *testing.T) {
		// init reconnect path
		runtime := c.runtimeClient()
		require.NotNil(t, runtime)
		runtime.Close()
		_, initErr := c.InitStmt(0)
		require.NoError(t, initErr)

		// prepare reconnect path
		prepareStmt, prepareErr := c.InitStmt(0)
		require.NoError(t, prepareErr)
		active := activeAdapterPort(t, c)
		stopByPort(t, active, stops)
		require.NoError(t, prepareStmt.Prepare(0, insertSQL))
		require.NoError(t, prepareStmt.Close(0))
		stops[active] = restartAdapterOnPort(t, active)

		// exec reconnect path
		execStmt, execInitErr := c.InitStmt(0)
		require.NoError(t, execInitErr)
		require.NoError(t, execStmt.Prepare(0, insertSQL))
		require.NoError(t, execStmt.Bind([]*commonstmt.TaosStmt2BindData{
			{
				Cols: [][]driver.Value{
					{time.Now().UTC().Round(time.Millisecond)},
					{int32(44)},
					{"note_4"},
				},
			},
		}))
		active = activeAdapterPort(t, c)
		stopByPort(t, active, stops)
		affected, execErr := execStmt.Exec(0)
		require.NoError(t, execErr)
		require.Equal(t, 1, affected)
		require.NoError(t, execStmt.Close(0))
		stops[active] = restartAdapterOnPort(t, active)

		// schema-changed branch during reprepare after reconnect.
		schemaStmt, schemaErr := c.InitStmt(0)
		require.NoError(t, schemaErr)
		require.NoError(t, schemaStmt.Prepare(0, insertSQL))
		schemaStmt.fieldsCount += 1
		require.NoError(t, schemaStmt.Bind([]*commonstmt.TaosStmt2BindData{
			{
				Cols: [][]driver.Value{
					{time.Now().UTC().Round(time.Millisecond)},
					{int32(55)},
					{"note_5"},
				},
			},
		}))
		active = activeAdapterPort(t, c)
		stopByPort(t, active, stops)
		_, schemaErr = schemaStmt.Exec(0)
		require.ErrorIs(t, schemaErr, ErrStmtReprepareSchemaChanged)
		_, schemaErr = schemaStmt.IsInsert()
		require.ErrorIs(t, schemaErr, ErrStmtSchemaChanged)
		require.NoError(t, schemaStmt.Prepare(0, insertSQL))
		require.NoError(t, schemaStmt.Close(0))
		stops[active] = restartAdapterOnPort(t, active)
	})

	t.Run("request_runtime_mismatch_paths", func(t *testing.T) {
		oldRuntime := c.runtimeClient()
		require.NotNil(t, oldRuntime)
		require.NoError(t, c.reconnectWithBootstrap(c.defaultBootstrap, nil))

		envelope := client.GlobalEnvelopePool.Get()
		defer client.GlobalEnvelopePool.Put(envelope)
		envelope.Type = websocket.TextMessage
		envelope.Msg.Reset()
		_, _ = envelope.Msg.WriteString("{}")

		_, acked, _, sendErr := c.sendEnvelopeWithRuntime(oldRuntime, uint64(time.Now().UnixNano()), envelope, 0, nil)
		require.ErrorIs(t, sendErr, client.ClosedError)
		require.False(t, acked)

		sendErr = c.sendEnvelopeNoResponse(oldRuntime, envelope)
		require.ErrorIs(t, sendErr, client.ClosedError)
	})
}

// TestUnifiedSmallCoverageEdges verifies the expected behavior for this scenario.
func TestUnifiedSmallCoverageEdges(t *testing.T) {
	t.Run("failover_endpoints_copy", func(t *testing.T) {
		state, err := newFailoverState([]string{"ws://a", "ws://b"})
		require.NoError(t, err)
		endpoints := state.endpointsCopy()
		require.Equal(t, []string{"ws://a", "ws://b"}, endpoints)
		endpoints[0] = "changed"
		require.Equal(t, "ws://a", state.endpointsCopy()[0])
	})

	t.Run("stmt_compat_state_clear_bind_data", func(t *testing.T) {
		state := newStmtCompatState()
		require.NoError(t, state.setRawBindData([]*commonstmt.TaosStmt2BindData{
			{
				TableName: "tb1",
				Cols:      [][]driver.Value{{int32(1)}},
			},
		}, true))
		require.True(t, state.hasBindData(true))
		state.clearBindData()
		require.False(t, state.hasBindData(true))
		require.Nil(t, state.bindData(true))
	})

	t.Run("rows_and_error_nil_branches", func(t *testing.T) {
		var rs *ResultSet
		require.Equal(t, uint64(0), rs.resultIDValue())
		require.ErrorIs(t, rs.freeResult(0), ErrQueryResultClosed)
		require.ErrorIs(t, rs.Close(), ErrQueryResultClosed)

		var unifiedErr *Error
		require.Equal(t, "", unifiedErr.Error())
		require.Nil(t, unifiedErr.Unwrap())

		baseErr := fmt.Errorf("root cause")
		wrapped := &Error{Type: ErrorTypeProtocol, Cause: baseErr}
		require.Equal(t, "root cause", wrapped.Error())
		require.ErrorIs(t, wrapped, baseErr)
	})

	t.Run("connector_nil_receiver", func(t *testing.T) {
		var connector *Connector
		require.Equal(t, Config{}, connector.Config())
		_, err := connector.Connect()
		require.ErrorIs(t, err, ErrNilConfig)
	})
}
