package unified

import (
	"database/sql/driver"
	"testing"

	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

// TestStmtCompatStateSetTableNameOverrideBeforeAddBatch verifies the expected behavior for this scenario.
func TestStmtCompatStateSetTableNameOverrideBeforeAddBatch(t *testing.T) {
	state := newStmtCompatState()
	state.setTableName("tb1")
	state.setTableName("tb2")
	state.bindParams([]*param.Param{param.NewParam(1).AddInt(1)}, param.NewColumnType(1).AddInt())
	if err := state.addBatch(true); err != nil {
		t.Fatalf("unexpected add batch error: %v", err)
	}

	data := state.bindData(true)
	if len(data) != 1 {
		t.Fatalf("expect 1 batch, got %d", len(data))
	}
	if data[0].TableName != "tb2" {
		t.Fatalf("expect overwritten table name, got %s", data[0].TableName)
	}
}

// TestStmtCompatStateBindParamsOverwriteBeforeAddBatch verifies the expected behavior for this scenario.
func TestStmtCompatStateBindParamsOverwriteBeforeAddBatch(t *testing.T) {
	state := newStmtCompatState()
	first := []*param.Param{param.NewParam(1).AddInt(1)}
	second := []*param.Param{param.NewParam(1).AddInt(2)}
	state.bindParams(first, param.NewColumnType(1).AddInt())
	state.bindParams(second, param.NewColumnType(1).AddInt())
	if err := state.addBatch(true); err != nil {
		t.Fatalf("unexpected add batch error: %v", err)
	}

	data := state.bindData(true)
	if len(data) != 1 {
		t.Fatalf("expect 1 batch, got %d", len(data))
	}
	if len(data[0].Cols) != 1 {
		t.Fatalf("expect 1 param col, got %d", len(data[0].Cols))
	}
	if got, ok := data[0].Cols[0][0].(int32); !ok || got != int32(2) {
		t.Fatalf("expect last bind params to win, got %v", data[0].Cols[0][0])
	}
}

// TestStmtCompatStateAddBatchResetsCurrent verifies the expected behavior for this scenario.
func TestStmtCompatStateAddBatchResetsCurrent(t *testing.T) {
	state := newStmtCompatState()
	state.setTableName("tb")
	state.setTags(param.NewParam(1).AddNchar("tag"), param.NewColumnType(1).AddNchar(16))
	state.bindParams([]*param.Param{param.NewParam(1).AddInt(3)}, param.NewColumnType(1).AddInt())
	if err := state.addBatch(true); err != nil {
		t.Fatalf("unexpected add batch error: %v", err)
	}

	if state.Current.TableName != "" {
		t.Fatalf("expect empty current table name, got %s", state.Current.TableName)
	}
	if state.Current.Tags != nil {
		t.Fatal("expect current tags reset")
	}
	if state.Current.Params != nil {
		t.Fatal("expect current params reset")
	}
}

// TestStmtCompatStateResetClearsBatches verifies the expected behavior for this scenario.
func TestStmtCompatStateResetClearsBatches(t *testing.T) {
	state := newStmtCompatState()
	err := state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(1)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}
	state.reset()

	if len(state.bindData(true)) != 0 {
		t.Fatalf("expect no batches after reset, got %d", len(state.bindData(true)))
	}
	if state.Current.TableName != "" {
		t.Fatalf("expect empty current table name, got %s", state.Current.TableName)
	}
}

// TestStmtCompatStateMergeSameTable verifies the expected behavior for this scenario.
func TestStmtCompatStateMergeSameTable(t *testing.T) {
	state := newStmtCompatState()
	err := state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(1)}, {int32(10)}},
		},
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(2)}, {int32(20)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}
	data := state.bindData(true)
	if len(data) != 1 {
		t.Fatalf("expect merged single table batch, got %d", len(data))
	}
	if len(data[0].Cols) != 2 || len(data[0].Cols[0]) != 2 || len(data[0].Cols[1]) != 2 {
		t.Fatalf("expect merged row count 2, got %+v", data[0].Cols)
	}
}

// TestStmtCompatStateMergeSameTableAcrossSetRawBindDataCalls verifies the expected behavior for this scenario.
func TestStmtCompatStateMergeSameTableAcrossSetRawBindDataCalls(t *testing.T) {
	state := newStmtCompatState()
	err := state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(1)}, {int32(10)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}
	err = state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(2)}, {int32(20)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}
	data := state.bindData(true)
	if len(data) != 1 {
		t.Fatalf("expect merged single table batch, got %d", len(data))
	}
	if len(data[0].Cols) != 2 || len(data[0].Cols[0]) != 2 || len(data[0].Cols[1]) != 2 {
		t.Fatalf("expect merged row count 2, got %+v", data[0].Cols)
	}
}

// TestStmtCompatStateSetRawBindDataInsertErrorDoesNotMutateExistingState verifies atomic behavior.
func TestStmtCompatStateSetRawBindDataInsertErrorDoesNotMutateExistingState(t *testing.T) {
	state := newStmtCompatState()
	err := state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(1)}, {int32(10)}},
		},
	}, true)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}

	err = state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{int32(2)}},
		},
	}, true)
	if err == nil {
		t.Fatal("expect col count mismatch error")
	}

	data := state.bindData(true)
	if len(data) != 1 {
		t.Fatalf("expect existing single table batch preserved, got %d", len(data))
	}
	if len(data[0].Cols) != 2 {
		t.Fatalf("expect 2 columns preserved, got %d", len(data[0].Cols))
	}
	if len(data[0].Cols[0]) != 1 || len(data[0].Cols[1]) != 1 {
		t.Fatalf("expect 1 row preserved, got %+v", data[0].Cols)
	}
	if got, ok := data[0].Cols[0][0].(int32); !ok || got != int32(1) {
		t.Fatalf("unexpected preserved col0 value: %v", data[0].Cols[0][0])
	}
	if got, ok := data[0].Cols[1][0].(int32); !ok || got != int32(10) {
		t.Fatalf("unexpected preserved col1 value: %v", data[0].Cols[1][0])
	}
}

// TestStmtCompatStateSetRawBindDataQueryRejectsRebindBeforeExec verifies query bind lifecycle.
func TestStmtCompatStateSetRawBindDataQueryRejectsRebindBeforeExec(t *testing.T) {
	state := newStmtCompatState()
	err := state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{int32(1)}},
		},
	}, false)
	if err != nil {
		t.Fatalf("unexpected set raw bind data error: %v", err)
	}

	err = state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{int32(2)}},
		},
	}, false)
	if err == nil {
		t.Fatal("expect query rebind before exec error")
	}
	if err != ErrStmtQueryRebindBeforeExec {
		t.Fatalf("unexpected query rebind error: %v", err)
	}
	if state.Query == nil || len(state.Query.Cols) != 1 || len(state.Query.Cols[0]) != 1 {
		t.Fatalf("expect first query bind preserved, got %+v", state.Query)
	}
	if got, ok := state.Query.Cols[0][0].(int32); !ok || got != int32(1) {
		t.Fatalf("unexpected preserved query bind value: %v", state.Query.Cols[0][0])
	}

	state.reset()
	err = state.setRawBindData([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{int32(3)}},
		},
	}, false)
	if err != nil {
		t.Fatalf("unexpected set raw bind data after reset error: %v", err)
	}
}
