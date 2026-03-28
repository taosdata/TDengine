package unified

import (
	"database/sql/driver"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
)

type stmt2Binder interface {
	Bind(params []*commonstmt.TaosStmt2BindData) error
}

var _ stmt2Binder = (*Stmt)(nil)

// TestStmtBindWithParamColumnsStillWorks verifies the expected behavior for this scenario.
func TestStmtBindWithParamColumnsStillWorks(t *testing.T) {
	s := &Stmt{
		sql:      "insert into t values(?, ?)",
		isInsert: true,
		colCount: 2,
		state:    newStmtCompatState(),
	}

	now := time.Now()
	err := s.BindParam([]*param.Param{
		param.NewParam(1).AddTimestamp(now, 0),
		param.NewParam(1).AddInt(1),
	}, param.NewColumnType(2).AddTimestamp().AddInt())
	require.NoError(t, err)

	err = s.AddBatch()
	require.NoError(t, err)
	bindData := s.state.bindData(true)
	require.Len(t, bindData, 1)
	assert.Equal(t, 2, len(bindData[0].Cols))
	_, ok := bindData[0].Cols[0][0].(int64)
	assert.True(t, ok, "compatible param bind should normalize timestamp to int64")

	err = s.BindParam([]*param.Param{
		param.NewParam(1).AddTimestamp(now.Add(time.Second), 0),
		param.NewParam(1).AddInt(2),
	}, param.NewColumnType(2).AddTimestamp().AddInt())
	require.NoError(t, err)
	err = s.AddBatch()
	require.NoError(t, err)
	require.Len(t, s.state.bindData(true), 1)
}

// TestStmtBindWithStmt2BindData verifies the expected behavior for this scenario.
func TestStmtBindWithStmt2BindData(t *testing.T) {
	s := &Stmt{
		sql:      "insert into t values(?, ?)",
		isInsert: true,
		colCount: 2,
		state:    newStmtCompatState(),
	}

	params := []*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{time.Now()},
				{int32(1)},
			},
		},
		{
			Cols: [][]driver.Value{
				{time.Now().Add(time.Second)},
				{int32(2)},
			},
		},
	}

	err := s.Bind(params)
	require.NoError(t, err)
	bindData := s.state.bindData(true)
	require.Len(t, bindData, 1)
	require.Len(t, bindData[0].Cols[0], 2)
	assert.Same(t, params[0], bindData[0], "stmt2 bind path should keep raw bind data without cloning")
}

// TestStmtBindWithStmt2BindDataRejectsQueryTableNameOrTags verifies the expected behavior for this scenario.
func TestStmtBindWithStmt2BindDataRejectsQueryTableNameOrTags(t *testing.T) {
	s := &Stmt{
		sql:         "select * from t where v > ?",
		isInsert:    false,
		fieldsCount: 1,
		state:       newStmtCompatState(),
	}

	err := s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb1",
			Cols:      [][]driver.Value{{int32(1)}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query statement does not support table name")

	err = s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Tags: []driver.Value{"x"},
			Cols: [][]driver.Value{{int32(1)}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query statement does not support tags")
}

// TestStmtBindWithStmt2BindDataQueryRejectsRebindBeforeExec verifies the expected behavior for this scenario.
func TestStmtBindWithStmt2BindDataQueryRejectsRebindBeforeExec(t *testing.T) {
	s := &Stmt{
		sql:         "select * from t where v > ?",
		isInsert:    false,
		fieldsCount: 1,
		state:       newStmtCompatState(),
	}

	err := s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{int32(1)}},
		},
	})
	require.NoError(t, err)

	err = s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{int32(2)}},
		},
	})
	require.ErrorIs(t, err, ErrStmtQueryRebindBeforeExec)

	// Simulate post-exec cleanup: after reset, query bind is allowed again.
	s.state.reset()
	err = s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{int32(3)}},
		},
	})
	require.NoError(t, err)
}

// TestStmtBindWithStmt2BindDataAppendsAcrossCalls verifies the expected behavior for this scenario.
func TestStmtBindWithStmt2BindDataAppendsAcrossCalls(t *testing.T) {
	s := &Stmt{
		sql:      "insert into t values(?, ?)",
		isInsert: true,
		colCount: 2,
		state:    newStmtCompatState(),
	}

	first := []*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{time.Now()}, {int32(1)}},
		},
	}
	err := s.Bind(first)
	require.NoError(t, err)
	require.Len(t, s.state.bindData(true), 1)

	second := []*commonstmt.TaosStmt2BindData{
		{
			TableName: "tb",
			Cols:      [][]driver.Value{{time.Now().Add(time.Second)}, {int32(2)}},
		},
	}
	err = s.Bind(second)
	require.NoError(t, err)
	bindData := s.state.bindData(true)
	require.Len(t, bindData, 1)
	require.Len(t, bindData[0].Cols[0], 2)
	require.Same(t, first[0], bindData[0], "same table should append to existing cached bind data")
}

// TestStmtBindWithStmt2BindDataRejectsCompatToRawModeSwitch verifies the expected behavior for this scenario.
func TestStmtBindWithStmt2BindDataRejectsCompatToRawModeSwitch(t *testing.T) {
	s := &Stmt{
		sql:      "insert into t values(?, ?)",
		isInsert: true,
		colCount: 2,
		state:    newStmtCompatState(),
	}

	err := s.BindParam([]*param.Param{
		param.NewParam(1).AddTimestamp(time.Now(), 0),
		param.NewParam(1).AddInt(1),
	}, param.NewColumnType(2).AddTimestamp().AddInt())
	require.NoError(t, err)
	err = s.AddBatch()
	require.NoError(t, err)

	err = s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{time.Now().Add(time.Second)}, {int32(2)}},
		},
	})
	require.ErrorIs(t, err, ErrStmtBindAfterCompatAPI)
}

// TestStmtBindParamRejectsRawToCompatModeSwitch verifies the expected behavior for this scenario.
func TestStmtBindParamRejectsRawToCompatModeSwitch(t *testing.T) {
	s := &Stmt{
		sql:      "insert into t values(?, ?)",
		isInsert: true,
		colCount: 2,
		state:    newStmtCompatState(),
	}

	err := s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{time.Now()}, {int32(1)}},
		},
	})
	require.NoError(t, err)

	err = s.BindParam([]*param.Param{
		param.NewParam(1).AddTimestamp(time.Now().Add(time.Second), 0),
		param.NewParam(1).AddInt(2),
	}, param.NewColumnType(2).AddTimestamp().AddInt())
	require.ErrorIs(t, err, ErrStmtCompatAPIAfterBind)

	err = s.AddBatch()
	require.ErrorIs(t, err, ErrStmtCompatAPIAfterBind)
}

// TestStmtBuildExecPayloadUsesRawBindData verifies the expected behavior for this scenario.
func TestStmtBuildExecPayloadUsesRawBindData(t *testing.T) {
	s := &Stmt{
		sql:         "select * from t where v > ?",
		isInsert:    false,
		fieldsCount: 1,
		state:       newStmtCompatState(),
	}

	err := s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{{int32(1)}},
		},
	})
	require.NoError(t, err)
	require.Len(t, s.state.bindData(false), 1)

	payload, err := s.buildExecPayloadLocked()
	require.NoError(t, err)
	require.NotEmpty(t, payload)
}

// TestStmtBuildExecPayloadWithRawDecimalAndBlob verifies the expected behavior for this scenario.
func TestStmtBuildExecPayloadWithRawDecimalAndBlob(t *testing.T) {
	s := &Stmt{
		sql:      "insert into t values(?, ?, ?)",
		isInsert: true,
		colCount: 3,
		fields: []*commonstmt.Stmt2AllField{
			{
				Name:      "v1",
				FieldType: common.TSDB_DATA_TYPE_DECIMAL,
				BindType:  commonstmt.TAOS_FIELD_COL,
			},
			{
				Name:      "v2",
				FieldType: common.TSDB_DATA_TYPE_DECIMAL64,
				BindType:  commonstmt.TAOS_FIELD_COL,
			},
			{
				Name:      "v3",
				FieldType: common.TSDB_DATA_TYPE_BLOB,
				BindType:  commonstmt.TAOS_FIELD_COL,
			},
		},
		state: newStmtCompatState(),
	}

	err := s.Bind([]*commonstmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{"123.456", "789.012"},
				{[]byte("3.1415"), "2.7182"},
				{[]byte{0x01, 0x02}, "blob-text"},
			},
		},
	})
	require.NoError(t, err)

	payload, err := s.buildExecPayloadLocked()
	require.NoError(t, err)
	require.Greater(t, len(payload), 28)
	assert.Equal(t, uint32(3), binary.LittleEndian.Uint32(payload[12:16]))
}
