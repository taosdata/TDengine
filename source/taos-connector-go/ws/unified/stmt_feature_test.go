package unified

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// TestStmtCompatFlowWithDeprecatedAPIsStillWorks verifies the expected behavior for this scenario.
func TestStmtCompatFlowWithDeprecatedAPIsStillWorks(t *testing.T) {
	s := &Stmt{
		sql:       "insert into ? using stb tags(?) values(?)",
		isInsert:  true,
		needTable: true,
		tagCount:  1,
		colCount:  1,
		fields: []*commonstmt.Stmt2AllField{
			{Name: "tg", FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_TAG},
			{Name: "v", FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
		},
		state: newStmtCompatState(),
	}

	require.NoError(t, s.SetTableName("tb1"))
	require.NoError(t, s.SetTags(param.NewParam(1).AddNchar("t1"), param.NewColumnType(1).AddNchar(8)))
	require.NoError(t, s.BindParam(
		[]*param.Param{param.NewParam(1).AddInt(1)},
		param.NewColumnType(1).AddInt(),
	))
	require.NoError(t, s.AddBatch())

	payload, err := s.buildExecPayloadLocked()
	require.NoError(t, err)
	require.NotEmpty(t, payload)
}

// TestStmtDeprecatedAPIValidation verifies the expected behavior for this scenario.
func TestStmtDeprecatedAPIValidation(t *testing.T) {
	s := &Stmt{
		sql:      "insert into t values(?)",
		isInsert: true,
		state:    newStmtCompatState(),
	}

	err := s.SetTableName("tb1")
	require.ErrorIs(t, err, ErrStmtTableNameNotRequired)

	err = s.SetTags(param.NewParam(1).AddNchar("t1"), param.NewColumnType(1).AddNchar(8))
	require.ErrorIs(t, err, ErrStmtTagsNotNeeded)
}

// TestStmtMetadataHelpers verifies the expected behavior for this scenario.
func TestStmtMetadataHelpers(t *testing.T) {
	s := &Stmt{
		sql:      "insert into t values(?)",
		isInsert: true,
		colCount: 2,
		fields: []*commonstmt.Stmt2AllField{
			{Name: "ts", FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: 0, BindType: commonstmt.TAOS_FIELD_COL},
			{Name: "v", FieldType: common.TSDB_DATA_TYPE_INT, Precision: 0, BindType: commonstmt.TAOS_FIELD_COL},
			{Name: "tg", FieldType: common.TSDB_DATA_TYPE_NCHAR, Precision: 0, BindType: commonstmt.TAOS_FIELD_TAG},
		},
		lastAffected: 3,
	}

	affected := s.AffectedRows()
	assert.Equal(t, 3, affected)

	isInsert, err := s.IsInsert()
	require.NoError(t, err)
	assert.True(t, isInsert)

	cols, err := s.ColFields()
	require.NoError(t, err)
	require.Len(t, cols, 2)
	assert.Equal(t, "ts", cols[0].Name)
	assert.Equal(t, "v", cols[1].Name)
}

// TestNormalizeStmtErrorCoversClosedAndConnectionRelated verifies the expected behavior for this scenario.
func TestNormalizeStmtErrorCoversClosedAndConnectionRelated(t *testing.T) {
	assert.Nil(t, normalizeStmtError(nil))
	assert.ErrorIs(t, normalizeStmtError(client.ClosedError), ErrStmtConnectionLost)
	assert.ErrorIs(t, normalizeStmtError(ErrUnifiedClosed), ErrUnifiedClosed)
}

// TestStmtResetPrepareLocked verifies the expected behavior for this scenario.
func TestStmtResetPrepareLocked(t *testing.T) {
	s := &Stmt{
		sql:           "insert into t values(?)",
		isInsert:      true,
		fieldsCount:   1,
		fields:        []*commonstmt.Stmt2AllField{{Name: "v", BindType: commonstmt.TAOS_FIELD_COL}},
		needTable:     true,
		tagCount:      1,
		colCount:      1,
		schemaChanged: true,
		lastAffected:  10,
		bindMode:      stmtBindModeRaw,
		state:         newStmtCompatState(),
	}
	require.NoError(t, s.state.setRawBindData([]*commonstmt.TaosStmt2BindData{{
		TableName: "tb1",
		Cols:      [][]driver.Value{{int32(1)}},
	}}, true))

	s.resetPrepareLocked()
	assert.Equal(t, "", s.sql)
	assert.False(t, s.isInsert)
	assert.Zero(t, s.fieldsCount)
	assert.Nil(t, s.fields)
	assert.False(t, s.needTable)
	assert.Zero(t, s.tagCount)
	assert.Zero(t, s.colCount)
	assert.False(t, s.schemaChanged)
	assert.Zero(t, s.lastAffected)
	assert.Equal(t, stmtBindModeUnset, s.bindMode)
	assert.False(t, s.state.hasBindData(true))
}

// TestSamePrepareMetadata verifies the expected behavior for this scenario.
func TestSamePrepareMetadata(t *testing.T) {
	current := &Stmt{
		isInsert:    true,
		fieldsCount: 1,
		fields: []*commonstmt.Stmt2AllField{
			{Name: "ts", FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: 0, BindType: commonstmt.TAOS_FIELD_COL},
		},
	}
	resp := &proto.Stmt2PrepareResponse{
		IsInsert:    true,
		FieldsCount: 1,
		Fields: []*commonstmt.Stmt2AllField{
			{Name: "ts", FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: 0, BindType: commonstmt.TAOS_FIELD_COL},
		},
	}
	assert.True(t, samePrepareMetadata(current, resp))

	resp.Fields[0].Name = "changed"
	assert.False(t, samePrepareMetadata(current, resp))
}
