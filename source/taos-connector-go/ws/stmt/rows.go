package stmt

import (
	"database/sql/driver"
	"io"
	"reflect"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

// Rows is a compatibility wrapper around unified.ResultSet.
// The primary path is created by stmt.UseResult() and delegates to unified.
// When created by deprecated NewRows, it carries metadata only.
// Deprecated: use unified.ResultSet from package ws/unified instead.
type Rows struct {
	resultSet *unified.ResultSet

	// Compatibility-only metadata for legacy constructor NewRows.
	fieldsNames      []string
	fieldsTypes      []uint8
	fieldsLengths    []int64
	fieldsPrecisions []int64
	fieldsScales     []int64
}

// NewRows is kept for API compatibility only.
// Deprecated: compatibility-only metadata wrapper; it no longer fetches row data.
// Use stmt.UseResult() or unified.ResultSet from package ws/unified instead.
func NewRows(_ *WSConn, _ *client.Client, resp *UseResultResp, _ *time.Location) *Rows {
	rows := &Rows{}
	if resp == nil {
		return rows
	}
	rows.fieldsNames = append(rows.fieldsNames, resp.FieldsNames...)
	rows.fieldsTypes = append(rows.fieldsTypes, resp.FieldsTypes...)
	rows.fieldsLengths = append(rows.fieldsLengths, resp.FieldsLengths...)
	rows.fieldsPrecisions = append(rows.fieldsPrecisions, resp.FieldsPrecisions...)
	rows.fieldsScales = append(rows.fieldsScales, resp.FieldsScales...)
	return rows
}

func newRowsFromResultSet(resultSet *unified.ResultSet) *Rows {
	return &Rows{
		resultSet: resultSet,
	}
}

// Deprecated: use unified.ResultSet.ColumnTypePrecisionScale instead.
func (rs *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if rs.resultSet != nil {
		return rs.resultSet.ColumnTypePrecisionScale(index)
	}
	if index < 0 || index >= len(rs.fieldsTypes) {
		return 0, 0, false
	}
	if rs.fieldsTypes[index] != common.TSDB_DATA_TYPE_DECIMAL && rs.fieldsTypes[index] != common.TSDB_DATA_TYPE_DECIMAL64 {
		return 0, 0, false
	}
	if index >= len(rs.fieldsPrecisions) || index >= len(rs.fieldsScales) {
		return 0, 0, false
	}
	return rs.fieldsPrecisions[index], rs.fieldsScales[index], true
}

// Deprecated: use unified.ResultSet.Columns instead.
func (rs *Rows) Columns() []string {
	if rs.resultSet != nil {
		return rs.resultSet.Columns()
	}
	return append([]string(nil), rs.fieldsNames...)
}

// Deprecated: use unified.ResultSet.ColumnTypeDatabaseTypeName instead.
func (rs *Rows) ColumnTypeDatabaseTypeName(i int) string {
	if rs.resultSet != nil {
		return rs.resultSet.ColumnTypeDatabaseTypeName(i)
	}
	if i < 0 || i >= len(rs.fieldsTypes) {
		return ""
	}
	return common.GetTypeName(int(rs.fieldsTypes[i]))
}

// Deprecated: use unified.ResultSet.ColumnTypeLength instead.
func (rs *Rows) ColumnTypeLength(i int) (length int64, ok bool) {
	if rs.resultSet != nil {
		return rs.resultSet.ColumnTypeLength(i)
	}
	if i < 0 || i >= len(rs.fieldsLengths) {
		return 0, false
	}
	return rs.fieldsLengths[i], true
}

// Deprecated: use unified.ResultSet.ColumnTypeScanType instead.
func (rs *Rows) ColumnTypeScanType(i int) reflect.Type {
	if rs.resultSet != nil {
		return rs.resultSet.ColumnTypeScanType(i)
	}
	if i < 0 || i >= len(rs.fieldsTypes) {
		return common.UnknownType
	}
	t, exist := common.ColumnTypeMap[int(rs.fieldsTypes[i])]
	if !exist {
		return common.UnknownType
	}
	return t
}

// Deprecated: use unified.ResultSet.Close instead.
func (rs *Rows) Close() error {
	if rs.resultSet != nil {
		return rs.resultSet.Close()
	}
	return nil
}

// Deprecated: use unified.ResultSet.Next instead.
func (rs *Rows) Next(dest []driver.Value) error {
	if rs.resultSet != nil {
		return rs.resultSet.Next(dest)
	}
	return io.EOF
}
