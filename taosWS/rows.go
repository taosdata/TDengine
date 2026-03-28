package taosWS

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"

	"github.com/taosdata/driver-go/v3/ws/unified"
)

type rows struct {
	unifiedResult *unified.ResultSet
}

func newRowsFromUnified(unifiedResult *unified.ResultSet) *rows {
	return &rows{
		unifiedResult: unifiedResult,
	}
}

func (rs *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return rs.unifiedResult.ColumnTypePrecisionScale(index)
}

func (rs *rows) Columns() []string {
	return rs.unifiedResult.Columns()
}

func (rs *rows) ColumnTypeDatabaseTypeName(index int) string {
	return rs.unifiedResult.ColumnTypeDatabaseTypeName(index)
}

func (rs *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	return rs.unifiedResult.ColumnTypeLength(index)
}

func (rs *rows) ColumnTypeScanType(index int) reflect.Type {
	return rs.unifiedResult.ColumnTypeScanType(index)
}

func (rs *rows) Close() error {
	if rs == nil || rs.unifiedResult == nil {
		return nil
	}
	return mapUnifiedConnError(rs.unifiedResult.Close())
}

func (rs *rows) Next(dest []driver.Value) error {
	if rs == nil || rs.unifiedResult == nil {
		return driver.ErrBadConn
	}
	err := rs.unifiedResult.Next(dest)
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	return mapUnifiedConnError(err)
}
