package taosRestful

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/taosdata/driver-go/v3/common"
)

type rows struct {
	result   *common.TDEngineRestfulResp
	rowIndex int
}

func (rs *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if rs.result.ColTypes[index] == common.TSDB_DATA_TYPE_DECIMAL || rs.result.ColTypes[index] == common.TSDB_DATA_TYPE_DECIMAL64 {
		return rs.result.Precisions[index], rs.result.Scales[index], true
	}
	return 0, 0, false
}

func (rs *rows) Columns() []string {
	return rs.result.ColNames
}

func (rs *rows) ColumnTypeDatabaseTypeName(i int) string {
	return common.GetTypeName(rs.result.ColTypes[i])
}

func (rs *rows) ColumnTypeLength(i int) (length int64, ok bool) {
	return rs.result.ColLength[i], ok
}

func (rs *rows) ColumnTypeScanType(i int) reflect.Type {
	t, exist := common.ColumnTypeMap[rs.result.ColTypes[i]]
	if !exist {
		return common.UnknownType
	}
	return t
}

func (rs *rows) Close() error {
	return nil
}

func (rs *rows) Next(dest []driver.Value) error {
	if rs.rowIndex >= len(rs.result.Data) {
		return io.EOF
	}
	copy(dest, rs.result.Data[rs.rowIndex])
	rs.rowIndex += 1
	return nil
}
