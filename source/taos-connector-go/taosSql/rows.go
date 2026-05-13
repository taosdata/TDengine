package taosSql

import (
	"database/sql/driver"
	"io"
	"reflect"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/handler"
)

type rows struct {
	handler     *handler.Handler
	rowsHeader  *wrapper.RowsHeader
	timezone    *time.Location
	done        bool
	block       unsafe.Pointer
	blockOffset int
	blockSize   int
	result      unsafe.Pointer
	precision   int
	isStmt      bool
}

func newRows(handler *handler.Handler, rowsHeader *wrapper.RowsHeader, result unsafe.Pointer, precision int, isStmt bool, timezone *time.Location) *rows {
	return &rows{
		handler:    handler,
		rowsHeader: rowsHeader,
		result:     result,
		precision:  precision,
		isStmt:     isStmt,
		timezone:   timezone,
	}
}
func (rs *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if rs.rowsHeader.ColTypes[index] == common.TSDB_DATA_TYPE_DECIMAL || rs.rowsHeader.ColTypes[index] == common.TSDB_DATA_TYPE_DECIMAL64 {
		return rs.rowsHeader.Precisions[index], rs.rowsHeader.Scales[index], true
	}
	return 0, 0, false
}

func (rs *rows) Columns() []string {
	return rs.rowsHeader.ColNames
}

func (rs *rows) ColumnTypeDatabaseTypeName(i int) string {
	return rs.rowsHeader.TypeDatabaseName(i)
}

func (rs *rows) ColumnTypeLength(i int) (length int64, ok bool) {
	return int64(rs.rowsHeader.ColLength[i]), true
}

func (rs *rows) ColumnTypeScanType(i int) reflect.Type {
	return rs.rowsHeader.ScanType(i)
}

func (rs *rows) Close() error {
	if rs.handler != nil {
		asyncHandlerPool.Put(rs.handler)
		rs.handler = nil
	}
	if !rs.isStmt && rs.result != nil {
		locker.Lock()
		wrapper.TaosFreeResult(rs.result)
		locker.Unlock()
	}
	rs.result = nil
	rs.block = nil
	return nil
}

func (rs *rows) Next(dest []driver.Value) error {
	if rs.done {
		return io.EOF
	}

	if rs.result == nil {
		return &errors.TaosError{Code: 0xffff, ErrStr: "result is nil!"}
	}

	if rs.block == nil {
		err := rs.taosFetchBlock()
		if err != nil {
			return err
		}
	}
	if rs.blockSize == 0 {
		rs.block = nil
		return io.EOF
	}

	if rs.blockOffset >= rs.blockSize {
		err := rs.taosFetchBlock()
		if err != nil {
			return err
		}
	}
	if rs.blockSize == 0 {
		rs.block = nil
		return io.EOF
	}
	var err error
	if rs.timezone == nil {
		err = parser.ReadRow(dest, rs.block, rs.blockSize, rs.blockOffset, rs.rowsHeader.ColTypes, rs.precision, rs.rowsHeader.Scales)
	} else {
		err = parser.ReadRowWithTimeFormat(dest, rs.block, rs.blockSize, rs.blockOffset, rs.rowsHeader.ColTypes, rs.precision, rs.rowsHeader.Scales, rs.FormatTime)
	}
	if err != nil {
		return err
	}
	rs.blockOffset++
	return nil
}

func (rs *rows) FormatTime(ts int64, precision int) driver.Value {
	return common.TimestampConvertToTimeWithLocation(ts, precision, rs.timezone)
}

func (rs *rows) taosFetchBlock() error {
	result := rs.asyncFetchRows()
	if result.N == 0 {
		rs.blockSize = 0
		return nil
	}
	if result.N < 0 {
		code := wrapper.TaosError(result.Res)
		errStr := wrapper.TaosErrorStr(result.Res)
		return errors.NewError(code, errStr)
	}
	rs.blockSize = result.N
	rs.block = wrapper.TaosGetRawBlock(result.Res)
	rs.blockOffset = 0
	return nil
}

func (rs *rows) asyncFetchRows() *handler.AsyncResult {
	locker.Lock()
	wrapper.TaosFetchRawBlockA(rs.result, rs.handler.Handler)
	locker.Unlock()
	r := <-rs.handler.Caller.FetchResult
	return r
}
