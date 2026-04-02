package parser

import (
	"database/sql/driver"
	"fmt"
	"math"
	"strconv"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/tools"
)

const (
	Int8Size    = common.Int8Size
	Int16Size   = common.Int16Size
	Int32Size   = common.Int32Size
	Int64Size   = common.Int64Size
	UInt8Size   = common.UInt8Size
	UInt16Size  = common.UInt16Size
	UInt32Size  = common.UInt32Size
	UInt64Size  = common.UInt64Size
	Float32Size = common.Float32Size
	Float64Size = common.Float64Size
)

const (
	ColInfoSize            = Int8Size + Int32Size
	RawBlockVersionOffset  = 0
	RawBlockLengthOffset   = RawBlockVersionOffset + Int32Size
	NumOfRowsOffset        = RawBlockLengthOffset + Int32Size
	NumOfColsOffset        = NumOfRowsOffset + Int32Size
	HasColumnSegmentOffset = NumOfColsOffset + Int32Size
	GroupIDOffset          = HasColumnSegmentOffset + Int32Size
	ColInfoOffset          = GroupIDOffset + UInt64Size
)

func RawBlockGetVersion(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(tools.AddPointer(rawBlock, RawBlockVersionOffset)))
}

func RawBlockGetLength(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(tools.AddPointer(rawBlock, RawBlockLengthOffset)))
}

func RawBlockGetNumOfRows(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(tools.AddPointer(rawBlock, NumOfRowsOffset)))
}

func RawBlockGetNumOfCols(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(tools.AddPointer(rawBlock, NumOfColsOffset)))
}

func RawBlockGetHasColumnSegment(rawBlock unsafe.Pointer) int32 {
	return *((*int32)(tools.AddPointer(rawBlock, HasColumnSegmentOffset)))
}

func RawBlockGetGroupID(rawBlock unsafe.Pointer) uint64 {
	return *((*uint64)(tools.AddPointer(rawBlock, GroupIDOffset)))
}

type RawBlockColInfo struct {
	ColType int8
	Bytes   int32
}

func RawBlockGetColInfo(rawBlock unsafe.Pointer, infos []RawBlockColInfo) {
	for i := 0; i < len(infos); i++ {
		offset := ColInfoOffset + ColInfoSize*uintptr(i)
		infos[i].ColType = *((*int8)(tools.AddPointer(rawBlock, offset)))
		infos[i].Bytes = *((*int32)(tools.AddPointer(rawBlock, offset+Int8Size)))
	}
}

func RawBlockGetDecimalInfo(rawBlock unsafe.Pointer, colIndex int) (uint8, uint8, uint8) {
	offset := ColInfoOffset + ColInfoSize*uintptr(colIndex)
	scale := *((*uint8)(tools.AddPointer(rawBlock, offset+UInt8Size)))
	precision := *((*uint8)(tools.AddPointer(rawBlock, offset+2*UInt8Size)))
	bytes := *((*uint8)(tools.AddPointer(rawBlock, offset+4*UInt8Size)))
	return bytes, precision, scale
}

func RawBlockGetColumnLengthOffset(colCount int) uintptr {
	return ColInfoOffset + uintptr(colCount)*ColInfoSize
}

func RawBlockGetColDataOffset(colCount int) uintptr {
	return ColInfoOffset + uintptr(colCount)*ColInfoSize + uintptr(colCount)*Int32Size
}

type FormatTimeFunc func(ts int64, precision int) driver.Value

func IsVarDataType(colType uint8) bool {
	switch colType {
	case common.TSDB_DATA_TYPE_BINARY,
		common.TSDB_DATA_TYPE_NCHAR,
		common.TSDB_DATA_TYPE_JSON,
		common.TSDB_DATA_TYPE_VARBINARY,
		common.TSDB_DATA_TYPE_GEOMETRY,
		common.TSDB_DATA_TYPE_BLOB:
		return true
	}
	return false
}

func BitmapLen(n int) int {
	return ((n) + ((1 << 3) - 1)) >> 3
}

func BitPos(n int) int {
	return n & ((1 << 3) - 1)
}

func CharOffset(n int) int {
	return n >> 3
}

func BMIsNull(c byte, n int) bool {
	return c&(1<<(7-BitPos(n))) == (1 << (7 - BitPos(n)))
}

type rawConvertFunc func(pStart unsafe.Pointer, row int, arg ...interface{}) driver.Value

type rawConvertVarDataFunc func(pHeader, pStart unsafe.Pointer, row int) driver.Value

var rawConvertFuncSlice = [common.TSDB_DATA_TYPE_MAX]rawConvertFunc{}

var rawConvertVarDataSlice = [common.TSDB_DATA_TYPE_MAX]rawConvertVarDataFunc{}

func ItemIsNull(pHeader unsafe.Pointer, row int) bool {
	offset := CharOffset(row)
	c := *((*byte)(tools.AddPointer(pHeader, uintptr(offset))))
	return BMIsNull(c, row)
}

func rawConvertBool(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return (*((*byte)(tools.AddPointer(pStart, uintptr(row)*1)))) != 0
}

func rawConvertTinyint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*int8)(tools.AddPointer(pStart, uintptr(row)*Int8Size)))
}

func rawConvertSmallint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*int16)(tools.AddPointer(pStart, uintptr(row)*Int16Size)))
}

func rawConvertInt(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*int32)(tools.AddPointer(pStart, uintptr(row)*Int32Size)))
}

func rawConvertBigint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*int64)(tools.AddPointer(pStart, uintptr(row)*Int64Size)))
}

func rawConvertUTinyint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*uint8)(tools.AddPointer(pStart, uintptr(row)*UInt8Size)))
}

func rawConvertUSmallint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*uint16)(tools.AddPointer(pStart, uintptr(row)*UInt16Size)))
}

func rawConvertUInt(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*uint32)(tools.AddPointer(pStart, uintptr(row)*UInt32Size)))
}

func rawConvertUBigint(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return *((*uint64)(tools.AddPointer(pStart, uintptr(row)*UInt64Size)))
}

func rawConvertFloat(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return math.Float32frombits(*((*uint32)(tools.AddPointer(pStart, uintptr(row)*Float32Size))))
}

func rawConvertDouble(pStart unsafe.Pointer, row int, _ ...interface{}) driver.Value {
	return math.Float64frombits(*((*uint64)(tools.AddPointer(pStart, uintptr(row)*Float64Size))))
}

func rawConvertTime(pStart unsafe.Pointer, row int, arg ...interface{}) driver.Value {
	if len(arg) == 1 {
		return common.TimestampConvertToTime(*((*int64)(tools.AddPointer(pStart, uintptr(row)*Int64Size))), arg[0].(int))
	} else if len(arg) == 2 {
		return arg[1].(FormatTimeFunc)(*((*int64)(tools.AddPointer(pStart, uintptr(row)*Int64Size))), arg[0].(int))
	}
	panic("convertTime error")
}

func rawConvertDecimal64(pStart unsafe.Pointer, row int, arg ...interface{}) driver.Value {
	if len(arg) != 1 {
		panic("convertDecimal error")
	}
	scale := int(arg[0].(uint8))
	value := *((*int64)(tools.AddPointer(pStart, uintptr(row)*Int64Size)))
	str := strconv.FormatInt(value, 10)
	return tools.FormatDecimal(str, scale)
}

func rawConvertDecimal128(pStart unsafe.Pointer, row int, arg ...interface{}) driver.Value {
	if len(arg) != 1 {
		panic("convertDecimal error")
	}
	scale := int(arg[0].(uint8))
	lo := *((*uint64)(tools.AddPointer(pStart, uintptr(row)*Int64Size*2)))
	hi := *((*int64)(tools.AddPointer(pStart, uintptr(row)*Int64Size*2+UInt64Size)))
	str := tools.FormatI128(hi, lo)
	return tools.FormatDecimal(str, scale)
}

func rawConvertVarBinary(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	result := rawGetBytes(pHeader, pStart, row, false)
	if result == nil {
		return nil
	}
	return result
}

func rawGetBytes(pHeader, pStart unsafe.Pointer, row int, isUint32 bool) []byte {
	offset := *((*int32)(tools.AddPointer(pHeader, uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := tools.AddPointer(pStart, uintptr(offset))
	var clen int
	var step uintptr
	if isUint32 {
		clen = int(*((*uint32)(currentRow)))
		step = 4
	} else {
		clen = int(*((*uint16)(currentRow)))
		step = 2
	}
	if clen == 0 {
		return make([]byte, 0)
	}
	currentRow = tools.AddPointer(currentRow, step)
	result := make([]byte, clen)
	Copy(currentRow, result, 0, clen)
	return result
}

func rawConvertGeometry(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	return rawConvertVarBinary(pHeader, pStart, row)
}

func rawConvertBinary(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	result := rawGetBytes(pHeader, pStart, row, false)
	if result == nil {
		return nil
	}
	return *(*string)(unsafe.Pointer(&result))
}

func rawConvertBlob(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	result := rawGetBytes(pHeader, pStart, row, true)
	if result == nil {
		return nil
	}
	return result
}

func rawConvertNchar(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	offset := *((*int32)(tools.AddPointer(pHeader, uintptr(row*4))))
	if offset == -1 {
		return nil
	}
	currentRow := tools.AddPointer(pStart, uintptr(offset))
	clen := *((*uint16)(currentRow)) / 4
	if clen == 0 {
		return ""
	}
	currentRow = unsafe.Pointer(uintptr(currentRow) + 2)
	binaryVal := make([]rune, clen)

	for index := uint16(0); index < clen; index++ {
		binaryVal[index] = *((*rune)(unsafe.Pointer(uintptr(currentRow) + uintptr(index*4))))
	}
	return string(binaryVal)
}

func rawConvertJson(pHeader, pStart unsafe.Pointer, row int) driver.Value {
	return rawConvertVarBinary(pHeader, pStart, row)
}

func ReadBlockSimple(block unsafe.Pointer, precision int) ([][]driver.Value, error) {
	blockSize := RawBlockGetNumOfRows(block)
	colCount := RawBlockGetNumOfCols(block)
	colInfo := make([]RawBlockColInfo, colCount)
	RawBlockGetColInfo(block, colInfo)
	colTypes := make([]uint8, colCount)
	for i := int32(0); i < colCount; i++ {
		colTypes[i] = uint8(colInfo[i].ColType)
	}
	return ReadBlock(block, int(blockSize), colTypes, precision)
}

// ReadBlock in-place
func ReadBlock(block unsafe.Pointer, blockSize int, colTypes []uint8, precision int) ([][]driver.Value, error) {
	err := validColumnType(colTypes)
	if err != nil {
		return nil, err
	}
	r := make([][]driver.Value, blockSize)
	colCount := len(colTypes)
	nullBitMapOffset := uintptr(BitmapLen(blockSize))
	lengthOffset := RawBlockGetColumnLengthOffset(colCount)
	pHeader := tools.AddPointer(block, RawBlockGetColDataOffset(colCount))
	var pStart unsafe.Pointer
	for column := 0; column < colCount; column++ {
		colLength := *((*int32)(tools.AddPointer(block, lengthOffset+uintptr(column)*Int32Size)))
		if IsVarDataType(colTypes[column]) {
			convertF := rawConvertVarDataSlice[colTypes[column]]
			pStart = tools.AddPointer(pHeader, Int32Size*uintptr(blockSize))
			for row := 0; row < blockSize; row++ {
				if column == 0 {
					r[row] = make([]driver.Value, colCount)
				}
				r[row][column] = convertF(pHeader, pStart, row)
			}
		} else {
			convertF := rawConvertFuncSlice[colTypes[column]]
			pStart = tools.AddPointer(pHeader, nullBitMapOffset)
			var args []interface{}
			switch colTypes[column] {
			case common.TSDB_DATA_TYPE_TIMESTAMP:
				args = []interface{}{precision}
			case common.TSDB_DATA_TYPE_DECIMAL, common.TSDB_DATA_TYPE_DECIMAL64:
				_, _, scale := RawBlockGetDecimalInfo(block, column)
				args = []interface{}{scale}
			}
			for row := 0; row < blockSize; row++ {
				if column == 0 {
					r[row] = make([]driver.Value, colCount)
				}
				if ItemIsNull(pHeader, row) {
					r[row][column] = nil
				} else {
					r[row][column] = convertF(pStart, row, args...)
				}
			}
		}
		pHeader = tools.AddPointer(pStart, uintptr(colLength))
	}
	return r, nil
}

func validColumnType(colTypes []uint8) error {
	for _, colType := range colTypes {
		if colType >= common.TSDB_DATA_TYPE_MAX {
			return fmt.Errorf("invalid column type %d", colType)
		}
		if rawConvertFuncSlice[colType] == nil && rawConvertVarDataSlice[colType] == nil {
			return fmt.Errorf("unsupported column type %d", colType)
		}
	}
	return nil
}

func init() {
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_BOOL)] = rawConvertBool
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_TINYINT)] = rawConvertTinyint
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_SMALLINT)] = rawConvertSmallint
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_INT)] = rawConvertInt
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_BIGINT)] = rawConvertBigint
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_UTINYINT)] = rawConvertUTinyint
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_USMALLINT)] = rawConvertUSmallint
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_UINT)] = rawConvertUInt
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_UBIGINT)] = rawConvertUBigint
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_FLOAT)] = rawConvertFloat
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_DOUBLE)] = rawConvertDouble
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_TIMESTAMP)] = rawConvertTime
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_DECIMAL64)] = rawConvertDecimal64
	rawConvertFuncSlice[uint8(common.TSDB_DATA_TYPE_DECIMAL)] = rawConvertDecimal128

	rawConvertVarDataSlice[uint8(common.TSDB_DATA_TYPE_BINARY)] = rawConvertBinary
	rawConvertVarDataSlice[uint8(common.TSDB_DATA_TYPE_NCHAR)] = rawConvertNchar
	rawConvertVarDataSlice[uint8(common.TSDB_DATA_TYPE_JSON)] = rawConvertJson
	rawConvertVarDataSlice[uint8(common.TSDB_DATA_TYPE_VARBINARY)] = rawConvertVarBinary
	rawConvertVarDataSlice[uint8(common.TSDB_DATA_TYPE_GEOMETRY)] = rawConvertGeometry
	rawConvertVarDataSlice[uint8(common.TSDB_DATA_TYPE_BLOB)] = rawConvertBlob
}
