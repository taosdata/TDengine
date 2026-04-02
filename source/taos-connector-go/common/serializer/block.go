package serializer

import (
	"bytes"
	"errors"
	"math"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	taosTypes "github.com/taosdata/driver-go/v3/types"
)

const (
	Int16Size   = int(common.Int16Size)
	Int32Size   = int(common.Int32Size)
	Int64Size   = int(common.Int64Size)
	UInt16Size  = int(common.UInt16Size)
	UInt32Size  = int(common.UInt32Size)
	UInt64Size  = int(common.UInt64Size)
	Float32Size = int(common.Float32Size)
	Float64Size = int(common.Float64Size)
)

func BitmapLen(n int) int {
	return ((n) + ((1 << 3) - 1)) >> 3
}

func BitPos(n int) int {
	return n & ((1 << 3) - 1)
}

func CharOffset(n int) int {
	return n >> 3
}

func BMSetNull(c byte, n int) byte {
	return c + (1 << (7 - BitPos(n)))
}

//revive:disable-next-line
var ColumnNumberNotMatch = errors.New("number of columns does not match")

//revive:disable-next-line
var DataTypeWrong = errors.New("wrong data type")

func SerializeRawBlock(params []*param.Param, colType *param.ColumnType) ([]byte, error) {
	columns := len(params)
	rows := len(params[0].GetValues())
	colTypes, err := colType.GetValue()
	if err != nil {
		return nil, err
	}
	if len(colTypes) != columns {
		return nil, ColumnNumberNotMatch
	}
	var block []byte
	//version int32
	block = appendUint32(block, uint32(1))
	//length int32
	block = appendUint32(block, uint32(0))
	//rows int32
	block = appendUint32(block, uint32(rows))
	//columns int32
	block = appendUint32(block, uint32(columns))
	//flagSegment int32
	block = appendUint32(block, uint32(0))
	//groupID uint64
	block = appendUint64(block, uint64(0))
	colInfoData := make([]byte, 0, 5*columns)
	lengthData := make([]byte, 0, 4*columns)
	bitMapLen := BitmapLen(rows)
	var data []byte
	//colInfo(type+bytes) (int8+int32) * columns
	buffer := bytes.NewBuffer(block)
	for colIndex := 0; colIndex < columns; colIndex++ {
		switch colTypes[colIndex].Type {
		case taosTypes.TaosBoolType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_BOOL)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_BOOL]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosBool)
					if !is {
						return nil, DataTypeWrong
					}
					if v {
						dataTmp[rowIndex+bitMapLen] = 1
					}
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosTinyintType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_TINYINT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_TINYINT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosTinyint)
					if !is {
						return nil, DataTypeWrong
					}
					dataTmp[rowIndex+bitMapLen] = byte(v)
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosSmallintType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_SMALLINT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_SMALLINT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*Int16Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosSmallint)
					if !is {
						return nil, DataTypeWrong
					}
					offset := rowIndex*Int16Size + bitMapLen
					dataTmp[offset] = byte(v)
					dataTmp[offset+1] = byte(v >> 8)
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosIntType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_INT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_INT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*Int32Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosInt)
					if !is {
						return nil, DataTypeWrong
					}
					offset := rowIndex*Int32Size + bitMapLen
					dataTmp[offset] = byte(v)
					dataTmp[offset+1] = byte(v >> 8)
					dataTmp[offset+2] = byte(v >> 16)
					dataTmp[offset+3] = byte(v >> 24)
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosBigintType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_BIGINT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_BIGINT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*Int64Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosBigint)
					if !is {
						return nil, DataTypeWrong
					}
					offset := rowIndex*Int64Size + bitMapLen
					dataTmp[offset] = byte(v)
					dataTmp[offset+1] = byte(v >> 8)
					dataTmp[offset+2] = byte(v >> 16)
					dataTmp[offset+3] = byte(v >> 24)
					dataTmp[offset+4] = byte(v >> 32)
					dataTmp[offset+5] = byte(v >> 40)
					dataTmp[offset+6] = byte(v >> 48)
					dataTmp[offset+7] = byte(v >> 56)
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosUTinyintType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_UTINYINT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_UTINYINT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosUTinyint)
					if !is {
						return nil, DataTypeWrong
					}
					dataTmp[rowIndex+bitMapLen] = uint8(v)
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosUSmallintType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_USMALLINT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_USMALLINT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*UInt16Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosUSmallint)
					if !is {
						return nil, DataTypeWrong
					}
					offset := rowIndex*UInt16Size + bitMapLen
					dataTmp[offset] = byte(v)
					dataTmp[offset+1] = byte(v >> 8)
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosUIntType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_UINT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_UINT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*UInt32Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosUInt)
					if !is {
						return nil, DataTypeWrong
					}
					offset := rowIndex*UInt32Size + bitMapLen
					dataTmp[offset] = byte(v)
					dataTmp[offset+1] = byte(v >> 8)
					dataTmp[offset+2] = byte(v >> 16)
					dataTmp[offset+3] = byte(v >> 24)
				}
			}
			data = append(data, dataTmp...)

		case taosTypes.TaosUBigintType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_UBIGINT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_UBIGINT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*UInt64Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosUBigint)
					if !is {
						return nil, DataTypeWrong
					}
					offset := rowIndex*UInt64Size + bitMapLen
					dataTmp[offset] = byte(v)
					dataTmp[offset+1] = byte(v >> 8)
					dataTmp[offset+2] = byte(v >> 16)
					dataTmp[offset+3] = byte(v >> 24)
					dataTmp[offset+4] = byte(v >> 32)
					dataTmp[offset+5] = byte(v >> 40)
					dataTmp[offset+6] = byte(v >> 48)
					dataTmp[offset+7] = byte(v >> 56)
				}
			}
			data = append(data, dataTmp...)

		case taosTypes.TaosFloatType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_FLOAT)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_FLOAT]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*Float32Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosFloat)
					if !is {
						return nil, DataTypeWrong
					}
					offset := rowIndex*Float32Size + bitMapLen
					vv := math.Float32bits(float32(v))
					dataTmp[offset] = byte(vv)
					dataTmp[offset+1] = byte(vv >> 8)
					dataTmp[offset+2] = byte(vv >> 16)
					dataTmp[offset+3] = byte(vv >> 24)
				}
			}
			data = append(data, dataTmp...)

		case taosTypes.TaosDoubleType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_DOUBLE)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_DOUBLE]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*Float64Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosDouble)
					if !is {
						return nil, DataTypeWrong
					}
					offset := rowIndex*Float64Size + bitMapLen
					vv := math.Float64bits(float64(v))
					dataTmp[offset] = byte(vv)
					dataTmp[offset+1] = byte(vv >> 8)
					dataTmp[offset+2] = byte(vv >> 16)
					dataTmp[offset+3] = byte(vv >> 24)
					dataTmp[offset+4] = byte(vv >> 32)
					dataTmp[offset+5] = byte(vv >> 40)
					dataTmp[offset+6] = byte(vv >> 48)
					dataTmp[offset+7] = byte(vv >> 56)
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosBinaryType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_BINARY)
			colInfoData = appendUint32(colInfoData, uint32(0))
			length := 0
			dataTmp := make([]byte, Int32Size*rows)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				offset := Int32Size * rowIndex
				if rowData[rowIndex] == nil {
					for i := 0; i < Int32Size; i++ {
						// -1
						dataTmp[offset+i] = byte(255)
					}
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosBinary)
					if !is {
						return nil, DataTypeWrong
					}
					for i := 0; i < Int32Size; i++ {
						dataTmp[offset+i] = byte(length >> (8 * i))
					}
					dataTmp = appendUint16(dataTmp, uint16(len(v)))
					dataTmp = append(dataTmp, v...)
					length += len(v) + Int16Size
				}
			}
			lengthData = appendUint32(lengthData, uint32(length))
			data = append(data, dataTmp...)
		case taosTypes.TaosVarBinaryType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_VARBINARY)
			colInfoData = appendUint32(colInfoData, uint32(0))
			length := 0
			dataTmp := make([]byte, Int32Size*rows)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				offset := Int32Size * rowIndex
				if rowData[rowIndex] == nil {
					for i := 0; i < Int32Size; i++ {
						// -1
						dataTmp[offset+i] = byte(255)
					}
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosVarBinary)
					if !is {
						return nil, DataTypeWrong
					}
					for i := 0; i < Int32Size; i++ {
						dataTmp[offset+i] = byte(length >> (8 * i))
					}
					dataTmp = appendUint16(dataTmp, uint16(len(v)))
					dataTmp = append(dataTmp, v...)
					length += len(v) + Int16Size
				}
			}
			lengthData = appendUint32(lengthData, uint32(length))
			data = append(data, dataTmp...)
		case taosTypes.TaosGeometryType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_GEOMETRY)
			colInfoData = appendUint32(colInfoData, uint32(0))
			length := 0
			dataTmp := make([]byte, Int32Size*rows)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				offset := Int32Size * rowIndex
				if rowData[rowIndex] == nil {
					for i := 0; i < Int32Size; i++ {
						// -1
						dataTmp[offset+i] = byte(255)
					}
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosGeometry)
					if !is {
						return nil, DataTypeWrong
					}
					for i := 0; i < Int32Size; i++ {
						dataTmp[offset+i] = byte(length >> (8 * i))
					}
					dataTmp = appendUint16(dataTmp, uint16(len(v)))
					dataTmp = append(dataTmp, v...)
					length += len(v) + Int16Size
				}
			}
			lengthData = appendUint32(lengthData, uint32(length))
			data = append(data, dataTmp...)
		case taosTypes.TaosNcharType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_NCHAR)
			colInfoData = appendUint32(colInfoData, uint32(0))
			length := 0
			dataTmp := make([]byte, Int32Size*rows)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				offset := Int32Size * rowIndex
				if rowData[rowIndex] == nil {
					for i := 0; i < Int32Size; i++ {
						// -1
						dataTmp[offset+i] = byte(255)
					}
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosNchar)
					if !is {
						return nil, DataTypeWrong
					}
					for i := 0; i < Int32Size; i++ {
						dataTmp[offset+i] = byte(length >> (8 * i))
					}
					rs := []rune(v)
					dataTmp = appendUint16(dataTmp, uint16(len(rs)*4))
					for _, r := range rs {
						dataTmp = appendUint32(dataTmp, uint32(r))
					}
					length += len(rs)*4 + Int16Size
				}
			}
			lengthData = appendUint32(lengthData, uint32(length))
			data = append(data, dataTmp...)
		case taosTypes.TaosTimestampType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_TIMESTAMP)
			length := common.TypeLengthMap[common.TSDB_DATA_TYPE_TIMESTAMP]
			colInfoData = appendUint32(colInfoData, uint32(length))
			lengthData = appendUint32(lengthData, uint32(length*rows))
			dataTmp := make([]byte, bitMapLen+rows*Int64Size)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				if rowData[rowIndex] == nil {
					charOffset := CharOffset(rowIndex)
					dataTmp[charOffset] = BMSetNull(dataTmp[charOffset], rowIndex)
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosTimestamp)
					if !is {
						return nil, DataTypeWrong
					}
					vv := common.TimeToTimestamp(v.T, v.Precision)
					offset := rowIndex*Int64Size + bitMapLen
					dataTmp[offset] = byte(vv)
					dataTmp[offset+1] = byte(vv >> 8)
					dataTmp[offset+2] = byte(vv >> 16)
					dataTmp[offset+3] = byte(vv >> 24)
					dataTmp[offset+4] = byte(vv >> 32)
					dataTmp[offset+5] = byte(vv >> 40)
					dataTmp[offset+6] = byte(vv >> 48)
					dataTmp[offset+7] = byte(vv >> 56)
				}
			}
			data = append(data, dataTmp...)
		case taosTypes.TaosJsonType:
			colInfoData = append(colInfoData, common.TSDB_DATA_TYPE_JSON)
			colInfoData = appendUint32(colInfoData, uint32(0))
			length := 0
			dataTmp := make([]byte, Int32Size*rows)
			rowData := params[colIndex].GetValues()
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				offset := Int32Size * rowIndex
				if rowData[rowIndex] == nil {
					for i := 0; i < Int32Size; i++ {
						// -1
						dataTmp[offset+i] = byte(255)
					}
				} else {
					v, is := rowData[rowIndex].(taosTypes.TaosJson)
					if !is {
						return nil, DataTypeWrong
					}
					for i := 0; i < Int32Size; i++ {
						dataTmp[offset+i] = byte(length >> (8 * i))
					}
					dataTmp = appendUint16(dataTmp, uint16(len(v)))
					dataTmp = append(dataTmp, v...)
					length += len(v) + Int16Size
				}
			}
			lengthData = appendUint32(lengthData, uint32(length))
			data = append(data, dataTmp...)
		}
	}
	buffer.Write(colInfoData)
	buffer.Write(lengthData)
	buffer.Write(data)
	block = buffer.Bytes()
	for i := 0; i < Int32Size; i++ {
		block[4+i] = byte(len(block) >> (8 * i))
	}
	return block, nil
}

func appendUint16(b []byte, v uint16) []byte {
	return append(b,
		byte(v),
		byte(v>>8),
	)
}

func appendUint32(b []byte, v uint32) []byte {
	return append(b,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
	)
}

func appendUint64(b []byte, v uint64) []byte {
	return append(b,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
		byte(v>>32),
		byte(v>>40),
		byte(v>>48),
		byte(v>>56),
	)
}
