package common

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const DefaultHttpPort = 6041

type TDEngineRestfulResp struct {
	Code       int
	Rows       int
	Desc       string
	ColNames   []string
	ColTypes   []int
	ColLength  []int64
	Precisions []int64
	Scales     []int64
	Data       [][]driver.Value
}

var jsonI = jsoniter.ConfigCompatibleWithStandardLibrary

func UnmarshalRestfulBody(body io.Reader, bufferSize int) (*TDEngineRestfulResp, error) {
	return UnmarshalRestfulBodyWithTimezone(body, bufferSize, nil)
}

func UnmarshalRestfulBodyWithTimezone(body io.Reader, bufferSize int, loc *time.Location) (*TDEngineRestfulResp, error) {
	var result TDEngineRestfulResp
	iter := jsonI.BorrowIterator(make([]byte, bufferSize))
	defer jsonI.ReturnIterator(iter)
	iter.Reset(body)
	timeFormat := time.RFC3339Nano
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, s string) bool {
		switch s {
		case "code":
			result.Code = iter.ReadInt()
		case "desc":
			result.Desc = iter.ReadString()
		case "column_meta":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				index := 0
				isDecimal := false
				iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
					switch index {
					case 0:
						result.ColNames = append(result.ColNames, iter.ReadString())
						index = 1
					case 1:
						typeStr := iter.ReadString()
						if strings.HasPrefix(typeStr, "DECIMAL(") {
							// parse DECIMAL(10,2) to DECIMAL, 10, 2
							precision, scale, err := parseDecimalType(typeStr)
							if err != nil {
								iter.ReportError("parse decimal", err.Error())
								return false
							}
							isDecimal = true
							result.Precisions = append(result.Precisions, precision)
							result.Scales = append(result.Scales, scale)
						} else {
							t, exist := NameTypeMap[typeStr]
							if exist {
								result.ColTypes = append(result.ColTypes, t)
							} else {
								iter.ReportError("unsupported type in column_meta", typeStr)
								return false
							}
							result.Precisions = append(result.Precisions, 0)
							result.Scales = append(result.Scales, 0)
						}
						index = 2
					case 2:
						colLen := iter.ReadInt64()
						result.ColLength = append(result.ColLength, colLen)
						index = 0
						if isDecimal {
							switch colLen {
							case 8:
								result.ColTypes = append(result.ColTypes, TSDB_DATA_TYPE_DECIMAL64)
							case 16:
								result.ColTypes = append(result.ColTypes, TSDB_DATA_TYPE_DECIMAL)
							default:
								iter.ReportError("parse decimal", fmt.Sprintf("invalid length %d", colLen))
								return false
							}
						}
						isDecimal = false
					}
					return true
				})
				return true
			})
		case "data":
			columnCount := len(result.ColTypes)
			column := 0
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				column = 0
				var row = make([]driver.Value, columnCount)
				iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
					defer func() {
						column += 1
					}()
					columnType := result.ColTypes[column]
					if columnType == TSDB_DATA_TYPE_JSON {
						row[column] = iter.SkipAndReturnBytes()
						return true
					}
					if iter.ReadNil() {
						row[column] = nil
						return true
					}
					var err error
					switch columnType {
					case TSDB_DATA_TYPE_NULL:
						iter.Skip()
						row[column] = nil
					case TSDB_DATA_TYPE_BOOL:
						row[column] = iter.ReadAny().ToBool()
					case TSDB_DATA_TYPE_TINYINT:
						row[column] = iter.ReadInt8()
					case TSDB_DATA_TYPE_SMALLINT:
						row[column] = iter.ReadInt16()
					case TSDB_DATA_TYPE_INT:
						row[column] = iter.ReadInt32()
					case TSDB_DATA_TYPE_BIGINT:
						row[column] = iter.ReadInt64()
					case TSDB_DATA_TYPE_FLOAT:
						row[column] = iter.ReadFloat32()
					case TSDB_DATA_TYPE_DOUBLE:
						row[column] = iter.ReadFloat64()
					case TSDB_DATA_TYPE_BINARY:
						row[column] = iter.ReadString()
					case TSDB_DATA_TYPE_TIMESTAMP:
						b := iter.ReadString()
						var tm time.Time
						tm, err = time.Parse(timeFormat, b)
						if err != nil {
							iter.ReportError("parse time", err.Error())
							return false
						}
						if loc != nil {
							tm = tm.In(loc)
						}
						row[column] = tm
					case TSDB_DATA_TYPE_NCHAR:
						row[column] = iter.ReadString()
					case TSDB_DATA_TYPE_UTINYINT:
						row[column] = iter.ReadUint8()
					case TSDB_DATA_TYPE_USMALLINT:
						row[column] = iter.ReadUint16()
					case TSDB_DATA_TYPE_UINT:
						row[column] = iter.ReadUint32()
					case TSDB_DATA_TYPE_UBIGINT:
						row[column] = iter.ReadUint64()
					case TSDB_DATA_TYPE_VARBINARY, TSDB_DATA_TYPE_GEOMETRY, TSDB_DATA_TYPE_BLOB:
						data := iter.ReadStringAsSlice()
						if len(data)%2 != 0 {
							iter.ReportError("read varbinary", fmt.Sprintf("invalid length %s", string(data)))
							return false
						}
						value := make([]byte, len(data)/2)
						for i := 0; i < len(data); i += 2 {
							value[i/2] = hexCharToDigit(data[i])<<4 | hexCharToDigit(data[i+1])
						}
						row[column] = value
					case TSDB_DATA_TYPE_DECIMAL, TSDB_DATA_TYPE_DECIMAL64:
						row[column] = iter.ReadString()
					default:
						row[column] = nil
						iter.Skip()
					}
					return iter.Error == nil
				})
				if iter.Error != nil {
					return false
				}
				result.Data = append(result.Data, row)
				return true
			})
		case "rows":
			result.Rows = iter.ReadInt()
		default:
			iter.Skip()
		}
		return iter.Error == nil
	})
	if iter.Error != nil && iter.Error != io.EOF {
		return nil, iter.Error
	}
	return &result, nil
}

func hexCharToDigit(char byte) uint8 {
	switch {
	case char >= '0' && char <= '9':
		return char - '0'
	case char >= 'a' && char <= 'f':
		return char - 'a' + 10
	default:
		panic("assertion failed: invalid hex char")
	}
}

func parseDecimalType(typeStr string) (int64, int64, error) {
	// parse DECIMAL(10,2) to 10, 2
	if len(typeStr) < 12 || typeStr[len(typeStr)-1] != ')' {
		return 0, 0, fmt.Errorf("invalid decimal type %s", typeStr)
	}
	for i := len(typeStr) - 2; i > 8; i-- {
		if typeStr[i] == ',' {
			precision, err := strconv.ParseInt(typeStr[8:i], 10, 8)
			if err != nil {
				return 0, 0, err
			}
			scale, err := strconv.ParseInt(typeStr[i+1:len(typeStr)-1], 10, 8)
			if err != nil {
				return 0, 0, err
			}
			return precision, scale, nil
		}
	}
	return 0, 0, fmt.Errorf("invalid decimal type %s", typeStr)
}
