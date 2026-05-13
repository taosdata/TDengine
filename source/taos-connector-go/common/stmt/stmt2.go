package stmt

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

const (
	TotalLengthPosition      = 0
	CountPosition            = TotalLengthPosition + 4
	TagCountPosition         = CountPosition + 4
	ColCountPosition         = TagCountPosition + 4
	TableNamesOffsetPosition = ColCountPosition + 4
	TagsOffsetPosition       = TableNamesOffsetPosition + 4
	ColsOffsetPosition       = TagsOffsetPosition + 4
	DataPosition             = ColsOffsetPosition + 4
)

const (
	BindDataTotalLengthOffset = 0
	BindDataTypeOffset        = BindDataTotalLengthOffset + 4
	BindDataNumOffset         = BindDataTypeOffset + 4
	BindDataIsNullOffset      = BindDataNumOffset + 4
)

const FixedHeaderLen = uint32(28)

var isVarDataType = [common.TSDB_DATA_TYPE_MAX]bool{
	common.TSDB_DATA_TYPE_BINARY:    true,
	common.TSDB_DATA_TYPE_NCHAR:     true,
	common.TSDB_DATA_TYPE_JSON:      true,
	common.TSDB_DATA_TYPE_VARBINARY: true,
	common.TSDB_DATA_TYPE_DECIMAL:   true,
	common.TSDB_DATA_TYPE_DECIMAL64: true,
	common.TSDB_DATA_TYPE_GEOMETRY:  true,
	common.TSDB_DATA_TYPE_BLOB:      true,
}

func IsVarDataType(colType int8) bool {
	if colType < 0 || colType >= common.TSDB_DATA_TYPE_MAX {
		return false
	}
	return isVarDataType[colType]
}

func MarshalStmt2Binary(bindData []*TaosStmt2BindData, isInsert bool, fields []*Stmt2AllField) ([]byte, error) {
	return marshalStmt2BinaryWithHeader(bindData, isInsert, fields, 0)
}

func marshalStmt2BinaryWithHeader(bindData []*TaosStmt2BindData, isInsert bool, fields []*Stmt2AllField, binaryHeaderLength int) ([]byte, error) {
	var colFields []*Stmt2AllField
	var tagFields []*Stmt2AllField
	needTableName := false
	for i := 0; i < len(fields); i++ {
		switch fields[i].BindType {
		case TAOS_FIELD_COL:
			colFields = append(colFields, fields[i])
		case TAOS_FIELD_TAG:
			tagFields = append(tagFields, fields[i])
		}
	}
	tableCount := len(bindData)
	if tableCount == 0 {
		return nil, fmt.Errorf("empty data")
	}
	colCount := len(colFields)
	tagCount := len(tagFields)
	needTags := tagCount > 0
	needCols := colCount > 0
	var queryParam [][]driver.Value
	if isInsert {
		for i := 0; i < tableCount; i++ {
			data := bindData[i]
			if data.TableName != "" {
				needTableName = true
			}
			if len(data.Tags) != tagCount {
				return nil, fmt.Errorf("tag count not match, data count:%d, type count:%d", len(data.Tags), tagCount)
			}
			if len(data.Cols) != colCount {
				return nil, fmt.Errorf("col count not match, data count:%d, type count:%d", len(data.Cols), colCount)
			}
		}
	} else {
		needCols = true
		if tagCount != 0 {
			return nil, fmt.Errorf("query not need tag types")
		}
		if colCount != 0 {
			return nil, fmt.Errorf("query not need col types")
		}
		if tableCount != 1 {
			return nil, fmt.Errorf("query only need one data")
		}

		data := bindData[0]
		if data.TableName != "" {
			return nil, fmt.Errorf("query not need table name")
		}
		if len(data.Tags) != 0 {
			return nil, fmt.Errorf("query not need tag")
		}
		if len(data.Cols) == 0 {
			return nil, fmt.Errorf("query need col")
		}
		colCount = len(data.Cols)
		colFields = make([]*Stmt2AllField, colCount)
		queryParam = make([][]driver.Value, colCount)
		for j := 0; j < colCount; j++ {
			if len(data.Cols[j]) != 1 {
				return nil, fmt.Errorf("query col data must be one row, col:%d, count:%d", j, len(data.Cols[j]))
			}
			queryParam[j] = []driver.Value{data.Cols[j][0]}
			colFields[j] = &Stmt2AllField{}
			switch v := data.Cols[j][0].(type) {
			case string:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_BINARY
			case []byte:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_BINARY
			case int8:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_TINYINT
			case int16:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_SMALLINT
			case int32:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_INT
			case int64:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_BIGINT
			case uint8:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_UTINYINT
			case uint16:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_USMALLINT
			case uint32:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_UINT
			case uint64:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_UBIGINT
			case float32:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_FLOAT
			case float64:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_DOUBLE
			case bool:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_BOOL
			case time.Time:
				colFields[j].FieldType = common.TSDB_DATA_TYPE_BINARY
				queryParam[j] = []driver.Value{v.Format(time.RFC3339Nano)}
			default:
				return nil, fmt.Errorf("unsupported query param type %T", v)
			}
		}
	}
	if !needTableName && !needTags && !needCols {
		return nil, fmt.Errorf("no data")
	}
	var tableNameLengthLen = uint32(0)
	var tableNameBufferLen uint32

	var tagsDataLengthLen = uint32(0)
	var tagsBufferLen = uint32(0)
	var colsDataLengthLen = uint32(0)
	var colsBufferLen = uint32(0)
	var tableColLengthList []uint32

	var utf8TableNameLen []uint16
	var tableTagLengthList []uint32
	if needTableName {
		utf8TableNameLen = make([]uint16, tableCount)
	}
	if needTags {
		tableTagLengthList = make([]uint32, tableCount)
	}
	if needCols {
		tableColLengthList = make([]uint32, tableCount)
	}
	for tmpTableIndex := 0; tmpTableIndex < len(bindData); tmpTableIndex++ {
		// calculate table name
		if needTableName {
			var tableName = bindData[tmpTableIndex].TableName
			if len(tableName) > math.MaxUint16-1 {
				return nil, fmt.Errorf("table name too long, index:%d, length:%d", tmpTableIndex, len(tableName))
			}
			utf8TableNameLen[tmpTableIndex] = uint16(len(tableName) + 1)
			tableNameBufferLen += uint32(len(tableName) + 1)
		}
		// calculate tags
		if needTags {
			var tableTagLength = uint32(0)
			for i := 0; i < tagCount; i++ {
				tagVal := bindData[tmpTableIndex].Tags[i]
				if IsVarDataType(tagFields[i].FieldType) {
					bsCount := uint32(0)
					if tagVal != nil {
						switch tagVal := tagVal.(type) {
						case []byte:
							bsCount = uint32(len(tagVal))
						case string:
							bsCount = uint32(len(tagVal))
						default:
							return nil, fmt.Errorf("unsupported tag type %T", tagVal)
						}
					}
					totalLength := 4 + // TotalLength field length
						4 + // DataType field length
						4 + // Num field length
						1 + // IsNull field length
						1 + // HaveLength field length
						4 + // Length field length, each length is 4 bytes
						4 + // BufferLength field length
						bsCount // Buffer field length
					tableTagLength += totalLength
					tagsBufferLen += totalLength
				} else {
					var typeLength = uint32(common.TypeLengthArr[int(tagFields[i].FieldType)])
					if tagVal == nil {
						typeLength = 0
					}
					totalLength := 4 + // TotalLength field length
						4 + // DataType field length
						4 + // Num field length
						1 + // IsNull field length
						1 + // HaveLength field length
						4 + // BufferLength field length
						typeLength // Buffer field length
					tableTagLength += totalLength
					tagsBufferLen += totalLength
				}
			}
			tableTagLengthList[tmpTableIndex] = tableTagLength
		}
		// calculate cols
		if needCols {
			var tableColLength = uint32(0)
			colData := bindData[tmpTableIndex].Cols
			if !isInsert {
				colData = queryParam
			}
			rows := len(colData[0])
			for i := 0; i < colCount; i++ {
				if len(colData[i]) != rows {
					return nil, fmt.Errorf("col row count not match, table_index:%d, col_index:%d, rows:%d, expect:%d", tmpTableIndex, i, len(colData[i]), rows)
				}
			}
			for i := 0; i < colCount; i++ {
				if IsVarDataType(colFields[i].FieldType) {
					var bsCount = uint32(0)
					for j := 0; j < rows; j++ {
						var colVal = colData[i][j]
						if colVal == nil {
							continue
						}
						switch colVal := colVal.(type) {
						case []byte:
							bsCount += uint32(len(colVal))
						case string:
							bsCount += uint32(len(colVal))
						default:
							return nil, fmt.Errorf("unsupported column type %T", colVal)
						}
					}
					totalLength := 4 + // TotalLength field length
						4 + // DataType field length
						4 + // Num field length
						(uint32)(1*rows) + // IsNull field length
						1 + // HaveLength field length
						(uint32)(4*rows) + // Length field length, each length is 4 bytes
						4 + // BufferLength field length
						bsCount // Buffer field length
					tableColLength += totalLength
					colsBufferLen += totalLength
				} else {
					bufferLength := uint32(common.TypeLengthArr[int(colFields[i].FieldType)] * rows)
					if checkAllNull(colData[i]) {
						bufferLength = 0
					}
					totalLength := 4 + // TotalLength field length
						4 + // DataType field length
						4 + // Num field length
						(uint32)(1*rows) + // IsNull field length
						1 + // HaveLength field length
						4 + // BufferLength field length
						bufferLength // Buffer field length
					tableColLength += totalLength
					colsBufferLen += totalLength
				}
			}
			tableColLengthList[tmpTableIndex] = tableColLength
		}
	}
	// table name
	if needTableName {
		tableNameLengthLen = (uint32)(tableCount * 2)
	}
	if needTags {
		tagsDataLengthLen = (uint32)(tableCount * 4)
	}
	if needCols {
		colsDataLengthLen = (uint32)(tableCount * 4)
	}
	var tableNameLength = tableNameLengthLen + tableNameBufferLen
	var tagsDataLength = tagsDataLengthLen + tagsBufferLen
	var colsDataLength = colsDataLengthLen + colsBufferLen
	var totalBufferLen = FixedHeaderLen + tableNameLength + tagsDataLength + colsDataLength
	var tableNameOffset = FixedHeaderLen
	var tagsOffset = tableNameOffset + tableNameLength
	var colsOffset = tagsOffset + tagsDataLength
	buffer := make([]byte, totalBufferLen+uint32(binaryHeaderLength))
	writeU32(buffer, binaryHeaderLength+0, totalBufferLen)       // TotalLength
	writeU32(buffer, binaryHeaderLength+4, (uint32)(tableCount)) // Count
	if needTags {
		writeU32(buffer, binaryHeaderLength+8, (uint32)(tagCount)) // TagCount
	}
	if needCols {
		writeU32(buffer, binaryHeaderLength+12, (uint32)(colCount)) // ColCount
	}
	if needTableName {
		writeU32(buffer, binaryHeaderLength+16, FixedHeaderLen) // TableNamesOffset
	}
	if needTags {
		writeU32(buffer, binaryHeaderLength+20, tagsOffset) // TagsOffset
	}
	if needCols {
		writeU32(buffer, binaryHeaderLength+24, colsOffset) // ColsOffset
	}

	var tableNameLengthOffset = binaryHeaderLength + (int)(tableNameOffset)
	var tableNameBufferOffset = tableNameLengthOffset + (int)(tableNameLengthLen)
	var tagsLengthOffset = binaryHeaderLength + (int)(tagsOffset)
	var tagsBufferOffset = tagsLengthOffset + (int)(tagsDataLengthLen)
	var colsLengthOffset = binaryHeaderLength + (int)(colsOffset)
	var colsBufferOffset = colsLengthOffset + (int)(colsDataLengthLen)

	if needTags {
		// tags length
		copyUint32SliceToBytes(buffer[tagsLengthOffset:], tableTagLengthList)
	}
	// cols length
	if needCols {
		copyUint32SliceToBytes(buffer[colsLengthOffset:], tableColLengthList)
	}
	if needTableName {
		copyUint16SliceToBytes(buffer[tableNameLengthOffset:], utf8TableNameLen)
	}

	var tmpTableNameOffset = tableNameBufferOffset

	var tagOffset = tagsBufferOffset
	var colOffset = colsBufferOffset
	var err error

	for tableIndex := 0; tableIndex < len(bindData); tableIndex++ {
		if needTableName {
			tableName := bindData[tableIndex].TableName
			if len(tableName) > 0 {
				copy(buffer[tmpTableNameOffset:], tableName)
			}
			tmpTableNameOffset += int(utf8TableNameLen[tableIndex])
		}
		if needTags {
			tagOffset, err = writeBindTag(tagFields, bindData[tableIndex].Tags, buffer, tagOffset)
			if err != nil {
				return nil, err
			}
		}
		if needCols {
			if isInsert {
				colOffset, err = writeBindCol(colFields, bindData[tableIndex].Cols, buffer, colOffset)
			} else {
				colOffset, err = writeBindCol(colFields, queryParam, buffer, colOffset)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return buffer, nil
}

func writeU32(buffer []byte, offset int, value uint32) {
	binary.LittleEndian.PutUint32(buffer[offset:offset+4], value)
}

func writeU16(buffer []byte, offset int, value uint16) {
	binary.LittleEndian.PutUint16(buffer[offset:offset+2], value)
}

func writeU64(buffer []byte, offset int, value uint64) {
	binary.LittleEndian.PutUint64(buffer[offset:offset+8], value)
}

func writeI64(buffer []byte, offset int, value int64) {
	binary.LittleEndian.PutUint64(buffer[offset:offset+8], uint64(value))
}

func writeI32(buffer []byte, offset int, value int32) {
	binary.LittleEndian.PutUint32(buffer[offset:offset+4], uint32(value))
}

func writeI16(buffer []byte, offset int, value int16) {
	binary.LittleEndian.PutUint16(buffer[offset:offset+2], uint16(value))
}

func writeFloat32(buffer []byte, offset int, value float32) {
	binary.LittleEndian.PutUint32(buffer[offset:offset+4], math.Float32bits(value))
}

func writeFloat64(buffer []byte, offset int, value float64) {
	binary.LittleEndian.PutUint64(buffer[offset:offset+8], math.Float64bits(value))
}

func writeBindTag(tagFields []*Stmt2AllField, tagVal []driver.Value, buffer []byte, offset int) (int, error) {
	var startOffset = offset
	for i := 0; i < len(tagVal); i++ {
		totalLength := uint32(0)
		// write DataType
		writeU32(buffer, startOffset+DataTypeOffset, (uint32)(tagFields[i].FieldType))
		// write Num
		writeU32(buffer, startOffset+NumOffset, 1)
		// hasLength
		isVarData := IsVarDataType(tagFields[i].FieldType)
		if tagVal[i] == nil {
			buffer[startOffset+IsNullOffset] = 1
			if isVarData {
				// have length
				buffer[startOffset+HaveLengthOffset] = 1
				// length
				//writeU32(buffer, startOffset+HaveLengthOffset+4, 0)
				// write TotalLength
				totalLength = 4 + // TotalLength field length
					4 + // DataType field length
					4 + // Num field length
					1 + // IsNull field length
					1 + // HaveLength field length
					4 + // Length field length, each length is 4 bytes
					4 // BufferLength field length
			} else {
				// write TotalLength
				totalLength = 4 + // TotalLength field length
					4 + // DataType field length
					4 + // Num field length
					1 + // IsNull field length
					1 + // HaveLength field length
					4 // BufferLength field length
			}
			writeU32(buffer, startOffset+TotalLengthOffset, totalLength)
		} else {
			if !isVarData {
				var dataLength = uint32(common.TypeLengthArr[int(tagFields[i].FieldType)])
				switch tagFields[i].FieldType {
				case common.TSDB_DATA_TYPE_BOOL:
					val, ok := tagVal[i].(bool)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect bool, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					if val {
						buffer[startOffset+FixedBufferOffset] = 1
					}
				case common.TSDB_DATA_TYPE_TINYINT:
					val, ok := tagVal[i].(int8)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect int8, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					buffer[startOffset+FixedBufferOffset] = byte(val)
				case common.TSDB_DATA_TYPE_SMALLINT:
					val, ok := tagVal[i].(int16)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect int16, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					writeI16(buffer, startOffset+FixedBufferOffset, val)
				case common.TSDB_DATA_TYPE_INT:
					val, ok := tagVal[i].(int32)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect int32, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					writeI32(buffer, startOffset+FixedBufferOffset, val)
				case common.TSDB_DATA_TYPE_BIGINT:
					val, ok := tagVal[i].(int64)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect int64, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					writeI64(buffer, startOffset+FixedBufferOffset, val)
				case common.TSDB_DATA_TYPE_FLOAT:
					val, ok := tagVal[i].(float32)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect float32, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					writeFloat32(buffer, startOffset+FixedBufferOffset, val)
				case common.TSDB_DATA_TYPE_DOUBLE:
					val, ok := tagVal[i].(float64)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect float64, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					writeFloat64(buffer, startOffset+FixedBufferOffset, val)
				case common.TSDB_DATA_TYPE_TIMESTAMP:
					switch tagVal[i].(type) {
					case int64:
						writeI64(buffer, startOffset+FixedBufferOffset, tagVal[i].(int64))
					case time.Time:
						writeI64(buffer, startOffset+FixedBufferOffset, common.TimeToTimestamp(tagVal[i].(time.Time), int(tagFields[i].Precision)))
					default:
						return 0, fmt.Errorf("tag field type not match, expect int64 or time.Time, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
				case common.TSDB_DATA_TYPE_UTINYINT:
					val, ok := tagVal[i].(uint8)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect uint8, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					buffer[startOffset+FixedBufferOffset] = val
				case common.TSDB_DATA_TYPE_USMALLINT:
					val, ok := tagVal[i].(uint16)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect uint16, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					writeU16(buffer, startOffset+FixedBufferOffset, val)
				case common.TSDB_DATA_TYPE_UINT:
					val, ok := tagVal[i].(uint32)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect uint32, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					writeU32(buffer, startOffset+FixedBufferOffset, val)
				case common.TSDB_DATA_TYPE_UBIGINT:
					val, ok := tagVal[i].(uint64)
					if !ok {
						return 0, fmt.Errorf("tag field type not match, expect uint64, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
					}
					writeU64(buffer, startOffset+FixedBufferOffset, val)
				default:
					return 0, fmt.Errorf("tag field type not support: %d, tag_index: %d, tag_name: %s", tagFields[i].FieldType, i, tagFields[i].Name)
				}
				totalLength = 4 + // TotalLength field length
					4 + // DataType field length
					4 + // Num field length
					1 + // IsNull field length
					1 + // HaveLength field length
					4 + // BufferLength field length
					dataLength // Buffer field length
				writeU32(buffer, startOffset+TotalLengthOffset, totalLength)
				// write BufferLength
				writeU32(buffer, startOffset+FixedBufferLengthOffset, dataLength)
			} else {
				var dataLength uint32
				switch tagVal[i].(type) {
				case string:
					val := tagVal[i].(string)
					dataLength = uint32(len(val))
					copy(buffer[startOffset+HaveLengthOffset+1+4+4:], val)
				case []byte:
					val := tagVal[i].([]byte)
					dataLength = uint32(len(val))
					copy(buffer[startOffset+HaveLengthOffset+1+4+4:], val)
				default:
					return 0, fmt.Errorf("tag field type not match, expect string or []byte, actual %T, tag_index: %d, tag_name: %s", tagVal[i], i, tagFields[i].Name)
				}
				totalLength = 4 + // TotalLength field length
					4 + // DataType field length
					4 + // Num field length
					1 + // IsNull field length
					1 + // HaveLength field length
					4 + // Length field length, each length is 4 bytes
					4 + // BufferLength field length
					dataLength // Buffer field length
				writeU32(buffer, startOffset+TotalLengthOffset, totalLength)
				buffer[startOffset+HaveLengthOffset] = 1
				// write LengthField
				writeU32(buffer, startOffset+HaveLengthOffset+1, dataLength)
				// write BufferLength
				writeU32(buffer, startOffset+HaveLengthOffset+1+4, dataLength)
			}
		}
		startOffset += (int)(totalLength)
	}
	return startOffset, nil
}

const (
	TotalLengthOffset       = 0
	DataTypeOffset          = 4
	NumOffset               = 8
	IsNullOffset            = 12
	HaveLengthOffset        = 13
	FixedBufferLengthOffset = 14
	FixedBufferOffset       = 18
)

func writeBindCol(colFields []*Stmt2AllField, colVals [][]driver.Value, buffer []byte, offset int) (int, error) {
	var startOffset = offset
	if len(colVals) != len(colFields) {
		return 0, fmt.Errorf("col count not match, data count:%d, field count:%d", len(colVals), len(colFields))
	}
	var rows = len(colVals[0])
	for colIndex := 0; colIndex < len(colVals); colIndex++ {
		if len(colVals[colIndex]) != rows {
			return 0, fmt.Errorf("col row count not match, col_index:%d, rows:%d, expect:%d", colIndex, len(colVals[colIndex]), rows)
		}
	}
	var haveLengthOffset = IsNullOffset + rows
	var fixedBufferLengthOffset = haveLengthOffset + 1
	var fixedBufferOffset = fixedBufferLengthOffset + 4
	var variableLengthOffset = haveLengthOffset + 1
	var variableBufferLengthOffset = variableLengthOffset + (4 * rows)
	var variableBufferOffset = variableBufferLengthOffset + 4

	for colIndex := 0; colIndex < len(colVals); colIndex++ {
		var colData = colVals[colIndex]
		var colField = colFields[colIndex]
		totalLength := 0
		// write DataType
		writeU32(buffer, startOffset+DataTypeOffset, uint32(colField.FieldType))
		// write Num
		writeU32(buffer, startOffset+NumOffset, uint32(rows))
		// hasLength
		var isVarData = IsVarDataType(colField.FieldType)
		if isVarData {
			buffer[startOffset+haveLengthOffset] = 1
			var variableOffset = startOffset + variableBufferOffset
			// variable length data
			var totalVarBufferLength = 0
			for rowIndex := 0; rowIndex < rows; rowIndex++ {
				var value = colData[rowIndex]
				if value == nil {
					buffer[startOffset+IsNullOffset+rowIndex] = 1
				} else {
					switch value := value.(type) {
					case []byte:
						bs := value
						// write length
						writeU32(buffer, startOffset+variableLengthOffset+(4*rowIndex), uint32(len(bs)))
						copy(buffer[variableOffset:], bs)
						totalVarBufferLength += len(bs)
						variableOffset += len(bs)
					case string:
						str := value
						// write length
						writeU32(buffer, startOffset+variableLengthOffset+(4*rowIndex), uint32(len(str)))
						copy(buffer[variableOffset:], str)
						totalVarBufferLength += len(str)
						variableOffset += len(str)
					default:
						return 0, fmt.Errorf("col field type not support: %d, value: %v, col_name: %s", colField.FieldType, value, colField.Name)
					}
				}
			}
			totalLength = 4 + // TotalLength field length
				4 + // DataType field length
				4 + // Num field length
				(1 * rows) + // IsNull field length
				1 + // HaveLength field length
				(4 * rows) + // Length field length, each length is 4 bytes
				4 + // BufferLength field length
				totalVarBufferLength // Buffer field length
			// write TotalLength
			writeU32(buffer, startOffset+TotalLengthOffset, (uint32)(totalLength))
			// write BufferLength
			writeU32(buffer, startOffset+variableBufferLengthOffset, (uint32)(totalVarBufferLength))
		} else {
			var typeLength = common.TypeLengthArr[int(colField.FieldType)]
			var fixedOffset = startOffset + fixedBufferOffset
			if checkAllNull(colData) {
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					buffer[startOffset+IsNullOffset+rowIndex] = 1
				}
				totalLength = 4 + // TotalLength field length
					4 + // DataType field length
					4 + // Num field length
					(1 * rows) + // IsNull field length
					1 + // HaveLength field length
					4 // BufferLength field length
				// write TotalLength
				writeU32(buffer, startOffset+TotalLengthOffset, (uint32)(totalLength))
				// write BufferLength
				writeU32(buffer, startOffset+fixedBufferLengthOffset, 0)
				startOffset += totalLength
				continue
			}
			switch colField.FieldType {
			case common.TSDB_DATA_TYPE_BOOL:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(bool)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect bool, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						if v {
							buffer[fixedOffset] = 1
						} else {
							buffer[fixedOffset] = 0
						}
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_TINYINT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(int8)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect int8, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						buffer[fixedOffset] = byte(v)
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_SMALLINT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(int16)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect int16, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						writeI16(buffer, fixedOffset, v)
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_INT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(int32)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect int32, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						writeI32(buffer, fixedOffset, v)
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_BIGINT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(int64)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect int64, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						writeI64(buffer, fixedOffset, v)
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_FLOAT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(float32)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect float32, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						writeFloat32(buffer, fixedOffset, v)
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_DOUBLE:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(float64)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect float64, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						writeFloat64(buffer, fixedOffset, v)
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_TIMESTAMP:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						switch value := value.(type) {
						case int64:
							writeI64(buffer, fixedOffset, value)
						case time.Time:
							v := value
							ts := common.TimeToTimestamp(v, int(colField.Precision))
							writeI64(buffer, fixedOffset, ts)
						default:
							return 0, fmt.Errorf("col field type not match, expect int64 or time.Time, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_UTINYINT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(uint8)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect uint8, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						buffer[fixedOffset] = v
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_USMALLINT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(uint16)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect uint16, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						writeU16(buffer, fixedOffset, v)
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_UINT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(uint32)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect uint32, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						writeU32(buffer, fixedOffset, v)
					}
					fixedOffset += typeLength
				}
			case common.TSDB_DATA_TYPE_UBIGINT:
				for rowIndex := 0; rowIndex < rows; rowIndex++ {
					var value = colData[rowIndex]
					if value == nil {
						buffer[startOffset+IsNullOffset+rowIndex] = 1
					} else {
						v, ok := value.(uint64)
						if !ok {
							return 0, fmt.Errorf("col field type not match, expect uint64, actual %T, col_index: %d, col_name: %s", value, colIndex, colField.Name)
						}
						writeU64(buffer, fixedOffset, v)
					}
					fixedOffset += typeLength
				}
			default:
				return 0, fmt.Errorf("col field type not support: %d, col_name: %s", colField.FieldType, colField.Name)
			}
			totalFixedBufferLength := typeLength * rows
			totalLength = 4 + // TotalLength field length
				4 + // DataType field length
				4 + // Num field length
				(1 * rows) + // IsNull field length
				1 + // HaveLength field length
				4 + // BufferLength field length
				totalFixedBufferLength // Buffer field length
			// write TotalLength
			writeU32(buffer, startOffset+TotalLengthOffset, (uint32)(totalLength))
			// write BufferLength
			writeU32(buffer, startOffset+fixedBufferLengthOffset, (uint32)(totalFixedBufferLength))
		}
		startOffset += totalLength
	}
	return startOffset, nil
}

func checkAllNull(data []driver.Value) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != nil {
			return false
		}
	}
	return true
}

type Stmt2AllField struct {
	Name      string `json:"name"`
	FieldType int8   `json:"field_type"`
	Precision uint8  `json:"precision"`
	Scale     uint8  `json:"scale"`
	Bytes     int32  `json:"bytes"`
	BindType  int8   `json:"bind_type"`
}
