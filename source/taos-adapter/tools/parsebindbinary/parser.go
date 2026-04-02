package parsebindbinary

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/stmt"
	"github.com/taosdata/taosadapter/v3/tools/jsonbuilder"
)

type TSDBTypeJson int

type Bind struct {
	Type TSDBTypeJson    `json:"type"` // type of the column
	Data json.RawMessage `json:"data"` // data buffer for each row
}

type Stmt2BindV struct {
	Count      int       `json:"count"`       // number of tables
	TableNames []string  `json:"table_names"` // table names
	Tags       [][]*Bind `json:"tags"`        // [table][tag] *Bind
	Cols       [][]*Bind `json:"cols"`        // [table][col] *Bind
}

func ParseStmt2BindV(bs []byte) (*Stmt2BindV, error) {
	result := &Stmt2BindV{}
	if len(bs) < stmt.DataPosition {
		return nil, fmt.Errorf("data length %d is less than header length %d", len(bs), stmt.DataPosition)
	}
	totalLength := len(bs)
	dataLength := bs[stmt.TotalLengthPosition : stmt.TotalLengthPosition+4]
	totalDataLength := int(binary.LittleEndian.Uint32(dataLength))
	if totalDataLength != totalLength {
		return nil, fmt.Errorf("total length not match, expect %d, but get %d", totalLength, totalDataLength)
	}
	count := bs[stmt.CountPosition : stmt.CountPosition+4]
	tagCount := bs[stmt.TagCountPosition : stmt.TagCountPosition+4]
	colCount := bs[stmt.ColCountPosition : stmt.ColCountPosition+4]
	tableNamesOffset := bs[stmt.TableNamesOffsetPosition : stmt.TableNamesOffsetPosition+4]
	tagsOffset := bs[stmt.TagsOffsetPosition : stmt.TagsOffsetPosition+4]
	colsOffset := bs[stmt.ColsOffsetPosition : stmt.ColsOffsetPosition+4]
	tbOffset := int(binary.LittleEndian.Uint32(tableNamesOffset))
	tagOffset := int(binary.LittleEndian.Uint32(tagsOffset))
	colOffset := int(binary.LittleEndian.Uint32(colsOffset))
	bindCount := int(binary.LittleEndian.Uint32(count))
	colCounts := int(binary.LittleEndian.Uint32(colCount))
	tagCounts := int(binary.LittleEndian.Uint32(tagCount))
	result.Count = bindCount
	if tbOffset > 0 {
		// check table names offset
		tableNameEnd := tbOffset + bindCount*2
		if tableNameEnd > totalLength {
			return nil, fmt.Errorf("table name lengths out of range, total length: %d, tableNamesLengthEnd: %d", totalLength, tbOffset)
		}
	}
	if tagOffset > 0 {
		// check tag offset
		if tagCounts == 0 {
			return nil, fmt.Errorf("tag count is 0 but tag offset is %d", tagOffset)
		}
		tagEnd := tagOffset + bindCount*4
		if tagEnd > totalLength {
			return nil, fmt.Errorf("tag lengths out of range, total length: %d, tagLengthsEnd: %d", totalLength, tagEnd)
		}
	}
	if colOffset > 0 {
		if colCounts == 0 {
			return nil, fmt.Errorf("column count is 0 but column offset is %d", colOffset)
		}
		colEnd := colOffset + bindCount*4
		if colEnd > totalLength {
			return nil, fmt.Errorf("column lengths out of range, total length: %d, colLengthsEnd: %d", totalLength, colEnd)
		}
	}
	if tbOffset > 0 {
		tableNameEnd := tbOffset + bindCount*2
		// parse table names
		result.TableNames = make([]string, bindCount)
		// use tagOffset as end first
		end := tagOffset
		if end == 0 {
			// if no tags, use colOffset as end
			end = colOffset
		}
		// if no cols, use length of bs as end
		if end == 0 {
			end = len(bs)
		}
		tableNameBuffer := bs[tbOffset:end]
		tableNameDataBuffer := tableNameBuffer[bindCount*2:]
		tableNameStart := 0
		for i := 0; i < bindCount; i++ {
			length := binary.LittleEndian.Uint16(tableNameBuffer[i*2:])
			if length == 0 {
				return nil, fmt.Errorf("table name length is 0 for table index %d", i)
			}
			tableNameEnd += int(length)
			if tableNameEnd > totalLength {
				return nil, fmt.Errorf("table names out of range, total length: %d, tableNameTotalLength: %d", totalLength, tableNameEnd)
			}
			tableNameEnd = tableNameStart + int(length)
			// length includes null terminator
			result.TableNames[i] = string(tableNameDataBuffer[tableNameStart : tableNameEnd-1])
			// move to next
			tableNameStart = tableNameEnd
		}
	}
	var err error
	if tagOffset > 0 {
		tagEnd := tagOffset + bindCount*4
		// parse tags
		// use colOffset as end first
		end := colOffset
		if end == 0 {
			// if no cols, use length of bs as end
			end = len(bs)
		}
		tags := bs[tagOffset:end]
		tagLength := make([]uint32, bindCount)
		for i := 0; i < bindCount; i++ {
			tagLength[i] = binary.LittleEndian.Uint32(tags[i*4:])
			tagEnd += int(tagLength[i])
		}
		if tagEnd > totalLength {
			return nil, fmt.Errorf("tags out of range, total length: %d, tagTotalLength: %d", totalLength, tagEnd)
		}
		dataBufferOffset := bindCount * 4
		tagsLengthsBuffer := tags[:dataBufferOffset]
		tagsDataBuffer := tags[dataBufferOffset:]
		result.Tags, err = parseBind(tagsDataBuffer, tagsLengthsBuffer, bindCount, tagCounts)
		if err != nil {
			return nil, err
		}
	}
	if colOffset > 0 {
		colEnd := colOffset + bindCount*4
		// parse cols
		cols := bs[colOffset:]
		colLength := make([]uint32, bindCount)
		for i := 0; i < bindCount; i++ {
			colLength[i] = binary.LittleEndian.Uint32(cols[i*4:])
			colEnd += int(colLength[i])
		}
		if colEnd > totalLength {
			return nil, fmt.Errorf("cols out of range, total length: %d, colTotalLength: %d", totalLength, colEnd)
		}
		dataBufferOffset := bindCount * 4
		colsLengthsBuffer := cols[:dataBufferOffset]
		colsDataBuffer := cols[dataBufferOffset:]
		result.Cols, err = parseBind(colsDataBuffer, colsLengthsBuffer, bindCount, colCounts)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

const (
	BindTypeOffset = 4
	RowNumOffset   = 8
	isNullOffset   = 12
)

func parseBind(dataBuffer []byte, lengthBuffer []byte, tableCount int, fieldCount int) ([][]*Bind, error) {
	stream := jsonbuilder.BorrowStream(nil)
	defer func() {
		stream.SetBuffer(nil)
		jsonbuilder.ReturnStream(stream)
	}()
	result := make([][]*Bind, tableCount)
	tableBindBufferStart := 0
	tableBindBufferEnd := 0
	for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
		bindLength := int(binary.LittleEndian.Uint32(lengthBuffer[tableIndex*4:]))
		tableBindBufferEnd = tableBindBufferStart + bindLength
		result[tableIndex] = make([]*Bind, fieldCount)
		binds := make([]Bind, bindLength)
		for i := 0; i < fieldCount; i++ {
			result[tableIndex][i] = &binds[i]
		}
		tableBuffer := dataBuffer[tableBindBufferStart:tableBindBufferEnd]
		tableBindBufferStart = tableBindBufferEnd
		bs := tableBuffer
		for fieldIndex := 0; fieldIndex < fieldCount; fieldIndex++ {
			currentLengthBufferLength := int(binary.LittleEndian.Uint32(bs))
			bind := result[tableIndex][fieldIndex]
			colType := int(binary.LittleEndian.Uint32(bs[BindTypeOffset:]))
			bind.Type = TSDBTypeJson(colType)
			rowNum := int(binary.LittleEndian.Uint32(bs[RowNumOffset:]))
			isNulls := make([]int8, rowNum)
			haveLength := bs[isNullOffset+rowNum] == 1
			bufferLengthOffset := isNullOffset + rowNum + 1
			var lengths []int32
			varLength := 0
			nullCount := 0
			if haveLength {
				lengths = make([]int32, rowNum)
				bufferLengthOffset += rowNum * 4
			}
			for i := 0; i < rowNum; i++ {
				isNull := int8(bs[isNullOffset+i])
				isNulls[i] = isNull
				if isNull == 1 {
					// null
					nullCount += 1
				}
				if haveLength {
					length := int32(binary.LittleEndian.Uint32(bs[isNullOffset+rowNum+1+i*4:]))
					lengths[i] += length
					varLength += int(length)
				}
			}
			bufferLength := int(binary.LittleEndian.Uint32(bs[bufferLengthOffset:]))
			checkOffset := bufferLengthOffset + 4 + bufferLength
			if checkOffset != currentLengthBufferLength {
				return nil, fmt.Errorf("buffer length not match for table %d field %d, expect %d, but get %d", tableIndex, fieldIndex, currentLengthBufferLength, checkOffset)
			}
			if bufferLength == 0 {
				bind.Data = nil
			} else {
				bufferOffset := bufferLengthOffset + 4
				bufferData := bs[bufferOffset : bufferOffset+bufferLength]
				// null * nullCount + '[' + ']' + (rowNum - 1) * ','
				baseBufferSize := nullCount*4 + 2 + rowNum - 1 + varLength + rowNum*2
				if baseBufferSize < 32 {
					baseBufferSize = 32
				}
				var err error
				bind.Data, err = parseBindBuffer(stream, bufferData, colType, rowNum, isNulls, lengths, baseBufferSize)
				if err != nil {
					return nil, err
				}
			}
			bs = bs[currentLengthBufferLength:]
		}
	}
	return result, nil
}

func parseBindBuffer(builder *jsonbuilder.Stream, bs []byte, dataType int, rowNum int, isnull []int8, dataLength []int32, baseBufferSize int) (json.RawMessage, error) {
	defer func() {
		builder.SetBuffer(nil)
	}()
	buf := make([]byte, 0, baseBufferSize)
	builder.SetBuffer(buf)
	builder.WriteArrayStart()
	switch dataType {
	case common.TSDB_DATA_TYPE_BOOL:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				if bs[i] == 0 {
					builder.WriteFalse()
				} else {
					builder.WriteTrue()
				}
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_TINYINT:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				builder.WriteInt32(int32(int8(bs[i])))
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_SMALLINT:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				start := i * 2
				value := int16(binary.LittleEndian.Uint16(bs[start:]))
				builder.WriteInt32(int32(value))
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_INT:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				start := i * 4
				value := int32(binary.LittleEndian.Uint32(bs[start:]))
				builder.WriteInt32(value)
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_BIGINT, common.TSDB_DATA_TYPE_TIMESTAMP:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				start := i * 8
				value := int64(binary.LittleEndian.Uint64(bs[start:]))
				builder.WriteInt64(value)
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_FLOAT:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				start := i * 4
				bits := binary.LittleEndian.Uint32(bs[start:])
				value := math.Float32frombits(bits)
				builder.WriteFloat32(value)
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_DOUBLE:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				start := i * 8
				bits := binary.LittleEndian.Uint64(bs[start:])
				value := math.Float64frombits(bits)
				builder.WriteFloat64(value)
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}

	case common.TSDB_DATA_TYPE_UTINYINT:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				builder.WriteUint32(uint32(bs[i]))
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_USMALLINT:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				start := i * 2
				value := binary.LittleEndian.Uint16(bs[start:])
				builder.WriteUint32(uint32(value))
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_UINT:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				start := i * 4
				value := binary.LittleEndian.Uint32(bs[start:])
				builder.WriteUint32(value)
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_UBIGINT:
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				start := i * 8
				value := binary.LittleEndian.Uint64(bs[start:])
				builder.WriteUint64(value)
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_BINARY, common.TSDB_DATA_TYPE_NCHAR, common.TSDB_DATA_TYPE_JSON, common.TSDB_DATA_TYPE_DECIMAL, common.TSDB_DATA_TYPE_DECIMAL64:
		// string
		start := 0
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				length := dataLength[i]
				end := start + int(length)
				value := string(bs[start:end])
				builder.WriteString(value)
				start = end
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	case common.TSDB_DATA_TYPE_VARBINARY, common.TSDB_DATA_TYPE_BLOB, common.TSDB_DATA_TYPE_GEOMETRY:
		// hex string
		start := 0
		for i := 0; i < rowNum; i++ {
			if isnull[i] == 1 {
				builder.WriteNil()
			} else {
				length := dataLength[i]
				end := start + int(length)
				value := bs[start:end]
				writeHexString(builder, value)
				start = end
			}
			if i < rowNum-1 {
				builder.WriteMore()
			}
		}
	default:
		return nil, fmt.Errorf("unsupported data type %d", dataType)
	}
	builder.WriteArrayEnd()
	return builder.Buffer(), nil
}

var hexTable = [16]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}

func writeHexString(builder *jsonbuilder.Stream, data []byte) {
	builder.AddByte('"')
	for index := 0; index < len(data); index++ {
		builder.AddByte(hexTable[data[index]>>4])
		builder.AddByte(hexTable[data[index]&0x0f])
	}
	builder.AddByte('"')
}
