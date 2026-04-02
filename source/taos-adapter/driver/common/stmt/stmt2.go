package stmt

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/taosdata/taosadapter/v3/driver/common"
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

func MarshalStmt2Binary(bindData []*TaosStmt2BindData, isInsert bool, fields []*Stmt2AllField) ([]byte, error) {
	var colType []*Stmt2AllField
	var tagType []*Stmt2AllField
	for i := 0; i < len(fields); i++ {
		switch fields[i].BindType {
		case TAOS_FIELD_COL:
			colType = append(colType, fields[i])
		case TAOS_FIELD_TAG:
			tagType = append(tagType, fields[i])
		}
	}
	// count
	count := len(bindData)
	if count == 0 {
		return nil, fmt.Errorf("empty data")
	}
	needTableNames := false
	needTags := false
	needCols := false
	tagCount := len(tagType)
	colCount := len(colType)
	if isInsert {
		for i := 0; i < count; i++ {
			data := bindData[i]
			if data.TableName != "" {
				needTableNames = true
			}
			if len(data.Tags) != tagCount {
				return nil, fmt.Errorf("tag count not match, data count:%d, type count:%d", len(data.Tags), tagCount)
			}
			if len(data.Cols) != colCount {
				return nil, fmt.Errorf("col count not match, data count:%d, type count:%d", len(data.Cols), colCount)
			}
		}
	} else {
		if tagCount != 0 {
			return nil, fmt.Errorf("query not need tag types")
		}
		if colCount != 0 {
			return nil, fmt.Errorf("query not need col types")
		}
		if count != 1 {
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
		for j := 0; j < colCount; j++ {
			if len(data.Cols[j]) != 1 {
				return nil, fmt.Errorf("query col data must be one row, col:%d, count:%d", j, len(data.Cols[j]))
			}
		}
	}

	header := make([]byte, DataPosition)
	// count
	binary.LittleEndian.PutUint32(header[CountPosition:], uint32(count))
	// tag count
	if tagCount != 0 {
		needTags = true
		binary.LittleEndian.PutUint32(header[TagCountPosition:], uint32(tagCount))
	}
	// col count
	if colCount != 0 {
		needCols = true
		binary.LittleEndian.PutUint32(header[ColCountPosition:], uint32(colCount))
	}
	if !needTableNames && !needTags && !needCols {
		return nil, fmt.Errorf("no data")
	}
	tmpBuf := &bytes.Buffer{}
	tableNameBuf := &bytes.Buffer{}
	var tableNameLength []uint16
	if needTableNames {
		tableNameLength = make([]uint16, count)
	}
	tagBuf := &bytes.Buffer{}
	var tagDataLength []uint32
	if needTags {
		tagDataLength = make([]uint32, count)
	}
	colBuf := &bytes.Buffer{}
	var colDataLength []uint32
	if needCols {
		colDataLength = make([]uint32, count)
	}
	for index, data := range bindData {
		// table name
		if needTableNames {
			if data.TableName != "" {
				if len(data.TableName) > math.MaxUint16-1 {
					return nil, fmt.Errorf("table name too long, index:%d, length:%d", index, len(data.TableName))
				}
				tableNameBuf.WriteString(data.TableName)
			}
			tableNameBuf.WriteByte(0)
			tableNameLength[index] = uint16(len(data.TableName) + 1)
		}

		// tag
		if needTags {
			length := 0
			for i := 0; i < len(data.Tags); i++ {
				tag := data.Tags[i]
				tagDataBuffer, err := generateBindColData([]driver.Value{tag}, tagType[i], tmpBuf)
				if err != nil {
					return nil, err
				}
				length += len(tagDataBuffer)
				tagBuf.Write(tagDataBuffer)
			}
			tagDataLength[index] = uint32(length)
		}
		// col
		if needCols {
			length := 0
			for i := 0; i < len(data.Cols); i++ {
				col := data.Cols[i]
				var colDataBuffer []byte
				var err error
				if isInsert {
					colDataBuffer, err = generateBindColData(col, colType[i], tmpBuf)
				} else {
					colDataBuffer, err = generateBindQueryData(col[0])
				}
				if err != nil {
					return nil, err
				}
				length += len(colDataBuffer)
				colBuf.Write(colDataBuffer)
			}
			colDataLength[index] = uint32(length)
		}
	}
	tableTotalLength := tableNameBuf.Len()
	tagTotalLength := tagBuf.Len()
	colTotalLength := colBuf.Len()
	tagOffset := DataPosition + tableTotalLength + len(tableNameLength)*2
	colOffset := tagOffset + tagTotalLength + len(tagDataLength)*4
	totalLength := colOffset + colTotalLength + len(colDataLength)*4
	if needTableNames {
		binary.LittleEndian.PutUint32(header[TableNamesOffsetPosition:], uint32(DataPosition))
	}
	if needTags {
		binary.LittleEndian.PutUint32(header[TagsOffsetPosition:], uint32(tagOffset))
	}
	if needCols {
		binary.LittleEndian.PutUint32(header[ColsOffsetPosition:], uint32(colOffset))
	}
	binary.LittleEndian.PutUint32(header[TotalLengthPosition:], uint32(totalLength))
	buffer := make([]byte, totalLength)
	copy(buffer, header)
	if needTableNames {
		offset := DataPosition
		for _, length := range tableNameLength {
			binary.LittleEndian.PutUint16(buffer[offset:], length)
			offset += 2
		}
		copy(buffer[offset:], tableNameBuf.Bytes())
	}
	if needTags {
		offset := tagOffset
		for _, length := range tagDataLength {
			binary.LittleEndian.PutUint32(buffer[offset:], length)
			offset += 4
		}
		copy(buffer[offset:], tagBuf.Bytes())
	}
	if needCols {
		offset := colOffset
		for _, length := range colDataLength {
			binary.LittleEndian.PutUint32(buffer[offset:], length)
			offset += 4
		}
		copy(buffer[offset:], colBuf.Bytes())
	}
	return buffer, nil
}

func getBindDataHeaderLength(num int, needLength bool) int {
	length := 17 + num
	if needLength {
		length += num * 4
	}
	return length
}

func generateBindColData(data []driver.Value, colType *Stmt2AllField, tmpBuffer *bytes.Buffer) ([]byte, error) {
	num := len(data)
	tmpBuffer.Reset()
	needLength := needLength(colType.FieldType)
	headerLength := getBindDataHeaderLength(num, needLength)
	tmpHeader := make([]byte, headerLength)
	// type
	binary.LittleEndian.PutUint32(tmpHeader[BindDataTypeOffset:], uint32(colType.FieldType))
	// num
	binary.LittleEndian.PutUint32(tmpHeader[BindDataNumOffset:], uint32(num))
	// is null
	isNull := tmpHeader[BindDataIsNullOffset : BindDataIsNullOffset+num]
	// has length
	if needLength {
		tmpHeader[BindDataIsNullOffset+num] = 1
	}
	bufferLengthOffset := BindDataIsNullOffset + num + 1
	isAllNull := checkAllNull(data)
	if isAllNull {
		for i := 0; i < num; i++ {
			isNull[i] = 1
		}
	} else {
		switch colType.FieldType {
		case common.TSDB_DATA_TYPE_BOOL:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					tmpBuffer.WriteByte(0)
				} else {
					v, ok := data[i].(bool)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect bool, but get %T, value:%v", data[i], data[i])
					}
					if v {
						tmpBuffer.WriteByte(1)
					} else {
						tmpBuffer.WriteByte(0)
					}
				}
			}
		case common.TSDB_DATA_TYPE_TINYINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					tmpBuffer.WriteByte(0)
				} else {
					v, ok := data[i].(int8)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect int8, but get %T, value:%v", data[i], data[i])
					}
					tmpBuffer.WriteByte(byte(v))
				}
			}

		case common.TSDB_DATA_TYPE_SMALLINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint16(tmpBuffer, uint16(0))
				} else {
					v, ok := data[i].(int16)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect int16, but get %T, value:%v", data[i], data[i])
					}
					writeUint16(tmpBuffer, uint16(v))
				}
			}

		case common.TSDB_DATA_TYPE_INT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint32(tmpBuffer, uint32(0))
				} else {
					v, ok := data[i].(int32)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect int32, but get %T, value:%v", data[i], data[i])
					}
					writeUint32(tmpBuffer, uint32(v))
				}
			}
		case common.TSDB_DATA_TYPE_BIGINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint64(tmpBuffer, 0)
				} else {
					v, ok := data[i].(int64)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect int64, but get %T, value:%v", data[i], data[i])
					}
					writeUint64(tmpBuffer, uint64(v))
				}
			}
		case common.TSDB_DATA_TYPE_FLOAT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint32(tmpBuffer, 0)
				} else {
					v, ok := data[i].(float32)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect float32, but get %T, value:%v", data[i], data[i])
					}
					writeUint32(tmpBuffer, math.Float32bits(v))
				}
			}
		case common.TSDB_DATA_TYPE_DOUBLE:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint64(tmpBuffer, 0)
				} else {
					v, ok := data[i].(float64)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect float64, but get %T, value:%v", data[i], data[i])
					}
					writeUint64(tmpBuffer, math.Float64bits(v))
				}
			}
		case common.TSDB_DATA_TYPE_TIMESTAMP:
			precision := int(colType.Precision)
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint64(tmpBuffer, 0)
				} else {
					switch v := data[i].(type) {
					case int64:
						writeUint64(tmpBuffer, uint64(v))
					case time.Time:
						ts := common.TimeToTimestamp(v, precision)
						writeUint64(tmpBuffer, uint64(ts))
					default:
						return nil, fmt.Errorf("data type not match, expect int64 or time.Time, but get %T, value:%v", data[i], data[i])
					}
				}
			}
		case common.TSDB_DATA_TYPE_BINARY, common.TSDB_DATA_TYPE_NCHAR, common.TSDB_DATA_TYPE_VARBINARY, common.TSDB_DATA_TYPE_GEOMETRY, common.TSDB_DATA_TYPE_JSON, common.TSDB_DATA_TYPE_DECIMAL, common.TSDB_DATA_TYPE_DECIMAL64, common.TSDB_DATA_TYPE_BLOB:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
				} else {
					switch v := data[i].(type) {
					case string:
						tmpBuffer.WriteString(v)
						binary.LittleEndian.PutUint32(tmpHeader[bufferLengthOffset+i*4:], uint32(len(v)))
					case []byte:
						tmpBuffer.Write(v)
						binary.LittleEndian.PutUint32(tmpHeader[bufferLengthOffset+i*4:], uint32(len(v)))
					default:
						return nil, fmt.Errorf("data type not match, expect string or []byte, but get %T, value:%v", data[i], data[i])
					}
				}
			}
		case common.TSDB_DATA_TYPE_UTINYINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					tmpBuffer.WriteByte(0)
				} else {
					v, ok := data[i].(uint8)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect uint8, but get %T, value:%v", data[i], data[i])
					}
					tmpBuffer.WriteByte(v)
				}
			}
		case common.TSDB_DATA_TYPE_USMALLINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint16(tmpBuffer, 0)
				} else {
					v, ok := data[i].(uint16)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect uint16, but get %T, value:%v", data[i], data[i])
					}
					writeUint16(tmpBuffer, v)
				}
			}
		case common.TSDB_DATA_TYPE_UINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint32(tmpBuffer, 0)
				} else {
					v, ok := data[i].(uint32)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect uint32, but get %T, value:%v", data[i], data[i])
					}
					writeUint32(tmpBuffer, v)
				}
			}
		case common.TSDB_DATA_TYPE_UBIGINT:
			for i := 0; i < num; i++ {
				if data[i] == nil {
					isNull[i] = 1
					writeUint64(tmpBuffer, 0)
				} else {
					v, ok := data[i].(uint64)
					if !ok {
						return nil, fmt.Errorf("data type not match, expect uint64, but get %T, value:%v", data[i], data[i])
					}
					writeUint64(tmpBuffer, v)
				}
			}
		default:
			return nil, fmt.Errorf("unsupported type: %d", colType.FieldType)
		}
	}
	buffer := tmpBuffer.Bytes()
	// bufferLength
	binary.LittleEndian.PutUint32(tmpHeader[headerLength-4:], uint32(len(buffer)))
	totalLength := len(buffer) + headerLength
	binary.LittleEndian.PutUint32(tmpHeader[BindDataTotalLengthOffset:], uint32(totalLength))
	dataBuffer := make([]byte, totalLength)
	copy(dataBuffer, tmpHeader)
	copy(dataBuffer[headerLength:], buffer)
	return dataBuffer, nil
}

func checkAllNull(data []driver.Value) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != nil {
			return false
		}
	}
	return true
}

func generateBindQueryData(data driver.Value) ([]byte, error) {
	var colType uint32
	var haveLength = false
	var length = 0
	var buf []byte
	switch v := data.(type) {
	case string:
		colType = common.TSDB_DATA_TYPE_BINARY
		haveLength = true
		length = len(v)
		buf = make([]byte, length)
		copy(buf, v)
	case []byte:
		colType = common.TSDB_DATA_TYPE_BINARY
		haveLength = true
		length = len(v)
		buf = make([]byte, length)
		copy(buf, v)
	case int8:
		colType = common.TSDB_DATA_TYPE_TINYINT
		buf = make([]byte, 1)
		buf[0] = byte(v)
	case int16:
		colType = common.TSDB_DATA_TYPE_SMALLINT
		buf = make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(v))
	case int32:
		colType = common.TSDB_DATA_TYPE_INT
		buf = make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(v))
	case int64:
		colType = common.TSDB_DATA_TYPE_BIGINT
		buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v))
	case uint8:
		colType = common.TSDB_DATA_TYPE_UTINYINT
		buf = make([]byte, 1)
		buf[0] = byte(v)
	case uint16:
		colType = common.TSDB_DATA_TYPE_USMALLINT
		buf = make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, v)
	case uint32:
		colType = common.TSDB_DATA_TYPE_UINT
		buf = make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, v)
	case uint64:
		colType = common.TSDB_DATA_TYPE_UBIGINT
		buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, v)
	case float32:
		colType = common.TSDB_DATA_TYPE_FLOAT
		buf = make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
	case float64:
		colType = common.TSDB_DATA_TYPE_DOUBLE
		buf = make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
	case bool:
		colType = common.TSDB_DATA_TYPE_BOOL
		buf = make([]byte, 1)
		if v {
			buf[0] = 1
		} else {
			buf[0] = 0
		}
	case time.Time:
		buf = make([]byte, 0, 35)
		colType = common.TSDB_DATA_TYPE_BINARY
		haveLength = true
		buf = v.AppendFormat(buf, time.RFC3339Nano)
		length = len(buf)
	default:
		return nil, fmt.Errorf("unsupported type: %T", data)
	}
	headerLength := getBindDataHeaderLength(1, haveLength)
	totalLength := len(buf) + headerLength
	dataBuf := make([]byte, totalLength)
	// type
	binary.LittleEndian.PutUint32(dataBuf[BindDataTypeOffset:], colType)
	// num
	binary.LittleEndian.PutUint32(dataBuf[BindDataNumOffset:], 1)
	// is null
	dataBuf[BindDataIsNullOffset] = 0
	// has length
	if haveLength {
		dataBuf[BindDataIsNullOffset+1] = 1
		binary.LittleEndian.PutUint32(dataBuf[BindDataIsNullOffset+2:], uint32(length))

	}
	// bufferLength
	binary.LittleEndian.PutUint32(dataBuf[headerLength-4:], uint32(len(buf)))
	copy(dataBuf[headerLength:], buf)
	binary.LittleEndian.PutUint32(dataBuf[BindDataTotalLengthOffset:], uint32(totalLength))
	return dataBuf, nil
}

func writeUint64(buffer *bytes.Buffer, v uint64) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
	buffer.WriteByte(byte(v >> 32))
	buffer.WriteByte(byte(v >> 40))
	buffer.WriteByte(byte(v >> 48))
	buffer.WriteByte(byte(v >> 56))
}

func writeUint32(buffer *bytes.Buffer, v uint32) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
	buffer.WriteByte(byte(v >> 16))
	buffer.WriteByte(byte(v >> 24))
}

func writeUint16(buffer *bytes.Buffer, v uint16) {
	buffer.WriteByte(byte(v))
	buffer.WriteByte(byte(v >> 8))
}

func needLength(colType int8) bool {
	switch colType {
	case common.TSDB_DATA_TYPE_BINARY,
		common.TSDB_DATA_TYPE_NCHAR,
		common.TSDB_DATA_TYPE_JSON,
		common.TSDB_DATA_TYPE_VARBINARY,
		common.TSDB_DATA_TYPE_GEOMETRY,
		common.TSDB_DATA_TYPE_BLOB,
		common.TSDB_DATA_TYPE_DECIMAL,
		common.TSDB_DATA_TYPE_DECIMAL64:
		return true
	}
	return false
}

type Stmt2AllField struct {
	Name      string `json:"name"`
	FieldType int8   `json:"field_type"`
	Precision uint8  `json:"precision"`
	Scale     uint8  `json:"scale"`
	Bytes     int32  `json:"bytes"`
	BindType  int8   `json:"bind_type"`
}
