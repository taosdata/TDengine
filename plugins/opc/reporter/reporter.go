package reporter

import (
	"collector/common"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type Reporter interface {
	Report(ctx context.Context, values []*common.NodeValue) error
	Close()
}

type ArrowReporter struct {
	address   *net.TCPAddr
	ipcWriter sync.Map // key-valueType, value- *schema
	debug     bool
	mutex     sync.Mutex
	counter   atomic.Uint64
}

var _ Reporter = (*ArrowReporter)(nil)

func NewArrowReporter(config common.Config) (*ArrowReporter, error) {
	if err := config.Report.Validate(); err != nil {
		return nil, err
	}
	address, err := net.ResolveTCPAddr("tcp", config.Report.Remote)
	if err != nil {
		return nil, fmt.Errorf("resolve remote address error %v", err)
	}
	return &ArrowReporter{address: address, debug: config.Debug}, nil
}

func (r *ArrowReporter) Report(_ context.Context, values []*common.NodeValue) error {
	if len(values) == 0 {
		return nil
	}
	if r.debug {
		j, _ := json.Marshal(values)
		log.Println("## Reporting to taosx", "values", string(j))
	}

	ws, err := r.getWriterAndSchemaByValueType(values[0].ValueType)
	if err != nil {
		log.Println("## get writer and schema error", "error", err)
		return err
	}

	record, err := r.packData(values, ws.schema)
	if err != nil {
		log.Println("## pack data error", "error", err)
		return fmt.Errorf("pack data error %v", err)
	}
	defer record.Release()

	if err = ws.writer.Write(record); err != nil {
		log.Println("## write record error", "error", err)
		return fmt.Errorf("report data error %v", err)
	}

	if r.debug {
		if r.counter.Load() > math.MaxUint64-uint64(record.NumRows()) {
			r.counter.Store(0)
		}
		r.counter.Add(uint64(record.NumRows()))
		log.Printf("## [%d] record already sent", r.counter.Load())

		for i, col := range record.Columns() {
			log.Printf("##  record column [%s] value [%v]", record.ColumnName(i), col)
		}
	}

	return nil
}

func (r *ArrowReporter) Close() {
	log.Println("## close reporter")
	r.ipcWriter.Range(func(key, value any) bool {
		ws := value.(*writerAndSchema)
		_ = ws.writer.Close()
		_ = ws.conn.Close()
		return true
	})
}

func (r *ArrowReporter) packData(values []*common.NodeValue, schema *arrow.Schema) (arrow.Record, error) {
	recordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer recordBuilder.Release()

	field0 := recordBuilder.Field(0).(*array.StringBuilder)
	defer field0.Release()
	field1 := recordBuilder.Field(1).(*array.TimestampBuilder)
	defer field1.Release()
	field2 := recordBuilder.Field(2)
	defer field2.Release()

	for _, value := range values {
		field0.Append(value.Identifier)
		field1.Append(arrow.Timestamp(value.Timestamp.UnixMilli()))
		if err := r.appendField(field2, value.ValueType, value.Value); err != nil {
			return nil, fmt.Errorf("append value field error %v", err)
		}
	}

	return recordBuilder.NewRecord(), nil
}

func (*ArrowReporter) appendField(builder array.Builder, valueType common.ValueType, value any) (err error) {
	f, err := getAppendFunc(valueType)
	if err != nil {
		return err
	}

	return f(builder, value)
}

var meta = arrow.MetadataFrom(map[string]string{
	"version": "1.0",
	"stream":  "point",
	"ack":     "none",
})

type writerAndSchema struct {
	writer *ipc.Writer
	conn   *net.TCPConn
	schema *arrow.Schema
}

func (r *ArrowReporter) getWriterAndSchemaByValueType(valueType common.ValueType) (ws *writerAndSchema, err error) {
	if sc, ok := r.ipcWriter.Load(valueType); ok {
		return sc.(*writerAndSchema), nil
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if sc, ok := r.ipcWriter.Load(valueType); ok {
		return sc.(*writerAndSchema), nil
	}

	dataType, err := r.getDataType(valueType)
	if err != nil {
		return nil, err
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond}},
			{Name: "value", Type: dataType},
		},
		&meta,
	)
	conn, err := net.DialTCP("tcp", nil, r.address)
	if err != nil {
		return nil, fmt.Errorf("conn to taosx error %v", err)
	}
	if r.debug {
		log.Println("## create connection for value type to taosx", "address", r.address.String(), "valueType",
			valueType)
	}
	writer := ipc.NewWriter(conn, ipc.WithSchema(schema))
	ws = &writerAndSchema{writer: writer, conn: conn, schema: schema}

	r.ipcWriter.Store(valueType, ws)
	return
}

func (*ArrowReporter) getDataType(valueType common.ValueType) (arrow.DataType, error) {
	switch valueType {
	case common.TIMESTAMP:
		return &arrow.TimestampType{}, nil
	case common.INT:
		return arrow.PrimitiveTypes.Int32, nil
	case common.INTUNSIGNED:
		return arrow.PrimitiveTypes.Uint32, nil
	case common.BIGINT:
		return arrow.PrimitiveTypes.Int64, nil
	case common.BIGINTUNSIGNED:
		return arrow.PrimitiveTypes.Uint64, nil
	case common.FLOAT:
		return arrow.PrimitiveTypes.Float32, nil
	case common.DOUBLE:
		return arrow.PrimitiveTypes.Float64, nil
	case common.BINARY,
		common.NCHAR,
		common.JSON,
		common.VARCHAR:
		return arrow.BinaryTypes.String, nil
	case common.SMALLINT:
		return arrow.PrimitiveTypes.Int16, nil
	case common.SMALLINTUNSIGNED:
		return arrow.PrimitiveTypes.Uint16, nil
	case common.TINYINT:
		return arrow.PrimitiveTypes.Int8, nil
	case common.TINYINTUNSIGNED:
		return arrow.PrimitiveTypes.Uint8, nil
	case common.BOOL:
		return &arrow.BooleanType{}, nil
	default:
		return arrow.BinaryTypes.String, fmt.Errorf("unsupported value type for reporter type %d", valueType)
	}
}

type appendFunc func(builder array.Builder, value any) error

func getAppendFunc(valueType common.ValueType) (appendFunc, error) {
	switch valueType {
	case common.TIMESTAMP:
		return appendTime, nil
	case common.INT:
		return appendInt32, nil
	case common.INTUNSIGNED:
		return appendUint32, nil
	case common.BIGINT:
		return appendInt64, nil
	case common.BIGINTUNSIGNED:
		return appendUint64, nil
	case common.FLOAT:
		return appendFloat32, nil
	case common.DOUBLE:
		return appendFloat64, nil
	case common.BINARY,
		common.NCHAR,
		common.JSON,
		common.VARCHAR:
		return appendString, nil
	case common.SMALLINT:
		return appendInt16, nil
	case common.SMALLINTUNSIGNED:
		return appendUInt16, nil
	case common.TINYINT:
		return appendInt8, nil
	case common.TINYINTUNSIGNED:
		return appendUint8, nil
	case common.BOOL:
		return appendBool, nil
	}
	return nil, fmt.Errorf("reporter unsupported value type for %d", valueType)
}

func appendBool(builder array.Builder, value any) error {
	v, err := common.Bool(value)
	if err != nil {
		return err
	}
	builder.(*array.BooleanBuilder).Append(v)
	return nil
}

func appendInt8(builder array.Builder, value any) error {
	v, err := common.TinyInt(value)
	if err != nil {
		return err
	}
	builder.(*array.Int8Builder).Append(v)
	return nil
}

func appendUint8(builder array.Builder, value any) error {
	v, err := common.TinyIntUnsigned(value)
	if err != nil {
		return err
	}
	builder.(*array.Uint8Builder).Append(v)
	return nil
}

func appendInt16(builder array.Builder, value any) error {
	v, err := common.SmallInt(value)
	if err != nil {
		return err
	}
	builder.(*array.Int16Builder).Append(v)
	return nil
}

func appendUInt16(builder array.Builder, value any) error {
	v, err := common.SmallIntUnsigned(value)
	if err != nil {
		return err
	}
	builder.(*array.Uint16Builder).Append(v)
	return nil
}

func appendInt32(builder array.Builder, value any) error {
	v, err := common.Int(value)
	if err != nil {
		return err
	}
	builder.(*array.Int32Builder).Append(int32(v))
	return nil
}

func appendUint32(builder array.Builder, value any) error {
	v, err := common.IntUnsigned(value)
	if err != nil {
		return err
	}
	builder.(*array.Uint32Builder).Append(uint32(v))
	return nil
}

func appendInt64(builder array.Builder, value any) error {
	v, err := common.BigInt(value)
	if err != nil {
		return err
	}
	builder.(*array.Int64Builder).Append(v)
	return nil
}

func appendUint64(builder array.Builder, value any) error {
	v, err := common.BigIntUnsigned(value)
	if err != nil {
		return err
	}
	builder.(*array.Uint64Builder).Append(v)
	return nil
}

func appendFloat32(builder array.Builder, value any) error {
	v, err := common.Float(value)
	if err != nil {
		return err
	}
	builder.(*array.Float32Builder).Append(v)
	return nil
}

func appendFloat64(builder array.Builder, value any) error {
	v, err := common.Double(value)
	if err != nil {
		return err
	}
	builder.(*array.Float64Builder).Append(v)
	return nil
}

func appendString(builder array.Builder, value any) error {
	v, err := common.String(value)
	if err != nil {
		return err
	}
	builder.(*array.StringBuilder).Append(v)
	return nil
}

func appendTime(builder array.Builder, value any) error {
	v, err := common.TimeStamp(value)
	if err != nil {
		return err
	}
	builder.(*array.TimestampBuilder).Append(arrow.Timestamp(v.UnixNano()))
	return nil
}
