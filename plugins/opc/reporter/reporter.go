package reporter

import (
	"collector/common"
	"context"
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
	Report(ctx context.Context, routineId int, values []*common.NodeValue) error
	Close()
}

// ArrowReporter is a reporter that sends data to taosx
type ArrowReporter struct {
	address         *net.TCPAddr
	valueTypeSchema sync.Map // key- valueType, value - *schema
	schemaLock      sync.Mutex
	schemaWriter    sync.Map // key-g-routine id with node id, value *ipc.Writer The *ipc.Writer is not thread-safe!
	writerLock      sync.Mutex
	debug           bool
	counter         atomic.Uint64
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
	reporter := ArrowReporter{address: address, debug: config.Debug}
	return &reporter, nil
}

func (r *ArrowReporter) Report(_ context.Context, routineId int, values []*common.NodeValue) error {
	if len(values) == 0 {
		return nil
	}

	schema, err := r.getSchemaByValueType(values[0].ValueType)
	if err != nil {
		log.Println("## get schema error", "error", err)
		return err
	}

	record, err := r.packData(values, schema)
	if err != nil {
		log.Println("## pack data error", "error", err)
		return fmt.Errorf("pack data error %v", err)
	}
	defer record.Release()

	if err = r.write(routineId, schema, record); err != nil {
		log.Println("## write data error", "error", err)
		return fmt.Errorf("write data error %v", err)
	}

	return nil
}

func (r *ArrowReporter) Close() {
	log.Println("## close reporter")
	r.schemaWriter.Range(func(key, value any) bool {
		writer := value.(*ipc.Writer)
		_ = writer.Close()
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

func (r *ArrowReporter) getSchemaByValueType(valueType common.ValueType) (*arrow.Schema, error) {
	if schema, ok := r.valueTypeSchema.Load(valueType); ok {
		return schema.(*arrow.Schema), nil
	}

	r.schemaLock.Lock()
	defer r.schemaLock.Unlock()

	if schema, ok := r.valueTypeSchema.Load(valueType); ok {
		return schema.(*arrow.Schema), nil
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
	r.valueTypeSchema.Store(valueType, schema)

	return schema, nil
}

func (r *ArrowReporter) write(routineID int, schema *arrow.Schema, record arrow.Record) (err error) {
	writer, err := r.getWriter(routineID, schema)
	if err != nil {
		log.Println("## get writer error", "error", err)
		return fmt.Errorf("get arrow writer error %v", err)
	}

	// ipc writer is not thread safe, so we need to get a writer for each routine.
	// and the ipc writer should be used in single routine.
	if err = writer.Write(record); err != nil {
		log.Println("## write record error", "error", err)
		return fmt.Errorf("report data error %v", err)
	}

	if r.debug {
		j, _ := record.MarshalJSON()
		log.Printf("report to taosx by writer [%p] values [%s]", writer, string(j))

		if r.counter.Load() > math.MaxUint64-uint64(record.NumRows()) {
			r.counter.Store(0)
		}
		r.counter.Add(uint64(record.NumRows()))
		log.Printf("## [%d] record already sent", r.counter.Load())
	}

	return nil
}

func (r *ArrowReporter) getWriter(routineID int, schema *arrow.Schema) (writer *ipc.Writer, err error) {
	// ipc writer is not thread safe, so we need to get a writer for each routine.
	writerKey := getWriterKey(routineID, schema)
	if writer, ok := r.schemaWriter.Load(writerKey); ok {
		return writer.(*ipc.Writer), nil
	}

	r.writerLock.Lock()
	defer r.writerLock.Unlock()

	if writer, ok := r.schemaWriter.Load(writerKey); ok {
		return writer.(*ipc.Writer), nil
	}

	conn, err := net.DialTCP("tcp", nil, r.address)
	if err != nil {
		return nil, fmt.Errorf("dial tcp error %v", err)
	}
	if r.debug {
		log.Printf("## create connection for routine [%d] and schema [%s] to taosx %s", routineID,
			schema.Fingerprint(), r.address.String())
	}

	writer = ipc.NewWriter(conn, ipc.WithSchema(schema))
	r.schemaWriter.Store(writerKey, writer)
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

func getWriterKey(routineID int, schema *arrow.Schema) string {
	return fmt.Sprintf("%d-%s", routineID, schema.Fingerprint())
}
