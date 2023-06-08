package reporter

import (
	"collector/common"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/sunpe/gobox/logger"
)

type Reporter interface {
	Report(ctx context.Context, value <-chan *common.NodeValue) error
	Stop(ctx context.Context)
}

func NewDataReporter(config common.Config) (Reporter, error) {
	address, err := net.ResolveTCPAddr("tcp", config.Report.Remote)
	if err != nil {
		return nil, fmt.Errorf("create opc reporter error. %v", err)
	}

	r := DataReporter{
		debug:        config.Debug,
		address:      address,
		batchSize:    config.Report.BatchSize,
		batchTimeout: time.Duration(config.Report.BatchTimeout) * time.Second,
		concurrent:   config.Report.Concurrent,
	}
	return &r, nil
}

type DataReporter struct {
	debug         bool
	address       *net.TCPAddr
	batchSize     int
	batchTimeout  time.Duration
	concurrent    int
	wait          sync.WaitGroup
	once          sync.Once
	valueChannels sync.Map // key - valueType, value - value channel
	writers       []writer
}

func (r *DataReporter) Report(ctx context.Context, ch <-chan *common.NodeValue) error {
	defer func() {
		logger.Debug("## read value is done.")

		r.valueChannels.Range(func(key, value any) bool {
			ch := value.(chan *common.NodeValue)
			close(ch)
			return true
		})
	}()

	for value := range ch {
		_valueCh, loaded := r.valueChannels.LoadOrStore(value.ValueType, make(chan *common.NodeValue, r.batchSize))
		valueCh := _valueCh.(chan *common.NodeValue)

		// create writer when first time
		if !loaded {
			writers, err := r.createWriters(value.ValueType, valueCh)
			if err != nil {
				logger.ErrorF("## create writer error. %v", err)
				return err
			}
			r.writers = append(r.writers, writers...)
			r.startWriters(ctx, writers)
		}

		valueCh <- value
	}
	return nil
}

func (r *DataReporter) startWriters(ctx context.Context, writers []writer) {
	for _, w := range writers {
		r.wait.Add(1)
		go func(w writer) {
			defer r.wait.Done()
			if err := w.write(ctx); err != nil {
				logger.PanicF("## write error. %v", err)
			}
		}(w)
	}
}

func (r *DataReporter) createWriters(valueType common.ValueType, ch chan *common.NodeValue) (writers []writer, err error) {
	logger.DebugF("## create %d writer for %s", r.concurrent, valueType)
	for i := 0; i < r.concurrent; i++ {
		w, err := r.createWriter(valueType, ch)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}
	return
}

func (r *DataReporter) createWriter(valueType common.ValueType, ch chan *common.NodeValue) (writer, error) {
	schema, err := getSchema(valueType)
	if err != nil {
		return nil, fmt.Errorf("create writer error. %v", err)
	}
	f, err := getAppendFunc(valueType)
	if err != nil {
		return nil, fmt.Errorf("create writer error. %v", err)
	}
	return NewArrowWriter(r.address, r.debug, r.batchSize, r.batchTimeout, schema, f, ch)
}

func (r *DataReporter) Stop(ctx context.Context) {
	r.once.Do(func() {
		defer r.wait.Wait()

		for _, w := range r.writers {
			if w == nil {
				continue
			}
			_ = w.close(ctx)
		}

		logger.Info("## opc reporter is stopping...")
	})
}

func packData(values []*common.NodeValue, schema *arrow.Schema, valueFunc appendFunc) (arrow.Record, error) {
	recordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer recordBuilder.Release()

	field0 := recordBuilder.Field(0).(*array.StringBuilder)    // id
	field1 := recordBuilder.Field(1).(*array.StringBuilder)    // name
	field2 := recordBuilder.Field(2).(*array.TimestampBuilder) // ts
	field3 := recordBuilder.Field(3).(*array.TimestampBuilder) // now
	field4 := recordBuilder.Field(4)                           // value
	field5 := recordBuilder.Field(5).(*array.Int64Builder)     // status

	defer func(fields ...array.Builder) {
		for _, field := range fields {
			field.Release()
		}
	}(field0, field1, field2, field3, field4, field5)

	for _, value := range values {
		field0.Append(value.Identifier)                             // id
		field1.Append(value.Name)                                   // name
		field2.Append(arrow.Timestamp(value.Timestamp.UnixMilli())) // ts
		field3.Append(arrow.Timestamp(value.Now.UnixMilli()))       //now

		if value.Value == nil {
			field4.AppendNull() // value
		} else if err := valueFunc(field4, value.Value); err != nil { // value
			return nil, fmt.Errorf("append value field error %v", err)
		}

		field5.Append(value.Status) // status
	}

	return recordBuilder.NewRecord(), nil
}

var meta = arrow.MetadataFrom(map[string]string{
	"version": "1.0",
	"stream":  "point",
	"ack":     "none",
})

var (
	valueTypeSchema sync.Map // key- valueType, value - *schema
	schemaLock      sync.Mutex
)

func getSchema(valueType common.ValueType) (*arrow.Schema, error) {
	if schema, ok := valueTypeSchema.Load(valueType); ok {
		return schema.(*arrow.Schema), nil
	}

	schemaLock.Lock()
	defer schemaLock.Unlock()

	if schema, ok := valueTypeSchema.Load(valueType); ok {
		return schema.(*arrow.Schema), nil
	}
	dataType, err := getDataType(valueType)
	if err != nil {
		return nil, err
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond}},       // server timestamp
			{Name: "received", Type: &arrow.TimestampType{Unit: arrow.Millisecond}}, // client timestamp
			{Name: "value", Type: dataType},
			{Name: "status", Type: arrow.PrimitiveTypes.Int64},
		},
		&meta,
	)
	valueTypeSchema.Store(valueType, schema)

	return schema, nil
}

func getDataType(valueType common.ValueType) (arrow.DataType, error) {
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
