package types

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
)

type ReporterType struct {
	Schema     *arrow.Schema
	AppendFunc AppendFunc
}

var ReporterTypeMap = map[ValueType]*ReporterType{
	BOOL: {
		Schema:     arrow.NewSchema(newArrowFields(&arrow.BooleanType{}), &meta),
		AppendFunc: appendBool,
	},
	TIMESTAMP: {
		Schema:     arrow.NewSchema(newArrowFields(&arrow.TimestampType{Unit: arrow.Nanosecond}), &meta),
		AppendFunc: appendTime,
	},
	INT8: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Int8), &meta),
		AppendFunc: appendInt8,
	},
	UINT8: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Uint8), &meta),
		AppendFunc: appendUint8,
	},
	INT16: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Int16), &meta),
		AppendFunc: appendInt16,
	},
	UINT16: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Uint16), &meta),
		AppendFunc: appendUint16,
	},
	INT32: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Int32), &meta),
		AppendFunc: appendInt32,
	},
	UINT32: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Uint32), &meta),
		AppendFunc: appendUint32,
	},
	INT64: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Int64), &meta),
		AppendFunc: appendInt64,
	},
	UINT64: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Uint64), &meta),
		AppendFunc: appendUint64,
	},
	Float: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Float32), &meta),
		AppendFunc: appendFloat32,
	},
	DOUBLE: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.PrimitiveTypes.Float64), &meta),
		AppendFunc: appendFloat64,
	},
	STRING: {
		Schema:     arrow.NewSchema(newArrowFields(arrow.BinaryTypes.String), &meta),
		AppendFunc: appendString,
	},
}

func appendBool(builder array.Builder, value interface{}) error {
	v, is := value.(bool)
	if !is {
		return fmt.Errorf("value is not bool: %v", value)
	}
	builder.(*array.BooleanBuilder).Append(v)
	return nil
}

func appendInt8(builder array.Builder, value interface{}) error {
	v, is := value.(int8)
	if !is {
		return fmt.Errorf("value is not int8: %v", value)
	}
	builder.(*array.Int8Builder).Append(v)
	return nil
}

func appendUint8(builder array.Builder, value interface{}) error {
	v, is := value.(uint8)
	if !is {
		return fmt.Errorf("value is not uint8: %v", value)
	}
	builder.(*array.Uint8Builder).Append(v)
	return nil
}

func appendInt16(builder array.Builder, value interface{}) error {
	v, is := value.(int16)
	if !is {
		return fmt.Errorf("value is not int16: %v", value)
	}
	builder.(*array.Int16Builder).Append(v)
	return nil
}

func appendUint16(builder array.Builder, value interface{}) error {
	v, is := value.(uint16)
	if !is {
		return fmt.Errorf("value is not uint16: %v", value)
	}
	builder.(*array.Uint16Builder).Append(v)
	return nil
}

func appendInt32(builder array.Builder, value interface{}) error {
	v, is := value.(int32)
	if !is {
		return fmt.Errorf("value is not int32: %v", value)
	}
	builder.(*array.Int32Builder).Append(v)
	return nil
}

func appendUint32(builder array.Builder, value interface{}) error {
	v, is := value.(uint32)
	if !is {
		return fmt.Errorf("value is not uint32: %v", value)
	}
	builder.(*array.Uint32Builder).Append(v)
	return nil
}

func appendInt64(builder array.Builder, value interface{}) error {
	v, is := value.(int64)
	if !is {
		return fmt.Errorf("value is not int64: %v", value)
	}
	builder.(*array.Int64Builder).Append(v)
	return nil
}

func appendUint64(builder array.Builder, value interface{}) error {
	v, is := value.(uint64)
	if !is {
		return fmt.Errorf("value is not uint64: %v", value)
	}
	builder.(*array.Uint64Builder).Append(v)
	return nil
}

func appendFloat32(builder array.Builder, value interface{}) error {
	v, is := value.(float32)
	if !is {
		return fmt.Errorf("value is not float32: %v", value)
	}
	builder.(*array.Float32Builder).Append(v)
	return nil
}

func appendFloat64(builder array.Builder, value interface{}) error {
	v, is := value.(float64)
	if !is {
		return fmt.Errorf("value is not float64: %v", value)
	}
	builder.(*array.Float64Builder).Append(v)
	return nil
}

func appendString(builder array.Builder, value interface{}) error {
	switch v := value.(type) {
	case string:
		builder.(*array.StringBuilder).Append(v)
	case []byte:
		builder.(*array.StringBuilder).Append(*(*string)(unsafe.Pointer(&v)))
	default:
		return fmt.Errorf("value is not string: %v", value)
	}
	return nil
}

func appendTime(builder array.Builder, value interface{}) error {
	v, is := value.(time.Time)
	if !is {
		return fmt.Errorf("value is not time: %v", value)
	}
	builder.(*array.TimestampBuilder).Append(arrow.Timestamp(v.UnixNano()))
	return nil
}

type AppendFunc func(array.Builder, interface{}) error

func newArrowFields(dataType arrow.DataType) []arrow.Field {
	return []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond}},       // server timestamp
		{Name: "received", Type: &arrow.TimestampType{Unit: arrow.Millisecond}}, // client timestamp
		{Name: "value", Type: dataType, Nullable: true},
		{Name: "status", Type: arrow.PrimitiveTypes.Int64},
		{Name: "request", Type: &arrow.TimestampType{Unit: arrow.Millisecond}},
	}
}

var meta = arrow.MetadataFrom(map[string]string{
	"version": "1.0",
	"stream":  "point",
	"ack":     "lush",
})
