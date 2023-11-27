package types

import "strconv"

type ValueType int

const (
	TIMESTAMP ValueType = iota + 1
	BOOL
	INT8
	UINT8
	INT16
	UINT16
	INT32
	UINT32
	INT64
	UINT64
	Float
	DOUBLE
	STRING
	All
)

func (vt ValueType) String() string {
	switch vt {
	case TIMESTAMP:
		return "TIMESTAMP"
	case BOOL:
		return "BOOL"
	case INT8:
		return "INT8"
	case UINT8:
		return "UINT8"
	case INT16:
		return "INT16"
	case UINT16:
		return "UINT16"
	case INT32:
		return "INT32"
	case UINT32:
		return "UINT32"
	case INT64:
		return "INT64"
	case UINT64:
		return "UINT64"
	case Float:
		return "Float"
	case DOUBLE:
		return "DOUBLE"
	case STRING:
		return "STRING"
	default:
		return "UNKNOWN:" + strconv.Itoa(int(vt))
	}
}

func GetValueType(value interface{}) ValueType {
	switch value.(type) {
	case bool:
		return BOOL
	case int8:
		return INT8
	case uint8:
		return UINT8
	case int16:
		return INT16
	case uint16:
		return UINT16
	case int32:
		return INT32
	case uint32:
		return UINT32
	case int64:
		return INT64
	case uint64:
		return UINT64
	case float32:
		return Float
	case float64:
		return DOUBLE
	case string:
		return STRING
	default:
		return -1
	}
}

func (vt ValueType) IsValid() bool {
	return vt > 0 && vt < All
}
