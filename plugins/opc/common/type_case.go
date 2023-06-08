package common

import (
	"fmt"
	"math/big"
	"strings"
	"time"
	"unsafe"
)

type ValueType int

const (
	Invalid ValueType = iota
	TIMESTAMP
	INT
	INTUNSIGNED
	BIGINT
	BIGINTUNSIGNED
	FLOAT
	DOUBLE
	BINARY
	SMALLINT
	SMALLINTUNSIGNED
	TINYINT
	TINYINTUNSIGNED
	BOOL
	NCHAR
	JSON
	VARCHAR
)

func ValueTypeFromString(vt string) (ValueType, error) {
	if strings.Contains(vt, "(") { // deal with varchar(10) binary(10) nchar(10)... etc
		vt = strings.Split(vt, "(")[0]
	}
	vt = strings.ToUpper(vt)
	switch vt {
	case "TIMESTAMP":
		return TIMESTAMP, nil
	case "INT":
		return INT, nil
	case "INT UNSIGNED":
		return INTUNSIGNED, nil
	case "BIGINT":
		return BIGINT, nil
	case "BIGINT UNSIGNED":
		return BIGINTUNSIGNED, nil
	case "FLOAT":
		return FLOAT, nil
	case "DOUBLE":
		return DOUBLE, nil
	case "BINARY":
		return BINARY, nil
	case "SMALLINT":
		return SMALLINT, nil
	case "SMALLINT UNSIGNED":
		return SMALLINTUNSIGNED, nil
	case "TINYINT":
		return TINYINT, nil
	case "TINYINT UNSIGNED":
		return TINYINTUNSIGNED, nil
	case "BOOL":
		return BOOL, nil
	case "NCHAR":
		return NCHAR, nil
	case "JSON":
		return JSON, nil
	case "VARCHAR":
		return VARCHAR, nil
	default:
		return VARCHAR, fmt.Errorf("config error. unknown value type %s", vt)
	}
}

func (v ValueType) String() string {
	switch v {
	case TIMESTAMP:
		return "TIMESTAMP"
	case INT:
		return "INT"
	case INTUNSIGNED:
		return "INTUNSIGNED"
	case BIGINT:
		return "BIGINT"
	case BIGINTUNSIGNED:
		return "BIGINTUNSIGNED"
	case FLOAT:
		return "FLOAT"
	case DOUBLE:
		return "DOUBLE"
	case BINARY:
		return "BINARY"
	case SMALLINT:
		return "SMALLINT"
	case SMALLINTUNSIGNED:
		return "SMALLINTUNSIGNED"
	case TINYINT:
		return "TINYINT"
	case TINYINTUNSIGNED:
		return "TINYINTUNSIGNED"
	case BOOL:
		return "BOOL"
	case NCHAR:
		return "NCHAR"
	case JSON:
		return "JSON"
	case VARCHAR:
		return "VARCHAR"
	}
	return ""
}

var typeErrorTemplate = "%T cannot cast to %s"

func TimeStamp(v any) (time.Time, error) {
	switch v := v.(type) {
	case time.Time:
		return v, nil
	default:
		return time.Time{}, fmt.Errorf(typeErrorTemplate, v, "time")
	}
}

func Int(v any) (int, error) {
	switch v := v.(type) {
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "int32")
	}
}

func IntUnsigned(v any) (uint, error) {
	switch v := v.(type) {
	case uint8:
		return uint(v), nil
	case uint16:
		return uint(v), nil
	case uint:
		return v, nil
	case uint32:
		return uint(v), nil
	case uint64:
		return uint(v), nil
	case int8:
		return uint(v), nil
	case int16:
		return uint(v), nil
	case int:
		return uint(v), nil
	case int32:
		return uint(v), nil
	case int64:
		return uint(v), nil
	case float32:
		return uint(v), nil
	case float64:
		return uint(v), nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "uint32")
	}
}

func BigInt(v any) (int64, error) {
	switch v := v.(type) {
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "int32")
	}
}

func BigIntUnsigned(v any) (uint64, error) {
	switch v := v.(type) {
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "uint64")
	}
}

func Float(v any) (float32, error) {
	switch v := v.(type) {
	case int8:
		return float32(v), nil
	case int16:
		return float32(v), nil
	case int:
		return float32(v), nil
	case int32:
		return float32(v), nil
	case int64:
		return float32(v), nil
	case float32:
		return v, nil
	case float64:
		f, _ := big.NewFloat(v).Float32()
		return f, nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "float32")
	}
}

func Double(v any) (float64, error) {
	switch v := v.(type) {
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil // cast float32 to float64 will lose precision
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "float64")
	}
}

func Binary(v any) (string, error) {
	return String(v)
}

func SmallInt(v any) (int16, error) {
	switch v := v.(type) {
	case int8:
		return int16(v), nil
	case int16:
		return v, nil
	case int:
		return int16(v), nil
	case int32:
		return int16(v), nil
	case int64:
		return int16(v), nil
	case float32:
		return int16(v), nil
	case float64:
		return int16(v), nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "int16")
	}
}

func SmallIntUnsigned(v any) (uint16, error) {
	switch v := v.(type) {
	case uint8:
		return uint16(v), nil
	case uint16:
		return v, nil
	case uint:
		return uint16(v), nil
	case uint32:
		return uint16(v), nil
	case uint64:
		return uint16(v), nil
	case int8:
		return uint16(v), nil
	case int16:
		return uint16(v), nil
	case int:
		return uint16(v), nil
	case int32:
		return uint16(v), nil
	case int64:
		return uint16(v), nil
	case float32:
		return uint16(v), nil
	case float64:
		return uint16(v), nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "uint16")
	}
}

func TinyInt(v any) (int8, error) {
	switch v := v.(type) {
	case uint8:
		return int8(v), nil
	case uint:
		return int8(v), nil
	case uint16:
		return int8(v), nil
	case uint32:
		return int8(v), nil
	case uint64:
		return int8(v), nil
	case int8:
		return v, nil
	case int:
		return int8(v), nil
	case int16:
		return int8(v), nil
	case int32:
		return int8(v), nil
	case int64:
		return int8(v), nil
	case float32:
		return int8(v), nil
	case float64:
		return int8(v), nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "byte")
	}
}

func TinyIntUnsigned(v any) (uint8, error) {
	switch v := v.(type) {
	case uint8:
		return v, nil
	case uint:
		return uint8(v), nil
	case uint16:
		return uint8(v), nil
	case uint32:
		return uint8(v), nil
	case uint64:
		return uint8(v), nil
	case int8:
		return uint8(v), nil
	case int:
		return uint8(v), nil
	case int16:
		return uint8(v), nil
	case int32:
		return uint8(v), nil
	case int64:
		return uint8(v), nil
	case float32:
		return uint8(v), nil
	case float64:
		return uint8(v), nil
	default:
		return 0, fmt.Errorf(typeErrorTemplate, v, "byte")
	}
}

func Bool(v any) (bool, error) {
	switch v := v.(type) {
	case bool:
		return v, nil
	default:
		return false, fmt.Errorf(typeErrorTemplate, v, "bool")
	}
}

func NChar(v any) (string, error) {
	return String(v)
}

func Json(v any) (string, error) {
	return String(v)
}

func Bytes(v any) ([]byte, error) {
	switch v := v.(type) {
	case []byte:
		return v, nil
	case string:
		return *(*[]byte)(unsafe.Pointer(&v)), nil
	default:
		return nil, fmt.Errorf(typeErrorTemplate, v, "bytes array")
	}
}

func String(v any) (string, error) {
	switch v := v.(type) {
	case string:
		return v, nil
	case []byte:
		return *(*string)(unsafe.Pointer(&v)), nil
	default:
		return "", fmt.Errorf(typeErrorTemplate, v, "string")
	}
}
