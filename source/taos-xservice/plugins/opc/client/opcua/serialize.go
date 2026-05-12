package opcua

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/gopcua/opcua/ua"
)

// serializePropertyValue 把 OPC UA Variant.Value() 返回的 interface{} 序列化为字符串，
// 用于写入 Point.Properties map（最终落入 TDengine VARCHAR(1024) Tag）。
//
// 序列化策略：
//   - nil                                → 空字符串
//   - bool / 数值 / string                → fmt.Sprintf("%v", v)，保留原值
//   - time.Time                          → RFC3339Nano 字符串
//   - ua.LocalizedText                   → 取 Text 字段（剥掉 Locale）
//   - 其余 struct / slice / array / map → json.Marshal（复杂类型 JSON 字符串化）
//
// 截断：调用方负责（写入 Tag 时由 TDengine 截断到 VARCHAR(1024)）。
func serializePropertyValue(v interface{}) (string, error) {
	if v == nil {
		return "", nil
	}
	// 已知友好类型先特判，避免 reflect 路径走到结构体时输出难读的 JSON
	switch tv := v.(type) {
	case string:
		return tv, nil
	case time.Time:
		return tv.Format(time.RFC3339Nano), nil
	case *time.Time:
		if tv == nil {
			return "", nil
		}
		return tv.Format(time.RFC3339Nano), nil
	case *ua.LocalizedText:
		if tv == nil {
			return "", nil
		}
		return tv.Text, nil
	case ua.LocalizedText:
		return tv.Text, nil
	case *ua.QualifiedName:
		if tv == nil {
			return "", nil
		}
		return tv.Name, nil
	case ua.QualifiedName:
		return tv.Name, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return "", nil
		}
		return serializePropertyValue(rv.Elem().Interface())
	case reflect.Struct, reflect.Slice, reflect.Array, reflect.Map:
		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("marshal property value: %w", err)
		}
		return string(b), nil
	default:
		// bool / 整数 / 浮点 / uintptr / chan / func 等
		return fmt.Sprintf("%v", v), nil
	}
}
