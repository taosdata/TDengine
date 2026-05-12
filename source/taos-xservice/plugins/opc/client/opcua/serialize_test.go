package opcua

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
)

func TestSerializePropertyValue_Nil(t *testing.T) {
	got, err := serializePropertyValue(nil)
	if err != nil || got != "" {
		t.Fatalf("nil: got %q, err=%v", got, err)
	}
}

func TestSerializePropertyValue_Primitives(t *testing.T) {
	cases := []struct {
		in   interface{}
		want string
	}{
		{"hello", "hello"},
		{int32(42), "42"},
		{int64(-7), "-7"},
		{float64(1.5), "1.5"},
		{true, "true"},
		{false, "false"},
		{uint16(8), "8"},
	}
	for _, c := range cases {
		got, err := serializePropertyValue(c.in)
		if err != nil {
			t.Fatalf("%v: err=%v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("%v: want %q, got %q", c.in, c.want, got)
		}
	}
}

func TestSerializePropertyValue_Time(t *testing.T) {
	ts := time.Date(2025, 6, 1, 12, 30, 45, 0, time.UTC)
	got, err := serializePropertyValue(ts)
	if err != nil || !strings.HasPrefix(got, "2025-06-01T12:30:45") {
		t.Fatalf("time: got %q, err=%v", got, err)
	}
}

func TestSerializePropertyValue_LocalizedText(t *testing.T) {
	lt := &ua.LocalizedText{Locale: "en-US", Text: "Engineering Units"}
	got, err := serializePropertyValue(lt)
	if err != nil || got != "Engineering Units" {
		t.Fatalf("LocalizedText: got %q, err=%v", got, err)
	}
	// 值类型也应可序列化
	got2, err := serializePropertyValue(ua.LocalizedText{Text: "X"})
	if err != nil || got2 != "X" {
		t.Fatalf("LocalizedText value: got %q, err=%v", got2, err)
	}
}

func TestSerializePropertyValue_QualifiedName(t *testing.T) {
	qn := &ua.QualifiedName{NamespaceIndex: 2, Name: "EURange"}
	got, err := serializePropertyValue(qn)
	if err != nil || got != "EURange" {
		t.Fatalf("QualifiedName: got %q, err=%v", got, err)
	}
}

// EUInformation 是 OPC UA 的工程单位结构体，验证复杂结构体走 JSON 路径
type fakeEUInformation struct {
	NamespaceURI string
	UnitId       int32
	DisplayName  string
	Description  string
}

func TestSerializePropertyValue_StructAsJSON(t *testing.T) {
	v := fakeEUInformation{
		NamespaceURI: "http://www.opcfoundation.org/UA/units/un/cefact",
		UnitId:       4408652,
		DisplayName:  "°C",
		Description:  "degree Celsius",
	}
	got, err := serializePropertyValue(v)
	if err != nil {
		t.Fatalf("struct: err=%v", err)
	}
	var roundTrip fakeEUInformation
	if err := json.Unmarshal([]byte(got), &roundTrip); err != nil {
		t.Fatalf("struct: not valid JSON: %v, raw=%q", err, got)
	}
	if roundTrip != v {
		t.Fatalf("struct: roundtrip mismatch want=%+v got=%+v", v, roundTrip)
	}
}

func TestSerializePropertyValue_SliceAsJSON(t *testing.T) {
	v := []float64{0.0, 100.0}
	got, err := serializePropertyValue(v)
	if err != nil {
		t.Fatalf("slice: err=%v", err)
	}
	if got != "[0,100]" {
		t.Fatalf("slice: want [0,100], got %q", got)
	}
}

func TestSerializePropertyValue_MapAsJSON(t *testing.T) {
	v := map[string]int{"a": 1}
	got, err := serializePropertyValue(v)
	if err != nil {
		t.Fatalf("map: err=%v", err)
	}
	if got != `{"a":1}` {
		t.Fatalf("map: got %q", got)
	}
}

func TestSerializePropertyValue_NilPtr(t *testing.T) {
	var p *time.Time
	got, err := serializePropertyValue(p)
	if err != nil || got != "" {
		t.Fatalf("nil ptr: got %q, err=%v", got, err)
	}
	var lt *ua.LocalizedText
	got, err = serializePropertyValue(lt)
	if err != nil || got != "" {
		t.Fatalf("nil LocalizedText: got %q, err=%v", got, err)
	}
}

func TestSerializePropertyValue_PtrToPrimitive(t *testing.T) {
	x := int32(42)
	got, err := serializePropertyValue(&x)
	if err != nil || got != "42" {
		t.Fatalf("ptr int: got %q, err=%v", got, err)
	}
}
