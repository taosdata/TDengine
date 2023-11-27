package types

import (
	"strconv"
	"testing"
)

func TestValueTypeString(t *testing.T) {
	tests := []struct {
		value    ValueType
		expected string
	}{
		{TIMESTAMP, "TIMESTAMP"},
		{BOOL, "BOOL"},
		{INT8, "INT8"},
		{UINT8, "UINT8"},
		{INT16, "INT16"},
		{UINT16, "UINT16"},
		{INT32, "INT32"},
		{UINT32, "UINT32"},
		{INT64, "INT64"},
		{UINT64, "UINT64"},
		{Float, "Float"},
		{DOUBLE, "DOUBLE"},
		{STRING, "STRING"},
		{All, "UNKNOWN:" + strconv.Itoa(int(All))},
	}

	for _, test := range tests {
		result := test.value.String()
		if result != test.expected {
			t.Errorf("For value %d, expected %s, but got %s", test.value, test.expected, result)
		}
	}
}

func TestGetValueType(t *testing.T) {
	tests := []struct {
		value    interface{}
		expected ValueType
	}{
		{true, BOOL},
		{int8(42), INT8},
		{uint8(42), UINT8},
		{int16(42), INT16},
		{uint16(42), UINT16},
		{int32(42), INT32},
		{uint32(42), UINT32},
		{int64(42), INT64},
		{uint64(42), UINT64},
		{float32(42.0), Float},
		{float64(42.0), DOUBLE},
		{"hello", STRING},
		{[]int{11, 2, 3}, -1},
	}

	for _, test := range tests {
		result := GetValueType(test.value)
		if result != test.expected {
			t.Errorf("For value %v, expected %s, but got %s", test.value, test.expected, result)
		}
	}
}

func TestIsValid(t *testing.T) {
	tests := []struct {
		value    ValueType
		expected bool
	}{
		{BOOL, true},
		{INT8, true},
		{UINT8, true},
		{INT16, true},
		{UINT16, true},
		{INT32, true},
		{UINT32, true},
		{INT64, true},
		{UINT64, true},
		{Float, true},
		{DOUBLE, true},
		{STRING, true},
		{All, false},
		{-1, false},
	}

	for _, test := range tests {
		result := test.value.IsValid()
		if result != test.expected {
			t.Errorf("For ValueType %d, expected %t, but got %t", test.value, test.expected, result)
		}
	}
}
