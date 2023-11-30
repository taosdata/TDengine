package types

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
)

var allocator = memory.NewGoAllocator()

func TestAppend(t *testing.T) {
	tests := []struct {
		name        string
		value       interface{}
		vt          ValueType
		expectError bool
	}{
		{
			name:        "bool",
			value:       true,
			vt:          BOOL,
			expectError: false,
		},
		{
			name:        "wrong bool",
			value:       1,
			vt:          BOOL,
			expectError: true,
		},
		{
			name:        "int8",
			value:       int8(1),
			vt:          INT8,
			expectError: false,
		},
		{
			name:        "wrong int8",
			value:       1,
			vt:          INT8,
			expectError: true,
		},
		{
			name:        "uint8",
			value:       uint8(1),
			vt:          UINT8,
			expectError: false,
		},
		{
			name:        "wrong uint8",
			value:       1,
			vt:          UINT8,
			expectError: true,
		},
		{
			name:        "int16",
			value:       int16(1),
			vt:          INT16,
			expectError: false,
		},

		{
			name:  "wrong int16",
			value: 1,

			vt:          INT16,
			expectError: true,
		},
		{
			name:        "uint16",
			value:       uint16(1),
			vt:          UINT16,
			expectError: false,
		},
		{

			name:        "wrong uint16",
			value:       1,
			vt:          UINT16,
			expectError: true,
		},
		{
			name:        "int32",
			value:       int32(1),
			vt:          INT32,
			expectError: false,
		},
		{
			name:        "wrong int32",
			value:       1,
			vt:          INT32,
			expectError: true,
		},
		{
			name:        "uint32",
			value:       uint32(1),
			vt:          UINT32,
			expectError: false,
		},
		{
			name:        "wrong uint32",
			value:       1,
			vt:          UINT32,
			expectError: true,
		},
		{
			name:        "int64",
			value:       int64(1),
			vt:          INT64,
			expectError: false,
		},
		{
			name:        "wrong int64",
			value:       1,
			vt:          INT64,
			expectError: true,
		},
		{
			name:        "uint64",
			value:       uint64(1),
			vt:          UINT64,
			expectError: false,
		},
		{
			name:        "wrong uint64",
			value:       1,
			vt:          UINT64,
			expectError: true,
		},
		{
			name:        "float32",
			value:       float32(1),
			vt:          Float,
			expectError: false,
		},
		{
			name:        "wrong float32",
			value:       1,
			vt:          Float,
			expectError: true,
		},
		{
			name:        "float64",
			value:       float64(1),
			vt:          DOUBLE,
			expectError: false,
		},
		{
			name:        "wrong float64",
			value:       1,
			vt:          DOUBLE,
			expectError: true,
		},
		{
			name:        "string",
			value:       "1",
			vt:          STRING,
			expectError: false,
		},
		{
			name:        "wrong string",
			value:       1,
			vt:          STRING,
			expectError: true,
		},
		{
			name:        "[]byte",
			value:       []byte("1"),
			vt:          STRING,
			expectError: false,
		},
		{
			name:        "wrong []byte",
			value:       1,
			vt:          STRING,
			expectError: true,
		},
		{
			name:        "time",
			value:       time.Now(),
			vt:          TIMESTAMP,
			expectError: false,
		},
		{
			name:        "wrong time",
			value:       1,
			vt:          TIMESTAMP,
			expectError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reportType, exists := ReporterTypeMap[test.vt]
			if !exists {
				t.Fatal("not exists")
			}
			recordBuilder := array.NewRecordBuilder(allocator, reportType.Schema)
			err := reportType.AppendFunc(recordBuilder.Field(4), test.value)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

}
