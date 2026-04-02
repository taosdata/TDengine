package param

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	taosTypes "github.com/taosdata/taosadapter/v3/driver/types"
)

func TestParam_SetBool(t *testing.T) {
	param := NewParam(1)
	param.SetBool(0, true)

	expected := []driver.Value{taosTypes.TaosBool(true)}
	assert.Equal(t, expected, param.GetValues())

	param = NewParam(0)
	param.SetBool(0, true)
	assert.Equal(t, 0, len(param.GetValues()))
}

func TestParam_SetNull(t *testing.T) {
	param := NewParam(1)
	param.SetNull(0)

	if param.GetValues()[0] != nil {
		t.Errorf("SetNull failed, expected nil, got %v", param.GetValues()[0])
	}
	param = NewParam(0)
	param.SetNull(0)
	assert.Equal(t, 0, len(param.GetValues()))
}

func TestParam_SetTinyint(t *testing.T) {
	param := NewParam(1)
	param.SetTinyint(0, 42)

	expected := []driver.Value{taosTypes.TaosTinyint(42)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetTinyint(1, 42)                      // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetSmallint(t *testing.T) {
	param := NewParam(1)
	param.SetSmallint(0, 42)

	expected := []driver.Value{taosTypes.TaosSmallint(42)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetSmallint(1, 42)                     // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetInt(t *testing.T) {
	param := NewParam(1)
	param.SetInt(0, 42)

	expected := []driver.Value{taosTypes.TaosInt(42)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetInt(1, 42)                          // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetBigint(t *testing.T) {
	param := NewParam(1)
	param.SetBigint(0, 42)

	expected := []driver.Value{taosTypes.TaosBigint(42)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetBigint(1, 42)                       // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetUTinyint(t *testing.T) {
	param := NewParam(1)
	param.SetUTinyint(0, 42)

	expected := []driver.Value{taosTypes.TaosUTinyint(42)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetUTinyint(1, 42)                     // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetUSmallint(t *testing.T) {
	param := NewParam(1)
	param.SetUSmallint(0, 42)

	expected := []driver.Value{taosTypes.TaosUSmallint(42)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetUSmallint(1, 42)                    // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetUInt(t *testing.T) {
	param := NewParam(1)
	param.SetUInt(0, 42)

	expected := []driver.Value{taosTypes.TaosUInt(42)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetUInt(1, 42)                         // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetUBigint(t *testing.T) {
	param := NewParam(1)
	param.SetUBigint(0, 42)

	expected := []driver.Value{taosTypes.TaosUBigint(42)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetUBigint(1, 42)                      // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetFloat(t *testing.T) {
	param := NewParam(1)
	param.SetFloat(0, 3.14)

	expected := []driver.Value{taosTypes.TaosFloat(3.14)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetFloat(1, 3.14)                      // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetDouble(t *testing.T) {
	param := NewParam(1)
	param.SetDouble(0, 3.14)

	expected := []driver.Value{taosTypes.TaosDouble(3.14)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetDouble(1, 3.14)                     // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetBinary(t *testing.T) {
	param := NewParam(1)
	param.SetBinary(0, []byte{0x01, 0x02})

	expected := []driver.Value{taosTypes.TaosBinary([]byte{0x01, 0x02})}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetBinary(1, []byte{0x01, 0x02})       // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetVarBinary(t *testing.T) {
	param := NewParam(1)
	param.SetVarBinary(0, []byte{0x01, 0x02})

	expected := []driver.Value{taosTypes.TaosVarBinary([]byte{0x01, 0x02})}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetVarBinary(1, []byte{0x01, 0x02})    // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetNchar(t *testing.T) {
	param := NewParam(1)
	param.SetNchar(0, "hello")

	expected := []driver.Value{taosTypes.TaosNchar("hello")}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetNchar(1, "hello")                   // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetTimestamp(t *testing.T) {
	timestamp := time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC)
	param := NewParam(1)
	param.SetTimestamp(0, timestamp, 6)

	expected := []driver.Value{taosTypes.TaosTimestamp{T: timestamp, Precision: 6}}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetTimestamp(1, timestamp, 6)          // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetJson(t *testing.T) {
	jsonData := []byte(`{"key": "value"}`)
	param := NewParam(1)
	param.SetJson(0, jsonData)

	expected := []driver.Value{taosTypes.TaosJson(jsonData)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetJson(1, jsonData)                   // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_SetGeometry(t *testing.T) {
	geometryData := []byte{0x01, 0x02, 0x03, 0x04}
	param := NewParam(1)
	param.SetGeometry(0, geometryData)

	expected := []driver.Value{taosTypes.TaosGeometry(geometryData)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.SetGeometry(1, geometryData)           // Attempt to set at index 1 with size 1
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddBool(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a bool value
	param.AddBool(true)

	expected := []driver.Value{taosTypes.TaosBool(true), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another bool value
	param.AddBool(false)

	expected = []driver.Value{taosTypes.TaosBool(true), taosTypes.TaosBool(false)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddBool(true)                          // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddNull(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a null value
	param.AddNull()

	expected := []driver.Value{nil, nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another null value
	param.AddNull()

	expected = []driver.Value{nil, nil}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddNull()                              // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddTinyint(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a tinyint value
	param.AddTinyint(42)

	expected := []driver.Value{taosTypes.TaosTinyint(42), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another tinyint value
	param.AddTinyint(84)

	expected = []driver.Value{taosTypes.TaosTinyint(42), taosTypes.TaosTinyint(84)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddTinyint(126)                        // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddSmallint(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a smallint value
	param.AddSmallint(42)

	expected := []driver.Value{taosTypes.TaosSmallint(42), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another smallint value
	param.AddSmallint(84)

	expected = []driver.Value{taosTypes.TaosSmallint(42), taosTypes.TaosSmallint(84)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddSmallint(126)                       // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddInt(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add an int value
	param.AddInt(42)

	expected := []driver.Value{taosTypes.TaosInt(42), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another int value
	param.AddInt(84)

	expected = []driver.Value{taosTypes.TaosInt(42), taosTypes.TaosInt(84)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddInt(126)                            // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not mod
}

func TestParam_AddBigint(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a bigint value
	param.AddBigint(42)

	expected := []driver.Value{taosTypes.TaosBigint(42), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another bigint value
	param.AddBigint(84)

	expected = []driver.Value{taosTypes.TaosBigint(42), taosTypes.TaosBigint(84)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddBigint(126)                         // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddUTinyint(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a utinyint value
	param.AddUTinyint(42)

	expected := []driver.Value{taosTypes.TaosUTinyint(42), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another utinyint value
	param.AddUTinyint(84)

	expected = []driver.Value{taosTypes.TaosUTinyint(42), taosTypes.TaosUTinyint(84)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddUTinyint(126)                       // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddUSmallint(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a usmallint value
	param.AddUSmallint(42)

	expected := []driver.Value{taosTypes.TaosUSmallint(42), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another usmallint value
	param.AddUSmallint(84)

	expected = []driver.Value{taosTypes.TaosUSmallint(42), taosTypes.TaosUSmallint(84)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddUSmallint(126)                      // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddUInt(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a uint value
	param.AddUInt(42)

	expected := []driver.Value{taosTypes.TaosUInt(42), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another uint value
	param.AddUInt(84)

	expected = []driver.Value{taosTypes.TaosUInt(42), taosTypes.TaosUInt(84)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddUInt(126)                           // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddUBigint(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a ubigint value
	param.AddUBigint(42)

	expected := []driver.Value{taosTypes.TaosUBigint(42), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another ubigint value
	param.AddUBigint(84)

	expected = []driver.Value{taosTypes.TaosUBigint(42), taosTypes.TaosUBigint(84)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddUBigint(126)                        // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddFloat(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a float value
	param.AddFloat(3.14)

	expected := []driver.Value{taosTypes.TaosFloat(3.14), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another float value
	param.AddFloat(6.28)

	expected = []driver.Value{taosTypes.TaosFloat(3.14), taosTypes.TaosFloat(6.28)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddFloat(9.42)                         // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddDouble(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a double value
	param.AddDouble(3.14)

	expected := []driver.Value{taosTypes.TaosDouble(3.14), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another double value
	param.AddDouble(6.28)

	expected = []driver.Value{taosTypes.TaosDouble(3.14), taosTypes.TaosDouble(6.28)}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddDouble(9.42)                        // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddBinary(t *testing.T) {
	param := NewParam(2) // Initialize with size 2
	binaryData := []byte{0x01, 0x02, 0x03}

	// Add a binary value
	param.AddBinary(binaryData)

	expected := []driver.Value{taosTypes.TaosBinary(binaryData), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another binary value
	param.AddBinary([]byte{0x04, 0x05, 0x06})

	expected = []driver.Value{taosTypes.TaosBinary(binaryData), taosTypes.TaosBinary([]byte{0x04, 0x05, 0x06})}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddBinary([]byte{0x07, 0x08, 0x09})    // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddVarBinary(t *testing.T) {
	param := NewParam(2) // Initialize with size 2
	binaryData := []byte{0x01, 0x02, 0x03}

	// Add a varbinary value
	param.AddVarBinary(binaryData)

	expected := []driver.Value{taosTypes.TaosVarBinary(binaryData), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another varbinary value
	param.AddVarBinary([]byte{0x04, 0x05, 0x06})

	expected = []driver.Value{taosTypes.TaosVarBinary(binaryData), taosTypes.TaosVarBinary([]byte{0x04, 0x05, 0x06})}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddVarBinary([]byte{0x07, 0x08, 0x09}) // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddNchar(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add an nchar value
	param.AddNchar("hello")

	expected := []driver.Value{taosTypes.TaosNchar("hello"), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another nchar value
	param.AddNchar("world")

	expected = []driver.Value{taosTypes.TaosNchar("hello"), taosTypes.TaosNchar("world")}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddNchar("test")                       // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddTimestamp(t *testing.T) {
	timestamp := time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC)
	param := NewParam(2) // Initialize with size 2

	// Add a timestamp value
	param.AddTimestamp(timestamp, 6)

	expected := []driver.Value{taosTypes.TaosTimestamp{T: timestamp, Precision: 6}, nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another timestamp value
	param.AddTimestamp(timestamp.Add(time.Hour), 9)

	expected = []driver.Value{
		taosTypes.TaosTimestamp{T: timestamp, Precision: 6},
		taosTypes.TaosTimestamp{T: timestamp.Add(time.Hour), Precision: 9},
	}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddTimestamp(timestamp.Add(2*time.Hour), 6) // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues())      // Should not modify values
}

func TestParam_AddJson(t *testing.T) {
	jsonData := []byte(`{"key": "value"}`)
	param := NewParam(2) // Initialize with size 2

	// Add a JSON value
	param.AddJson(jsonData)

	expected := []driver.Value{taosTypes.TaosJson(jsonData), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another JSON value
	param.AddJson([]byte(`{"key2": "value2"}`))

	expected = []driver.Value{
		taosTypes.TaosJson(jsonData),
		taosTypes.TaosJson([]byte(`{"key2": "value2"}`)),
	}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddJson([]byte(`{"key3": "value3"}`))  // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddGeometry(t *testing.T) {
	geometryData := []byte{0x01, 0x02, 0x03}
	param := NewParam(2) // Initialize with size 2

	// Add a geometry value
	param.AddGeometry(geometryData)

	expected := []driver.Value{taosTypes.TaosGeometry(geometryData), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add another geometry value
	param.AddGeometry([]byte{0x04, 0x05, 0x06})

	expected = []driver.Value{
		taosTypes.TaosGeometry(geometryData),
		taosTypes.TaosGeometry([]byte{0x04, 0x05, 0x06}),
	}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddGeometry([]byte{0x07, 0x08, 0x09})  // Attempt to add at index 2 with size 2
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestParam_AddValue(t *testing.T) {
	param := NewParam(2) // Initialize with size 2

	// Add a binary value
	binaryData := []byte{0x01, 0x02, 0x03}
	param.AddValue(taosTypes.TaosBinary(binaryData))

	expected := []driver.Value{taosTypes.TaosBinary(binaryData), nil}
	assert.Equal(t, expected, param.GetValues())

	// Add a varchar value
	param.AddValue(taosTypes.TaosVarBinary("hello"))

	expected = []driver.Value{taosTypes.TaosBinary(binaryData), taosTypes.TaosVarBinary("hello")}
	assert.Equal(t, expected, param.GetValues())

	// Test when offset is out of range
	param.AddValue(taosTypes.TaosVarBinary("world"))
	assert.Equal(t, expected, param.GetValues()) // Should not modify values
}

func TestNewParamsWithRowValue(t *testing.T) {
	rowValues := []driver.Value{taosTypes.TaosBool(true), taosTypes.TaosInt(42), taosTypes.TaosNchar("hello")}

	params := NewParamsWithRowValue(rowValues)

	expected := []*Param{
		{
			size:   1,
			value:  []driver.Value{taosTypes.TaosBool(true)},
			offset: 1,
		},
		{
			size:   1,
			value:  []driver.Value{taosTypes.TaosInt(42)},
			offset: 1,
		},
		{
			size:   1,
			value:  []driver.Value{taosTypes.TaosNchar("hello")},
			offset: 1,
		},
	}

	for i, param := range params {
		assert.Equal(t, expected[i].size, param.size)
		assert.Equal(t, expected[i].value, param.value)
		assert.Equal(t, expected[i].offset, param.offset)
	}
}
