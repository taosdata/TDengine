package param

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/types"
)

func TestColumnType_AddBool(t *testing.T) {
	colType := NewColumnType(1)
	colType.AddBool()

	expected := []*types.ColumnType{
		{
			Type: types.TaosBoolType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddBool()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddTinyint(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddTinyint()

	expected := []*types.ColumnType{
		{
			Type: types.TaosTinyintType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddTinyint()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddSmallint(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddSmallint()

	expected := []*types.ColumnType{
		{
			Type: types.TaosSmallintType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddSmallint()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddInt(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddInt()

	expected := []*types.ColumnType{
		{
			Type: types.TaosIntType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddInt()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddBigint(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddBigint()

	expected := []*types.ColumnType{
		{
			Type: types.TaosBigintType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddBigint()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddUTinyint(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddUTinyint()

	expected := []*types.ColumnType{
		{
			Type: types.TaosUTinyintType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddUTinyint()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddUSmallint(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddUSmallint()

	expected := []*types.ColumnType{
		{
			Type: types.TaosUSmallintType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddUSmallint()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddUInt(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddUInt()

	expected := []*types.ColumnType{
		{
			Type: types.TaosUIntType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddUInt()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddUBigint(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddUBigint()

	expected := []*types.ColumnType{
		{
			Type: types.TaosUBigintType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddUBigint()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddFloat(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddFloat()

	expected := []*types.ColumnType{
		{
			Type: types.TaosFloatType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddFloat()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddDouble(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddDouble()

	expected := []*types.ColumnType{
		{
			Type: types.TaosDoubleType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddDouble()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddBinary(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddBinary(100)

	expected := []*types.ColumnType{
		{
			Type:   types.TaosBinaryType,
			MaxLen: 100,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddBinary(50)

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddVarBinary(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddVarBinary(100)

	expected := []*types.ColumnType{
		{
			Type:   types.TaosVarBinaryType,
			MaxLen: 100,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddVarBinary(50)
	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddNchar(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddNchar(100)

	expected := []*types.ColumnType{
		{
			Type:   types.TaosNcharType,
			MaxLen: 100,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddNchar(50)

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddTimestamp(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddTimestamp()

	expected := []*types.ColumnType{
		{
			Type: types.TaosTimestampType,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddTimestamp()

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddJson(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddJson(100)

	expected := []*types.ColumnType{
		{
			Type:   types.TaosJsonType,
			MaxLen: 100,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddJson(50)

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_AddGeometry(t *testing.T) {
	colType := NewColumnType(1)

	colType.AddGeometry(100)

	expected := []*types.ColumnType{
		{
			Type:   types.TaosGeometryType,
			MaxLen: 100,
		},
	}

	values, err := colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	colType.AddGeometry(50)

	values, err = colType.GetValue()
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func TestColumnType_GetValue(t *testing.T) {
	// Initialize ColumnType with size 3
	colType := NewColumnType(3)

	// Add column types
	colType.AddBool()
	colType.AddTinyint()
	colType.AddFloat()

	// Try to get values
	values, err := colType.GetValue()
	assert.NoError(t, err)

	// Check if the length of values matches the expected size
	expectedSize := 3
	assert.Equal(t, expectedSize, len(values))

	// Initialize ColumnType with size 3
	colType = NewColumnType(3)

	// Add only 2 column types
	colType.AddBool()
	colType.AddTinyint()

	// Try to get values
	_, err = colType.GetValue()

	// Check if an error is returned due to incomplete column
	assert.Error(t, err)
	assert.Equal(t, "incomplete column expect 3 columns set 2 columns", err.Error())
}

func TestNewColumnTypeWithValue(t *testing.T) {
	value := []*types.ColumnType{
		{Type: types.TaosBoolType},
		{Type: types.TaosTinyintType},
	}

	colType := NewColumnTypeWithValue(value)

	expectedSize := len(value)
	assert.Equal(t, expectedSize, colType.size)

	expectedValue := value
	assert.Equal(t, expectedValue, colType.value)

	expectedColumn := len(value)
	assert.Equal(t, expectedColumn, colType.column)
}
