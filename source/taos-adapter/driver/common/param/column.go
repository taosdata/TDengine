package param

import (
	"fmt"

	"github.com/taosdata/taosadapter/v3/driver/types"
)

type ColumnType struct {
	size   int
	value  []*types.ColumnType
	column int
}

func NewColumnType(size int) *ColumnType {
	return &ColumnType{size: size, value: make([]*types.ColumnType, size)}
}

func NewColumnTypeWithValue(value []*types.ColumnType) *ColumnType {
	return &ColumnType{size: len(value), value: value, column: len(value)}
}

func (c *ColumnType) AddBool() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosBoolType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddTinyint() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosTinyintType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddSmallint() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosSmallintType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddInt() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosIntType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddBigint() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosBigintType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddUTinyint() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosUTinyintType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddUSmallint() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosUSmallintType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddUInt() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosUIntType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddUBigint() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosUBigintType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddFloat() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosFloatType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddDouble() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosDoubleType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddBinary(strMaxLen int) *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type:   types.TaosBinaryType,
		MaxLen: strMaxLen,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddVarBinary(strMaxLen int) *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type:   types.TaosVarBinaryType,
		MaxLen: strMaxLen,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddNchar(strMaxLen int) *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type:   types.TaosNcharType,
		MaxLen: strMaxLen,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddTimestamp() *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type: types.TaosTimestampType,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddJson(strMaxLen int) *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type:   types.TaosJsonType,
		MaxLen: strMaxLen,
	}
	c.column += 1
	return c
}

func (c *ColumnType) AddGeometry(strMaxLen int) *ColumnType {
	if c.column >= c.size {
		return c
	}
	c.value[c.column] = &types.ColumnType{
		Type:   types.TaosGeometryType,
		MaxLen: strMaxLen,
	}
	c.column += 1
	return c
}

func (c *ColumnType) GetValue() ([]*types.ColumnType, error) {
	if c.size != c.column {
		return nil, fmt.Errorf("incomplete column expect %d columns set %d columns", c.size, c.column)
	}
	return c.value, nil
}
