package param

import (
	"database/sql/driver"
	"time"

	taosTypes "github.com/taosdata/driver-go/v3/types"
)

// Param holds positional bind values for stmt compatibility APIs.
//
// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
type Param struct {
	size   int
	value  []driver.Value
	offset int
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func NewParam(size int) *Param {
	return &Param{
		size:  size,
		value: make([]driver.Value, size),
	}
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func NewParamsWithRowValue(value []driver.Value) []*Param {
	params := make([]*Param, len(value))
	for i, d := range value {
		params[i] = NewParam(1)
		params[i].AddValue(d)
	}
	return params
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetBool(offset int, value bool) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosBool(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetNull(offset int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = nil
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetTinyint(offset int, value int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosTinyint(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetSmallint(offset int, value int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosSmallint(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetInt(offset int, value int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosInt(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetBigint(offset int, value int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosBigint(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetUTinyint(offset int, value uint) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosUTinyint(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetUSmallint(offset int, value uint) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosUSmallint(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetUInt(offset int, value uint) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosUInt(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetUBigint(offset int, value uint) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosUBigint(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetFloat(offset int, value float32) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosFloat(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetDouble(offset int, value float64) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosDouble(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetBinary(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosBinary(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetVarBinary(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosVarBinary(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetNchar(offset int, value string) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosNchar(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetTimestamp(offset int, value time.Time, precision int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosTimestamp{
		T:         value,
		Precision: precision,
	}
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetJson(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosJson(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetGeometry(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosGeometry(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetBlob(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosBlob(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) SetDecimal(offset int, value string) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosDecimal(value)
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddBool(value bool) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosBool(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddNull() *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = nil
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddTinyint(value int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosTinyint(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddSmallint(value int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosSmallint(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddInt(value int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosInt(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddBigint(value int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosBigint(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddUTinyint(value uint) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosUTinyint(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddUSmallint(value uint) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosUSmallint(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddUInt(value uint) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosUInt(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddUBigint(value uint) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosUBigint(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddFloat(value float32) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosFloat(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddDouble(value float64) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosDouble(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddBinary(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosBinary(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddVarBinary(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosVarBinary(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddNchar(value string) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosNchar(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddTimestamp(value time.Time, precision int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosTimestamp{
		T:         value,
		Precision: precision,
	}
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddJson(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosJson(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddGeometry(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosGeometry(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddBlob(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosBlob(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddDecimal(value string) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosDecimal(value)
	p.offset += 1
	return p
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) GetValues() []driver.Value {
	return p.value
}

// Deprecated: use []*commonstmt.TaosStmt2BindData with unified.Stmt.Bind instead.
func (p *Param) AddValue(value interface{}) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = value
	p.offset += 1
	return p
}
