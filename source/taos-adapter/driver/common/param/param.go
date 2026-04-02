package param

import (
	"database/sql/driver"
	"time"

	taosTypes "github.com/taosdata/taosadapter/v3/driver/types"
)

type Param struct {
	size   int
	value  []driver.Value
	offset int
}

func NewParam(size int) *Param {
	return &Param{
		size:  size,
		value: make([]driver.Value, size),
	}
}

func NewParamsWithRowValue(value []driver.Value) []*Param {
	params := make([]*Param, len(value))
	for i, d := range value {
		params[i] = NewParam(1)
		params[i].AddValue(d)
	}
	return params
}

func (p *Param) SetBool(offset int, value bool) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosBool(value)
}

func (p *Param) SetNull(offset int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = nil
}

func (p *Param) SetTinyint(offset int, value int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosTinyint(value)
}

func (p *Param) SetSmallint(offset int, value int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosSmallint(value)
}

func (p *Param) SetInt(offset int, value int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosInt(value)
}

func (p *Param) SetBigint(offset int, value int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosBigint(value)
}

func (p *Param) SetUTinyint(offset int, value uint) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosUTinyint(value)
}

func (p *Param) SetUSmallint(offset int, value uint) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosUSmallint(value)
}

func (p *Param) SetUInt(offset int, value uint) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosUInt(value)
}

func (p *Param) SetUBigint(offset int, value uint) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosUBigint(value)
}

func (p *Param) SetFloat(offset int, value float32) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosFloat(value)
}

func (p *Param) SetDouble(offset int, value float64) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosDouble(value)
}

func (p *Param) SetBinary(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosBinary(value)
}

func (p *Param) SetVarBinary(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosVarBinary(value)
}

func (p *Param) SetNchar(offset int, value string) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosNchar(value)
}

func (p *Param) SetTimestamp(offset int, value time.Time, precision int) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosTimestamp{
		T:         value,
		Precision: precision,
	}
}

func (p *Param) SetJson(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosJson(value)
}

func (p *Param) SetGeometry(offset int, value []byte) {
	if offset >= p.size {
		return
	}
	p.value[offset] = taosTypes.TaosGeometry(value)
}

func (p *Param) AddBool(value bool) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosBool(value)
	p.offset += 1
	return p
}

func (p *Param) AddNull() *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = nil
	p.offset += 1
	return p
}

func (p *Param) AddTinyint(value int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosTinyint(value)
	p.offset += 1
	return p
}

func (p *Param) AddSmallint(value int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosSmallint(value)
	p.offset += 1
	return p
}

func (p *Param) AddInt(value int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosInt(value)
	p.offset += 1
	return p
}

func (p *Param) AddBigint(value int) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosBigint(value)
	p.offset += 1
	return p
}

func (p *Param) AddUTinyint(value uint) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosUTinyint(value)
	p.offset += 1
	return p
}

func (p *Param) AddUSmallint(value uint) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosUSmallint(value)
	p.offset += 1
	return p
}

func (p *Param) AddUInt(value uint) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosUInt(value)
	p.offset += 1
	return p
}

func (p *Param) AddUBigint(value uint) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosUBigint(value)
	p.offset += 1
	return p
}

func (p *Param) AddFloat(value float32) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosFloat(value)
	p.offset += 1
	return p
}

func (p *Param) AddDouble(value float64) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosDouble(value)
	p.offset += 1
	return p
}

func (p *Param) AddBinary(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosBinary(value)
	p.offset += 1
	return p
}

func (p *Param) AddVarBinary(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosVarBinary(value)
	p.offset += 1
	return p
}

func (p *Param) AddNchar(value string) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosNchar(value)
	p.offset += 1
	return p
}

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

func (p *Param) AddJson(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosJson(value)
	p.offset += 1
	return p
}

func (p *Param) AddGeometry(value []byte) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = taosTypes.TaosGeometry(value)
	p.offset += 1
	return p
}

func (p *Param) GetValues() []driver.Value {
	return p.value
}

func (p *Param) AddValue(value interface{}) *Param {
	if p.offset >= p.size {
		return p
	}
	p.value[p.offset] = value
	p.offset += 1
	return p
}
