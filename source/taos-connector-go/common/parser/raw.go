package parser

import (
	"fmt"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common/pointer"
)

type TMQRawDataParser struct {
	block  unsafe.Pointer
	offset uintptr
}

func NewTMQRawDataParser() *TMQRawDataParser {
	return &TMQRawDataParser{}
}

type TMQBlockInfo struct {
	RawBlock  unsafe.Pointer
	Precision int
	Schema    []*TMQRawDataSchema
	TableName string
}

type TMQRawDataSchema struct {
	ColType uint8
	Flag    int8
	Bytes   int64
	ColID   int
	Name    string
}

func (p *TMQRawDataParser) getTypeSkip(t int8) (int, error) {
	skip := 8
	switch t {
	case 1:
	case 2, 3:
		skip = 16
	default:
		return 0, fmt.Errorf("unknown type %d", t)
	}
	return skip, nil
}

func (p *TMQRawDataParser) skipHead() error {
	v := p.parseInt8()
	if v >= 100 {
		skip := p.parseInt32()
		p.skip(int(skip))
		return nil
	}
	skip, err := p.getTypeSkip(v)
	if err != nil {
		return err
	}
	p.skip(skip)
	v = p.parseInt8()
	skip, err = p.getTypeSkip(v)
	if err != nil {
		return err
	}
	p.skip(skip)
	return nil
}

func (p *TMQRawDataParser) skip(count int) {
	p.offset += uintptr(count)
}

func (p *TMQRawDataParser) parseBlockInfos() []*TMQBlockInfo {
	blockNum := p.parseInt32()
	blockInfos := make([]*TMQBlockInfo, blockNum)
	withTableName := p.parseBool()
	withSchema := p.parseBool()
	for i := int32(0); i < blockNum; i++ {
		blockInfo := &TMQBlockInfo{}
		blockTotalLen := p.parseVariableByteInteger()
		p.skip(17)
		blockInfo.Precision = int(p.parseUint8())
		blockInfo.RawBlock = pointer.AddUintptr(p.block, p.offset)
		p.skip(blockTotalLen - 18)
		if withSchema {
			cols := p.parseZigzagVariableByteInteger()
			//version
			_ = p.parseZigzagVariableByteInteger()

			blockInfo.Schema = make([]*TMQRawDataSchema, cols)
			for j := 0; j < cols; j++ {
				blockInfo.Schema[j] = p.parseSchema()
			}
		}
		if withTableName {
			blockInfo.TableName = p.parseName()
		}
		blockInfos[i] = blockInfo
	}
	return blockInfos
}

func (p *TMQRawDataParser) parseZigzagVariableByteInteger() int {
	return zigzagDecode(p.parseVariableByteInteger())
}

func (p *TMQRawDataParser) parseBool() bool {
	v := *(*int8)(pointer.AddUintptr(p.block, p.offset))
	p.skip(1)
	return v != 0
}

func (p *TMQRawDataParser) parseUint8() uint8 {
	v := *(*uint8)(pointer.AddUintptr(p.block, p.offset))
	p.skip(1)
	return v
}

func (p *TMQRawDataParser) parseInt8() int8 {
	v := *(*int8)(pointer.AddUintptr(p.block, p.offset))
	p.skip(1)
	return v
}

func (p *TMQRawDataParser) parseInt32() int32 {
	v := *(*int32)(pointer.AddUintptr(p.block, p.offset))
	p.skip(4)
	return v
}

func (p *TMQRawDataParser) parseSchema() *TMQRawDataSchema {
	colType := p.parseUint8()
	flag := p.parseInt8()
	bytes := int64(p.parseZigzagVariableByteInteger())
	colID := p.parseZigzagVariableByteInteger()
	name := p.parseName()
	return &TMQRawDataSchema{
		ColType: colType,
		Flag:    flag,
		Bytes:   bytes,
		ColID:   colID,
		Name:    name,
	}
}

func (p *TMQRawDataParser) parseName() string {
	nameLen := p.parseVariableByteInteger()
	name := make([]byte, nameLen-1)
	for i := 0; i < nameLen-1; i++ {
		name[i] = *(*byte)(pointer.AddUintptr(p.block, p.offset+uintptr(i)))
	}
	p.skip(nameLen)
	return string(name)
}

func (p *TMQRawDataParser) Parse(block unsafe.Pointer) ([]*TMQBlockInfo, error) {
	p.reset(block)
	err := p.skipHead()
	if err != nil {
		return nil, err
	}
	return p.parseBlockInfos(), nil
}

func (p *TMQRawDataParser) reset(block unsafe.Pointer) {
	p.block = block
	p.offset = 0
}

func (p *TMQRawDataParser) parseVariableByteInteger() int {
	multiplier := 1
	value := 0
	for {
		encodedByte := p.parseUint8()
		value += int(encodedByte&127) * multiplier
		if encodedByte&128 == 0 {
			break
		}
		multiplier *= 128
	}
	return value
}

func zigzagDecode(n int) int {
	return (n >> 1) ^ (-(n & 1))
}
