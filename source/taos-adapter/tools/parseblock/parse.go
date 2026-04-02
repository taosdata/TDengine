package parseblock

import (
	"database/sql/driver"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/tools"
)

func ParseBlock(data []byte, colTypes []uint8, rows int, precision int) (uint64, [][]driver.Value, error) {
	p0 := unsafe.Pointer(&data[0])
	id := *(*uint64)(p0)
	columnPtr := tools.AddPointer(p0, 8)
	result, err := parser.ReadBlock(columnPtr, rows, colTypes, precision)
	if err != nil {
		return 0, nil, err
	}
	return id, result, nil
}

func ParseTmqBlock(data []byte, colTypes []uint8, rows int, precision int) (uint64, uint64, [][]driver.Value, error) {
	p0 := unsafe.Pointer(&data[0])
	id := *(*uint64)(p0)
	messageID := *(*uint64)(tools.AddPointer(p0, 8))
	columnPtr := tools.AddPointer(p0, 16)
	result, err := parser.ReadBlock(columnPtr, rows, colTypes, precision)
	return id, messageID, result, err
}
