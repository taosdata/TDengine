package wrapper

/*
#include <taos.h>
*/
import "C"
import (
	"bytes"
	"reflect"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
)

type RowsHeader struct {
	ColNames   []string
	ColTypes   []uint8
	ColLength  []int64
	Precisions []int64
	Scales     []int64
}

func ReadColumn(result unsafe.Pointer, count int) (*RowsHeader, error) {
	if result == nil {
		return nil, &errors.TaosError{Code: 0xffff, ErrStr: "invalid result"}
	}
	rowsHeader := &RowsHeader{
		ColNames:   make([]string, count),
		ColTypes:   make([]uint8, count),
		ColLength:  make([]int64, count),
		Precisions: make([]int64, count),
		Scales:     make([]int64, count),
	}
	pFields := TaosFetchFieldsE(result)
	for i := 0; i < count; i++ {
		field := *(*C.struct_TAOS_FIELD_E)(unsafe.Pointer(uintptr(pFields) + uintptr(C.sizeof_struct_TAOS_FIELD_E*C.int(i))))
		buf := bytes.NewBufferString("")
		for _, c := range field.name {
			if c == 0 {
				break
			}
			buf.WriteByte(byte(c))
		}
		rowsHeader.ColNames[i] = buf.String()
		rowsHeader.ColTypes[i] = (uint8)(field._type)
		rowsHeader.ColLength[i] = int64((uint32)(field.bytes))
		rowsHeader.Precisions[i] = int64((uint8)(field.precision))
		rowsHeader.Scales[i] = int64((uint8)(field.scale))
	}
	return rowsHeader, nil
}

func (rh *RowsHeader) TypeDatabaseName(i int) string {
	return common.GetTypeName(int(rh.ColTypes[i]))
}

func (rh *RowsHeader) ScanType(i int) reflect.Type {
	t, exist := common.ColumnTypeMap[int(rh.ColTypes[i])]
	if !exist {
		return common.UnknownType
	}
	return t
}

func FetchLengths(res unsafe.Pointer, count int) []int {
	lengths := TaosFetchLengths(res)
	result := make([]int, count)
	for i := 0; i < count; i++ {
		result[i] = int(*(*C.int)(unsafe.Pointer(uintptr(lengths) + uintptr(C.sizeof_int*C.int(i)))))
	}
	return result
}
