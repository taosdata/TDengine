package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"bytes"
	"database/sql/driver"
	"errors"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/stmt"
	taosError "github.com/taosdata/driver-go/v3/errors"
	taosTypes "github.com/taosdata/driver-go/v3/types"
)

// TaosStmtInit TAOS_STMT *taos_stmt_init(TAOS *taos);
func TaosStmtInit(taosConnect unsafe.Pointer) unsafe.Pointer {
	return C.taos_stmt_init(taosConnect)
}

// TaosStmtInitWithReqID TAOS_STMT *taos_stmt_init_with_reqid(TAOS *taos, int64_t reqid);
func TaosStmtInitWithReqID(taosConn unsafe.Pointer, reqID int64) unsafe.Pointer {
	return C.taos_stmt_init_with_reqid(taosConn, (C.int64_t)(reqID))
}

// TaosStmtPrepare int        taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length);
func TaosStmtPrepare(stmt unsafe.Pointer, sql string) int {
	cSql := C.CString(sql)
	cLen := C.ulong(len(sql))
	defer C.free(unsafe.Pointer(cSql))
	return int(C.taos_stmt_prepare(stmt, cSql, cLen))
}

//typedef struct TAOS_MULTI_BIND {
//int       buffer_type;
//void     *buffer;
//int32_t   buffer_length;
//int32_t  *length;
//char     *is_null;
//int       num;
//} TAOS_MULTI_BIND;

// TaosStmtSetTags int        taos_stmt_set_tags(TAOS_STMT *stmt, TAOS_MULTI_BIND *tags);
func TaosStmtSetTags(stmt unsafe.Pointer, tags []driver.Value) int {
	if len(tags) == 0 {
		return int(C.taos_stmt_set_tags(stmt, nil))
	}
	binds, needFreePointer, err := generateTaosBindList(tags)
	defer func() {
		for _, pointer := range needFreePointer {
			C.free(pointer)
		}
	}()
	if err != nil {
		return -1
	}
	result := int(C.taos_stmt_set_tags(stmt, (*C.TAOS_MULTI_BIND)(&binds[0])))
	return result
}

// TaosStmtSetTBNameTags int        taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_MULTI_BIND* tags);
func TaosStmtSetTBNameTags(stmt unsafe.Pointer, name string, tags []driver.Value) int {
	cStr := C.CString(name)
	defer C.free(unsafe.Pointer(cStr))
	if len(tags) == 0 {
		return int(C.taos_stmt_set_tbname_tags(stmt, cStr, nil))
	}
	binds, needFreePointer, err := generateTaosBindList(tags)
	defer func() {
		for _, pointer := range needFreePointer {
			C.free(pointer)
		}
	}()
	if err != nil {
		return -1
	}
	result := int(C.taos_stmt_set_tbname_tags(stmt, cStr, (*C.TAOS_MULTI_BIND)(&binds[0])))
	return result
}

// TaosStmtSetTBName int        taos_stmt_set_tbname(TAOS_STMT* stmt, const char* name);
func TaosStmtSetTBName(stmt unsafe.Pointer, name string) int {
	cStr := C.CString(name)
	defer C.free(unsafe.Pointer(cStr))
	return int(C.taos_stmt_set_tbname(stmt, cStr))
}

// TaosStmtIsInsert int        taos_stmt_is_insert(TAOS_STMT *stmt, int *insert);
func TaosStmtIsInsert(stmt unsafe.Pointer) (is bool, errorCode int) {
	p := C.malloc(C.size_t(4))
	isInsert := (*C.int)(p)
	defer C.free(p)
	errorCode = int(C.taos_stmt_is_insert(stmt, isInsert))
	return int(*isInsert) == 1, errorCode
}

// TaosStmtNumParams int        taos_stmt_num_params(TAOS_STMT *stmt, int *nums);
func TaosStmtNumParams(stmt unsafe.Pointer) (count int, errorCode int) {
	p := C.malloc(C.size_t(4))
	num := (*C.int)(p)
	defer C.free(p)
	errorCode = int(C.taos_stmt_num_params(stmt, num))
	return int(*num), errorCode
}

// TaosStmtBindParam int        taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind);
func TaosStmtBindParam(stmt unsafe.Pointer, params []driver.Value) int {
	if len(params) == 0 {
		return int(C.taos_stmt_bind_param(stmt, nil))
	}
	binds, needFreePointer, err := generateTaosBindList(params)
	defer func() {
		for _, pointer := range needFreePointer {
			if pointer != nil {
				C.free(pointer)
			}
		}
	}()
	if err != nil {
		return -1
	}
	result := int(C.taos_stmt_bind_param(stmt, (*C.TAOS_MULTI_BIND)(unsafe.Pointer(&binds[0]))))
	return result
}

func generateTaosBindList(params []driver.Value) ([]C.TAOS_MULTI_BIND, []unsafe.Pointer, error) {
	binds := make([]C.TAOS_MULTI_BIND, len(params))
	var needFreePointer []unsafe.Pointer
	for i, param := range params {
		bind := C.TAOS_MULTI_BIND{}
		bind.num = C.int(1)
		if param == nil {
			bind.buffer_type = C.TSDB_DATA_TYPE_BOOL
			p := C.malloc(1)
			*(*C.char)(p) = C.char(1)
			needFreePointer = append(needFreePointer, p)
			bind.is_null = (*C.char)(p)
		} else {
			switch value := param.(type) {
			case taosTypes.TaosBool:
				bind.buffer_type = C.TSDB_DATA_TYPE_BOOL
				p := C.malloc(1)
				if value {
					*(*C.int8_t)(p) = C.int8_t(1)
				} else {
					*(*C.int8_t)(p) = C.int8_t(0)
				}
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(1)
			case taosTypes.TaosTinyint:
				bind.buffer_type = C.TSDB_DATA_TYPE_TINYINT
				p := C.malloc(1)
				*(*C.int8_t)(p) = C.int8_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(1)
			case taosTypes.TaosSmallint:
				bind.buffer_type = C.TSDB_DATA_TYPE_SMALLINT
				p := C.malloc(2)
				*(*C.int16_t)(p) = C.int16_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(2)
			case taosTypes.TaosInt:
				bind.buffer_type = C.TSDB_DATA_TYPE_INT
				p := C.malloc(4)
				*(*C.int32_t)(p) = C.int32_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(4)
			case taosTypes.TaosBigint:
				bind.buffer_type = C.TSDB_DATA_TYPE_BIGINT
				p := C.malloc(8)
				*(*C.int64_t)(p) = C.int64_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(8)
			case taosTypes.TaosUTinyint:
				bind.buffer_type = C.TSDB_DATA_TYPE_UTINYINT
				cbuf := C.malloc(1)
				*(*C.uint8_t)(cbuf) = C.uint8_t(value)
				needFreePointer = append(needFreePointer, cbuf)
				bind.buffer = cbuf
				bind.buffer_length = C.uintptr_t(1)
			case taosTypes.TaosUSmallint:
				bind.buffer_type = C.TSDB_DATA_TYPE_USMALLINT
				p := C.malloc(2)
				*(*C.uint16_t)(p) = C.uint16_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(2)
			case taosTypes.TaosUInt:
				bind.buffer_type = C.TSDB_DATA_TYPE_UINT
				p := C.malloc(4)
				*(*C.uint32_t)(p) = C.uint32_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(4)
			case taosTypes.TaosUBigint:
				bind.buffer_type = C.TSDB_DATA_TYPE_UBIGINT
				p := C.malloc(8)
				*(*C.uint64_t)(p) = C.uint64_t(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(8)
			case taosTypes.TaosFloat:
				bind.buffer_type = C.TSDB_DATA_TYPE_FLOAT
				p := C.malloc(4)
				*(*C.float)(p) = C.float(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(4)
			case taosTypes.TaosDouble:
				bind.buffer_type = C.TSDB_DATA_TYPE_DOUBLE
				p := C.malloc(8)
				*(*C.double)(p) = C.double(value)
				needFreePointer = append(needFreePointer, p)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(8)
			case taosTypes.TaosBinary:
				bind.buffer_type = C.TSDB_DATA_TYPE_BINARY
				cbuf := C.CString(string(value))
				needFreePointer = append(needFreePointer, unsafe.Pointer(cbuf))
				bind.buffer = unsafe.Pointer(cbuf)
				clen := int32(len(value))
				p := C.malloc(C.size_t(unsafe.Sizeof(clen)))
				bind.length = (*C.int32_t)(p)
				*(bind.length) = C.int32_t(clen)
				needFreePointer = append(needFreePointer, p)
				bind.buffer_length = C.uintptr_t(clen)
			case taosTypes.TaosVarBinary:
				bind.buffer_type = C.TSDB_DATA_TYPE_VARBINARY
				cbuf := C.CString(string(value))
				needFreePointer = append(needFreePointer, unsafe.Pointer(cbuf))
				bind.buffer = unsafe.Pointer(cbuf)
				clen := int32(len(value))
				p := C.malloc(C.size_t(unsafe.Sizeof(clen)))
				bind.length = (*C.int32_t)(p)
				*(bind.length) = C.int32_t(clen)
				needFreePointer = append(needFreePointer, p)
				bind.buffer_length = C.uintptr_t(clen)
			case taosTypes.TaosGeometry:
				bind.buffer_type = C.TSDB_DATA_TYPE_GEOMETRY
				cbuf := C.CString(string(value))
				needFreePointer = append(needFreePointer, unsafe.Pointer(cbuf))
				bind.buffer = unsafe.Pointer(cbuf)
				clen := int32(len(value))
				p := C.malloc(C.size_t(unsafe.Sizeof(clen)))
				bind.length = (*C.int32_t)(p)
				*(bind.length) = C.int32_t(clen)
				needFreePointer = append(needFreePointer, p)
				bind.buffer_length = C.uintptr_t(clen)
			case taosTypes.TaosNchar:
				bind.buffer_type = C.TSDB_DATA_TYPE_NCHAR
				p := unsafe.Pointer(C.CString(string(value)))
				needFreePointer = append(needFreePointer, p)
				bind.buffer = unsafe.Pointer(p)
				clen := int32(len(value))
				bind.length = (*C.int32_t)(C.malloc(C.size_t(unsafe.Sizeof(clen))))
				*(bind.length) = C.int32_t(clen)
				needFreePointer = append(needFreePointer, unsafe.Pointer(bind.length))
				bind.buffer_length = C.uintptr_t(clen)
			case taosTypes.TaosTimestamp:
				bind.buffer_type = C.TSDB_DATA_TYPE_TIMESTAMP
				ts := common.TimeToTimestamp(value.T, value.Precision)
				p := C.malloc(8)
				needFreePointer = append(needFreePointer, p)
				*(*C.int64_t)(p) = C.int64_t(ts)
				bind.buffer = p
				bind.buffer_length = C.uintptr_t(8)
			case taosTypes.TaosJson:
				bind.buffer_type = C.TSDB_DATA_TYPE_JSON
				cbuf := C.CString(string(value))
				needFreePointer = append(needFreePointer, unsafe.Pointer(cbuf))
				bind.buffer = unsafe.Pointer(cbuf)
				clen := int32(len(value))
				p := C.malloc(C.size_t(unsafe.Sizeof(clen)))
				bind.length = (*C.int32_t)(p)
				*(bind.length) = C.int32_t(clen)
				needFreePointer = append(needFreePointer, p)
				bind.buffer_length = C.uintptr_t(clen)
			default:
				return nil, nil, errors.New("unsupported type")
			}
		}
		binds[i] = bind
	}
	return binds, needFreePointer, nil
}

// TaosStmtAddBatch int        taos_stmt_add_batch(TAOS_STMT *stmt);
func TaosStmtAddBatch(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_add_batch(stmt))
}

// TaosStmtExecute int        taos_stmt_execute(TAOS_STMT *stmt);
func TaosStmtExecute(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_execute(stmt))
}

// TaosStmtUseResult TAOS_RES * taos_stmt_use_result(TAOS_STMT *stmt);
func TaosStmtUseResult(stmt unsafe.Pointer) unsafe.Pointer {
	return C.taos_stmt_use_result(stmt)
}

// TaosStmtClose int        taos_stmt_close(TAOS_STMT *stmt);
func TaosStmtClose(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_close(stmt))
}

// TaosStmtSetSubTBName int        taos_stmt_set_sub_tbname(TAOS_STMT* stmt, const char* name);
func TaosStmtSetSubTBName(stmt unsafe.Pointer, name string) int {
	cStr := C.CString(name)
	defer C.free(unsafe.Pointer(cStr))
	return int(C.taos_stmt_set_tbname(stmt, cStr))
}

// TaosStmtBindParamBatch int        taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind);
func TaosStmtBindParamBatch(stmt unsafe.Pointer, multiBind [][]driver.Value, bindType []*taosTypes.ColumnType) int {
	var binds = make([]C.TAOS_MULTI_BIND, len(multiBind))
	var needFreePointer []unsafe.Pointer
	defer func() {
		for _, pointer := range needFreePointer {
			C.free(pointer)
		}
	}()
	for columnIndex, columnData := range multiBind {
		bind := C.TAOS_MULTI_BIND{}
		//malloc
		rowLen := len(multiBind[0])
		bind.num = C.int(rowLen)
		nullList := unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen))))
		needFreePointer = append(needFreePointer, nullList)
		lengthList := unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen * 4))))
		needFreePointer = append(needFreePointer, lengthList)
		var p unsafe.Pointer
		columnType := bindType[columnIndex]
		switch columnType.Type {
		case taosTypes.TaosBoolType:
			//1
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_BOOL
			bind.buffer_length = C.uintptr_t(1)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosBool)
					current := unsafe.Pointer(uintptr(p) + uintptr(i))
					if value {
						*(*C.int8_t)(current) = C.int8_t(1)
					} else {
						*(*C.int8_t)(current) = C.int8_t(0)
					}

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(1)
				}
			}
		case taosTypes.TaosTinyintType:
			//1
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_TINYINT
			bind.buffer_length = C.uintptr_t(1)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosTinyint)
					current := unsafe.Pointer(uintptr(p) + uintptr(i))
					*(*C.int8_t)(current) = C.int8_t(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(1)
				}
			}
		case taosTypes.TaosSmallintType:
			//2
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(2 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_SMALLINT
			bind.buffer_length = C.uintptr_t(2)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosSmallint)
					current := unsafe.Pointer(uintptr(p) + uintptr(2*i))
					*(*C.int16_t)(current) = C.int16_t(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(2)
				}
			}
		case taosTypes.TaosIntType:
			//4
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(4 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_INT
			bind.buffer_length = C.uintptr_t(4)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosInt)
					current := unsafe.Pointer(uintptr(p) + uintptr(4*i))
					*(*C.int32_t)(current) = C.int32_t(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(4)
				}
			}
		case taosTypes.TaosBigintType:
			//8
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(8 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_BIGINT
			bind.buffer_length = C.uintptr_t(8)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosBigint)
					current := unsafe.Pointer(uintptr(p) + uintptr(8*i))
					*(*C.int64_t)(current) = C.int64_t(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(8)
				}
			}
		case taosTypes.TaosUTinyintType:
			//1
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_UTINYINT
			bind.buffer_length = C.uintptr_t(1)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosUTinyint)
					current := unsafe.Pointer(uintptr(p) + uintptr(i))
					*(*C.uint8_t)(current) = C.uint8_t(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(1)
				}
			}
		case taosTypes.TaosUSmallintType:
			//2
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(2 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_USMALLINT
			bind.buffer_length = C.uintptr_t(2)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosUSmallint)
					current := unsafe.Pointer(uintptr(p) + uintptr(2*i))
					*(*C.uint16_t)(current) = C.uint16_t(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(2)
				}
			}
		case taosTypes.TaosUIntType:
			//4
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(4 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_UINT
			bind.buffer_length = C.uintptr_t(4)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosUInt)
					current := unsafe.Pointer(uintptr(p) + uintptr(4*i))
					*(*C.uint32_t)(current) = C.uint32_t(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(4)
				}
			}
		case taosTypes.TaosUBigintType:
			//8
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(8 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_UBIGINT
			bind.buffer_length = C.uintptr_t(8)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosUBigint)
					current := unsafe.Pointer(uintptr(p) + uintptr(8*i))
					*(*C.uint64_t)(current) = C.uint64_t(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(8)
				}
			}
		case taosTypes.TaosFloatType:
			//4
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(4 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_FLOAT
			bind.buffer_length = C.uintptr_t(4)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosFloat)
					current := unsafe.Pointer(uintptr(p) + uintptr(4*i))
					*(*C.float)(current) = C.float(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(4)
				}
			}
		case taosTypes.TaosDoubleType:
			//8
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(8 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_DOUBLE
			bind.buffer_length = C.uintptr_t(8)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosDouble)
					current := unsafe.Pointer(uintptr(p) + uintptr(8*i))
					*(*C.double)(current) = C.double(value)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(8)
				}
			}
		case taosTypes.TaosBinaryType:
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(columnType.MaxLen * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_BINARY
			bind.buffer_length = C.uintptr_t(columnType.MaxLen)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosBinary)
					for j := 0; j < len(value); j++ {
						*(*C.char)(unsafe.Pointer(uintptr(p) + uintptr(columnType.MaxLen*i+j))) = (C.char)(value[j])
					}
					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(len(value))
				}
			}
		case taosTypes.TaosVarBinaryType:
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(columnType.MaxLen * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_VARBINARY
			bind.buffer_length = C.uintptr_t(columnType.MaxLen)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosVarBinary)
					for j := 0; j < len(value); j++ {
						*(*C.char)(unsafe.Pointer(uintptr(p) + uintptr(columnType.MaxLen*i+j))) = (C.char)(value[j])
					}
					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(len(value))
				}
			}
		case taosTypes.TaosGeometryType:
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(columnType.MaxLen * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_GEOMETRY
			bind.buffer_length = C.uintptr_t(columnType.MaxLen)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosGeometry)
					for j := 0; j < len(value); j++ {
						*(*C.char)(unsafe.Pointer(uintptr(p) + uintptr(columnType.MaxLen*i+j))) = (C.char)(value[j])
					}
					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(len(value))
				}
			}
		case taosTypes.TaosNcharType:
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(columnType.MaxLen * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_NCHAR
			bind.buffer_length = C.uintptr_t(columnType.MaxLen)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosNchar)
					for j := 0; j < len(value); j++ {
						*(*C.char)(unsafe.Pointer(uintptr(p) + uintptr(columnType.MaxLen*i+j))) = (C.char)(value[j])
					}
					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(len(value))
				}
			}
		case taosTypes.TaosTimestampType:
			//8
			p = unsafe.Pointer(C.malloc(C.size_t(C.uint(8 * rowLen))))
			bind.buffer_type = C.TSDB_DATA_TYPE_TIMESTAMP
			bind.buffer_length = C.uintptr_t(8)
			for i, rowData := range columnData {
				currentNull := unsafe.Pointer(uintptr(nullList) + uintptr(i))
				if rowData == nil {
					*(*C.char)(currentNull) = C.char(1)
				} else {
					*(*C.char)(currentNull) = C.char(0)
					value := rowData.(taosTypes.TaosTimestamp)
					ts := common.TimeToTimestamp(value.T, value.Precision)
					current := unsafe.Pointer(uintptr(p) + uintptr(8*i))
					*(*C.int64_t)(current) = C.int64_t(ts)

					l := unsafe.Pointer(uintptr(lengthList) + uintptr(4*i))
					*(*C.int32_t)(l) = C.int32_t(8)
				}
			}
		}
		needFreePointer = append(needFreePointer, p)
		bind.buffer = p
		bind.length = (*C.int32_t)(lengthList)
		bind.is_null = (*C.char)(nullList)
		binds[columnIndex] = bind
	}
	return int(C.taos_stmt_bind_param_batch(stmt, (*C.TAOS_MULTI_BIND)(&binds[0])))
}

// TaosStmtErrStr char       *taos_stmt_errstr(TAOS_STMT *stmt);
func TaosStmtErrStr(stmt unsafe.Pointer) string {
	return C.GoString(C.taos_stmt_errstr(stmt))
}

// TaosStmtAffectedRows int         taos_stmt_affected_rows(TAOS_STMT *stmt);
func TaosStmtAffectedRows(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_affected_rows(stmt))
}

// TaosStmtAffectedRowsOnce  int         taos_stmt_affected_rows_once(TAOS_STMT *stmt);
func TaosStmtAffectedRowsOnce(stmt unsafe.Pointer) int {
	return int(C.taos_stmt_affected_rows_once(stmt))
}

//typedef struct TAOS_FIELD_E {
//char    name[65];
//int8_t  type;
//uint8_t precision;
//uint8_t scale;
//int32_t bytes;
//} TAOS_FIELD_E;

// TaosStmtGetTagFields DLL_EXPORT int        taos_stmt_get_tag_fields(TAOS_STMT *stmt, int* fieldNum, TAOS_FIELD_E** fields);
func TaosStmtGetTagFields(stmt unsafe.Pointer) (code, num int, fields unsafe.Pointer) {
	cNum := unsafe.Pointer(&num)
	var cField *C.TAOS_FIELD_E
	code = int(C.taos_stmt_get_tag_fields(stmt, (*C.int)(cNum), (**C.TAOS_FIELD_E)(unsafe.Pointer(&cField))))
	if code != 0 {
		return code, num, nil
	}
	if num == 0 {
		return code, num, nil
	}
	return code, num, unsafe.Pointer(cField)
}

// TaosStmtGetColFields DLL_EXPORT int        taos_stmt_get_col_fields(TAOS_STMT *stmt, int* fieldNum, TAOS_FIELD_E** fields);
func TaosStmtGetColFields(stmt unsafe.Pointer) (code, num int, fields unsafe.Pointer) {
	cNum := unsafe.Pointer(&num)
	var cField *C.TAOS_FIELD_E
	code = int(C.taos_stmt_get_col_fields(stmt, (*C.int)(cNum), (**C.TAOS_FIELD_E)(unsafe.Pointer(&cField))))
	if code != 0 {
		return code, num, nil
	}
	if num == 0 {
		return code, num, nil
	}
	return code, num, unsafe.Pointer(cField)
}

func StmtParseFields(num int, fields unsafe.Pointer) []*stmt.StmtField {
	if num == 0 {
		return nil
	}
	if fields == nil {
		return nil
	}
	result := make([]*stmt.StmtField, num)
	buf := bytes.NewBufferString("")
	for i := 0; i < num; i++ {
		r := &stmt.StmtField{}
		field := *(*C.TAOS_FIELD_E)(unsafe.Pointer(uintptr(fields) + uintptr(C.sizeof_struct_TAOS_FIELD_E*C.int(i))))
		for _, c := range field.name {
			if c == 0 {
				break
			}
			buf.WriteByte(byte(c))
		}
		r.Name = buf.String()
		buf.Reset()
		r.FieldType = int8(field._type)
		r.Precision = uint8(field.precision)
		r.Scale = uint8(field.scale)
		r.Bytes = int32(field.bytes)
		result[i] = r
	}
	return result
}

// TaosStmtReclaimFields DLL_EXPORT void       taos_stmt_reclaim_fields(TAOS_STMT *stmt, TAOS_FIELD_E *fields);
func TaosStmtReclaimFields(stmt unsafe.Pointer, fields unsafe.Pointer) {
	C.taos_stmt_reclaim_fields(stmt, (*C.TAOS_FIELD_E)(fields))
}

// TaosStmtGetParam  DLL_EXPORT int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes)
func TaosStmtGetParam(stmt unsafe.Pointer, idx int) (dataType int, dataLength int, err error) {
	code := C.taos_stmt_get_param(stmt, C.int(idx), (*C.int)(unsafe.Pointer(&dataType)), (*C.int)(unsafe.Pointer(&dataLength)))
	if code != 0 {
		err = &taosError.TaosError{
			Code:   int32(code),
			ErrStr: TaosStmtErrStr(stmt),
		}
	}
	return
}
