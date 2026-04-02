package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import "unsafe"

//revive:disable
const (
	InfluxDBLineProtocol       = 1
	OpenTSDBTelnetLineProtocol = 2
	OpenTSDBJsonFormatProtocol = 3
)
const (
	TSDB_SML_TIMESTAMP_NOT_CONFIGURED = iota
	TSDB_SML_TIMESTAMP_HOURS
	TSDB_SML_TIMESTAMP_MINUTES
	TSDB_SML_TIMESTAMP_SECONDS
	TSDB_SML_TIMESTAMP_MILLI_SECONDS
	TSDB_SML_TIMESTAMP_MICRO_SECONDS
	TSDB_SML_TIMESTAMP_NANO_SECONDS
)

//revive:enable

// TaosSchemalessInsert TAOS_RES *taos_schemaless_insert(TAOS* taos, char* lines[], int numLines, int protocol, int precision);
// Deprecated
func TaosSchemalessInsert(taosConnect unsafe.Pointer, lines []string, protocol int, precision string) unsafe.Pointer {
	numLines, cLines, needFree := taosSchemalessInsertParams(lines)
	defer func() {
		for _, p := range needFree {
			C.free(p)
		}
	}()
	return unsafe.Pointer(C.taos_schemaless_insert(
		taosConnect,
		(**C.char)(&cLines[0]),
		(C.int)(numLines),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
	))
}

// TaosSchemalessInsertTTL TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl)
// Deprecated
func TaosSchemalessInsertTTL(taosConnect unsafe.Pointer, lines []string, protocol int, precision string, ttl int) unsafe.Pointer {
	numLines, cLines, needFree := taosSchemalessInsertParams(lines)
	defer func() {
		for _, p := range needFree {
			C.free(p)
		}
	}()
	return unsafe.Pointer(C.taos_schemaless_insert_ttl(
		taosConnect,
		(**C.char)(&cLines[0]),
		(C.int)(numLines),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
		(C.int32_t)(ttl),
	))
}

// TaosSchemalessInsertWithReqID TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int64_t reqid);
// Deprecated
func TaosSchemalessInsertWithReqID(taosConnect unsafe.Pointer, lines []string, protocol int, precision string, reqID int64) unsafe.Pointer {
	numLines, cLines, needFree := taosSchemalessInsertParams(lines)
	defer func() {
		for _, p := range needFree {
			C.free(p)
		}
	}()
	return unsafe.Pointer(C.taos_schemaless_insert_with_reqid(
		taosConnect,
		(**C.char)(&cLines[0]),
		(C.int)(numLines),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
		(C.int64_t)(reqID),
	))
}

// TaosSchemalessInsertTTLWithReqID TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl, int64_t reqid)
// Deprecated
func TaosSchemalessInsertTTLWithReqID(taosConnect unsafe.Pointer, lines []string, protocol int, precision string, ttl int, reqID int64) unsafe.Pointer {
	numLines, cLines, needFree := taosSchemalessInsertParams(lines)
	defer func() {
		for _, p := range needFree {
			C.free(p)
		}
	}()
	return unsafe.Pointer(C.taos_schemaless_insert_ttl_with_reqid(
		taosConnect,
		(**C.char)(&cLines[0]),
		(C.int)(numLines),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
		(C.int32_t)(ttl),
		(C.int64_t)(reqID),
	))
}

func taosSchemalessInsertParams(lines []string) (numLines int, cLines []*C.char, needFree []unsafe.Pointer) {
	numLines = len(lines)
	cLines = make([]*C.char, numLines)
	needFree = make([]unsafe.Pointer, numLines)
	for i, line := range lines {
		cLine := C.CString(line)
		needFree[i] = unsafe.Pointer(cLine)
		cLines[i] = cLine
	}
	return
}

// TaosSchemalessInsertRaw TAOS_RES *taos_schemaless_insert_raw(TAOS* taos, char* lines, int len, int32_t *totalRows, int protocol, int precision);
func TaosSchemalessInsertRaw(taosConnect unsafe.Pointer, lines string, protocol int, precision string) (int32, unsafe.Pointer) {
	cLine := C.CString(lines)
	defer C.free(unsafe.Pointer(cLine))
	var rows int32
	pTotalRows := unsafe.Pointer(&rows)
	result := unsafe.Pointer(C.taos_schemaless_insert_raw(
		taosConnect,
		cLine,
		(C.int)(len(lines)),
		(*C.int32_t)(pTotalRows),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
	))
	return rows, result
}

// TaosSchemalessInsertRawTTL TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl);
func TaosSchemalessInsertRawTTL(taosConnect unsafe.Pointer, lines string, protocol int, precision string, ttl int) (int32, unsafe.Pointer) {
	cLine := C.CString(lines)
	defer C.free(unsafe.Pointer(cLine))
	var rows int32
	pTotalRows := unsafe.Pointer(&rows)
	result := unsafe.Pointer(C.taos_schemaless_insert_raw_ttl(
		taosConnect,
		cLine,
		(C.int)(len(lines)),
		(*C.int32_t)(pTotalRows),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
		(C.int32_t)(ttl),
	))
	return rows, result
}

// TaosSchemalessInsertRawWithReqID TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid);
func TaosSchemalessInsertRawWithReqID(taosConnect unsafe.Pointer, lines string, protocol int, precision string, reqID int64) (int32, unsafe.Pointer) {
	cLine := C.CString(lines)
	defer C.free(unsafe.Pointer(cLine))
	var rows int32
	pTotalRows := unsafe.Pointer(&rows)
	result := unsafe.Pointer(C.taos_schemaless_insert_raw_with_reqid(
		taosConnect,
		cLine,
		(C.int)(len(lines)),
		(*C.int32_t)(pTotalRows),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
		(C.int64_t)(reqID),
	))
	return rows, result
}

// TaosSchemalessInsertRawTTLWithReqID TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl, int64_t reqid)
func TaosSchemalessInsertRawTTLWithReqID(taosConnect unsafe.Pointer, lines string, protocol int, precision string, ttl int, reqID int64) (int32, unsafe.Pointer) {
	cLine := C.CString(lines)
	defer C.free(unsafe.Pointer(cLine))
	var rows int32
	pTotalRows := unsafe.Pointer(&rows)
	result := C.taos_schemaless_insert_raw_ttl_with_reqid(
		taosConnect,
		cLine,
		(C.int)(len(lines)),
		(*C.int32_t)(pTotalRows),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
		(C.int32_t)(ttl),
		(C.int64_t)(reqID),
	)
	return rows, result
}

// TaosSchemalessInsertRawTTLWithReqIDTBNameKey TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl, int64_t reqid, char *tbnameKey);
func TaosSchemalessInsertRawTTLWithReqIDTBNameKey(taosConnect unsafe.Pointer, lines string, protocol int, precision string, ttl int, reqID int64, tbNameKey string) (int32, unsafe.Pointer) {
	cLine := C.CString(lines)
	defer C.free(unsafe.Pointer(cLine))
	cTBNameKey := (*C.char)(nil)
	if tbNameKey != "" {
		cTBNameKey = C.CString(tbNameKey)
		defer C.free(unsafe.Pointer(cTBNameKey))
	}
	var rows int32
	pTotalRows := unsafe.Pointer(&rows)
	result := C.taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
		taosConnect,
		cLine,
		(C.int)(len(lines)),
		(*C.int32_t)(pTotalRows),
		(C.int)(protocol),
		(C.int)(exchange(precision)),
		(C.int32_t)(ttl),
		(C.int64_t)(reqID),
		cTBNameKey,
	)
	return rows, result
}

func exchange(ts string) int {
	switch ts {
	case "":
		return TSDB_SML_TIMESTAMP_NOT_CONFIGURED
	case "h":
		return TSDB_SML_TIMESTAMP_HOURS
	case "m":
		return TSDB_SML_TIMESTAMP_MINUTES
	case "s":
		return TSDB_SML_TIMESTAMP_SECONDS
	case "ms":
		return TSDB_SML_TIMESTAMP_MILLI_SECONDS
	case "u", "μ":
		return TSDB_SML_TIMESTAMP_MICRO_SECONDS
	case "ns":
		return TSDB_SML_TIMESTAMP_NANO_SECONDS
	}
	return TSDB_SML_TIMESTAMP_NOT_CONFIGURED
}
