package wrapper

/*
#cgo CFLAGS: -IC:/TDengine/include -I/usr/include
#cgo linux LDFLAGS: -L/usr/lib -ltaosnative
#cgo windows LDFLAGS: -LC:/TDengine/driver -ltaosnative
#cgo darwin LDFLAGS: -L/usr/local/lib -ltaosnative
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
extern void QueryCallback(void *param,TAOS_RES *,int code);
extern void FetchRowsCallback(void *param,TAOS_RES *,int numOfRows);
extern void FetchRawBlockCallback(void *param,TAOS_RES *,int numOfRows);
int taos_options_wrapper(TSDB_OPTION option, char *arg) {
	return taos_options(option,arg);
};
void taos_fetch_rows_a_wrapper(TAOS_RES *res, void *param){
	return taos_fetch_rows_a(res,FetchRowsCallback,param);
};
void taos_query_a_wrapper(TAOS *taos,const char *sql, void *param){
	return taos_query_a(taos,sql,QueryCallback,param);
};
void taos_query_a_with_req_id_wrapper(TAOS *taos,const char *sql, void *param, int64_t reqID){
	return taos_query_a_with_reqid(taos, sql, QueryCallback, param, reqID);
};
void taos_fetch_raw_block_a_wrapper(TAOS_RES *res, void *param){
	return taos_fetch_raw_block_a(res,FetchRawBlockCallback,param);
};
int taos_options_connection_wrapper(TAOS *taos, TSDB_OPTION_CONNECTION option, void *arg) {
	return taos_options_connection(taos,option,arg);
};
*/
import "C"
import (
	"strings"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/tools"
)

// TaosFreeResult void taos_free_result(TAOS_RES *res);
func TaosFreeResult(res unsafe.Pointer) {
	C.taos_free_result(res)
}

// TaosConnect TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port);
func TaosConnect(host, user, pass, db string, port int) (taos unsafe.Pointer, err error) {
	cUser := C.CString(user)
	defer C.free(unsafe.Pointer(cUser))
	cPass := C.CString(pass)
	defer C.free(unsafe.Pointer(cPass))
	cdb := (*C.char)(nil)
	if len(db) > 0 {
		cdb = C.CString(db)
		defer C.free(unsafe.Pointer(cdb))
	}
	var taosObj unsafe.Pointer
	if len(host) == 0 {
		taosObj = C.taos_connect(nil, cUser, cPass, cdb, (C.ushort)(port))
	} else {
		cHost := C.CString(host)
		defer C.free(unsafe.Pointer(cHost))
		taosObj = C.taos_connect(cHost, cUser, cPass, cdb, (C.ushort)(port))
	}

	if taosObj == nil {
		errCode := TaosError(nil)
		return nil, errors.NewError(errCode, TaosErrorStr(nil))
	}
	return taosObj, nil
}

// TaosClose void  taos_close(TAOS *taos);
func TaosClose(taosConnect unsafe.Pointer) {
	C.taos_close(taosConnect)
}

// TaosQuery TAOS_RES *taos_query(TAOS *taos, const char *sql);
func TaosQuery(taosConnect unsafe.Pointer, sql string) unsafe.Pointer {
	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cSql))
	return unsafe.Pointer(C.taos_query(taosConnect, cSql))
}

// TaosQueryWithReqID TAOS_RES *taos_query_with_reqid(TAOS *taos, const char *sql, int64_t reqID);
func TaosQueryWithReqID(taosConn unsafe.Pointer, sql string, reqID int64) unsafe.Pointer {
	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cSql))
	return unsafe.Pointer(C.taos_query_with_reqid(taosConn, cSql, (C.int64_t)(reqID)))
}

// TaosError int taos_errno(TAOS_RES *tres);
func TaosError(result unsafe.Pointer) int {
	return int(C.taos_errno(result))
}

// TaosErrorStr char *taos_errstr(TAOS_RES *tres);
func TaosErrorStr(result unsafe.Pointer) string {
	return C.GoString(C.taos_errstr(result))
}

// TaosFieldCount int taos_field_count(TAOS_RES *res);
func TaosFieldCount(result unsafe.Pointer) int {
	return int(C.taos_field_count(result))
}

// TaosAffectedRows int taos_affected_rows(TAOS_RES *res);
func TaosAffectedRows(result unsafe.Pointer) int {
	return int(C.taos_affected_rows(result))
}

// TaosFetchFields TAOS_FIELD *taos_fetch_fields(TAOS_RES *res);
func TaosFetchFields(result unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.taos_fetch_fields(result))
}

// TaosFetchFieldsE TAOS_FIELD_E *taos_fetch_fields_e(TAOS_RES *res); 3.3.6.0
func TaosFetchFieldsE(result unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.taos_fetch_fields_e(result))
}

// TaosFetchBlock int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows);
func TaosFetchBlock(result unsafe.Pointer) (int, unsafe.Pointer) {
	var block C.TAOS_ROW
	b := unsafe.Pointer(&block)
	blockSize := int(C.taos_fetch_block(result, (*C.TAOS_ROW)(b)))
	return blockSize, b
}

// TaosResultPrecision int taos_result_precision(TAOS_RES *res);
func TaosResultPrecision(result unsafe.Pointer) int {
	return int(C.taos_result_precision(result))
}

// TaosNumFields int taos_num_fields(TAOS_RES *res);
func TaosNumFields(result unsafe.Pointer) int {
	return int(C.taos_num_fields(result))
}

// TaosFetchRow TAOS_ROW taos_fetch_row(TAOS_RES *res);
func TaosFetchRow(result unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.taos_fetch_row(result))
}

// TaosSelectDB int taos_select_db(TAOS *taos, const char *db);
func TaosSelectDB(taosConnect unsafe.Pointer, db string) int {
	cDB := C.CString(db)
	defer C.free(unsafe.Pointer(cDB))
	return int(C.taos_select_db(taosConnect, cDB))
}

// TaosOptions int   taos_options(TSDB_OPTION option, const void *arg, ...);
func TaosOptions(option int, value string) int {
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	return int(C.taos_options_wrapper((C.TSDB_OPTION)(option), cValue))
}

// TaosQueryA void taos_query_a(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, int code), void *param);
func TaosQueryA(taosConnect unsafe.Pointer, sql string, caller cgo.Handle) {
	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cSql))
	C.taos_query_a_wrapper(taosConnect, cSql, caller.Pointer())
}

// TaosQueryAWithReqID void taos_query_a_with_reqid(TAOS *taos, const char *sql, __taos_async_fn_t fp, void *param, int64_t reqid);
func TaosQueryAWithReqID(taosConn unsafe.Pointer, sql string, caller cgo.Handle, reqID int64) {
	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cSql))
	C.taos_query_a_with_req_id_wrapper(taosConn, cSql, caller.Pointer(), (C.int64_t)(reqID))
}

// TaosFetchRowsA void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);
func TaosFetchRowsA(res unsafe.Pointer, caller cgo.Handle) {
	C.taos_fetch_rows_a_wrapper(res, caller.Pointer())
}

// TaosResetCurrentDB void taos_reset_current_db(TAOS *taos);
func TaosResetCurrentDB(taosConnect unsafe.Pointer) {
	C.taos_reset_current_db(taosConnect)
}

// TaosValidateSql int taos_validate_sql(TAOS *taos, const char *sql);
func TaosValidateSql(taosConnect unsafe.Pointer, sql string) int {
	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cSql))
	return int(C.taos_validate_sql(taosConnect, cSql))
}

// TaosIsUpdateQuery bool taos_is_update_query(TAOS_RES *res);
func TaosIsUpdateQuery(res unsafe.Pointer) bool {
	return bool(C.taos_is_update_query(res))
}

// TaosFetchLengths int* taos_fetch_lengths(TAOS_RES *res);
func TaosFetchLengths(res unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.taos_fetch_lengths(res))
}

// TaosFetchRawBlockA void        taos_fetch_raw_block_a(TAOS_RES* res, __taos_async_fn_t fp, void* param);
func TaosFetchRawBlockA(res unsafe.Pointer, caller cgo.Handle) {
	C.taos_fetch_raw_block_a_wrapper(res, caller.Pointer())
}

// TaosGetRawBlock const void *taos_get_raw_block(TAOS_RES* res);
func TaosGetRawBlock(result unsafe.Pointer) unsafe.Pointer {
	return unsafe.Pointer(C.taos_get_raw_block(result))
}

// TaosGetClientInfo const char *taos_get_client_info();
func TaosGetClientInfo() string {
	return C.GoString(C.taos_get_client_info())
}

// TaosLoadTableInfo taos_load_table_info(TAOS *taos, const char* tableNameList);
func TaosLoadTableInfo(taosConnect unsafe.Pointer, tableNameList []string) int {
	s := strings.Join(tableNameList, ",")
	buf := C.CString(s)
	defer C.free(unsafe.Pointer(buf))
	return int(C.taos_load_table_info(taosConnect, buf))
}

// TaosGetTableVgID
// DLL_EXPORT int taos_get_table_vgId(TAOS *taos, const char *db, const char *table, int *vgId)
func TaosGetTableVgID(conn unsafe.Pointer, db, table string) (vgID int, code int) {
	cDB := C.CString(db)
	defer C.free(unsafe.Pointer(cDB))
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))

	code = int(C.taos_get_table_vgId(conn, cDB, cTable, (*C.int)(unsafe.Pointer(&vgID))))
	return
}

// TaosGetTablesVgID DLL_EXPORT int taos_get_tables_vgId(TAOS *taos, const char *db, const char *table[], int tableNum, int *vgId)
func TaosGetTablesVgID(conn unsafe.Pointer, db string, tables []string) (vgIDs []int, code int) {
	cDB := C.CString(db)
	defer C.free(unsafe.Pointer(cDB))
	numTables := len(tables)
	cTables := make([]*C.char, numTables)
	needFree := make([]unsafe.Pointer, numTables)
	defer func() {
		for _, p := range needFree {
			C.free(p)
		}
	}()
	for i, table := range tables {
		cTable := C.CString(table)
		needFree[i] = unsafe.Pointer(cTable)
		cTables[i] = cTable
	}
	p := C.malloc(C.sizeof_int * C.size_t(numTables))
	defer C.free(p)
	code = int(C.taos_get_tables_vgId(conn, cDB, (**C.char)(&cTables[0]), (C.int)(numTables), (*C.int)(p)))
	if code != 0 {
		return nil, code
	}
	vgIDs = make([]int, numTables)
	for i := 0; i < numTables; i++ {
		vgIDs[i] = int(*(*C.int)(tools.AddPointer(p, uintptr(C.sizeof_int*C.int(i)))))
	}
	return
}

//typedef enum {
//TAOS_CONN_MODE_BI = 0,
//} TAOS_CONN_MODE;
//
//DLL_EXPORT int taos_set_conn_mode(TAOS* taos, int mode, int value);

func TaosSetConnMode(conn unsafe.Pointer, mode int, value int) int {
	return int(C.taos_set_conn_mode(conn, C.int(mode), C.int(value)))
}

// TaosGetCurrentDB DLL_EXPORT int taos_get_current_db(TAOS *taos, char *database, int len, int *required)
func TaosGetCurrentDB(conn unsafe.Pointer) (db string, err error) {
	cDb := (*C.char)(C.malloc(195))
	defer C.free(unsafe.Pointer(cDb))
	var required int

	code := C.taos_get_current_db(conn, cDb, C.int(195), (*C.int)(unsafe.Pointer(&required)))
	if code != 0 {
		return "", errors.NewError(int(code), TaosErrorStr(nil))
	}
	db = C.GoString(cDb)
	return db, nil
}

// TaosGetServerInfo DLL_EXPORT const char *taos_get_server_info(TAOS *taos)
func TaosGetServerInfo(conn unsafe.Pointer) string {
	info := C.taos_get_server_info(conn)
	return C.GoString(info)
}

// TaosOptionsConnection int taos_options_connection(TAOS *taos, TSDB_OPTION_CONNECTION option, const void *arg, ...)
func TaosOptionsConnection(conn unsafe.Pointer, option int, value *string) int {
	cValue := unsafe.Pointer(nil)
	if value != nil {
		cValue = unsafe.Pointer(C.CString(*value))
		defer C.free(cValue)
	}
	return int(C.taos_options_connection_wrapper(conn, (C.TSDB_OPTION_CONNECTION)(option), cValue))
}

// TaosCheckServerStatus TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen);
func TaosCheckServerStatus(fqdn *string, port int32) (status int32, details string) {
	var cFqdn = (*C.char)(nil)
	if fqdn != nil {
		cFqdn = C.CString(*fqdn)
	}
	defer C.free(unsafe.Pointer(cFqdn))
	cDetails := (*C.char)(C.calloc(C.size_t(C.uint(1024)), C.size_t(C.uint(1))))
	defer C.free(unsafe.Pointer(cDetails))
	status = int32(C.taos_check_server_status(cFqdn, C.int(port), cDetails, 1024))
	details = C.GoString(cDetails)
	return
}

// TaosRegisterInstance int32_t taos_register_instance(const char *id, const char *type, const char *desc,int32_t expire);
func TaosRegisterInstance(id, registerType, desc string, expire int32) int32 {
	idC := C.CString(id)
	defer C.free(unsafe.Pointer(idC))
	typeC := C.CString(registerType)
	defer C.free(unsafe.Pointer(typeC))
	descC := C.CString(desc)
	defer C.free(unsafe.Pointer(descC))
	ret := int32(C.taos_register_instance(idC, typeC, descC, C.int32_t(expire)))
	return ret
}

// TaosListInstances int32_t taos_list_instances(const char *filter_type, char ***pList, int32_t *pCount);
func TaosListInstances(filterType string) (instances []string, err error) {
	typeC := C.CString(filterType)
	defer C.free(unsafe.Pointer(typeC))
	var pList **C.char
	var pCount C.int32_t
	ret := int32(C.taos_list_instances(typeC, &pList, &pCount))
	if ret != 0 {
		return nil, errors.NewError(int(ret), TaosErrorStr(nil))
	}
	defer func() {
		C.taos_free_instances(&pList, pCount)
	}()
	count := int(pCount)
	instances = make([]string, count)
	ptrSize := unsafe.Sizeof(uintptr(0))
	for i := 0; i < count; i++ {
		cStr := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(pList)) + uintptr(i)*ptrSize))
		instances[i] = C.GoString(cStr)
	}
	return instances, nil
}

// TaosConnectTOTP TAOS *taos_connect_totp(const char *ip, const char *user, const char *pass, const char* totp, const char *db, uint16_t port);
func TaosConnectTOTP(host, user, pass, totp, db string, port int) (taos unsafe.Pointer, err error) {
	cUser := C.CString(user)
	defer C.free(unsafe.Pointer(cUser))
	cPass := C.CString(pass)
	defer C.free(unsafe.Pointer(cPass))
	cTotp := C.CString(totp)
	defer C.free(unsafe.Pointer(cTotp))
	cdb := (*C.char)(nil)
	if len(db) > 0 {
		cdb = C.CString(db)
		defer C.free(unsafe.Pointer(cdb))
	}
	var taosObj unsafe.Pointer
	if len(host) == 0 {
		taosObj = C.taos_connect_totp(nil, cUser, cPass, cTotp, cdb, (C.ushort)(port))
	} else {
		cHost := C.CString(host)
		defer C.free(unsafe.Pointer(cHost))
		taosObj = C.taos_connect_totp(cHost, cUser, cPass, cTotp, cdb, (C.ushort)(port))
	}

	if taosObj == nil {
		errCode := TaosError(nil)
		return nil, errors.NewError(errCode, TaosErrorStr(nil))
	}
	return taosObj, nil
}

// TaosConnectToken TAOS *taos_connect_token(const char *ip, const char *token, const char *db, uint16_t port);
func TaosConnectToken(host, token, db string, port int) (taos unsafe.Pointer, err error) {
	cToken := C.CString(token)
	defer C.free(unsafe.Pointer(cToken))
	cdb := (*C.char)(nil)
	if len(db) > 0 {
		cdb = C.CString(db)
		defer C.free(unsafe.Pointer(cdb))
	}
	var taosObj unsafe.Pointer
	if len(host) == 0 {
		taosObj = C.taos_connect_token(nil, cToken, cdb, (C.ushort)(port))
	} else {
		cHost := C.CString(host)
		defer C.free(unsafe.Pointer(cHost))
		taosObj = C.taos_connect_token(cHost, cToken, cdb, (C.ushort)(port))
	}

	if taosObj == nil {
		errCode := TaosError(nil)
		return nil, errors.NewError(errCode, TaosErrorStr(nil))
	}
	return taosObj, nil
}

// TaosGetConnectionInfo int taos_get_connection_info(TAOS* taos, TSDB_CONNECTION_INFO info, char* buffer, int* len)
func TaosGetConnectionInfo(taos unsafe.Pointer, info int) (string, int) {
	cBuffer := (*C.char)(C.malloc(256))
	defer C.free(unsafe.Pointer(cBuffer))
	var cLen C.int = 256
	code := C.taos_get_connection_info(taos, (C.TSDB_CONNECTION_INFO)(info), cBuffer, (*C.int)(unsafe.Pointer(&cLen)))
	if code != 0 {
		return "", int(code)
	}
	value := C.GoStringN(cBuffer, cLen)
	return value, 0
}
