const ref = require('ref-napi');
const ffi = require('ffi-napi');
const ArrayType = require('ref-array-di')(ref);
const Struct = require('ref-struct-di')(ref);
const os = require('os');

if ('win32' == os.platform()) {
    taoslibname = 'taos';
} else {
    taoslibname = 'libtaos';
}

var clientVersion = ffi.Library(taoslibname, {
    'taos_get_client_info': ['string', []],
});

function getClientVersion() {
    return clientVersion.taos_get_client_info().slice(0, 1)
}

// Define TaosField structure
var char_arr = ArrayType(ref.types.char);
var TaosField = Struct({
    'name': char_arr,
});
TaosField.fields.name.type.size = 65;
TaosField.defineProperty('type', ref.types.uint8);
TaosField.defineProperty('bytes', ref.types.int32);

// Define stmt taps_field_e
var TaosFiled_E = Struct({
    'name':char_arr,
    'type':ref.types.int8,
    'precision':ref.types.uint8,
    'scale':ref.types.uint8,
    'bytes':ref.types.uint32,  
  })

var smlLine = ArrayType(ref.coerceType('char *'))

ref.types.char_ptr = ref.refType(ref.types.char);
ref.types.void_ptr = ref.refType(ref.types.void);
ref.types.void_ptr2 = ref.refType(ref.types.void_ptr);

var libtaos = ffi.Library(taoslibname, {
    'taos_options': [ref.types.int, [ref.types.int, ref.types.void_ptr]],

    //TAOS *taos_connect(char *ip, char *user, char *pass, char *db, int port)
    'taos_connect': [ref.types.void_ptr, [ref.types.char_ptr, ref.types.char_ptr, ref.types.char_ptr, ref.types.char_ptr, ref.types.int]],
    //void taos_close(TAOS *taos)
    'taos_close': [ref.types.void, [ref.types.void_ptr]],
    //int *taos_fetch_lengths(TAOS_RES *res);
    'taos_fetch_lengths': [ref.types.void_ptr, [ref.types.void_ptr]],
    //int taos_query(TAOS *taos, char *sqlstr)
    'taos_query': [ref.types.void_ptr, [ref.types.void_ptr, ref.types.char_ptr]],
    //int taos_affected_rows(TAOS_RES *res)
    'taos_affected_rows': [ref.types.int, [ref.types.void_ptr]],
    //int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)
    'taos_fetch_block': [ref.types.int, [ref.types.void_ptr, ref.types.void_ptr]],
    //int taos_num_fields(TAOS_RES *res);
    'taos_num_fields': [ref.types.int, [ref.types.void_ptr]],
    //TAOS_ROW taos_fetch_row(TAOS_RES *res)
    //TAOS_ROW is void **, but we set the return type as a reference instead to get the row
    'taos_fetch_row': [ref.refType(ref.types.void_ptr2), [ref.types.void_ptr]],
    'taos_print_row': [ref.types.int, [ref.types.char_ptr, ref.types.void_ptr, ref.types.void_ptr, ref.types.int]],
    //int taos_result_precision(TAOS_RES *res)
    'taos_result_precision': [ref.types.int, [ref.types.void_ptr]],
    //void taos_free_result(TAOS_RES *res)
    'taos_free_result': [ref.types.void, [ref.types.void_ptr]],
    //int taos_field_count(TAOS *taos)
    'taos_field_count': [ref.types.int, [ref.types.void_ptr]],
    //TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)
    'taos_fetch_fields': [ref.refType(TaosField), [ref.types.void_ptr]],
    //int taos_errno(TAOS *taos)
    'taos_errno': [ref.types.int, [ref.types.void_ptr]],
    //char *taos_errstr(TAOS *taos)
    'taos_errstr': [ref.types.char_ptr, [ref.types.void_ptr]],
    //void taos_stop_query(TAOS_RES *res);
    'taos_stop_query': [ref.types.void, [ref.types.void_ptr]],
    //char *taos_get_server_info(TAOS *taos);
    'taos_get_server_info': [ref.types.char_ptr, [ref.types.void_ptr]],
    //char *taos_get_client_info();
    'taos_get_client_info': [ref.types.char_ptr, []],

    // ========================ASYNC QUERY==========================
    // void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *, TAOS_RES *, int), void *param)
    'taos_query_a': [ref.types.void, [ref.types.void_ptr, ref.types.char_ptr, ref.types.void_ptr, ref.types.void_ptr]],
    // void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);
    'taos_fetch_rows_a': [ref.types.void, [ref.types.void_ptr, ref.types.void_ptr, ref.types.void_ptr]],
    // TAOS_ROW *taos_result_block(TAOS_RES *res);
    'taos_result_block': [ref.types.void_ptr2, [ref.types.void_ptr]],
    
    // void taos_fetch_raw_block_a(TAOS_RES *res, __taos_async_fn_t fp, void *param);
    'taos_fetch_raw_block_a':[ref.types.void,[ref.types.void_ptr,ref.types.void_ptr,ref.types.void_ptr]]
    
    //const void *taos_get_raw_block(TAOS_RES *res);
    ,'taos_get_raw_block':[ref.types.void_ptr,[ref.types.void_ptr]]
    
    // Subscription using TMQ instead

    //======================== Schemaless ==========================
    //TAOS_RES* taos_schemaless_insert(TAOS* taos, char* lines[], int numLines, int protocol，int precision)
    // 'taos_schemaless_insert': [ref.types.void_ptr, [ref.types.void_ptr, ref.types.char_ptr, ref.types.int, ref.types.int, ref.types.int]]
    ,'taos_schemaless_insert': [ref.types.void_ptr, [ref.types.void_ptr, smlLine, 'int', 'int', 'int']]

    //====================================stmt APIs===============================
    // TAOS_STMT* taos_stmt_init(TAOS *taos)
    , 'taos_stmt_init': [ref.types.void_ptr, [ref.types.void_ptr]]

    // int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)
    , 'taos_stmt_prepare': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr, ref.types.ulong]]

    // int taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_MULTI_BIND* tags)
    , 'taos_stmt_set_tbname_tags': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr, ref.types.void_ptr]]

    // int taos_stmt_set_tbname(TAOS_STMT* stmt, const char* name)
    , 'taos_stmt_set_tbname': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr]]

    // int taos_stmt_set_sub_tbname(TAOS_STMT* stmt, const char* name)
    , 'taos_stmt_set_sub_tbname': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr]]

    // int taos_stmt_get_tag_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields);
    , 'taos_stmt_get_tag_fields': [ref.types.int, [ref.types.void_ptr, ref.refType(ref.types.int), ref.types.void_ptr2]]

    // int taos_stmt_get_col_fields(TAOS_STMT *stmt, int *fieldNum, TAOS_FIELD_E **fields);
    , 'taos_stmt_get_col_fields': [ref.types.int, [ref.types.void_ptr, ref.refType(ref.types.int), ref.types.void_ptr2]]

    // int taos_stmt_is_insert(TAOS_STMT *stmt, int *insert);
    , 'taos_stmt_is_insert': [ref.types.int, [ref.types.void_ptr, ref.refType(ref.types.int)]]

    // int taos_stmt_num_params(TAOS_STMT *stmt, int *nums);
    , 'taos_stmt_num_params': [ref.types.int, [ref.types.void_ptr, ref.refType(ref.types.int)]]

    // int taos_stmt_get_param(TAOS_STMT *stmt, int idx, int *type, int *bytes);
    , 'taos_stmt_get_param': [ref.types.int, [ref.types.void_ptr, ref.refType(ref.types.int), ref.refType(ref.types.int), ref.refType(ref.types.int)]]

    // int taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind) 
    , 'taos_stmt_bind_param_batch': [ref.types.int, [ref.types.void_ptr, ref.types.void_ptr]]

    // int taos_stmt_bind_single_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind, int colIdx)  
    , 'taos_stmt_bind_single_param_batch': [ref.types.int, [ref.types.void_ptr, ref.types.void_ptr, ref.types.int]]

    // int taos_stmt_add_batch(TAOS_STMT *stmt)
    , 'taos_stmt_add_batch': [ref.types.int, [ref.types.void_ptr]]

    // int taos_stmt_execute(TAOS_STMT *stmt) 
    , 'taos_stmt_execute': [ref.types.int, [ref.types.void_ptr]]

    // TAOS_RES* taos_stmt_use_result(TAOS_STMT *stmt)  
    , 'taos_stmt_use_result': [ref.types.void_ptr, [ref.types.void_ptr]]

    // int taos_stmt_close(TAOS_STMT *stmt) 
    , 'taos_stmt_close': [ref.types.int, [ref.types.void_ptr]]

    // char * taos_stmt_errstr(TAOS_STMT *stmt)
    , 'taos_stmt_errstr': [ref.types.char_ptr, [ref.types.void_ptr]]

    // int taos_stmt_affected_rows(TAOS_STMT *stmt);
    , 'taos_stmt_affected_rows': [ref.types.int, [ref.types.void_ptr]]

    // int taos_stmt_affected_rows_once(TAOS_STMT *stmt);
    , 'taos_stmt_affected_rows_once': [ref.types.int, [ref.types.void_ptr]]

    // int taos_load_table_info(TAOS *taos, const char* tableNameList)
    , 'taos_load_table_info': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr]]

    // =======new 2022/07/07=====
    //int taos_fetch_raw_block(TAOS_RES *res, int *numOfRows, void **pData);
    , 'taos_fetch_raw_block': [ref.types.int, [ref.types.void_ptr, 'void*', 'void**']]
    // ,'taos_fetch_raw_block':[ref.types.int,[ref.types.void_ptr,'int *',ref.refType(ref.refType(ref.types.void_ptr))]]

    //taos_validate_sql(TAOS *taos, const char *sql);
    , 'taos_validate_sql': [ref.types.int, [ref.types.void_ptr, ref.types.char_ptr]]

    //char *taos_get_server_info(TAOS *taos);
    , 'taos_get_server_info': [ref.types.char_ptr, [ref.types.void_ptr]]

    //char *taos_get_client_info();
    , 'taos_get_client_info': [ref.types.char_ptr, []]

    //TSDB_SERVER_STATUS taos_check_server_status(const char *fqdn, int port, char *details, int maxlen);
    // ,'taos_check_server_status':[ref.types.int,[ref.types.char_ptr,ref.types.int,ref.types.char_ptr,ref.types.int]]   
   
});
module.exports = libtaos;
