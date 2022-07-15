use std::os::raw::*;
use taos_macros::c_cfg;

use crate::types::*;

pub type TAOS = c_void;
pub type TAOS_STMT = c_void;
pub type TAOS_RES = c_void;
pub type TAOS_STREAM = c_void;
pub type TAOS_SUB = c_void;
pub type TAOS_ROW = *mut *mut c_void;

pub type taos_subscribe_cb =
    unsafe extern "C" fn(sub: *mut TAOS_SUB, res: *mut TAOS_RES, param: *mut c_void, code: c_int);

pub type taos_stream_cb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut TAOS_RES, row: TAOS_ROW);

pub type taos_stream_close_cb = unsafe extern "C" fn(param: *mut c_void);

extern "C" {
    pub fn taos_cleanup();

    pub fn taos_options(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int;

    pub fn taos_get_client_info() -> *const c_char;

    pub fn taos_data_type(type_: c_int) -> *const c_char;
}

#[c_cfg(taos_parse_time)]
extern "C" {
    pub fn taos_parse_time(
        time_str: *const c_char,
        time: *mut i64,
        len: i32,
        time_precision: Precision,
        daylight: i8, // if in daylight saving time (DST) { 1 } else { 0 }
    ) -> i32;
}

extern "C" {
    pub fn taos_connect(
        ip: *const c_char,
        user: *const c_char,
        pass: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> *mut TAOS;

    pub fn taos_connect_auth(
        ip: *const c_char,
        user: *const c_char,
        auth: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> *mut TAOS;

    pub fn taos_close(taos: *mut TAOS);

}

pub type taos_async_fetch_cb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut c_void, rows: c_int);

pub type taos_async_query_cb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut c_void, code: c_int);

extern "C" {
    pub fn taos_fetch_rows_a(res: *mut TAOS_RES, fp: taos_async_fetch_cb, param: *mut c_void);

    pub fn taos_query_a(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: taos_async_query_cb,
        param: *mut c_void,
    );
}

extern "C" {
    pub fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> c_int;

    pub fn taos_stmt_init(taos: *mut TAOS) -> *mut TAOS_STMT;

    pub fn taos_stmt_prepare(stmt: *mut TAOS_STMT, sql: *const c_char, length: c_ulong) -> c_int;

    pub fn taos_stmt_set_tbname_tags(
        stmt: *mut TAOS_STMT,
        name: *const c_char,
        tags: *mut TaosBind,
    ) -> c_int;

    pub fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int;

    pub fn taos_stmt_set_tags(stmt: *mut TAOS_STMT, tags: *mut TaosBind) -> c_int;

    pub fn taos_stmt_set_sub_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int;

    pub fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int;

    pub fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int;

    pub fn taos_stmt_get_param(
        stmt: *mut TAOS_STMT,
        idx: c_int,
        type_: *mut c_int,
        bytes: *mut c_int,
    ) -> c_int;

    pub fn taos_stmt_bind_param(stmt: *mut TAOS_STMT, bind: *const TaosBind) -> c_int;

    pub fn taos_stmt_bind_param_batch(stmt: *mut TAOS_STMT, bind: *const TaosMultiBind) -> c_int;

    pub fn taos_stmt_bind_single_param_batch(
        stmt: *mut TAOS_STMT,
        bind: *const TaosMultiBind,
        colIdx: c_int,
    ) -> c_int;

    pub fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int;

    pub fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int;

    pub fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int;

    pub fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES;

    pub fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int;

    pub fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *const c_char;
}

extern "C" {
    pub fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES;

    pub fn taos_fetch_row(res: *mut TAOS_RES) -> TAOS_ROW;

    pub fn taos_result_precision(res: *mut TAOS_RES) -> c_int;

    pub fn taos_free_result(res: *mut TAOS_RES);

    pub fn taos_field_count(res: *mut TAOS_RES) -> c_int;

    pub fn taos_affected_rows(res: *mut TAOS_RES) -> c_int;

    pub fn taos_fetch_fields(res: *mut TAOS_RES) -> *mut TAOS_FIELD;

    pub fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> c_int;

    pub fn taos_print_row(
        str_: *mut c_char,
        row: TAOS_ROW,
        fields: *mut TAOS_FIELD,
        num_fields: c_int,
    ) -> c_int;

    pub fn taos_stop_query(res: *mut TAOS_RES);

    pub fn taos_is_null(res: *mut TAOS_RES, row: i32, col: i32) -> bool;

    pub fn taos_is_update_query(res: *mut TAOS_RES) -> bool;

    pub fn taos_fetch_block(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int;

    pub fn taos_fetch_lengths(res: *mut TAOS_RES) -> *mut c_int;

    pub fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int;

    pub fn taos_reset_current_db(taos: *mut TAOS);

    pub fn taos_get_server_info(taos: *mut TAOS) -> *mut c_char;

    pub fn taos_errstr(tres: *mut TAOS_RES) -> *mut c_char;

    pub fn taos_errno(tres: *mut TAOS_RES) -> c_int;

}

#[c_cfg(taos_v3)]
extern "C" {
    pub fn taos_get_column_data_offset(res: *mut TAOS_RES, col: i32) -> *mut i32;

    pub fn taos_fetch_raw_block(res: *mut TAOS_RES, num: *mut i32, data: *mut *mut c_void)
        -> c_int;

    pub fn taos_fetch_raw_block_a(res: *mut TAOS_RES, fp: taos_async_fetch_cb, param: *mut c_void);

    pub fn taos_get_raw_block(taos: *mut TAOS_RES) -> *mut c_void;
}

#[c_cfg(taos_result_block)]
extern "C" {
    pub fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW;
}

#[cfg(taos_fetch_block_s)]
extern "C" {
    pub fn taos_fetch_block_s(
        res: *mut TAOS_RES,
        num_of_rows: *mut c_int,
        rows: *mut TAOS_ROW,
    ) -> c_int;
}

#[cfg(not(taos_fetch_block_s))]
#[no_mangle]
pub unsafe extern "C" fn taos_fetch_block_s(
    res: *mut TAOS_RES,
    num_of_rows: *mut c_int,
    rows: *mut TAOS_ROW,
) -> c_int {
    *num_of_rows = taos_fetch_block(res, rows);
    return 0;
}

extern "C" {
    pub fn taos_subscribe(
        taos: *mut TAOS,
        restart: c_int,
        topic: *const c_char,
        sql: *const c_char,
        fp: Option<taos_subscribe_cb>,
        param: *mut c_void,
        interval: c_int,
    ) -> *mut TAOS_SUB;

    pub fn taos_consume(tsub: *mut TAOS_SUB) -> *mut TAOS_RES;

    pub fn taos_unsubscribe(tsub: *mut TAOS_SUB, keep_progress: c_int);
}

extern "C" {
    pub fn taos_open_stream(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: Option<taos_stream_cb>,
        stime: i64,
        param: *mut c_void,
        callback: Option<taos_stream_close_cb>,
    ) -> *mut TAOS_STREAM;

    pub fn taos_close_stream(stream: *mut TAOS_STREAM);
}
