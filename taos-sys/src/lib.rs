#![allow(non_camel_case_types)]
use std::{ffi::CStr, os::raw::*, str::Utf8Error};

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

mod common;
pub use common::*;

mod set_config;
pub use set_config::*;

mod time;
pub use time::*;

mod basic;
pub use basic::*;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TAOS_FIELD {
    pub name: [u8; 65usize],
    pub type_: u8,
    pub bytes: i16,
}

impl TAOS_FIELD {
    pub fn name(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.name.as_ptr() as _) }
        // CStr::from_bytes_with_nul(&self.name).expect("field name should always be valid C-str")
    }
    pub fn type_(&self) -> TaosDataType {
        self.type_.into()
    }

    pub fn bytes(&self) -> i16 {
        self.bytes
    }
}

#[cfg(feature = "serde")]
impl<'de, 'a> serde::de::Deserializer<'de> for &'a TAOS_FIELD {
    type Error = taos_error::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.name()
            .to_str()
            .map_err(|err| taos_error::Error::from_string(format!("{}", err)))
            .and_then(|s| visitor.visit_str(s))
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}
#[cfg(feature = "serde")]
impl<'de, 'a> serde::de::IntoDeserializer<'de, taos_error::Error> for &'a TAOS_FIELD {
    type Deserializer = &'a TAOS_FIELD;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct TAOS_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub buffer_length: usize,
    pub length: *mut usize,
    pub is_null: *mut c_int,
    pub is_unsigned: c_int,
    pub error: *mut c_int,
    pub u: taos_bind_field_anonym_union,
    pub allocated: c_uint,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub union taos_bind_field_anonym_union {
    pub ts: i64,
    pub b: i8,
    pub v1: i8,
    pub v2: i16,
    pub v4: i32,
    pub v8: i64,
    pub f4: f32,
    pub f8: f64,
    pub bin: *mut c_uchar,
    pub nchar: *mut c_char,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TAOS_MULTI_BIND {
    pub buffer_type: c_int,
    pub buffer: *const c_void,
    pub buffer_length: usize,
    pub length: *const i32,
    pub is_null: *const c_char,
    pub num: c_int,
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

extern "C" {
    pub fn taos_load_table_info(taos: *mut TAOS, tableNameList: *const c_char) -> c_int;

    pub fn taos_stmt_init(taos: *mut TAOS) -> *mut TAOS_STMT;

    pub fn taos_stmt_prepare(stmt: *mut TAOS_STMT, sql: *const c_char, length: c_ulong) -> c_int;

    pub fn taos_stmt_set_tbname_tags(
        stmt: *mut TAOS_STMT,
        name: *const c_char,
        tags: *mut TAOS_BIND,
    ) -> c_int;

    pub fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int;

    pub fn taos_stmt_set_sub_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int;

    pub fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int;

    pub fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int;

    pub fn taos_stmt_get_param(
        stmt: *mut TAOS_STMT,
        idx: c_int,
        type_: *mut c_int,
        bytes: *mut c_int,
    ) -> c_int;

    pub fn taos_stmt_bind_param(stmt: *mut TAOS_STMT, bind: *mut TAOS_BIND) -> c_int;

    pub fn taos_stmt_bind_param_batch(stmt: *mut TAOS_STMT, bind: *mut TAOS_MULTI_BIND) -> c_int;

    pub fn taos_stmt_bind_single_param_batch(
        stmt: *mut TAOS_STMT,
        bind: *mut TAOS_MULTI_BIND,
        colIdx: c_int,
    ) -> c_int;

    pub fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int;

    pub fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int;

    pub fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int;

    pub fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES;

    pub fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int;

    pub fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *const c_char;

    pub fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES;

    pub fn taos_fetch_row(res: *mut TAOS_RES) -> TAOS_ROW;

    pub fn taos_result_precision(res: *mut TAOS_RES) -> c_int;

    pub fn taos_free_result(res: *mut TAOS_RES);

    pub fn taos_field_count(res: *mut TAOS_RES) -> c_int;

    pub fn taos_num_fields(res: *mut TAOS_RES) -> c_int;

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

    #[cfg(taos_result_block)]
    pub fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW;

    pub fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int;

    pub fn taos_reset_current_db(taos: *mut TAOS);

    pub fn taos_get_server_info(taos: *mut TAOS) -> *mut c_char;

    pub fn taos_errstr(tres: *mut TAOS_RES) -> *mut c_char;

    pub fn taos_errno(tres: *mut TAOS_RES) -> c_int;

}

#[cfg(not(taos_result_block))]
pub extern "C" fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    todo!()
}

pub mod query_a;
pub use query_a::*;

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

mod schemaless;
pub use schemaless::*;

mod tmq;
pub use tmq::*;
