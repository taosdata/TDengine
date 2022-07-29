use std::{borrow::Cow, os::raw::*};

use taos_macros::c_cfg;
use taos_query::common::raw_data_t;

use crate::ffi::{TAOS, TAOS_RES};

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct tmq_resp_err_t(i32);

impl PartialEq<i32> for tmq_conf_res_t {
    fn eq(&self, other: &i32) -> bool {
        self == other
    }
}

impl tmq_resp_err_t {
    pub fn ok_or(self, s: impl Into<Cow<'static, str>>) -> Result<(), taos_error::Error> {
        match self {
            Self(0) => Ok(()),
            _ => Err(taos_error::Error::from_string(s.into())),
        }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_t {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_conf_t {
    _unused: [u8; 0],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_list_t {
    _unused: [u8; 0],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_message_t {
    _unused: [u8; 0],
}

#[repr(C)]
pub enum tmq_conf_res_t {
    Unknown = -2,
    Invalid = -1,
    Ok = 0,
}

impl tmq_conf_res_t {
    pub fn ok(self, k: &str, v: &str) -> Result<(), taos_error::Error> {
        match self {
            Self::Ok => Ok(()),
            Self::Invalid => Err(taos_error::Error::from_string(format!(
                "Invalid key value pair ({k}, {v})"
            ))),
            Self::Unknown => Err(taos_error::Error::from_string(format!("Unknown key {k}"))),
        }
    }
}

pub type tmq_commit_cb =
    unsafe extern "C" fn(tmq: *mut tmq_t, resp: tmq_resp_err_t, param: *mut c_void);

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum tmq_res_t {
    TMQ_RES_INVALID = -1,
    TMQ_RES_DATA = 1,
    TMQ_RES_TABLE_META = 2,
}

// #[repr(C)]
// #[derive(Debug)]
// pub struct raw_data_t {
//     pub raw: *mut c_void,
//     pub raw_len: u32,
//     pub raw_type: u16,
// }
// TMQ streaming/consuming API.
#[c_cfg(taos_tmq)]
extern "C" {
    pub fn tmq_list_new() -> *mut tmq_list_t;
    pub fn tmq_list_append(arg1: *mut tmq_list_t, arg2: *const c_char) -> i32;
    pub fn tmq_list_destroy(list: *mut tmq_list_t);
    pub fn tmq_list_get_size(list: *const tmq_list_t) -> i32;
    pub fn tmq_list_to_c_array(list: *const tmq_list_t) -> *mut *mut c_char;

    pub fn tmq_consumer_new(
        conf: *mut tmq_conf_t,
        errstr: *mut c_char,
        errstr_len: i32,
    ) -> *mut tmq_t;

    pub fn tmq_err2str(err: tmq_resp_err_t) -> *const c_char;

    pub fn tmq_subscribe(tmq: *mut tmq_t, topic_list: *mut tmq_list_t) -> tmq_resp_err_t;
    pub fn tmq_unsubscribe(tmq: *mut tmq_t) -> tmq_resp_err_t;

    pub fn tmq_subscription(tmq: *mut tmq_t, topic_list: *mut *mut tmq_list_t) -> tmq_resp_err_t;

    pub fn tmq_consumer_poll(tmq: *mut tmq_t, blocking_time: i64) -> *mut TAOS_RES;

    pub fn tmq_consumer_close(tmq: *mut tmq_t) -> tmq_resp_err_t;

    pub fn tmq_commit_sync(tmq: *mut tmq_t, msg: *const TAOS_RES) -> tmq_resp_err_t;

    pub fn tmq_commit_async(
        tmq: *mut tmq_t,
        msg: *const TAOS_RES,
        cb: tmq_commit_cb,
        param: *mut c_void,
    );

    pub fn tmq_get_raw(res: *mut TAOS_RES, meta: *mut raw_data_t) -> i32;
    pub fn tmq_write_raw(taos: *mut TAOS, meta: raw_data_t) -> i32;

    pub fn taos_write_raw_block(
        taos: *mut TAOS,
        nrows: i32,
        ptr: *const c_char,
        tbname: *const c_char,
    ) -> i32;

    pub fn tmq_get_json_meta(res: *mut TAOS_RES) -> *mut c_char;
    pub fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char;
    pub fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char;
    pub fn tmq_get_db_name(res: *mut TAOS_RES) -> *const c_char;
    pub fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32;
}

#[cfg(taos_tmq)]
extern "C" {
    pub fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t;
}

#[cfg(not(taos_tmq))]
pub unsafe fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t {
    tmq_res_t::TMQ_RES_INVALID
}

// TMQ Conf API
#[c_cfg(taos_tmq)]
extern "C" {
    pub fn tmq_conf_new() -> *mut tmq_conf_t;

    pub fn tmq_conf_destroy(conf: *mut tmq_conf_t);

    pub fn tmq_conf_set(
        conf: *mut tmq_conf_t,
        key: *const c_char,
        value: *const c_char,
    ) -> tmq_conf_res_t;

    pub fn tmq_conf_set_auto_commit_cb(
        conf: *mut tmq_conf_t,
        cb: tmq_commit_cb,
        param: *mut c_void,
    );
}
