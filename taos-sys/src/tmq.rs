use std::os::raw::*;

use taos_macros::c_cfg;

use crate::{TAOS_FIELD, TAOS_ROW, ffi::TAOS_RES};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum tmq_resp_err_t {
    Fail = -1,
    Success = 0,
}

pub const TMQ_RESP_ERR__FAIL: tmq_resp_err_t = tmq_resp_err_t::Fail;
pub const TMQ_RESP_ERR__SUCCESS: tmq_resp_err_t = tmq_resp_err_t::Success;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_t {
    _unused: [u8; 0],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_topic_vgroup_t {
    _unused: [u8; 0],
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_topic_vgroup_list_t {
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

pub const TMQ_CONF_UNKNOWN: tmq_conf_res_t = tmq_conf_res_t::Unknown;
pub const TMQ_CONF_INVALID: tmq_conf_res_t = tmq_conf_res_t::Invalid;
pub const TMQ_CONF_OK: tmq_conf_res_t = tmq_conf_res_t::Ok;

pub type tmq_commit_cb = unsafe extern "C" fn(
    tmq: *mut tmq_t,
    resp: tmq_resp_err_t,
    topic: *mut tmq_topic_vgroup_list_t,
    param: *mut c_void,
);

// TMQ streaming/consuming API.
#[c_cfg(tmq)]
extern "C" {
    pub fn tmq_list_new() -> *mut tmq_list_t;
    pub fn tmq_list_append(arg1: *mut tmq_list_t, arg2: *const c_char) -> i32;
    pub fn tmq_list_destroy(list: *mut tmq_list_t);

    pub fn tmq_consumer_new(
        conn: *mut c_void,
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

    pub fn tmq_commit(
        tmq: *mut tmq_t,
        offsets: *const tmq_topic_vgroup_list_t,
        async_: i32,
    ) -> tmq_resp_err_t;

    pub fn tmq_seek(tmq: *mut tmq_t, offset: *const tmq_topic_vgroup_t) -> tmq_resp_err_t;
}

// TMQ message API
#[c_cfg(tmq)]
extern "C" {

    pub fn tmq_get_row(message: *mut tmq_message_t) -> TAOS_ROW;
    pub fn tmq_get_topic_name(message: *mut tmq_message_t) -> *const c_char;
    pub fn tmq_get_vgroup_id(message: *mut tmq_message_t) -> i32;
    pub fn tmq_get_request_offset(message: *mut tmq_message_t) -> i64;
    pub fn tmq_get_response_offset(message: *mut tmq_message_t) -> i64;
    pub fn tmq_get_fields(tmq: *const tmq_t, topic: *const c_char) -> *const TAOS_FIELD;
    pub fn tmq_field_count(tmq: *const tmq_t, topic: *const c_char) -> i32;

    pub fn tmq_message_destroy(message: *mut tmq_message_t);
}

// TMQ Conf API
#[c_cfg(tmq)]
extern "C" {
    pub fn tmq_conf_new() -> *mut tmq_conf_t;

    pub fn tmq_conf_destroy(conf: *mut tmq_conf_t);

    pub fn tmq_conf_set(
        conf: *mut tmq_conf_t,
        key: *const c_char,
        value: *const c_char,
    ) -> tmq_conf_res_t;

    pub fn tmq_conf_set_offset_commit_cb(conf: *mut tmq_conf_t, cb: tmq_commit_cb);
}

// temporary used function for demo only
#[c_cfg(tmq)]
extern "C" {
    pub fn tmqShowMsg(tmq_message: *const tmq_message_t);
    pub fn tmqGetSkipLogNum(tmq_message: *const tmq_message_t) -> i32;
}
