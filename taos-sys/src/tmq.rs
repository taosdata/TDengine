use std::{borrow::Cow, os::raw::*};

use taos_macros::c_cfg;

use crate::{ffi::TAOS_RES, TAOS_FIELD, TAOS_ROW};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum tmq_resp_err_t {
    Fail = -1,
    Success = 0,
}

impl tmq_resp_err_t {
    pub fn ok_or(self, s: impl Into<Cow<'static, str>>) -> Result<(), taos_error::Error> {
        match self {
            tmq_resp_err_t::Success => Ok(()),
            tmq_resp_err_t::Fail => Err(taos_error::Error::from_string(s.into())),
        }
    }
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

    pub fn tmq_subscription(tmq: *mut tmq_t, topic_list: *mut *mut tmq_list_t) -> tmq_resp_err_t;

    pub fn tmq_consumer_poll(tmq: *mut tmq_t, blocking_time: i64) -> *mut TAOS_RES;

    pub fn tmq_consumer_close(tmq: *mut tmq_t) -> tmq_resp_err_t;

    pub fn tmq_commit(
        tmq: *mut tmq_t,
        offsets: *const tmq_topic_vgroup_list_t,
        async_: i32,
    ) -> tmq_resp_err_t;

    pub fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char;
    pub fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char;
    pub fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32;
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

    pub fn tmq_conf_set_offset_commit_cb(
        conf: *mut tmq_conf_t,
        cb: tmq_commit_cb,
        param: *mut c_void,
    );
}
