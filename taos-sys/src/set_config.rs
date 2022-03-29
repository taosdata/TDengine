use std::os::raw::*;

pub const SET_CONF_RET_SUCC: SET_CONF_RET_CODE = SET_CONF_RET_CODE::Succ;
pub const SET_CONF_RET_ERR_PART: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrPart;
pub const SET_CONF_RET_ERR_INNER: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrInner;
pub const SET_CONF_RET_ERR_JSON_INVALID: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrJsonInvalid;
pub const SET_CONF_RET_ERR_JSON_PARSE: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrJsonParse;
pub const SET_CONF_RET_ERR_ONLY_ONCE: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrOnlyOnce;
pub const SET_CONF_RET_ERR_TOO_LONG: SET_CONF_RET_CODE = SET_CONF_RET_CODE::ErrTooLong;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct SetConfRet {
    pub code: SET_CONF_RET_CODE,
    pub msg: [c_char; 1024usize],
}

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum SET_CONF_RET_CODE {
    Succ = 0,
    ErrPart = -1,
    ErrInner = -2,
    ErrJsonInvalid = -3,
    ErrJsonParse = -4,
    ErrOnlyOnce = -5,
    ErrTooLong = -6,
}

extern "C" {
    pub fn taos_set_config(config: *const c_char) -> SetConfRet;
}
