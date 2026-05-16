use std::borrow::Cow;
use std::ffi::c_void;

use taos_query::prelude::RawError;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct tmq_resp_err_t(pub i32);

impl tmq_resp_err_t {
    pub const OK: i32 = 0;

    pub fn is_ok(self) -> bool {
        self.0 == Self::OK
    }

    pub fn is_err(self) -> bool {
        !self.is_ok()
    }

    pub fn ok_or<T: Into<Cow<'static, str>>>(self, s: T) -> Result<(), RawError> {
        match self {
            Self(0) => Ok(()),
            _ => Err(RawError::new(self.0, s.into())),
        }
    }
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct tmq_t {
    _unused: [u8; 0],
}

#[derive(Debug)]
pub struct SafeTmqT(pub *mut tmq_t);

unsafe impl Send for SafeTmqT {}
unsafe impl Sync for SafeTmqT {}

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
#[allow(dead_code)]
#[derive(PartialEq, Eq)]
pub enum tmq_conf_res_t {
    Unknown = -2,
    Invalid = -1,
    Ok = 0,
}

impl tmq_conf_res_t {
    pub fn ok(self, k: &str, v: &str) -> Result<(), RawError> {
        match self {
            Self::Ok => Ok(()),
            Self::Invalid => Err(RawError::from_string(format!(
                "Invalid key value pair ({k}, {v})"
            ))),
            Self::Unknown => {
                if k == "msg.enable.batchmeta" {
                    tracing::warn!(
                        "Ignore unsupported `msg.enable.batchmeta` setting, fallback to builtin settings"
                    );
                    return Ok(());
                }
                tracing::warn!(
                    consumer.conf.key = k,
                    consumer.conf.value = v,
                    "Unknown key {k}"
                );
                Err(RawError::from_string(format!("Unknown key {k}")))
            }
        }
    }
}

pub type tmq_commit_cb =
    unsafe extern "C" fn(tmq: *mut tmq_t, resp: tmq_resp_err_t, param: *mut c_void);

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[allow(dead_code)]
#[non_exhaustive]
pub enum tmq_res_t {
    TMQ_RES_INVALID = -1,
    TMQ_RES_DATA = 1,
    TMQ_RES_TABLE_META = 2,
    TMQ_RES_METADATA = 3,
    TMQ_RES_RAWDATA = 4,
}
