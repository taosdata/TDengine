use crate::{Error, IntoCStr, Result};
use taos_sys::*;
/* tmq conf */
pub struct TmqConf(*mut tmq_conf_t);

impl TmqConf {
    pub(crate) fn as_ptr(&self) -> *mut tmq_conf_t {
        self.0
    }
    pub fn new() -> Self {
        Self(unsafe { tmq_conf_new() })
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<&mut Self> {
        let ret = unsafe {
            tmq_conf_set(
                self.0,
                key.into_c_str().as_ptr(),
                value.into_c_str().as_ptr(),
            )
        };
        match ret {
            tmq_conf_res_t::Ok => Ok(self),
            tmq_conf_res_t::Invalid => Err(Error::from_string("invalid key value set for tmq")),
            tmq_conf_res_t::Unknown => Err(Error::from_string("unknown key for tmq conf")),
        }
    }

    pub fn set_offset_commit_cb(&mut self, cb: tmq_commit_cb) -> () {
        unsafe {
            tmq_conf_set_offset_commit_cb(self.0, cb);
        }
    }
}

impl Drop for TmqConf {
    fn drop(&mut self) {
        unsafe { tmq_conf_destroy(self.0) }
    }
}
