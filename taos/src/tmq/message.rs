use std::ffi::CStr;

use crate::{Result, Taos, TaosCode, TaosError, TaosResult, ToCString};
use taos_sys::*;

pub struct Message(pub(crate) *mut tmq_message_t);

impl Drop for Message {
    fn drop(&mut self) {
        unsafe {
            tmq_message_destroy(self.0);
        }
    }
}
