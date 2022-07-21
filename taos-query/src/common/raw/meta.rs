use std::ffi::c_void;

use bytes::Bytes;

#[derive(Debug)]
pub struct RawMeta {
    data: Bytes,
    raw_meta_type: i16,
}

impl RawMeta {
    pub fn new(ptr: *const c_void) -> Self {
        Self {
            data: Bytes::new(),
            raw_meta_type: 0,
        }
    }
}
