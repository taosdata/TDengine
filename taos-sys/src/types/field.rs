use std::ffi::CStr;

// use super::Ty;
use taos_query::common::{Field, Ty};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TAOS_FIELD {
    pub name: [u8; 65usize],
    pub type_: u8,
    #[cfg(taos_v2)]
    pub bytes: i16,
    #[cfg(not(taos_v2))]
    pub bytes: i32,
}

impl TAOS_FIELD {
    pub fn name(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.name.as_ptr() as _) }
    }
    pub fn type_(&self) -> Ty {
        self.type_.into()
    }

    pub fn bytes(&self) -> u32 {
        self.bytes as _
    }
}

impl Into<Field> for &TAOS_FIELD {
    fn into(self) -> Field {
        Field::new(
            self.name()
                .to_str()
                .expect("invalid utf-8 field name")
                .to_string(),
            self.type_(),
            self.bytes(),
        )
    }
}

pub fn from_raw_fields<'a>(ptr: *const TAOS_FIELD, len: usize) -> Vec<Field> {
    unsafe { std::slice::from_raw_parts(ptr, len) }
        .into_iter()
        .map(Into::into)
        .collect()
}
