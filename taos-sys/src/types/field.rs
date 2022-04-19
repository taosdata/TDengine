use std::{borrow::Cow, ffi::CStr};

// use super::Ty;
use taos_query::common::{Field, Ty};

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
    pub fn type_(&self) -> Ty {
        self.type_.into()
    }

    pub fn bytes(&self) -> u32 {
        self.bytes as _
    }
}

fn from_v2<'a>(fields: &'a [TAOS_FIELD]) -> Cow<'a, [Field]> {
    let f: Vec<Field> = fields
        .into_iter()
        .map(|field| {
            Field::new(
                &field.name().to_string_lossy(),
                field.type_(),
                field.bytes(),
            )
        })
        .collect::<Vec<_>>();
    Cow::Owned(f)
}

#[cfg(taos_v3)]
pub fn from_raw_fields<'a>(ptr: *const TAOS_FIELD, len: usize) -> Cow<'a, [Field]> {
    // let f: &'a [Field] = unsafe { std::mem::transmute(fields) };
    let ptr: *const Field = unsafe { std::mem::transmute(ptr) };
    Cow::Borrowed(unsafe { std::slice::from_raw_parts(ptr, len) })
}

#[cfg(not(taos_v3))]
pub fn from_raw_fields<'a>(ptr: *const TAOS_FIELD, len: usize) -> Cow<'a, [Field]> {
    let raw = unsafe { std::slice::from_raw_parts(ptr, len) };
    let fields: Vec<Field> = raw
        .into_iter()
        .map(|field| {
            Field::new(
                &field.name().to_string_lossy(),
                field.type_(),
                field.bytes(),
            )
        })
        .collect();
    Cow::Owned(fields)
}
