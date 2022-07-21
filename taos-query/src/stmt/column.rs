use std::os::raw::{c_char, c_int, c_void};

use crate::common::{itypes::IsValue, Ty};

#[repr(C)]
#[derive(Debug, Clone)]
pub struct RawMultiBind {
    pub buffer_type: c_int,
    pub buffer: *const c_void,
    pub buffer_length: usize,
    pub length: *const i32,
    pub is_null: *const c_char,
    pub num: c_int,
}

impl RawMultiBind {
    pub fn new(ty: Ty) -> Self {
        Self {
            buffer_type: ty as _,
            buffer: std::ptr::null_mut(),
            buffer_length: 0,
            length: std::ptr::null_mut(),
            is_null: std::ptr::null_mut(),
            num: 1,
        }
    }
}

pub trait BindFrom: Sized {
    fn null() -> Self;
    fn from_primitive<T: IsValue>(v: &T) -> Self;
    fn from_timestamp(v: i64) -> Self;
    fn from_varchar(v: &str) -> Self;
    fn from_nchar(v: &str) -> Self;
    fn from_json(v: &str) -> Self;
    fn from_binary(v: &str) -> Self {
        Self::from_varchar(v)
    }
}

fn box_into_raw<T>(v: T) -> *mut T {
    Box::into_raw(Box::new(v))
}

impl BindFrom for RawMultiBind {
    #[inline]
    fn null() -> Self {
        RawMultiBind {
            buffer_type: Ty::Null as _,
            buffer_length: 0,
            buffer: std::ptr::null_mut(),
            length: std::ptr::null_mut(),
            is_null: std::ptr::null_mut(),
            num: 1 as _,
        }
    }

    fn from_primitive<T: IsValue>(v: &T) -> Self {
        let mut param = RawMultiBind::new(T::TY);
        param.buffer_length = T::TY.fixed_length();
        param.buffer = box_into_raw(v.clone()) as *const T as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0) as _;
        param
    }

    fn from_timestamp(v: i64) -> Self {
        let mut param = RawMultiBind::new(Ty::Timestamp);
        param.buffer_length = std::mem::size_of::<i64>();
        param.buffer = box_into_raw(v) as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0i8) as _;
        param
    }

    fn from_varchar(v: &str) -> Self {
        let mut param = RawMultiBind::new(Ty::VarChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0i8) as _;
        param
    }
    fn from_json(v: &str) -> Self {
        let mut param = RawMultiBind::new(Ty::Json);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0i8) as _;
        param
    }
    fn from_nchar(v: &str) -> Self {
        let mut param = RawMultiBind::new(Ty::NChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0i8) as _;
        param
    }
}
