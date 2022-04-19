use std::{ffi::CStr, os::raw::*};

mod data;
pub use data::*;

mod field;
pub use field::TAOS_FIELD;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum TSDB_OPTION {
    Locale = 0,
    Charset,
    Timezone,
    ConfigDir,
    ShellActivityTimer,
    MaxOptions,
}
pub const TSDB_OPTION_LOCALE: TSDB_OPTION = TSDB_OPTION::Locale;
pub const TSDB_OPTION_CHARSET: TSDB_OPTION = TSDB_OPTION::Charset;
pub const TSDB_OPTION_TIMEZONE: TSDB_OPTION = TSDB_OPTION::Timezone;
pub const TSDB_OPTION_CONFIGDIR: TSDB_OPTION = TSDB_OPTION::Locale;
pub const TSDB_OPTION_SHELL_ACTIVITY_TIMER: TSDB_OPTION = TSDB_OPTION::ShellActivityTimer;
pub const TSDB_MAX_OPTIONS: TSDB_OPTION = TSDB_OPTION::MaxOptions;


use num_enum::FromPrimitive;

#[repr(i32)]
#[derive(Debug, Copy, Clone, FromPrimitive)]
#[cfg_attr(
    feature = "serde",
    derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)
)]
pub enum Precision {
    #[num_enum(default)]
    Millisecond = 0,
    Microsecond,
    Nanosecond,
}

pub const TIMESTAMP_MILLISECOND: Precision = Precision::Millisecond;
pub const TIMESTAMP_MICROSECOND: Precision = Precision::Microsecond;
pub const TIMESTAMP_NANOSECOND: Precision = Precision::Nanosecond;


#[repr(C)]
#[derive(Copy, Clone)]
pub struct TAOS_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub buffer_length: usize,
    pub length: *mut usize,
    pub is_null: *mut c_int,
    pub is_unsigned: c_int,
    pub error: *mut c_int,
    pub u: taos_bind_field_anonym_union,
    pub allocated: c_uint,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub union taos_bind_field_anonym_union {
    pub ts: i64,
    pub b: i8,
    pub v1: i8,
    pub v2: i16,
    pub v4: i32,
    pub v8: i64,
    pub f4: f32,
    pub f8: f64,
    pub bin: *mut c_uchar,
    pub nchar: *mut c_char,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TAOS_MULTI_BIND {
    pub buffer_type: c_int,
    pub buffer: *const c_void,
    pub buffer_length: usize,
    pub length: *const i32,
    pub is_null: *const c_char,
    pub num: c_int,
}
