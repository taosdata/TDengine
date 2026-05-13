use std::mem::ManuallyDrop;
use std::os::raw::*;
use std::ptr;
use std::{ffi::CString, fmt::Debug};

mod field;
use bytes::Bytes;
use derive_more::Deref;
pub use field::from_raw_fields;
use taos_query::common::itypes::*;
pub use taos_query::common::Ty;
use taos_query::common::{ColumnView, Value};

mod tmq;
pub use tmq::*;

#[allow(clippy::upper_case_acronyms)]
pub type TAOS = c_void;
pub type TAOS_STMT = c_void;
pub type TAOS_STMT2 = c_void;
pub type TAOS_RES = c_void;
pub type TAOS_ROW = *mut *mut c_void;

pub type taos_async_fetch_cb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut c_void, rows: c_int);

pub type taos_async_query_cb =
    unsafe extern "C" fn(param: *mut c_void, res: *mut c_void, code: c_int);

pub type taos_async_stmt2_exec_cb = taos_async_query_cb;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
pub enum TSDB_OPTION {
    Locale = 0,
    Charset,
    Timezone,
    ConfigDir,
    ShellActivityTimer,
    MaxOptions,
}

#[repr(C)]
#[derive(Clone)]
pub struct TaosBindV2 {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub buffer_length: usize,
    pub length: *mut usize,
    pub is_null: *mut c_int,
    pub is_unsigned: c_int,
    pub error: *mut c_int,
    pub u: TaosBindUnionV2,
    pub allocated: c_uint,
}

impl TaosBindV2 {
    #[inline]
    pub fn new(buffer_type: Ty) -> Self {
        let buffer: *mut c_void = ptr::null_mut();
        let length: *mut usize = ptr::null_mut();
        let is_null: *mut c_int = ptr::null_mut();
        let error: *mut c_int = ptr::null_mut();
        TaosBindV2 {
            buffer_type: buffer_type as _,
            buffer,
            buffer_length: 0,
            length,
            is_null,
            is_unsigned: 0,
            error,
            allocated: 1,
            u: TaosBindUnionV2 { ts: 0 },
        }
    }

    #[cfg(test)]
    pub(crate) fn buffer(&self) -> *const c_void {
        self.buffer
    }

    fn ty(&self) -> Ty {
        Ty::from(self.buffer_type)
    }

    #[inline]
    unsafe fn free(&mut self) {
        let ty = self.ty();
        match ty {
            Ty::Bool => {
                let _ = Box::from_raw(self.buffer as *mut bool);
            }
            Ty::TinyInt => {
                let _ = Box::from_raw(self.buffer as *mut i8);
            }
            Ty::SmallInt => {
                let _ = Box::from_raw(self.buffer as *mut i16);
            }
            Ty::Int => {
                let _ = Box::from_raw(self.buffer as *mut i32);
            }
            Ty::BigInt | Ty::Timestamp => {
                let _ = Box::from_raw(self.buffer as *mut i64);
            }
            Ty::UTinyInt => {
                let _ = Box::from_raw(self.buffer as *mut u8);
            }
            Ty::USmallInt => {
                let _ = Box::from_raw(self.buffer as *mut u16);
            }
            Ty::UInt => {
                let _ = Box::from_raw(self.buffer as *mut u32);
            }
            Ty::UBigInt => {
                let _ = Box::from_raw(self.buffer as *mut u64);
            }
            Ty::Float => {
                let _ = Box::from_raw(self.buffer as *mut f32);
            }
            Ty::Double => {
                let _ = Box::from_raw(self.buffer as *mut f64);
            }
            _ => (),
        };
        if !self.length.is_null() {
            let _ = Box::from_raw(self.length);
        }
        if !self.is_null.is_null() {
            let _ = Box::from_raw(self.is_null);
        }
        if !self.error.is_null() {
            let _ = Box::from_raw(self.error);
        }
    }
}

impl Debug for TaosBindV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.is_null.is_null(), self.buffer.is_null()) {
            (true, true) => unreachable!(),
            (false, true) => f.write_str("NULL"),
            (no_is_null, _) => unsafe {
                if !no_is_null && *(self.is_null as *const bool) {
                    f.write_str("NULL")
                } else {
                    match self.ty() {
                        Ty::Bool => (self.buffer as *const bool).read().fmt(f),
                        Ty::TinyInt => f
                            .debug_tuple("TinyInt")
                            .field(&*(self.buffer as *const i8))
                            .finish(),
                        Ty::SmallInt => f
                            .debug_tuple("SmallInt")
                            .field(&*(self.buffer as *const i16))
                            .finish(),
                        Ty::Int => f
                            .debug_tuple("Int")
                            .field(&*(self.buffer as *const i32))
                            .finish(),
                        Ty::BigInt => f
                            .debug_tuple("BigInt")
                            .field(&*(self.buffer as *const i64))
                            .finish(),
                        Ty::UTinyInt => f
                            .debug_tuple("UTinyInt")
                            .field(&*(self.buffer as *const u8))
                            .finish(),
                        Ty::USmallInt => f
                            .debug_tuple("USmallInt")
                            .field(&*(self.buffer as *const u16))
                            .finish(),
                        Ty::UInt => f
                            .debug_tuple("UInt")
                            .field(&*(self.buffer as *const u32))
                            .finish(),
                        Ty::UBigInt => f
                            .debug_tuple("UBigInt")
                            .field(&*(self.buffer as *const u64))
                            .finish(),
                        Ty::Float => f
                            .debug_tuple("Float")
                            .field(&*(self.buffer as *const f32))
                            .finish(),
                        Ty::Double => f
                            .debug_tuple("Double")
                            .field(&*(self.buffer as *const f64))
                            .finish(),
                        Ty::Timestamp => f
                            .debug_tuple("Timestamp")
                            .field(&*(self.buffer as *const i64))
                            .finish(),
                        Ty::VarChar => f
                            .debug_tuple("VarChar")
                            .field(&bytes::Bytes::from(std::slice::from_raw_parts(
                                self.buffer as *const u8,
                                *self.length,
                            )))
                            .finish(),
                        Ty::NChar => f
                            .debug_tuple("NChar")
                            .field(&bytes::Bytes::from(std::slice::from_raw_parts(
                                self.buffer as *const u8,
                                *self.length,
                            )))
                            .finish(),
                        Ty::Json => f
                            .debug_tuple("Json")
                            .field(&bytes::Bytes::from(std::slice::from_raw_parts(
                                self.buffer as *const u8,
                                *self.length,
                            )))
                            .finish(),
                        _ => unreachable!(),
                    }
                }
            },
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union TaosBindUnionV2 {
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
#[derive(Debug, Clone)]
pub struct TaosMultiBind {
    pub buffer_type: c_int,
    pub buffer: *const c_void,
    pub buffer_length: usize,
    pub length: *const i32,
    pub is_null: *const c_char,
    pub num: c_int,
}

impl TaosMultiBind {
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

    pub(crate) fn from_primitives_ptr<T: IValue>(nulls: Vec<bool>, ptr: *const T) -> Self {
        TaosMultiBind {
            buffer_type: T::TY as _,
            buffer: ptr as _,
            buffer_length: std::mem::size_of::<T>(),
            length: nulls.len() as _,
            num: nulls.len() as _,
            is_null: ManuallyDrop::new(nulls).as_ptr() as _,
        }
    }

    pub(crate) fn from_raw_timestamps_ptr(nulls: Vec<bool>, ptr: *const i64) -> Self {
        TaosMultiBind {
            buffer_type: Ty::Timestamp as _,
            buffer: ptr as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: nulls.len() as _,
            num: nulls.len() as _,
            is_null: ManuallyDrop::new(nulls).as_ptr() as _,
        }
    }

    pub(crate) fn from_binary_vec(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let mut buffer_length = 0;
        let num = values.len();
        let mut nulls = ManuallyDrop::new(Vec::with_capacity(num));
        unsafe { nulls.set_len(num) };
        nulls.fill(false);
        let mut length: ManuallyDrop<Vec<i32>> = ManuallyDrop::new(Vec::with_capacity(num));
        unsafe { length.set_len(num) };
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                length[i] = v.len() as _;
                if v.len() > buffer_length {
                    buffer_length = v.len();
                }
            } else {
                nulls[i] = true;
            }
        }
        let buffer_size = buffer_length * values.len();
        let mut buffer: ManuallyDrop<Vec<u8>> = ManuallyDrop::new(Vec::with_capacity(buffer_size));
        unsafe { buffer.set_len(buffer_size) };
        buffer.fill(0);
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                unsafe {
                    let dst = buffer.as_mut_ptr().add(buffer_length * i);
                    std::ptr::copy_nonoverlapping(v.as_ptr(), dst, v.len());
                }
            }
        }
        TaosMultiBind {
            buffer_type: Ty::VarChar as _,
            buffer: buffer.as_ptr() as _,
            buffer_length,
            length: length.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: num as _,
        }
    }

    pub(crate) fn from_string_vec(values: &[Option<impl AsRef<str>>]) -> Self {
        let values: Vec<_> = values
            .iter()
            .map(|f| f.as_ref().map(|s| s.as_ref().as_bytes()))
            .collect();
        let mut s = Self::from_binary_vec(&values);
        s.buffer_type = Ty::NChar as _;
        s
    }

    pub(crate) fn from_json(values: &[Option<impl AsRef<str>>]) -> Self {
        let values: Vec<_> = values
            .iter()
            .map(|f| f.as_ref().map(|s| s.as_ref().as_bytes()))
            .collect();
        let mut s = Self::from_binary_vec(&values);
        s.buffer_type = Ty::Json as _;
        s
    }

    pub(crate) fn from_bytes(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let mut s = Self::from_binary_vec(values);
        s.buffer_type = Ty::VarBinary as _;
        s
    }

    pub(crate) fn from_geobytes(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let mut s = Self::from_binary_vec(values);
        s.buffer_type = Ty::Geometry as _;
        s
    }

    #[cfg(test)]
    pub(crate) fn buffer(&self) -> *const c_void {
        self.buffer
    }
}

impl Drop for TaosMultiBind {
    fn drop(&mut self) {
        if !self.is_null.is_null() {
            unsafe { Vec::from_raw_parts(self.is_null as *mut i8, self.num as _, self.num as _) };
        }
    }
}

impl BindFrom for TaosBindV3 {
    #[inline]
    fn null() -> Self {
        Self(TaosMultiBind {
            buffer_type: Ty::Null as _,
            buffer_length: 0,
            buffer: std::ptr::null_mut(),
            length: std::ptr::null_mut(),
            is_null: box_into_raw(1) as _,
            num: 1 as _,
        })
    }

    fn from_primitive<T: IsValue + Clone>(v: &T) -> Self {
        let mut param = TaosMultiBind::new(T::TY);
        param.buffer_length = v.fixed_length();
        param.buffer = box_into_raw(v.clone()) as *const T as _;
        param.is_null = box_into_raw(0) as _;
        Self(param)
    }

    fn from_timestamp(v: i64) -> Self {
        let mut param = TaosMultiBind::new(Ty::Timestamp);
        param.buffer_length = std::mem::size_of::<i64>();
        param.buffer = box_into_raw(v) as _;
        param.length = box_into_raw(param.buffer_length as i32) as _;
        param.is_null = box_into_raw(0i8) as _;
        Self(param)
    }

    fn from_varchar(v: &str) -> Self {
        let mut param = TaosMultiBind::new(Ty::VarChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length as i32) as _;
        param.is_null = box_into_raw(0i8) as _;
        Self(param)
    }

    fn from_json(v: &str) -> Self {
        let mut param = TaosMultiBind::new(Ty::Json);
        let buffer = CString::new(v).expect("failed to build json tag");
        param.buffer_length = v.len();
        param.buffer = buffer.into_raw() as _;
        param.length = box_into_raw(param.buffer_length as i32) as _;
        param.is_null = box_into_raw(0i8) as _;
        Self(param)
    }

    fn from_nchar(v: &str) -> Self {
        let mut param = TaosMultiBind::new(Ty::NChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length as i32) as _;
        param.is_null = box_into_raw(0i8) as _;
        Self(param)
    }

    fn from_varbinary(_: &Bytes) -> Self {
        unreachable!("VarBinary not supported");
    }

    fn from_geometry(_: &Bytes) -> Self {
        unreachable!("Geometry not supported");
    }
}

#[derive(Debug, Deref)]
#[repr(transparent)]
pub struct TaosBindV3(TaosMultiBind);

impl TaosBindV3 {
    fn free(&self) {
        if !self.length.is_null() {
            let _ = unsafe { Box::from_raw(self.length as *mut i32) };
        }

        if !self.buffer.is_null() {
            let ty = Ty::from(self.buffer_type as u8);
            match ty {
                Ty::Bool => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut bool) };
                }
                Ty::TinyInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i8) };
                }
                Ty::SmallInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i16) };
                }
                Ty::Int => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i32) };
                }
                Ty::BigInt | Ty::Timestamp => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut i64) };
                }
                Ty::UTinyInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u8) };
                }
                Ty::USmallInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u16) };
                }
                Ty::UInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u32) };
                }
                Ty::UBigInt => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut u64) };
                }
                Ty::Float => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut f32) };
                }
                Ty::Double => {
                    let _ = unsafe { Box::from_raw(self.buffer as *mut f64) };
                }
                Ty::Json => {
                    let _ = unsafe { CString::from_raw(self.buffer as *mut c_char) };
                }
                _ => (),
            }
        }
    }
}

impl Drop for TaosBindV3 {
    fn drop(&mut self) {
        self.free();
    }
}

pub trait BindFrom: Sized {
    fn null() -> Self;
    fn from_primitive<T: IsValue + Clone>(v: &T) -> Self;
    fn from_timestamp(v: i64) -> Self;
    fn from_varchar(v: &str) -> Self;
    fn from_nchar(v: &str) -> Self;
    fn from_json(v: &str) -> Self;
    fn from_varbinary(v: &Bytes) -> Self;
    fn from_geometry(v: &Bytes) -> Self;
    #[allow(dead_code)]
    fn from_binary(v: &str) -> Self {
        Self::from_varchar(v)
    }
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Null(_) => Self::null(),
            Value::Bool(v) => Self::from_primitive(v),
            Value::TinyInt(v) => Self::from_primitive(v),
            Value::SmallInt(v) => Self::from_primitive(v),
            Value::Int(v) => Self::from_primitive(v),
            Value::BigInt(v) => Self::from_primitive(v),
            Value::UTinyInt(v) => Self::from_primitive(v),
            Value::USmallInt(v) => Self::from_primitive(v),
            Value::UInt(v) => Self::from_primitive(v),
            Value::UBigInt(v) => Self::from_primitive(v),
            Value::Float(v) => Self::from_primitive(v),
            Value::Double(v) => Self::from_primitive(v),
            Value::VarChar(v) => Self::from_varchar(v),
            Value::Timestamp(v) => Self::from_timestamp(v.as_raw_i64()),
            Value::NChar(v) => Self::from_nchar(v),
            Value::Json(v) => Self::from_json(&v.to_string()),
            Value::VarBinary(v) => Self::from_varbinary(v),
            Value::Geometry(v) => Self::from_geometry(v),
            _ => unreachable!("unsupported type"),
        }
    }
}

fn box_into_raw<T>(v: T) -> *mut T {
    Box::into_raw(Box::new(v))
}

impl BindFrom for TaosBindV2 {
    #[inline]
    fn null() -> Self {
        let mut null = Self::new(Ty::Null);
        let v = Box::new(1i8);
        null.is_null = Box::into_raw(v) as _;
        null
    }

    fn from_timestamp(v: i64) -> Self {
        let mut param = Self::new(Ty::Timestamp);
        param.buffer_length = std::mem::size_of::<i64>();
        param.buffer = box_into_raw(v) as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param
    }

    fn from_varchar(v: &str) -> Self {
        let mut param = Self::new(Ty::VarChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param
    }

    fn from_json(v: &str) -> Self {
        let mut param = Self::new(Ty::Json);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param
    }

    fn from_nchar(v: &str) -> Self {
        let mut param = Self::new(Ty::NChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param
    }

    fn from_primitive<T: IsValue>(v: &T) -> Self {
        let mut param = Self::new(T::TY);
        param.buffer_length = v.fixed_length();
        param.buffer = box_into_raw(v.clone()) as *const T as _;
        param
    }

    fn from_varbinary(_: &Bytes) -> Self {
        unreachable!("VarBinary not supported");
    }

    fn from_geometry(_: &Bytes) -> Self {
        unreachable!("Geometry not supported");
    }
}

impl Drop for TaosBindV2 {
    fn drop(&mut self) {
        unsafe { self.free() }
    }
}

impl<'b> From<&'b ColumnView> for DropMultiBind {
    fn from(view: &'b ColumnView) -> Self {
        use ColumnView::*;
        match view {
            Bool(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            TinyInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            SmallInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            Int(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            BigInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            Float(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            Double(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            Timestamp(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_raw_timestamps_ptr(
                    nulls,
                    view.as_raw_ptr(),
                ))
            }
            UTinyInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            USmallInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            UInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            UBigInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                DropMultiBind::new(TaosMultiBind::from_primitives_ptr(nulls, view.as_raw_ptr()))
            }
            VarChar(view) => DropMultiBind::new(TaosMultiBind::from_binary_vec(&view.to_vec())),
            NChar(view) => DropMultiBind::new(TaosMultiBind::from_string_vec(&view.to_vec())),
            Json(view) => DropMultiBind::new(TaosMultiBind::from_json(&view.to_vec())),
            VarBinary(view) => DropMultiBind::new(TaosMultiBind::from_bytes(&view.to_vec())),
            Geometry(view) => DropMultiBind::new(TaosMultiBind::from_geobytes(&view.to_vec())),
            Decimal(_) | Decimal64(_) => unreachable!("decimal type is not supported in stmt"),
            Blob(_) => unreachable!("blob type is not supported in stmt"),
        }
    }
}

pub struct DropMultiBind(TaosMultiBind);

impl DropMultiBind {
    fn new(taos_multi_bind: TaosMultiBind) -> Self {
        Self(taos_multi_bind)
    }
}

impl Drop for DropMultiBind {
    fn drop(&mut self) {
        let ty = Ty::from(self.0.buffer_type as u8);
        use Ty::*;
        if matches!(ty, VarChar | NChar | Json | VarBinary | Geometry) {
            let len = self.0.buffer_length * self.0.num as usize;
            unsafe {
                Vec::from_raw_parts(self.0.buffer as *mut u8, len, len);
                Vec::from_raw_parts(self.0.length as *mut i32, self.0.num as _, self.0.num as _);
            }
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct TaosStmt2Option {
    pub reqid: i64,
    pub single_stb_insert: bool,
    pub single_table_bind_once: bool,
    pub async_exec_fn: taos_async_stmt2_exec_cb,
    pub userdata: *mut c_void,
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct TaosStmt2Bind {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct TaosStmt2Bindv {
    pub count: c_int,
    pub tbnames: *mut *mut c_char,
    pub tags: *mut *mut TaosStmt2Bind,
    pub bind_cols: *mut *mut TaosStmt2Bind,
}
