use std::{fmt::Debug, mem::ManuallyDrop, os::raw::*, ptr};

mod field;
use derive_more::Deref;
pub use field::*;
pub use taos_query::common::{Precision, Ty};

use taos_query::common::{itypes::*, BorrowedColumn, Column, ColumnView, Value};

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
pub const TSDB_OPTION_CONFIGDIR: TSDB_OPTION = TSDB_OPTION::ConfigDir;
pub const TSDB_OPTION_SHELL_ACTIVITY_TIMER: TSDB_OPTION = TSDB_OPTION::ShellActivityTimer;
pub const TSDB_MAX_OPTIONS: TSDB_OPTION = TSDB_OPTION::MaxOptions;

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

impl Debug for TaosBindV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaosBindV2")
            .field("buffer_type", &self.buffer_type)
            .field("buffer", &self.buffer)
            .field("buffer_length", &self.buffer_length)
            .field("length", &self.length)
            .field("is_null", &self.is_null)
            .field("is_unsigned", &self.is_unsigned)
            .field("error", &self.error)
            .field("allocated", &self.allocated)
            .finish()
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
}
impl BindFrom for TaosBindV3 {
    #[inline]
    fn null() -> Self {
        Self(TaosMultiBind {
            buffer_type: Ty::Null as _,
            buffer_length: 0,
            buffer: std::ptr::null_mut(),
            length: std::ptr::null_mut(),
            is_null: std::ptr::null_mut(),
            num: 1 as _,
        })
    }

    fn from_primitive<T: IsValue>(v: &T) -> Self {
        let mut param = TaosMultiBind::new(T::TY);
        param.buffer_length = v.fixed_length();
        param.buffer = box_into_raw(v.clone()) as *const T as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0) as _;
        Self(param)
    }

    fn from_timestamp(v: i64) -> Self {
        let mut param = TaosMultiBind::new(Ty::Timestamp);
        param.buffer_length = std::mem::size_of::<i64>();
        param.buffer = box_into_raw(v) as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0i8) as _;
        Self(param)
    }

    fn from_varchar(v: &str) -> Self {
        let mut param = TaosMultiBind::new(Ty::VarChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0i8) as _;
        Self(param)
    }
    fn from_json(v: &str) -> Self {
        let mut param = TaosMultiBind::new(Ty::Json);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0i8) as _;
        Self(param)
    }
    fn from_nchar(v: &str) -> Self {
        let mut param = TaosMultiBind::new(Ty::NChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param.is_null = box_into_raw(0i8) as _;
        Self(param)
    }
}

#[derive(Debug, Deref)]
#[repr(transparent)]
pub struct TaosBindV3(TaosMultiBind);

#[cfg(taos_v3)]
pub type TaosBind = TaosBindV3;
#[cfg(not(taos_v3))]
pub type TaosBind = TaosBindV2;

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

    pub(crate) fn buffer(&self) -> *const c_void {
        self.buffer
    }

    fn ty(&self) -> Ty {
        Ty::from(self.buffer_type)
    }

    #[inline]
    unsafe fn free(&mut self) {
        if self.ty() == Ty::Json && !self.buffer.is_null() {
            Vec::from_raw_parts(self.buffer as _, *self.length, *self.length);
        }
        if !self.length.is_null() {
            Box::from_raw(self.length);
        }
        if !self.is_null.is_null() {
            Box::from_raw(self.is_null);
        }
        if !self.error.is_null() {
            Box::from_raw(self.error);
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
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => todo!(),
            Value::Bool(_) => todo!(),
            Value::TinyInt(_) => todo!(),
            Value::SmallInt(_) => todo!(),
            Value::Int(_) => todo!(),
            Value::BigInt(_) => todo!(),
            Value::Float(_) => todo!(),
            Value::Double(_) => todo!(),
            Value::VarChar(_) => todo!(),
            Value::Timestamp(_) => todo!(),
            Value::NChar(_) => todo!(),
            Value::UTinyInt(_) => todo!(),
            Value::USmallInt(_) => todo!(),
            Value::UInt(_) => todo!(),
            Value::UBigInt(_) => todo!(),
            Value::Json(_) => todo!(),
            Value::VarBinary(_) => todo!(),
            Value::Decimal(_) => todo!(),
            Value::Blob(_) => todo!(),
            Value::MediumBlob(_) => todo!(),
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
        param.buffer = v as *const T as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param
    }
}

impl Drop for TaosBindV2 {
    fn drop(&mut self) {
        unsafe { self.free() }
    }
}

pub trait ToMultiBind {
    fn to_multi_bind(&self) -> TaosMultiBind;
}

impl TaosMultiBind {
    pub(crate) fn nulls(n: usize) -> Self {
        TaosMultiBind {
            buffer_type: Ty::Null as _,
            buffer: std::ptr::null_mut(),
            buffer_length: 0,
            length: n as _,
            is_null: std::ptr::null_mut(),
            num: n as _,
        }
    }
    pub(crate) fn from_primitives<T: IValue>(nulls: Vec<bool>, values: &[T]) -> Self {
        TaosMultiBind {
            buffer_type: T::TY as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<T>(),
            length: values.len() as _,
            is_null: ManuallyDrop::new(nulls).as_ptr() as _,
            num: values.len() as _,
        }
    }
    pub(crate) fn from_raw_timestamps(nulls: Vec<bool>, values: &[i64]) -> Self {
        TaosMultiBind {
            buffer_type: Ty::Timestamp as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: values.len() as _,
            is_null: ManuallyDrop::new(nulls).as_ptr() as _,
            num: values.len() as _,
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
                    std::intrinsics::copy_nonoverlapping(v.as_ptr(), dst, v.len());
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

    pub(crate) fn buffer(&self) -> *const c_void {
        self.buffer
    }
}

impl Drop for TaosMultiBind {
    fn drop(&mut self) {
        let ty = Ty::from(self.buffer_type as u8);
        if ty == Ty::VarChar || ty == Ty::NChar {
            let len = self.buffer_length * self.num as usize;
            unsafe { Vec::from_raw_parts(self.buffer as *mut u8, len, len as _) };
            unsafe { Vec::from_raw_parts(self.length as *mut i32, self.num as _, self.num as _) };
        }
        unsafe { Vec::from_raw_parts(self.is_null as *mut i8, self.num as _, self.num as _) };
    }
}

impl<'c> From<&'c Column> for TaosMultiBind {
    fn from(col: &'c Column) -> Self {
        match col {
            Column::Null(n) => Self::nulls(*n),
            Column::Bool(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::TinyInt(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::SmallInt(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::Int(nulls, values) => Self::from_primitives(nulls.clone().into_bools(), values),
            Column::BigInt(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::UTinyInt(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::USmallInt(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::UInt(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::UBigInt(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::Float(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::Double(nulls, values) => {
                Self::from_primitives(nulls.clone().into_bools(), values)
            }
            Column::Timestamp(nulls, values) => {
                Self::from_raw_timestamps(nulls.clone().into_bools(), values)
            }
            Column::Binary(values) => Self::from_binary_vec(values),
            Column::NChar(values) => Self::from_string_vec(values),
            Column::Json(_, _) => todo!(),
            Column::VarChar(_, _) => todo!(),
            Column::VarBinary(_, _) => todo!(),
            Column::Decimal(_, _) => todo!(),
            Column::Blob(_, _) => todo!(),
            // _ => unreachable!(),
        }
    }
}

impl<'b> From<BorrowedColumn<'b>> for TaosMultiBind {
    fn from(col: BorrowedColumn<'b>) -> Self {
        use BorrowedColumn::*;
        match col {
            Null(n) => Self::nulls(n),
            Bool(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            TinyInt(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            SmallInt(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            Int(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            BigInt(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            UTinyInt(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            USmallInt(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            UInt(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            UBigInt(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            Float(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            Double(nulls, values) => TaosMultiBind::from_primitives(nulls.into_bools(), values),
            Timestamp(nulls, values) => {
                TaosMultiBind::from_raw_timestamps(nulls.into_bools(), values)
            }
            Binary(values) => TaosMultiBind::from_binary_vec(&values),
            NChar(values) => TaosMultiBind::from_string_vec(&values),
            _ => unreachable!(),
        }
    }
}

impl<'b> From<&'b ColumnView> for TaosMultiBind {
    fn from(view: &'b ColumnView) -> Self {
        use ColumnView::*;
        match view {
            Bool(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            TinyInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            SmallInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            Int(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            BigInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            Float(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            Double(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            VarChar(view) => TaosMultiBind::from_binary_vec(&view.to_vec()),
            Timestamp(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_raw_timestamps(nulls, values)
            }
            NChar(view) => TaosMultiBind::from_string_vec(&view.to_vec()),
            UTinyInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            USmallInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            UInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            UBigInt(view) => {
                let nulls: Vec<_> = view.is_null_iter().collect();
                let values = view.as_raw_slice();
                TaosMultiBind::from_primitives(nulls, values)
            }
            Json(view) => TaosMultiBind::from_json(&view.to_vec()),
        }
    }
}
