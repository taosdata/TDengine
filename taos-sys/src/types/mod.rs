use std::{
    any::{Any, TypeId},
    borrow::Cow,
    fmt::Debug,
    intrinsics::transmute,
    mem::ManuallyDrop,
    os::raw::*,
    ptr,
};

mod field;
use derive_more::Deref;
pub use field::*;
pub use taos_query::common::{Precision, Ty};

use taos_query::common::{itypes::*, BorrowedColumn, Column, Timestamp};

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
            buffer: std::ptr::null_mut(),
            buffer_length: 0,
            length: std::ptr::null_mut(),
            is_null: std::ptr::null_mut(),
            num: 1 as _,
        })
    }

    fn from_primitive<T: IsValue>(v: &T) -> Self {
        let mut param = TaosMultiBind::new(T::TY);
        param.buffer_length = T::TY.fixed_length();
        param.buffer = v as *const T as _;
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
    fn from_binary(v: &str) -> Self {
        Self::from_varchar(v)
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

    fn from_nchar(v: &str) -> Self {
        let mut param = Self::new(Ty::NChar);
        param.buffer_length = v.len();
        param.buffer = v.as_ptr() as _;
        param.length = box_into_raw(param.buffer_length) as _;
        param
    }

    fn from_primitive<T: IsValue>(v: &T) -> Self {
        let mut param = Self::new(T::TY);
        param.buffer_length = T::TY.fixed_length();
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

// impl<T> From<T> for TaosBind
// where
//     T: IValue + Any,
// {
//     fn from(value: T) -> Self {
//         match T::TY {
//             Ty::Null => Self::null(),
//             Ty::Bool => {
//                 let inner = value.into_inner();
//                 let inner: &bool = unsafe { std::mem::transmute(&inner) };
//                 Self::from_bool(*inner)
//             }
//             Ty::TinyInt => {
//                 let inner = value.into_inner();
//                 let inner: &i8 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_tiny_int(*inner)
//             }
//             Ty::SmallInt => {
//                 let inner = value.into_inner();
//                 let inner: &i16 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_small_int(*inner)
//             }
//             Ty::Int => {
//                 let inner = value.into_inner();
//                 let inner: &i32 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_int(*inner)
//             }
//             Ty::BigInt => {
//                 let inner = value.into_inner();
//                 let inner: &i64 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_big_int(*inner)
//             }

//             Ty::UTinyInt => {
//                 let inner = value.into_inner();
//                 let inner: &u8 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_tiny_int_unsigned(*inner)
//             }
//             Ty::USmallInt => {
//                 let inner = value.into_inner();
//                 let inner: &u16 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_small_int_unsigned(*inner)
//             }
//             Ty::UInt => {
//                 let inner = value.into_inner();
//                 let inner: &u32 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_int_unsigned(*inner)
//             }
//             Ty::UBigInt => {
//                 let inner = value.into_inner();
//                 let inner: &u64 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_big_int_unsigned(*inner)
//             }
//             Ty::Float => {
//                 let inner = value.into_inner();
//                 let inner: &f32 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_float(*inner)
//             }
//             Ty::Double => {
//                 let inner = value.into_inner();
//                 let inner: &f64 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_double(*inner)
//             }
//             Ty::Timestamp => {
//                 let inner = value.into_inner();
//                 let inner: &i64 = unsafe { std::mem::transmute(&inner) };
//                 Self::from_timestamp(*inner)
//             }
//             Ty::VarChar => {
//                 let inner = value.into_inner();
//                 let inner: &String = unsafe { std::mem::transmute(&inner) };
//                 Self::from_varchar(inner)
//             }
//             Ty::NChar => {
//                 let inner = value.into_inner();
//                 let inner: &String = unsafe { std::mem::transmute(&inner) };
//                 Self::from_nchar(inner)
//             }
//             Ty::Json => todo!(),
//             _ => Self::null(),
//         }
//     }
// }

// impl<T> From<Vec<T>> for TaosMultiBind
// where
//     T: IValue,
// {
//     fn from(_: Vec<T>) -> Self {
//         todo!()
//     }
// }

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
            .map(|f| {
                f.as_ref()
                    .map(|s| dbg!(s.as_ref().to_string()).into_bytes())
            })
            .collect();
        let mut s = Self::from_binary_vec(&values);
        s.buffer_type = Ty::NChar as _;
        s
    }

    pub(crate) fn buffer(&self) -> *const c_void {
        self.buffer
    }
}

impl Drop for TaosMultiBind {
    fn drop(&mut self) {
        let ty = Ty::from(self.buffer_type as u8);
        // if ty == Ty::VarChar || ty == Ty::NChar {
        //     let len = self.buffer_length * self.num as usize;
        //     unsafe { Vec::from_raw_parts(self.buffer as *mut u8, len, len as _) };
        //     unsafe { Vec::from_raw_parts(self.length as *mut i32, self.num as _, self.num as _) };
        // }
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
        match col {
            BorrowedColumn::Null(n) => todo!(),
            // BorrowedColumn::Bool(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::TinyInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::SmallInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::Int(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::BigInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::UTinyInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::USmallInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::UInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::UBigInt(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::Float(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::Double(nulls, values) => MultiBind::from_primitives(&nulls, values),
            // BorrowedColumn::Timestamp(nulls, values) => {
            //     MultiBind::from_raw_timestamps(&nulls, values)
            // }
            // BorrowedColumn::Binary(values) => MultiBind::from_binary_vec(&values),
            // BorrowedColumn::NChar(values) => MultiBind::from_string_vec(&values),
            _ => unreachable!(),
        }
    }
}

// impl<'b, 'c> From<&'c BorrowedColumn<'b>> for MultiBind<'c> {
//     fn from(col: &'c BorrowedColumn<'b>) -> Self {
//         match col {
//             BorrowedColumn::Null(n) => MultiBind::nulls(*n),
//             BorrowedColumn::Bool(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::TinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::SmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::Int(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::BigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::UTinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::USmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::UInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::UBigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::Float(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::Double(nulls, values) => MultiBind::from_primitives(nulls, values),
//             BorrowedColumn::Timestamp(nulls, values) => {
//                 MultiBind::from_raw_timestamps(nulls, values)
//             }
//             BorrowedColumn::Binary(values) => MultiBind::from_binary_vec(values),
//             BorrowedColumn::NChar(values) => MultiBind::from_string_vec(values),
//             _ => unreachable!(),
//         }
//     }
// }

// impl<T> From<Vec<T>> for TaosMultiBind {
//     fn from(_: Vec<T>) -> Self {
//         todo!()
//     }
// }
