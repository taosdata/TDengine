use derive_more::{Deref, DerefMut, Display, From};

use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use super::{Ty, Value};

pub type INull = ();
pub type IBool = bool;
pub type ITinyInt = i8;
pub type ISmallInt = i16;
pub type IInt = i32;
pub type IBigInt = i64;
pub type IUTinyInt = u8;
pub type IUSmallInt = u16;
pub type IUInt = u32;
pub type IUBigInt = u64;
pub type IFloat = f32;
pub type IDouble = f64;
pub type IJson = Json;
pub type IDecimal = Decimal;

#[derive(Debug, Clone, Copy, Deref, DerefMut, Deserialize, Serialize, Display, From)]
pub struct ITimestamp(pub i64);

#[derive(Debug, Deref, DerefMut, Clone, Deserialize, Serialize)]
pub struct IVarChar(String);

impl AsRef<str> for IVarChar {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
impl AsRef<[u8]> for IVarChar {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// Alias of [IVarChar].
pub type IBinary = IVarChar;

#[derive(Debug, Deref, DerefMut, Clone, From, Deserialize, Serialize)]
pub struct INChar(String);

impl AsRef<str> for INChar {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
impl AsRef<[u8]> for INChar {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Debug, Deref, DerefMut, Clone, From, Deserialize, Serialize)]
pub struct IVarBinary(Vec<u8>);

#[derive(Debug, Deref, DerefMut, Clone, From, Deserialize, Serialize)]
pub struct IMediumBlob(Vec<u8>);

#[derive(Debug, Deref, DerefMut, Clone, From, Deserialize, Serialize)]
pub struct IBlob(Vec<u8>);

impl From<String> for IVarChar {
    fn from(v: String) -> Self {
        Self(v)
    }
}
impl From<&str> for IVarChar {
    fn from(v: &str) -> Self {
        Self(v.to_string())
    }
}

impl From<IVarChar> for String {
    fn from(v: IVarChar) -> Self {
        v.0
    }
}

impl From<INChar> for String {
    fn from(v: INChar) -> Self {
        v.0
    }
}

// impl From<IJson> for String {
//     fn from(v: IJson) -> Self {
//         v.0.to_string()
//     }
// }

impl IVarChar {
    pub const fn new() -> Self {
        Self(String::new())
    }
    pub fn with_capacity(cap: usize) -> Self {
        Self(String::with_capacity(cap))
    }
}
pub trait IsValue: Sized {
    const TY: Ty;

    fn is_null(&self) -> bool {
        false
    }

    fn is_primitive(&self) -> bool {
        std::mem::size_of::<Self>() == Self::TY.fixed_length()
    }

    fn fixed_length(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn as_timestamp(&self) -> i64 {
        debug_assert!(Self::TY == Ty::Timestamp);
        unimplemented!()
    }

    fn as_var_char(&self) -> &str {
        debug_assert!(Self::TY == Ty::VarChar);
        unimplemented!()
    }

    fn as_nchar(&self) -> &str {
        debug_assert!(Self::TY == Ty::NChar);
        unimplemented!()
    }

    fn as_medium_blob(&self) -> &[u8] {
        debug_assert!(Self::TY == Ty::MediumBlob);
        unimplemented!()
    }

    fn as_blob(&self) -> &[u8] {
        debug_assert!(Self::TY == Ty::Blob);
        unimplemented!()
    }
}

impl<T> IsValue for Option<T>
where
    T: IsValue,
{
    const TY: Ty = T::TY;

    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn is_primitive(&self) -> bool {
        self.as_ref().unwrap().is_primitive()
    }

    fn as_timestamp(&self) -> i64 {
        self.as_ref().unwrap().as_timestamp()
    }

    fn as_var_char(&self) -> &str {
        self.as_ref().unwrap().as_var_char()
    }
    fn as_nchar(&self) -> &str {
        self.as_ref().unwrap().as_nchar()
    }
    fn as_medium_blob(&self) -> &[u8] {
        self.as_ref().unwrap().as_medium_blob()
    }
    fn as_blob(&self) -> &[u8] {
        self.as_ref().unwrap().as_blob()
    }
}

pub trait IValue: Sized {
    const TY: Ty;

    type Inner: Sized;

    fn is_null(&self) -> bool {
        false
    }

    fn into_value(self) -> Value;

    fn into_inner(self) -> Self::Inner;
}

impl IValue for INull {
    const TY: Ty = Ty::Null;

    fn is_null(&self) -> bool {
        true
    }
    fn into_value(self) -> Value {
        Value::Null
    }

    type Inner = ();

    fn into_inner(self) -> Self::Inner {
        ()
    }
}

/// Primitive type to TDengine data type.
macro_rules! impl_prim {
    ($($ty:ident = $inner:ty)*) => {
        $(paste::paste! {
            impl IValue for [<I $ty>] {
                const TY: Ty = Ty::$ty;
                type Inner = $inner;

                #[inline]
                fn is_null(&self) -> bool {
                    false
                }

                #[inline]
                fn into_value(self) -> Value {
                    Value::$ty(self)
                }

                #[inline]
                fn into_inner(self) -> Self::Inner {
                    self
                }
            }
        })*
    };
}

impl_prim!(
    Bool = bool
    TinyInt = i8
    SmallInt =  i16
    Int = i32
    BigInt = i64
    UTinyInt = u8
    USmallInt = u16
    UInt = u32
    UBigInt = u64
    Float = f32
    Double = f64
    Decimal = Decimal
    Json = Json
);

pub trait IsPrimitive: Copy {
    const TY: Ty;
    fn is_primitive(&self) -> bool {
        std::mem::size_of::<Self>() == Self::TY.fixed_length()
    }
}

macro_rules! impl_is_primitive {
    ($($ty:ident) *) => {
        $(paste::paste! {
            impl IsPrimitive for [<I $ty>] {
                const TY: Ty = Ty::$ty;
            }
            impl IsValue for [<I $ty>] {
                const TY: Ty = Ty::$ty;
            }
        })*
    };
}

impl_is_primitive!(
    Bool TinyInt SmallInt Int BigInt
    UTinyInt USmallInt UInt UBigInt
    Float Double Decimal
);

impl IsValue for ITimestamp {
    const TY: Ty = Ty::Timestamp;

    #[inline]
    fn as_timestamp(&self) -> i64 {
        self.0
    }
}

impl IsValue for IVarChar {
    const TY: Ty = Ty::VarChar;

    #[inline]
    fn as_var_char(&self) -> &str {
        &self.0
    }
}

impl IsValue for INChar {
    const TY: Ty = Ty::NChar;

    #[inline]
    fn as_nchar(&self) -> &str {
        &self.0
    }
}

pub trait IsVarChar {
    fn as_var_char(&self) -> &str;
}

impl IsVarChar for IVarChar {
    fn as_var_char(&self) -> &str {
        &self.0
    }
}

pub trait IsNChar {
    fn as_nchar(&self) -> &str;
}

impl IsNChar for INChar {
    fn as_nchar(&self) -> &str {
        &self.0
    }
}

pub trait IsJson {
    fn to_json(&self) -> String;
}

impl IsJson for IJson {
    fn to_json(&self) -> String {
        self.to_string()
    }
}

pub trait IsMediumBlob {
    fn as_medium_blob(&self) -> &[u8];
}

impl IsMediumBlob for IMediumBlob {
    fn as_medium_blob(&self) -> &[u8] {
        &self.0
    }
}

pub trait IsBlob {
    fn as_blob(&self) -> &[u8];
}

impl IsBlob for IBlob {
    fn as_blob(&self) -> &[u8] {
        &self.0
    }
}

impl IValue for ITimestamp {
    const TY: Ty = Ty::Timestamp;

    type Inner = i64;

    fn into_value(self) -> Value {
        todo!()
    }

    fn into_inner(self) -> Self::Inner {
        self.0
    }
}

macro_rules! impl_wrapper_struct {
    ($($ty:ident)*) => {
        $(paste::paste! {
            impl IValue for [<I $ty>] {
                const TY: Ty = Ty::$ty;
                #[inline]
                fn into_value(self) -> Value {
                    Value::$ty(self.0)
                }
            }
        })*
    };
    ($($ty:ident, $inner:ty;)*) => {
        $(paste::paste! {
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize)]
            pub struct [<I $ty>]($inner);

            impl Deref for [<I $ty>] {
                type Target = $inner;

                #[inline]
                fn deref(&self) -> &Self::Target {
                        &self.0
                }
            }

            impl IValue for [<I $ty>] {
                const TY: Ty = Ty::$ty;
                #[inline]
                fn into_value(self) -> Value {
                    Value::$ty(self.0)
                }
            }
        })*
    };
}
