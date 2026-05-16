use std::borrow::Cow;
use std::fmt::Display;
use std::str::Utf8Error;

use bigdecimal::{BigDecimal, Zero};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::util::hex;

use super::decimal::Decimal;
use super::{Timestamp, Ty};

mod de;

#[derive(Debug, Clone, PartialEq)]
pub enum BorrowedValue<'b> {
    Null(Ty),
    Bool(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    VarChar(&'b str),
    Timestamp(Timestamp),
    NChar(Cow<'b, str>),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64),
    Json(Cow<'b, [u8]>),
    VarBinary(Cow<'b, [u8]>),
    Decimal(Decimal<i128>),
    Blob(Cow<'b, [u8]>),
    MediumBlob(Cow<'b, [u8]>),
    Geometry(Cow<'b, [u8]>),
    Decimal64(Decimal<i64>),
}

macro_rules! borrowed_value_to_native {
    ($v:expr) => {{
        use bigdecimal::ToPrimitive;
        match $v {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Bool(v) => Some(if *v { 1 } else { 0 }),
            BorrowedValue::TinyInt(v) => Some(*v as _),
            BorrowedValue::SmallInt(v) => Some(*v as _),
            BorrowedValue::Int(v) => Some(*v as _),
            BorrowedValue::BigInt(v) => Some(*v as _),
            BorrowedValue::Float(v) => Some(*v as _),
            BorrowedValue::Double(v) => Some(*v as _),
            BorrowedValue::VarChar(s) => s.parse().map(Some).unwrap_or(None),
            BorrowedValue::Timestamp(v) => Some(v.as_raw_i64() as _),
            BorrowedValue::NChar(s) => s.parse().map(Some).unwrap_or(None),
            BorrowedValue::UTinyInt(v) => Some(*v as _),
            BorrowedValue::USmallInt(v) => Some(*v as _),
            BorrowedValue::UInt(v) => Some(*v as _),
            BorrowedValue::UBigInt(v) => Some(*v as _),
            BorrowedValue::Json(v) => serde_json::from_slice(&v).ok(),
            BorrowedValue::Decimal(v) => v.as_bigdecimal().to_i128().map(|v| v as _),
            BorrowedValue::Decimal64(v) => v.as_bigdecimal().to_i64().map(|v| v as _),
            _ => todo!(),
        }
    }};
}

macro_rules! borrowed_value_to_float {
    ($v:expr) => {{
        use bigdecimal::ToPrimitive;
        match $v {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Bool(v) => Some(if *v { 1. } else { 0. }),
            BorrowedValue::TinyInt(v) => Some(*v as _),
            BorrowedValue::SmallInt(v) => Some(*v as _),
            BorrowedValue::Int(v) => Some(*v as _),
            BorrowedValue::BigInt(v) => Some(*v as _),
            BorrowedValue::Float(v) => Some(*v as _),
            BorrowedValue::Double(v) => Some(*v as _),
            BorrowedValue::VarChar(s) => s.parse().map(Some).unwrap_or(None),
            BorrowedValue::Timestamp(v) => Some(v.as_raw_i64() as _),
            BorrowedValue::NChar(s) => s.parse().map(Some).unwrap_or(None),
            BorrowedValue::UTinyInt(v) => Some(*v as _),
            BorrowedValue::USmallInt(v) => Some(*v as _),
            BorrowedValue::UInt(v) => Some(*v as _),
            BorrowedValue::UBigInt(v) => Some(*v as _),
            BorrowedValue::Json(v) => serde_json::from_slice(&v).ok(),
            BorrowedValue::Decimal(v) => v.as_bigdecimal().to_f64().map(|v| v as _),
            BorrowedValue::Decimal64(v) => v.as_bigdecimal().to_f64().map(|v| v as _),
            _ => todo!(),
        }
    }};
}

impl BorrowedValue<'_> {
    /// The data type of this value.
    pub const fn ty(&self) -> Ty {
        use BorrowedValue::*;
        match self {
            Null(ty) => *ty,
            Bool(_) => Ty::Bool,
            TinyInt(_) => Ty::TinyInt,
            SmallInt(_) => Ty::SmallInt,
            Int(_) => Ty::Int,
            BigInt(_) => Ty::BigInt,
            UTinyInt(_) => Ty::UTinyInt,
            USmallInt(_) => Ty::USmallInt,
            UInt(_) => Ty::UInt,
            UBigInt(_) => Ty::UBigInt,
            Float(_) => Ty::Float,
            Double(_) => Ty::Double,
            VarChar(_) => Ty::VarChar,
            Timestamp(_) => Ty::Timestamp,
            Json(_) => Ty::Json,
            NChar(_) => Ty::NChar,
            VarBinary(_) => Ty::VarBinary,
            Decimal(_) => Ty::Decimal,
            Blob(_) => Ty::Blob,
            MediumBlob(_) => Ty::MediumBlob,
            Geometry(_) => Ty::Geometry,
            Decimal64(_) => Ty::Decimal64,
        }
    }

    pub fn to_sql_value(&self) -> String {
        use BorrowedValue::*;
        match self {
            Null(_) => "NULL".to_string(),
            Bool(v) => format!("{v}"),
            TinyInt(v) => format!("{v}"),
            SmallInt(v) => format!("{v}"),
            Int(v) => format!("{v}"),
            BigInt(v) => format!("{v}"),
            Float(v) => format!("{v}"),
            Double(v) => format!("{v}"),
            VarChar(v) => format!("\"{}\"", v.escape_debug()),
            Timestamp(v) => format!("{}", v.as_raw_i64()),
            NChar(v) => format!("\"{}\"", v.escape_debug()),
            UTinyInt(v) => format!("{v}"),
            USmallInt(v) => format!("{v}"),
            UInt(v) => format!("{v}"),
            UBigInt(v) => format!("{v}"),
            Json(v) => format!("\"{}\"", unsafe { std::str::from_utf8_unchecked(v) }),
            Decimal(v) => v.to_string(),
            Decimal64(v) => v.to_string(),
            VarBinary(v) | Blob(v) => hex::bytes_to_sql_hex_string(v),
            v => unreachable!("unsupported type: {}", v.ty()),
        }
    }

    pub fn to_sql_value_with_rfc3339(&self) -> String {
        use BorrowedValue::*;
        match self {
            Null(_) => "NULL".to_string(),
            Bool(v) => format!("{v}"),
            TinyInt(v) => format!("{v}"),
            SmallInt(v) => format!("{v}"),
            Int(v) => format!("{v}"),
            BigInt(v) => format!("{v}"),
            Float(v) => format!("{v}"),
            Double(v) => format!("{v}"),
            VarChar(v) => format!("\"{}\"", v.escape_debug()),
            Timestamp(v) => format!("\"{v}\""),
            NChar(v) => format!("\"{}\"", v.escape_debug()),
            UTinyInt(v) => format!("{v}"),
            USmallInt(v) => format!("{v}"),
            UInt(v) => format!("{v}"),
            UBigInt(v) => format!("{v}"),
            Json(v) => format!("\"{}\"", unsafe { std::str::from_utf8_unchecked(v) }),
            Decimal(v) => v.to_string(),
            Decimal64(v) => v.to_string(),
            VarBinary(v) | Blob(v) => hex::bytes_to_sql_hex_string(v),
            v => unreachable!("unsupported type: {}", v.ty()),
        }
    }

    /// Check if the value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, BorrowedValue::Null(_))
    }

    /// Only VarChar, NChar, Json could be treated as [&str].
    fn strict_as_str(&self) -> &str {
        use BorrowedValue::*;
        match self {
            VarChar(v) => v,
            NChar(v) => v,
            Null(_) => panic!("expect str but value is null"),
            Timestamp(_) => panic!("expect str but value is timestamp"),
            _ => panic!("expect str but only varchar/binary/nchar is supported"),
        }
    }

    pub fn to_string(&self) -> Result<String, Utf8Error> {
        use BorrowedValue::*;
        match self {
            Null(_) => Ok(String::new()),
            Bool(v) => Ok(format!("{v}")),
            VarChar(v) => Ok((*v).to_string()),
            Json(v) => Ok(unsafe { std::str::from_utf8_unchecked(v) }.to_string()),
            NChar(v) => Ok(v.to_string()),
            TinyInt(v) => Ok(format!("{v}")),
            SmallInt(v) => Ok(format!("{v}")),
            Int(v) => Ok(format!("{v}")),
            BigInt(v) => Ok(format!("{v}")),
            UTinyInt(v) => Ok(format!("{v}")),
            USmallInt(v) => Ok(format!("{v}")),
            UInt(v) => Ok(format!("{v}")),
            UBigInt(v) => Ok(format!("{v}")),
            Float(v) => Ok(format!("{v}")),
            Double(v) => Ok(format!("{v}")),
            Timestamp(v) => Ok(v.to_datetime_with_tz().to_rfc3339()),
            Decimal(v) => Ok(v.to_string()),
            Decimal64(v) => Ok(v.to_string()),
            VarBinary(v) | Blob(v) | Geometry(v) => std::str::from_utf8(v).map(|s| s.to_string()),
            v @ MediumBlob(_) => unreachable!("unsupported type: {}", v.ty()),
        }
    }

    pub fn to_value(&self) -> Value {
        use BorrowedValue::*;
        match self {
            Null(ty) => Value::Null(*ty),
            Bool(v) => Value::Bool(*v),
            TinyInt(v) => Value::TinyInt(*v),
            SmallInt(v) => Value::SmallInt(*v),
            Int(v) => Value::Int(*v),
            BigInt(v) => Value::BigInt(*v),
            UTinyInt(v) => Value::UTinyInt(*v),
            USmallInt(v) => Value::USmallInt(*v),
            UInt(v) => Value::UInt(*v),
            UBigInt(v) => Value::UBigInt(*v),
            Float(v) => Value::Float(*v),
            Double(v) => Value::Double(*v),
            VarChar(v) => Value::VarChar((*v).to_string()),
            Timestamp(v) => Value::Timestamp(*v),
            Json(v) => {
                Value::Json(serde_json::from_slice(v).expect("json should always be deserialized"))
            }
            NChar(str) => Value::NChar(str.to_string()),
            VarBinary(v) => Value::VarBinary(Bytes::copy_from_slice(v.as_ref())),
            Decimal(v) => Value::Decimal(v.as_bigdecimal()),
            Decimal64(v) => Value::Decimal64(v.as_bigdecimal()),
            Blob(v) => Value::Blob(Bytes::copy_from_slice(v.as_ref())),
            MediumBlob(_) => todo!(),
            Geometry(v) => Value::Geometry(Bytes::copy_from_slice(v.as_ref())),
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        use BorrowedValue::*;
        match self {
            Null(_) => serde_json::Value::Null,
            Bool(v) => serde_json::Value::Bool(*v),
            TinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            SmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            BigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UTinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            USmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UBigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Float(v) => serde_json::Number::from_f64(*v as f64)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            Double(v) => serde_json::Number::from_f64(*v)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            VarChar(v) => serde_json::Value::String((*v).to_string()),
            Timestamp(v) => serde_json::Value::Number(serde_json::Number::from(v.as_raw_i64())),
            Json(v) => serde_json::from_slice(v).expect("json should always be deserialized"),
            NChar(str) => serde_json::Value::String(str.to_string()),
            Decimal(v) => serde_json::Value::String(format!("{v}")),
            Decimal64(v) => serde_json::Value::String(format!("{v}")),
            Blob(v) | MediumBlob(v) | VarBinary(v) | Geometry(v) => {
                serde_json::Value::String(format!("{:?}", v.to_vec()))
            }
        }
    }

    #[inline]
    pub fn into_value(self) -> Value {
        use BorrowedValue::*;
        match self {
            Null(ty) => Value::Null(ty),
            Bool(v) => Value::Bool(v),
            TinyInt(v) => Value::TinyInt(v),
            SmallInt(v) => Value::SmallInt(v),
            Int(v) => Value::Int(v),
            BigInt(v) => Value::BigInt(v),
            UTinyInt(v) => Value::UTinyInt(v),
            USmallInt(v) => Value::USmallInt(v),
            UInt(v) => Value::UInt(v),
            UBigInt(v) => Value::UBigInt(v),
            Float(v) => Value::Float(v),
            Double(v) => Value::Double(v),
            VarChar(v) => Value::VarChar(v.to_string()),
            Timestamp(v) => Value::Timestamp(v),
            Json(v) => {
                Value::Json(serde_json::from_slice(&v).expect("json should always be deserialized"))
            }
            NChar(str) => Value::NChar(str.to_string()),
            VarBinary(v) => Value::VarBinary(Bytes::from(v.into_owned())),
            Decimal(v) => Value::Decimal(v.as_bigdecimal()),
            Decimal64(v) => Value::Decimal64(v.as_bigdecimal()),
            Blob(v) => Value::Blob(Bytes::from(v.into_owned())),
            MediumBlob(_) => todo!(),
            Geometry(v) => Value::Geometry(Bytes::from(v.into_owned())),
        }
    }

    pub(crate) fn to_bool(&self) -> Option<bool> {
        match self {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Bool(v) => Some(*v),
            BorrowedValue::TinyInt(v) => Some(*v > 0),
            BorrowedValue::SmallInt(v) => Some(*v > 0),
            BorrowedValue::Int(v) => Some(*v > 0),
            BorrowedValue::BigInt(v) => Some(*v > 0),
            BorrowedValue::Float(v) => Some(*v > 0.),
            BorrowedValue::Double(v) => Some(*v > 0.),
            BorrowedValue::VarChar(s) => match *s {
                "" => None,
                "false" | "f" | "F" | "FALSE" | "False" => Some(false),
                _ => Some(true),
            },
            BorrowedValue::Timestamp(_) | BorrowedValue::Json(_) => Some(true),
            BorrowedValue::NChar(s) => match s.as_ref() {
                "" => None,
                "false" | "f" | "F" | "FALSE" | "False" => Some(false),
                _ => Some(true),
            },
            BorrowedValue::UTinyInt(v) => Some(*v != 0),
            BorrowedValue::USmallInt(v) => Some(*v != 0),
            BorrowedValue::UInt(v) => Some(*v != 0),
            BorrowedValue::UBigInt(v) => Some(*v != 0),
            BorrowedValue::Decimal(v) => Some(v.as_bigdecimal() > bigdecimal::BigDecimal::zero()),
            BorrowedValue::Decimal64(v) => Some(v.as_bigdecimal() > bigdecimal::BigDecimal::zero()),
            _ => todo!(),
        }
    }

    pub(crate) fn to_i8(&self) -> Option<i8> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_i16(&self) -> Option<i16> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_i32(&self) -> Option<i32> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_i64(&self) -> Option<i64> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_u8(&self) -> Option<u8> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_u16(&self) -> Option<u16> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_u32(&self) -> Option<u32> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_u64(&self) -> Option<u64> {
        borrowed_value_to_native!(self)
    }

    pub(crate) fn to_f32(&self) -> Option<f32> {
        borrowed_value_to_float!(self)
    }

    pub(crate) fn to_f64(&self) -> Option<f64> {
        borrowed_value_to_float!(self)
    }

    pub(crate) fn to_str(&self) -> Option<Cow<'_, str>> {
        match self {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Bool(v) => Some(v.to_string().into()),
            BorrowedValue::TinyInt(v) => Some(v.to_string().into()),
            BorrowedValue::SmallInt(v) => Some(v.to_string().into()),
            BorrowedValue::Int(v) => Some(v.to_string().into()),
            BorrowedValue::BigInt(v) => Some(v.to_string().into()),
            BorrowedValue::Float(v) => Some(v.to_string().into()),
            BorrowedValue::Double(v) => Some(v.to_string().into()),
            BorrowedValue::VarChar(s) => Some((*s).into()),
            BorrowedValue::Timestamp(v) => Some(v.to_datetime_with_tz().to_string().into()),
            BorrowedValue::NChar(s) => Some(s.as_ref().into()),
            BorrowedValue::UTinyInt(v) => Some(v.to_string().into()),
            BorrowedValue::USmallInt(v) => Some(v.to_string().into()),
            BorrowedValue::UInt(v) => Some(v.to_string().into()),
            BorrowedValue::UBigInt(v) => Some(v.to_string().into()),
            BorrowedValue::Json(v) => Some(unsafe { std::str::from_utf8_unchecked(v) }.into()),
            BorrowedValue::Decimal(v) => Some(v.to_string().into()),
            BorrowedValue::Decimal64(v) => Some(v.to_string().into()),
            _ => todo!(),
        }
    }

    pub(crate) fn to_bytes(&self) -> Option<Bytes> {
        match self {
            BorrowedValue::VarBinary(v) | BorrowedValue::Geometry(v) | BorrowedValue::Blob(v) => {
                Some(Bytes::from(v.to_vec()))
            }
            _ => None,
        }
    }

    pub(crate) fn to_timestamp(&self) -> Option<Timestamp> {
        match self {
            BorrowedValue::Null(_) => None,
            BorrowedValue::Timestamp(v) => Some(*v),
            _ => panic!("Unsupported conversion from {} to timestamp", self.ty()),
        }
    }
}

impl Display for BorrowedValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BorrowedValue::*;
        match self {
            Null(_) => f.write_str("NULL"),
            Bool(v) => f.write_fmt(format_args!("{v}")),
            TinyInt(v) => f.write_fmt(format_args!("{v}")),
            SmallInt(v) => f.write_fmt(format_args!("{v}")),
            Int(v) => f.write_fmt(format_args!("{v}")),
            BigInt(v) => f.write_fmt(format_args!("{v}")),
            Float(v) => f.write_fmt(format_args!("{v}")),
            Double(v) => f.write_fmt(format_args!("{v}")),
            VarChar(v) => f.write_fmt(format_args!("{v}")),
            Timestamp(v) => f.write_fmt(format_args!("{v}")),
            NChar(v) => f.write_fmt(format_args!("{v}")),
            UTinyInt(v) => f.write_fmt(format_args!("{v}")),
            USmallInt(v) => f.write_fmt(format_args!("{v}")),
            UInt(v) => f.write_fmt(format_args!("{v}")),
            UBigInt(v) => f.write_fmt(format_args!("{v}")),
            Json(v) => f.write_fmt(format_args!("{}", v.as_ref().escape_ascii())),
            VarBinary(v) | Geometry(v) | Blob(v) => f.write_fmt(format_args!("{:?}", v.to_vec())),
            Decimal(v) => f.write_fmt(format_args!("{v}")),
            Decimal64(v) => f.write_fmt(format_args!("{v}")),
            MediumBlob(_) => todo!(),
        }
    }
}

unsafe impl Send for BorrowedValue<'_> {}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum Value {
    Null(Ty),
    Bool(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    VarChar(String),
    Timestamp(Timestamp),
    NChar(String),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64),
    Json(serde_json::Value),
    VarBinary(Bytes),
    Decimal(BigDecimal),
    Blob(Bytes),
    MediumBlob(Bytes),
    Geometry(Bytes),
    Decimal64(BigDecimal),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Value::*;
        match self {
            Null(_) => f.write_str("NULL"),
            Bool(v) => f.write_fmt(format_args!("{v}")),
            TinyInt(v) => f.write_fmt(format_args!("{v}")),
            SmallInt(v) => f.write_fmt(format_args!("{v}")),
            Int(v) => f.write_fmt(format_args!("{v}")),
            BigInt(v) => f.write_fmt(format_args!("{v}")),
            Float(v) => f.write_fmt(format_args!("{v}")),
            Double(v) => f.write_fmt(format_args!("{v}")),
            VarChar(v) | NChar(v) => f.write_fmt(format_args!("{v}")),
            Timestamp(v) => f.write_fmt(format_args!("{v}")),
            UTinyInt(v) => f.write_fmt(format_args!("{v}")),
            USmallInt(v) => f.write_fmt(format_args!("{v}")),
            UInt(v) => f.write_fmt(format_args!("{v}")),
            UBigInt(v) => f.write_fmt(format_args!("{v}")),
            Json(v) => f.write_fmt(format_args!("{v}")),
            VarBinary(v) | Geometry(v) | Blob(v) => f.write_fmt(format_args!("{:?}", v.to_vec())),
            Decimal(v) | Decimal64(v) => f.write_fmt(format_args!("{v}")),
            MediumBlob(_) => todo!(),
        }
    }
}

impl Value {
    /// The data type of this value.
    pub const fn ty(&self) -> Ty {
        use Value::*;
        match self {
            Null(ty) => *ty,
            Bool(_) => Ty::Bool,
            TinyInt(_) => Ty::TinyInt,
            SmallInt(_) => Ty::SmallInt,
            Int(_) => Ty::Int,
            BigInt(_) => Ty::BigInt,
            UTinyInt(_) => Ty::UTinyInt,
            USmallInt(_) => Ty::USmallInt,
            UInt(_) => Ty::UInt,
            UBigInt(_) => Ty::UBigInt,
            Float(_) => Ty::Float,
            Double(_) => Ty::Double,
            VarChar(_) => Ty::VarChar,
            Timestamp(_) => Ty::Timestamp,
            Json(_) => Ty::Json,
            NChar(_) => Ty::NChar,
            VarBinary(_) => Ty::VarBinary,
            Decimal(_) => Ty::Decimal,
            Decimal64(_) => Ty::Decimal64,
            Blob(_) => Ty::Blob,
            MediumBlob(_) => Ty::MediumBlob,
            Geometry(_) => Ty::Geometry,
        }
    }

    pub fn to_borrowed_value(&self) -> BorrowedValue<'_> {
        use Value::*;
        match self {
            Null(ty) => BorrowedValue::Null(*ty),
            Bool(v) => BorrowedValue::Bool(*v),
            TinyInt(v) => BorrowedValue::TinyInt(*v),
            SmallInt(v) => BorrowedValue::SmallInt(*v),
            Int(v) => BorrowedValue::Int(*v),
            BigInt(v) => BorrowedValue::BigInt(*v),
            UTinyInt(v) => BorrowedValue::UTinyInt(*v),
            USmallInt(v) => BorrowedValue::USmallInt(*v),
            UInt(v) => BorrowedValue::UInt(*v),
            UBigInt(v) => BorrowedValue::UBigInt(*v),
            Float(v) => BorrowedValue::Float(*v),
            Double(v) => BorrowedValue::Double(*v),
            VarChar(v) => BorrowedValue::VarChar(v),
            Timestamp(v) => BorrowedValue::Timestamp(*v),
            Json(j) => BorrowedValue::Json(j.to_string().into_bytes().into()),
            NChar(v) => BorrowedValue::NChar(v.as_str().into()),
            VarBinary(v) => BorrowedValue::VarBinary(Cow::Borrowed(v.as_ref())),
            Decimal(v) => BorrowedValue::Decimal(
                super::decimal::Decimal::<i128>::from_bigdecimal(v)
                    .expect("cannot convert to raw decimal type"),
            ),
            Decimal64(v) => BorrowedValue::Decimal64(
                super::decimal::Decimal::<i64>::from_bigdecimal(v)
                    .expect("cannot convert to raw decimal type"),
            ),
            Blob(v) => BorrowedValue::Blob(Cow::Borrowed(v.as_ref())),
            MediumBlob(v) => BorrowedValue::MediumBlob(Cow::Borrowed(v.as_ref())),
            Geometry(v) => BorrowedValue::Geometry(Cow::Borrowed(v.as_ref())),
        }
    }

    /// Check if the value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, Value::Null(_))
    }

    /// Only VarChar, NChar, Json could be treated as [&str].
    pub fn strict_as_str(&self) -> &str {
        use Value::*;
        match self {
            VarChar(v) | NChar(v) => v.as_str(),
            Json(v) => v.as_str().expect("invalid str type"),
            Null(_) => "Null",
            Timestamp(_) => panic!("expect str but value is timestamp"),
            s => panic!("expect str but only varchar/binary/json/nchar is supported: {s:?}"),
        }
    }

    pub fn to_sql_value(&self) -> String {
        use Value::*;
        match self {
            Null(_) => "NULL".to_string(),
            Bool(v) => format!("{v}"),
            TinyInt(v) => format!("{v}"),
            SmallInt(v) => format!("{v}"),
            Int(v) => format!("{v}"),
            BigInt(v) => format!("{v}"),
            Float(v) => format!("{v}"),
            Double(v) => format!("{v}"),
            VarChar(v) | NChar(v) => format!("\"{}\"", v.escape_debug()),
            Timestamp(v) => format!("{}", v.as_raw_i64()),
            UTinyInt(v) => format!("{v}"),
            USmallInt(v) => format!("{v}"),
            UInt(v) => format!("{v}"),
            UBigInt(v) => format!("{v}"),
            Json(v) => format!("\"{v}\""),
            Decimal(v) | Decimal64(v) => format!("{v}"),
            VarBinary(v) | Blob(v) => hex::bytes_to_sql_hex_string(v),
            v => unreachable!("unsupported type: {}", v.ty()),
        }
    }

    pub fn to_sql_value_with_rfc3339(&self) -> String {
        use Value::*;
        match self {
            Null(_) => "NULL".to_string(),
            Bool(v) => format!("{v}"),
            TinyInt(v) => format!("{v}"),
            SmallInt(v) => format!("{v}"),
            Int(v) => format!("{v}"),
            BigInt(v) => format!("{v}"),
            Float(v) => format!("{v}"),
            Double(v) => format!("{v}"),
            VarChar(v) | NChar(v) => format!("\"{}\"", v.escape_debug()),
            Timestamp(v) => format!("\"{v}\""),
            UTinyInt(v) => format!("{v}"),
            USmallInt(v) => format!("{v}"),
            UInt(v) => format!("{v}"),
            UBigInt(v) => format!("{v}"),
            Json(v) => format!("\"{v}\""),
            Decimal(v) | Decimal64(v) => format!("{v}"),
            VarBinary(v) | Blob(v) => hex::bytes_to_sql_hex_string(v),
            v => unreachable!("unsupported type: {}", v.ty()),
        }
    }

    pub fn to_string(&self) -> Result<String, Utf8Error> {
        use Value::*;
        match self {
            Null(_) => Ok(String::new()),
            Bool(v) => Ok(format!("{v}")),
            VarChar(v) | NChar(v) => Ok(v.to_string()),
            Json(v) => Ok(v.to_string()),
            TinyInt(v) => Ok(format!("{v}")),
            SmallInt(v) => Ok(format!("{v}")),
            Int(v) => Ok(format!("{v}")),
            BigInt(v) => Ok(format!("{v}")),
            UTinyInt(v) => Ok(format!("{v}")),
            USmallInt(v) => Ok(format!("{v}")),
            UInt(v) => Ok(format!("{v}")),
            UBigInt(v) => Ok(format!("{v}")),
            Float(v) => Ok(format!("{v}")),
            Double(v) => Ok(format!("{v}")),
            Decimal(v) | Decimal64(v) => Ok(v.to_string()),
            Timestamp(v) => Ok(v.to_datetime_with_tz().to_rfc3339()),
            VarBinary(v) | Blob(v) | Geometry(v) => std::str::from_utf8(v).map(|s| s.to_string()),
            v @ MediumBlob(_) => unreachable!("unsupported type: {}", v.ty()),
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        use Value::*;
        match self {
            Null(_) => serde_json::Value::Null,
            Bool(v) => serde_json::Value::Bool(*v),
            TinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            SmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            BigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UTinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            USmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UBigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Float(v) => serde_json::Number::from_f64(*v as f64)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            Double(v) => serde_json::Number::from_f64(*v)
                .map_or(serde_json::Value::Null, serde_json::Value::Number),
            VarChar(v) => serde_json::Value::String(v.to_string()),
            Timestamp(v) => serde_json::Value::Number(serde_json::Number::from(v.as_raw_i64())),
            Json(v) => v.clone(),
            NChar(str) => serde_json::Value::String(str.to_string()),
            Decimal(v) | Decimal64(v) => serde_json::Value::String(format!("{v}")),
            Blob(v) | MediumBlob(v) | VarBinary(v) | Geometry(v) => {
                serde_json::Value::String(format!("{:?}", v.to_vec()))
            }
        }
    }
}

impl PartialEq<&Value> for BorrowedValue<'_> {
    fn eq(&self, other: &&Value) -> bool {
        self == *other
    }
}

impl PartialEq<Value> for BorrowedValue<'_> {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Self::Null(l0), Value::Null(r0)) => l0 == r0,
            (Self::Bool(l0), Value::Bool(r0)) => l0 == r0,
            (Self::TinyInt(l0), Value::TinyInt(r0)) => l0 == r0,
            (Self::SmallInt(l0), Value::SmallInt(r0)) => l0 == r0,
            (Self::Int(l0), Value::Int(r0)) => l0 == r0,
            (Self::BigInt(l0), Value::BigInt(r0)) => l0 == r0,
            (Self::Float(l0), Value::Float(r0)) => l0 == r0,
            (Self::Double(l0), Value::Double(r0)) => l0 == r0,
            (Self::VarChar(l0), Value::VarChar(r0)) => l0 == r0,
            (Self::Timestamp(l0), Value::Timestamp(r0)) => l0 == r0,
            (Self::NChar(l0), Value::NChar(r0)) => l0 == r0,
            (Self::UTinyInt(l0), Value::UTinyInt(r0)) => l0 == r0,
            (Self::USmallInt(l0), Value::USmallInt(r0)) => l0 == r0,
            (Self::UInt(l0), Value::UInt(r0)) => l0 == r0,
            (Self::UBigInt(l0), Value::UBigInt(r0)) => l0 == r0,
            (Self::Json(l0), Value::Json(r0)) => l0.as_ref() == serde_json::to_vec(r0).unwrap(),
            (Self::Decimal(l0), Value::Decimal(r0)) => &l0.as_bigdecimal() == r0,
            (Self::Decimal64(l0), Value::Decimal64(r0)) => &l0.as_bigdecimal() == r0,
            (Self::VarBinary(l0), Value::VarBinary(r0))
            | (Self::Geometry(l0), Value::Geometry(r0))
            | (Self::Blob(l0), Value::Blob(r0))
            | (Self::MediumBlob(l0), Value::MediumBlob(r0)) => l0.as_ref() == r0.as_ref(),
            _ => false,
        }
    }
}

impl<'b> PartialEq<BorrowedValue<'b>> for Value {
    fn eq(&self, other: &BorrowedValue<'b>) -> bool {
        match (other, self) {
            (BorrowedValue::Null(l0), Value::Null(r0)) => l0 == r0,
            (BorrowedValue::Bool(l0), Value::Bool(r0)) => l0 == r0,
            (BorrowedValue::TinyInt(l0), Value::TinyInt(r0)) => l0 == r0,
            (BorrowedValue::SmallInt(l0), Value::SmallInt(r0)) => l0 == r0,
            (BorrowedValue::Int(l0), Value::Int(r0)) => l0 == r0,
            (BorrowedValue::BigInt(l0), Value::BigInt(r0)) => l0 == r0,
            (BorrowedValue::Float(l0), Value::Float(r0)) => l0 == r0,
            (BorrowedValue::Double(l0), Value::Double(r0)) => l0 == r0,
            (BorrowedValue::VarChar(l0), Value::VarChar(r0)) => l0 == r0,
            (BorrowedValue::Timestamp(l0), Value::Timestamp(r0)) => l0 == r0,
            (BorrowedValue::NChar(l0), Value::NChar(r0)) => l0 == r0,
            (BorrowedValue::UTinyInt(l0), Value::UTinyInt(r0)) => l0 == r0,
            (BorrowedValue::USmallInt(l0), Value::USmallInt(r0)) => l0 == r0,
            (BorrowedValue::UInt(l0), Value::UInt(r0)) => l0 == r0,
            (BorrowedValue::UBigInt(l0), Value::UBigInt(r0)) => l0 == r0,
            (BorrowedValue::Json(l0), Value::Json(r0)) => {
                l0.as_ref() == serde_json::to_vec(r0).unwrap()
            }
            (BorrowedValue::Decimal(l0), Value::Decimal(r0)) => &l0.as_bigdecimal() == r0,
            (BorrowedValue::Decimal64(l0), Value::Decimal64(r0)) => &l0.as_bigdecimal() == r0,
            (BorrowedValue::VarBinary(l0), Value::VarBinary(r0))
            | (BorrowedValue::Geometry(l0), Value::Geometry(r0))
            | (BorrowedValue::Blob(l0), Value::Blob(r0))
            | (BorrowedValue::MediumBlob(l0), Value::MediumBlob(r0)) => l0.as_ref() == r0.as_ref(),
            _ => false,
        }
    }
}

macro_rules! _impl_primitive_from {
    ($f:ident, $t:ident) => {
        impl From<$f> for Value {
            fn from(value: $f) -> Self {
                Value::$t(value)
            }
        }
        impl From<Option<$f>> for Value {
            fn from(value: Option<$f>) -> Self {
                match value {
                    Some(value) => Value::$t(value),
                    None => Value::Null(Ty::$t),
                }
            }
        }
    };
}

_impl_primitive_from!(bool, Bool);
_impl_primitive_from!(i8, TinyInt);
_impl_primitive_from!(i16, SmallInt);
_impl_primitive_from!(i32, Int);
_impl_primitive_from!(i64, BigInt);
_impl_primitive_from!(u8, UTinyInt);
_impl_primitive_from!(u16, USmallInt);
_impl_primitive_from!(u32, UInt);
_impl_primitive_from!(u64, UBigInt);
_impl_primitive_from!(f32, Float);
_impl_primitive_from!(f64, Double);
_impl_primitive_from!(Timestamp, Timestamp);

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::common::Precision;

    #[test]
    fn test_borrowed_value_to_native() {
        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(tiny_int_value.to_i8(), Some(42));
        assert_eq!(tiny_int_value.to_i16(), Some(42));
        assert_eq!(tiny_int_value.to_i32(), Some(42));
        assert_eq!(tiny_int_value.to_i64(), Some(42));
        assert_eq!(tiny_int_value.to_u8(), Some(42));
        assert_eq!(tiny_int_value.to_u16(), Some(42));
        assert_eq!(tiny_int_value.to_u32(), Some(42));
        assert_eq!(tiny_int_value.to_u64(), Some(42));

        let float_value = BorrowedValue::Float(3.14);
        println!("float_value: {:?}", float_value.to_f32());
        println!("float_value: {:?}", float_value.to_f64());
    }

    #[test]
    fn test_null_value() {
        let null_value = Value::Null(Ty::Int);
        assert_eq!(null_value.ty(), Ty::Int);
        assert_eq!(null_value.to_sql_value(), "NULL".to_string());
        assert_eq!(null_value.to_string(), Ok("".to_string()));
        assert_eq!(null_value.to_json_value(), serde_json::Value::Null);
        assert_eq!(format!("{}", null_value), "NULL");
        let null_value_borrowed = null_value.to_borrowed_value();
        assert_eq!(null_value_borrowed.ty(), Ty::Int);
        assert_eq!(null_value_borrowed.to_sql_value(), "NULL".to_string());
        assert_eq!(null_value_borrowed.to_string(), Ok("".to_string()));
        assert_eq!(null_value_borrowed.to_json_value(), serde_json::Value::Null);
        assert_eq!(format!("{}", null_value_borrowed), "NULL");
        println!("{:?}", null_value_borrowed.to_str());
        assert_eq!(null_value_borrowed.to_bool(), None);
        assert_eq!(null_value_borrowed.to_value(), null_value);
        assert_eq!(null_value_borrowed.clone().into_value(), null_value);
        assert_eq!(null_value_borrowed, null_value);
        assert_eq!(null_value, null_value_borrowed);
        assert_eq!(null_value_borrowed, &null_value);
    }

    #[test]
    fn test_bool_value() {
        let bool_value = Value::Bool(true);
        assert_eq!(bool_value.ty(), Ty::Bool);
        assert_eq!(bool_value.to_sql_value(), "true".to_string());
        assert_eq!(bool_value.to_string(), Ok("true".to_string()));
        assert_eq!(bool_value.to_json_value(), serde_json::Value::Bool(true));
        assert_eq!(format!("{}", bool_value), "true");
        let bool_value_borrowed = bool_value.to_borrowed_value();
        assert_eq!(bool_value_borrowed.ty(), Ty::Bool);
        assert_eq!(bool_value_borrowed.to_sql_value(), "true".to_string());
        assert_eq!(bool_value_borrowed.to_string(), Ok("true".to_string()));
        assert_eq!(
            bool_value_borrowed.to_json_value(),
            serde_json::Value::Bool(true)
        );
        assert_eq!(format!("{}", bool_value_borrowed), "true");
        println!("{:?}", bool_value_borrowed.to_str());
        assert_eq!(bool_value_borrowed.to_bool(), Some(true));
        assert_eq!(bool_value_borrowed.to_value(), bool_value);
        assert_eq!(bool_value_borrowed.clone().into_value(), bool_value);
        assert_eq!(bool_value_borrowed, bool_value);
        assert_eq!(bool_value, bool_value_borrowed);
        assert_eq!(bool_value_borrowed, &bool_value);
    }

    #[test]
    fn test_tiny_int_value() {
        let tiny_int_value = Value::TinyInt(42);
        assert_eq!(tiny_int_value.ty(), Ty::TinyInt);
        assert_eq!(tiny_int_value.to_sql_value(), "42".to_string());
        assert_eq!(tiny_int_value.to_string(), Ok("42".to_string()));
        assert_eq!(
            tiny_int_value.to_json_value(),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(format!("{}", tiny_int_value), "42");
        let tiny_int_value_borrowed = tiny_int_value.to_borrowed_value();
        assert_eq!(tiny_int_value_borrowed.ty(), Ty::TinyInt);
        assert_eq!(tiny_int_value_borrowed.to_sql_value(), "42".to_string());
        assert_eq!(tiny_int_value_borrowed.to_string(), Ok("42".to_string()));
        assert_eq!(
            tiny_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(format!("{}", tiny_int_value_borrowed), "42");
        println!("{:?}", tiny_int_value_borrowed.to_str());
        assert_eq!(tiny_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(tiny_int_value_borrowed.to_value(), tiny_int_value);
        assert_eq!(tiny_int_value_borrowed.clone().into_value(), tiny_int_value);
        assert_eq!(tiny_int_value_borrowed, tiny_int_value);
        assert_eq!(tiny_int_value, tiny_int_value_borrowed);
        assert_eq!(tiny_int_value_borrowed, &tiny_int_value);
    }

    #[test]
    fn test_small_int_value() {
        let small_int_value = Value::SmallInt(1000);
        assert_eq!(small_int_value.ty(), Ty::SmallInt);
        assert_eq!(small_int_value.to_sql_value(), "1000".to_string());
        assert_eq!(small_int_value.to_string(), Ok("1000".to_string()));
        assert_eq!(
            small_int_value.to_json_value(),
            serde_json::Value::Number(1000.into())
        );
        assert_eq!(format!("{}", small_int_value), "1000");
        let small_int_value_borrowed = small_int_value.to_borrowed_value();
        assert_eq!(small_int_value_borrowed.ty(), Ty::SmallInt);
        assert_eq!(small_int_value_borrowed.to_sql_value(), "1000".to_string());
        assert_eq!(small_int_value_borrowed.to_string(), Ok("1000".to_string()));
        assert_eq!(
            small_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(1000.into())
        );
        assert_eq!(format!("{}", small_int_value_borrowed), "1000");
        println!("{:?}", small_int_value_borrowed.to_str());
        assert_eq!(small_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(small_int_value_borrowed.to_value(), small_int_value);
        assert_eq!(
            small_int_value_borrowed.clone().into_value(),
            small_int_value
        );
        assert_eq!(small_int_value_borrowed, small_int_value);
        assert_eq!(small_int_value, small_int_value_borrowed);
        assert_eq!(small_int_value_borrowed, &small_int_value);
    }

    #[test]
    fn test_int_value() {
        let int_value = Value::Int(-500);
        assert_eq!(int_value.ty(), Ty::Int);
        assert_eq!(int_value.to_sql_value(), "-500".to_string());
        assert_eq!(int_value.to_string(), Ok("-500".to_string()));
        assert_eq!(
            int_value.to_json_value(),
            serde_json::Value::Number((-500).into())
        );
        assert_eq!(format!("{}", int_value), "-500");
        let int_value_borrowed = int_value.to_borrowed_value();
        assert_eq!(int_value_borrowed.ty(), Ty::Int);
        assert_eq!(int_value_borrowed.to_sql_value(), "-500".to_string());
        assert_eq!(int_value_borrowed.to_string(), Ok("-500".to_string()));
        assert_eq!(
            int_value_borrowed.to_json_value(),
            serde_json::Value::Number((-500).into())
        );
        assert_eq!(format!("{}", int_value_borrowed), "-500");
        println!("{:?}", int_value_borrowed.to_str());
        assert_eq!(int_value_borrowed.to_bool(), Some(false));
        assert_eq!(int_value_borrowed.to_value(), int_value);
        assert_eq!(int_value_borrowed.clone().into_value(), int_value);
        assert_eq!(int_value_borrowed, int_value);
        assert_eq!(int_value, int_value_borrowed);
        assert_eq!(int_value_borrowed, &int_value);
    }

    #[test]
    fn test_big_int_value() {
        let big_int_value = Value::BigInt(1234567890);
        assert_eq!(big_int_value.ty(), Ty::BigInt);
        assert_eq!(big_int_value.to_sql_value(), "1234567890".to_string());
        assert_eq!(big_int_value.to_string(), Ok("1234567890".to_string()));
        assert_eq!(
            big_int_value.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );
        assert_eq!(format!("{}", big_int_value), "1234567890");
        let big_int_value_borrowed = big_int_value.to_borrowed_value();
        assert_eq!(big_int_value_borrowed.ty(), Ty::BigInt);
        assert_eq!(
            big_int_value_borrowed.to_sql_value(),
            "1234567890".to_string()
        );
        assert_eq!(
            big_int_value_borrowed.to_string(),
            Ok("1234567890".to_string())
        );
        assert_eq!(
            big_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );
        assert_eq!(format!("{}", big_int_value_borrowed), "1234567890");
        println!("{:?}", big_int_value_borrowed.to_str());
        assert_eq!(big_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(big_int_value_borrowed.to_value(), big_int_value);
        assert_eq!(big_int_value_borrowed.clone().into_value(), big_int_value);
        assert_eq!(big_int_value_borrowed, big_int_value);
        assert_eq!(big_int_value, big_int_value_borrowed);
        assert_eq!(big_int_value_borrowed, &big_int_value);
    }

    #[test]
    fn test_utiny_int_value() {
        let utiny_int_value = Value::UTinyInt(42);
        assert_eq!(utiny_int_value.ty(), Ty::UTinyInt);
        assert_eq!(utiny_int_value.to_sql_value(), "42".to_string());
        assert_eq!(utiny_int_value.to_string(), Ok("42".to_string()));
        assert_eq!(
            utiny_int_value.to_json_value(),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(format!("{}", utiny_int_value), "42");
        let utiny_int_value_borrowed = utiny_int_value.to_borrowed_value();
        assert_eq!(utiny_int_value_borrowed.ty(), Ty::UTinyInt);
        assert_eq!(utiny_int_value_borrowed.to_sql_value(), "42".to_string());
        assert_eq!(utiny_int_value_borrowed.to_string(), Ok("42".to_string()));
        assert_eq!(
            utiny_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(42.into())
        );
        assert_eq!(format!("{}", utiny_int_value_borrowed), "42");
        println!("{:?}", utiny_int_value_borrowed.to_str());
        assert_eq!(utiny_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(utiny_int_value_borrowed.to_value(), utiny_int_value);
        assert_eq!(
            utiny_int_value_borrowed.clone().into_value(),
            utiny_int_value
        );
        assert_eq!(utiny_int_value_borrowed, utiny_int_value);
        assert_eq!(utiny_int_value, utiny_int_value_borrowed);
        assert_eq!(utiny_int_value_borrowed, &utiny_int_value);
    }

    #[test]
    fn test_usmall_int_value() {
        let usmall_int_value = Value::USmallInt(1000);
        assert_eq!(usmall_int_value.ty(), Ty::USmallInt);
        assert_eq!(usmall_int_value.to_sql_value(), "1000".to_string());
        assert_eq!(usmall_int_value.to_string(), Ok("1000".to_string()));
        assert_eq!(
            usmall_int_value.to_json_value(),
            serde_json::Value::Number(1000.into())
        );
        assert_eq!(format!("{}", usmall_int_value), "1000");
        let usmall_int_value_borrowed = usmall_int_value.to_borrowed_value();
        assert_eq!(usmall_int_value_borrowed.ty(), Ty::USmallInt);
        assert_eq!(usmall_int_value_borrowed.to_sql_value(), "1000".to_string());
        assert_eq!(
            usmall_int_value_borrowed.to_string(),
            Ok("1000".to_string())
        );
        assert_eq!(
            usmall_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(1000.into())
        );
        assert_eq!(format!("{}", usmall_int_value_borrowed), "1000");
        println!("{:?}", usmall_int_value_borrowed.to_str());
        assert_eq!(usmall_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(usmall_int_value_borrowed.to_value(), usmall_int_value);
        assert_eq!(
            usmall_int_value_borrowed.clone().into_value(),
            usmall_int_value
        );
        assert_eq!(usmall_int_value_borrowed, usmall_int_value);
        assert_eq!(usmall_int_value, usmall_int_value_borrowed);
        assert_eq!(usmall_int_value_borrowed, &usmall_int_value);
    }

    #[test]
    fn test_uint_value() {
        let uint_value = Value::UInt(5000);
        assert_eq!(uint_value.ty(), Ty::UInt);
        assert_eq!(uint_value.to_sql_value(), "5000".to_string());
        assert_eq!(uint_value.to_string(), Ok("5000".to_string()));
        assert_eq!(
            uint_value.to_json_value(),
            serde_json::Value::Number(5000.into())
        );
        assert_eq!(format!("{}", uint_value), "5000");
        let uint_value_borrowed = uint_value.to_borrowed_value();
        assert_eq!(uint_value_borrowed.ty(), Ty::UInt);
        assert_eq!(uint_value_borrowed.to_sql_value(), "5000".to_string());
        assert_eq!(uint_value_borrowed.to_string(), Ok("5000".to_string()));
        assert_eq!(
            uint_value_borrowed.to_json_value(),
            serde_json::Value::Number(5000.into())
        );
        assert_eq!(format!("{}", uint_value_borrowed), "5000");
        println!("{:?}", uint_value_borrowed.to_str());
        assert_eq!(uint_value_borrowed.to_bool(), Some(true));
        assert_eq!(uint_value_borrowed.to_value(), uint_value);
        assert_eq!(uint_value_borrowed.clone().into_value(), uint_value);
        assert_eq!(uint_value_borrowed, uint_value);
        assert_eq!(uint_value, uint_value_borrowed);
        assert_eq!(uint_value_borrowed, &uint_value);
    }

    #[test]
    fn test_ubig_int_value() {
        let ubig_int_value = Value::UBigInt(1234567890);
        assert_eq!(ubig_int_value.ty(), Ty::UBigInt);
        assert_eq!(ubig_int_value.to_sql_value(), "1234567890".to_string());
        assert_eq!(ubig_int_value.to_string(), Ok("1234567890".to_string()));
        assert_eq!(
            ubig_int_value.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );
        assert_eq!(format!("{}", ubig_int_value), "1234567890");
        let ubig_int_value_borrowed = ubig_int_value.to_borrowed_value();
        assert_eq!(ubig_int_value_borrowed.ty(), Ty::UBigInt);
        assert_eq!(
            ubig_int_value_borrowed.to_sql_value(),
            "1234567890".to_string()
        );
        assert_eq!(
            ubig_int_value_borrowed.to_string(),
            Ok("1234567890".to_string())
        );
        assert_eq!(
            ubig_int_value_borrowed.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );
        assert_eq!(format!("{}", ubig_int_value_borrowed), "1234567890");
        println!("{:?}", ubig_int_value_borrowed.to_str());
        assert_eq!(ubig_int_value_borrowed.to_bool(), Some(true));
        assert_eq!(ubig_int_value_borrowed.to_value(), ubig_int_value);
        assert_eq!(ubig_int_value_borrowed.clone().into_value(), ubig_int_value);
        assert_eq!(ubig_int_value_borrowed, ubig_int_value);
        assert_eq!(ubig_int_value, ubig_int_value_borrowed);
        assert_eq!(ubig_int_value_borrowed, &ubig_int_value);
    }

    #[test]
    fn test_float_value() {
        let float_value = Value::Float(3.14);
        assert_eq!(float_value.ty(), Ty::Float);
        assert_eq!(float_value.to_sql_value(), "3.14".to_string());
        assert_eq!(float_value.to_string(), Ok("3.14".to_string()));
        println!("{:?}", float_value.to_json_value());
        assert_eq!(format!("{}", float_value), "3.14");
        let float_value_borrowed = float_value.to_borrowed_value();
        assert_eq!(float_value_borrowed.ty(), Ty::Float);
        assert_eq!(float_value_borrowed.to_sql_value(), "3.14".to_string());
        assert_eq!(float_value_borrowed.to_string(), Ok("3.14".to_string()));
        println!("{:?}", float_value_borrowed.to_json_value());
        assert_eq!(format!("{}", float_value_borrowed), "3.14");
        println!("{:?}", float_value_borrowed.to_str());
        assert_eq!(float_value_borrowed.to_bool(), Some(true));
        assert_eq!(float_value_borrowed.to_value(), float_value);
        assert_eq!(float_value_borrowed.clone().into_value(), float_value);
        assert_eq!(float_value_borrowed, float_value);
        assert_eq!(float_value, float_value_borrowed);
        assert_eq!(float_value_borrowed, &float_value);
    }

    #[test]
    fn test_double_value() {
        let double_value = Value::Double(2.71828);
        assert_eq!(double_value.ty(), Ty::Double);
        assert_eq!(double_value.to_sql_value(), "2.71828".to_string());
        assert_eq!(double_value.to_string(), Ok("2.71828".to_string()));
        println!("{:?}", double_value.to_json_value());
        assert_eq!(format!("{}", double_value), "2.71828");
        let double_value_borrowed = double_value.to_borrowed_value();
        assert_eq!(double_value_borrowed.ty(), Ty::Double);
        assert_eq!(double_value_borrowed.to_sql_value(), "2.71828".to_string());
        assert_eq!(double_value_borrowed.to_string(), Ok("2.71828".to_string()));
        println!("{:?}", double_value_borrowed.to_json_value());
        assert_eq!(format!("{}", double_value_borrowed), "2.71828");
        println!("{:?}", double_value_borrowed.to_str());
        assert_eq!(double_value_borrowed.to_bool(), Some(true));
        assert_eq!(double_value_borrowed.to_value(), double_value);
        assert_eq!(double_value_borrowed.clone().into_value(), double_value);
        assert_eq!(double_value_borrowed, double_value);
        assert_eq!(double_value, double_value_borrowed);
        assert_eq!(double_value_borrowed, &double_value);
    }

    #[test]
    fn test_var_char_value() {
        let varchar_value = Value::VarChar("hello".to_string());
        assert_eq!(varchar_value.ty(), Ty::VarChar);
        assert_eq!(varchar_value.to_sql_value(), "\"hello\"".to_string());
        assert_eq!(varchar_value.to_string(), Ok("hello".to_string()));
        assert_eq!(
            varchar_value.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(format!("{}", varchar_value), "hello");
        let varchar_value_borrowed = varchar_value.to_borrowed_value();
        assert_eq!(varchar_value_borrowed.ty(), Ty::VarChar);
        assert_eq!(
            varchar_value_borrowed.to_sql_value(),
            "\"hello\"".to_string()
        );
        assert_eq!(varchar_value_borrowed.to_string(), Ok("hello".to_string()));
        assert_eq!(
            varchar_value_borrowed.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(format!("{}", varchar_value_borrowed), "hello");
        println!("{:?}", varchar_value_borrowed.to_str());
        assert_eq!(varchar_value_borrowed.to_bool(), Some(true));
        assert_eq!(varchar_value_borrowed.to_value(), varchar_value);
        assert_eq!(varchar_value_borrowed.clone().into_value(), varchar_value);
        assert_eq!(varchar_value_borrowed, varchar_value);
        assert_eq!(varchar_value, varchar_value_borrowed);
        assert_eq!(varchar_value_borrowed, &varchar_value);
    }

    #[test]
    fn test_timestamp_value() {
        let timestamp_value = Value::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert_eq!(timestamp_value.ty(), Ty::Timestamp);
        assert_eq!(timestamp_value.to_sql_value(), "1".to_string());
        println!("{:?}", timestamp_value.to_string());
        assert_eq!(
            timestamp_value.to_json_value(),
            serde_json::Value::Number(1.into())
        );
        println!("{}", format!("{}", timestamp_value));
        let timestamp_value_borrowed = timestamp_value.to_borrowed_value();
        assert_eq!(timestamp_value_borrowed.ty(), Ty::Timestamp);
        assert_eq!(timestamp_value_borrowed.to_sql_value(), "1".to_string());
        println!("{:?}", timestamp_value_borrowed.to_string());
        assert_eq!(
            timestamp_value_borrowed.to_json_value(),
            serde_json::Value::Number(1.into())
        );
        println!("{}", format!("{}", timestamp_value_borrowed));
        println!("{:?}", timestamp_value_borrowed.to_str());
        assert_eq!(timestamp_value_borrowed.to_bool(), Some(true));
        assert_eq!(timestamp_value_borrowed.to_value(), timestamp_value);
        assert_eq!(
            timestamp_value_borrowed.clone().into_value(),
            timestamp_value
        );
        assert_eq!(timestamp_value_borrowed, timestamp_value);
        assert_eq!(timestamp_value, timestamp_value_borrowed);
        assert_eq!(timestamp_value_borrowed, &timestamp_value);
    }

    #[test]
    fn test_nchar_value() {
        let nchar_value = Value::NChar("hello".to_string());
        assert_eq!(nchar_value.ty(), Ty::NChar);
        assert_eq!(nchar_value.to_sql_value(), "\"hello\"".to_string());
        assert_eq!(nchar_value.to_string(), Ok("hello".to_string()));
        assert_eq!(
            nchar_value.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(format!("{}", nchar_value), "hello");
        let nchar_value_borrowed = nchar_value.to_borrowed_value();
        assert_eq!(nchar_value_borrowed.ty(), Ty::NChar);
        assert_eq!(nchar_value_borrowed.to_sql_value(), "\"hello\"".to_string());
        assert_eq!(nchar_value_borrowed.to_string(), Ok("hello".to_string()));
        assert_eq!(
            nchar_value_borrowed.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
        assert_eq!(format!("{}", nchar_value_borrowed), "hello");
        println!("{:?}", nchar_value_borrowed.to_str());
        assert_eq!(nchar_value_borrowed.to_bool(), Some(true));
        assert_eq!(nchar_value_borrowed.to_value(), nchar_value);
        assert_eq!(nchar_value_borrowed.clone().into_value(), nchar_value);
        assert_eq!(nchar_value_borrowed, nchar_value);
        assert_eq!(nchar_value, nchar_value_borrowed);
        assert_eq!(nchar_value_borrowed, &nchar_value);
    }

    #[test]
    fn test_json_value() {
        let json_value = Value::Json(serde_json::json!({"hello": "world"}));
        assert_eq!(json_value.ty(), Ty::Json);
        assert_eq!(
            json_value.to_sql_value(),
            "\"{\"hello\":\"world\"}\"".to_string()
        );
        assert_eq!(
            json_value.to_string(),
            Ok("{\"hello\":\"world\"}".to_string())
        );
        assert_eq!(
            json_value.to_json_value(),
            serde_json::json!({"hello": "world"})
        );
        assert_eq!(format!("{}", json_value), "{\"hello\":\"world\"}");
        let json_value_borrowed = json_value.to_borrowed_value();
        assert_eq!(json_value_borrowed.ty(), Ty::Json);
        assert_eq!(
            json_value_borrowed.to_sql_value(),
            "\"{\"hello\":\"world\"}\"".to_string()
        );
        assert_eq!(
            json_value_borrowed.to_string(),
            Ok("{\"hello\":\"world\"}".to_string())
        );
        assert_eq!(
            json_value_borrowed.to_json_value(),
            serde_json::json!({"hello": "world"})
        );
        assert_eq!(
            format!("{}", json_value_borrowed),
            "{\\\"hello\\\":\\\"world\\\"}"
        );
        println!("{:?}", json_value_borrowed.to_str());
        assert_eq!(json_value_borrowed.to_bool(), Some(true));
        assert_eq!(json_value_borrowed.to_value(), json_value);
        assert_eq!(json_value_borrowed.clone().into_value(), json_value);
        assert_eq!(json_value_borrowed, json_value);
        assert_eq!(json_value, json_value_borrowed);
        assert_eq!(json_value_borrowed, &json_value);
    }

    #[test]
    fn test_ty() {
        let null_value = BorrowedValue::Null(Ty::Int);
        assert_eq!(null_value.ty(), Ty::Int);

        let bool_value = BorrowedValue::Bool(true);
        assert_eq!(bool_value.ty(), Ty::Bool);

        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(tiny_int_value.ty(), Ty::TinyInt);

        let small_int_value = BorrowedValue::SmallInt(1000);
        assert_eq!(small_int_value.ty(), Ty::SmallInt);

        let int_value = BorrowedValue::Int(-500);
        assert_eq!(int_value.ty(), Ty::Int);

        let big_int_value = BorrowedValue::BigInt(1234567890);
        assert_eq!(big_int_value.ty(), Ty::BigInt);

        let utiny_int_value = BorrowedValue::UTinyInt(42);
        assert_eq!(utiny_int_value.ty(), Ty::UTinyInt);

        let usmall_int_value = BorrowedValue::USmallInt(1000);
        assert_eq!(usmall_int_value.ty(), Ty::USmallInt);

        let uint_value = BorrowedValue::UInt(5000);
        assert_eq!(uint_value.ty(), Ty::UInt);

        let ubig_int_value = BorrowedValue::UBigInt(1234567890);
        assert_eq!(ubig_int_value.ty(), Ty::UBigInt);

        let float_value = BorrowedValue::Float(3.14);
        assert_eq!(float_value.ty(), Ty::Float);

        let double_value = BorrowedValue::Double(2.71828);
        assert_eq!(double_value.ty(), Ty::Double);

        let varchar_value = BorrowedValue::VarChar("hello");
        assert_eq!(varchar_value.ty(), Ty::VarChar);

        let timestamp_value = BorrowedValue::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert_eq!(timestamp_value.ty(), Ty::Timestamp);

        let blob_value = BorrowedValue::Blob(Cow::from(vec![1, 2, 3]));
        assert_eq!(blob_value.ty(), Ty::Blob);

        let medium_blob_value = BorrowedValue::MediumBlob(Cow::from(vec![1, 2, 3]));
        assert_eq!(medium_blob_value.ty(), Ty::MediumBlob);

        let varbinary_value = BorrowedValue::VarBinary(Cow::from(vec![1, 2, 3]));
        assert_eq!(varbinary_value.ty(), Ty::VarBinary);

        let geometry_value = BorrowedValue::Geometry(Cow::from(vec![1, 2, 3]));
        assert_eq!(geometry_value.ty(), Ty::Geometry);
    }

    #[test]
    fn test_value_to_sql_value() {
        let varbinary_value = Value::VarBinary(Bytes::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert_eq!(varbinary_value.to_sql_value(), "\"\\x0001ABFF\"");

        let blob_value = Value::Blob(Bytes::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert_eq!(blob_value.to_sql_value(), "\"\\x0001ABFF\"");
    }

    #[test]
    fn test_value_to_sql_value_with_rfc3339() {
        let null_value = Value::Null(Ty::Int);
        assert_eq!(null_value.to_sql_value_with_rfc3339(), "NULL".to_string());

        let bool_value = Value::Bool(true);
        assert_eq!(bool_value.to_sql_value_with_rfc3339(), "true".to_string());

        let tiny_int_value = Value::TinyInt(42);
        assert_eq!(tiny_int_value.to_sql_value_with_rfc3339(), "42".to_string());

        let small_int_value = Value::SmallInt(1000);
        assert_eq!(
            small_int_value.to_sql_value_with_rfc3339(),
            "1000".to_string()
        );

        let int_value = Value::Int(-500);
        assert_eq!(int_value.to_sql_value_with_rfc3339(), "-500".to_string());

        let big_int_value = Value::BigInt(1234567890);
        assert_eq!(
            big_int_value.to_sql_value_with_rfc3339(),
            "1234567890".to_string()
        );

        let utiny_int_value = Value::UTinyInt(42);
        assert_eq!(
            utiny_int_value.to_sql_value_with_rfc3339(),
            "42".to_string()
        );

        let usmall_int_value = Value::USmallInt(1000);
        assert_eq!(
            usmall_int_value.to_sql_value_with_rfc3339(),
            "1000".to_string()
        );

        let uint_value = Value::UInt(5000);
        assert_eq!(uint_value.to_sql_value_with_rfc3339(), "5000".to_string());

        let ubig_int_value = Value::UBigInt(1234567890);
        assert_eq!(
            ubig_int_value.to_sql_value_with_rfc3339(),
            "1234567890".to_string()
        );

        let float_value = Value::Float(3.14);
        assert_eq!(float_value.to_sql_value_with_rfc3339(), "3.14".to_string());

        let double_value = Value::Double(2.71828);
        assert_eq!(
            double_value.to_sql_value_with_rfc3339(),
            "2.71828".to_string()
        );

        let varchar_value = Value::VarChar("hello".into());
        assert_eq!(
            varchar_value.to_sql_value_with_rfc3339(),
            "\"hello\"".to_string()
        );

        let timestamp_value = Value::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert!(timestamp_value
            .to_sql_value_with_rfc3339()
            .contains("1970-01-01T"));

        let nchar_value = Value::NChar("hello".to_string());
        assert_eq!(
            nchar_value.to_sql_value_with_rfc3339(),
            "\"hello\"".to_string()
        );

        let json_value = Value::Json(serde_json::json!({"hello": "world"}));
        assert_eq!(
            json_value.to_sql_value_with_rfc3339(),
            "\"{\"hello\":\"world\"}\"".to_string()
        );

        let varbinary_value = Value::VarBinary(Bytes::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert_eq!(
            varbinary_value.to_sql_value_with_rfc3339(),
            "\"\\x0001ABFF\""
        );

        let blob_value = Value::Blob(Bytes::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert_eq!(blob_value.to_sql_value_with_rfc3339(), "\"\\x0001ABFF\"");
    }

    #[test]
    fn test_value_to_string() {
        let varbinary_value = Value::VarBinary(Bytes::from(vec![0x68, 0x65, 0x6C, 0x6C, 0x6F]));
        assert_eq!(varbinary_value.to_string(), Ok("hello".to_string()));

        let varbinary_value = Value::VarBinary(Bytes::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert!(varbinary_value.to_string().is_err());

        let blob_value = Value::Blob(Bytes::from(vec![0x68, 0x65, 0x6C, 0x6C, 0x6F]));
        assert_eq!(blob_value.to_string(), Ok("hello".to_string()));

        let blob_value = Value::Blob(Bytes::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert!(blob_value.to_string().is_err());

        let geo_value = Value::Geometry(Bytes::from(vec![
            0x70, 0x6F, 0x69, 0x6E, 0x74, 0x28, 0x31, 0x20, 0x31, 0x29,
        ]));
        assert_eq!(geo_value.to_string(), Ok("point(1 1)".to_string()));

        let geo_value = Value::Geometry(Bytes::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert!(geo_value.to_string().is_err());
    }

    #[test]
    fn test_to_sql_value_with_rfc3339() {
        let null_value = BorrowedValue::Null(Ty::Int);
        assert_eq!(null_value.to_sql_value_with_rfc3339(), "NULL".to_string());

        let bool_value = BorrowedValue::Bool(true);
        assert_eq!(bool_value.to_sql_value_with_rfc3339(), "true".to_string());

        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(tiny_int_value.to_sql_value_with_rfc3339(), "42".to_string());

        let small_int_value = BorrowedValue::SmallInt(1000);
        assert_eq!(
            small_int_value.to_sql_value_with_rfc3339(),
            "1000".to_string()
        );

        let int_value = BorrowedValue::Int(-500);
        assert_eq!(int_value.to_sql_value_with_rfc3339(), "-500".to_string());

        let big_int_value = BorrowedValue::BigInt(1234567890);
        assert_eq!(
            big_int_value.to_sql_value_with_rfc3339(),
            "1234567890".to_string()
        );

        let utiny_int_value = BorrowedValue::UTinyInt(42);
        assert_eq!(
            utiny_int_value.to_sql_value_with_rfc3339(),
            "42".to_string()
        );

        let usmall_int_value = BorrowedValue::USmallInt(1000);
        assert_eq!(
            usmall_int_value.to_sql_value_with_rfc3339(),
            "1000".to_string()
        );

        let uint_value = BorrowedValue::UInt(5000);
        assert_eq!(uint_value.to_sql_value_with_rfc3339(), "5000".to_string());

        let ubig_int_value = BorrowedValue::UBigInt(1234567890);
        assert_eq!(
            ubig_int_value.to_sql_value_with_rfc3339(),
            "1234567890".to_string()
        );

        let float_value = BorrowedValue::Float(3.14);
        assert_eq!(float_value.to_sql_value_with_rfc3339(), "3.14".to_string());

        let double_value = BorrowedValue::Double(2.71828);
        assert_eq!(
            double_value.to_sql_value_with_rfc3339(),
            "2.71828".to_string()
        );

        let varchar_value = BorrowedValue::VarChar("hello");
        assert_eq!(
            varchar_value.to_sql_value_with_rfc3339(),
            "\"hello\"".to_string()
        );

        let timestamp_value = BorrowedValue::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert!(timestamp_value
            .to_sql_value_with_rfc3339()
            .contains("1970-01-01T"));

        let nchar_value = Value::NChar("hello".to_string());
        let b_nchar_value = nchar_value.to_borrowed_value();
        assert_eq!(b_nchar_value.to_sql_value(), "\"hello\"".to_string());

        let varbinary_value = BorrowedValue::VarBinary(Cow::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert_eq!(
            varbinary_value.to_sql_value_with_rfc3339(),
            "\"\\x0001ABFF\""
        );

        let blob_value = BorrowedValue::Blob(Cow::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert_eq!(blob_value.to_sql_value_with_rfc3339(), "\"\\x0001ABFF\"");
    }

    #[test]
    fn test_to_sql_value() {
        let null_value = BorrowedValue::Null(Ty::Int);
        assert_eq!(null_value.to_sql_value(), "NULL".to_string());

        let bool_value = BorrowedValue::Bool(true);
        assert_eq!(bool_value.to_sql_value(), "true".to_string());

        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(tiny_int_value.to_sql_value(), "42".to_string());

        let small_int_value = BorrowedValue::SmallInt(1000);
        assert_eq!(small_int_value.to_sql_value(), "1000".to_string());

        let int_value = BorrowedValue::Int(-500);
        assert_eq!(int_value.to_sql_value(), "-500".to_string());

        let big_int_value = BorrowedValue::BigInt(1234567890);
        assert_eq!(big_int_value.to_sql_value(), "1234567890".to_string());

        let utiny_int_value = BorrowedValue::UTinyInt(42);
        assert_eq!(utiny_int_value.to_sql_value(), "42".to_string());

        let usmall_int_value = BorrowedValue::USmallInt(1000);
        assert_eq!(usmall_int_value.to_sql_value(), "1000".to_string());

        let uint_value = BorrowedValue::UInt(5000);
        assert_eq!(uint_value.to_sql_value(), "5000".to_string());

        let ubig_int_value = BorrowedValue::UBigInt(1234567890);
        assert_eq!(ubig_int_value.to_sql_value(), "1234567890".to_string());

        let float_value = BorrowedValue::Float(3.14);
        assert_eq!(float_value.to_sql_value(), "3.14".to_string());

        let double_value = BorrowedValue::Double(2.71828);
        assert_eq!(double_value.to_sql_value(), "2.71828".to_string());

        let varchar_value = BorrowedValue::VarChar("hello");
        assert_eq!(varchar_value.to_sql_value(), "\"hello\"".to_string());

        let timestamp_value = BorrowedValue::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert_eq!(timestamp_value.to_sql_value(), "1".to_string());

        let nchar_value = Value::NChar("hello".to_string());
        let b_nchar_value = nchar_value.to_borrowed_value();
        assert_eq!(b_nchar_value.to_sql_value(), "\"hello\"".to_string());

        let varbinary_value = BorrowedValue::VarBinary(Cow::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert_eq!(varbinary_value.to_sql_value(), "\"\\x0001ABFF\"");

        let blob_value = BorrowedValue::Blob(Cow::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert_eq!(blob_value.to_sql_value(), "\"\\x0001ABFF\"");
    }

    #[test]
    fn test_to_string() {
        let varbinary_value =
            BorrowedValue::VarBinary(Cow::from(vec![0x68, 0x65, 0x6C, 0x6C, 0x6F]));
        assert_eq!(varbinary_value.to_string(), Ok("hello".to_string()));

        let varbinary_value = BorrowedValue::VarBinary(Cow::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert!(varbinary_value.to_string().is_err());

        let blob_value = BorrowedValue::Blob(Cow::from(vec![0x68, 0x65, 0x6C, 0x6C, 0x6F]));
        assert_eq!(blob_value.to_string(), Ok("hello".to_string()));

        let blob_value = BorrowedValue::Blob(Cow::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert!(blob_value.to_string().is_err());

        let geo_value = BorrowedValue::Geometry(Cow::from(vec![
            0x70, 0x6F, 0x69, 0x6E, 0x74, 0x28, 0x31, 0x20, 0x31, 0x29,
        ]));
        assert_eq!(geo_value.to_string(), Ok("point(1 1)".to_string()));

        let geo_value = BorrowedValue::Geometry(Cow::from(vec![0x00, 0x01, 0xAB, 0xFF]));
        assert!(geo_value.to_string().is_err());
    }

    #[test]
    fn test_to_json_value() {
        let null_value = BorrowedValue::Null(Ty::Int);
        assert_eq!(null_value.to_json_value(), serde_json::Value::Null);

        let bool_value = BorrowedValue::Bool(true);
        assert_eq!(bool_value.to_json_value(), serde_json::Value::Bool(true));

        let tiny_int_value = BorrowedValue::TinyInt(42);
        assert_eq!(
            tiny_int_value.to_json_value(),
            serde_json::Value::Number(42.into())
        );

        let small_int_value = BorrowedValue::SmallInt(1000);
        assert_eq!(
            small_int_value.to_json_value(),
            serde_json::Value::Number(1000.into())
        );

        let int_value = BorrowedValue::Int(-500);
        assert_eq!(
            int_value.to_json_value(),
            serde_json::Value::Number((-500).into())
        );

        let big_int_value = BorrowedValue::BigInt(1234567890);
        assert_eq!(
            big_int_value.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );

        let utiny_int_value = BorrowedValue::UTinyInt(42);
        assert_eq!(
            utiny_int_value.to_json_value(),
            serde_json::Value::Number(42.into())
        );

        let usmall_int_value = BorrowedValue::USmallInt(1000);
        assert_eq!(
            usmall_int_value.to_json_value(),
            serde_json::Value::Number(1000.into())
        );

        let uint_value = BorrowedValue::UInt(5000);
        assert_eq!(
            uint_value.to_json_value(),
            serde_json::Value::Number(5000.into())
        );

        let ubig_int_value = BorrowedValue::UBigInt(1234567890);
        assert_eq!(
            ubig_int_value.to_json_value(),
            serde_json::Value::Number(1234567890.into())
        );

        let float_value = BorrowedValue::Float(3.14);
        assert_eq!(
            float_value.to_json_value(),
            serde_json::json!(3.140000104904175)
        );

        let double_value = BorrowedValue::Double(2.71828);
        assert_eq!(double_value.to_json_value(), serde_json::json!(2.71828));

        let varchar_value = BorrowedValue::VarChar("hello");
        assert_eq!(
            varchar_value.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );

        let timestamp_value = BorrowedValue::Timestamp(Timestamp::new(1, Precision::Millisecond));
        assert_eq!(
            timestamp_value.to_json_value(),
            serde_json::Value::Number(1.into())
        );

        let json_value = Value::Json(serde_json::json!({"hello": "world"}));
        let b_json_value = json_value.to_borrowed_value();
        assert_eq!(
            b_json_value.to_json_value(),
            serde_json::json!({"hello": "world"})
        );

        let nchar_value = Value::NChar("hello".to_string());
        let b_nchar_value = nchar_value.to_borrowed_value();
        assert_eq!(
            b_nchar_value.to_json_value(),
            serde_json::Value::String("hello".to_string())
        );
    }

    #[test]
    fn to_value_test() -> anyhow::Result<()> {
        let ref1 = BorrowedValue::Decimal64(Decimal {
            data: 12345,
            precision: 5,
            scale: 2,
        });
        let Value::Decimal64(decimal) = ref1.to_value() else {
            anyhow::bail!("not decimal value")
        };
        assert_eq!(decimal.to_string(), "123.45");
        let value1 = ref1.to_value();
        let ref2 = value1.to_borrowed_value();
        assert_eq!(
            ref2,
            BorrowedValue::Decimal64(Decimal {
                data: 12345,
                precision: 5,
                scale: 2
            })
        );
        Ok(())
    }

    #[test]
    fn desirialize_decimal_test() -> anyhow::Result<()> {
        {
            let value = Value::Decimal64(BigDecimal::from_str("123.4556")?);

            let s: String = serde::Deserialize::deserialize(&value)?;
            assert_eq!(s, "123.4556");

            let s: String = serde::Deserialize::deserialize(value)?;
            assert_eq!(s, "123.4556");
        }

        {
            let value = Value::Decimal64(BigDecimal::from_str("123.4556")?);
            let f: f64 = serde::Deserialize::deserialize(&value)?;
            assert_eq!(f, 123.4556);
            let f: f64 = serde::Deserialize::deserialize(value)?;
            assert_eq!(f, 123.4556);
        }

        {
            let value = Value::Decimal64(BigDecimal::from_str("1234556")?);
            let i: i64 = serde::Deserialize::deserialize(&value)?;
            assert_eq!(i, 1234556);
            let i: i64 = serde::Deserialize::deserialize(value)?;
            assert_eq!(i, 1234556);
        }

        {
            #[derive(serde::Deserialize)]
            struct A(i64);
            let value = Value::Decimal64(BigDecimal::from_str("1234556")?);
            let i: A = serde::Deserialize::deserialize(&value)?;
            assert_eq!(i.0, 1234556);
            let i: A = serde::Deserialize::deserialize(value)?;
            assert_eq!(i.0, 1234556);
        }

        {
            let value = Value::Decimal64(BigDecimal::from_str("1234556")?);
            let b: bool = serde::Deserialize::deserialize(&value)?;
            assert!(b);
            let b: bool =
                serde::Deserialize::deserialize(Value::Decimal64(BigDecimal::from_str("0")?))?;
            assert!(!b)
        }

        {
            let value = Value::Decimal64(BigDecimal::from_str("1234.556")?);
            let dec: BigDecimal = serde::Deserialize::deserialize(&value)?;
            assert_eq!(dec.to_string(), "1234.556");
            let dec: BigDecimal = serde::Deserialize::deserialize(value)?;
            assert_eq!(dec.to_string(), "1234.556");
        }
        Ok(())
    }

    #[test]
    fn desirialize_decimal_borrowed_value_test() -> anyhow::Result<()> {
        {
            let value = BorrowedValue::Decimal64(Decimal {
                data: 1234556,
                precision: 7,
                scale: 4,
            });

            let s: String = serde::Deserialize::deserialize(value)?;
            assert_eq!(s, "123.4556");
        }

        {
            let value = BorrowedValue::Decimal64(Decimal {
                data: 1234556,
                precision: 7,
                scale: 4,
            });
            let f: f64 = serde::Deserialize::deserialize(value)?;
            assert_eq!(f, 123.4556);
        }

        {
            let value = BorrowedValue::Decimal64(Decimal {
                data: 1234556,
                precision: 7,
                scale: 0,
            });
            let i: i64 = serde::Deserialize::deserialize(value)?;
            assert_eq!(i, 1234556);
        }

        {
            #[derive(serde::Deserialize)]
            struct A(i64);
            let value = BorrowedValue::Decimal64(Decimal {
                data: 1234556,
                precision: 7,
                scale: 0,
            });
            let i: A = serde::Deserialize::deserialize(value)?;
            assert_eq!(i.0, 1234556);
        }

        {
            let value = BorrowedValue::Decimal64(Decimal {
                data: 1234556,
                precision: 7,
                scale: 4,
            });
            let b: bool = serde::Deserialize::deserialize(value)?;
            assert!(b)
        }

        {
            let value = BorrowedValue::Decimal64(Decimal {
                data: 1234556,
                precision: 7,
                scale: 3,
            });
            let dec: BigDecimal = serde::Deserialize::deserialize(value)?;
            assert_eq!(dec.to_string(), "1234.556");
        }
        Ok(())
    }

    #[test]
    fn decimal_borrowed_value_test() -> anyhow::Result<()> {
        let value = BorrowedValue::Decimal(Decimal::new(20_446_744_073_709_551_615i128, 20, 2));
        assert_eq!(value.ty(), Ty::Decimal);
        assert_eq!(value.to_i64(), Some(204467440737095516));
        assert_eq!(value.to_bool(), Some(true));
        assert_eq!(value.to_i8(), Some(92));
        assert_eq!(value.to_f32(), Some(204467440737095516.15));
        assert_eq!(value.to_sql_value(), "204467440737095516.15");
        assert_eq!(value.to_sql_value_with_rfc3339(), "204467440737095516.15");
        assert_eq!(value.to_string(), Ok("204467440737095516.15".to_string()));
        assert_eq!(
            value.to_value(),
            Value::Decimal(bigdecimal::BigDecimal::new(
                20446744073709551615i128.into(),
                2
            ))
        );
        assert_eq!(
            value.to_json_value(),
            serde_json::Value::String("204467440737095516.15".to_string())
        );
        assert_eq!(value.to_str(), Some("204467440737095516.15".into()));
        assert_eq!(format!("{value}"), "204467440737095516.15");
        assert!(value.eq(&Value::Decimal(bigdecimal::BigDecimal::new(
            20446744073709551615i128.into(),
            2
        ))));
        assert_eq!(
            value.into_value(),
            Value::Decimal(bigdecimal::BigDecimal::new(
                20446744073709551615i128.into(),
                2
            ))
        );

        let value = BorrowedValue::Decimal64(Decimal::new(12345, 10, 0));
        assert_eq!(value.ty(), Ty::Decimal64);
        assert_eq!(value.to_i64(), Some(12345));
        assert_eq!(value.to_bool(), Some(true));
        assert_eq!(value.to_i8(), Some(57));
        assert_eq!(value.to_f32(), Some(12345.00));
        assert_eq!(value.to_sql_value(), "12345");
        assert_eq!(value.to_sql_value_with_rfc3339(), "12345");
        assert_eq!(value.to_string(), Ok("12345".to_string()));
        assert_eq!(
            value.to_value(),
            Value::Decimal64(bigdecimal::BigDecimal::new(12345.into(), 0))
        );
        assert_eq!(
            value.to_json_value(),
            serde_json::Value::String("12345".to_string())
        );
        assert_eq!(value.to_str(), Some("12345".into()));
        assert_eq!(format!("{value}"), "12345");
        assert!(value.eq(&Value::Decimal64(bigdecimal::BigDecimal::new(
            12345.into(),
            0
        ))));
        assert_eq!(
            value.into_value(),
            Value::Decimal64(bigdecimal::BigDecimal::new(12345.into(), 0))
        );
        Ok(())
    }

    #[test]
    fn decimal_value_test() -> anyhow::Result<()> {
        let value = Value::Decimal(bigdecimal::BigDecimal::new(
            20446744073709551615i128.into(),
            2,
        ));
        assert_eq!(value.ty(), Ty::Decimal);
        assert_eq!(
            value.to_borrowed_value(),
            BorrowedValue::Decimal(Decimal::new(20446744073709551615i128, 20, 2))
        );
        assert_eq!(value.to_sql_value(), "204467440737095516.15");
        assert_eq!(value.to_sql_value_with_rfc3339(), "204467440737095516.15");
        assert_eq!(value.to_string(), Ok("204467440737095516.15".to_string()));
        assert_eq!(
            value.to_json_value(),
            serde_json::Value::String("204467440737095516.15".to_string())
        );
        assert!(value.eq(&BorrowedValue::Decimal(Decimal::new(
            20446744073709551615i128,
            5,
            2
        ))));

        let value = Value::Decimal64(bigdecimal::BigDecimal::new(12345.into(), 0));
        assert_eq!(value.ty(), Ty::Decimal64);
        assert_eq!(
            value.to_borrowed_value(),
            BorrowedValue::Decimal64(Decimal::new(12345, 5, 0))
        );
        assert_eq!(value.to_sql_value(), "12345");
        assert_eq!(value.to_sql_value_with_rfc3339(), "12345");
        assert_eq!(value.to_string(), Ok("12345".to_string()));
        assert_eq!(
            value.to_json_value(),
            serde_json::Value::String("12345".to_string())
        );
        assert!(value.eq(&BorrowedValue::Decimal64(Decimal::new(12345, 5, 0))));
        Ok(())
    }

    #[test]
    fn test_borrowed_value_strict_as_str() {
        let varchar_value_borrowed = BorrowedValue::VarChar("hello");
        let str = varchar_value_borrowed.strict_as_str();
        assert_eq!(str, "hello");

        let nchar_value_borrowed = BorrowedValue::NChar(Cow::from("hello"));
        let str = nchar_value_borrowed.strict_as_str();
        assert_eq!(str, "hello");
    }

    #[test]
    fn test_value_strict_as_str() {
        let varchar_value = Value::VarChar("hello".to_string());
        let str = varchar_value.strict_as_str();
        assert_eq!(str, "hello");

        let nchar_value = Value::NChar("hello".to_string());
        let str = nchar_value.strict_as_str();
        assert_eq!(str, "hello");
    }

    #[test]
    fn test_blob_value() -> anyhow::Result<()> {
        let blob_value = Value::Blob(Bytes::from(vec![1, 2, 3]));
        assert_eq!(blob_value.ty(), Ty::Blob);
        assert_eq!(
            blob_value.to_json_value(),
            serde_json::Value::String("[1, 2, 3]".to_string())
        );
        assert_eq!(format!("{}", blob_value), "[1, 2, 3]");

        let blob_value_borrowed = blob_value.to_borrowed_value();
        assert_eq!(blob_value_borrowed.ty(), Ty::Blob);
        assert_eq!(
            blob_value_borrowed.to_json_value(),
            serde_json::Value::String("[1, 2, 3]".to_string())
        );
        assert_eq!(format!("{}", blob_value_borrowed), "[1, 2, 3]");

        assert_eq!(blob_value_borrowed.to_value(), blob_value);
        assert_eq!(blob_value_borrowed.clone().into_value(), blob_value);
        assert_eq!(
            blob_value_borrowed.to_bytes(),
            Some(Bytes::from(vec![1, 2, 3]))
        );

        assert_eq!(blob_value_borrowed, blob_value);
        assert_eq!(blob_value, blob_value_borrowed);
        assert_eq!(blob_value_borrowed, &blob_value);

        Ok(())
    }

    #[test]
    fn test_nonfinite_float_value() {
        let cases = [f32::NAN, f32::INFINITY, f32::NEG_INFINITY];
        for &v in &cases {
            assert_eq!(Value::Float(v).to_json_value(), serde_json::Value::Null);
            assert_eq!(
                BorrowedValue::Float(v).to_json_value(),
                serde_json::Value::Null
            );
        }
    }

    #[test]
    fn test_nonfinite_double_value() {
        let cases = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        for &v in &cases {
            assert_eq!(Value::Double(v).to_json_value(), serde_json::Value::Null);
            assert_eq!(
                BorrowedValue::Double(v).to_json_value(),
                serde_json::Value::Null
            );
        }
    }
}
