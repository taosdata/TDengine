use std::{borrow::Cow, fmt::Display, str::Utf8Error};

use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use super::{Timestamp, Ty};

#[derive(Debug, Clone)]
pub enum BorrowedValue<'b> {
    Null,        // 0
    Bool(bool),  // 1
    TinyInt(i8), // 2
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
    UBigInt(u64), // 14
    Json(Cow<'b, [u8]>),
    VarBinary(&'b [u8]),
    Decimal(Decimal),
    Blob(&'b [u8]),
    MediumBlob(&'b [u8]),
}

impl<'b> BorrowedValue<'b> {
    /// The data type of this value.
    pub const fn ty(&self) -> Ty {
        use BorrowedValue::*;
        match self {
            Null => Ty::Null,
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
        }
    }

    /// Check if the value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, BorrowedValue::Null)
    }
    /// Only VarChar, NChar, Json could be treated as [&str].
    fn strict_as_str(&self) -> &str {
        use BorrowedValue::*;
        match self {
            VarChar(v) => *v,
            NChar(v) => &v,
            Null => panic!("expect str but value is null"),
            Timestamp(_) => panic!("expect str but value is timestamp"),
            _ => panic!("expect str but only varchar/binary/nchar is supported"),
        }
    }
    pub fn to_string(&self) -> Result<String, Utf8Error> {
        use BorrowedValue::*;
        match self {
            Null => Ok(String::new()),
            VarChar(v) => Ok(v.to_string()),
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
            Timestamp(v) => Ok(v
                .to_naive_datetime()
                .format("%Y-%m-%dT%H:%M:%S%.f")
                .to_string()),
            _ => unreachable!("un supported type to string"),
        }
    }

    pub fn to_value(&self) -> Value {
        use BorrowedValue::*;
        match self {
            Null => Value::Null,
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
            VarChar(v) => Value::VarChar(v.to_string()),
            Timestamp(v) => Value::Timestamp(*v),
            Json(v) => {
                Value::Json(serde_json::from_slice(v).expect("json should always be deserialized"))
            }
            NChar(str) => Value::NChar(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        use BorrowedValue::*;
        match self {
            Null => serde_json::Value::Null,
            Bool(v) => serde_json::Value::Bool(*v),
            TinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            SmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            BigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UTinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            USmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UBigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Float(v) => serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap()),
            Double(v) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap())
            }
            VarChar(v) => serde_json::Value::String(v.to_string()),
            Timestamp(v) => serde_json::Value::Number(serde_json::Number::from(v.as_raw_i64())),
            Json(v) => serde_json::Value::Number(
                serde_json::from_slice(v).expect("json should always be deserialized"),
            ),
            NChar(str) => serde_json::Value::String(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }

    #[inline]
    pub fn into_value(self) -> Value {
        use BorrowedValue::*;
        match self {
            Null => Value::Null,
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
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }
}

unsafe impl<'b> Send for BorrowedValue<'b> {}

// #[derive(Debug, Clone)]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum Value {
    Null,        // 0
    Bool(bool),  // 1
    TinyInt(i8), // 2
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
    UBigInt(u64), // 14
    Json(serde_json::Value),
    VarBinary(Vec<u8>),
    Decimal(Decimal),
    Blob(Vec<u8>),
    MediumBlob(Vec<u8>),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Value::*;
        match self {
            Null => f.write_str("NULL"),
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
            Json(v) => f.write_fmt(format_args!("{v}")),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }
}

impl Value {
    /// The data type of this value.
    pub const fn ty(&self) -> Ty {
        use Value::*;
        match self {
            Null => Ty::Null,
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
        }
    }

    pub fn to_borrowed_value(&self) -> BorrowedValue {
        use Value::*;
        match self {
            Null => BorrowedValue::Null,
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
            VarBinary(v) => BorrowedValue::VarBinary(v),
            Decimal(v) => BorrowedValue::Decimal(*v),
            Blob(v) => BorrowedValue::Blob(v),
            MediumBlob(v) => BorrowedValue::MediumBlob(v),
        }
    }

    /// Check if the value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
    /// Only VarChar, NChar, Json could be treated as [&str].
    pub fn strict_as_str(&self) -> &str {
        use Value::*;
        match self {
            VarChar(v) => v.as_str(),
            NChar(v) => v.as_str(),
            Json(v) => v.as_str().expect("invalid str type"),
            Null => "Null",
            Timestamp(_) => panic!("expect str but value is timestamp"),
            _ => panic!("expect str but only varchar/binary/json/nchar is supported"),
        }
    }

    pub fn to_sql_value(&self) -> String {
        use Value::*;
        match self {
            Null => "NULL".to_string(),
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
            Json(v) => format!("\"{}\"", v),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }

    pub fn to_string(&self) -> Result<String, Utf8Error> {
        use Value::*;
        match self {
            Null => Ok(String::new()),
            VarChar(v) => Ok(v.to_string()),
            Json(v) => Ok(v.to_string()),
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
            Timestamp(v) => Ok(v
                .to_naive_datetime()
                .format("%Y-%m-%dT%H:%M:%S%.f")
                .to_string()),
            _ => unreachable!("un supported type to string"),
        }
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        use Value::*;
        match self {
            Null => serde_json::Value::Null,
            Bool(v) => serde_json::Value::Bool(*v),
            TinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            SmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            BigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UTinyInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            USmallInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            UBigInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Float(v) => serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap()),
            Double(v) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*v as f64).unwrap())
            }
            VarChar(v) => serde_json::Value::String(v.to_string()),
            Timestamp(v) => serde_json::Value::Number(serde_json::Number::from(v.as_raw_i64())),
            Json(v) => v.clone(),
            NChar(str) => serde_json::Value::String(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }
}

mod de;
