use serde::Deserialize;
use taos_sys::{TaosDataType, TSDB_DATA_TYPE_BINARY};

use crate::{timestamp::TimestampValue, Error};

mod borrowed;

pub use borrowed::*;

mod de;
mod from;

#[derive(Debug, Clone)]
pub enum BorrowedValue<'block> {
    Null,        // 0
    Bool(bool),  // 1
    TinyInt(i8), // 2
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Binary(&'block [u8]),
    Timestamp(TimestampValue),
    NChar(&'block str),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64), // 14
    Json(&'block [u8]),
    VarChar(&'block [u8]),
    VarBinary(&'block [u8]),
    Decimal(f64),
    Blob(&'block [u8]),
}

impl<'block> BorrowedValue<'block> {
    pub const fn taos_type(&self) -> TaosDataType {
        use BorrowedValue::*;
        match self {
            Null => TaosDataType::Null,
            TinyInt(_) => TaosDataType::TinyInt,
            SmallInt(_) => TaosDataType::SmallInt,
            Int(_) => TaosDataType::Int,
            BigInt(_) => TaosDataType::BigInt,
            UTinyInt(_) => TaosDataType::UTinyInt,
            USmallInt(_) => TaosDataType::USmallInt,
            UInt(_) => TaosDataType::UInt,
            UBigInt(_) => TaosDataType::UBigInt,
            Float(_) => TaosDataType::Float,
            Double(_) => TaosDataType::Double,
            Binary(_) => TSDB_DATA_TYPE_BINARY,
            Timestamp(_) => TaosDataType::Timestamp,
            Json(_) => TaosDataType::Json,
            NChar(_) => TaosDataType::NChar,
            _ => err!("un supported type as borrowed str"),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, BorrowedValue::Null)
    }
    pub fn strict_as_str(&self) -> Result<&str, Error> {
        use BorrowedValue::*;
        match self {
            Binary(v) | Json(v) => std::str::from_utf8(v).map_err(|err| custom_error!(err)),
            NChar(v) => Ok(*v),
            Null => err!("expect str but value is null"),
            Timestamp(v) => err!("expect str but value is timestamp"),
            _ => err!("un supported type as borrowed str"),
        }
    }
    pub fn to_string(&self) -> Result<String, Error> {
        use BorrowedValue::*;
        match self {
            Null => err!("expect string but value is null"),
            Binary(v) | Json(v) => std::str::from_utf8(v)
                .map_err(|err| err!(custom err))
                .map(|s| s.to_string()),
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
            _ => err!("un supported type as borrowed str"),
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
            Binary(v) => Value::Binary(v.to_vec()),
            Timestamp(v) => Value::Timestamp(*v),
            Json(v) => {
                Value::Json(serde_json::from_slice(*v).expect("json should be deserialized"))
            }
            NChar(str) => Value::NChar(str.to_string()),
            VarChar(_) => todo!(),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            // _ => err!("un supported type as borrowed str"),
        }
    }
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
            Timestamp(v) => Value::Timestamp(v),
            Binary(v) => Value::Binary(v.to_vec()),
            Json(v) => Value::Json(serde_json::from_slice(v).expect("json should be deserialized")),
            NChar(str) => Value::NChar(str.to_string()),
            _ => err!("un supported type as borrowed str"),
        }
    }
}

// #[derive(Debug, Clone)]
#[derive(Debug, Clone, Deserialize)]
pub enum Value {
    Null,        // 0
    Bool(bool),  // 1
    TinyInt(i8), // 2
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Binary(Vec<u8>),
    Timestamp(TimestampValue),
    NChar(String),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64), // 14
    Json(serde_json::Value),
    VarChar(String),
    VarBinary(Vec<u8>),
    Decimal(f64),
    Blob(Vec<u8>),
}

impl Value {
    pub fn taos_type(&self) -> TaosDataType {
        use Value::*;
        match self {
            Null => TaosDataType::Null,
            TinyInt(_) => TaosDataType::TinyInt,
            SmallInt(_) => TaosDataType::SmallInt,
            Int(_) => TaosDataType::Int,
            BigInt(_) => TaosDataType::BigInt,
            UTinyInt(_) => TaosDataType::UTinyInt,
            USmallInt(_) => TaosDataType::USmallInt,
            UInt(_) => TaosDataType::UInt,
            UBigInt(_) => TaosDataType::UBigInt,
            Float(_) => TaosDataType::Float,
            Double(_) => TaosDataType::Double,
            Binary(_) => TSDB_DATA_TYPE_BINARY,
            Timestamp(_) => TaosDataType::Timestamp,
            Json(_) => TaosDataType::Json,
            _ => err!("un supported type as borrowed str"),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}
