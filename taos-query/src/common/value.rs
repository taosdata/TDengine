use std::str::Utf8Error;

use serde::{Deserialize, Serialize};

use crate::Valuable;

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
    NChar(&'b str),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64), // 14
    Json(&'b [u8]),
    VarBinary(&'b [u8]),
    Decimal(f64),
    Blob(&'b [u8]),
    MediumBlob(&'b [u8]),
}

impl<'de, 'b: 'de, 'r: 'b, 'q: 'r> Valuable<'de, 'b, 'r, 'q> for BorrowedValue<'b> {
    #[inline]
    fn is_null(&self) -> bool {
        use BorrowedValue::*;
        matches!(self, Null)
    }

    #[inline]
    fn as_borrowed_value(&self) -> BorrowedValue<'b> {
        self.clone()
    }

    #[inline]
    fn into_owned_value(self) -> crate::Value {
        self.into_value()
    }

    fn ty(&self) -> Ty {
        self.ty()
    }
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
    const fn strict_as_str(&self) -> &str {
        use BorrowedValue::*;
        match self {
            VarChar(v) => *v,
            NChar(v) => *v,
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
            Json(v) => Ok(unsafe { std::str::from_utf8_unchecked(*v) }.to_string()),
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
            _ => unreachable!("un supported type as borrowed str"),
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
                Value::Json(serde_json::from_slice(*v).expect("json should always be deserialized"))
            }
            NChar(str) => Value::NChar(str.to_string()),
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
                Value::Json(serde_json::from_slice(v).expect("json should always be deserialized"))
            }
            NChar(str) => Value::NChar(str.to_string()),
            VarBinary(_) => todo!(),
            Decimal(_) => todo!(),
            Blob(_) => todo!(),
            MediumBlob(_) => todo!(),
        }
    }
}

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
    Decimal(f64),
    Blob(Vec<u8>),
    MediumBlob(Vec<u8>),
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

    /// Check if the value is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
    /// Only VarChar, NChar, Json could be treated as [&str].
    fn strict_as_str(&self) -> &str {
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
}

impl<'b> crate::Valuable2<'b> for BorrowedValue<'b> {
    fn is_null(&self) -> bool {
      use BorrowedValue::*;
        matches!(self, Null)
    }

    fn as_borrowed_value(&self) -> BorrowedValue<'b> {
        todo!()
    }

    fn into_owned_value(self) -> crate::Value {
        self.to_value()
    }

    fn ty(&self) -> Ty {
        self.ty()
    }
}
impl<'de, 'b: 'de, 'r: 'b, 'q: 'r> Valuable<'de, 'b, 'q, 'q> for Value {
    fn is_null(&self) -> bool {
        use Value::*;
        matches!(self, Null)
    }

    fn as_borrowed_value(&self) -> BorrowedValue<'b> {
        todo!()
    }

    fn into_owned_value(self) -> crate::Value {
        self
    }

    fn ty(&self) -> Ty {
        self.ty()
    }
}

impl<'de, 'v: 'de, 'b: 'de, 'r: 'b, 'q: 'r> Valuable<'de, 'b, 'r, 'q> for &'v Value {
    fn is_null(&self) -> bool {
        use Value::*;
        matches!(self, Null)
    }

    fn as_borrowed_value(&self) -> BorrowedValue<'b> {
        todo!()
    }

    fn into_owned_value(self) -> crate::Value {
        (*self).clone()
    }

    fn ty(&self) -> Ty {
        (*self).ty()
    }
}

mod de;
