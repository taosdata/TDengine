use serde::de;

use crate::timestamp::TimestampValue;


#[derive(Debug)]
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
    pub fn is_null(&self) -> bool {
        matches!(self, BorrowedValue::Null)
    }
}

impl<'de> serde::de::Deserializer<'de> for BorrowedValue<'de> {
    type Error = taos_error::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::debug!("call deserialize_any");
        use BorrowedValue::*;
        match self {
            Null => visitor.visit_none(),
            Bool(v) => visitor.visit_bool(v),
            TinyInt(v) => visitor.visit_i8(v),
            SmallInt(v) => visitor.visit_i16(v),
            Int(v) => visitor.visit_i32(v),
            BigInt(v) => visitor.visit_i64(v),
            UTinyInt(v) => visitor.visit_u8(v),
            USmallInt(v) => visitor.visit_u16(v),
            UInt(v) => visitor.visit_u32(v),
            UBigInt(v) => visitor.visit_u64(v),
            Float(v) => visitor.visit_f32(v),
            Double(v) => visitor.visit_f64(v),
            Binary(v) => visitor.visit_bytes(v),
            NChar(v) => visitor.visit_str(v),
            Json(v) => serde_json::Deserializer::from_slice(v)
                .deserialize_any(visitor)
                .map_err(<Self::Error as de::Error>::custom),
            Timestamp(v) => visitor.visit_i64(*v.as_raw_i64()),
            _ => Err(Self::Error::from_string("un supported type to deserialize")),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::debug!("call deserialize_string");
        use BorrowedValue::*;
        match self {
            Null => visitor.visit_str(""), // todo: empty string or error?
            // Null => Err(Self::Error::from_string(
            // "expect non-optional String, but value is null",
            // )),
            Bool(v) => visitor.visit_bool(v),
            TinyInt(v) => visitor.visit_i8(v),
            SmallInt(v) => visitor.visit_i16(v),
            Int(v) => visitor.visit_i32(v),
            BigInt(v) => visitor.visit_i64(v),
            UTinyInt(v) => visitor.visit_u8(v),
            USmallInt(v) => visitor.visit_u16(v),
            UInt(v) => visitor.visit_u32(v),
            UBigInt(v) => visitor.visit_u64(v),
            Float(v) => visitor.visit_f32(v),
            Double(v) => visitor.visit_f64(v),
            Binary(v) | Json(v) => std::str::from_utf8(v)
                .map_err(<Self::Error as serde::de::Error>::custom)
                .and_then(|s| visitor.visit_str(s)),
            NChar(v) => visitor.visit_str(v),
            Timestamp(v) => visitor.visit_string(
                v.to_naive_datetime()
                    .format("%Y-%m-%dT%H:%M:%S%.f")
                    .to_string(),
            ),
            _ => Err(Self::Error::from_string("un supported type to deserialize")),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::debug!("call deserialize_str");
        self.deserialize_str(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::debug!("call deserialize_option");
        if self.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char unit
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

impl<'de> serde::de::IntoDeserializer<'de, taos_error::Error>
    for BorrowedValue<'de>
{
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}
