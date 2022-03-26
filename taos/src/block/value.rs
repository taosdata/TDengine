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

impl<'de, 'a, 'block> serde::de::Deserializer<'de> for BorrowedValue<'block> {
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
            Json(v) => visitor.visit_bytes(v),
            Timestamp(v) => visitor.visit_i64(*v.as_raw_i64()),
            _ => Err(Self::Error::from_string("un supported type to deserialize")),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::debug!("call deserialize_string");
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
            Json(v) => visitor.visit_bytes(v),
            Timestamp(v) => visitor.visit_string(dbg!(v
                .to_naive_datetime()
                .format("%Y-%m-%dT%H:%M:%S%.f")
                .to_string())),
            _ => Err(Self::Error::from_string("un supported type to deserialize")),
        }
    }
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::debug!("call deserialize_str");
        self.deserialize_string(visitor)
    }

    serde::forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

impl<'de, 'a, 'block> serde::de::IntoDeserializer<'de, taos_error::Error>
    for BorrowedValue<'block>
{
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}
