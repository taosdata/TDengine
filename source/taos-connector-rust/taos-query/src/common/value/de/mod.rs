//! Value de-serialization.
//!
//! All values queried from TDengine sql could be deserialized to a Rust type.
//! Commonly, you can deserialize a value directly into a [crate::common::Value].
//!
use serde::de::value::Error;
use serde::{de, forward_to_deserialize_any};

const TIMESTAMP_VARIANTS: [&str; 3] = ["Milliseconds", "Microseconds", "Nanoseconds"];
const VALUE_VARIANTS: [&str; 22] = [
    "Null",
    "Bool",
    "TinyInt",
    "SmallInt",
    "Int",
    "BigInt",
    "Float",
    "Double",
    "VarChar",
    "Timestamp",
    "NChar",
    "UTinyInt",
    "USmallInt",
    "UInt",
    "UBigInt",
    "Json",
    "VarBinary",
    "Decimal",
    "Blob",
    "MediumBlob",
    "Geometry",
    "Decimal64",
];

pub struct UnitOnly;

impl<'de> de::VariantAccess<'de> for UnitOnly {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        Err(de::Error::invalid_type(
            de::Unexpected::UnitVariant,
            &"newtype variant",
        ))
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        Err(de::Error::invalid_type(
            de::Unexpected::UnitVariant,
            &"tuple variant",
        ))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        Err(de::Error::invalid_type(
            de::Unexpected::UnitVariant,
            &"struct variant",
        ))
    }
}

#[derive(Debug, Clone)]
struct VariantTimestampDeserializer {
    value: i64,
}

impl<'de> de::Deserializer<'de> for VariantTimestampDeserializer {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i64(self.value)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}

impl<'de> de::VariantAccess<'de> for VariantTimestampDeserializer {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        todo!()
    }
}

mod borrowed;
mod ref_value;

mod value;
