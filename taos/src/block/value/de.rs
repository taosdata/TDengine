use super::*;

use serde::{
    de::{self, DeserializeSeed, IntoDeserializer, Visitor},
    forward_to_deserialize_any, Deserialize,
};

macro_rules! _de_primitive {
    ($($ty:ident) *) => {
        paste::paste! {
            $(
                fn [<deserialize_ $ty>] <V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: serde::de::Visitor<'de>,
            {
                log::debug!(stringify!([<call_deserialize_ $ty>]));
                use BorrowedValue::*;
                dbg!(&self);
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
                    Timestamp(v) => visitor.visit_i64(*v.as_raw_i64()),
                    _ => err!("expect u64 but not support"),
                }
            }
            )*
        }
    };
}

impl<'a, 'de> serde::de::EnumAccess<'de> for BorrowedValue<'de> {
    type Error = Error;

    type Variant = UnitOnly;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let s = self.strict_as_str()?;
        return Ok((
            seed.deserialize(StringDeserializer {
                input: s.to_string(),
            })?,
            UnitOnly,
        ));
    }

    fn variant<V>(self) -> Result<(V, Self::Variant), Self::Error>
    where
        V: Deserialize<'de>,
    {
        // todo!();
        self.variant_seed(std::marker::PhantomData)
    }
}

#[derive(Debug, Clone)]
struct EnumTimestampValueDeserializer<'a> {
    value: BorrowedValue<'a>,
}

#[derive(Debug, Clone)]
struct VariantTimestampValueDeserializer {
    value: i64,
}
impl<'de> de::Deserializer<'de> for VariantTimestampValueDeserializer {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.value)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}
impl<'de> de::VariantAccess<'de> for VariantTimestampValueDeserializer {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
        // self.input.get(1).map_or(
        //     Err(de::Error::custom(
        //         "Expected a tuple variant, got nothing instead.",
        //     )),
        //     |item| de::Deserializer::deserialize_seq(&Deserializer::new(&item.1), visitor),
        // )
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
        // self.input.get(1).map_or(
        //     Err(de::Error::custom("Expected a struct variant, got nothing")),
        //     |item| {
        //         de::Deserializer::deserialize_struct(
        //             &Deserializer::new(&item.1),
        //             "",
        //             fields,
        //             visitor,
        //         )
        //     },
        // )
    }
}

impl<'de> serde::de::EnumAccess<'de> for EnumTimestampValueDeserializer<'de> {
    type Error = Error;

    type Variant = VariantTimestampValueDeserializer;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        // let variant = self.value.taos_type().as_variant_str();
        use BorrowedValue::*;
        match self.value {
            Timestamp(TimestampValue::Microseconds(v)) => Ok((
                seed.deserialize(StringDeserializer {
                    input: "Microseconds".to_string(),
                })
                .expect(""),
                VariantTimestampValueDeserializer { value: v },
            )),
            Timestamp(TimestampValue::Milliseconds(v)) => Ok((
                seed.deserialize(StringDeserializer {
                    input: "Milliseconds".to_string(),
                })
                .expect(""),
                VariantTimestampValueDeserializer { value: v },
            )),
            Timestamp(TimestampValue::Nanoseconds(v)) => Ok((
                seed.deserialize(StringDeserializer {
                    input: "Nanoseconds".to_string(),
                })
                .expect(""),
                VariantTimestampValueDeserializer { value: v },
            )),
            _ => todo!(),
        }
    }
}

#[derive(Debug, Clone)]
struct EnumValueDeserializer<'a> {
    value: BorrowedValue<'a>,
}

#[derive(Clone)]
struct StringDeserializer {
    input: String,
}

impl<'de> de::Deserializer<'de> for StringDeserializer {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.input)
    }

    forward_to_deserialize_any! {
        bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
        seq bytes byte_buf map unit_struct newtype_struct
        tuple_struct struct tuple enum identifier ignored_any
    }
}
impl<'de> serde::de::EnumAccess<'de> for EnumValueDeserializer<'de> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let variant = self.value.taos_type().as_variant_str();

        Ok((
            seed.deserialize(StringDeserializer {
                input: variant.to_string(),
            })
            .expect(""),
            self,
        ))
    }
}

impl<'de> de::VariantAccess<'de> for EnumValueDeserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.value)
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
        // self.input.get(1).map_or(
        //     Err(de::Error::custom(
        //         "Expected a tuple variant, got nothing instead.",
        //     )),
        //     |item| de::Deserializer::deserialize_seq(&Deserializer::new(&item.1), visitor),
        // )
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
        // self.input.get(1).map_or(
        //     Err(de::Error::custom("Expected a struct variant, got nothing")),
        //     |item| {
        //         de::Deserializer::deserialize_struct(
        //             &Deserializer::new(&item.1),
        //             "",
        //             fields,
        //             visitor,
        //         )
        //     },
        // )
    }
}

impl<'de> serde::de::Deserializer<'de> for BorrowedValue<'de> {
    type Error = taos_error::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize_any: {self:?}");
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
            Binary(v) => unsafe { std::str::from_utf8_unchecked(v) }
                .into_deserializer()
                .deserialize_any(visitor),
            NChar(v) => visitor.visit_str(v),
            Json(v) => serde_json::Deserializer::from_slice(v)
                .deserialize_any(visitor)
                .map_err(<Self::Error as de::Error>::custom),
            Timestamp(v) => visitor.visit_i64(*v.as_raw_i64()),
            _ => Err(Self::Error::from_string("un supported type to deserialize")),
        }
    }

    _de_primitive!(bool u8 u16 u32 i8 i16 i32 u64 f32 f64 char);

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call_deserialize_i64");
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
            Timestamp(v) => visitor.visit_i64(*v.as_raw_i64()),
            _ => err!("expect u64 but not support"),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize_str");
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
        log::trace!("call deserialize_string");
        visitor.visit_string(self.to_string()?)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize_option");
        if self.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        log::trace!("deserialize_newtype_struct: {_name}");
        self.deserialize_any(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize_map");
        self.deserialize_any(visitor)
    }
    serde::forward_to_deserialize_any! {
        unit
        bytes byte_buf unit_struct
        tuple_struct tuple identifier ignored_any
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize seq by value");
        use BorrowedValue::*;
        match self {
            Null => Vec::<u8>::new()
                .into_deserializer()
                .deserialize_seq(visitor),
            Binary(v) | Json(v) => v.to_vec().into_deserializer().deserialize_seq(visitor),
            Timestamp(_) => todo!(),
            NChar(v) => v
                .as_bytes()
                .to_vec()
                .into_deserializer()
                .deserialize_seq(visitor),
            _ => todo!(),
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        const TIMESTAMP_VARIANTS: [&'static str; 3] =
            ["Milliseconds", "Microseconds", "Nanoseconds"];
        const VALUE_VARIANTS: [&'static str; 20] = [
            "Null",
            "Bool",
            "TinyInt",
            "SmallInt",
            "Int",
            "BigInt",
            "Float",
            "Double",
            "Binary",
            "Timestamp",
            "NChar",
            "UTinyInt",
            "USmallInt",
            "UInt",
            "UBigInt",
            "Json",
            "VarChar",
            "VarBinary",
            "Decimal",
            "Blob",
        ];

        log::trace!("name: {name}, variants: {variants:?}");

        if name == "TimestampValue" && variants == TIMESTAMP_VARIANTS {
            return visitor.visit_enum(EnumTimestampValueDeserializer { value: self });
        }
        if name == "Value" && variants == VALUE_VARIANTS {
            return visitor.visit_enum(EnumValueDeserializer {
                // variants,
                value: self,
            });
        }

        visitor.visit_enum(self)
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            BorrowedValue::Json(json) => serde_json::Deserializer::from_slice(json)
                .deserialize_struct(name, fields, visitor)
                .map_err(<Self::Error as serde::de::Error>::custom),
            _ => self.deserialize_any(visitor),
        }
    }
}

impl<'de> serde::de::IntoDeserializer<'de, taos_error::Error> for BorrowedValue<'de> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

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

// impl<'de> serde::de::Deserializer<'de> for BinaryRef<'de> {
//     type Error = taos_error::Error;

//     fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         // todo!()
//         log::trace!("binary ref deserialize any");
//         visitor.visit_bytes(self.0)
//         // .map_err(<Self::Error as de::Error>::custom)
//         // .or_else(|_| {
//         //     std::str::from_utf8(self.0)
//         //         .map_err(|e| TaosError::from_string(e.to_string()))
//         //         .and_then(|s| {
//         //             visitor
//         //                 .visit_str(s)
//         //                 .map_err(<Self::Error as de::Error>::custom)
//         //         })
//         // })
//     }
//     fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: serde::de::Visitor<'de>,
//     {
//         // todo!()
//         log::trace!("binary ref deserialize str");

//         std::str::from_utf8(self.0)
//             .map_err(|e| Error::from_string(e.to_string()))
//             .and_then(|s| {
//                 visitor.visit_str(s)
//                 // .map_err(<Self::Error as de::Error>::custom)
//             })
//         // })
//     }

//     fn deserialize_enum<V>(
//         self,
//         _name: &str,
//         _variants: &'static [&'static str],
//         visitor: V,
//     ) -> Result<V::Value, Self::Error>
//     where
//         V: de::Visitor<'de>,
//     {
//         log::trace!("BinaryRef deserialize enum");
//         visitor.visit_enum(self)
//     }
//     serde::forward_to_deserialize_any! {
//         bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char string
//         bytes byte_buf option unit unit_struct newtype_struct seq tuple
//         tuple_struct map struct identifier ignored_any
//     }
// }
// impl<'de> de::EnumAccess<'de> for BinaryRef<'de> {
//     type Error = Error;
//     type Variant = TaosDataType;

//     fn variant_seed<T>(self, seed: T) -> Result<(T::Value, Self::Variant), Self::Error>
//     where
//         T: de::DeserializeSeed<'de>,
//     {
//         let value = seed.deserialize(self)?;
//         Ok((value, TaosDataType::Binary))
//     }
// }

// struct EnumDeserializer<'de, 'block> {
//     input: &'de [(String, BinaryRef<'block>)],
// }

// impl<'de, 'block> EnumDeserializer<'de, 'block> {
//     fn new(input: &'de [(String, BinaryRef<'block>)]) -> Self {
//         EnumDeserializer { input }
//     }
// }
// impl<'de, 'block> de::VariantAccess<'de> for EnumDeserializer<'de, 'block> {
//     type Error = Error;

//     fn unit_variant(self) -> Result<(), Self::Error> {
//         Ok(())
//     }

//     fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
//     where
//         T: DeserializeSeed<'de>,
//     {
//         self.input.get(1).map_or(
//             Err(de::Error::custom(
//                 "Expected a newtype variant, got nothing instead.",
//             )),
//             |item| seed.deserialize(item.1.clone()),
//         )
//     }

//     fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         self.input.get(1).map_or(
//             Err(de::Error::custom(
//                 "Expected a tuple variant, got nothing instead.",
//             )),
//             |item| de::Deserializer::deserialize_seq(item.1.clone(), visitor),
//         )
//     }

//     fn struct_variant<V>(
//         self,
//         fields: &'static [&'static str],
//         visitor: V,
//     ) -> Result<V::Value, Self::Error>
//     where
//         V: Visitor<'de>,
//     {
//         self.input.get(1).map_or(
//             Err(de::Error::custom("Expected a struct variant, got nothing")),
//             |item| de::Deserializer::deserialize_struct(item.1.clone(), "", fields, visitor),
//         )
//     }
// }

// impl<'de, 'block> de::EnumAccess<'de> for EnumDeserializer<'de, 'block> {
//     type Error = Error;
//     type Variant = Self;

//     fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
//     where
//         V: DeserializeSeed<'de>,
//     {
//         self.input.first().map_or(
//             Err(de::Error::custom("A record must have a least one field")),
//             |item| {
//                 dbg!(&item.0);
//                 todo!()
//                 // match (item.0.as_ref(), &item.1) {
//                 //     ("type", Value::String(x)) | ("type", Value::Enum(_, x)) => Ok((
//                 //         seed.deserialize(StringDeserializer {
//                 //             input: x.to_owned(),
//                 //         })?,
//                 //         self,
//                 //     )),
//                 //     (field, Value::String(_)) => Err(de::Error::custom(format!(
//                 //         "Expected first field named 'type': got '{}' instead",
//                 //         field
//                 //     ))),
//                 //     (_, _) => Err(de::Error::custom(
//                 //         "Expected first field of type String or Enum for the type name".to_string(),
//                 //     )),
//                 // }
//             },
//         )
//     }
// }
