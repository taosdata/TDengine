use super::super::*;
use super::*;
use serde::{
    de::{self, value::Error, DeserializeSeed, IntoDeserializer, Visitor},
    forward_to_deserialize_any,
};

impl<'de, 'v> serde::de::EnumAccess<'de> for &'v Value {
    type Error = Error;

    type Variant = UnitOnly;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let s = self.strict_as_str();
        return Ok((
            seed.deserialize(StringDeserializer {
                input: s.to_string(),
            })?,
            UnitOnly,
        ));
    }
}

#[derive(Debug, Clone)]
struct EnumTimestampDeserializer<'v> {
    value: &'v Value,
}

impl<'v, 'de> serde::de::EnumAccess<'de> for EnumTimestampDeserializer<'v> {
    type Error = Error;

    type Variant = VariantTimestampDeserializer;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        // let variant = self.value.ty().as_variant_str();
        // use BorrowedValue::*;
        match self.value {
            Value::Timestamp(Timestamp::Microseconds(v)) => Ok((
                seed.deserialize(StringDeserializer {
                    input: "Microseconds".to_string(),
                })
                .expect(""),
                VariantTimestampDeserializer { value: *v },
            )),
            Value::Timestamp(Timestamp::Milliseconds(v)) => Ok((
                seed.deserialize(StringDeserializer {
                    input: "Milliseconds".to_string(),
                })
                .expect(""),
                VariantTimestampDeserializer { value: *v },
            )),
            Value::Timestamp(Timestamp::Nanoseconds(v)) => Ok((
                seed.deserialize(StringDeserializer {
                    input: "Nanoseconds".to_string(),
                })
                .expect(""),
                VariantTimestampDeserializer { value: *v },
            )),
            _ => todo!(),
        }
    }
}

#[derive(Debug, Clone)]
struct EnumValueDeserializer<'v> {
    value: &'v Value,
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
impl<'de, 'v: 'de> serde::de::EnumAccess<'de> for EnumValueDeserializer<'v> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let variant = self.value.ty().as_variant_str();

        Ok((
            seed.deserialize(StringDeserializer {
                input: variant.to_string(),
            })
            .expect(""),
            self,
        ))
    }
}

impl<'de, 'v: 'de> de::VariantAccess<'de> for EnumValueDeserializer<'v> {
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
    }
}

impl<'de, 'v: 'de> serde::de::Deserializer<'de> for &'v Value {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize_any: {self:?}");
        use Value::*;
        // todo!()
        match self {
            Null => visitor.visit_none(),
            Bool(v) => visitor.visit_bool(*v),
            TinyInt(v) => visitor.visit_i8(*v),
            SmallInt(v) => visitor.visit_i16(*v),
            Int(v) => visitor.visit_i32(*v),
            BigInt(v) => visitor.visit_i64(*v),
            UTinyInt(v) => visitor.visit_u8(*v),
            USmallInt(v) => visitor.visit_u16(*v),
            UInt(v) => visitor.visit_u32(*v),
            UBigInt(v) => visitor.visit_u64(*v),
            Float(v) => visitor.visit_f32(*v),
            Double(v) => visitor.visit_f64(*v),
            VarChar(v) => visitor.visit_borrowed_str(v),
            NChar(v) => visitor.visit_borrowed_str(v),
            Json(v) => v
                .clone()
                .into_deserializer()
                .deserialize_any(visitor)
                .map_err(<Self::Error as de::Error>::custom),
            Timestamp(v) => visitor.visit_i64(v.as_raw_i64()),
            VarBinary(v) | Blob(v) | MediumBlob(v) => visitor.visit_borrowed_bytes(v),
            _ => Err(<Self::Error as de::Error>::custom(
                "un supported type to deserialize",
            )),
        }
    }

    serde::forward_to_deserialize_any! {bool i8 u8 i16 u16 i32 u32 i64 u64 f32 f64 char}

    serde::forward_to_deserialize_any! {
        unit
        bytes byte_buf unit_struct
        tuple_struct tuple identifier ignored_any
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize_str");
        use Value::*;
        match self {
            Null => visitor.visit_borrowed_str(""), // todo: empty string or error?
            Bool(v) => visitor.visit_bool(*v),
            TinyInt(v) => visitor.visit_i8(*v),
            SmallInt(v) => visitor.visit_i16(*v),
            Int(v) => visitor.visit_i32(*v),
            BigInt(v) => visitor.visit_i64(*v),
            UTinyInt(v) => visitor.visit_u8(*v),
            USmallInt(v) => visitor.visit_u16(*v),
            UInt(v) => visitor.visit_u32(*v),
            UBigInt(v) => visitor.visit_u64(*v),
            Float(v) => visitor.visit_f32(*v),
            Double(v) => visitor.visit_f64(*v),
            VarChar(v) | NChar(v) => visitor.visit_borrowed_str(v),
            Json(v) => visitor.visit_string(v.to_string()),
            Timestamp(v) => visitor.visit_string(
                v.to_naive_datetime()
                    .format("%Y-%m-%dT%H:%M:%S%.f")
                    .to_string(),
            ),
            _ => Err(<Self::Error as de::Error>::custom(
                "un supported type to deserialize",
            )),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize_string");
        self.deserialize_str(visitor)
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
        use Value::*;
        macro_rules! _v_ {
            ($v:expr) => {
                visitor.visit_newtype_struct($v.into_deserializer())
            };
        }
        match self {
            Null => visitor.visit_none(),
            Bool(v) => _v_!(v),
            TinyInt(v) => _v_!(v),
            SmallInt(v) => _v_!(v),
            Int(v) => _v_!(v),
            BigInt(v) => _v_!(v),
            UTinyInt(v) => _v_!(v),
            USmallInt(v) => _v_!(v),
            UInt(v) => _v_!(v),
            UBigInt(v) => _v_!(v),
            Float(v) => _v_!(v),
            Double(v) => _v_!(v),
            VarChar(v) | NChar(v) => visitor.visit_newtype_struct(v.as_str().into_deserializer()),
            Json(v) => visitor
                .visit_newtype_struct(v.clone().into_deserializer())
                .map_err(<Self::Error as de::Error>::custom),
            Timestamp(v) => _v_!(v.as_raw_i64()),
            VarBinary(v) | Blob(v) | MediumBlob(v) => {
                visitor.visit_newtype_struct(v.as_slice().into_deserializer())
            }
            _ => Err(<Self::Error as de::Error>::custom(
                "un supported type to deserialize",
            )),
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize_map");
        self.deserialize_any(visitor)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        log::trace!("call deserialize seq by value");
        use Value::*;
        match self {
            Null => Vec::<u8>::new()
                .into_deserializer()
                .deserialize_seq(visitor),
            Json(v) => v
                .clone()
                .into_deserializer()
                .deserialize_seq(visitor)
                .map_err(<Self::Error as de::Error>::custom),
            VarChar(v) | NChar(v) => v
                .as_bytes()
                .to_vec()
                .into_deserializer()
                .deserialize_seq(visitor),
            VarBinary(v) | Blob(v) | MediumBlob(v) => {
                v.clone().into_deserializer().deserialize_any(visitor)
            }
            _ => self.deserialize_any(visitor),
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
        log::trace!("name: {name}, variants: {variants:?}");

        if name == "Timestamp" && variants == TIMESTAMP_VARIANTS {
            return visitor.visit_enum(EnumTimestampDeserializer { value: self });
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
            Value::Json(json) => json
                .clone()
                .into_deserializer()
                .deserialize_struct(name, fields, visitor)
                .map_err(<Self::Error as serde::de::Error>::custom),
            _ => self.deserialize_any(visitor),
        }
    }
}

impl<'de, 'b: 'de> serde::de::IntoDeserializer<'de, Error> for &'b Value {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn value_de_value_ref() {
        use std::cmp::PartialEq;
        use Value::*;
        macro_rules! _de_value {
            ($($v:expr) *) => {
                $(
                    {
                        let v = &$v;
                        let d = Value::deserialize(v.into_deserializer()).expect("");
                        assert!(dbg!(d).eq(&$v));
                    }
                )*
            }
        }
        _de_value!(
            Null Bool(true) TinyInt(0xf) SmallInt(0xfff) Int(0xffff) BigInt(-1) Float(1.0) Double(1.0)
            UTinyInt(0xf) USmallInt(0xfff) UInt(0xffff) UBigInt(0xffffffff)
            Timestamp(crate::common::timestamp::Timestamp::Milliseconds(0)) VarChar("anything".to_string())
            NChar("你好，世界".to_string()) VarBinary(vec![1,2,3]) Blob(vec![1,2, 3]) MediumBlob(vec![1,2,3])
            Json(serde_json::json!({"name": "ABC"}))
        );
    }

    #[test]
    fn de_value_as_inner() {
        use Value::*;
        macro_rules! _de_value {
            ($($v:expr, $ty:ty, $tv:expr) *) => {
                $(
                    {
                        let v = &$v;
                        let d = <$ty>::deserialize(v.into_deserializer()).expect("");
                        assert_eq!(d, $tv);
                    }
                )*
            }
        }

        _de_value!(
            Null, Option<u8>, None
            TinyInt(-1), i8, -1
            SmallInt(-1), i16, -1
            Int(0x0fff_ffff), i32, 0x0fff_ffff
            BigInt(0xffffffff), i64, 0xffffffff
            UTinyInt(0xff), u8, 0xff
            USmallInt(0xffff), u16, 0xffff
            UInt(0x_ffff_ffff), u32, 0x_ffff_ffff
            UBigInt(0x_ffff_ffff_ffff_ffff), u64, 0x_ffff_ffff_ffff_ffff
            Float(1.0), f32, 1.0
            Double(f64::MAX), f64, f64::MAX
            VarChar("".to_string()), String, "".to_string()
            NChar("".to_string()), String, "".to_string()
            Timestamp(crate::Timestamp::Milliseconds(1)), crate::Timestamp, crate::Timestamp::Milliseconds(1)
            VarBinary(vec![0, 1,2]), Vec<u8>, vec![0, 1, 2]
            Blob(vec![0, 1,2]), Vec<u8>, vec![0, 1, 2]
            MediumBlob(vec![0, 1,2]), Vec<u8>, vec![0, 1, 2]
        );
    }

    #[test]
    fn de_borrowed_str() {
        use serde_json::json;
        use Value::*;

        macro_rules! _de_str {
            ($v:expr, is_err) => {
                assert!(<&str>::deserialize((&$v).into_deserializer()).is_err());
            };
            ($v:expr, $tv:expr) => {
                assert_eq!(<&str>::deserialize((&$v).into_deserializer()).expect("str"), $tv);
            };
            ($($v:expr, $c:ident) *) => {
                $(
                    {
                        assert!(<&str>::deserialize(($v).into_deserializer()).$c());
                    }
                )*
            };
            ($($v2:expr, is_err) +; $($v:expr, $tv:expr) + ) => {
                $(
                    _de_str!($v2, is_err);
                )*
                $(
                    _de_str!($v, $tv);
                )*
            }
        }
        _de_str! {
            TinyInt(-1), is_err
            SmallInt(-1), is_err
            Int(-1), is_err
            BigInt(-1), is_err
            UTinyInt(1), is_err
            USmallInt(1), is_err
            UInt(1), is_err
            UBigInt(1), is_err
            Json(json!({ "name": "abc"})), is_err
            Json(json!(1)), is_err
            Json(json!(null)), is_err
            Json(json!("abc")), is_err
            Timestamp(crate::Timestamp::Milliseconds(0)), is_err
            ;
            Null, ""
            VarChar("String".to_string()), "String"
            VarChar("你好，世界".to_string()), "你好，世界"
        };
    }
    #[test]
    fn de_string() {
        use serde_json::json;
        use Value::*;

        macro_rules! _de_str {
            ($v:expr, is_err) => {
                assert!(String::deserialize((&$v).into_deserializer()).is_err());
            };
            ($v:expr, $tv:expr) => {
                assert_eq!(String::deserialize((&$v).into_deserializer()).expect("str"), $tv);
            };
            ($($v:expr, $c:ident) *) => {
                $(
                    {
                        assert!(String::deserialize(($v).into_deserializer()).$c());
                    }
                )*
            };
            ($($v2:expr, is_err) +; $($v:expr, $tv:expr) + ) => {
                $(
                    _de_str!($v2, is_err);
                )*
                $(
                    _de_str!($v, $tv);
                )*
            }
        }
        _de_str! {

            TinyInt(-1), is_err
            SmallInt(-1), is_err
            Int(-1), is_err
            BigInt(-1), is_err
            UTinyInt(1), is_err
            USmallInt(1), is_err
            UInt(1), is_err
            UBigInt(1), is_err
            ;

            Null, ""
            Timestamp(crate::Timestamp::Milliseconds(0)), "1970-01-01T00:00:00"
            VarChar("String".to_string()), "String"
            VarChar("你好，世界".to_string()), "你好，世界"
            Json(json!("abc")), json!("abc").to_string()
            Json(json!({ "name": "abc"})), json!({ "name": "abc"}).to_string()
            Json(json!(1)), json!(1).to_string()
            Json(json!(null)), json!(null).to_string()
        };
    }

    #[test]
    fn de_json() {
        use Value::*;

        macro_rules! _de_json {
            ($v:expr, $ty:ty, $tv:expr) => {{
                let v = &Json($v);
                let d = <$ty>::deserialize(v.into_deserializer()).expect("de json");
                assert_eq!(d, $tv);
            }};
        }

        _de_json!(
            serde_json::json!("string"),
            String,
            "\"string\"".to_string()
        );

        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct Obj {
            name: String,
        }
        _de_json!(
            serde_json::json!({ "name": "string" }),
            Obj,
            Obj {
                name: "string".to_string()
            }
        );
    }

    #[test]
    fn de_newtype_struct() {
        use serde_json::json;
        use Value::*;

        macro_rules! _de_ty {
            ($v:expr, $ty:ty, $tv:expr) => {{
                let d = <$ty>::deserialize((&$v).into_deserializer()).expect("de type");
                assert_eq!(d, $tv);
            }};
        }
        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct JsonStr(String);
        _de_ty!(
            Json(json!("string")),
            JsonStr,
            JsonStr("string".to_string())
        );

        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct Primi<T>(T);
        _de_ty!(Json(json!(1)), Primi<i32>, Primi(1));
        _de_ty!(TinyInt(1), Primi<i8>, Primi(1));
        _de_ty!(SmallInt(1), Primi<i64>, Primi(1));
        _de_ty!(Int(1), Primi<i64>, Primi(1));
        _de_ty!(BigInt(1), Primi<i64>, Primi(1));

        macro_rules! _de_prim {
            ($v:ident, $ty:ty, $inner:literal) => {
                log::info!(
                    "ty: {}, prim: {}",
                    stringify!($v),
                    std::any::type_name::<$ty>()
                );
                _de_ty!($v($inner), Primi<$ty>, Primi($inner));
            };
        }

        macro_rules! _de_prim_cross {
            ($($vty:ident) +, $tt:ty) => {
                $(
                  _de_prim!($vty, $tt, 1);
                )*
            };
            ($($ty:ty) +) => {
                $(
                    _de_prim_cross!(TinyInt SmallInt Int BigInt UTinyInt USmallInt UInt UBigInt, $ty);
                )*
            };
            () => {
                _de_prim_cross!(i8 i16 i32 i64 u8 u16 u32 u64);
            };
        }
        _de_prim_cross!();
    }
}
