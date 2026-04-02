use bigdecimal::ToPrimitive;
use serde::de::{self, DeserializeSeed, IntoDeserializer, Visitor};
use serde::forward_to_deserialize_any;

use super::super::*;
use super::*;

impl<'b, 'de: 'b> serde::de::EnumAccess<'de> for BorrowedValue<'b> {
    type Error = Error;

    type Variant = UnitOnly;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let d = self.strict_as_str().into_deserializer();
        seed.deserialize(d).map(|v| (v, UnitOnly))
    }
}

#[derive(Debug, Clone)]
struct EnumTimestampDeserializer<'b> {
    value: BorrowedValue<'b>,
}

#[derive(Debug, Clone)]
struct EnumValueDeserializer<'b> {
    value: BorrowedValue<'b>,
}

impl<'de, 'b: 'de> serde::de::EnumAccess<'de> for EnumValueDeserializer<'b> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(self.value.ty().as_variant_str().into_deserializer())
            .map(|v| (v, self))
    }
}

impl<'de, 'b: 'de> de::VariantAccess<'de> for EnumValueDeserializer<'b> {
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

impl<'de> serde::de::EnumAccess<'de> for EnumTimestampDeserializer<'_> {
    type Error = Error;

    type Variant = VariantTimestampDeserializer;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        match &self.value {
            BorrowedValue::Timestamp(Timestamp::Microseconds(v)) => Ok((
                seed.deserialize("Microseconds".into_deserializer())?,
                VariantTimestampDeserializer { value: *v },
            )),
            BorrowedValue::Timestamp(Timestamp::Milliseconds(v)) => Ok((
                seed.deserialize("Milliseconds".into_deserializer())?,
                VariantTimestampDeserializer { value: *v },
            )),
            BorrowedValue::Timestamp(Timestamp::Nanoseconds(v)) => Ok((
                seed.deserialize("Nanoseconds".into_deserializer())?,
                VariantTimestampDeserializer { value: *v },
            )),
            _ => todo!(),
        }
    }
}

macro_rules! value_forward_to_deserialize_any {
    ($($ty: ty)*) => {
        $(paste::paste! {
            fn [<deserialize_$ty>]<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                match self {
                    BorrowedValue::Decimal(v)  => {
                        visitor.[<visit_$ty>](v.as_bigdecimal().[<to_$ty>]().ok_or(de::Error::custom(format!("decimal cannot cast to target type: {}", stringify!($ty))))?)
                    }
                    BorrowedValue::Decimal64(v) => {
                        visitor.[<visit_$ty>](v.as_bigdecimal().[<to_$ty>]().ok_or(de::Error::custom(format!("decimal cannot cast to target type: {}", stringify!($ty))))?)
                    }
                    v => v.deserialize_any(visitor),
                }
            }
        })*
    };
}

impl<'de> serde::de::Deserializer<'de> for BorrowedValue<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        use BorrowedValue::*;
        match self {
            Null(_) => visitor.visit_none(),
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
            VarChar(v) => visitor.visit_borrowed_str(v),
            NChar(v) => match v {
                Cow::Borrowed(v) => visitor.visit_borrowed_str(v),
                Cow::Owned(v) => visitor.visit_string(v),
            },
            Json(v) => match v {
                Cow::Borrowed(v) => serde_json::Deserializer::from_slice(v)
                    .deserialize_any(visitor)
                    .map_err(<Self::Error as de::Error>::custom),
                Cow::Owned(v) => serde_json::from_slice::<serde_json::Value>(&v)
                    .map_err(<Self::Error as de::Error>::custom)?
                    .into_deserializer()
                    .deserialize_any(visitor)
                    .map_err(<Self::Error as de::Error>::custom),
            },
            Timestamp(v) => visitor.visit_i64(v.as_raw_i64()),
            VarBinary(v) | Geometry(v) | Blob(v) | MediumBlob(v) => match v {
                Cow::Borrowed(v) => visitor.visit_bytes(v),
                Cow::Owned(v) => visitor.visit_bytes(v.as_slice()),
            },
            Decimal(v) => visitor.visit_string(v.to_string()),
            Decimal64(v) => visitor.visit_string(v.to_string()),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            BorrowedValue::Decimal(v) => visitor.visit_bool(!v.as_bigdecimal().is_zero()),
            BorrowedValue::Decimal64(v) => visitor.visit_bool(!v.as_bigdecimal().is_zero()),
            v => v.deserialize_any(visitor),
        }
    }

    value_forward_to_deserialize_any! {i8 u8 i16 u16 i32 u32 i64 u64 f32 f64 i128 u128}

    forward_to_deserialize_any! {
        char
        bytes byte_buf
        tuple identifier
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        use BorrowedValue::*;
        match self {
            Null(_) => visitor.visit_borrowed_str(""), // todo: empty string or error?
            Bool(v) => visitor.visit_string(format!("{v}")),
            TinyInt(v) => visitor.visit_string(format!("{v}")),
            SmallInt(v) => visitor.visit_string(format!("{v}")),
            Int(v) => visitor.visit_string(format!("{v}")),
            BigInt(v) => visitor.visit_string(format!("{v}")),
            UTinyInt(v) => visitor.visit_string(format!("{v}")),
            USmallInt(v) => visitor.visit_string(format!("{v}")),
            UInt(v) => visitor.visit_string(format!("{v}")),
            UBigInt(v) => visitor.visit_string(format!("{v}")),
            Float(v) => visitor.visit_string(format!("{v}")),
            Double(v) => visitor.visit_string(format!("{v}")),
            Json(v) => match v {
                Cow::Borrowed(v) => std::str::from_utf8(v)
                    .map_err(<Self::Error as serde::de::Error>::custom)
                    .and_then(|s| visitor.visit_borrowed_str(s)),
                Cow::Owned(v) => String::from_utf8(v)
                    .map_err(<Self::Error as serde::de::Error>::custom)
                    .and_then(|s| visitor.visit_string(s)),
            },
            VarChar(v) => visitor.visit_borrowed_str(v),
            NChar(v) => match v {
                Cow::Borrowed(v) => visitor.visit_borrowed_str(v),
                Cow::Owned(v) => visitor.visit_str(&v),
            },
            Timestamp(v) => visitor.visit_string(v.to_datetime_with_tz().to_rfc3339()),
            Decimal(v) => visitor.visit_string(v.as_bigdecimal().to_string()),
            Decimal64(v) => visitor.visit_string(v.as_bigdecimal().to_string()),
            _ => Err(<Self::Error as de::Error>::custom(
                "unsupported type to deserialize",
            )),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if self.is_null() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        use BorrowedValue::*;
        macro_rules! _v_ {
            ($v:expr) => {
                visitor.visit_newtype_struct($v.into_deserializer())
            };
        }
        match self {
            Null(_) => visitor.visit_none(),
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
            VarChar(v) => _v_!(v),
            NChar(v) => _v_!(v),
            Json(v) => match v {
                Cow::Borrowed(v) => serde_json::Deserializer::from_slice(v)
                    .deserialize_newtype_struct(name, visitor)
                    .map_err(<Self::Error as de::Error>::custom),
                Cow::Owned(v) => serde_json::from_slice::<serde_json::Value>(&v)
                    .map_err(<Self::Error as de::Error>::custom)?
                    .into_deserializer()
                    .deserialize_newtype_struct(name, visitor)
                    .map_err(<Self::Error as de::Error>::custom),
            },
            Timestamp(v) => visitor.visit_i64(v.as_raw_i64()),
            VarBinary(v) | Geometry(v) | Blob(v) | MediumBlob(v) => match v {
                Cow::Borrowed(v) => visitor.visit_borrowed_bytes(v),
                Cow::Owned(v) => visitor.visit_bytes(v.as_slice()),
            },
            v @ (Decimal(_) | Decimal64(_)) => visitor.visit_newtype_struct(v),
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        use BorrowedValue::*;
        match self {
            Null(_) => Vec::<u8>::new()
                .into_deserializer()
                .deserialize_seq(visitor),
            Json(v) => v.to_vec().into_deserializer().deserialize_seq(visitor),
            VarChar(v) => v
                .as_bytes()
                .to_vec()
                .into_deserializer()
                .deserialize_seq(visitor),
            NChar(v) => v
                .as_bytes()
                .to_vec()
                .into_deserializer()
                .deserialize_seq(visitor),
            Blob(v) | VarBinary(v) | Geometry(v) => {
                let bytes = match v {
                    std::borrow::Cow::Borrowed(b) => b.to_vec(),
                    std::borrow::Cow::Owned(b) => b,
                };
                bytes.into_deserializer().deserialize_seq(visitor)
            }
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
        if name == "Timestamp" && variants == TIMESTAMP_VARIANTS {
            return visitor.visit_enum(EnumTimestampDeserializer { value: self });
        }
        if name == "Value" && variants == VALUE_VARIANTS {
            return visitor.visit_enum(EnumValueDeserializer { value: self });
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
            BorrowedValue::Json(v) => match v {
                Cow::Borrowed(v) => serde_json::Deserializer::from_slice(v)
                    .deserialize_struct(name, fields, visitor)
                    .map_err(<Self::Error as serde::de::Error>::custom),
                Cow::Owned(v) => serde_json::from_slice::<serde_json::Value>(&v)
                    .map_err(<Self::Error as de::Error>::custom)?
                    .into_deserializer()
                    .deserialize_struct(name, fields, visitor)
                    .map_err(<Self::Error as de::Error>::custom),
            },
            _ => self.deserialize_any(visitor),
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            BorrowedValue::Json(v) => match v {
                Cow::Borrowed(v) => serde_json::Deserializer::from_slice(v)
                    .deserialize_tuple_struct(name, len, visitor)
                    .map_err(<Self::Error as serde::de::Error>::custom),
                Cow::Owned(v) => serde_json::from_slice::<serde_json::Value>(&v)
                    .map_err(<Self::Error as de::Error>::custom)?
                    .into_deserializer()
                    .deserialize_tuple_struct(name, len, visitor)
                    .map_err(<Self::Error as de::Error>::custom),
            },
            _ => self.deserialize_any(visitor),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }
}

impl<'de> serde::de::IntoDeserializer<'de, Error> for BorrowedValue<'de> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn de_value_as_inner() {
        use BorrowedValue::*;

        macro_rules! _de_value {
            ($($v:expr, $ty:ty, $tv:expr) *) => {
                $(
                    {
                        let d = <$ty>::deserialize($v.into_deserializer()).expect("");
                        assert_eq!(d, $tv);
                    }
                )*
            }
        }

        _de_value!(
            Null(Ty::UTinyInt), Option<u8>, None
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
            VarChar(""), String, "".to_string()
            NChar("".into()), String, "".to_string()
            Timestamp(crate::Timestamp::Milliseconds(1)), crate::Timestamp, crate::Timestamp::Milliseconds(1)
        );
    }

    #[test]
    fn de_borrowed_str() {
        use serde_json::json;
        use BorrowedValue::*;

        macro_rules! _de_str {
            ($v:expr, is_err) => {
                assert!(<&str>::deserialize(($v).into_deserializer()).is_err());
            };
            ($v:expr, $tv:expr) => {
                assert_eq!(<&str>::deserialize(($v).into_deserializer()).expect("str"), $tv);
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
            Json(json!({ "name": "abc"}).to_string().into_bytes().into()), is_err
            Json(json!(1).to_string().into_bytes().into()), is_err
            Json(json!(null).to_string().into_bytes().into()), is_err
            Json(json!("abc").to_string().into_bytes().into()), is_err
            Timestamp(crate::Timestamp::Milliseconds(0)), is_err
            ;
            Null(Ty::VarChar), ""
            VarChar("String"), "String"
            VarChar("你好，世界"), "你好，世界"
        };
    }

    #[test]
    fn de_string() {
        use serde_json::json;
        use BorrowedValue::*;

        macro_rules! _de_str {
            ($v:expr, is_err) => {
                assert!(String::deserialize(($v).into_deserializer()).is_err());
            };
            ($v:expr, $tv:expr) => {
                assert_eq!(String::deserialize(($v).into_deserializer()).expect("str"), $tv);
            };
            ($($v:expr, $tv:expr) *) => {
                $(_de_str!($v, $tv);)*
            };
            ($($v2:expr, is_err) *; $($v:expr, $tv:expr) * ) => {
                $(
                    _de_str!($v2, is_err);
                )*
                $(
                    _de_str!($v, $tv);
                )*
            }
        }
        _de_str! {
            Null(Ty::VarChar), ""
            TinyInt(-1), "-1"
            Timestamp(crate::Timestamp::Milliseconds(0)), "1970-01-01T08:00:00+08:00"
            VarChar("String"), "String"
            VarChar("你好，世界"), "你好，世界"
            Json(json!("abc").to_string().into_bytes().into()), json!("abc").to_string()
            Json(json!({ "name": "abc"}).to_string().into_bytes().into()), json!({ "name": "abc"}).to_string()
            Json(json!(1).to_string().into_bytes().into()), json!(1).to_string()
            Json(json!(null).to_string().into_bytes().into()), json!(null).to_string()
        };
    }

    #[test]
    fn de_json() {
        use serde_json::json;
        use BorrowedValue::*;

        macro_rules! _de_json {
            ($v:expr, $ty:ty, $tv:expr) => {{
                let d = <$ty>::deserialize(
                    Json($v.to_string().into_bytes().into()).into_deserializer(),
                )
                .expect("de json");
                assert_eq!(d, $tv);
            }};
        }

        _de_json!(json!("string"), String, json!("string").to_string());

        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct Obj {
            name: String,
        }
        _de_json!(
            json!({ "name": "string" }),
            Obj,
            Obj {
                name: "string".to_string()
            }
        );

        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct JsonStr(String);
        _de_json!(json!("string"), JsonStr, JsonStr("string".to_string()));
    }

    #[test]
    fn de_newtype_struct() {
        use serde_json::json;
        use BorrowedValue::*;
        macro_rules! _de_ty {
            ($v:expr, $ty:ty, $tv:expr) => {{
                let d = <$ty>::deserialize($v.into_deserializer()).expect("de type");
                assert_eq!(d, $tv);
            }};
        }
        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct JsonStr(String);
        _de_ty!(
            Json(json!("string").to_string().into_bytes().into()),
            JsonStr,
            JsonStr("string".to_string())
        );

        #[derive(Debug, PartialEq, Eq, Deserialize)]
        struct Primi<T>(T);
        _de_ty!(
            Json(json!(1).to_string().into_bytes().into()),
            Primi<i32>,
            Primi(1)
        );
        _de_ty!(TinyInt(1), Primi<i8>, Primi(1));
        _de_ty!(SmallInt(1), Primi<i64>, Primi(1));
        _de_ty!(Int(1), Primi<i64>, Primi(1));
        _de_ty!(BigInt(1), Primi<i64>, Primi(1));

        macro_rules! _de_prim {
            ($v:ident, $ty:ty, $inner:literal) => {
                println!(
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
