use std::{fmt, str::FromStr};

use num_enum::TryFromPrimitive;
use serde::Deserialize;

/// The precision of a timestamp or a database.
#[repr(i32)]
#[derive(Debug, Copy, Clone, TryFromPrimitive, PartialEq, Eq, serde_repr::Serialize_repr)]
pub enum Precision {
    #[num_enum(default)]
    Millisecond = 0,
    Microsecond,
    Nanosecond,
}

impl FromStr for Precision {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ms" => Ok(Precision::Millisecond),
            "us" => Ok(Precision::Microsecond),
            "ns" => Ok(Precision::Nanosecond),
            s => Err(format!("unknown precision string: {}", s)),
        }
    }
}

impl<'de> Deserialize<'de> for Precision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PrecisionVisitor;

        impl<'de> serde::de::Visitor<'de> for PrecisionVisitor {
            type Value = Precision;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("expect integer 0/1/2 or string ms/us/ns")
            }

            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_i32(v as _)
            }
            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_i32(v as _)
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Precision::try_from(v).map_err(<E as serde::de::Error>::custom)
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_i32(v as _)
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_i32(v as _)
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_i32(v as _)
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_i32(v as _)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_i32(v as _)
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Precision::from_str(v).map_err(<E as serde::de::Error>::custom)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Precision::Millisecond)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_any(self)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Precision::Millisecond)
            }
        }

        deserializer.deserialize_any(PrecisionVisitor)
    }
}

#[cfg(test)]
mod tests {
    use serde::{
        de::{
            value::{I32Deserializer, StrDeserializer, UnitDeserializer},
            IntoDeserializer,
        },
        Deserialize, Serialize,
    };

    use super::Precision;

    #[test]
    fn de() {
        type SD<'a> = StrDeserializer<'a, serde::de::value::Error>;
        let precision = Precision::deserialize::<SD>("ms".into_deserializer()).unwrap();
        assert_eq!(precision, Precision::Millisecond);
        let precision = Precision::deserialize::<SD>("us".into_deserializer()).unwrap();
        assert_eq!(precision, Precision::Microsecond);
        let precision = Precision::deserialize::<SD>("ns".into_deserializer()).unwrap();
        assert_eq!(precision, Precision::Nanosecond);

        type I32D = I32Deserializer<serde::de::value::Error>;
        let precision = Precision::deserialize::<I32D>(0.into_deserializer()).unwrap();
        assert_eq!(precision, Precision::Millisecond);
        let precision = Precision::deserialize::<I32D>(1.into_deserializer()).unwrap();
        assert_eq!(precision, Precision::Microsecond);
        let precision = Precision::deserialize::<I32D>(2.into_deserializer()).unwrap();
        assert_eq!(precision, Precision::Nanosecond);

        type UnitD = UnitDeserializer<serde::de::value::Error>;
        let precision = Precision::deserialize::<UnitD>(().into_deserializer()).unwrap();
        assert_eq!(precision, Precision::Millisecond);

        let json = serde_json::to_string(&precision).unwrap();
        assert_eq!(json, "0");
    }
}
