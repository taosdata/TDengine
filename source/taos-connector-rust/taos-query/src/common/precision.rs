use std::fmt::{self, Display};
use std::str::FromStr;

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum PrecisionError {
    #[error("invalid precision repr: {0}")]
    Invalid(String),
}

/// The precision of a timestamp or a database.
#[repr(i32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, serde_repr::Serialize_repr)]
pub enum Precision {
    Millisecond = 0,
    Microsecond,
    Nanosecond,
}

impl Default for Precision {
    fn default() -> Self {
        Self::Millisecond
    }
}

impl PartialEq<str> for Precision {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Precision {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Precision {
    pub const fn as_str(&self) -> &'static str {
        use Precision::*;
        match self {
            Millisecond => "ms",
            Microsecond => "us",
            Nanosecond => "ns",
        }
    }

    pub const fn as_u8(&self) -> u8 {
        match self {
            Self::Millisecond => 0,
            Self::Microsecond => 1,
            Self::Nanosecond => 2,
        }
    }
    pub const fn from_u8(precision: u8) -> Self {
        match precision {
            0 => Self::Millisecond,
            1 => Self::Microsecond,
            2 => Self::Nanosecond,
            _ => panic!("precision integer only allow 0/1/2"),
        }
    }

    pub const fn to_seconds_format(self) -> chrono::SecondsFormat {
        match self {
            Precision::Millisecond => chrono::SecondsFormat::Millis,
            Precision::Microsecond => chrono::SecondsFormat::Micros,
            Precision::Nanosecond => chrono::SecondsFormat::Nanos,
        }
    }
}

macro_rules! _impl_from {
    ($($ty:ty) *) => {
        $(impl From<$ty> for Precision {
            fn from(v: $ty) -> Self {
                Self::from_u8(v as _)
            }
        })*
    }
}

_impl_from!(i8 i16 i32 i64 isize u8 u16 u32 u64 usize);

impl Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for Precision {
    type Err = PrecisionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ms" => Ok(Precision::Millisecond),
            "us" => Ok(Precision::Microsecond),
            "ns" => Ok(Precision::Nanosecond),
            s => Err(PrecisionError::Invalid(s.to_string())),
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
                Ok(v.into())
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
    use serde::de::value::{I32Deserializer, StrDeserializer, UnitDeserializer};
    use serde::de::IntoDeserializer;
    use serde::Deserialize;

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

    #[test]
    fn ord() {
        assert!(Precision::Millisecond <= Precision::Millisecond);
        assert!(Precision::Millisecond < Precision::Microsecond);
        assert!(Precision::Microsecond < Precision::Nanosecond);
    }
}
