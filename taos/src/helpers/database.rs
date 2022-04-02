use chrono::NaiveDateTime;
use serde::{de, Deserialize, Serialize};

use std::{collections::HashMap, fmt};

/// A show database representation struct.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub created_time: NaiveDateTime,
    pub ntables: usize,
    pub precision: Precision,
    #[serde(flatten)]
    pub props: HashMap<String, PropValue>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precision {
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(rename = "us")]
    Microseconds,
    #[serde(rename = "ns")]
    Nanoseconds,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum PropValue {
    None,
    Int(u64),
    String(String),
}

impl<'de> Deserialize<'de> for PropValue {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        macro_rules! _into_value {
            ($($ty:ident)*) => {
                paste::paste!{$(
                    fn [<visit_ $ty>]<E>(self, v: $ty) -> Result<Self::Value, E>
                    where
                        E: de::Error,
                    {
                        Ok(PropValue::Int(v as _))
                    }
                )*}
            }
        }

        impl<'de> de::Visitor<'de> for ValueVisitor {
            type Value = PropValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("any valid integer or string")
            }

            _into_value!(bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64);

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_borrowed_str(v)
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value::String(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value::String(v))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&String::from_utf8_lossy(v))
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_bytes(v)
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_bytes(&v)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value::None)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Self::Value::None)
            }
            fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                // deserializer.deserialize_any(self)
                err!("can't deserialize from newtype struct")
            }
        }
        log::trace!("deserialize prop value");
        deserializer.deserialize_any(ValueVisitor)
    }
}

#[test]
fn serde() {
    let db = Database {
        name: "abc".into(),
        created_time: chrono::Local::now().naive_local(),
        ntables: 100,
        precision: Precision::Microseconds,
        props: vec![
            ("1".into(), PropValue::None),
            ("2".into(), PropValue::Int(100)),
            ("3".into(), PropValue::String("value".into())),
        ]
        .into_iter()
        .collect(),
    };

    let s = serde_json::to_string(&db).expect("");

    let db2: Database = serde_json::from_str(dbg!(&s)).unwrap();
    dbg!(&db2);
    assert_eq!(db, db2);
}
