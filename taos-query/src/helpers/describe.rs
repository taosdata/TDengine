use std::{
    fmt,
    ops::{Deref, DerefMut},
    str::FromStr,
};

use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};

use crate::common::Ty;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Described {
    pub field: String,
    #[serde(rename = "type")]
    pub ty: Ty,
    pub length: usize,
}

impl Described {
    /// Represent the data type in sql.
    ///
    /// For example: "INT", "VARCHAR(100)".
    pub fn sql_repr(&self) -> String {
        let ty = self.ty;
        if ty.is_var_type() {
            format!("{} {}({})", self.field, ty, self.length)
        } else {
            format!("{} {}", self.field, self.ty)
        }
    }
}
#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
#[serde(tag = "note")]
pub enum ColumnMeta {
    Column(Described),
    Tag(Described),
}

impl Deref for ColumnMeta {
    type Target = Described;

    fn deref(&self) -> &Self::Target {
        match self {
            ColumnMeta::Column(v) => v,
            ColumnMeta::Tag(v) => v,
        }
    }
}

impl DerefMut for ColumnMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ColumnMeta::Column(v) => v,
            ColumnMeta::Tag(v) => v,
        }
    }
}
unsafe impl Send for ColumnMeta {}
impl<'de> Deserialize<'de> for ColumnMeta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Meta {
            Field,
            Type,
            Length,
            Note,
        }

        impl<'de> Deserialize<'de> for Meta {
            fn deserialize<D>(deserializer: D) -> Result<Meta, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Meta;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`field`, `type`, `length` or `note`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Meta, E>
                    where
                        E: de::Error,
                    {
                        match value.to_lowercase().as_str() {
                            "field" => Ok(Meta::Field),
                            "type" => Ok(Meta::Type),
                            "length" => Ok(Meta::Length),
                            "note" => Ok(Meta::Note),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct MetaVisitor;

        impl<'de> Visitor<'de> for MetaVisitor {
            type Value = ColumnMeta;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ColumnMeta")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let field = dbg!(seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?);
                let ty = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))
                    .and_then(|s| Ty::from_str(s).map_err(de::Error::custom))?;
                let length = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let note: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let desc = Described { field, ty, length };
                if note.is_empty() {
                    Ok(ColumnMeta::Column(desc))
                } else {
                    Ok(ColumnMeta::Tag(desc))
                }
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut field = None;
                let mut ty = None;
                let mut length = None;
                let mut note = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Meta::Field => {
                            if field.is_some() {
                                return Err(de::Error::duplicate_field("field"));
                            }
                            field = Some(map.next_value()?);
                        }
                        Meta::Type => {
                            if ty.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            let t: Ty = map.next_value()?;
                            ty = Some(t);
                        }
                        Meta::Length => {
                            if length.is_some() {
                                return Err(de::Error::duplicate_field("length"));
                            }
                            length = Some(map.next_value()?);
                        }
                        Meta::Note => {
                            if note.is_some() {
                                return Err(de::Error::duplicate_field("note"));
                            }
                            let t: String = map.next_value()?;
                            note = Some(t.is_empty() || t == "Column")
                        }
                    }
                }
                let field = field.ok_or_else(|| de::Error::missing_field("field"))?;
                let ty = ty.ok_or_else(|| de::Error::missing_field("type"))?;
                let length = length.ok_or_else(|| de::Error::missing_field("length"))?;
                let desc = Described { field, ty, length };
                let note = note.ok_or_else(|| de::Error::missing_field("note"))?;
                if note {
                    Ok(ColumnMeta::Column(desc))
                } else {
                    Ok(ColumnMeta::Tag(desc))
                }
            }
        }

        const FIELDS: &[&str] = &["field", "type", "length", "note"];
        deserializer.deserialize_struct("ColumnMeta", FIELDS, MetaVisitor)
    }
}
impl ColumnMeta {
    pub fn field(&self) -> &str {
        match self {
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.field.as_str(),
        }
    }
    pub fn ty(&self) -> Ty {
        match self {
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.ty,
        }
    }
    pub fn length(&self) -> usize {
        match self {
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.length,
        }
    }
    pub fn note(&self) -> &str {
        match self {
            ColumnMeta::Tag(_) => "TAG",
            _ => "",
        }
    }
    pub fn is_tag(&self) -> bool {
        matches!(self, ColumnMeta::Tag(_))
    }
}

#[test]
fn serde_meta() {
    let meta = ColumnMeta::Column(Described {
        field: "name".to_string(),
        ty: Ty::BigInt,
        length: 8,
    });

    let a = serde_json::to_string(&meta).unwrap();

    let d: ColumnMeta = serde_json::from_str(&a).unwrap();

    assert_eq!(meta, d);
}
//erive(Debug, Clone, Deserialize)]
// pub struct ColumnMeta {
//     pub name: String,
//     pub type_: Ty,
//     pub bytes: i16,
// }
// #[derive(Debug)]
// pub struct TaosQueryData {
//     pub column_meta: Vec<ColumnMeta>,
//     pub rows: Vec<Vec<Field>>,
// }

// #[derive(Debug)]
// pub struct TaosDescribe {
//     pub cols: Vec<ColumnMeta>,
//     pub tags: Vec<ColumnMeta>,
// }

// impl TaosDescribe {
//     pub fn names(&self) -> Vec<&String> {
//         self.cols
//             .iter()
//             .chain(self.tags.iter())
//             .map(|t| &t.name)
//             .collect_vec()
//     }

//     pub fn col_names(&self) -> Vec<&String> {
//         self.cols.iter().map(|t| &t.name).collect_vec()
//     }
//     pub fn tag_names(&self) -> Vec<&String> {
//         self.tags.iter().map(|t| &t.name).collect_vec()
//     }
// }
// impl FromStr for Ty {
//     type Err = &'static str;
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s.to_lowercase().as_str() {
//             "timestamp" => Ok(Ty::Timestamp),
//             "bool" => Ok(Ty::Bool),
//             "tinyint" => Ok(Ty::TinyInt),
//             "smallint" => Ok(Ty::SmallInt),
//             "int" => Ok(Ty::Int),
//             "bigint" => Ok(Ty::BigInt),
//             "tinyint unsigned" => Ok(Ty::UTinyInt),
//             "smallint unsigned" => Ok(Ty::USmallInt),
//             "int unsigned" => Ok(Ty::UInt),
//             "bigint unsigned" => Ok(Ty::UBigInt),
//             "float" => Ok(Ty::Float),
//             "double" => Ok(Ty::Double),
//             "binary" => Ok(Ty::Binary),
//             "nchar" => Ok(Ty::NChar),
//             _ => Err("not a valid data type string"),
//         }
//     }
// }
// impl From<TaosQueryData> for TaosDescribe {
//     fn from(rhs: TaosQueryData) -> Self {
//         let (cols, tags): (Vec<_>, Vec<_>) = rhs
//             .rows
//             .iter()
//             .partition(|row| row[3] != Field::Binary("TAG".into()));
//         Self {
//             cols: cols
//                 .into_iter()
//                 .map(|row| ColumnMeta {
//                     name: row[0].to_string(),
//                     type_: Ty::from_str(&row[1].to_string()).expect("from describe"),
//                     bytes: *row[2].as_int().unwrap() as _,
//                 })
//                 .collect_vec(),
//             tags: tags
//                 .into_iter()
//                 .map(|row| ColumnMeta {
//                     name: row[0].to_string(),
//                     type_: Ty::from_str(&row[1].to_string()).expect("from describe"),
//                     bytes: *row[2].as_int().unwrap() as _,
//                 })
//                 .collect_vec(),
//         }
//     }
// }
// impl TaosQueryData {
//     /// Total rows count of query result
//     pub fn rows(&self) -> usize {
//         self.rows.len()
//     }
// }

// #[derive(Debug, PartialEq, Clone)]
// pub enum Field {
//     Null,        // 0
//     Bool(bool),  // 1
//     TinyInt(i8), // 2
//     SmallInt(i16),
//     Int(i32),
//     BigInt(i64),
//     Float(f32),
//     Double(f64),
//     Binary(BString),
//     Timestamp(Timestamp),
//     NChar(String),
//     UTinyInt(u8),
//     USmallInt(u16),
//     UInt(u32),
//     UBigInt(u64), // 14
//     Json(serde_json::Value),
// }

// impl fmt::Display for Field {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match self {
//             Field::Null => write!(f, "NULL"),
//             Field::Bool(v) => write!(f, "{}", v),
//             Field::TinyInt(v) => write!(f, "{}", v),
//             Field::SmallInt(v) => write!(f, "{}", v),
//             Field::Int(v) => write!(f, "{}", v),
//             Field::BigInt(v) => write!(f, "{}", v),
//             Field::Float(v) => write!(f, "{}", v),
//             Field::Double(v) => write!(f, "{}", v),
//             Field::Binary(v) => write!(f, "{}", v),
//             Field::NChar(v) => write!(f, "{}", v),
//             Field::Timestamp(v) => write!(f, "{}", v),
//             Field::UTinyInt(v) => write!(f, "{}", v),
//             Field::USmallInt(v) => write!(f, "{}", v),
//             Field::UInt(v) => write!(f, "{}", v),
//             Field::UBigInt(v) => write!(f, "{}", v),
//             Field::Json(v) => write!(f, "{}", v),
//         }
//     }
// }

// impl Field {
//     pub fn as_bool(&self) -> Option<&bool> {
//         match self {
//             Field::Bool(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_tiny_int(&self) -> Option<&i8> {
//         match self {
//             Field::TinyInt(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_small_int(&self) -> Option<&i16> {
//         match self {
//             Field::SmallInt(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_int(&self) -> Option<&i32> {
//         match self {
//             Field::Int(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_big_int(&self) -> Option<&i64> {
//         match self {
//             Field::BigInt(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_float(&self) -> Option<&f32> {
//         match self {
//             Field::Float(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_double(&self) -> Option<&f64> {
//         match self {
//             Field::Double(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_binary(&self) -> Option<&BStr> {
//         match self {
//             Field::Binary(v) => Some(v.as_ref()),
//             _ => None,
//         }
//     }
//     pub fn as_nchar(&self) -> Option<&str> {
//         match self {
//             Field::NChar(v) => Some(v),
//             _ => None,
//         }
//     }

//     /// BINARY or NCHAR typed string reference
//     pub fn as_string(&self) -> Option<String> {
//         match self {
//             Field::Binary(v) => Some(v.to_string()),
//             Field::NChar(v) => Some(v.to_string()),
//             _ => None,
//         }
//     }
//     pub fn as_timestamp(&self) -> Option<&Timestamp> {
//         match self {
//             Field::Timestamp(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_raw_timestamp(&self) -> Option<i64> {
//         match self {
//             Field::Timestamp(v) => Some(v.as_raw_timestamp()),
//             _ => None,
//         }
//     }
//     pub fn as_unsigned_tiny_int(&self) -> Option<&u8> {
//         match self {
//             Field::UTinyInt(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_unsigned_samll_int(&self) -> Option<&u16> {
//         match self {
//             Field::USmallInt(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_unsigned_int(&self) -> Option<&u32> {
//         match self {
//             Field::UInt(v) => Some(v),
//             _ => None,
//         }
//     }
//     pub fn as_unsigned_big_int(&self) -> Option<&u64> {
//         match self {
//             Field::UBigInt(v) => Some(v),
//             _ => None,
//         }
//     }

//     pub fn as_json(&self) -> Option<&serde_json::Value> {
//         match self {
//             Field::Json(v) => Some(v),
//             _ => None,
//         }
//     }

//     pub fn data_type(&self) -> Ty {
//         match self {
//             Field::Null => Ty::Null,
//             Field::Bool(_v) => Ty::Bool,
//             Field::TinyInt(_v) => Ty::TinyInt,
//             Field::SmallInt(_v) => Ty::SmallInt,
//             Field::Int(_v) => Ty::Int,
//             Field::BigInt(_v) => Ty::BigInt,
//             Field::Float(_v) => Ty::Float,
//             Field::Double(_v) => Ty::Double,
//             Field::Binary(_v) => Ty::Binary,
//             Field::NChar(_v) => Ty::NChar,
//             Field::Timestamp(_v) => Ty::Timestamp,
//             Field::UTinyInt(_v) => Ty::UTinyInt,
//             Field::USmallInt(_v) => Ty::USmallInt,
//             Field::UInt(_v) => Ty::UInt,
//             Field::UBigInt(_v) => Ty::UBigInt,
//             Field::Json(_v) => Ty::Json,
//         }
//     }
// }

// pub trait IntoField {
//     fn into_field(self) -> Field;
// }

// macro_rules! _impl_primitive_type {
//     ($ty:ty, $target:ident, $v:expr) => {
//         impl IntoField for $ty {
//             fn into_field(self) -> Field {
//                 Field::$target(self)
//             }
//         }
//         paste! {
//             #[test]
//             fn [<test_ $ty:snake>]() {
//                 let v: $ty = $v;
//                 assert_eq!(v.clone().into_field(), Field::$target(v));
//             }
//         }
//     };
// }

// _impl_primitive_type!(bool, Bool, true);
// _impl_primitive_type!(i8, TinyInt, 0);
// _impl_primitive_type!(i16, SmallInt, 0);
// _impl_primitive_type!(i32, Int, 0);
// _impl_primitive_type!(i64, BigInt, 0);
// _impl_primitive_type!(u8, UTinyInt, 0);
// _impl_primitive_type!(u16, USmallInt, 0);
// _impl_primitive_type!(u32, UInt, 0);
// _impl_primitive_type!(u64, UBigInt, 0);
// _impl_primitive_type!(f32, Float, 0.);
// _impl_primitive_type!(f64, Double, 0.);
// _impl_primitive_type!(BString, Binary, "A".into());
// _impl_primitive_type!(String, NChar, "A".into());
// // _impl_primitive_type!(serde_json::Value, Json, );

// impl IntoField for &BStr {
//     fn into_field(self) -> Field {
//         self.to_owned().into_field()
//     }
// }
// impl IntoField for &str {
//     fn into_field(self) -> Field {
//         self.to_owned().into_field()
//     }
// }

// #[cfg(test)]
// mod test {
//     use crate::test::taos;
//     use crate::*;

//     #[tokio::test]
//     #[proc_test_catalog::test_catalogue]
//     /// Test describe sql
//     async fn test_describe() -> Result<(), Error> {
//         let db = stdext::function_name!()
//             .replace("::{{closure}}", "")
//             .replace("::", "_");
//         println!("{}", db);
//         let taos = taos()?;
//         let desc = taos.describe("log.dn").await?;
//         assert_eq!(desc.cols.len(), 15);
//         assert_eq!(desc.tags.len(), 2);
//         Ok(())
//     }
// }
