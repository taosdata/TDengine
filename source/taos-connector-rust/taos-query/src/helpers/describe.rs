use std::fmt;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

use crate::common::views::DataType;
use crate::common::Ty;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

/// Compress options for column, supported since TDengine 3.3.0.0 .
///
/// The `encode` field is the encoding method for the column, it can be one of the following values:
/// - `disabled`
/// - `delta-i`
/// - `delta-d`
/// - `simple8b`
///
/// The `compress` field is the compression method for the column, it can be one of the following values:
/// - `none`
/// - `lz4`
/// - `gzip`
/// - `zstd`
///
/// The `level` field is the compression level for the column, it can be one of the following values:
/// - `low`
/// - `medium`
/// - `high`
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct CompressOptions {
    pub encode: String,
    pub compress: String,
    pub level: String,
}

macro_rules! disabled_or_empty {
    ($field:expr) => {
        $field == "disabled" || $field.is_empty()
    };
    () => {};
}

impl CompressOptions {
    pub fn new<E, C, L>(encode: E, compress: C, level: L) -> Self
    where
        E: Into<String>,
        C: Into<String>,
        L: Into<String>,
    {
        Self {
            encode: encode.into(),
            compress: compress.into(),
            level: level.into(),
        }
    }

    pub(crate) fn is_disabled(&self) -> bool {
        disabled_or_empty!(self.encode)
            && disabled_or_empty!(self.compress)
            && disabled_or_empty!(self.level)
    }

    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        Self {
            encode: "disabled".to_string(),
            compress: "disabled".to_string(),
            level: "disabled".to_string(),
        }
    }
}

impl fmt::Display for CompressOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut opts = 0;
        if !disabled_or_empty!(self.encode) {
            write!(f, "ENCODE '{}'", self.encode)?;
            opts += 1;
        }
        if !disabled_or_empty!(self.compress) {
            if opts > 0 {
                write!(f, " ")?;
            }
            write!(f, "COMPRESS '{}'", self.compress)?;
            opts += 1;
        }
        if !disabled_or_empty!(self.level) {
            if opts > 0 {
                write!(f, " ")?;
            }
            write!(f, "LEVEL '{}'", self.level)?;
        }
        Ok(())
    }
}

#[allow(clippy::partial_pub_fields)]
#[derive(Debug, Serialize, Deserialize, Eq, Clone)]
pub struct Described {
    pub field: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(skip)]
    origin_ty: Option<String>,
    pub length: usize,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(flatten, default)]
    pub compression: Option<CompressOptions>,
}

impl PartialEq for Described {
    /// Skip origin_ty field when [PartialEq], but keep it for internal use.
    fn eq(&self, other: &Self) -> bool {
        self.field == other.field
            && self.data_type == other.data_type
            && self.length == other.length
            && self.note == other.note
            && self.compression == other.compression
    }
}

impl Described {
    /// Create a new column description without primary-key/compression feature.
    ///
    /// Decimal should not set as is.
    pub fn new<F, L>(field: F, ty: Ty, length: L) -> Self
    where
        F: Into<String>,
        L: Into<Option<usize>>,
    {
        debug_assert!(!ty.is_decimal());

        let field = field.into();
        let length = length.into();
        let length = length.unwrap_or_else(|| {
            if ty.is_var_type() {
                32
            } else {
                ty.fixed_length()
            }
        });

        Self {
            field,
            data_type: DataType::new(ty, length as _),
            origin_ty: None,
            length,
            note: None,
            compression: None,
        }
    }

    pub fn ty(&self) -> Ty {
        self.data_type.ty()
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Represent the data type in sql.
    ///
    /// For example: "INT", "VARCHAR(100)".
    pub fn sql_repr(&self) -> String {
        self.sql_repr_by_compression(self.compression.as_ref())
    }

    /// Represent the data type in sql without compression options.
    pub fn short_sql_repr(&self) -> String {
        self.sql_repr_by_compression(None)
    }

    fn sql_repr_by_compression(&self, compression: Option<&CompressOptions>) -> String {
        let ty = self.ty();
        let is_var_ty = ty.is_var_type();

        let ty = if matches!(ty, Ty::Decimal | Ty::Decimal64) {
            self.origin_ty.clone().unwrap_or_else(|| ty.to_string())
        } else {
            ty.to_string()
        };

        let base = if is_var_ty {
            format!("`{}` {}({})", self.field, ty, self.length)
        } else {
            format!("`{}` {}", self.field, ty)
        };

        let compress = compression
            .filter(|c| !c.is_disabled())
            .map(|c| format!(" {c}"))
            .unwrap_or_default();

        let pk = if self.is_primary_key() {
            " PRIMARY KEY"
        } else {
            ""
        };

        format!("{base}{compress}{pk}")
    }

    /// Create a new column description with compression feature.
    pub fn with_compression(mut self, compression: CompressOptions) -> Self {
        self.compression = Some(compression);
        self
    }

    pub fn with_origin_ty_name(mut self, ty_name: &str) -> Self {
        self.origin_ty = Some(ty_name.to_string());
        self
    }

    /// Return true if the field is primary key.
    pub fn is_primary_key(&self) -> bool {
        self.note.as_deref() == Some("PRIMARY KEY")
    }

    /// Return true if the field is tag.
    pub fn is_tag(&self) -> bool {
        self.note.as_deref() == Some("TAG")
    }

    pub fn origin_ty_name(&self) -> Option<&str> {
        self.origin_ty.as_deref()
    }
}

#[derive(Debug, Serialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum ColumnMeta {
    Column(Described),
    Tag(Described),
}

unsafe impl Send for ColumnMeta {}

impl Deref for ColumnMeta {
    type Target = Described;

    fn deref(&self) -> &Self::Target {
        match self {
            ColumnMeta::Column(v) | ColumnMeta::Tag(v) => v,
        }
    }
}

impl DerefMut for ColumnMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ColumnMeta::Column(v) | ColumnMeta::Tag(v) => v,
        }
    }
}

#[inline(always)]
fn empty_as_none(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

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
            Encode,
            Compress,
            Level,
        }

        impl<'de> Deserialize<'de> for Meta {
            fn deserialize<D>(deserializer: D) -> Result<Meta, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl Visitor<'_> for FieldVisitor {
                    type Value = Meta;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str(
                            "one of `field`, `type`, `length`, `note`, `encode`, `compress`, `level`",
                        )
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
                            "encode" => Ok(Meta::Encode),
                            "compress" => Ok(Meta::Compress),
                            "level" => Ok(Meta::Level),
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
                let field = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;

                let data_type: DataType = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let origin_ty = Some(data_type.to_string());

                let length = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let note: Option<String> = seq
                    .next_element::<Option<&str>>()?
                    .and_then(|opt| opt)
                    .and_then(empty_as_none);

                let encode: Option<String> = seq
                    .next_element::<Option<&str>>()?
                    .and_then(|opt| opt)
                    .and_then(empty_as_none);
                let compress: Option<String> = seq
                    .next_element::<Option<&str>>()?
                    .and_then(|opt| opt)
                    .and_then(empty_as_none);
                let level: Option<String> = seq
                    .next_element::<Option<&str>>()?
                    .and_then(|opt| opt)
                    .and_then(empty_as_none);

                let compression = if let (Some(encode), Some(compress), Some(level)) =
                    (encode, compress, level)
                {
                    Some(CompressOptions::new(encode, compress, level))
                } else {
                    None
                };

                let desc = Described {
                    field,
                    data_type,
                    origin_ty,
                    length,
                    note,
                    compression,
                };

                if !desc.is_tag() {
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
                let mut origin_ty = None;
                let mut data_type: Option<DataType> = None;
                let mut length = None;
                let mut note = None;
                let mut encode = None;
                let mut compress = None;
                let mut level = None;
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
                            let origin: serde_json::Value = map.next_value()?;
                            let t = match origin {
                                serde_json::Value::Number(n) => {
                                    if let Some(n) = n.as_u64() {
                                        Ty::from(n as u8)
                                    } else {
                                        return Err(de::Error::custom("invalid Ty number"));
                                    }
                                }
                                serde_json::Value::String(s) => {
                                    origin_ty = Some(s.clone());
                                    data_type = Some(s.parse().map_err(de::Error::custom)?);
                                    Ty::from_str(&s).map_err(de::Error::custom)?
                                }
                                _ => return Err(de::Error::custom("invalid Ty")),
                            };
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
                            note = map.next_value::<Option<&str>>()?.and_then(empty_as_none);
                        }
                        Meta::Encode => {
                            if encode.is_some() {
                                return Err(de::Error::duplicate_field("encode"));
                            }
                            encode = map.next_value::<Option<&str>>()?.and_then(empty_as_none);
                        }
                        Meta::Compress => {
                            if compress.is_some() {
                                return Err(de::Error::duplicate_field("compress"));
                            }
                            compress = map.next_value::<Option<&str>>()?.and_then(empty_as_none);
                        }
                        Meta::Level => {
                            if level.is_some() {
                                return Err(de::Error::duplicate_field("level"));
                            }
                            level = map.next_value::<Option<&str>>()?.and_then(empty_as_none);
                        }
                    }
                }
                let mut data_type = data_type.ok_or_else(|| de::Error::missing_field("type"))?;
                let field = field.ok_or_else(|| de::Error::missing_field("field"))?;
                let length = length.ok_or_else(|| de::Error::missing_field("length"))?;
                if data_type.ty().is_decimal() {
                    data_type.set_len(length as u32);
                }
                let compression = if let (Some(encode), Some(compress), Some(level)) =
                    (encode, compress, level)
                {
                    Some(CompressOptions::new(encode, compress, level))
                } else {
                    None
                };
                let desc = Described {
                    field,
                    data_type,
                    origin_ty,
                    length,
                    note,
                    compression,
                };
                if !desc.is_tag() {
                    Ok(ColumnMeta::Column(desc))
                } else {
                    Ok(ColumnMeta::Tag(desc))
                }
            }
        }

        const FIELDS: &[&str] = &[
            "field", "type", "length", "note", "encode", "compress", "level",
        ];

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
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.ty(),
        }
    }

    pub fn origin_ty(&self) -> Option<&str> {
        match self {
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.origin_ty.as_deref(),
        }
    }

    pub fn length(&self) -> usize {
        match self {
            ColumnMeta::Column(desc) | ColumnMeta::Tag(desc) => desc.length,
        }
    }

    pub fn note(&self) -> &str {
        match self {
            ColumnMeta::Column(_) => "",
            ColumnMeta::Tag(_) => "TAG",
        }
    }

    pub fn is_tag(&self) -> bool {
        matches!(self, ColumnMeta::Tag(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_expr() {
        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::from_ty(Ty::Timestamp),
            origin_ty: None,
            length: 0,
            note: None,
            compression: None,
        };
        assert_eq!(desc.sql_repr(), "`ts` TIMESTAMP");

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::from_ty(Ty::Timestamp),
            origin_ty: None,
            length: 0,
            note: Some("PRIMARY KEY".to_string()),
            compression: None,
        };
        assert_eq!(desc.sql_repr(), "`ts` TIMESTAMP PRIMARY KEY");

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::from_ty(Ty::Timestamp),
            origin_ty: None,
            length: 0,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::disabled()),
        };
        assert_eq!(desc.sql_repr(), "`ts` TIMESTAMP PRIMARY KEY");

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::from_ty(Ty::Timestamp),
            origin_ty: None,
            length: 0,
            note: None,
            compression: Some(CompressOptions::disabled()),
        };
        assert_eq!(desc.sql_repr(), "`ts` TIMESTAMP");

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::new(Ty::VarChar, 100),
            origin_ty: None,
            length: 100,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::disabled()),
        };
        assert_eq!(desc.sql_repr(), "`ts` BINARY(100) PRIMARY KEY");

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::new(Ty::VarChar, 100),
            origin_ty: None,
            length: 100,
            note: None,
            compression: Some(CompressOptions::disabled()),
        };
        assert_eq!(desc.sql_repr(), "`ts` BINARY(100)");

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::from_ty(Ty::Timestamp),
            origin_ty: None,
            length: 0,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::new("delta-i", "lz4", "medium")),
        };
        assert_eq!(
            desc.sql_repr(),
            "`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY"
        );

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::from_ty(Ty::Timestamp),
            origin_ty: None,
            length: 0,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::new("disabled", "lz4", "medium")),
        };
        assert_eq!(
            desc.sql_repr(),
            "`ts` TIMESTAMP COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY"
        );

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::from_ty(Ty::Timestamp),
            origin_ty: None,
            length: 0,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::new("disabled", "disabled", "medium")),
        };
        assert_eq!(desc.sql_repr(), "`ts` TIMESTAMP LEVEL 'medium' PRIMARY KEY");

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::from_ty(Ty::Timestamp),
            origin_ty: None,
            length: 0,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::new("disabled", "lz4", "medium")),
        };
        assert_eq!(
            desc.sql_repr(),
            "`ts` TIMESTAMP COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY"
        );

        let desc = Described {
            field: "ts".to_string(),
            data_type: DataType::new(Ty::VarBinary, 100),
            origin_ty: None,
            length: 100,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::new("disabled", "disabled", "medium")),
        };
        assert_eq!(
            desc.sql_repr(),
            "`ts` VARBINARY(100) LEVEL 'medium' PRIMARY KEY"
        );
    }

    #[test]
    fn serde_meta() {
        // ordinary column
        let meta = ColumnMeta::Column(Described {
            field: "name".to_string(),
            data_type: Ty::BigInt.into(),
            origin_ty: None,
            length: 8,
            note: None,
            compression: None,
        });

        let sql = meta.deref().sql_repr();
        assert_eq!(sql, "`name` BIGINT");

        let json = serde_json::to_string(&meta).unwrap();
        let col_meta: ColumnMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, col_meta);

        // primary key column
        let meta = ColumnMeta::Column(Described {
            field: "name".to_string(),
            data_type: Ty::BigInt.into(),
            origin_ty: None,
            length: 8,
            note: Some("PRIMARY KEY".to_string()),
            compression: None,
        });

        let sql = meta.deref().sql_repr();
        assert_eq!(sql, "`name` BIGINT PRIMARY KEY");

        let json = serde_json::to_string(&meta).unwrap();
        let col_meta: ColumnMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, col_meta);

        // with compression
        let meta = ColumnMeta::Column(Described {
            field: "name".to_string(),
            data_type: Ty::BigInt.into(),
            origin_ty: None,
            length: 8,
            note: None,
            compression: Some(CompressOptions::new("delta-i", "lz4", "medium")),
        });

        let sql = meta.deref().sql_repr();
        assert_eq!(
            sql,
            "`name` BIGINT ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium'"
        );

        let json = serde_json::to_string(&meta).unwrap();
        let col_meta: ColumnMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, col_meta);

        // primary key with compression
        let meta = ColumnMeta::Column(Described {
            field: "name".to_string(),
            data_type: Ty::BigInt.into(),
            origin_ty: None,
            length: 8,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::new("delta-i", "lz4", "medium")),
        });

        let sql = meta.deref().sql_repr();
        assert_eq!(
            sql,
            "`name` BIGINT ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY"
        );

        let json = serde_json::to_string(&meta).unwrap();
        let col_meta: ColumnMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, col_meta);

        // deserialize from sequence.
        let json = r#"["name", "BIGINT", 8, null, null, null, null]"#;
        let col_meta: ColumnMeta = serde_json::from_str(json).unwrap();
        assert_eq!(
            col_meta,
            ColumnMeta::Column(Described {
                field: "name".to_string(),
                data_type: Ty::BigInt.into(),
                origin_ty: Some("BIGINT".to_string()),
                length: 8,
                note: None,
                compression: None,
            })
        );
    }

    #[test]
    fn test_decimal_describe() -> anyhow::Result<()> {
        let desc = Described {
            field: "v".to_string(),
            data_type: DataType::new_decimal(Ty::Decimal64, 5, 2),
            origin_ty: Some("DECIMAL(5, 2)".to_string()),
            length: 16,
            note: None,
            compression: None,
        };

        let sql = desc.sql_repr();
        assert_eq!(sql, "`v` DECIMAL(5, 2)");

        let desc = desc.with_origin_ty_name("DECIMAL(10, 2)");
        assert_eq!(desc.origin_ty_name(), Some("DECIMAL(10, 2)"));

        Ok(())
    }

    #[test]
    fn test_blob_describe() -> anyhow::Result<()> {
        let desc = Described {
            field: "c1".to_string(),
            data_type: Ty::Blob.into(),
            origin_ty: None,
            length: 16,
            note: None,
            compression: None,
        };

        let sql = desc.sql_repr();
        assert_eq!(sql, "`c1` BLOB");

        let desc = Described {
            field: "c1".to_string(),
            data_type: Ty::Blob.into(),
            origin_ty: None,
            length: 32,
            note: Some("PRIMARY KEY".to_string()),
            compression: None,
        };

        let sql = desc.sql_repr();
        assert_eq!(sql, "`c1` BLOB PRIMARY KEY");

        let desc = Described {
            field: "c1".to_string(),
            data_type: Ty::Blob.into(),
            origin_ty: None,
            length: 64,
            note: None,
            compression: Some(CompressOptions::new("delta-i", "lz4", "medium")),
        };

        let sql = desc.sql_repr();
        assert_eq!(
            sql,
            "`c1` BLOB ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium'"
        );

        let desc = Described {
            field: "c1".to_string(),
            data_type: Ty::Blob.into(),
            origin_ty: None,
            length: 64,
            note: Some("PRIMARY KEY".to_string()),
            compression: Some(CompressOptions::new("delta-i", "lz4", "medium")),
        };

        let sql = desc.sql_repr();
        assert_eq!(
            sql,
            "`c1` BLOB ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY"
        );

        Ok(())
    }
}
