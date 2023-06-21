use std::{collections::HashMap, fmt::Display, str::FromStr, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, FieldRef, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use taosx_ipc::prelude::{IpcDataType, IpcField};

use super::{MessageArrowRecords, TransformExt};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
struct SelectItem {
    name: String,
    alias: Option<String>,
    cast: Option<IpcDataType>,
}

impl SelectItem {
    pub fn new(name: impl Display) -> Self {
        SelectItem {
            name: name.to_string(),
            alias: None,
            cast: None,
        }
    }
}
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct Exclude {
    exclude: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum IncludeInner {
    Str(String),
    Select(SelectItem),
}

#[derive(Debug, Error)]
enum SelectItemError {
    #[error("Invalid cast type: {0}")]
    InvalidCastType(String),
}

impl FromStr for SelectItem {
    type Err = SelectItemError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((name, alias)) = s.split_once('=') {
            if let Some((alias, cast)) = alias.split_once("::") {
                Ok(SelectItem {
                    name: name.to_string(),
                    alias: Some(alias.to_string()),
                    cast: Some(cast.parse().map_err(SelectItemError::InvalidCastType)?),
                })
            } else {
                Ok(SelectItem {
                    name: name.to_string(),
                    alias: Some(alias.to_string()),
                    cast: None,
                })
            }
        } else if let Some((name, cast)) = s.split_once("::") {
            Ok(SelectItem {
                name: name.to_string(),
                alias: None,
                cast: Some(cast.parse().map_err(SelectItemError::InvalidCastType)?),
            })
        } else {
            Ok(SelectItem {
                name: s.to_string(),
                alias: None,
                cast: None,
            })
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IncludeItem(SelectItem);

impl IncludeItem {
    fn new(name: impl Display) -> Self {
        Self(SelectItem::new(name))
    }
    fn with_alias(mut self, alias: impl Display) -> Self {
        self.0.alias.replace(alias.to_string());
        self
    }
    fn with_cast(mut self, cast: IpcDataType) -> Self {
        self.0.cast.replace(cast);
        self
    }

    pub fn name(&self) -> &str {
        &self.0.name
    }

    pub fn alias(&self) -> Option<&str> {
        self.0.alias.as_deref()
    }

    pub fn cast(&self) -> Option<&IpcDataType> {
        self.0.cast.as_ref()
    }

    fn has_cast(&self) -> bool {
        self.0.cast.is_some()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct Include(Vec<IncludeItem>);

impl std::ops::Deref for Include {
    type Target = [IncludeItem];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Include {
    fn with_schema_change(&self) -> bool {
        self.iter().any(|item| item.has_cast())
    }
}

impl<'de> Deserialize<'de> for IncludeItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let select_item = IncludeInner::deserialize(deserializer)?;
        match select_item {
            IncludeInner::Str(s) => Ok(Self(
                SelectItem::from_str(&s).map_err(serde::de::Error::custom)?,
            )),
            IncludeInner::Select(item) => Ok(Self(item)),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum Select {
    #[serde(with = "serde_regex")]
    Pattern(Regex),
    Include(Include),
    Exclude(Exclude),
}

impl FromStr for Select {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

impl PartialEq for Select {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Pattern(l0), Self::Pattern(r0)) => l0.as_str() == r0.as_str(),
            (Self::Include(l0), Self::Include(r0)) => l0 == r0,
            (Self::Exclude(l0), Self::Exclude(r0)) => l0 == r0,
            _ => false,
        }
    }
}

pub fn query(fields: &Fields, path: &str) -> Option<(usize, FieldRef)> {
    if path.starts_with('$') {
        let path = path.trim_start_matches('$').trim_start_matches('.');
        if let Some((name, path)) = path.split_once('.') {
            let (i, field) = fields.find(name)?;
            return if let DataType::Struct(fields) = field.data_type() {
                let (_, field) = query(fields, path)?;
                Some((i, field))
            } else {
                None
            };
        } else {
            return fields.find(path).map(|(i, f)| (i, f.clone()));
        }
    }
    fields.find(path).map(|(i, f)| (i, f.clone()))
}
impl Select {
    pub fn pattern(pattern: Regex) -> Self {
        Self::Pattern(pattern)
    }
    pub fn include(names: &[impl Display]) -> Self {
        Self::Include(Include(names.iter().map(IncludeItem::new).collect()))
    }

    pub fn exclude(names: &[impl Display]) -> Self {
        Self::Exclude(Exclude {
            exclude: names.iter().map(ToString::to_string).collect_vec(),
        })
    }

    // pub fn field(&self, field: &IpcField) -> Option<IpcField> {
    //     None
    // }

    pub fn schema(&self, schema: &Schema) -> Schema {
        match self {
            Select::Pattern(regex) => {
                let indices = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(i, f)| regex.is_match(f.name()))
                    .map(|(i, _)| i)
                    .collect_vec();
                schema.project(&indices).unwrap()
            }
            Select::Include(include) => {
                let metadata = schema.metadata.clone();
                let fields = &schema.fields;

                let fields = include
                    .iter()
                    .filter_map(|item| {
                        query(fields, item.name()).map(|(i, f)| match (item.alias(), item.cast()) {
                            (None, None) => {
                                let mut m = HashMap::new();
                                m.insert("query".to_string(), item.name().to_string());
                                m.insert("name".to_string(), f.name().to_string());
                                m.insert("index".to_string(), i.to_string());
                                Field::new(f.name(), f.data_type().clone(), f.is_nullable())
                                    .with_metadata(m)
                            }
                            (Some(alias), None) => {
                                let mut m = HashMap::new();
                                m.insert("query".to_string(), item.name().to_string());
                                m.insert("name".to_string(), f.name().to_string());
                                m.insert("index".to_string(), i.to_string());
                                Field::new(alias, f.data_type().clone(), f.is_nullable())
                                    .with_metadata(m)
                            }
                            (None, Some(cast)) => {
                                let mut m = HashMap::new();
                                m.insert("query".to_string(), item.name().to_string());
                                m.insert("name".to_string(), f.name().to_string());
                                m.insert("index".to_string(), i.to_string());
                                m.insert("cast_from".to_string(), f.data_type().to_string());
                                match cast {
                                    IpcDataType::VarChar(len) | IpcDataType::NChar(len) => { 
                                        m.insert("length".to_string(), len.to_string()); 
                                        m.insert("cast_to".to_string(), cast.ty().name().to_string());
                                    },
                                    IpcDataType::Json => {
                                        m.insert("cast_to".to_string(), cast.ty().name().to_string());
                                    },
                                    _ => (),
                                }
                                Field::new(f.name(), cast.arrow_data_type(), f.is_nullable())
                                    .with_metadata(m)
                            }
                            (Some(alias), Some(cast)) => {
                                let mut m = HashMap::new();
                                m.insert("query".to_string(), item.name().to_string());
                                m.insert("name".to_string(), f.name().to_string());
                                m.insert("index".to_string(), i.to_string());
                                m.insert("cast_from".to_string(), f.data_type().to_string());
                                match cast {
                                    IpcDataType::VarChar(len) | IpcDataType::NChar(len) => { 
                                        m.insert("length".to_string(), len.to_string()); 
                                        m.insert("cast_to".to_string(), cast.ty().name().to_string());
                                    },
                                    IpcDataType::Json => {
                                        m.insert("cast_to".to_string(), cast.ty().name().to_string());
                                    }
                                    _ => (),
                                }
                                Field::new(alias, cast.arrow_data_type(), f.is_nullable())
                                    .with_metadata(m)
                            }
                        })
                    })
                    .collect_vec();
                Schema::new_with_metadata(fields, metadata)
            }
            Select::Exclude(exclude) => {
                let indices = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, f)| {
                        if exclude.exclude.iter().any(|n| n == f.name()) {
                            None
                        } else {
                            Some(i)
                        }
                    })
                    .collect_vec();
                schema.project(&indices).unwrap()
            }
        }
    }

    pub fn record_batch(&self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let schema = self.schema(&batch.schema());
        let schema_ref = Arc::new(schema);
        let columns = schema_ref
            .fields()
            .iter()
            .map(|field| {
                // dbg!(&field);
                let metadata = field.metadata();
                let name = &metadata["name"];
                let column = batch.column_by_name(&name).unwrap();
                // let dt0 = column.data_type();
                let dt = field.data_type();

                arrow::compute::cast(column, dt)
            })
            .try_collect()?;

        Ok(RecordBatch::try_new(schema_ref, columns)?)
    }
}

impl TransformExt for Select {
    fn transform_message(
        &self,
        item: super::Message,
    ) -> Result<Option<super::Message>, super::Error> {
        match item {
            super::Message::Records(records) => Ok(Some(super::Message::Records(
                records
                    .into_iter()
                    .map(|batch| {
                        self.record_batch(&batch.records)
                            .map(|records| MessageArrowRecords {
                                table: batch.table.clone(),
                                records,
                            })
                    })
                    .try_collect()?,
            ))),
            item => Ok(Some(item)),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, StringArray};

    use crate::plugins::transform::{Message, MessageTableMeta};

    use super::*;

    #[test]
    fn json() {
        let json = r#"["a", "b"]"#;
        let select: Select = serde_json::from_str(&json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![IncludeItem::new("a"), IncludeItem::new("b")]))
        );

        let json = r#"[{"name": "a"}, "b"]"#;
        let select: Select = serde_json::from_str(&json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![IncludeItem::new("a"), IncludeItem::new("b")]))
        );

        let json = r#"[{"name": "a", "cast": "timestamp"}, "b"]"#;
        let select: Select = serde_json::from_str(&json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new("a").with_cast(IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond)),
                IncludeItem::new("b")
            ]))
        );

        let json = r#"[{"name": "a", "alias": "c", "cast": "timestamp"}, "b"]"#;
        let select: Select = serde_json::from_str(&json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new("a")
                    .with_alias("c")
                    .with_cast(IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond)),
                IncludeItem::new("b")
            ]))
        );

        let json = r#"["a=c", "b"]"#;
        let select: Select = serde_json::from_str(&json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new("a").with_alias("c"),
                IncludeItem::new("b")
            ]))
        );

        let json = r#"["a=c::timestamp", "b"]"#;
        let select: Select = serde_json::from_str(&json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new("a")
                    .with_alias("c")
                    .with_cast(IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond)),
                IncludeItem::new("b")
            ]))
        );

        let json = r#""a|b""#;
        let select: Select = serde_json::from_str(&json).unwrap();
        assert_eq!(select, Select::pattern("a|b".parse().unwrap()));

        let json = r#"{ "exclude": ["a", "b"]}"#;
        let select: Select = serde_json::from_str(&json).unwrap();
        assert_eq!(select, Select::exclude(&["a", "b"]));
    }

    #[test]
    fn message_select() {
        let json = r#"["a=c", "b"]"#;
        let select: Select = serde_json::from_str(&json).unwrap();

        let b: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"a1": "a1", "b1": 1}"#,
            r#"{"a1": "a2", "c1": 1}"#,
        ]));

        let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let item = Message::records(vec![MessageArrowRecords {
            table: MessageTableMeta::new(Arc::new("tb1".to_string()), None, None),
            records,
        }]);

        let records = select.transform_message(item).unwrap();

        dbg!(&records);
    }
}
