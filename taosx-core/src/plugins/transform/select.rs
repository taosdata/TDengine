use std::{borrow::Cow, collections::HashMap, fmt::Display, str::FromStr, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, FieldRef, Fields, Schema},
    record_batch::RecordBatch,
};
use arrow_schema::ArrowError;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json_path::JsonPath;
use serde_with::{serde_as, DisplayFromStr};
use thiserror::Error;

use taosx_ipc::prelude::IpcDataType;

#[derive(Debug, Clone, PartialEq, Eq)]
enum SelectItemPattern {
    Name(String),
    JsonPath(JsonPath),
}

impl FromStr for SelectItemPattern {
    type Err = serde_json_path::ParseError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        if name.starts_with("$") {
            Ok(SelectItemPattern::JsonPath(JsonPath::parse(name)?))
        } else {
            Ok(SelectItemPattern::Name(name.to_string()))
        }
    }
}

impl Display for SelectItemPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectItemPattern::Name(name) => write!(f, "{name}"),
            SelectItemPattern::JsonPath(json_path) => write!(f, "{json_path}"),
        }
    }
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
struct SelectItem {
    #[serde_as(as = "DisplayFromStr")]
    name: SelectItemPattern,
    alias: Option<String>,
    cast: Option<IpcDataType>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct Exclude {
    exclude: Vec<String>,
}

#[derive(Debug, Error)]
pub enum SelectItemError {
    #[error("Invalid cast type: {0}")]
    InvalidCastType(String),
}

impl FromStr for SelectItem {
    type Err = SelectItemError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let get_name = |name: &str| {
            if name.starts_with("$") {
                match JsonPath::parse(name) {
                    Ok(path) => SelectItemPattern::JsonPath(path),
                    Err(_) => SelectItemPattern::Name(name.to_string()),
                }
            } else {
                SelectItemPattern::Name(name.to_string())
            }
        };
        if let Some((name, alias)) = s.split_once('=') {
            let name = get_name(name);
            if let Some((alias, cast)) = alias.split_once("::") {
                Ok(SelectItem {
                    name,
                    alias: Some(alias.to_string()),
                    cast: Some(cast.parse().map_err(SelectItemError::InvalidCastType)?),
                })
            } else {
                Ok(SelectItem {
                    name,
                    alias: Some(alias.to_string()),
                    cast: None,
                })
            }
        } else if let Some((name, cast)) = s.split_once("::") {
            Ok(SelectItem {
                name: get_name(name),
                alias: None,
                cast: Some(cast.parse().map_err(SelectItemError::InvalidCastType)?),
            })
        } else {
            Ok(SelectItem {
                name: get_name(s),
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
    pub fn name(&self) -> Cow<str> {
        match &self.0.name {
            SelectItemPattern::Name(name) => Cow::Borrowed(name),
            SelectItemPattern::JsonPath(json_path) => Cow::Owned(json_path.to_string()),
        }
    }

    fn readable_name(&self) -> Cow<str> {
        match &self.0.name {
            SelectItemPattern::Name(name) => Cow::Borrowed(name),
            SelectItemPattern::JsonPath(json_path) => {
                let path = json_path.to_string();
                let name = get_json_path_last_field(&path);
                Cow::Owned(name.map(|v| v.to_string()).unwrap_or(path))
            }
        }
    }

    pub fn alias(&self) -> Option<&str> {
        self.0.alias.as_deref()
    }

    pub fn cast(&self) -> Option<&IpcDataType> {
        self.0.cast.as_ref()
    }

    #[cfg(test)]
    fn new(name: &str) -> Self {
        Self(SelectItem {
            name: SelectItemPattern::Name(name.to_string()),
            alias: None,
            cast: None,
        })
    }

    #[cfg(test)]
    fn new_json_path_item(name: &str) -> Self {
        Self(SelectItem {
            name: SelectItemPattern::JsonPath(JsonPath::parse(name).unwrap()),
            alias: None,
            cast: None,
        })
    }

    #[cfg(test)]
    fn with_alias(mut self, alias: &str) -> Self {
        self.0.alias = Some(alias.to_string());
        self
    }

    #[cfg(test)]
    fn with_cast(mut self, cast: IpcDataType) -> Self {
        self.0.cast = Some(cast);
        self
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

impl<'de> Deserialize<'de> for IncludeItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Debug, Deserialize, Clone)]
        #[serde(untagged)]
        enum IncludeInner {
            Str(String),
            Select(SelectItem),
        }
        let select_item = IncludeInner::deserialize(deserializer)?;
        match select_item {
            IncludeInner::Str(s) => Ok(Self(
                SelectItem::from_str(&s).map_err(serde::de::Error::custom)?,
            )),
            IncludeInner::Select(item) => Ok(Self(item)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Select {
    All,
    Pattern(Regex),
    Include(Include),
    Exclude(Exclude),
}

impl Select {
    pub fn from_includes(includes: Vec<String>) -> Result<Self, SelectItemError> {
        let items = includes
            .into_iter()
            .map(|s| SelectItem::from_str(&s))
            .map(|v| v.map(IncludeItem))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::Include(Include(items)))
    }
}

impl<'de> Deserialize<'de> for Select {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if let Some(s) = value.as_str() {
            if s.trim().is_empty() {
                return Ok(Select::All);
            } else {
                use serde::de::Error;
                return Ok(Select::Pattern(Regex::new(s).map_err(D::Error::custom)?));
            }
        }
        if let Ok(v) = serde_json::from_value(value.clone()) {
            return Ok(Select::Include(v));
        }
        if let Ok(v) = serde_json::from_value(value) {
            return Ok(Select::Exclude(v));
        }
        Err(serde::de::Error::custom("unsupported type for json select"))
    }
}

impl Serialize for Select {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Select::All => serializer.serialize_str(""),
            Select::Pattern(re) => serde_regex::serialize(re, serializer),
            Select::Include(include) => include.serialize(serializer),
            Select::Exclude(exclude) => exclude.serialize(serializer),
        }
    }
}

impl PartialEq for Select {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::All, Self::All) => true,
            (Self::Pattern(l0), Self::Pattern(r0)) => l0.as_str() == r0.as_str(),
            (Self::Include(l0), Self::Include(r0)) => l0 == r0,
            (Self::Exclude(l0), Self::Exclude(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Select::All => write!(f, ""),
            _ => write!(
                f,
                "{}",
                serde_json::to_string(&self).map_err(|_| std::fmt::Error)?
            ),
        }
    }
}

pub fn query(fields: &Fields, path: &str) -> Option<(usize, FieldRef)> {
    if path.starts_with('$') {
        let _ = serde_json_path::JsonPath::from_str(path).ok()?;
        // match json_path
        let path = path.trim_start_matches('$').trim_start_matches('.');
        if let Some((name, path)) = path.split_once('.') {
            let (i, field) = fields.find(name)?;
            if let DataType::Struct(fields) = field.data_type() {
                let path = if path.contains(['.', '['].as_ref()) {
                    format!("$.{}", path)
                } else {
                    path.to_string()
                };
                let (_, field) = query(fields, &path)?;
                return Some((i, field));
            }
        }
        if let Some((name, index)) = path.find('[').map(|i| path.split_at(i)) {
            let (i, field) = fields.find(name)?;
            if let DataType::List(field) = field.data_type() {
                // [index]
                if index.ends_with(']') && !index.trim_end_matches(']').contains(']') {
                    return Some((i, field.clone()));
                }
                // [index].path
                if let Some((_, next_path)) = index.find(']').map(|i| index.split_at(i)) {
                    let path = if next_path.contains(['.', '['].as_ref()) {
                        format!("$.{}", next_path)
                    } else {
                        next_path.to_string()
                    };
                    if let DataType::Struct(fields) = field.data_type() {
                        let (_, field) = query(fields, &path)?;
                        return Some((i, field));
                    }
                }
            }
        }
        return fields.find(path).map(|(i, f)| (i, f.clone()));
    }
    fields.find(path).map(|(i, f)| (i, f.clone()))
}
impl Select {
    #[cfg(test)]
    fn pattern(pattern: Regex) -> Self {
        Select::Pattern(pattern)
    }

    #[cfg(test)]
    fn exclude(excludes: &[&str]) -> Self {
        Select::Exclude(Exclude {
            exclude: excludes.iter().map(|v| v.to_string()).collect(),
        })
    }

    pub fn schema(&self, schema: &Schema) -> Schema {
        match self {
            Select::All => schema.clone(),
            Select::Pattern(regex) => {
                let indices = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| regex.is_match(f.name()))
                    .map(|(i, _)| i)
                    .collect_vec();
                schema.project(&indices).unwrap()
            }
            Select::Include(include) => {
                let metadata = schema.metadata.clone();
                let fields = &schema.fields;

                let fields = include
                    .iter()
                    .map(|item| {
                        query(fields, &item.name())
                            .map(|(i, f)| match (item.alias(), item.cast()) {
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
                                            m.insert(
                                                "cast_to".to_string(),
                                                cast.ty().name().to_string(),
                                            );
                                        }
                                        IpcDataType::Json => {
                                            m.insert(
                                                "cast_to".to_string(),
                                                cast.ty().name().to_string(),
                                            );
                                        }
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
                                            m.insert(
                                                "cast_to".to_string(),
                                                cast.ty().name().to_string(),
                                            );
                                        }
                                        IpcDataType::Json => {
                                            m.insert(
                                                "cast_to".to_string(),
                                                cast.ty().name().to_string(),
                                            );
                                        }
                                        _ => (),
                                    }
                                    Field::new(alias, cast.arrow_data_type(), f.is_nullable())
                                        .with_metadata(m)
                                }
                            })
                            .unwrap_or_else(|| {
                                let mut m = HashMap::new();
                                m.insert("query".to_string(), item.name().to_string());
                                m.insert(
                                    "name".to_string(),
                                    item.alias().unwrap_or(&item.name()).to_string(),
                                );
                                let dt = item
                                    .cast()
                                    .map(|cast| {
                                        match cast {
                                            IpcDataType::VarChar(len) | IpcDataType::NChar(len) => {
                                                m.insert("length".to_string(), len.to_string());
                                                m.insert(
                                                    "cast_to".to_string(),
                                                    cast.ty().name().to_string(),
                                                );
                                            }
                                            IpcDataType::Json => {
                                                m.insert(
                                                    "cast_to".to_string(),
                                                    cast.ty().name().to_string(),
                                                );
                                            }
                                            _ => (),
                                        }
                                        cast.arrow_data_type()
                                    })
                                    .unwrap_or(DataType::Null);
                                Field::new(item.alias().unwrap_or(&item.name()), dt, true)
                                    .with_metadata(m)
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
        let schema_ref = std::sync::Arc::new(schema);
        let columns = schema_ref
            .fields()
            .iter()
            .map(|field| {
                // dbg!(&field);
                let metadata = field.metadata();
                let name = &metadata["name"];
                let column = batch.column_by_name(name).unwrap();
                // let dt0 = column.data_type();
                let dt = field.data_type();

                arrow::compute::cast(column, dt)
            })
            .try_collect()?;

        RecordBatch::try_new(schema_ref, columns)
    }

    pub fn parse_json(
        &self,
        field: &str,
        mut value: serde_json::Value,
    ) -> Option<serde_json::Value> {
        match &self {
            Select::All => Some(value),
            Select::Pattern(re) => {
                let mut map = serde_json::Map::with_capacity(re.capture_names().len());
                for caps in re.captures_iter(&value.to_string()) {
                    for (i, group) in caps.iter().enumerate().skip(1) {
                        dbg!(i, &group);
                        let Some(group) = group else {
                            continue;
                        };
                        let key = re
                            .capture_names()
                            .nth(i)
                            .and_then(|name| name.map(|s| s.to_string()))
                            .unwrap_or_else(|| format!("{field}_{i}"));
                        map.insert(key, serde_json::Value::String(group.as_str().to_string()));
                    }
                }
                if map.is_empty() {
                    None
                } else {
                    Some(serde_json::Value::Object(map))
                }
            }
            Select::Include(paths) => {
                let mut map = serde_json::Map::with_capacity(paths.len());
                for item in &paths.0 {
                    let alias = item.alias().map(|v| v.to_string());
                    match &item.0.name {
                        SelectItemPattern::Name(name) => {
                            if let Some(value) = value.get(name) {
                                map.insert(alias.unwrap_or(name.clone()), value.clone());
                            } else {
                                map.insert(alias.unwrap_or(name.clone()), serde_json::Value::Null);
                            }
                        }
                        SelectItemPattern::JsonPath(path) => {
                            let name = item.readable_name().into();
                            if let Some(value) = path.query(&value).first().cloned() {
                                map.insert(alias.unwrap_or(name), value.clone());
                            } else {
                                map.insert(alias.unwrap_or(name), serde_json::Value::Null);
                            }
                        }
                    }
                }
                if map.is_empty() {
                    None
                } else {
                    Some(serde_json::Value::Object(map))
                }
            }
            Select::Exclude(exclude) => {
                let Some(value) = value.as_object_mut() else {
                    return Some(value);
                };
                for name in &exclude.exclude {
                    value.remove(name);
                }
                if value.is_empty() {
                    None
                } else {
                    Some(serde_json::Value::Object(std::mem::take(value)))
                }
            }
        }
    }

    pub fn rebuild_fields_type(&self, fields: &Fields) -> Fields {
        let Select::Include(Include(items)) = self else {
            return fields.clone();
        };
        let mut new_fields = Vec::with_capacity(fields.len());
        for item in items {
            let name = item.readable_name();
            let name = item.alias().unwrap_or(&name);
            let Some((_, field)) = fields.find(name) else {
                continue;
            };
            let Some(dt) = item.cast().map(|t| t.arrow_data_type()) else {
                new_fields.push(field.clone());
                continue;
            };
            new_fields.push(Arc::new(Field::new(name, dt, field.is_nullable())));
        }
        Fields::from(new_fields)
    }
}

fn get_json_path_last_field(json_path: &str) -> Option<&str> {
    let path = json_path.strip_prefix('$').unwrap_or(json_path);
    if path.is_empty() {
        return None;
    }

    let last_dot = path.rfind('.');
    let last_bracket = path.rfind(']');

    match (last_dot, last_bracket) {
        (Some(dot_pos), Some(bracket_pos)) if dot_pos > bracket_pos => {
            let result = &path[dot_pos + 1..];
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        (Some(dot_pos), None) => {
            let result = &path[dot_pos + 1..];
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        (_, Some(bracket_pos)) => extract_bracket(&path[..bracket_pos + 1]),
        (None, None) => None,
    }
}

fn extract_bracket(bracket_section: &str) -> Option<&str> {
    let start_bracket = bracket_section.rfind('[')?;
    let content = &bracket_section[start_bracket + 1..bracket_section.len() - 1];

    if content.len() >= 2 {
        if content.starts_with('\'') && content.ends_with('\'') {
            return Some(&content[1..content.len() - 1]);
        }
        if content.starts_with('"') && content.ends_with('"') {
            return Some(&content[1..content.len() - 1]);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};

    use super::*;

    #[test]
    fn json() {
        let json = r#"["a", "b"]"#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![IncludeItem::new("a"), IncludeItem::new("b")]))
        );

        let json = r#"[{"name": "a"}, "b"]"#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![IncludeItem::new("a"), IncludeItem::new("b")]))
        );

        let json = r#"[{"name": "a", "cast": "timestamp"}, "b"]"#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new("a").with_cast(IpcDataType::Timestamp(
                    arrow::datatypes::TimeUnit::Millisecond
                )),
                IncludeItem::new("b")
            ]))
        );

        let json = r#"[{"name": "a", "alias": "c", "cast": "timestamp"}, "b"]"#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new("a")
                    .with_alias("c")
                    .with_cast(IpcDataType::Timestamp(
                        arrow::datatypes::TimeUnit::Millisecond
                    )),
                IncludeItem::new("b")
            ]))
        );

        let json = r#"["a=c", "b"]"#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new("a").with_alias("c"),
                IncludeItem::new("b")
            ]))
        );

        let json = r#"["a=c::timestamp", "b"]"#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new("a")
                    .with_alias("c")
                    .with_cast(IpcDataType::Timestamp(
                        arrow::datatypes::TimeUnit::Millisecond
                    )),
                IncludeItem::new("b")
            ]))
        );

        let json = r#""""#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(select, Select::All);

        let json = r#"["$['a']=a::double", "$.a.b=c"]"#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(
            select,
            Select::Include(Include(vec![
                IncludeItem::new_json_path_item("$['a']")
                    .with_alias("a")
                    .with_cast(IpcDataType::Float64),
                IncludeItem::new_json_path_item("$.a.b").with_alias("c")
            ]))
        );

        let json = r#""a|b""#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(select, Select::pattern("a|b".parse().unwrap()));

        let json = r#"{ "exclude": ["a", "b"]}"#;
        let select: Select = serde_json::from_str(json).unwrap();
        assert_eq!(select, Select::exclude(&["a", "b"]));
    }

    #[test]
    fn message_select() {
        let json = r#"["a=c", "b"]"#;
        let select: Select = serde_json::from_str(json).unwrap();

        let b: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"a1": "a1", "b1": 1}"#,
            r#"{"a1": "a2", "c1": 1}"#,
        ]));

        let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let _ = select;

        dbg!(&records);
    }

    #[test]
    fn json_path_parse() {
        let test_cases = vec![
            ("$.a.b.c", Some("c")),
            ("$['a']['b']", Some("b")),
            (r#"$["a"]["b"]"#, Some("b")),
            ("$['a'].b", Some("b")),
            (r#"$["a"].b"#, Some("b")),
            ("$.a['b']", Some("b")),
            (r#"$.a["b"]"#, Some("b")),
            ("$.users[0].name", Some("name")),
            ("$['users'][0]['name']", Some("name")),
            (r#"$['users'][0]["name"]"#, Some("name")),
            ("", None),
            ("$", None),
            ("abc", None),
            ("$[12]", None),
        ];

        for (input, expected) in test_cases {
            assert_eq!(get_json_path_last_field(input), expected);
        }
    }
}
