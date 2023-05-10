use std::{any::Any, collections::HashMap, fmt::Display, str::FromStr, sync::Arc};

use arrow::{
    array::{Array, ArrayBuilder, ArrayRef, StructBuilder, UInt8Array},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};

pub use arrow::datatypes::DataType as ArrowDataType;
use serde::{de::Visitor, Deserialize, Serialize};

use taos_query::prelude::{Itertools, Ty, Value};

use crate::{
    ack::AckType,
    constants::{__ATTRS__, __RECORDS__, __TABLES__, __TABLE_NAME__, __TYPE__},
};

use self::attrs_builder::AttrsBuilder;

use super::components::ListOfStructBuilder;

mod attrs_builder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpcDataType {
    Bool,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Timestamp,
    VarChar(u32),
    NChar(u32),
    Json,
}

impl IpcDataType {
    pub fn short(&self) -> String {
        match self {
            IpcDataType::Bool => "b".to_string(),
            IpcDataType::UInt8 => "u8".to_string(),
            IpcDataType::UInt16 => "u16".to_string(),
            IpcDataType::UInt32 => "u32".to_string(),
            IpcDataType::UInt64 => "u64".to_string(),
            IpcDataType::Int8 => "i8".to_string(),
            IpcDataType::Int16 => "i16".to_string(),
            IpcDataType::Int32 => "i32".to_string(),
            IpcDataType::Int64 => "i64".to_string(),
            IpcDataType::Float32 => "f32".to_string(),
            IpcDataType::Float64 => "f32".to_string(),
            IpcDataType::Timestamp => "timestamp".to_string(),
            IpcDataType::VarChar(len) => format!("varchar({len})"),
            IpcDataType::NChar(len) => format!("nchar({len})"),
            IpcDataType::Json => "json".to_string(),
        }
    }
    pub fn sql_repr(&self) -> String {
        match self {
            IpcDataType::Bool => "bool".to_string(),
            IpcDataType::UInt8 => "tinyint unsigned".to_string(),
            IpcDataType::UInt16 => "smallint unsigned".to_string(),
            IpcDataType::UInt32 => "int unsigned".to_string(),
            IpcDataType::UInt64 => "bigint unsigned".to_string(),
            IpcDataType::Int8 => "tinyint".to_string(),
            IpcDataType::Int16 => "smallint".to_string(),
            IpcDataType::Int32 => "int".to_string(),
            IpcDataType::Int64 => "bigint".to_string(),
            IpcDataType::Float32 => "float".to_string(),
            IpcDataType::Float64 => "double".to_string(),
            IpcDataType::Timestamp => "timestamp".to_string(),
            IpcDataType::VarChar(len) => format!("varchar({len})"),
            IpcDataType::NChar(len) => format!("nchar({len})"),
            IpcDataType::Json => "json".to_string(),
        }
    }

    pub fn ty(&self) -> Ty {
        match self {
            IpcDataType::Bool => Ty::Bool,
            IpcDataType::UInt8 => Ty::UTinyInt,
            IpcDataType::UInt16 => Ty::USmallInt,
            IpcDataType::UInt32 => Ty::UInt,
            IpcDataType::UInt64 => Ty::UBigInt,
            IpcDataType::Int8 => Ty::TinyInt,
            IpcDataType::Int16 => Ty::SmallInt,
            IpcDataType::Int32 => Ty::Int,
            IpcDataType::Int64 => Ty::BigInt,
            IpcDataType::Float32 => Ty::Float,
            IpcDataType::Float64 => Ty::Double,
            IpcDataType::Timestamp => Ty::Timestamp,
            IpcDataType::VarChar(_len) => Ty::VarChar,
            IpcDataType::NChar(_len) => Ty::NChar,
            IpcDataType::Json => Ty::Json,
        }
    }

    pub fn arrow_data_type(&self) -> DataType {
        match self {
            IpcDataType::Bool => DataType::Boolean,
            IpcDataType::UInt8 => DataType::UInt8,
            IpcDataType::UInt16 => DataType::UInt16,
            IpcDataType::UInt32 => DataType::UInt32,
            IpcDataType::UInt64 => DataType::UInt64,
            IpcDataType::Int8 => DataType::Int8,
            IpcDataType::Int16 => DataType::Int16,
            IpcDataType::Int32 => DataType::Int32,
            IpcDataType::Int64 => DataType::Int64,
            IpcDataType::Float32 => DataType::Float32,
            IpcDataType::Float64 => DataType::Float64,
            IpcDataType::Timestamp => DataType::Int64,
            IpcDataType::VarChar(_) => DataType::Binary,
            IpcDataType::NChar(_) => DataType::Utf8,
            IpcDataType::Json => DataType::Utf8,
        }
    }
}

impl FromStr for IpcDataType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "b" | "bool" => Ok(Self::Bool),
            "i8" | "tinyint" => Ok(Self::Int8),
            "i16" | "smallint" => Ok(Self::Int16),
            "i32" | "int" => Ok(Self::Int32),
            "i64" | "bigint" => Ok(Self::Int64),
            "u8" | "tinyint unsigned" => Ok(Self::UInt8),
            "u16" | "smallint unsigned" => Ok(Self::UInt16),
            "u32" | "int unsigned" => Ok(Self::UInt32),
            "u64" | "bigint unsigned" => Ok(Self::UInt64),
            "f32" | "float" => Ok(Self::Float32),
            "f64" | "double" => Ok(Self::Float64),
            "timestamp" => Ok(Self::Timestamp),
            "json" => Ok(Self::Json),
            s => {
                let items: Vec<_> = s.split_terminator(['(', ')']).collect();
                match (items.get(0), items.get(1)) {
                    (Some(t), Some(l)) => match *t {
                        "binary" | "varchar" => Ok(Self::VarChar(l.parse().unwrap())),
                        "nchar" => Ok(Self::NChar(l.parse().unwrap())),
                        _ => Err(s.to_string()),
                    },
                    (Some(t), None) => match *t {
                        "binary" | "varchar" => Ok(Self::VarChar(8)),
                        "nchar" => Ok(Self::NChar(8)),
                        _ => Err(s.to_string()),
                    },
                    _ => Err(s.to_string()),
                }
            }
        }
    }
}

impl Serialize for IpcDataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.sql_repr())
    }
}

impl<'de> Deserialize<'de> for IpcDataType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IpcDataTypeVisitor;
        impl<'de> Visitor<'de> for IpcDataTypeVisitor {
            type Value = IpcDataType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("TDengine type string")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Self::Value::from_str(v)
                    .map_err(|s| E::custom(format!("unknown ipc data type string: {s}")))
            }
        }
        deserializer.deserialize_str(IpcDataTypeVisitor)
    }
}

impl From<&ArrowDataType> for IpcDataType {
    fn from(value: &ArrowDataType) -> Self {
        match value {
            ArrowDataType::Boolean => IpcDataType::Bool,
            ArrowDataType::Int8 => IpcDataType::Int8,
            ArrowDataType::Int16 => IpcDataType::Int16,
            ArrowDataType::Int32 => IpcDataType::Int32,
            ArrowDataType::Int64 => IpcDataType::Int64,
            ArrowDataType::UInt8 => IpcDataType::UInt8,
            ArrowDataType::UInt16 => IpcDataType::UInt16,
            ArrowDataType::UInt32 => IpcDataType::UInt32,
            ArrowDataType::UInt64 => IpcDataType::UInt64,
            ArrowDataType::Float16 => IpcDataType::Float32,
            ArrowDataType::Float32 => IpcDataType::Float32,
            ArrowDataType::Float64 => IpcDataType::Float64,
            ArrowDataType::Timestamp(_, _) => IpcDataType::Timestamp,
            ArrowDataType::Binary => IpcDataType::VarChar(8),
            ArrowDataType::FixedSizeBinary(len) => IpcDataType::VarChar(*len as _),
            ArrowDataType::LargeBinary => IpcDataType::VarChar(4096),
            ArrowDataType::Utf8 => IpcDataType::VarChar(8),
            ArrowDataType::LargeUtf8 => IpcDataType::VarChar(4096),
            ArrowDataType::Null => todo!(),
            ArrowDataType::Date32 => todo!(),
            ArrowDataType::Date64 => todo!(),
            ArrowDataType::Time32(_) => todo!(),
            ArrowDataType::Time64(_) => todo!(),
            ArrowDataType::Duration(_) => todo!(),
            ArrowDataType::Interval(_) => todo!(),
            ArrowDataType::List(_) => todo!(),
            ArrowDataType::FixedSizeList(_, _) => todo!(),
            ArrowDataType::LargeList(_) => todo!(),
            ArrowDataType::Struct(_) => todo!(),
            ArrowDataType::Union(_, _) => todo!(),
            ArrowDataType::Dictionary(_, _) => todo!(),
            ArrowDataType::Decimal128(_, _) => todo!(),
            ArrowDataType::Decimal256(_, _) => todo!(),
            ArrowDataType::Map(_, _) => todo!(),
            ArrowDataType::RunEndEncoded(_, _) => todo!(),
        }
    }
}
impl From<ArrowDataType> for IpcDataType {
    fn from(value: ArrowDataType) -> Self {
        Self::from(&value)
    }
}
impl Display for IpcDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sql_repr())
    }
}

const CURRENT_MESSAGE_SCHEMA_VERSION: &'static str = "1.0";

#[repr(u8)]
pub enum LushMessageType {
    Table = 1,
    Children,
    Insert,
}

#[derive(Debug, Clone, Copy)]
pub enum StreamType {
    /// Line-protocol stream.
    Line,
    /// Table-like stream.
    Flat,
    /// Flow-control stream with lush messages.
    Lush,
    /// OPC POINT
    Point,
}

impl StreamType {
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamType::Line => "line",
            StreamType::Flat => "flat",
            StreamType::Lush => "lush",
            StreamType::Point => "point",
        }
    }
}

impl FromStr for StreamType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "line" => Ok(Self::Line),
            "flat" => Ok(Self::Flat),
            "lush" => Ok(Self::Lush),
            "point" => Ok(Self::Point),
            _ => Err(s.to_string()),
        }
    }
}

impl AsRef<str> for StreamType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Debug)]
pub struct IpcMetadata {
    /// Current version is 1.0 .
    version: String,
    /// Stream type enumeration, include `line`, `flat`, `lush`.
    stream: StreamType,
    /// ACK type enumeration, include `none`, `code`, `lush`.
    ack: AckType,
    init: Option<LushMessageInit>,
    preset: Option<String>,
}

impl<'a> From<&'a HashMap<String, String>> for IpcMetadata {
    fn from(value: &'a HashMap<String, String>) -> Self {
        let version = value
            .get("version")
            .expect("version not found in metadata")
            .to_string();
        let stream = value
            .get("stream")
            .expect("stream not found in metadata")
            .parse()
            .unwrap();
        let ack = value
            .get("ack")
            .expect("ack not found in metadata")
            .parse()
            .unwrap();
        let init = value
            .get("init")
            .map(Clone::clone)
            .map(|s| serde_json::from_str(&s).unwrap());
        // let ack = value.get("ack").expect("ack not found in metadata");
        let preset = value.get("preset").map(Clone::clone);
        Self {
            version,
            stream,
            ack,
            init,
            preset,
        }
    }
}

impl IpcMetadata {
    pub fn new(stream: StreamType) -> Self {
        Self {
            version: CURRENT_MESSAGE_SCHEMA_VERSION.to_string(),
            stream,
            ack: AckType::None,
            preset: None,
            init: None,
        }
    }
    pub fn stream_type(&self) -> &StreamType {
        &self.stream
    }
    pub fn with_preset(mut self, preset: impl Into<String>) -> Self {
        self.preset.replace(preset.into());
        self
    }

    pub fn ack(&self) -> AckType {
        self.ack
    }

    pub fn init(&self) -> Option<&LushMessageInit> {
        self.init.as_ref()
    }

    pub fn init_sql_string(&self) -> Option<String> {
        self.init().map(|s| s.to_sql_string())
    }

    fn to_hashmap(&self) -> HashMap<String, String> {
        let mut map = HashMap::from_iter(vec![
            ("version".to_string(), self.version.to_string()),
            ("stream".to_string(), self.stream.as_str().to_string()),
            ("ack".to_string(), self.ack.as_str().to_string()),
        ]);
        if let Some(init) = self
            .init
            .as_ref()
            .map(|s| serde_json::to_string(s).unwrap())
        {
            map.insert("init".to_string(), init);
        }
        if let Some(trans) = self.preset.as_ref() {
            map.insert("preset".to_string(), trans.to_string());
        }
        map
    }
}
pub struct LushMessageBuilder {
    metadata: IpcMetadata,
    table: Option<String>,
    columns: Vec<IpcField>,
    tags: Vec<IpcField>,
    schema: Option<Arc<Schema>>,
    // attrs_builder: attrs_builder::AttrsBuilder,
}

pub struct IpcField {
    name: String,
    nullable: bool,
    arrow_data_type: ArrowDataType,
    ipc_data_type: Option<IpcDataType>,
}

impl IpcField {
    pub fn new(
        name: impl Into<String>,
        nullable: bool,
        arrow_data_type: ArrowDataType,
        ipc_data_type: impl Into<Option<IpcDataType>>,
    ) -> Self {
        Self {
            name: name.into(),
            nullable,
            arrow_data_type,
            ipc_data_type: ipc_data_type.into(),
        }
    }

    pub fn to_arrow_field(&self) -> Field {
        Field::new(self.name.as_str(), self.arrow_data_type.clone(), true)
    }
    pub fn to_arrow_field_with_dict(&self, dict_id: i64, dict_is_ordered: bool) -> Field {
        Field::new_dict(
            self.name.as_str(),
            self.arrow_data_type.clone(),
            true,
            dict_id,
            dict_is_ordered,
        )
    }
}

pub struct LushInsertBuilder<'a> {
    schema: &'a LushMessageBuilder,
    columns_builder: ListOfStructBuilder,
    attrs_builder: AttrsBuilder,
    table: Option<String>,
    tag_idx: usize,
    using: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LushField {
    name: String,
    r#type: IpcDataType,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LushMessageInit {
    name: String,
    columns: Vec<LushField>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<LushField>,
}

pub struct ChildTablesBuilder<'a> {
    schema: &'a LushMessageBuilder,
    fields: Vec<Field>,
    builder: ListOfStructBuilder,
    name: Option<String>,
    tag_index: usize,
    attrs_builder: AttrsBuilder,
    columns_builder: ListOfStructBuilder,
}

impl<'a> ChildTablesBuilder<'a> {
    pub fn create_table_with_tags(&mut self, _name: &str, _tags: &[Value]) -> &mut Self {
        self
    }

    pub fn next_table(&mut self, name: &str) -> &mut Self {
        let _ = self.builder.append(&name.to_string());
        self
    }

    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        // self.columns_builder.append(true);
        let tables = self.builder.finish();

        // let list = self.columns_builder.finish();

        let msg_type = LushMessageType::Children;
        let columns = self.columns_builder.append_null_row().finish();
        let attrs = self.attrs_builder.append_null_row().finish();
        // dbg!(attrs.len(), tables.len(), columns.len());

        let batch = RecordBatch::try_new(
            self.schema.schema_ref(),
            vec![
                Arc::new(UInt8Array::from(vec![msg_type as u8])), // __type
                Arc::new(tables),                                 // __tables__
                Arc::new(attrs),                                  // __attrs__
                Arc::new(columns),
            ],
        )
        .unwrap();
        println!("build child table record batch");
        Ok(batch)
    }

    pub fn append(&mut self, value: &dyn Any) -> &mut Self {
        self.builder.append(value);

        self
    }
    pub fn append_null(&mut self) -> &mut Self {
        self.builder.append_null();

        self
    }
    pub fn fill_nulls_to_end(&mut self) -> &mut Self {
        self.builder.fill_nulls_to_end();
        self
    }
}

impl LushMessageInit {
    pub fn to_sql_string(&self) -> String {
        let columns = self
            .columns
            .iter()
            .filter(|f| f.name != __TABLE_NAME__)
            .map(|f| format!("`{}` {}", f.name, f.r#type.sql_repr()))
            .join(",");
        if self.tags.len() > 0 {
            let tags = self
                .tags
                .iter()
                .map(|f| format!("`{}` {}", f.name, f.r#type.sql_repr()))
                .join(",");
            format!(
                "CREATE TABLE IF NOT EXISTS `{}` ({}) TAGS ({})",
                self.name, columns, tags
            )
        } else {
            format!("CREATE TABLE IF NOT EXISTS `{}` ({})", self.name, columns)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_data_type(&self, name: &str) -> Option<&IpcDataType> {
        self.columns
            .iter()
            .find(|f| f.name == name)
            .map(|f| &f.r#type)
    }
    pub fn tag_data_type(&self, name: &str) -> Option<&IpcDataType> {
        self.tags.iter().find(|f| f.name == name).map(|f| &f.r#type)
    }
}
impl<'a> LushInsertBuilder<'a> {
    /// Specify which table to insert into, it could be None to use the table name set in `init`
    /// as normal table or last `name` field for child table.
    pub fn table(&mut self, table: impl Into<String>) -> &mut Self {
        let table: String = table.into();
        let _ = self.attrs_builder.append(&table);
        self.table.replace(table);
        self
    }

    /// Set anther stable if it could use consist schema with which initialized in `init` metadata.
    pub fn using(&mut self, _stable: impl Into<String>) -> &mut Self {
        // debug_assert!(self.table.is_some());
        // let stable = stable.into();
        // self.using.replace(stable.into());
        self
    }
    /// Build a record batch with ordered tag values set at `init` metadata.
    ///
    /// Call it multiple times in order to add each tags declared in the `init` metadata.
    pub fn with_tag(&mut self, tag_value: &dyn Any) -> &mut Self {
        debug_assert!(self.table.is_some());
        let _ = self.attrs_builder.append(tag_value);
        self
    }

    /// A struct builder to columns.
    ///
    /// Get the field builder by id or by name to add records in one batch.
    pub fn columns_builder(&mut self) -> &mut StructBuilder {
        self.columns_builder.values()
    }

    pub fn append(&mut self) -> &mut Self {
        // self.columns_builder.append(true);
        self
    }

    fn build_attrs(&mut self) -> ArrayRef {
        let builder = &mut self.attrs_builder;
        if self.table.is_none() {
            builder.append_null_row();
        }
        Arc::new(builder.finish())
    }

    /// Build the record batch and clear the cache, so that you can reuse the builder.
    pub fn build(&mut self) -> Result<RecordBatch, ArrowError> {
        // self.columns_builder.append(&true);

        let list = self.columns_builder.finish();

        let msg_type = LushMessageType::Insert;

        let attrs = self.build_attrs();

        let tables = self.schema.tables_builder().append_null_row().finish();

        let batch = RecordBatch::try_new(
            self.schema.schema_ref(),
            vec![
                Arc::new(UInt8Array::from(vec![msg_type as u8])), // __type
                Arc::new(tables),                                 // __tables__
                attrs,                                            // __attrs__
                Arc::new(list),
            ],
        )?;
        Ok(batch)
    }
}
impl LushMessageBuilder {
    pub fn new() -> Self {
        Self {
            metadata: IpcMetadata::new(StreamType::Lush),
            table: None,
            columns: vec![],
            tags: vec![],
            schema: None,
        }
    }
    pub fn with_preset(mut self, preset: impl Into<String>) -> Self {
        self.metadata.preset.replace(preset.into());
        self
    }

    pub fn with_stable(
        mut self,
        name: impl Into<String>,
        columns: Vec<IpcField>,
        tags: Vec<IpcField>,
    ) -> Self {
        let name = name.into();
        let init = LushMessageInit {
            name: name.clone(),
            columns: columns
                .iter()
                .map(|f| LushField {
                    name: f.name.clone(),
                    r#type: f
                        .ipc_data_type
                        .clone()
                        .unwrap_or_else(|| IpcDataType::from(&f.arrow_data_type)),
                })
                .collect(),
            tags: tags
                .iter()
                .map(|f| LushField {
                    name: f.name.clone(),
                    r#type: f
                        .ipc_data_type
                        .clone()
                        .unwrap_or_else(|| IpcDataType::from(&f.arrow_data_type)),
                })
                .collect(),
        };
        self.table.replace(name);
        self.columns = columns;
        self.tags = tags;
        self.metadata.init.replace(init);
        // let init =
        self
    }

    pub fn child_tables_builder(&self) -> ChildTablesBuilder {
        let fields = self.table_fields();
        let builder = self.tables_builder();
        let columns_builder = self.columns_builder();
        let attrs_builder = self.attrs_builder();
        ChildTablesBuilder {
            schema: self,
            fields,
            builder,
            name: None,
            tag_index: 0,
            attrs_builder,
            columns_builder,
        }
    }

    fn create_fields(&self) -> Vec<Field> {
        let columns = DataType::Struct(
            self.columns
                .iter()
                .map(|f| Field::new(&f.name, f.arrow_data_type.clone(), false))
                .collect(),
        );
        let tags = DataType::Struct(
            self.tags
                .iter()
                .map(|f| Field::new(&f.name, f.arrow_data_type.clone(), false))
                .collect(),
        );
        vec![
            Field::new("name", DataType::Binary, false),
            Field::new("columns", columns, false),
            Field::new("tags", tags, true),
        ]
    }

    fn table_fields(&self) -> Vec<Field> {
        Some(Field::new(__TABLE_NAME__, DataType::Binary, true))
            .into_iter()
            .chain(
                self.tags
                    .iter()
                    .map(|f| f.to_arrow_field_with_dict(1, false)),
            )
            .collect_vec()
    }

    pub fn build(mut self) -> Self {
        let metadata = self.metadata.to_hashmap();

        // let name =

        let dict_is_ordered = false;

        let record = DataType::Struct(self.columns.iter().map(IpcField::to_arrow_field).collect());
        // let tags = DataType::Struct(self.tags.iter().map(IpcField::to_arrow_field).collect());

        let attrs_fields = self.table_fields();

        let attr = DataType::Struct(Fields::from(attrs_fields));

        let record_list = DataType::List(Arc::new(Field::new("item", record.clone(), true)));

        let schema = Schema::new(vec![
            Field::new(__TYPE__, DataType::UInt8, false),
            Field::new_dict(
                __TABLES__,
                DataType::List(Arc::new(Field::new("item", attr.clone(), true))),
                true,
                LushMessageType::Children as u8 as _,
                dict_is_ordered,
            ),
            Field::new_dict(
                __ATTRS__,
                attr,
                true,
                LushMessageType::Insert as u8 as _,
                dict_is_ordered,
            ),
            Field::new_dict(
                __RECORDS__,
                record_list,
                true,
                LushMessageType::Insert as u8 as _,
                dict_is_ordered,
            ),
        ])
        .with_metadata(metadata);
        self.schema.replace(Arc::new(schema));
        self
    }

    pub fn schema_ref(&self) -> Arc<Schema> {
        self.schema.as_ref().unwrap().clone()
    }

    fn column_fields(&self) -> Vec<Field> {
        self.columns.iter().map(IpcField::to_arrow_field).collect()
    }

    fn columns_builder(&self) -> ListOfStructBuilder {
        ListOfStructBuilder::new(self.column_fields(), 1)
    }
    fn attrs_builder(&self) -> AttrsBuilder {
        let fields = self.table_fields();

        AttrsBuilder::new(fields, 1)
    }

    fn tables_builder(&self) -> ListOfStructBuilder {
        let fields = self.table_fields();
        ListOfStructBuilder::new(fields, 1)
    }

    pub fn insert_builder(&self) -> LushInsertBuilder {
        let columns_builder = self.columns_builder();
        let attrs_builder = self.attrs_builder();
        LushInsertBuilder {
            schema: self,
            columns_builder,
            attrs_builder,
            table: None,
            tag_idx: 0,
            using: None,
        }
    }

    pub fn record_batch_builder(&self) {}
}
