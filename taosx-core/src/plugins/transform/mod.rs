//! Transform is the bridge of messages from data source and sink
//!
//! A transform will have some specific effect such as:
//! - Extract: one field into several fields.
//! - Select: select specific fields by name or conditions.
//! - Mutate: adds new variables that are functions of existing variables.
//! - Mutate: rebuild one or more fields by existing fields.
//! - Convert: fields data into other data types.
//! - Filter: filter one or more rows in the stream.

use std::{ops::Range, str::FromStr, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use either::Either;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use taos::{
    taos_query::{
        common::Describe,
        helpers::{ColumnMeta, Described},
    },
    JsonMeta, RawBlock, Ty, Value,
};

mod select;

pub use select::Select;
use taosx_ipc::prelude::IpcDataType;
use thiserror::Error;
use tinytemplate::TinyTemplate;

// mod json;

mod parse;

mod filter;

mod map;

mod modeler;

use crate::plugins::transform::parse::ArrayForTaos;

use self::{
    modeler::Modeler,
    parse::{FieldParser, ParserImpl},
};

/// Field parser composer.
///
/// ```json
/// {
///   "parse": { "payload": { "json": ["value::double"] } },
///   "model": {
///     "table": "{topic}",
///     "using": "mqtt",
///     "tags": ["topic"],
///     "columns": ["ts", "value", "qos"]
///   }
/// }
/// ```
///
/// ```json
/// {
///   "parse": { "payload": {
///      "json": ["metric", "location::nchar", "value::double"]
///   } },
///   "model": [{
///     "name": "{topic}-{location}",
///     "using": "{metric}",
///     "tags": ["topic", "location"],
///     "columns": ["ts", "value", "qos"]
///   }
/// }]
/// ```
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Parser {
    parse: ParserImpl,
    model: Modeler,
}

#[derive(Debug, Error)]
pub enum ParserError {
    #[error("Read parser from path {input} error: {error}")]
    IoError {
        input: String,
        error: std::io::Error,
    },
    #[error("Deserialize parser from string {input} error: {error}")]
    DeserializeError {
        input: String,
        error: serde_json::Error,
    },
}
impl FromStr for Parser {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with('@') {
            let s = &s[1..];
            let s = std::fs::read_to_string(s).map_err(|error| ParserError::IoError {
                input: s.to_string(),
                error,
            })?;
            return serde_json::from_str(&s).map_err(|error| ParserError::DeserializeError {
                input: s.to_string(),
                error,
            });
        }
        serde_json::from_str(s).map_err(|error| ParserError::DeserializeError {
            input: s.to_string(),
            error,
        })
    }
}

impl Parser {
    pub fn get_ipcdatatype_from_parser(&self, column_name: &str) -> Option<&IpcDataType> {
        let payload = self.parse.get("payload");
        if payload.is_none() {
            return None;
        }
        let payload = payload.unwrap();
        match payload {
            FieldParser::Json(json) => {
                if json.json.is_none() {
                    None
                } else {
                    let select = json.json.as_ref().unwrap();
                    match select {
                        Select::Include(incl) => {
                            for item in incl.iter() {
                                if (item.alias().is_some() && item.alias().unwrap() == column_name)
                                    || item.name() == column_name
                                {
                                    return item.cast();
                                }
                            }
                            None
                        }
                        _ => None,
                    }
                }
            }
            _ => None,
        }
    }

    pub fn parse_schema(&self, schema: &Arc<Schema>) -> Arc<Schema> {
        let _ = schema;
        todo!()
    }

    fn get_schema_column_with_name<'a>(
        schema: &'a Arc<Schema>,
        name: &str,
    ) -> Option<(usize, &'a Field)> {
        let (idx, field) = schema.fields().into_iter().enumerate().find(|(_, b)| {
            let meta_name = b.metadata().get("name");
            (meta_name.is_some() && name == meta_name.unwrap()) || b.name() == name
        })?;
        Some((idx, field.as_ref()))
    }

    pub fn parse_message_from_records(&self, records: &RecordBatch) -> Result<Message, Error> {
        let batch = self.parse.transform_record_batch(&records)?;
        let schema = batch.schema();
        let batches = vec![batch];
        let batch = &batches[0];
        // tracing::info!("Parse message {:?}", batch);

        fn to_json_valid_batches(batches: &[RecordBatch]) -> Vec<RecordBatch> {
            batches
                .iter()
                .map(|batch| {
                    let schema = batch.schema();
                    let fields = schema.fields();

                    RecordBatch::try_from_iter(batch.columns().iter().enumerate().filter_map(
                        |(idx, data)| {
                            let dt = fields[idx].data_type();
                            if matches!(dt, DataType::Binary | DataType::LargeBinary) {
                                arrow::compute::cast(data, &DataType::Utf8)
                                    .ok()
                                    .map(|data| (fields[idx].name(), data))
                            } else {
                                Some((fields[idx].name(), data.clone()))
                            }
                        },
                    ))
                    .unwrap()
                })
                .collect()
        }
        let json_batches = to_json_valid_batches(&batches);

        let json = arrow::json::writer::record_batches_to_json_rows(
            json_batches.iter().collect_vec().as_slice(),
        )?;

        let mut data = vec![];
        for table in &self.model {
            let mut template = TinyTemplate::new();
            template.add_template("name", &table.name).unwrap();
            if let Some(using) = table.using.as_ref() {
                template.add_template("using", using).unwrap();
            }

            let mut columns_indices = Vec::from_iter(0..batch.num_columns());
            let spec_columns = if let Some(cols) = table.columns.as_ref() {
                //
                let mut indices = Vec::new();
                for name in cols {
                    // if let Some((index, _)) = schema.column_with_name(name) {
                    if let Some((index, _)) =
                        Self::get_schema_column_with_name(&schema, name.as_str())
                    {
                        indices.push(index);
                    } else {
                        tracing::warn!("Selected column {} not found in stream message", name);
                    }
                }
                Some(indices)
            } else {
                None
            };
            let (tags, columns) = if let Some(tags) = &table.tags {
                let mut indices = vec![];
                for name in tags {
                    let (i, _) = Self::get_schema_column_with_name(&schema, name.as_str())
                        .ok_or_else(|| anyhow::format_err!("Invalid field name `{name}`"))?;
                    // let (i, _) = schema
                    // .column_with_name(&name)
                    // .ok_or_else(|| anyhow::format_err!("Invalid field name `{name}`"))?;

                    indices.push(i);
                    columns_indices[i] = usize::MAX;
                }
                let tags = batches[0].project(&indices)?;
                let cols = spec_columns.unwrap_or(
                    columns_indices
                        .into_iter()
                        .filter(|v| *v != usize::MAX)
                        .collect_vec(),
                );
                (Some(tags), batch.project(&cols).unwrap())
            } else {
                (
                    None,
                    batch
                        .project(&spec_columns.unwrap_or(columns_indices))
                        .unwrap(),
                )
            };

            let tables = (0..batch.num_rows())
                .map(|row| (template.render("name", &json[row]).unwrap(), row))
                .into_group_map();

            for (name, indices) in tables {
                let ranges = indices_to_ranges(&indices);
                let name_row = indices[0];
                let batches = ranges
                    .into_iter()
                    .map(|range| columns.slice(range.start, range.len()))
                    .collect_vec();
                let batch = arrow::compute::concat_batches(&columns.schema(), batches.iter())?;

                let using = if table.using.is_some() {
                    template.render("using", &json[name_row]).ok()
                } else {
                    None
                };

                let tags = tags.as_ref().map(|batch| batch.slice(name_row, 1));

                let meta = MessageTableMeta::new(name, using, tags);
                let item = MessageArrowRecords {
                    table: meta,
                    records: batch,
                };
                data.push(item);
            }
        }
        Ok(Message::Records(data))
    }
    pub fn parse(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
        self.self_check()?;
        self.parse.transform_record_batch(&records)
    }

    fn self_check(&self) -> Result<(), Error> {
        for table in &self.model {
            if table.name.is_empty() {
                return Err(Error::EmptyTableName);
            } else if table.name.contains('.') {
                return Err(Error::TableNameContainsDot(table.name.clone()));
            }

            if let Some(columns) = table.columns.as_ref() {
                if columns.is_empty() {
                    return Err(Error::EmptyTableColumns(table.name.clone()));
                }
                for dup in columns.iter().duplicates() {
                    return Err(Error::DuplicatedColumns(dup.clone()));
                }
            }

            if let Some(tags) = table.tags.as_ref() {
                if table.using.as_ref().is_none() {
                    return Err(Error::STableNameRequired);
                }
                for dup in tags.iter().duplicates() {
                    return Err(Error::DuplicatedTags(dup.clone()));
                }
            }
            if let Some(stable) = table.using.as_ref() {
                if stable.is_empty() {
                    return Err(Error::EmptySTableName);
                } else if stable.contains('.') {
                    return Err(Error::STableNameContainsDot(stable.clone()));
                }
            }
        }
        Ok(())
    }
}

// impl TransformExt for Parser {
//     // fn transform_message(&self, item: Message) -> Result<Option<Message>, Error> {
//     //     match item {
//     //         // todo: transformers should works on all kinds of message.
//     //         Message::Raw(raw) => Ok(Some(Message::Raw(raw))),
//     //         Message::Tables(tables) => Ok(Some(Message::Tables(tables))),
//     //         Message::ChildTables(tables) => Ok(Some(Message::ChildTables(tables))),
//     //         Message::Records(records) => {
//     //             let mut new = vec![];
//     //             for records in records {
//     //                 let batch = self.transform_record_batch(&records.records)?;
//     //                 if batch.num_rows() == 0 {
//     //                     continue;
//     //                 }
//     //                 let item = MessageArrowRecords {
//     //                     table: records.table.clone(),
//     //                     records: batch,
//     //                 };
//     //                 new.push(item);
//     //             }
//     //             Ok(Some(Message::Records(new)))
//     //         }
//     //     }
//     // }

//     fn transform_schema(
//         &self,
//         schema: std::sync::Arc<arrow::datatypes::Schema>,
//     ) -> Result<std::sync::Arc<arrow::datatypes::Schema>, Error> {
//         Ok(schema)
//     }

//     fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
//         self.parse(records)
//     }
// }

#[derive(Debug)]
pub struct MessageTable {
    pub name: Arc<String>,
    pub fields: Vec<taos::Field>,
    pub tags: Option<Vec<taos::Field>>,
}

#[derive(Debug)]
pub struct MessageChildTable {
    pub table: Arc<String>,
    pub stable: Option<(String, Vec<Value>)>,
}
#[derive(Debug, Clone)]
pub struct MessageTableMeta {
    pub name: Arc<String>,
    pub using: Option<String>,
    pub tags: Option<RecordBatch>,
}

impl MessageTableMeta {
    pub fn new(
        name: impl Into<Arc<String>>,
        using: impl Into<Option<String>>,
        tags: impl Into<Option<RecordBatch>>,
    ) -> Self {
        Self {
            name: name.into(),
            using: using.into(),
            tags: tags.into(),
        }
    }
}
#[derive(Debug)]
pub struct MessageArrowRecords {
    pub table: MessageTableMeta,
    pub records: RecordBatch,
}

trait ArrowFieldExt {
    fn ty(&self) -> Ty;
}
impl ArrowFieldExt for Field {
    fn ty(&self) -> Ty {
        match self.data_type() {
            arrow::datatypes::DataType::Null => taos::Ty::Null,
            arrow::datatypes::DataType::Boolean => taos::Ty::Bool,
            arrow::datatypes::DataType::Int8 => taos::Ty::TinyInt,
            arrow::datatypes::DataType::Int16 => taos::Ty::SmallInt,
            arrow::datatypes::DataType::Int32 => taos::Ty::Int,
            arrow::datatypes::DataType::Int64 => taos::Ty::BigInt,
            arrow::datatypes::DataType::UInt8 => taos::Ty::UTinyInt,
            arrow::datatypes::DataType::UInt16 => taos::Ty::USmallInt,
            arrow::datatypes::DataType::UInt32 => taos::Ty::Int,
            arrow::datatypes::DataType::UInt64 => taos::Ty::UBigInt,
            arrow::datatypes::DataType::Float16 => taos::Ty::Float,
            arrow::datatypes::DataType::Float32 => taos::Ty::Float,
            arrow::datatypes::DataType::Float64 => taos::Ty::Double,
            arrow::datatypes::DataType::Timestamp(_, _) => taos::Ty::Timestamp,
            arrow::datatypes::DataType::Date32 => taos::Ty::Int,
            arrow::datatypes::DataType::Date64 => taos::Ty::BigInt,
            arrow::datatypes::DataType::Time32(_) => taos::Ty::Int,
            arrow::datatypes::DataType::Time64(_) => taos::Ty::BigInt,
            arrow::datatypes::DataType::Duration(_) => taos::Ty::BigInt,
            arrow::datatypes::DataType::Interval(_) => taos::Ty::BigInt,
            arrow::datatypes::DataType::Binary => taos::Ty::VarChar,
            arrow::datatypes::DataType::FixedSizeBinary(_) => taos::Ty::VarChar,
            arrow::datatypes::DataType::LargeBinary => taos::Ty::VarChar,
            arrow::datatypes::DataType::Utf8 => taos::Ty::VarChar,
            arrow::datatypes::DataType::LargeUtf8 => taos::Ty::VarChar,
            _ => todo!(),
        }
    }
}
impl MessageArrowRecords {
    pub fn schema(&self) -> Describe {
        let schema = self.records.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|field| ColumnMeta::Column(Described::new(field.name(), field.ty(), None)));
        if let Some(tags) = self.table.tags.as_ref() {
            Describe::from_iter(
                columns.chain(
                    tags.schema().fields().iter().map(|field| {
                        ColumnMeta::Tag(Described::new(field.name(), field.ty(), None))
                    }),
                ),
            )
        } else {
            Describe::from_iter(columns)
        }
    }
    pub fn column_meta(&self) -> Vec<ColumnMeta> {
        self.records
            .schema()
            .fields()
            .iter()
            .map(|field| {
                match field.data_type() {
                    DataType::Binary
                    | DataType::Utf8
                    | DataType::LargeBinary
                    | DataType::LargeUtf8 => {
                        let cast_to = field.metadata().get("cast_to");
                        if cast_to.is_some() {
                            let cast_to = cast_to.unwrap();
                            let length = field.metadata().get("length");
                            if length.is_some() {
                                // varchar or nchar
                                let res = length.unwrap().parse::<usize>();
                                match res {
                                    Ok(length) => ColumnMeta::Column(Described::new(
                                        field.name(),
                                        Ty::from_str(cast_to.as_str()).unwrap(),
                                        Some(length),
                                    )),
                                    Err(err) => {
                                        tracing::error!(
                                            "varchar/nchar parse error: {}",
                                            err.to_string()
                                        );
                                        ColumnMeta::Column(Described::new(
                                            field.name(),
                                            Ty::from_str(cast_to.as_str()).unwrap(),
                                            None,
                                        ))
                                    }
                                }
                            } else {
                                // json
                                ColumnMeta::Column(Described::new(
                                    field.name(),
                                    Ty::from_str(cast_to.as_str()).unwrap(),
                                    None,
                                ))
                            }
                        } else {
                            ColumnMeta::Column(Described::new(field.name(), field.ty(), None))
                        }
                    }
                    _ => ColumnMeta::Column(Described::new(field.name(), field.ty(), None)),
                }
            })
            .collect()
    }
    pub fn tag_meta(&self) -> Option<Vec<ColumnMeta>> {
        self.table.tags.as_ref().map(|tags| {
            tags.schema()
                .fields()
                .iter()
                .map(|field| {
                    match field.data_type() {
                        DataType::Binary
                        | DataType::Utf8
                        | DataType::LargeBinary
                        | DataType::LargeUtf8 => {
                            let cast_to = field.metadata().get("cast_to");
                            if cast_to.is_some() {
                                let cast_to = cast_to.unwrap();
                                let length = field.metadata().get("length");
                                if length.is_some() {
                                    // varchar or nchar
                                    let res = length.unwrap().parse::<usize>();
                                    match res {
                                        Ok(length) => ColumnMeta::Column(Described::new(
                                            field.name(),
                                            Ty::from_str(cast_to.as_str()).unwrap(),
                                            Some(length),
                                        )),
                                        Err(err) => {
                                            tracing::error!(
                                                "varchar/nchar parse error: {}",
                                                err.to_string()
                                            );
                                            ColumnMeta::Column(Described::new(
                                                field.name(),
                                                Ty::from_str(cast_to.as_str()).unwrap(),
                                                None,
                                            ))
                                        }
                                    }
                                } else {
                                    // json
                                    ColumnMeta::Column(Described::new(
                                        field.name(),
                                        Ty::from_str(cast_to.as_str()).unwrap(),
                                        None,
                                    ))
                                }
                            } else {
                                ColumnMeta::Column(Described::new(field.name(), field.ty(), None))
                            }
                        }
                        _ => ColumnMeta::Column(Described::new(field.name(), field.ty(), None)),
                    }
                })
                .collect_vec()
        })
    }

    pub fn stable_sql(&self) -> Option<String> {
        if let Some(using) = self.table.using.as_ref() {
            let fields = self.column_meta();
            let columns = fields.iter().map(|f| f.sql_repr()).join(",");
            let tags = self
                .tag_meta()
                .unwrap()
                .iter()
                .map(|f| f.sql_repr())
                .join(",");
            Some(format!(
                "create table if not exists `{}` ({}) tags ({})",
                using, columns, tags
            ))
        } else {
            None
        }
    }
    pub fn table_sql(&self) -> String {
        if let Some(using) = self.table.using.as_ref() {
            let names = self
                .table
                .tags
                .as_ref()
                .unwrap()
                .schema()
                .fields()
                .iter()
                .map(|f| format!("`{}`", f.name()))
                .join(",");

            let values = self
                .table
                .tags
                .as_ref()
                .unwrap()
                .columns()
                .iter()
                .map(|c| c.taos_value(0).to_sql_value())
                .join(",");
            format!(
                "create table if not exists `{}` using `{}` ({}) tags({})",
                self.table.name, using, names, values
            )
        } else {
            let fields = self.column_meta();
            let columns = fields.iter().map(|f| f.sql_repr()).join(",");
            format!(
                "create table if not exists `{}` ({})",
                self.table.name, columns
            )
        }
    }
}

#[derive(Debug)]
pub enum Message {
    Raw(MessageRaw),
    Tables(Vec<MessageTable>),
    ChildTables(Vec<MessageChildTable>),
    Records(Vec<MessageArrowRecords>),
}

#[derive(Debug)]
pub struct MessageItemDecodedData(Either<JsonMeta, Vec<RawBlock>>);

impl From<JsonMeta> for MessageItemDecodedData {
    fn from(value: JsonMeta) -> Self {
        Self(Either::Left(value))
    }
}
impl From<&JsonMeta> for MessageItemDecodedData {
    fn from(value: &JsonMeta) -> Self {
        Self(Either::Left(value.clone()))
    }
}

impl From<RawBlock> for MessageItemDecodedData {
    fn from(value: RawBlock) -> Self {
        Self(Either::Right(vec![value]))
    }
}
impl From<Vec<RawBlock>> for MessageItemDecodedData {
    fn from(value: Vec<RawBlock>) -> Self {
        Self(Either::Right(value))
    }
}

#[derive(Debug)]
pub struct MessageRaw {
    pub raw: Bytes,
    pub decoded: Option<MessageItemDecodedData>,
}

impl MessageRaw {
    fn new(raw: Bytes) -> Self {
        Self { raw, decoded: None }
    }

    fn new_with_decoded(raw: Bytes, decoded: impl Into<MessageItemDecodedData>) -> Self {
        Self {
            raw,
            decoded: Some(decoded.into()),
        }
    }
}

impl Message {
    /// Raw message only.
    ///
    /// The data comes from C API `tmq_get_raw`.
    pub fn raw(raw: Bytes) -> Self {
        Message::Raw(MessageRaw::new(raw))
    }

    /// Raw message with decoded data.
    pub fn raw_with_decoded(raw: Bytes, decoded: impl Into<MessageItemDecodedData>) -> Self {
        Message::Raw(MessageRaw::new_with_decoded(raw, decoded))
    }

    /// Table creation message.
    pub fn tables(tables: Vec<MessageTable>) -> Self {
        Message::Tables(tables)
    }

    /// Child tables creation message.
    pub fn child_tables(tables: Vec<MessageChildTable>) -> Self {
        Message::ChildTables(tables)
    }

    /// Records message in Arrow format.
    pub fn records(records: Vec<MessageArrowRecords>) -> Self {
        Message::Records(records)
    }
}

pub trait TransformExt {
    fn transform_schema(&self, schema: Arc<Schema>) -> Result<Arc<Schema>, Error> {
        let empty = RecordBatch::new_empty(schema);
        self.transform_record_batch(&empty)
            .map(|batch| batch.schema())
    }

    fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
        Ok(records.clone())
    }
}


#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Parse error for field `{field}`: {error}")]
    FieldParserError {
        field: String,
        error: parse::ParseError,
    },
    #[error("The first column should be timestamp, but set as {0:?}")]
    TimestampAtFirst(String),
    #[error("Table name should not be empty")]
    EmptyTableName,
    #[error("Table columns should not be empty for table `{0}`")]
    EmptyTableColumns(String),
    #[error("Table contains duplicated columns: `{0}`")]
    DuplicatedColumns(String),
    #[error("Table contains duplicated tags: `{0}`")]
    DuplicatedTags(String),
    #[error("Table name should not contain dot: {0}")]
    TableNameContainsDot(String),
    #[error("STable name should be set when tags not empty")]
    STableNameRequired,
    #[error("STable name should not be empty")]
    EmptySTableName,
    #[error("STable name should not contain dot: {0}")]
    STableNameContainsDot(String),
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
    #[error("Unknown error: {0}")]
    Other(#[from] anyhow::Error),
}

fn indices_to_ranges(indices: &[usize]) -> Vec<Range<usize>> {
    debug_assert!(!indices.is_empty());
    let mut ranges = vec![];
    let mut start = indices[0];
    let mut end = start + 1;

    for index in &indices[1..] {
        if end == *index {
            end = index + 1;
        } else {
            ranges.push(start..end);
            start = *index;
            end = index + 1;
        }
    }
    ranges.push(start..end);

    ranges
}

#[test]
fn test_indices_to_ranges() {
    let indices = vec![0, 1, 2, 3, 5, 6, 7, 8, 10];
    let ranges = indices_to_ranges(&indices);
    dbg!(&ranges);
    assert_eq!(ranges, vec![0..4, 5..9, 10..11]);
}
