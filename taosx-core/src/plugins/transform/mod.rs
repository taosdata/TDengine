//! Transform is the bridge of messages from data source and sink
//!
//! A transform will have some specific effect such as:
//! - Extract: one field into several fields.
//! - Select: select specific fields by name or conditions.
//! - Mutate: adds new variables that are functions of existing variables.
//! - Mutate: rebuild one or more fields by existing fields.
//! - Convert: fields data into other data types.
//! - Filter: filter one or more rows in the stream.

use std::{any::Any, collections::HashMap, sync::Arc, task::Poll};

use arrow::{
    array::{
        make_builder, Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use either::Either;
use futures::{Sink, Stream};
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use taos::{
    taos_query::{
        common::{Describe, RawData},
        helpers::{ColumnMeta, Described},
    },
    JsonMeta, RawBlock, Ty, Value,
};

use serde_json::Value as JsonValue;

mod select;

pub use select::Select;
pub use taosx_ipc::prelude::IpcDataType;

mod json;
pub use json::Json;

mod parse;

pub use parse::Parser;

use crate::plugins::transform::parse::ArrayForTaos;

pub struct Create {}

pub struct Raw {}

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
            .map(|field| ColumnMeta::Column(Described::new(field.name(), field.ty(), None)))
            .collect()
    }
    pub fn tag_meta(&self) -> Option<Vec<ColumnMeta>> {
        self.table.tags.as_ref().map(|tags| {
            tags.schema()
                .fields()
                .iter()
                .map(|field| ColumnMeta::Tag(Described::new(field.name(), field.ty(), None)))
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

fn append(builder: &mut dyn Any, datatype: &DataType, value: &JsonValue) {
    match datatype {
        DataType::Null => todo!(),
        DataType::Boolean => todo!(),
        DataType::Int8 => todo!(),
        DataType::Int16 => todo!(),
        DataType::Int32 => todo!(),
        DataType::Int64 => todo!(),
        DataType::UInt8 => todo!(),
        DataType::UInt16 => todo!(),
        DataType::UInt32 => todo!(),
        DataType::UInt64 => todo!(),
        DataType::Float16 => todo!(),
        DataType::Float32 => todo!(),
        DataType::Float64 => todo!(),
        DataType::Timestamp(_, _) => todo!(),
        DataType::Date32 => todo!(),
        DataType::Date64 => todo!(),
        DataType::Time32(_) => todo!(),
        DataType::Time64(_) => todo!(),
        DataType::Duration(_) => todo!(),
        DataType::Interval(_) => todo!(),
        DataType::Binary => todo!(),
        DataType::FixedSizeBinary(_) => todo!(),
        DataType::LargeBinary => todo!(),
        DataType::Utf8 => todo!(),
        DataType::LargeUtf8 => todo!(),
        DataType::List(_) => todo!(),
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::LargeList(_) => todo!(),
        DataType::Struct(_) => todo!(),
        DataType::Union(_, _) => todo!(),
        DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal128(_, _) => todo!(),
        DataType::Decimal256(_, _) => todo!(),
        DataType::Map(_, _) => todo!(),
        DataType::RunEndEncoded(_, _) => todo!(),
    }
}
pub enum Extractor {
    Json {
        at: String,
        flatten: bool,
        select: Option<Vec<String>>,
        keep: bool,
    },
    Regex {
        pattern: Regex,
    },
}

pub trait Source {
    type Offset;

    fn describe(&self, table: &str) -> Describe;
    fn commit(&self, offset: Self::Offset);
}

pub trait TransformExt {
    fn transform_message(&self, item: Message) -> Result<Option<Message>, Error> {
        Ok(Some(item))
    }

    fn transform_schema(&self, schema: Arc<Schema>) -> Result<Arc<Schema>, Error> {
        Ok(schema)
    }
    fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
        Ok(records.clone())
    }
}

// #[derive(Debug, Deserialize, Serialize, Clone)]
// #[serde(rename_all = "snake_case")]
// pub enum Parser {
//     Headers(),
//     Json(Json),
//     Select(Select),
// }

// pub struct Parsers {
//     parser: Vec<Parser>,
// }

// impl Parsers {
//     pub fn from_parser(parser: Vec<Parser>) -> Self {
//         Self { parser }
//     }

//     pub fn parse(&self, item: Message) -> Option<Message> {
//         for p in &self.parser {
//             // p.parse(&mut item)
//         }
//         None
//     }
// }

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Parse error for field `{field}`: {error}")]
    FieldParserError {
        field: String,
        error: parse::ParseError,
    },
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
    #[error("Unknown error: {0}")]
    Other(#[from] anyhow::Error),
}
// impl Sink<Message> for Parsers {
//     type Error = Error;

//     fn poll_ready(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Result<(), Self::Error>> {
//         cx.waker().wake_by_ref();
//         std::task::Poll::Ready(Ok(()))
//     }

//     fn start_send(self: std::pin::Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
//         todo!();
//         Ok(())
//     }

//     fn poll_flush(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Result<(), Self::Error>> {
//         Poll::Ready(Ok(()))
//     }

//     fn poll_close(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Result<(), Self::Error>> {
//         Poll::Ready(Ok(()))
//     }
// }

// impl Stream for Parsers {
//     type Item = Result<Message, Error>;

//     fn poll_next(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         todo!()
//     }
// }

#[cfg(test)]
mod tests {
    use arrow::array::ArrayRef;
    use futures::{SinkExt, StreamExt};

    use super::*;

    // #[tokio::test]
    // async fn test_parser() -> anyhow::Result<()> {
    //     let mut parser = Parsers::from_parser(vec![]);
    //     let item = Message::tables(vec![]);
    //     let _ = parser.send(item).await;
    //     parser.flush().await?;

    //     let message = parser.next().await.transpose()?;

    //     Ok(())
    // }
}
