use std::{ops::Range, str::FromStr, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Float16Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{Field, Fields, Schema},
    error::ArrowError,
    ipc::FixedSizeBinary,
    record_batch::RecordBatch,
};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tinytemplate::TinyTemplate;

use crate::plugins::transform::MessageTableMeta;

use super::{Message, MessageArrowRecords, TransformExt};

mod json;

use json::Json;

mod cast;

use cast::Cast;

mod regex;
use self::regex::Regex;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error(transparent)]
    JsonPathError(#[from] serde_json_path::ParseError),
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
}

/// Parse will be applied to one filed of data with [ArrayRef].
///
/// ```rust,no-run
/// use Parse;
/// let field = Field::new("a1", DataType::Utf8, false);
/// let array = Arc::new(StringArray::try_from_iter(["2022-02-02"]))
///
/// let cast = Cast { as: Timestamp, datetime_format: "%Y-%m-%d" };
/// assert!(cast.is_scala());
/// let array = cast.parse_scalar(&field, array)?;
///
/// let parser = Json { keep: false, select: ["c1", "c2"] };
/// assert!(!cast.is_scala());
///
/// let (records, may_be_indices) = parser.parse_array(field, array)?;
/// assert!(records.num_columns(), 2);
/// ```
///
pub trait Parse {
    /// The parser will remove or append some rows to the record batch.
    fn num_rows_will_be_changed(&self) -> bool;

    /// The parser will spread one field to many.
    fn num_columns_will_be_changed(&self) -> bool;

    /// Parse an array into a record batch, returns the original array indices and the final record batch as a tuple.
    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), ParseError>;

    /// A parser is scala means its rows and columns number will not be changed by parser.
    fn is_scala(&self) -> bool {
        !self.num_columns_will_be_changed() && !self.num_columns_will_be_changed()
    }

    /// Parse the array to new array without rows/columns changed, returns a tuple with field and array data.
    fn parse_scalar(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(Field, ArrayRef), ParseError> {
        debug_assert!(self.is_scala());
        self.parse_array(field, array)
            .map(|(batch, _)| (batch.schema().field(0).clone(), batch.column(0).clone()))
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
enum FieldParser {
    Json(Json),
    Cast(Cast),
    Regex(Regex),
    Alias { alias: String },
}

impl Parse for FieldParser {
    fn num_rows_will_be_changed(&self) -> bool {
        if let FieldParser::Json(json) = self {
            json.num_rows_will_be_changed()
        } else {
            false
        }
    }

    fn num_columns_will_be_changed(&self) -> bool {
        if let FieldParser::Json(json) = self {
            json.num_columns_will_be_changed()
        } else {
            false
        }
    }

    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), ParseError> {
        match self {
            FieldParser::Json(json) => json.parse_array(field, array),
            FieldParser::Cast(cast) => cast.parse_array(field, array),
            FieldParser::Regex(regex) => regex.parse_array(field, array),
            FieldParser::Alias { alias } => {
                let batch = RecordBatch::try_from_iter([(alias, array.clone())])?;
                Ok((batch, None))
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum Model {
    V(Vec<Table>),
    O(Table),
}

impl From<Model> for Vec<Table> {
    fn from(value: Model) -> Self {
        match value {
            Model::V(v) => v,
            Model::O(i) => vec![i],
        }
    }
}

mod model_serde {
    use super::{Model, Table};
    use serde::{self, Deserialize, Deserializer};

    type Target = Vec<Table>;
    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<D>(D) -> Result<T, D::Error> where D: Deserializer
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Target, D::Error>
    where
        D: Deserializer<'de>,
    {
        Model::deserialize(deserializer).map(Into::into)
    }
}

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
///   "model": {
///     "table": "{topic}-{location}",
///     "using": "{metric}",
///     "tags": ["topic", "location"],
///     "columns": ["ts", "value", "qos"]
///   }
/// }
/// ```
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Parser {
    parse: LinkedHashMap<String, FieldParser>,
    #[serde(deserialize_with = "model_serde::deserialize")]
    model: Vec<Table>,
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

impl<T: Array> ArrayForTaos for T {}

pub trait ArrayForTaos: Array {
    fn taos_type(&self) -> taos::Ty {
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
            arrow::datatypes::DataType::List(_) => todo!(),
            arrow::datatypes::DataType::FixedSizeList(_, _) => todo!(),
            arrow::datatypes::DataType::LargeList(_) => todo!(),
            arrow::datatypes::DataType::Struct(_) => todo!(),
            arrow::datatypes::DataType::Union(_, _) => todo!(),
            arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
            arrow::datatypes::DataType::Decimal128(_, _) => todo!(),
            arrow::datatypes::DataType::Decimal256(_, _) => todo!(),
            arrow::datatypes::DataType::Map(_, _) => todo!(),
            arrow::datatypes::DataType::RunEndEncoded(_, _) => todo!(),
        }
    }

    fn taos_value(&self, index: usize) -> taos::Value {
        let ty = self.taos_type();
        if self.is_null(index) {
            taos::Value::Null(ty)
        } else {
            match self.data_type() {
                arrow::datatypes::DataType::Boolean => {
                    let array = self.as_any().downcast_ref::<BooleanArray>().unwrap();
                    taos::Value::Bool(array.value(index))
                }
                arrow::datatypes::DataType::Int8 => {
                    let array = self.as_any().downcast_ref::<Int8Array>().unwrap();
                    taos::Value::TinyInt(array.value(index))
                }
                arrow::datatypes::DataType::Int16 => {
                    let array = self.as_any().downcast_ref::<Int16Array>().unwrap();
                    taos::Value::SmallInt(array.value(index))
                }
                arrow::datatypes::DataType::Int32 => {
                    let array = self.as_any().downcast_ref::<Int32Array>().unwrap();
                    taos::Value::Int(array.value(index))
                }
                arrow::datatypes::DataType::Int64 => {
                    let array = self.as_any().downcast_ref::<Int64Array>().unwrap();
                    taos::Value::BigInt(array.value(index))
                }
                arrow::datatypes::DataType::UInt8 => {
                    let array = self.as_any().downcast_ref::<UInt8Array>().unwrap();
                    taos::Value::UTinyInt(array.value(index))
                }
                arrow::datatypes::DataType::UInt16 => {
                    let array = self.as_any().downcast_ref::<UInt16Array>().unwrap();
                    taos::Value::USmallInt(array.value(index))
                }
                arrow::datatypes::DataType::UInt32 => {
                    let array = self.as_any().downcast_ref::<UInt32Array>().unwrap();
                    taos::Value::UInt(array.value(index))
                }
                arrow::datatypes::DataType::UInt64 => {
                    let array = self.as_any().downcast_ref::<UInt64Array>().unwrap();
                    taos::Value::UBigInt(array.value(index))
                }
                arrow::datatypes::DataType::Float16 => {
                    let array = self.as_any().downcast_ref::<Float16Array>().unwrap();
                    taos::Value::Float(array.value(index).to_f32())
                }
                arrow::datatypes::DataType::Float32 => {
                    let array = self.as_any().downcast_ref::<Float32Array>().unwrap();
                    taos::Value::Float(array.value(index))
                }
                arrow::datatypes::DataType::Float64 => {
                    let array = self.as_any().downcast_ref::<Float64Array>().unwrap();
                    taos::Value::Double(array.value(index))
                }
                arrow::datatypes::DataType::Timestamp(unit, None) => match unit {
                    arrow::datatypes::TimeUnit::Second => {
                        let array = self
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .unwrap();
                        taos::Value::Timestamp(taos::taos_query::common::Timestamp::Milliseconds(
                            array.value(index) * 1000,
                        ))
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        let array = self
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        taos::Value::Timestamp(taos::taos_query::common::Timestamp::Milliseconds(
                            array.value(index),
                        ))
                    }
                    arrow::datatypes::TimeUnit::Microsecond => {
                        let array = self
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        taos::Value::Timestamp(taos::taos_query::common::Timestamp::Microseconds(
                            array.value(index),
                        ))
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        let array = self
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        taos::Value::Timestamp(taos::taos_query::common::Timestamp::Nanoseconds(
                            array.value(index),
                        ))
                    }
                },
                arrow::datatypes::DataType::Binary => {
                    let array = self.as_any().downcast_ref::<BinaryArray>().unwrap();
                    taos::Value::VarChar(String::from_utf8(array.value(index).to_vec()).unwrap())
                }
                arrow::datatypes::DataType::FixedSizeBinary(_) => {
                    let array = self.as_any().downcast_ref::<FixedSizeBinary>().unwrap();
                    let start = index * array.byteWidth() as usize;
                    let end = (index + 1) * array.byteWidth() as usize;
                    let bytes = &array._tab.buf()[start..end];
                    taos::Value::VarChar(String::from_utf8(bytes.to_vec()).unwrap())
                }
                arrow::datatypes::DataType::LargeBinary => {
                    let array = self.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                    taos::Value::VarChar(String::from_utf8(array.value(index).to_vec()).unwrap())
                }
                arrow::datatypes::DataType::Utf8 => {
                    let array = self.as_any().downcast_ref::<StringArray>().unwrap();
                    taos::Value::VarChar(array.value(index).into())
                }
                arrow::datatypes::DataType::LargeUtf8 => {
                    let array = self.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    taos::Value::VarChar(array.value(index).into())
                }
                _ => todo!(),
            }
        }
    }
    fn taos_values(&self) -> Vec<taos::Value> {
        (0..self.len())
            .map(|index| self.taos_value(index))
            .collect()
    }
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
impl Parser {
    pub fn parse_schema(&self, schema: &Arc<Schema>) -> Arc<Schema> {
        todo!()
    }
    pub fn parse_message_from_records(
        &self,
        records: &RecordBatch,
    ) -> Result<Message, super::Error> {
        let batch = self.parse(records)?;
        let schema = batch.schema();
        let batches = vec![batch];
        let batch = &batches[0];
        let json = arrow::json::writer::record_batches_to_json_rows(&batches)?;

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
                    let (index, _) = schema.column_with_name(name).ok_or_else(|| {
                        anyhow::format_err!("Selected column {} not found in stream message", name)
                    })?;
                    indices.push(index);
                }
                Some(indices)
            } else {
                None
            };
            let (tags, columns) = if let Some(tags) = &table.tags {
                let mut indices = vec![];
                for name in tags {
                    let (i, _) = schema
                        .column_with_name(&name)
                        .ok_or_else(|| anyhow::format_err!("Invalid field name `{name}`"))?;

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
    pub fn parse(&self, records: &RecordBatch) -> Result<RecordBatch, super::Error> {
        let schema = records.schema();
        let metadata = schema.metadata().clone();

        let mut new_fields = vec![];
        let mut new_data = vec![];

        for field in schema.fields() {
            let name = field.name();
            let array = records.column_by_name(&name).unwrap();

            if let Some(parser) = self.parse.get(name) {
                let (batch, indices) = parser.parse_array(field, array).map_err(|error| {
                    super::Error::FieldParserError {
                        field: name.to_string(),
                        error,
                    }
                })?;
                debug_assert!(indices.is_none(), "Indices not supported currently");
                for field in batch.schema().fields() {
                    new_fields.push(field.as_ref().clone());
                    let array = batch.column_by_name(field.name()).unwrap();
                    new_data.push(array.clone());
                }
            } else {
                new_fields.push(field.as_ref().clone());
                new_data.push(array.clone());
            }
        }
        let schema = Schema::new_with_metadata(new_fields, metadata);
        let batch = RecordBatch::try_new(Arc::new(schema), new_data)?;
        Ok(batch)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Op {
    eq: Option<String>,
    is: Option<String>,
    lt: Option<String>,
    lte: Option<String>,
    gt: Option<String>,
    gte: Option<String>,
    r#in: Option<Vec<String>>,
}
// #[serde(untagged)]
// pub enum Op {
//     // Type is
//     Eq { eq: String },
//     Is { is: String },
//     Lt { lt: String },
//     Lte { lte: String },
//     Gt { gt: String },
//     Gte { gte: String },
//     In { r#in: Vec<String> },
// }

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum FieldOp {
    Or { or: Vec<Op> },
    And { and: Vec<Op> },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Table {
    name: String,
    #[serde(default)]
    using: Option<String>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    columns: Option<Vec<String>>,
    #[serde(default)]
    r#where: LinkedHashMap<String, Op>,
}

impl TransformExt for Parser {
    fn transform_message(
        &self,
        item: super::Message,
    ) -> Result<Option<super::Message>, super::Error> {
        match item {
            // todo: transformers should works on all kinds of message.
            Message::Raw(raw) => Ok(Some(Message::Raw(raw))),
            Message::Tables(tables) => Ok(Some(Message::Tables(tables))),
            Message::ChildTables(tables) => Ok(Some(Message::ChildTables(tables))),
            Message::Records(records) => {
                let mut new = vec![];
                for records in records {
                    let batch = self.transform_record_batch(&records.records)?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let item = MessageArrowRecords {
                        table: records.table.clone(),
                        records: batch,
                    };
                    new.push(item);
                }
                Ok(Some(Message::Records(new)))
            }
        }
    }

    fn transform_schema(
        &self,
        schema: std::sync::Arc<arrow::datatypes::Schema>,
    ) -> Result<std::sync::Arc<arrow::datatypes::Schema>, super::Error> {
        Ok(schema)
    }

    fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, super::Error> {
        self.parse(records)
    }
}
