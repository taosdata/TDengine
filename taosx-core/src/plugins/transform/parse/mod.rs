use std::{collections::HashMap, sync::Arc};

use self::cast::Cast;
use self::join::Join;
use self::json::Json;
use self::regex::Regex;
use self::split::Split;
use self::udt::Udt;

use super::TransformExt;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Float16Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
        ListArray, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{Field, Schema},
    error::ArrowError,
    ipc::FixedSizeBinary,
    record_batch::RecordBatch,
};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod cast;
mod join;
mod json;
pub mod plugin;
mod regex;
mod split;
mod udt;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error(transparent)]
    JsonPathError(#[from] serde_json_path::ParseError),
    #[error("Deserialize json from {0} error: {1:#}")]
    JsonDeserializeError(String, serde_json::Error),
    #[error("Expect json object, got unsupported value: {0:#}")]
    UnsupportedJsonValue(serde_json::Value),
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
    #[error(transparent)]
    UdtError(#[from] rhai::EvalAltResult),
    #[error(transparent)]
    RegexError(#[from] regex::RegexError),
    #[error(transparent)]
    SplitError(#[from] split::SplitError),
    #[error("Unsupported data type: {0:?}")]
    UnsupportedDataType(arrow::datatypes::DataType),
    #[error(transparent)]
    OtherError(#[from] anyhow::Error),
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
    /// Parse an array into a record batch, returns the original array indices and the final record batch as a tuple.
    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), ParseError>;

    /// Parse the array to new array without rows/columns changed, returns a tuple with field and array data.
    fn parse_scalar(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(Field, ArrayRef), ParseError> {
        self.parse_array(field, array)
            .map(|(batch, _)| (batch.schema().field(0).clone(), batch.column(0).clone()))
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum FieldParser {
    Regex(Regex),
    Cast(Cast),
    Alias { alias: String },
    Split(Split),
    Udt(Udt),
    Join(Join),
    Plugin(plugin::ParserPlugin),
    // Json must be the last one, because it has default value. If not, other parsers will be ignored.
    Json(Json),
}

impl Parse for FieldParser {
    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), ParseError> {
        match self {
            FieldParser::Join(join) => join.parse_array(field, array),
            FieldParser::Json(json) => json.parse_array(field, array),
            FieldParser::Split(split) => split.parse_array(field, array),
            FieldParser::Udt(udt) => udt.parse_array(field, array),
            FieldParser::Cast(cast) => cast.parse_array(field, array),
            FieldParser::Regex(regex) => regex.parse_array(field, array),
            FieldParser::Plugin(plugin) => plugin.parse_array(field, array),

            FieldParser::Alias { alias } => {
                let batch = RecordBatch::try_from_iter([(alias, array.clone())])?;
                Ok((batch, None))
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ParserImpl(LinkedHashMap<String, FieldParser>);

impl ParserImpl {
    pub fn new(map: LinkedHashMap<String, FieldParser>) -> Self {
        Self(map)
    }

    // pub fn list_plugins() -> LinkedHashMap<String, FieldParser> {
    //     let mut map = LinkedHashMap::new();
    //     map.insert("regex".to_string(), FieldParser::Regex(Regex::default()));
    //     map.insert("cast".to_string(), FieldParser::Cast(Cast::default()));
    //     map.insert("alias".to_string(), FieldParser::Alias { alias: "".to_string() });
    //     map.insert("split".to_string(), FieldParser::Split(Split::default()));
    //     map.insert("udt".to_string(), FieldParser::Udt(Udt::default()));
    //     map.insert("join".to_string(), FieldParser::Join(Join::default()));
    //     map.insert("json".to_string(), FieldParser::Json(Json::default()));
    //     map
    // }
}
impl std::ops::Deref for ParserImpl {
    type Target = LinkedHashMap<String, FieldParser>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn duplicate_rows(data_array: &Arc<dyn Array>, indices: &[usize]) -> Arc<dyn Array> {
    let mut duplicated_data_array: Vec<Arc<dyn Array>> = Vec::with_capacity(indices.len());
    for i in indices {
        // 获取 data_array 中第i个数据
        // let value = data_array.value(*i);
        let value = data_array.slice(*i, 1);
        // value 转为 dyn Array
        duplicated_data_array.push(value);
    }
    let duplicated_data_array = duplicated_data_array
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<_>>();

    arrow::compute::concat(&duplicated_data_array).unwrap()
}

impl TransformExt for ParserImpl {
    fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, super::Error> {
        if self.is_empty() {
            return Ok(records.clone());
        }
        // dbg!("before parser------");
        // dbg!(records);

        let schema = records.schema();
        let metadata = schema.metadata().clone();

        let mut old_fields = vec![];
        let mut fields = vec![];
        let mut columns = vec![];

        let mut multi_fields = vec![];
        let mut multi_columns = vec![];
        let mut multi_indices = None;

        for field in schema.fields() {
            let name = field.name();
            let array = records.column_by_name(name).unwrap();

            if let Some(parser) = self.0.get(name) {
                let (batch, indices) = parser.parse_array(field, array).map_err(|error| {
                    super::Error::FieldParserError {
                        field: name.to_string(),
                        error,
                    }
                })?;
                if indices.is_some() {
                    for field in batch.schema().fields() {
                        multi_fields.push(field.clone());
                        let array = batch.column_by_name(field.name()).unwrap();
                        multi_columns.push(array.clone());
                    }
                    multi_indices = indices;
                } else {
                    for field in batch.schema().fields() {
                        fields.push(field.clone());
                        let array = batch.column_by_name(field.name()).unwrap();
                        columns.push(array.clone());
                    }
                }
            } else {
                old_fields.push(field);
            }
        }

        let (rfields, rcolumns): (Vec<_>, Vec<_>) = old_fields
            .iter()
            .map(|f| f.name().clone())
            .chain(fields.iter().map(|f| f.name().clone()))
            .chain(multi_fields.iter().map(|f| f.name().clone()))
            .unique()
            .map(|name| {
                if multi_indices.is_some() {
                    if let Some((idx, field)) =
                        multi_fields.iter().find_position(|f| name == *f.name())
                    {
                        (field.clone(), multi_columns[idx].clone())
                    } else if let Some((idx, field)) =
                        fields.iter().find_position(|f| name == *f.name())
                    {
                        (
                            field.clone(),
                            duplicate_rows(&columns[idx], multi_indices.as_ref().unwrap()),
                        )
                    } else {
                        (
                            schema.fields().find(&name).map(|(_, f)| f.clone()).unwrap(),
                            duplicate_rows(
                                records.column_by_name(&name).unwrap(),
                                multi_indices.as_ref().unwrap(),
                            ),
                        )
                    }
                } else if let Some((idx, field)) =
                    fields.iter().find_position(|f| name == *f.name())
                {
                    (field.clone(), columns[idx].clone())
                } else {
                    // dbg!(&name);
                    (
                        schema.fields().find(&name).map(|(_, f)| f.clone()).unwrap(),
                        records.column_by_name(&name).unwrap().clone(),
                    )
                }
            })
            .unzip();

        let rschema = Schema::new_with_metadata(rfields, metadata);

        let batch = RecordBatch::try_new(Arc::new(rschema), rcolumns)?;

        // dbg!("after parser------");
        // dbg!(&batch);

        Ok(batch)
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
            arrow::datatypes::DataType::List(_) => taos::Ty::VarChar,
            _ => todo!(),
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
                arrow::datatypes::DataType::List(field) => {
                    let array = self.as_any().downcast_ref::<ListArray>().unwrap();
                    let value = array.value(index);
                    let result = match field.data_type() {
                        arrow_schema::DataType::UInt8 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<UInt8Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::UInt16 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<UInt16Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::UInt32 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::UInt64 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<UInt64Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::Int8 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<Int8Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::Int16 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<Int16Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::Int32 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::Int64 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::Binary | arrow_schema::DataType::LargeBinary => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<LargeBinaryArray>()
                                .unwrap()
                                .iter()
                                .map(|v| String::from_utf8(v.unwrap_or_default().to_vec()).unwrap())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::Float32 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<Float32Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::Float64 => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        arrow_schema::DataType::Boolean => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                        _ => {
                            let vec = arrow::array::Array::as_any(&value)
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .iter()
                                .map(|v| v.unwrap_or_default())
                                .collect::<Vec<_>>();
                            serde_json::to_string(&vec).unwrap()
                        }
                    };
                    taos::Value::VarChar(result)
                }
                arrow::datatypes::DataType::Struct(_) => {
                    let array = self.as_any().downcast_ref::<StructArray>().unwrap();
                    let values: HashMap<String, taos::Value> = array
                        .fields()
                        .iter()
                        .map(|field| {
                            let array = array.column_by_name(field.name()).unwrap();
                            (field.name().clone(), array.taos_value(index))
                        })
                        .collect();
                    taos::Value::VarChar(format!("{:?}", values))
                }
                arrow::datatypes::DataType::Null => taos::Value::Null(ty),
                _ => {
                    tracing::warn!("Unsupported data type: {:?}", self.data_type());
                    taos::Value::Null(ty)
                }
            }
        }
    }

    #[allow(dead_code)] // FIXME(@huolinhe): remove this?
    fn taos_values(&self) -> Vec<taos::Value> {
        (0..self.len())
            .map(|index| self.taos_value(index))
            .collect()
    }
}

#[test]
fn test_regex() {
    let parse = r#"
    {
      "regex": "current (?<current>\\S+) with voltage (?<voltage>\\S+) and phase (?<phase>\\S+)",
      "select": ["current::float", "voltage::int", "phase::float"]
    }"#;
    let parse: FieldParser = serde_json::from_str(parse).unwrap();
    dbg!(&parse);
    assert!(matches!(parse, FieldParser::Regex(_)));
}

#[test]
fn test_join() {
    let join = r#"{
        "join": ","
    }"#;
    let join: FieldParser = serde_json::from_str(join).unwrap();
    dbg!(&join);
    assert!(matches!(join, FieldParser::Join(_)));
}
