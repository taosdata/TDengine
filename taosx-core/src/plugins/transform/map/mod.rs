use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{FieldRef, Schema},
    record_batch::RecordBatch,
};
use arrow_schema::{ArrowError, Field};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use taosx_ipc::prelude::IpcDataType;
use thiserror::Error;

use super::{constants::META_FIELD_TYPE, TransformExt};

mod cast;
mod constant;
pub(crate) mod expr;
mod format;
mod generator;
mod join;
mod sum;
mod timestamp;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Map(LinkedHashMap<String, FieldValue>);

impl Map {
    pub fn new(map: LinkedHashMap<String, FieldValue>) -> Self {
        Self(map)
    }
}

impl TransformExt for Map {
    fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, super::Error> {
        let (fields, columns): (Vec<_>, Vec<_>) = self
            .0
            .iter()
            .map(|(name, value)| {
                let (field, array) =
                    value
                        .builder
                        .build_field(name.as_str(), records, value.r#as.clone())?;
                Ok::<_, ValueBuilderError>((field, array))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .unzip();
        let old_schema = records.schema();
        let (fields, columns): (Vec<_>, Vec<_>) = old_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .chain(fields.iter().map(|f| f.name().clone()))
            .unique()
            .map(|name| {
                if let Some((idx, field)) =
                    fields.iter().find_position(|field| name == *field.name())
                {
                    (field.clone(), columns[idx].clone())
                } else {
                    (
                        old_schema
                            .fields()
                            .find(&name)
                            .map(|(_, f)| f.clone())
                            .unwrap(),
                        records.column_by_name(&name).unwrap().clone(),
                    )
                }
            })
            .unzip();
        let schema = Schema::new(fields);
        let records = RecordBatch::try_new(Arc::new(schema), columns)?;
        Ok(records.clone())
    }
}

#[derive(Error, Debug)]

pub enum ValueBuilderError {
    #[error("constant error, cause: {0}")]
    Constant(String),
    #[error("expr error, cause: {0}")]
    Expr(String),
    #[error("format error, cause: {0}")]
    Format(String),
    #[error("generator error, cause: {0}")]
    Generator(String),
    #[error("join error, cause: {0}")]
    Join(String),
    #[error("sum error, cause: {0}")]
    Sum(String),
    #[error("datatype cast error, cause: {0}")]
    Cast(ArrowError),
}

/// ValueBuilder is used to build a new column from a record batch.
///
/// The result is a new record batch with the new column.
///
trait ValueBuilder {
    fn build_field(
        &self,
        name: &str,
        record: &RecordBatch,
        r#as: Option<IpcDataType>,
    ) -> Result<(FieldRef, ArrayRef), ValueBuilderError> {
        let array = self.build_from(record)?;

        if let Some(ty) = r#as {
            let mut m = HashMap::new();
            m.insert(META_FIELD_TYPE.to_string(), ty.to_string());
            m.insert("cast_from".to_string(), array.data_type().to_string());
            match &ty {
                IpcDataType::VarChar(len) | IpcDataType::NChar(len) => {
                    m.insert("length".to_string(), len.to_string());
                    m.insert("cast_to".to_string(), ty.ty().name().to_string());
                }
                IpcDataType::Json => {
                    m.insert("cast_to".to_string(), ty.ty().name().to_string());
                }
                _ => (),
            }
            let ty = ty.arrow_data_type();
            let array =
                arrow_cast_guess_precision::cast(&array, &ty).map_err(ValueBuilderError::Cast)?;
            Ok((Arc::new(Field::new(name, ty, true).with_metadata(m)), array))
        } else {
            Ok((
                Arc::new(Field::new(name, array.data_type().clone(), true)),
                array,
            ))
        }
    }

    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    #[serde(flatten)]
    builder: FieldValueBuilder,
    r#as: Option<IpcDataType>,
}

impl FieldValue {
    pub fn new(builder: FieldValueBuilder, r#as: Option<IpcDataType>) -> Self {
        Self { builder, r#as }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldValueBuilder {
    Cast(cast::CastValueBuilder),
    Value(constant::ConstantValueBuilder),
    Expr(expr::ExprValueBuilder),
    Format(format::FormatValueBuilder),
    Generator(generator::GeneratorValueBuilder),
    Join(join::JoinValueBuilder),
    Sum(sum::SumValueBuilder),
}

impl ValueBuilder for FieldValueBuilder {
    fn build_field(
        &self,
        name: &str,
        record: &RecordBatch,
        r#as: Option<IpcDataType>,
    ) -> Result<(FieldRef, ArrayRef), ValueBuilderError> {
        match self {
            FieldValueBuilder::Cast(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Value(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Expr(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Format(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Generator(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Join(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Sum(builder) => builder.build_field(name, record, r#as),
        }
    }

    fn build_from(&self, record: &RecordBatch) -> Result<ArrayRef, ValueBuilderError> {
        match self {
            FieldValueBuilder::Cast(builder) => builder.build_from(record),
            FieldValueBuilder::Value(builder) => builder.build_from(record),
            FieldValueBuilder::Expr(builder) => builder.build_from(record),
            FieldValueBuilder::Format(builder) => builder.build_from(record),
            FieldValueBuilder::Generator(builder) => builder.build_from(record),
            FieldValueBuilder::Join(builder) => builder.build_from(record),
            FieldValueBuilder::Sum(builder) => builder.build_from(record),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray, TimestampNanosecondArray};

    #[test]
    fn test_sum() {
        let map: Map =
            serde_json::from_str(r#"{"col_new_sum":{"sum":["a","c"],"as":"INT"}}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let result = map.transform_record_batch(&batch);
        dbg!(&result);
        let arr = result.unwrap();
        let arr = arr
            .column_by_name("col_new_sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>();
        dbg!(arr);
    }

    #[test]
    fn test_cast() {
        let map: Map = serde_json::from_str(r#"{"col_new_cast":{"cast": "b"}}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![7, 8, 9])) as ArrayRef),
        ])
        .unwrap();

        let result = map.transform_record_batch(&batch);
        dbg!(&result);
        let arr = result.unwrap();
        let arr = arr
            .column_by_name("col_new_cast")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>();
        dbg!(arr);
    }

    #[test]
    fn test_cast_field_not_found() {
        let map: Map = serde_json::from_str(r#"{"col_new_cast":{"cast": "d"}}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![7, 8, 9])) as ArrayRef),
        ])
        .unwrap();

        let result = map.transform_record_batch(&batch);
        dbg!(&result);
        let arr = result.unwrap();
        let arr = arr
            .column_by_name("col_new_cast")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>();
        dbg!(arr);
    }

    #[test]
    fn test_constant() {
        let map: Map = serde_json::from_str(r#"{"col_new_constant":{"value":"str"}}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let result = map.transform_record_batch(&batch);
        dbg!(&result);
        let arr = result.unwrap();
        let arr = arr
            .column_by_name("col_new_constant")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>();
        dbg!(arr);
    }

    #[test]
    fn test_expr() {
        let map: Map = serde_json::from_str(r#"{"col_new_expr":{"expr":"a + b * c"}}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let result = map.transform_record_batch(&batch);
        dbg!(&result);
        let arr = result.unwrap();
        let arr = arr
            .column_by_name("col_new_expr")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>();
        dbg!(arr);
    }

    #[test]
    fn test_format() {
        let map: Map =
            serde_json::from_str(r#"{"col_new_format":{"format": "${a}-${b}"}}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let result = map.transform_record_batch(&batch);
        dbg!(&result);
        let arr = result.unwrap();
        let arr = arr
            .column_by_name("col_new_format")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>();
        dbg!(arr);
    }

    #[test]
    fn test_generator() {
        let map: Map =
            serde_json::from_str(r#"{"col_new_generator":{"generator":"now"}}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let result = map.transform_record_batch(&batch);
        dbg!(&result);
        let arr = result.unwrap();
        let arr = arr
            .column_by_name("col_new_generator")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>();
        dbg!(arr);
    }

    #[test]
    fn test_join() {
        let map: Map =
            serde_json::from_str(r#"{"col_new_join":{"join":["a","b"],"with":"&&"}}"#).unwrap();
        let batch = RecordBatch::try_from_iter([
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("c", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ])
        .unwrap();

        let result = map.transform_record_batch(&batch);
        dbg!(&result);
        let arr = result.unwrap();
        let arr = arr
            .column_by_name("col_new_join")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>();
        dbg!(arr);
    }
}
