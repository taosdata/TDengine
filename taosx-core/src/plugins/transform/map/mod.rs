use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::{FieldRef, Fields, Schema},
    record_batch::RecordBatch,
};
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use taosx_ipc::prelude::IpcDataType;
use thiserror::Error;

use super::TransformExt;

mod constant;
mod expr;
mod format;
mod generator;
mod join;
mod sum;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Map(LinkedHashMap<String, FieldValue>);

impl TransformExt for Map {
    fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, super::Error> {
        let (fields, columns): (Vec<_>, Vec<_>) = self
            .0
            .iter()
            .map(|(name, value)| {
                let (field, array) =
                    value
                        .builder
                        .build_field(&name.as_str(), records, value.r#as.clone())?;
                Ok::<_, ValueBuilderError>((field, array))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .unzip();
        let old_schema = records.schema();
        let schema = old_schema.fields().iter().cloned().chain(fields);
        let columns = records.columns().into_iter().cloned().chain(columns);

        let schema = Schema::new(Fields::from_iter(schema));
        let columns = columns.collect::<Vec<_>>();
        let records = RecordBatch::try_new(Arc::new(schema), columns)?;
        Ok(records.clone())
    }
}

#[derive(Error, Debug)]
pub enum ValueBuilderError {
    #[error("constant error, cause: {0}")]
    ConstantError(String),
    #[error("expr error, cause: {0}")]
    ExprError(String),
    #[error("format error, cause: {0}")]
    FormatError(String),
    #[error("generator error, cause: {0}")]
    GeneratorError(String),
    #[error("join error, cause: {0}")]
    JoinError(String),
    #[error("sum error, cause: {0}")]
    SumError(String),
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
    ) -> Result<(FieldRef, ArrayRef), ValueBuilderError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    #[serde(flatten)]
    builder: FieldValueBuilder,
    r#as: Option<IpcDataType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldValueBuilder {
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
            FieldValueBuilder::Value(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Expr(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Format(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Generator(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Join(builder) => builder.build_field(name, record, r#as),
            FieldValueBuilder::Sum(builder) => builder.build_field(name, record, r#as),
        }
    }
}
