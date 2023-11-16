use std::ops::Deref;
use std::sync::Arc;

use anyhow::Context;
use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taosx_ipc::prelude::IpcDataType;

use crate::plugins::{
    expr::{BooleanExpr, Expr},
    transform::constants::{
        FIELD_NAME_TBNAME, FIELD_NAME_USING, SCOPE_COLUMN, SCOPE_PRIMARY_KEY, SCOPE_S_TABLE_NAME,
        SCOPE_TABLE_NAME, SCOPE_TAG,
    },
};

use super::constants::{META_FIELD_SCOPE, META_FIELD_TYPE};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Modeler(#[serde(deserialize_with = "model_serde::deserialize")] Vec<Table>);

#[derive(Debug, Clone)]
pub struct ModeledRecordBatch {
    pub records: RecordBatch,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
pub enum FieldScope {
    /// Sub or ordinary table name
    TableName,
    /// S(uper)Table name.
    STableName,
    /// Primary key field(must in timestamp format).
    PrimaryKey,
    /// Command field.
    Column,
    /// Tag field.
    Tag,
    /// Unspecified field.
    #[default]
    Unspecified,
}

impl FieldScope {
    pub fn new(scope: impl AsRef<str>) -> Self {
        let scope = scope.as_ref();
        match scope {
            "TableName" => Self::TableName,
            "STableName" => Self::STableName,
            "PrimaryKey" => Self::PrimaryKey,
            "Column" => Self::Column,
            "Tag" => Self::Tag,
            _ => Self::Unspecified,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModeledField {
    pub name: String,
    pub scope: FieldScope,
    pub r#type: IpcDataType,
    pub arrow_type: DataType,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModeledJsonOutput {
    pub fields: Vec<ModeledField>,
    pub columns: Vec<Vec<serde_json::Value>>,
}

impl From<&RecordBatch> for ModeledJsonOutput {
    fn from(value: &RecordBatch) -> Self {
        let fields = value
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let metadata = field.metadata();

                ModeledField {
                    name: field.name().to_string(),
                    scope: metadata
                        .get(META_FIELD_SCOPE)
                        .map(FieldScope::new)
                        .unwrap_or_default(),
                    r#type: metadata
                        .get(META_FIELD_TYPE)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(field.data_type().into()),
                    arrow_type: field.data_type().clone(),
                }
            })
            .collect();
        Self {
            fields,
            columns: arrow::json::writer::record_batches_to_json_rows(&[value])
                .unwrap()
                .into_iter()
                .map(|value| value.into_iter().map(|(_, v)| v).collect_vec())
                .collect_vec(),
        }
    }
}

impl ModeledRecordBatch {
    pub fn new(records: RecordBatch) -> Self {
        Self { records }
    }

    fn inner(&self) -> &RecordBatch {
        &self.records
    }

    pub fn into_modeled_json(&self) -> ModeledJsonOutput {
        self.inner().into()
    }
}

impl Modeler {
    pub fn apply(&self, records: &RecordBatch) -> Result<Vec<ModeledRecordBatch>, super::Error> {
        let records: Vec<_> = self
            .0
            .iter()
            .map(|table| table.apply(records))
            .try_collect()?;
        Ok(records)
    }
}

impl Deref for Modeler {
    type Target = Vec<Table>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for Modeler {
    type Item = Table;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Modeler {
    type Item = &'a Table;
    type IntoIter = std::slice::Iter<'a, Table>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Table {
    pub name: String,
    #[serde(default)]
    pub using: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default)]
    pub r#where: Option<BooleanExpr>,
}

impl Table {
    pub fn apply(&self, records: &RecordBatch) -> Result<ModeledRecordBatch, super::Error> {
        // Check if the table has at least two column.
        assert!(records.num_columns() >= 2);
        fn template_to_expr(template: &str) -> Result<Expr, super::Error> {
            if template.starts_with("`") {
                Expr::try_new(template, false)
                    .map_err(|err| super::Error::TemplateError(template.to_string(), err))
            } else {
                let name = template.replace("{", "${");
                Expr::try_new(format!("`{name}`"), false)
                    .map_err(|err| super::Error::TemplateError(template.to_string(), err))
            }
        }
        let records = if let Some(expr) = self.r#where.as_ref() {
            expr.filter(records)?
        } else {
            records.clone()
        };

        // New records
        let capacity = records.num_columns() + 2;
        let mut fields = Vec::with_capacity(capacity);
        let mut columns = Vec::with_capacity(capacity);

        let name_expr = template_to_expr(&self.name)?;
        let name_field = Field::new(FIELD_NAME_TBNAME, DataType::Utf8, false).with_metadata(
            [(META_FIELD_SCOPE.to_string(), SCOPE_TABLE_NAME.to_string())]
                .into_iter()
                .collect(),
        );
        let name_array = name_expr.eval_as(&records, DataType::Utf8)?;

        fields.push(name_field);
        columns.push(name_array);

        if let Some(using_expr) = self.using.as_deref().map(template_to_expr).transpose()? {
            let using_array = using_expr.eval_as(&records, DataType::Utf8)?;
            let using_field = Field::new(FIELD_NAME_USING, DataType::Utf8, false).with_metadata(
                [(META_FIELD_SCOPE.to_string(), SCOPE_S_TABLE_NAME.to_string())]
                    .into_iter()
                    .collect(),
            );

            fields.push(using_field);
            columns.push(using_array);
        }

        let schema = records.schema();

        if let Some(names) = self.columns.as_deref() {
            let primary = names[0].as_str();
            let timestamp = DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None);
            let primary_array = arrow::compute::cast(
                records
                    .column_by_name(primary)
                    .with_context(|| format!("Primary key `{primary}` does not exist in data"))?,
                &DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            )?;
            let primary_field = schema
                .field_with_name(primary)?
                .clone()
                .with_metadata(
                    [(META_FIELD_SCOPE.to_string(), SCOPE_PRIMARY_KEY.to_string())]
                        .into_iter()
                        .collect(),
                )
                .with_data_type(timestamp);

            fields.push(primary_field);
            columns.push(primary_array);

            for name in &names[1..] {
                let field = schema.field_with_name(name)?.clone().with_metadata(
                    [(META_FIELD_SCOPE.to_string(), SCOPE_COLUMN.to_string())]
                        .into_iter()
                        .collect(),
                );
                let column = records.column_by_name(name).unwrap().clone();

                fields.push(field);
                columns.push(column);
            }
        }

        if let Some(tags) = self.tags.as_ref() {
            for name in tags {
                let field = schema.field_with_name(name)?.clone().with_metadata(
                    [(META_FIELD_SCOPE.to_string(), SCOPE_TAG.to_string())]
                        .into_iter()
                        .collect(),
                );
                let column = records.column_by_name(name).unwrap().clone();

                fields.push(field);
                columns.push(column);
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)?;
        Ok(ModeledRecordBatch::new(batch))
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
