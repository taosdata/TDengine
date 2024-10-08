use std::ops::Deref;
use std::sync::Arc;

use anyhow::Context;
use arrow::{
    array::{ArrayRef, StringArray},
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

use super::{
    constants::{META_FIELD_SCOPE, META_FIELD_TYPE},
    TableOptions,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Modeler(#[serde(deserialize_with = "model_serde::deserialize")] Vec<Table>);

impl Modeler {
    pub fn new(tables: Vec<Table>) -> Self {
        Self(tables)
    }

    pub fn table0(&self) -> Option<&Table> {
        self.0.first()
    }
}

#[derive(Debug, Clone)]
pub struct ModeledRecordBatch {
    pub records: RecordBatch,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, Default)]
pub enum FieldScope {
    /// Sub or ordinary table name
    TableName,
    /// SuperTable name.
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
    /// Modeled fields.
    pub fields: Vec<ModeledField>,
    // #[serde_as(as = "DefaultOnNull")]
    /// The order of columns is the same as the order of fields.
    pub columns: Vec<Vec<serde_json::Value>>,
}

impl From<&RecordBatch> for ModeledJsonOutput {
    fn from(value: &RecordBatch) -> Self {
        let schema = value.schema();
        let fields = schema
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
                .map(|mut value| {
                    // Keep the order and null values.
                    schema
                        .fields()
                        .iter()
                        .map(|field| value.remove(field.name()).unwrap_or_default())
                        .collect_vec()
                })
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

    pub fn to_modeled_json(&self) -> ModeledJsonOutput {
        self.inner().into()
    }

    pub fn to_modeled_json_with_tz(&self, tz: &str) -> ModeledJsonOutput {
        let schema = self.records.schema();
        let (fields, columns): (Vec<_>, Vec<_>) = (0..self.records.num_columns())
            .map(|i| {
                let field = schema.fields().get(i).unwrap();
                let column = self.records.column(i);
                if let DataType::Timestamp(unit, left_tz) = field.data_type() {
                    if left_tz.is_some() {
                        let dt = DataType::Timestamp(unit.clone(), Some(tz.to_string().into()));
                        let column = arrow::compute::cast(column, &dt).unwrap();
                        (
                            Arc::new(Field::new(field.name(), dt, field.is_nullable())),
                            column,
                        )
                    } else {
                        let dt = DataType::Timestamp(unit.clone(), Some("UTC".into()));
                        let column = arrow::compute::cast(column, &dt).unwrap();
                        let dt = DataType::Timestamp(unit.clone(), Some(tz.to_string().into()));
                        let column = arrow::compute::cast(&column, &dt).unwrap();
                        (
                            Arc::new(Field::new(field.name(), dt, field.is_nullable())),
                            column,
                        )
                    }
                } else {
                    (field.clone(), column.clone())
                }
            })
            .unzip();

        let records = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

        (&records).into()
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
    #[serde(default, with = "once_lock_serde")]
    pub global: std::sync::OnceLock<Arc<TableOptions>>,
}
mod once_lock_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<std::sync::OnceLock<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        let t = Option::<T>::deserialize(deserializer)?;
        let v = std::sync::OnceLock::new();
        if let Some(t) = t {
            let _ = v.set(t);
        }
        Ok(v)
    }

    pub fn serialize<T, S>(value: &std::sync::OnceLock<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
    {
        value.get().serialize(serializer)
    }
}

fn template_to_expr(template: &str) -> Result<Expr, super::Error> {
    if template.starts_with("`") {
        Expr::try_new(template, false)
            .map_err(|err| super::Error::TemplateError(template.to_string(), err))
    } else {
        let name = template.replace("{", "${").replace("$$", "$");
        Expr::try_new(format!("`{name}`"), false)
            .map_err(|err| super::Error::TemplateError(template.to_string(), err))
    }
}

impl Table {
    pub fn eval_table_name(&self, records: &RecordBatch) -> Result<StringArray, super::Error> {
        let name_expr = template_to_expr(&self.name)?;
        let name_array = name_expr.eval_as(records, DataType::Utf8)?;
        let name_array = name_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        Ok(name_array)
    }

    pub fn apply(&self, records: &RecordBatch) -> Result<ModeledRecordBatch, super::Error> {
        // Check if the table has at least two column.
        assert!(records.num_columns() >= 2);
        if self.name.is_empty() {
            return Err(super::Error::EmptyTableName);
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

        fields.push(name_field);
        let name_array = name_expr.eval_as(&records, DataType::Utf8)?;
        if let Some(global) = self.global.get() {
            let name_array = name_array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .into_iter()
                .map(|s| s.map(|s| global.canonical_table_name(s)));
            let name_array = Arc::new(StringArray::from_iter(name_array)) as ArrayRef;
            columns.push(name_array);
        } else {
            columns.push(name_array);
        }
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
            if names.is_empty() {
                return Err(super::Error::EmptyTableColumns(self.name.clone()));
            }
            let primary = names[0].as_str();

            let timestamp = schema
                .field_with_name(primary)
                .map(|f| {
                    if let DataType::Timestamp(unit, tz) = f.data_type() {
                        DataType::Timestamp(
                            unit.clone(),
                            if tz.is_some() {
                                tz.clone()
                            } else {
                                Some("UTC".into())
                            },
                        )
                    } else {
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into()))
                    }
                })
                .unwrap_or_else(|_| {
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into()))
                });

            // Primary key column.
            let primary_array = records
                .column_by_name(primary)
                .with_context(|| format!("Primary key `{primary}` does not exist in data"))?;

            if primary_array.null_count() > 0 {
                return Err(super::Error::NullPrimaryKey(primary.to_string()));
            }
            // Cast primary key column to timestamp.
            let primary_array = arrow_cast_guess_precision::cast(&primary_array, &timestamp)
                .map_err(|err| super::Error::PrimaryKeyCastError(self.name.clone(), err))?;
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
                let field = schema.field_with_name(name)?;
                let mut metadata = field.metadata().clone();
                metadata.insert(META_FIELD_SCOPE.to_string(), SCOPE_COLUMN.to_string());
                let field = Field::new(name, field.data_type().clone(), field.is_nullable())
                    .with_metadata(metadata);

                let column = records.column_by_name(name).unwrap().clone();

                fields.push(field);
                columns.push(column);
            }
        }

        if let Some(tags) = self.tags.as_ref() {
            for name in tags {
                let field = schema.field_with_name(name)?;
                let mut metadata = field.metadata().clone();
                metadata.insert(META_FIELD_SCOPE.to_string(), SCOPE_TAG.to_string());
                let field = Field::new(name, field.data_type().clone(), field.is_nullable())
                    .with_metadata(metadata);
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
    use serde::{Deserialize, Deserializer};

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

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, TimestampMillisecondArray};

    use super::*;

    #[test]
    fn test_serde() {
        let model = r#"[
            {
                "name": "table",
                "columns": ["time", "value"]
            }
        ]"#;
        let model: Modeler = serde_json::from_str(model).unwrap();
        dbg!(&model);

        let model = r#"{
            "name": "table",
            "using": "using",
            "tags": ["tag"],
            "columns": ["time", "value"]
        }"#;

        let model: Modeler = serde_json::from_str(model).unwrap();
        dbg!(&model);

        let model = r#"{
            "name": "{topic}",
            "using": "mqtt",
            "tags": ["topic"],
            "columns": ["ts", "value", "qos"]
        }"#;
        let model: Modeler = serde_json::from_str(model).unwrap();
        dbg!(&model);
    }

    #[test]
    fn test_into_modeled_json_with_tz() {
        let schema = Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]);
        let records = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(TimestampMillisecondArray::from_iter(vec![
                    Some(1619740800000),
                    Some(1619740800000),
                ])) as ArrayRef,
                Arc::new(Float64Array::from_iter(vec![Some(1.0), Some(2.0)])),
            ],
        )
        .unwrap();

        let modeled = ModeledRecordBatch::new(records);
        let output = modeled.to_modeled_json_with_tz("UTC");
        assert_eq!(
            output.columns,
            vec![
                vec![
                    serde_json::Value::String("2021-04-30T00:00:00Z".to_string()),
                    serde_json::Value::Number(serde_json::Number::from_f64(1.0).unwrap())
                ],
                vec![
                    serde_json::Value::String("2021-04-30T00:00:00Z".to_string()),
                    serde_json::Value::Number(serde_json::Number::from_f64(2.0).unwrap())
                ]
            ]
        );
        let output = modeled.to_modeled_json_with_tz("Asia/Shanghai");
        assert_eq!(
            output.columns,
            vec![
                vec![
                    serde_json::Value::String("2021-04-30T08:00:00+08:00".to_string()),
                    serde_json::Value::Number(serde_json::Number::from_f64(1.0).unwrap()),
                ],
                vec![
                    serde_json::Value::String("2021-04-30T08:00:00+08:00".to_string()),
                    serde_json::Value::Number(serde_json::Number::from_f64(2.0).unwrap())
                ]
            ]
        );
    }
}
