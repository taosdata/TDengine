use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use arrow_schema::{DataType, Field};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

use crate::plugins::transform::parse::{FieldParser, ParserImpl};
use utoipa::ToSchema;

use super::to_json_valid_batches;

/// Sample data input with transform pipeline.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[schema(example = r#"
{
  "parser": {
    "parse": { "payload": { "json": ["value::double", "id::int"] } },
    "mutate": [{ "filter": "value > 1.2" }],
    "model": [
      {
        "name": "d{id}",
        "using": "meters",
        "tags": ["id"],
        "columns": ["ts", "value"]
      }
    ]
  },
  "input": [
    { "ts": "2023-11-16T00:00:00Z", "payload": "{\"value\":1.4, \"id\": 1}" },
    { "ts": "2023-11-16T00:00:01Z", "payload": "{\"value\":1.4, \"id\": 2}" }
  ]
}
"#)]
pub struct DsSampleIn {
    /// Transform pipeline definition.
    parser: crate::Pipeline,
    /// Sample data input, an array of object.
    input: Vec<LinkedHashMap<String, serde_json::Value>>,
}

impl DsSampleIn {
    pub fn transform(&self, tz: Option<&str>) -> anyhow::Result<impl Serialize> {
        if self.input.is_empty() {
            anyhow::bail!("Input should not be empty");
        }

        let json = self
            .input
            .iter()
            .flat_map(|value| serde_json::to_vec(value).unwrap())
            .collect_vec();

        let schema = self.to_schema()?;

        let mut reader = arrow::json::reader::ReaderBuilder::new(Arc::new(schema))
            .build(json.as_slice())
            .context("Could not build record reader from json stream")?;
        let batch = reader.next().unwrap()?;

        let output = self.parser.transform(&batch)?;

        if let Some(tz) = tz {
            let _ = arrow::array::timezone::Tz::from_str(tz).context("Invalid timezone")?;
            return Ok(output
                .iter()
                .map(|batch| batch.to_modeled_json_with_tz(tz))
                .collect_vec());
        }

        let output = output
            .iter()
            .map(|batch| batch.to_modeled_json())
            .collect_vec();
        Ok(output)
    }

    pub fn stable_preview(&self) -> anyhow::Result<impl Serialize> {
        if self.input.is_empty() {
            anyhow::bail!("Input should not be empty");
        }

        let json = self
            .input
            .iter()
            .flat_map(|value| serde_json::to_vec(value).unwrap())
            .collect_vec();

        let schema = self.to_schema()?;

        let mut reader = arrow::json::reader::ReaderBuilder::new(Arc::new(schema))
            .build(json.as_slice())
            .context("Could not build record reader from json stream")?;
        let batch = reader.next().unwrap()?;

        let batch = self.parser.transform_records(&batch)?;

        let json_batches = to_json_valid_batches(&[batch]);

        let Some(records) = json_batches.first() else {
            return Ok(vec![]);
        };

        let stables = self
            .parser
            .s_model
            .as_ref()
            .map(|s| s.apply(records, &self.parser.global))
            .transpose()?
            .context("stable model not found")?;

        Ok(stables.values().cloned().collect::<Vec<_>>())
    }

    pub fn to_schema(&self) -> anyhow::Result<arrow::datatypes::Schema> {
        let schema = match &self.parser.parse {
            None => None,
            Some(parse) => Self::to_schema_by_parse(parse),
        };

        let schema = Self::to_schema_by_first_input(&self.input, schema);

        if schema.is_none() {
            anyhow::bail!("Could not infer schema from sample data");
        }

        Ok(schema.unwrap())
    }

    fn to_schema_by_parse(parse: &ParserImpl) -> Option<arrow::datatypes::Schema> {
        let mut fields = Vec::new();
        for (name, field_parser) in parse.deref() {
            let data_type = match field_parser {
                FieldParser::Cast(c) => {
                    let arrow_dt = c.r#as().clone().arrow_data_type();
                    match arrow_dt {
                        // sample 接口使用 json reader 不支持 binary 类型
                        DataType::Binary => {
                            DataType::List(Arc::new(Field::new(name, DataType::UInt8, true)))
                        }
                        _ => arrow_dt,
                    }
                }
                _ => {
                    tracing::warn!(
                        "Could not infer data type for field {}, use DataType::Utf8",
                        name
                    );
                    DataType::Utf8
                }
            };
            fields.push(Field::new(name, data_type, true));
        }

        Some(arrow::datatypes::Schema::new(fields))
    }

    fn to_schema_by_first_input(
        input: &[LinkedHashMap<String, serde_json::Value>],
        schema: Option<arrow::datatypes::Schema>,
    ) -> Option<arrow::datatypes::Schema> {
        if input.is_empty() {
            return None;
        }

        let fields = input[0]
            .iter()
            .map(|(name, value)| {
                let dt = match value {
                    serde_json::Value::Null
                    | serde_json::Value::String(_)
                    | serde_json::Value::Object(_)
                    | serde_json::Value::Array(_) // array/object is not supported actually
                    => DataType::Utf8,
                    serde_json::Value::Bool(_) => DataType::Boolean,
                    serde_json::Value::Number(num) => {
                        if num.is_u64() {
                            DataType::UInt64
                        } else if num.is_f64() {
                            DataType::Float64
                        } else {
                            DataType::Int64
                        }
                    }
                };
                let dt = match &schema {
                    Some(schema) => schema
                        .field_with_name(name)
                        .map(|f| f.data_type())
                        .unwrap_or(&dt),
                    None => &dt,
                };
                Field::new(name, dt.clone(), true)
            })
            .collect_vec();

        Some(arrow::datatypes::Schema::new(fields))
    }
}
