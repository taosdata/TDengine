use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use arrow_schema::{DataType, Field};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};

use utoipa::ToSchema;

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
            .map(|value| serde_json::to_vec(value).unwrap())
            .flatten()
            .collect_vec();

        let schema = arrow::datatypes::Schema::new(
            self.input[0]
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
                    Field::new(name, dt, true)
                })
                .collect_vec(),
        );
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
}
