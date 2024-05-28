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
                FieldParser::Cast(c) => c.r#as().clone().arrow_data_type(),
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
        input: &Vec<LinkedHashMap<String, serde_json::Value>>,
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

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_udt_tranform() {
//         let udf = r#"let blacks = ["xxx-21", "yyy-2"];let whites = ["ddd", "ccc", "xxx-1"];
//     if (blacks.len > 0 && blacks.contains(data["DEV_ID"]) || whites.len > 0 && !whites.contains(data["DEV_ID"])) {
//         return [];
//     }

//     let result = [];
//     let share_data = #{};

//     for (k, i) in data.keys() {
//         if (k.len == 5 && (k.starts_with("U0") || k.starts_with("U1") || k.starts_with("U2"))) {
//             let item = #{"_ts": `${data["DATA_DATE"]} ${k.sub_string(1,2)}:${k.sub_string(3,2)}`, "_value": data[k]};
//             result.push(item);
//         } else if (k != "DATA_DATE") {
//             share_data.set(k, data[k]);
//         }
//     }

//     for (item, i) in result {
//         result[i] += share_data;
//     }

//     result
// "#;

//         let raw_data1 = r#"{
// "DATA_ITEM_ID": "aaa-0123456",
// "MONITOR_OBJ_TYPE": "b",
// "MONITOR_OBJ_CODE": 'c',
// "PRO_MGT_ORG_CODE": "hebei",
// "MGT_ORG_CODE": "ddd",
// "PUSH_DATE": "2024-3-20 12:23:30",
// "U2358": 18.38,
// "U2359": 219382.82827,
// "PHASE_FLAG": true,
// "DATA_POINT_FLAG": "3",
// "DATA_DATE": "2024-3-20",
// "PRODUCT_CODE": 1,
// "DEV_ID":"xxx-1",
// "TERMINAL_ID":"zzz"
// }"#;

//         let raw_data2 = r#"{
// "DATA_ITEM_ID": "aaa-0123456",
// "MONITOR_OBJ_TYPE": "b",
// "MONITOR_OBJ_CODE": 'c',
// "PRO_MGT_ORG_CODE": "hebei",
// "PUSH_DATE": "2024-3-20 12:23:30",
// "U2358": 18.38,
// "U2357": 219.382,
// "PHASE_FLAG": true,
// "DATA_POINT_FLAG": "3",
// "DATA_DATE": "2024-3-21",
// "CMD_TYPE": "eee",
// "PRODUCT_CODE": 1,
// "DEV_ID":"xxx-1",
// "TERMINAL_ID":"zzz"
// }"#;

//         let input = format!(
//             r#"{{
//     "parser":{{
//         "parse":{{
//             "payload":{{"udt": "{}"}}
//         }}
//     }},
//     "input":[{{
//         "topic":"topic",
//         "qos":"qos",
//         "payload":"{}"
//     }}, {{
//         "topic":"topic",
//         "qos":"qos",
//         "payload":"{}"
//     }}]
// }}"#,
//             udf.replace("\n", "").replace("\"", "\\\""),
//             raw_data1.replace("\n", "").replace("\"", "\\\""),
//             raw_data2.replace("\n", "").replace("\"", "\\\"")
//         );

//         let ds: DsSampleIn = serde_json::from_str(&input).unwrap();
//         let result = ds.transform(Some("Asia/Pyongyang"));
//         match result {
//             Ok(r) => {
//                 println!("result");
//             }
//             Err(e) => {
//                 println!("error: {:?}", e);
//             }
//         }
//     }

//     #[test]
//     fn test_json_tranform() {
//         let json = r#"{"a": 1}"#;

//         let input = format!(
//             r#"{{
//     "parser":{{
//         "parse":{{
//             "payload":{{"json": ""}}
//         }}
//     }},
//     "input":[{{
//         "topic":"topic",
//         "qos":"qos",
//         "payload":"{}"
//     }}]
// }}"#,
//             json.replace("\n", "").replace("\"", "\\\"")
//         );

//         let ds: DsSampleIn = serde_json::from_str(&input).unwrap();
//         let result = ds.transform(Some("Asia/Pyongyang"));
//         match result {
//             Ok(r) => {
//                 println!("result");
//             }
//             Err(e) => {
//                 println!("error: {:?}", e);
//             }
//         }
//     }
// }
