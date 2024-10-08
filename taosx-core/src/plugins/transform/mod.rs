//! Transform is the bridge of messages from data source and sink
//!
//! A transform will have some specific effect such as:
//! - Extract: one field into several fields.
//! - Select: select specific fields by name or conditions.
//! - Mutate: adds new variables that are functions of existing variables.
//! - Mutate: rebuild one or more fields by existing fields.
//! - Convert: fields data into other data types.
//! - Filter: filter one or more rows in the stream.

use std::{
    borrow::{Borrow, Cow},
    ops::Range,
    str::FromStr,
    sync::Arc,
};

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use either::Either;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::{
    taos_query::{
        common::Describe,
        helpers::{ColumnMeta, Described},
    },
    JsonMeta, RawBlock, Ty, Value,
};
use thiserror::Error;
use tinytemplate::TinyTemplate;

pub use select::Select;
use taosx_ipc::prelude::IpcDataType;

use crate::plugins::transform::parse::ArrayForTaos;

use super::expr;

use self::{
    modeler::{ModeledRecordBatch, Modeler},
    mutate::Mutate,
    parse::{FieldParser, ParserImpl},
};

pub(crate) mod select;

// mod json;
pub mod constants;

pub mod parse;

pub(crate) mod filter;

pub(crate) mod map;
pub(crate) mod modeler;
pub(crate) mod mutate;
pub mod sample;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Pipeline {
    #[serde(default)]
    global: Arc<TableOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parse: Option<ParserImpl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    mutate: Vec<Mutate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<Modeler>,
}

impl Pipeline {
    pub fn transform(&self, records: &RecordBatch) -> Result<Vec<ModeledRecordBatch>, Error> {
        self.check()?;
        let batch = self
            .parse
            .as_ref()
            .map(|parse| parse.transform_record_batch(records))
            .transpose()?;

        let batch = batch.unwrap_or_else(|| records.clone());
        let batch = self.mutate.iter().fold(Ok(batch), |batch, mutate| {
            batch.and_then(|batch| mutate.transform_record_batch(&batch))
        })?;
        if let Some(model) = self.model.as_ref() {
            model.apply(&batch)
        } else {
            Ok(vec![ModeledRecordBatch::new(batch)])
        }
    }

    fn check(&self) -> Result<(), Error> {
        if self.mutate.is_empty() && self.parse.is_none() {
            Err(anyhow::anyhow!(
                "Either parse or mutate must be set in pipeline"
            ))?;
        }
        if let Some(model) = self.model.as_ref() {
            for table in model {
                if table.name.is_empty() {
                    return Err(Error::EmptyTableName);
                } else if table.name.contains('.') {
                    return Err(Error::TableNameContainsDot(table.name.clone()));
                }
                table.global.get_or_init(|| self.global.clone());

                if let Some(columns) = table.columns.as_ref() {
                    if columns.is_empty() {
                        return Err(Error::EmptyTableColumns(table.name.clone()));
                    }
                    if let Some(dup) = columns.iter().duplicates().next() {
                        return Err(Error::DuplicatedColumns(dup.clone()));
                    }
                }

                if let Some(tags) = table.tags.as_ref() {
                    if table.using.as_ref().is_none() {
                        return Err(Error::STableNameRequired);
                    }
                    if let Some(dup) = tags.iter().duplicates().next() {
                        return Err(Error::DuplicatedTags(dup.clone()));
                    }
                }
                if let Some(stable) = table.using.as_ref() {
                    if stable.is_empty() {
                        return Err(Error::EmptySTableName);
                    } else if stable.contains('.') {
                        return Err(Error::STableNameContainsDot(stable.clone()));
                    } else if table.tags.as_ref().map(Vec::is_empty).unwrap_or(true) {
                        return Err(Error::STableTagsRequired);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod pipeline_tests {
    use std::{ops::Sub, time::Duration};

    use arrow::array::ArrayRef;
    use arrow::datatypes::TimeUnit;

    use super::*;

    #[test]
    fn test_expr_functions() {
        let records = demo_mqtt_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double", "value", "$.id=id::int"] } }
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser and mutate
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int"] } },
            "mutate": [
                {"map": { "v0": { "value": "ssstr" } } },
                {"map": {
                    "e1": { "expr": "v0.append(\"abc\")" },
                    "e2": { "expr": "v0.replace(\"s\", \"a\")" },
                    "e3": { "expr": "v0.replace(\"s\", \"a\", 1)" },
                    "e4": { "expr": "v0.truncate(1)" }
            } } ]
        }"#
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone()))
                .collect_vec(),
            vec![
                ("ts", DataType::Timestamp(TimeUnit::Millisecond, None)),
                ("price", DataType::Float64),
                ("value", DataType::Float64),
                ("id", DataType::Int32),
                ("v0", DataType::Utf8),
                ("e1", DataType::Utf8),
                ("e2", DataType::Utf8),
                ("e3", DataType::Utf8),
                ("e4", DataType::Utf8)
            ]
        );
    }

    #[test]
    fn test_pipeline_map() {
        let records = demo_mqtt_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double", "value", "$.id=id::int"] } }
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser and mutate
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int"] } },
            "mutate": [{"map": {
                "c1": { "value": 2, "as": "int" },
                "g1": { "generator": "now" },
                "f1": { "format": "format-${value}-suffix", "as": "varchar" },
                "e1": { "expr": "if value > 1 { true } else { false }" },
                "e2": { "expr": "value + 2" }
            }}]
        }"#
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone()))
                .collect_vec(),
            vec![
                ("ts", DataType::Timestamp(TimeUnit::Millisecond, None)),
                ("price", DataType::Float64),
                ("value", DataType::Float64),
                ("id", DataType::Int32),
                ("c1", DataType::Int32),
                (
                    "g1",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
                ),
                ("f1", DataType::Utf8),
                ("e1", DataType::Boolean),
                ("e2", DataType::Float64)
            ]
        );

        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // over write previous value.
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int"] } },
            "mutate": [{"map": {
                "c1": { "value": 2, "as": "int" },
                "g1": { "generator": "now" },
                "f1": { "format": "format-${value}-suffix", "as": "varchar" },
                "e1": { "expr": "if value > 1 { true } else { false }" },
                "e2": { "expr": "value + 2" }
            }}, {"map": {
                "c1": { "value": 4, "as": "int" },
                "g1": { "generator": "now" },
                "f1": { "format": "prefix-${value}-suffix", "as": "varchar" },
                "e1": { "expr": "if value > 1 { 1 } else { 2 }" },
                "e2": { "expr": "value + 4" }
            }}]
        }"#
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output_over_written = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        assert_eq!(
            output_over_written[0]
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect_vec(),
            vec!["ts", "price", "value", "id", "c1", "g1", "f1", "e1", "e2"]
        );
        assert_eq!(output_over_written[0].columns[0][4].as_i64().unwrap(), 4); // c1
        assert_eq!(
            output_over_written[0].columns[0][6].as_str().unwrap(),
            "prefix-1.1-suffix"
        ); // f1
        assert_eq!(output_over_written[0].columns[0][7].as_i64().unwrap(), 1); // e1, bool
        assert_eq!(output_over_written[0].columns[0][8].as_f64().unwrap(), 5.1); // e2, double
        let json_over_written = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json_over_written);

        // With parser, mutate and model
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int"] } },
            "mutate": [{"map": {
                "c1": { "value": 2, "as": "int" },
                "g1": { "generator": "now" },
                "f1": { "format": "format-${value}-suffix", "as": "varchar" },
                "e1": { "expr": "if value > 1 { true } else { false }" },
                "e2": { "expr": "value + 2" }
            }}],
            "model": [{
                "name": "d{id}",
                "using": "meters",
                "tags": ["id"],
                "columns": ["ts", "value", "price", "c1", "g1", "f1", "e1", "e2"]
            }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_pipeline_map_with_null_column() {
        let records = demo_mqtt_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double", "value", "$.id=id::int"] } }
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser and mutate
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int"] } },
            "mutate": [{"map": {
                "c1": { "value": 2, "as": "int" },
                "g1": { "generator": "now" },
                "f1": { "format": "format-${value}-suffix", "as": "varchar" },
                "e1": { "expr": "if value > 1 { true } else { false }" },
                "e2": { "expr": "value + 2" },
                "n1": { "cast": "value", "as": "timestamp" }
            }}]
        }"#
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone()))
                .collect_vec(),
            vec![
                ("ts", DataType::Timestamp(TimeUnit::Millisecond, None)),
                ("price", DataType::Float64),
                ("value", DataType::Float64),
                ("id", DataType::Int32),
                ("c1", DataType::Int32),
                (
                    "g1",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
                ),
                ("f1", DataType::Utf8),
                ("e1", DataType::Boolean),
                ("e2", DataType::Float64),
                ("n1", DataType::Timestamp(TimeUnit::Millisecond, None))
            ]
        );

        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // over write previous value.
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int"] } },
            "mutate": [{"map": {
                "c1": { "value": 2, "as": "int" },
                "g1": { "generator": "now" },
                "f1": { "format": "format-${value}-suffix", "as": "varchar" },
                "e1": { "expr": "if value > 1 { true } else { false }" },
                "e2": { "expr": "value + 2" }
            }}, {"map": {
                "c1": { "value": 4, "as": "int" },
                "g1": { "generator": "now" },
                "f1": { "format": "prefix-${value}-suffix", "as": "varchar" },
                "e1": { "expr": "if value > 1 { 1 } else { 2 }" },
                "e2": { "expr": "value + 4" }
            }}]
        }"#
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output_over_written = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        assert_eq!(
            output_over_written[0]
                .fields
                .iter()
                .map(|f| f.name.as_str())
                .collect_vec(),
            vec!["ts", "price", "value", "id", "c1", "g1", "f1", "e1", "e2"]
        );
        assert_eq!(output_over_written[0].columns[0][4].as_i64().unwrap(), 4); // c1
        assert_eq!(
            output_over_written[0].columns[0][6].as_str().unwrap(),
            "prefix-1.1-suffix"
        ); // f1
        assert_eq!(output_over_written[0].columns[0][7].as_i64().unwrap(), 1); // e1, bool
        assert_eq!(output_over_written[0].columns[0][8].as_f64().unwrap(), 5.1); // e2, double
        let json_over_written = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json_over_written);

        // With parser, mutate and model
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int"] } },
            "mutate": [{"map": {
                "c1": { "value": 2, "as": "int" },
                "g1": { "generator": "now" },
                "f1": { "format": "format-${value}-suffix", "as": "varchar" },
                "e1": { "expr": "if value > 1 { true } else { false }" },
                "e2": { "expr": "value + 2" }
            }}],
            "model": [{
                "name": "d{id}",
                "using": "meters",
                "tags": ["id"],
                "columns": ["ts", "value", "price", "c1", "g1", "f1", "e1", "e2"]
            }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_pipeline_json_array() {
        let records = demo_mqtt_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price", "value", "$.id=id::int", "$.null=null"] } }
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser and mutate
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int", "$.null=null"] } },
            "mutate": [{ "filter": "value > 1.2" }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser, mutate and model
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["$.events[0].price=price::double","value", "$.id=id::int", "$.null=null"] } },
            "mutate": [{ "filter": "value > 1.2" }],
            "model": [{
                "name": "d{id}",
                "using": "meters",
                "tags": ["id"],
                "columns": ["ts", "value", "price"]
            }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);
    }
    #[test]
    fn test_pipeline_empty_input() {
        let records = demo_mqtt_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } }
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser and mutate
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } },
            "mutate": [{ "filter": "value > 1.2" }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser, mutate and model
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } },
            "mutate": [{ "filter": "value > 1.2" }],
            "model": [{
                "name": "d{id}",
                "using": "meters",
                "tags": ["id"],
                "columns": ["ts", "value"]
            }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_split() {
        let records = demo_text_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "text": { "split": {"sep": ",", "names": ["name","value","id", "price"] } } }
        }"#,
        )
        .unwrap();
        dbg!(&pipeline);
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();

        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone()))
                .collect_vec(),
            vec![
                ("ts", DataType::Timestamp(TimeUnit::Millisecond, None)),
                ("name", DataType::Utf8),
                ("value", DataType::Utf8),
                ("id", DataType::Utf8),
                ("price", DataType::Utf8),
            ]
        );
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "text": { "split": {"sep": ",", "n": 4 } } }
        }"#,
        )
        .unwrap();
        dbg!(&pipeline);
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();

        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone()))
                .collect_vec(),
            vec![
                ("ts", DataType::Timestamp(TimeUnit::Millisecond, None)),
                ("text_0", DataType::Utf8),
                ("text_1", DataType::Utf8),
                ("text_2", DataType::Utf8),
                ("text_3", DataType::Utf8),
            ]
        );
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_regex() {
        let records = demo_text_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "text": { "regex": "\\d{1}" } }
        }"#,
        )
        .unwrap();
        dbg!(&pipeline);
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();

        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone()))
                .collect_vec(),
            vec![
                ("ts", DataType::Timestamp(TimeUnit::Millisecond, None)),
                ("text", DataType::Utf8),
            ]
        );
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "text": { "regex": "(\\d{1}).*" } }
        }"#,
        )
        .unwrap();
        dbg!(&pipeline);
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();

        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone()))
                .collect_vec(),
            vec![
                ("ts", DataType::Timestamp(TimeUnit::Millisecond, None)),
                ("text0", DataType::Utf8),
                ("text1", DataType::Utf8),
            ]
        );
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);
    }

    #[test]
    fn test_pipeline() {
        let records = demo_mqtt_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } }
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser and mutate
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } },
            "mutate": [{ "filter": "value > 1.2" }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser, mutate and model
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } },
            "mutate": [{ "filter": "value > 1.2" }],
            "model": [{
                "name": "d{id}",
                "using": "meters",
                "tags": ["id"],
                "columns": ["ts", "value"]
            }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);
    }

    /// https://jira.taosdata.com:18080/browse/TD-27751
    #[test]
    fn test_nchar() {
        let records = demo_mqtt_records();

        // With parser only
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } }
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);

        // With parser and mutate
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } },
            "mutate": [
                { "filter": "value > 1.2" },
                { "map": {
                    "m1": { "cast": "value", "as": "nchar(256)" },
                    "m2": { "expr": "\"abc - \" + value", "as": "nchar(128)" },
                    "m3": { "format": "${id}-${value}", "as": "nchar(128)" },
                    "m4": { "sum": ["value", "value"], "as": "nchar(128)" },
                    "m5": { "join": ["value", "value"], "with": "-", "as": "nchar(128)" }
                } }
            ]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();
        let json = serde_json::to_string_pretty(&output).unwrap();

        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone(), f.r#type.clone()))
                .collect_vec(),
            vec![
                (
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    IpcDataType::Timestamp(TimeUnit::Millisecond)
                ),
                ("value", DataType::Float64, IpcDataType::Float64),
                ("id", DataType::Int32, IpcDataType::Int32),
                ("m1", DataType::Utf8, IpcDataType::NChar(256)),
                ("m2", DataType::Utf8, IpcDataType::NChar(128)),
                ("m3", DataType::Utf8, IpcDataType::NChar(128)),
                ("m4", DataType::Utf8, IpcDataType::NChar(128)),
                ("m5", DataType::Utf8, IpcDataType::NChar(128)),
            ]
        );
        println!("{}", json);

        // With parser, mutate and model
        let pipeline: Pipeline = serde_json::from_str(
            r#"{
            "parse": { "payload": { "json": ["value::double", "id::int"] } },
            "mutate": [
                { "filter": "value > 1.2" },
                { "map": {
                    "m1": { "cast": "value", "as": "nchar(256)" },
                    "m2": { "expr": "\"abc - \" + value", "as": "nchar(128)" }
                } }
            ],
            "model": [{
                "name": "d{id}",
                "using": "meters",
                "tags": ["id"],
                "columns": ["ts", "value", "m1", "m2"]
            }]
        }"#,
        )
        .unwrap();
        let res = pipeline.transform(&records).unwrap();
        dbg!(&res);
        let output = res.iter().map(|m| m.to_modeled_json()).collect_vec();

        assert_eq!(
            output[0]
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.arrow_type.clone(), f.r#type.clone()))
                .collect_vec(),
            vec![
                ("__tbname__", DataType::Utf8, IpcDataType::VarChar(128)),
                ("__using__", DataType::Utf8, IpcDataType::VarChar(128)),
                (
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    IpcDataType::Timestamp(TimeUnit::Millisecond)
                ),
                ("value", DataType::Float64, IpcDataType::Float64),
                ("m1", DataType::Utf8, IpcDataType::NChar(256)),
                ("m2", DataType::Utf8, IpcDataType::NChar(128)),
                ("id", DataType::Int32, IpcDataType::Int32),
            ]
        );
        let json = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", json);
    }

    fn demo_text_records() -> RecordBatch {
        let fields = vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("text", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let now = chrono::Utc::now()
            .sub(Duration::from_secs(60 * 60 * 24))
            .timestamp_millis();
        let columns = vec![
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                now,
                now + 1000,
                now + 2000,
                now + 3000,
                now + 4000,
                now + 5000,
            ])) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(vec![
                // name,value,id,price
                r#"a,1.1,1,1.1"#,
                r#"b,1.2,2,1.1"#,
                r#"a,1.3,1,1.1"#,
                r#"b,1.4,2,1.1"#,
                r#"a,1.5,1,1.1"#,
                r#"b,1.6,2,1.1"#,
            ])) as ArrayRef,
        ];
        RecordBatch::try_new(schema, columns).unwrap()
    }
    fn demo_mqtt_records() -> RecordBatch {
        let fields = vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("payload", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let now = chrono::Utc::now()
            .sub(Duration::from_secs(60 * 60 * 24))
            .timestamp_millis();
        let columns = vec![
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                now,
                now + 1000,
                now + 2000,
                now + 3000,
                now + 4000,
                now + 5000,
            ])) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(vec![
                r#"{"value":1.1, "id": 1, "events":[{"price": "1.1"}]}"#,
                r#"{"value":1.2, "id": 2, "events":[{"price": "1.1"}]}"#,
                r#"{"value":1.3, "id": 1, "events":[{"price": "1.1"}]}"#,
                r#"{"value":1.4, "id": 2, "events":[{"price": "1.1"}]}"#,
                r#"{"value":1.5, "id": 1, "events":[{"price": "1.1"}]}"#,
                r#"{"value":1.6, "id": 2, "events":[{"price": "1.1"}]}"#,
            ])) as ArrayRef,
        ];
        RecordBatch::try_new(schema, columns).unwrap()
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
///   "model": [{
///     "name": "{topic}-{location}",
///     "using": "{metric}",
///     "tags": ["topic", "location"],
///     "columns": ["ts", "value", "qos"]
///   }
/// }]
/// ```
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Parser {
    #[serde(default)]
    global: Arc<TableOptions>,
    parse: Option<ParserImpl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    mutate: Vec<Mutate>,
    model: Modeler,
}

impl Parser {
    pub fn global(&self) -> &TableOptions {
        &self.global
    }

    pub fn modeler(&self) -> &Modeler {
        &self.model
    }
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

impl Parser {
    pub fn new(parse: Option<ParserImpl>, mutate: Vec<Mutate>, model: Modeler) -> Self {
        Self {
            global: Arc::new(TableOptions::default()),
            parse,
            mutate,
            model,
        }
    }

    pub fn get_ipcdatatype_from_parser(&self, column_name: &str) -> Option<&IpcDataType> {
        let payload = self.parse.as_ref()?.get("payload");
        payload?;
        let payload = payload.unwrap();
        match payload {
            FieldParser::Json(json) => {
                if json.json.is_none() {
                    None
                } else {
                    let select = json.json.as_ref().unwrap();
                    match select {
                        Select::Include(incl) => {
                            for item in incl.iter() {
                                if (item.alias().is_some() && item.alias().unwrap() == column_name)
                                    || item.name() == column_name
                                {
                                    return item.cast();
                                }
                            }
                            None
                        }
                        _ => None,
                    }
                }
            }
            _ => None,
        }
    }

    pub fn parse_schema(&self, schema: &Arc<Schema>) -> Arc<Schema> {
        let _ = schema;
        todo!()
    }

    fn get_schema_column_with_name<'a>(
        schema: &'a Arc<Schema>,
        name: &str,
    ) -> Option<(usize, &'a Field)> {
        let (idx, field) = schema.fields().into_iter().enumerate().find(|(_, b)| {
            let meta_name = b.metadata().get("name");
            (meta_name.is_some() && name == meta_name.unwrap()) || b.name() == name
        })?;
        Some((idx, field.as_ref()))
    }

    fn transform_records(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
        let batch = self
            .parse
            .as_ref()
            .map(|parse| parse.transform_record_batch(records))
            .transpose()?
            .unwrap_or_else(|| records.clone());
        self.mutate.iter().fold(Ok(batch), |batch, mutate| {
            batch.and_then(|batch| mutate.transform_record_batch(&batch))
        })
    }

    fn to_json_valid_batches(batches: &[RecordBatch]) -> Vec<RecordBatch> {
        batches
            .iter()
            .map(|batch| {
                let schema = batch.schema();
                let fields = schema.fields();

                RecordBatch::try_from_iter(batch.columns().iter().enumerate().filter_map(
                    |(idx, data)| {
                        let dt = fields[idx].data_type();
                        if matches!(dt, DataType::Binary | DataType::LargeBinary) {
                            arrow::compute::cast(data, &DataType::Utf8)
                                .ok()
                                .map(|data| (fields[idx].name(), data))
                        } else {
                            Some((fields[idx].name(), data.clone()))
                        }
                    },
                ))
                .unwrap()
            })
            .collect()
    }

    pub fn parse_message_from_records(
        &self,
        records: &RecordBatch,
        filter_ts: bool,
    ) -> Result<Message, Error> {
        let batch = self.transform_records(records)?;
        let schema = batch.schema();
        let batches = vec![batch];
        let batch = &batches[0];
        // tracing::info!("Parse message {:?}", batch);

        let json_batches = Parser::to_json_valid_batches(&batches);

        let json = arrow::json::writer::record_batches_to_json_rows(
            json_batches.iter().collect_vec().as_slice(),
        )?;

        let mut data = vec![];
        for table in &self.model {
            let name = table.name.replace("${", "{");
            let mut template = TinyTemplate::new();
            template.add_template("name", &name).unwrap();
            if let Some(using) = table.using.as_ref() {
                template.add_template("using", using).unwrap();
            }

            let mut columns_indices = Vec::from_iter(0..batch.num_columns());
            let spec_columns = if let Some(cols) = table.columns.as_ref() {
                //
                let mut indices = Vec::new();
                for name in cols {
                    // if let Some((index, _)) = schema.column_with_name(name) {
                    if let Some((index, _)) =
                        Self::get_schema_column_with_name(&schema, name.as_str())
                    {
                        indices.push(index);
                    } else {
                        tracing::warn!("Selected column {} not found in stream message", name);
                    }
                }
                Some(indices)
            } else {
                None
            };
            let (tags, columns) = if let Some(tags) = &table.tags {
                let mut indices = vec![];
                for name in tags {
                    let (i, _) = Self::get_schema_column_with_name(&schema, name.as_str())
                        .ok_or_else(|| anyhow::format_err!("Invalid field name `{name}`"))?;
                    // let (i, _) = schema
                    // .column_with_name(&name)
                    // .ok_or_else(|| anyhow::format_err!("Invalid field name `{name}`"))?;

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

            let tables = if filter_ts {
                (0..batch.num_rows())
                    .filter(|row| {
                        let is_valid_primary_key = !columns.column(0).is_null(*row);
                        if !is_valid_primary_key {
                            let mut str = Vec::new();
                            let mut cursor = std::io::Cursor::new(&mut str);
                            let mut writer =
                                arrow::json::writer::LineDelimitedWriter::new(&mut cursor);
                            let _ = writer.write(&columns.slice(*row, 1));
                            tracing::warn!(
                                lost = %String::from_utf8_lossy(&str),
                                "Primary key is null, skip row {}",
                                row,
                            );
                        }
                        is_valid_primary_key
                    })
                    .map(|row| {
                        let result = template.render("name", &json[row]);
                        match result {
                            Ok(name) => (name, row),
                            Err(e) => {
                                // notice: we should set a useful name for the table
                                tracing::error!("Error rendering template: {}", e);
                                (String::new(), row)
                            }
                        }
                    })
                    .into_group_map()
            } else {
                (0..batch.num_rows())
                    .map(|row| {
                        let result = template.render("name", &json[row]);
                        match result {
                            Ok(name) => (name, row),
                            Err(e) => {
                                // notice: we should set a useful name for the table
                                tracing::error!("Error rendering template: {}", e);
                                (String::new(), row)
                            }
                        }
                    })
                    .into_group_map()
            };

            for (name, indices) in tables {
                // because we did not set a useful name, so we skip it
                if name.is_empty() || indices.is_empty() {
                    continue;
                }
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
                    opts: self.global.clone(),
                };
                data.push(item);
            }
        }
        Ok(Message::Records(data))
    }

    pub fn parse(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
        self.self_check()?;
        Ok(self
            .parse
            .as_ref()
            .map(|parse| parse.transform_record_batch(records))
            .transpose()?
            .unwrap_or_else(|| records.clone()))
    }

    fn self_check(&self) -> Result<(), Error> {
        if self.mutate.is_empty() && self.parse.is_none() {
            Err(anyhow::anyhow!(
                "Either parse or mutate must be set in pipeline"
            ))?;
        }
        for table in &self.model {
            if table.name.is_empty() {
                return Err(Error::EmptyTableName);
            } else if table.name.contains('.') {
                return Err(Error::TableNameContainsDot(table.name.clone()));
            }

            if let Some(columns) = table.columns.as_ref() {
                if columns.is_empty() {
                    return Err(Error::EmptyTableColumns(table.name.clone()));
                }
                if let Some(dup) = columns.iter().duplicates().next() {
                    return Err(Error::DuplicatedColumns(dup.clone()));
                }
            }

            if let Some(tags) = table.tags.as_ref() {
                if table.using.as_ref().is_none() {
                    return Err(Error::STableNameRequired);
                }
                if let Some(dup) = tags.iter().duplicates().next() {
                    return Err(Error::DuplicatedTags(dup.clone()));
                }
            }
            if let Some(stable) = table.using.as_ref() {
                if stable.is_empty() {
                    return Err(Error::EmptySTableName);
                } else if stable.contains('.') {
                    return Err(Error::STableNameContainsDot(stable.clone()));
                } else if table.tags.as_ref().map(Vec::is_empty).unwrap_or(true) {
                    return Err(Error::STableTagsRequired);
                }
            }
        }
        Ok(())
    }
}

// impl TransformExt for Parser {
//     // fn transform_message(&self, item: Message) -> Result<Option<Message>, Error> {
//     //     match item {
//     //         // todo: transformers should works on all kinds of message.
//     //         Message::Raw(raw) => Ok(Some(Message::Raw(raw))),
//     //         Message::Tables(tables) => Ok(Some(Message::Tables(tables))),
//     //         Message::ChildTables(tables) => Ok(Some(Message::ChildTables(tables))),
//     //         Message::Records(records) => {
//     //             let mut new = vec![];
//     //             for records in records {
//     //                 let batch = self.transform_record_batch(&records.records)?;
//     //                 if batch.num_rows() == 0 {
//     //                     continue;
//     //                 }
//     //                 let item = MessageArrowRecords {
//     //                     table: records.table.clone(),
//     //                     records: batch,
//     //                 };
//     //                 new.push(item);
//     //             }
//     //             Ok(Some(Message::Records(new)))
//     //         }
//     //     }
//     // }

//     fn transform_schema(
//         &self,
//         schema: std::sync::Arc<arrow::datatypes::Schema>,
//     ) -> Result<std::sync::Arc<arrow::datatypes::Schema>, Error> {
//         Ok(schema)
//     }

//     fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
//         self.parse(records)
//     }
// }

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

    pub fn get_tag_value_by_name(&self, name: &str) -> Option<&str> {
        self.tags
            .as_ref()
            .and_then(|batch| {
                let column = batch.column_by_name(name)?;
                column.as_any().downcast_ref::<StringArray>()
            })
            .map(|array| array.value(0))
    }
}
#[derive(Debug)]
pub struct MessageArrowRecords {
    pub table: MessageTableMeta,
    pub records: RecordBatch,
    pub opts: Arc<TableOptions>,
}

impl MessageArrowRecords {
    pub fn get_ts_column(&self) -> Option<&TimestampMillisecondArray> {
        self.records
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
    }

    pub fn filter_by_primary_timestamp(mut self, min: &DateTime<Utc>) -> Option<Self> {
        let col = self.records.column(0);
        if let DataType::Timestamp(unit, _) = col.data_type() {
            match unit {
                arrow_schema::TimeUnit::Second => {
                    let array = col.as_any().downcast_ref::<TimestampSecondArray>()?;
                    let min = min.timestamp();
                    let filter = BooleanArray::from_iter((0..array.len()).map(|index| {
                        if array.is_null(index) {
                            Some(false)
                        } else {
                            Some(array.value(index) > min)
                        }
                    }));
                    let records =
                        arrow::compute::filter_record_batch(&self.records, &filter).ok()?;
                    if records.num_rows() > 0 {
                        self.records = records;
                        Some(self)
                    } else {
                        None
                    }
                }
                arrow_schema::TimeUnit::Millisecond => {
                    let array = col.as_any().downcast_ref::<TimestampMillisecondArray>()?;
                    let min = min.timestamp_millis();
                    let filter = BooleanArray::from_iter((0..array.len()).map(|index| {
                        if array.is_null(index) {
                            Some(false)
                        } else {
                            Some(array.value(index) > min)
                        }
                    }));
                    let records =
                        arrow::compute::filter_record_batch(&self.records, &filter).ok()?;
                    if records.num_rows() > 0 {
                        self.records = records;
                        Some(self)
                    } else {
                        None
                    }
                }
                arrow_schema::TimeUnit::Microsecond => {
                    let array = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
                    let min = min.timestamp_micros();
                    let filter = BooleanArray::from_iter((0..array.len()).map(|index| {
                        if array.is_null(index) {
                            Some(false)
                        } else {
                            Some(array.value(index) > min)
                        }
                    }));
                    let records =
                        arrow::compute::filter_record_batch(&self.records, &filter).ok()?;
                    if records.num_rows() > 0 {
                        self.records = records;
                        Some(self)
                    } else {
                        None
                    }
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    let array = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
                    let min = min.timestamp_nanos_opt()?;
                    let filter = BooleanArray::from_iter((0..array.len()).map(|index| {
                        if array.is_null(index) {
                            Some(false)
                        } else {
                            Some(array.value(index) > min)
                        }
                    }));
                    let records =
                        arrow::compute::filter_record_batch(&self.records, &filter).ok()?;
                    if records.num_rows() > 0 {
                        self.records = records;
                        Some(self)
                    } else {
                        None
                    }
                }
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum WrittenProtocol {
    #[default]
    Auto,
    Sql,
    Stmt,
    Sml,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum WrittenMethod {
    #[default]
    Concurrent,
    VgroupConcurrent,
    VgroupSequential,
    Sequential,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum NullValues {
    #[default]
    Null,
    Skip,
}

impl NullValues {
    pub fn skip(&self) -> bool {
        matches!(self, Self::Skip)
    }
}

impl FromStr for WrittenMethod {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "concurrent" => Ok(Self::Concurrent),
            "vgroup" => Ok(Self::VgroupConcurrent),
            "vgroup_concurrent" => Ok(Self::VgroupConcurrent),
            "vgroup_sequential" => Ok(Self::VgroupSequential),
            "sequential" => Ok(Self::Sequential),
            _ => Err(Error::InvalidWrittenMethod(s.to_string())),
        }
    }
}

impl FromStr for NullValues {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "null" => Ok(Self::Null),
            "skip" => Ok(Self::Skip),
            _ => Err(Error::InvalidNullValues(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableOptions {
    // TODO: support case insensitive identifier, including table name and column name.
    /// Whether identifier is case insensitive. Not work for now.
    ///
    /// Default is `false`, which means identifier is case sensitive.
    #[serde(skip, default)]
    pub identifier_case_insensitive: bool,
    /// Replace dot in table name with this string.
    ///
    /// For example, if `replace_dot_in_table_name` is set to `_`, then table name `custom.table` will be converted to `custom_table`.
    ///
    /// Without this, table name `custom.table` will cause error 0x2617: "The table name cannot contain '.'".
    ///
    /// Default is `_`.
    #[serde(default)]
    pub replace_dot_in_table_name: String,

    /// Written method for insert.
    /// Default is `auto`.
    ///
    /// - `auto`: auto detect written method.
    /// - `sql`: use sql insert.
    /// - `stmt`: use stmt insert.
    /// - `sml`: use sml insert.
    #[serde(default)]
    pub written_protocol: WrittenProtocol,

    /// Flat written method
    written_method: Option<WrittenMethod>,

    /// Concurrent limit
    written_concurrent: Option<usize>,

    workers_per_vgroup: Option<usize>,

    /// How to deal with null values.
    null_values: Option<NullValues>,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            identifier_case_insensitive: false,
            replace_dot_in_table_name: "_".to_string(),
            written_protocol: WrittenProtocol::default(),
            written_method: None,
            written_concurrent: None,
            workers_per_vgroup: None,
            null_values: None,
        }
    }
}
impl TableOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn written_method(&self) -> WrittenMethod {
        self.written_method.unwrap_or_else(|| {
            std::env::var("TAOSX_WRITTEN_METHOD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(WrittenMethod::Concurrent)
        })
    }

    pub fn concurrent_limit(&self) -> usize {
        self.written_concurrent.unwrap_or_else(|| {
            std::env::var("TAOSX_WRITTEN_CONCURRENT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(
                    std::thread::available_parallelism()
                        .ok()
                        .map_or(4, |v| v.get()),
                )
        })
    }

    pub fn workers_per_vgroup(&self) -> usize {
        self.workers_per_vgroup.unwrap_or_else(|| {
            std::env::var("TAOSX_WORKERS_PER_VGROUP")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4)
        })
    }

    pub fn null_values(&self) -> NullValues {
        self.null_values.unwrap_or_else(|| {
            std::env::var("TAOSX_NULL_VALUES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_default()
        })
    }

    pub fn canonical_table_name<'b>(&self, name: &'b str) -> Cow<'b, str> {
        let dot = name.contains('.');
        match (self.identifier_case_insensitive, dot) {
            (true, true) => Cow::Owned(
                name.to_lowercase()
                    .replace('.', &self.replace_dot_in_table_name),
            ),
            (true, false) => Cow::Owned(name.to_lowercase()),
            (false, true) => Cow::Owned(name.replace('.', &self.replace_dot_in_table_name)),
            (false, false) => Cow::Borrowed(name),
        }
    }
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
            .map(|field| {
                match field.data_type() {
                    DataType::Binary
                    | DataType::Utf8
                    | DataType::LargeBinary
                    | DataType::LargeUtf8 => {
                        let cast_to = field.metadata().get("cast_to");
                        if cast_to.is_some() {
                            let cast_to = cast_to.unwrap();
                            let length = field.metadata().get("length");
                            if length.is_some() {
                                // varchar or nchar
                                let res = length.unwrap().parse::<usize>();
                                match res {
                                    Ok(length) => ColumnMeta::Column(Described::new(
                                        field.name(),
                                        Ty::from_str(cast_to.as_str()).unwrap(),
                                        Some(length),
                                    )),
                                    Err(err) => {
                                        tracing::error!(
                                            "varchar/nchar parse error: {}",
                                            err.to_string()
                                        );
                                        ColumnMeta::Column(Described::new(
                                            field.name(),
                                            Ty::from_str(cast_to.as_str()).unwrap(),
                                            None,
                                        ))
                                    }
                                }
                            } else {
                                // json
                                ColumnMeta::Column(Described::new(
                                    field.name(),
                                    Ty::from_str(cast_to.as_str()).unwrap(),
                                    None,
                                ))
                            }
                        } else {
                            ColumnMeta::Column(Described::new(field.name(), field.ty(), None))
                        }
                    }
                    _ => ColumnMeta::Column(Described::new(field.name(), field.ty(), None)),
                }
            })
            .collect()
    }
    pub fn tag_meta(&self) -> Option<Vec<ColumnMeta>> {
        self.table.tags.as_ref().map(|tags| {
            tags.schema()
                .fields()
                .iter()
                .map(|field| {
                    match field.data_type() {
                        DataType::Binary
                        | DataType::Utf8
                        | DataType::LargeBinary
                        | DataType::LargeUtf8 => {
                            if let Some(ty) = field.metadata().get("type") {
                                let ipc_ty = IpcDataType::from_str(ty.as_str()).unwrap();
                                ColumnMeta::Tag(Described::new(
                                    field.name(),
                                    ipc_ty.ty(),
                                    ipc_ty.length(),
                                ))
                            } else {
                                let cast_to = field.metadata().get("cast_to");
                                if cast_to.is_some() {
                                    let cast_to = cast_to.unwrap();
                                    let length = field.metadata().get("length");
                                    if length.is_some() {
                                        // varchar or nchar
                                        let res = length.unwrap().parse::<usize>();
                                        match res {
                                            Ok(length) => ColumnMeta::Tag(Described::new(
                                                field.name(),
                                                Ty::from_str(cast_to.as_str()).unwrap(),
                                                Some(length),
                                            )),
                                            Err(err) => {
                                                tracing::error!(
                                                    "varchar/nchar parse error: {}",
                                                    err.to_string()
                                                );
                                                ColumnMeta::Tag(Described::new(
                                                    field.name(),
                                                    Ty::from_str(cast_to.as_str()).unwrap(),
                                                    None,
                                                ))
                                            }
                                        }
                                    } else {
                                        // json
                                        ColumnMeta::Tag(Described::new(
                                            field.name(),
                                            Ty::from_str(cast_to.as_str()).unwrap(),
                                            None,
                                        ))
                                    }
                                } else {
                                    ColumnMeta::Tag(Described::new(field.name(), field.ty(), None))
                                }
                            }
                        }
                        _ => ColumnMeta::Column(Described::new(field.name(), field.ty(), None)),
                    }
                })
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
                "create table `{}` ({}) tags ({})",
                using, columns, tags
            ))
        } else {
            None
        }
    }
    pub fn table_sql(&self) -> String {
        let table_name = self
            .opts
            .canonical_table_name(self.table.name.as_str())
            .to_string();

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
                table_name, using, names, values
            )
        } else {
            let fields = self.column_meta();
            let columns = fields.iter().map(|f| f.sql_repr()).join(",");
            format!("create table if not exists `{}` ({})", table_name, columns)
        }
    }

    pub fn sql_insert_part(
        &self,
        precision: taos::Precision,
        with_meta: bool,
        with_field_names: bool,
    ) -> Vec<(String, usize)> {
        let primary_key_null_count = self.records.column(0).null_count();
        if primary_key_null_count == self.records.num_rows() {
            return vec![];
        }
        if primary_key_null_count > 0 {
            tracing::warn!(
                "Primary key column has null value, count: {}",
                primary_key_null_count
            );
            let nulls = self.records.column(0).nulls().unwrap();
            let indices = nulls.valid_indices().collect_vec();
            // self.records
            tracing::warn!(records = ?self.records,  "Null indices in records: {:?} ", indices);
        }
        let col_values = crate::utils::sql::sql_values_from_record_batch(
            &self.records,
            precision,
            // TODO: with field names in values.
            with_field_names,
        )
        .expect("Sql values should be recognizable");
        let tbname = self.opts.canonical_table_name(self.table.name.as_str());
        col_values
            .into_iter()
            .map(|(col_values, rows)| {
                if !with_meta || self.table.using.is_none() {
                    return (format!("`{}` {}", tbname, col_values), rows);
                }
                let using = self.table.using.as_ref().unwrap();

                // TODO: with field name in tags.
                if true {
                    // with_field_names
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

                    let tag_values = self
                        .table
                        .tags
                        .as_ref()
                        .unwrap()
                        .columns()
                        .iter()
                        .map(|c| c.taos_value(0).to_sql_value())
                        .join(",");

                    (
                        format!(
                            "`{}` using `{}` ({}) tags({}) {}",
                            tbname, using, names, tag_values, col_values
                        ),
                        rows,
                    )
                } else {
                    let tag_values = self
                        .table
                        .tags
                        .as_ref()
                        .unwrap()
                        .columns()
                        .iter()
                        .map(|c| c.taos_value(0).to_sql_value())
                        .join(",");
                    (
                        format!(
                            "`{}` using `{}` tags ({}) {}",
                            tbname, using, tag_values, col_values
                        ),
                        rows,
                    )
                }
            })
            .collect()
    }

    pub fn sql_insert_part_skip_null(&self, target_precision: taos::Precision) -> Vec<String> {
        if self.records.num_rows() == 0 {
            return vec![];
        }

        let primary_key_null_count = self.records.column(0).null_count();
        if primary_key_null_count == self.records.num_rows() {
            return vec![];
        }
        let tbname = self.opts.canonical_table_name(self.table.name.as_str());
        // panic on ArrowError
        crate::utils::sql::sql_values_from_record_batch_skip_null(
            tbname.borrow(),
            &self.records,
            target_precision,
        )
        .expect("Sql values should be recognizable")
    }

    pub fn stable_name(&self) -> Option<&str> {
        self.table.using.as_deref()
    }

    pub fn table_name(&self) -> &str {
        self.table.name.as_str()
    }

    pub fn max_var_length(&self, field: &str) -> Option<usize> {
        fn array_max_var_length(array: &dyn Array) -> Option<usize> {
            match array.data_type() {
                DataType::Binary => {
                    let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                    array.iter().map(|v| v.map(|v| v.len()).unwrap_or(0)).max()
                }
                DataType::Utf8 => {
                    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    array.iter().map(|v| v.map(|v| v.len()).unwrap_or(0)).max()
                }
                _ => None,
            }
        }
        if let Some(array) = self.records.column_by_name(field) {
            array_max_var_length(array)
        } else {
            array_max_var_length(self.table.tags.as_ref()?.column_by_name(field)?)
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
#[allow(dead_code)]
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

pub trait TransformExt {
    fn transform_schema(&self, schema: Arc<Schema>) -> Result<Arc<Schema>, Error> {
        let empty = RecordBatch::new_empty(schema);
        self.transform_record_batch(&empty)
            .map(|batch| batch.schema())
    }

    fn transform_record_batch(&self, records: &RecordBatch) -> Result<RecordBatch, Error>;
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid written method: {0}")]
    InvalidWrittenMethod(String),
    #[error("Invalid null values strategy: {0}")]
    InvalidNullValues(String),
    #[error(transparent)]
    EvalError(#[from] expr::EvalError),
    #[error("Template {0:?} error: {1:#}")]
    TemplateError(String, rhai::EvalAltResult),
    #[error("Parse error for field `{field}`: {error}")]
    FieldParserError {
        field: String,
        error: parse::ParseError,
    },
    #[error("The first column should be timestamp, but set as {0:?}")]
    TimestampAtFirst(String),
    #[error("Table name should not be empty")]
    EmptyTableName,
    #[error("Table columns should not be empty for table `{0}`")]
    EmptyTableColumns(String),
    #[error("Table contains duplicated columns: `{0}`")]
    DuplicatedColumns(String),
    #[error("Table contains duplicated tags: `{0}`")]
    DuplicatedTags(String),
    #[error("Table name should not contain dot: {0}")]
    TableNameContainsDot(String),
    #[error("STable name should be set when tags not empty")]
    STableNameRequired,
    #[error("Tags should not be empty when when stable set")]
    STableTagsRequired,
    #[error("STable name should not be empty")]
    EmptySTableName,
    #[error("STable name should not contain dot: {0}")]
    STableNameContainsDot(String),
    #[error("Internal transform error: {0:#}")]
    ArrowError(#[from] ArrowError),
    #[error("Transform mapper error: {0:#}")]
    MapValueError(#[from] map::ValueBuilderError),
    #[error("Transform error: {0:#}")]
    Other(#[from] anyhow::Error),
    #[error("Primary key({0}) value can't be cast to available timestamp")]
    NullPrimaryKey(String),
    #[error("Primary key({0}) must be or could be casted to timestamp: {1:#}")]
    PrimaryKeyCastError(String, arrow_schema::ArrowError),
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

#[cfg(test)]
mod parser_tests {
    use crate::plugins::transform::modeler::Modeler;

    use super::Parser;

    #[test]
    fn test_parser_serde() {
        let model = r#"{
            "name": "{topic}",
            "using": "mqtt",
            "tags": ["topic"],
            "columns": ["ts", "value", "qos"]
        }"#;
        let _: Modeler = serde_json::from_str(model).unwrap();
        let parser = r#"{
            "parse": {
                "payload": { "json": ["value::double"] },
                "ts": { "as": "timestamp(ns)", "with": "%F %T%.f", "tz": "UTC" }
            },
            "model": {
                "name": "{topic}",
                "using": "mqtt",
                "tags": ["topic"],
                "columns": ["ts", "value", "qos"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let json = serde_json::to_string_pretty(&parser).unwrap();
        println!("{}", json);
    }
}
