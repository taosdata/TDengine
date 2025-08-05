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
    cell::OnceCell,
    collections::{BTreeMap, HashMap, HashSet},
    ops::Range,
    str::FromStr,
    sync::Arc,
};

use anyhow::Context;
use archive::ArchiveType;
use arrow::{
    array::{
        Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, BooleanArray, Decimal128Array,
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    compute::concat_batches,
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
    util::display::{ArrayFormatter, FormatOptions},
};
use arrow_compute_ext::RecordBatchExt;
use arrow_schema::{FieldRef, TimeUnit};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use either::Either;
use faststr::FastStr;
use flume::Sender;
use handling_strategy::{HandlingResult, ProcessOnAbnormal};
use itertools::Itertools;
use modeler::stable::STableModel;
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
use tracing::instrument;

use super::expr;
use crate::plugins::transform::{modeler::stable::FastStrExpr, parse::ArrayForTaos};
use crate::{core_metrics::CoreMetrics, plugins::transform::modeler::Table};
use crate::{
    get_data_dir,
    global::{SQL_TAG_CACHE_CAPACITY, TABLE_TAG_CACHE},
};

use self::{
    modeler::{ModeledRecordBatch, Modeler},
    mutate::Mutate,
    parse::{FieldParser, ParserImpl},
};

pub(crate) mod select;

// mod json;
pub mod constants;

pub mod parse;

pub mod filter;
pub mod map;
pub mod modeler;
pub mod mutate;
pub mod sample;

pub mod handling_strategy;

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct Pipeline {
    #[serde(default)]
    global: Arc<TableOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parse: Option<ParserImpl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    mutate: Vec<Mutate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    s_model: Option<STableModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<Modeler>,
}

impl Pipeline {
    pub fn with_parse(self, parse: ParserImpl) -> Self {
        Self {
            parse: Some(parse),
            ..self
        }
    }

    pub fn transform(&self, records: &RecordBatch) -> Result<Vec<ModeledRecordBatch>, Error> {
        self.check()?;
        let batch = self
            .parse
            .as_ref()
            .map(|parse| parse.transform_record_batch(records))
            .transpose()?;

        let batch = batch.unwrap_or_else(|| records.clone());
        let batch = self
            .mutate
            .iter()
            .try_fold(batch, |batch, mutate| mutate.transform_record_batch(&batch))?;
        if let Some(model) = self.model.as_ref() {
            model.apply(&batch)
        } else {
            Ok(vec![ModeledRecordBatch::new(batch)])
        }
    }

    pub fn transform_records(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
        let batch = self
            .parse
            .as_ref()
            .map(|parse| parse.transform_record_batch(records))
            .transpose()?
            .unwrap_or_else(|| records.clone());
        self.mutate
            .iter()
            .try_fold(batch, |batch, mutate| mutate.transform_record_batch(&batch))
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
    s_model: Option<STableModel>,
    model: Modeler,
    metrics: Option<Arc<CoreMetrics>>,
}

impl PartialEq for Parser {
    fn eq(&self, other: &Self) -> bool {
        self.global == other.global
            && self.parse == other.parse
            && self.mutate == other.mutate
            && self.s_model == other.s_model
            && self.model == other.model
    }
}

impl Parser {
    pub fn global(&self) -> &TableOptions {
        &self.global
    }

    pub fn modeler(&self) -> &Modeler {
        &self.model
    }

    pub fn set_metrics(&mut self, metrics: Arc<CoreMetrics>) {
        self.metrics = Some(metrics);
    }

    pub fn set_maximum_timestamp(&mut self, ts: DateTime<Utc>) {
        Arc::make_mut(&mut self.global).maximum_timestamp = Some(ts);
    }

    pub fn set_minimum_timestamp(&mut self, ts: DateTime<Utc>) {
        Arc::make_mut(&mut self.global).minimum_timestamp = Some(ts);
    }

    pub fn organize_cache(&mut self, task_id: i64) -> Result<(), ParserError> {
        let cache = &mut Arc::make_mut(&mut self.global).process_on_abnormal.cache;
        let data_dir = get_data_dir();
        cache
            .organize_params(task_id, data_dir, true)
            .map_err(|error| ParserError::OrganizeCacheError { error })?;
        Ok(())
    }

    pub fn organize_archive(&mut self, task_id: i64) -> Result<(), ParserError> {
        let archive = &mut Arc::make_mut(&mut self.global).process_on_abnormal.archive;
        let data_dir = get_data_dir();
        archive
            .organize_params(task_id, data_dir, false)
            .map_err(|error| ParserError::OrganizeArchiveError { error })?;
        Ok(())
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
    #[error("Organize archive error: {error}")]
    OrganizeArchiveError { error: archive::CollateError },
    #[error("Organize cache error: {error}")]
    OrganizeCacheError { error: archive::CollateError },
}
impl FromStr for Parser {
    type Err = ParserError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(s) = s.strip_prefix('@') {
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
    pub fn new(
        parse: Option<ParserImpl>,
        mutate: Vec<Mutate>,
        s_model: Option<STableModel>,
        model: Modeler,
    ) -> Self {
        Self {
            global: Arc::new(TableOptions::default()),
            parse,
            mutate,
            s_model,
            model,
            metrics: None,
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

    #[instrument(skip_all)]
    fn transform_records(&self, records: &RecordBatch) -> Result<RecordBatch, Error> {
        let batch = self
            .parse
            .as_ref()
            .map(|parse| parse.transform_record_batch(records))
            .transpose()?
            .unwrap_or_else(|| records.clone());
        let parsed_rows = batch.num_rows() as u64;
        // parse 后展开的 rows
        if let Some(metrics) = self.metrics.as_ref() {
            let metrics = metrics.ipc();
            metrics.add_parsed_rows(parsed_rows);
        }
        let new_batch = self
            .mutate
            .iter()
            .try_fold(batch, |batch, mutate| mutate.transform_record_batch(&batch))?;
        // 经过 extract/ filter/ map 后的 rows
        if let Some(metrics) = self.metrics.as_ref() {
            let skipped_rows = parsed_rows.saturating_sub(new_batch.num_rows() as u64);
            let metrics = metrics.ipc();
            // filter_skipped_rows 是通过用户配置的 filter 过滤掉的 rows
            metrics.add_filter_skipped_rows(skipped_rows);
        }

        Ok(new_batch)
    }

    #[instrument(skip_all)]
    pub fn parse_message_from_records(
        &self,
        records: &RecordBatch,
        filter_ts: bool,
        archive_tx: Sender<ArchiveType>,
    ) -> Result<Message, Error> {
        // (ts, value, point_name, ${point_name}, site_controller_id)
        let transformed_batch = self.transform_records(records)?;
        let schema = transformed_batch.schema();
        // tracing::info!("Parse message {:?}", batch);

        let pivot_fields = schema
            .fields()
            .iter()
            .filter_map(|f| {
                f.name()
                    .strip_prefix("${")
                    .and_then(|name| name.strip_suffix("}"))
                    .map(|name| (name, f.name().as_str()))
            })
            .collect_vec();

        let stables = self
            .s_model
            .as_ref()
            .map(|s| s.apply(&transformed_batch, self.global()))
            .transpose()?;

        let mut data = vec![];

        let json_batch = OnceCell::new();

        'table: for table in &self.model {
            let mut archive_indices = HashMap::new();
            let mut skip_indices = HashMap::new();
            let mut use_current_time_indices = HashMap::new();

            // get the columns and tags
            let mut columns_indices = Vec::from_iter(0..transformed_batch.num_columns());
            let spec_columns = if let Some(cols) = &table.columns {
                let mut indices = Vec::new();
                for name in cols {
                    // TS-6763: 首先找名字完全匹配的列，再找 metadata 里 name 匹配的列
                    if let Ok(index) = schema.index_of(name) {
                        indices.push(index);
                        continue;
                    }
                    if let Some((index, _)) =
                        Self::get_schema_column_with_name(&schema, name.as_str())
                    {
                        indices.push(index);
                        continue;
                    }
                    tracing::warn!("Selected column {name} not found in stream message");
                }
                indices
            } else {
                Vec::new()
            };
            let (tags, columns) = if let Some(tags) = &table.tags {
                let mut indices = vec![];
                for name in tags {
                    if let Ok(index) = schema.index_of(name) {
                        indices.push(index);
                        columns_indices[index] = usize::MAX;
                        continue;
                    }
                    let (i, _) = Self::get_schema_column_with_name(&schema, name.as_str())
                        .ok_or_else(|| anyhow::format_err!("Invalid field name `{name}`"))?;
                    indices.push(i);
                    columns_indices[i] = usize::MAX;
                }
                let tags = transformed_batch.project(&indices)?;
                let cols = if spec_columns.is_empty() {
                    columns_indices
                        .into_iter()
                        .filter(|v| *v != usize::MAX)
                        .collect_vec()
                } else {
                    spec_columns
                };
                (Some(tags), transformed_batch.project(&cols).unwrap())
            } else {
                let cols = if spec_columns.is_empty() {
                    columns_indices
                } else {
                    spec_columns
                };
                (None, transformed_batch.project(&cols).unwrap())
            };

            // check the field length of columns and tags
            let all_fields: Vec<&Arc<arrow_schema::Field>> = if let Some(tags) = &tags {
                columns
                    .schema_ref()
                    .fields()
                    .iter()
                    .chain(tags.schema_ref().fields().iter())
                    .collect()
            } else {
                columns.schema_ref().fields().iter().collect()
            };
            for field in all_fields.clone() {
                let field_name = field.name();
                if field_name.len() > 64 {
                    match self
                        .global
                        .process_on_abnormal
                        .field_name_length_overflow
                        .handle(
                            vec![field_name.clone()],
                            64,
                            format!("the length of field name '{field_name}' should not exceed 64"),
                        ) {
                        Ok((HandlingResult::Skip, err)) => {
                            tracing::warn!("skip the batch due to {err}");
                            break 'table;
                        }
                        Ok((HandlingResult::Archive, err)) => {
                            tracing::warn!("archive and skip the batch due to {err}");
                            let mut err_vec = Vec::new();
                            let mut err_timestamp_vec = Vec::new();
                            for _ in 0..transformed_batch.num_rows() {
                                err_vec.push(err.clone());
                                err_timestamp_vec.push(Utc::now().timestamp_nanos_opt().unwrap());
                            }
                            archive_records(
                                &transformed_batch,
                                err_vec,
                                err_timestamp_vec,
                                archive_tx.clone(),
                            )?;
                            break 'table;
                        }
                        Ok((HandlingResult::Modify(_), _)) => todo!(), // TODO1
                        Ok((HandlingResult::ModifyAndArchive(_), _)) => todo!(), // TODO1
                        Ok((HandlingResult::Retry, _)) => unreachable!(),
                        Err(e) => {
                            Err(Error::FieldNameLengthOverflowError(
                                field_name.to_string(),
                                e,
                            ))?;
                        }
                    }
                }
            }

            // check primary timestamp
            if filter_ts {
                for row in 0..columns.num_rows() {
                    let col = columns.column(0);
                    let ts = get_primary_timestamp_ns(all_fields[0].name(), col, row)?;
                    // primary timestamp null
                    if ts.is_none() {
                        match self
                            .global
                            .process_on_abnormal
                            .primary_timestamp_null
                            .handle("the primary timestamp should not be null".to_string())
                        {
                            Ok((HandlingResult::Skip, err)) => {
                                let _ = skip_indices.insert(row, err);
                            }
                            Ok((HandlingResult::Archive, err)) => {
                                let _ = skip_indices.insert(row, err.clone());
                                let _ = archive_indices.insert(row, err);
                            }
                            Ok((HandlingResult::Modify(_), err)) => {
                                let _ = use_current_time_indices.insert(row, err);
                            }
                            Ok((HandlingResult::ModifyAndArchive(_), err)) => {
                                let _ = use_current_time_indices.insert(row, err.clone());
                                let _ = archive_indices.insert(row, err);
                            }
                            Ok((HandlingResult::Retry, _)) => unreachable!(),
                            Err(_) => {
                                Err(Error::NullPrimaryKey(all_fields[0].name().clone()))?;
                            }
                        }
                        continue;
                    }
                    // primary timestamp overflow
                    let ts = ts.unwrap();
                    let ts = ts / 1_000_000;
                    let mut primary_timestamp_overflow_flag = false;
                    if let Some(max_ts) = self.global.maximum_timestamp {
                        if ts > max_ts.timestamp_millis() {
                            primary_timestamp_overflow_flag = true;
                        }
                    }
                    if let Some(min_ts) = self.global.minimum_timestamp {
                        if ts < min_ts.timestamp_millis() {
                            primary_timestamp_overflow_flag = true;
                        }
                    }
                    if primary_timestamp_overflow_flag {
                        match self
                            .global
                            .process_on_abnormal
                            .primary_timestamp_overflow
                            .handle(format!("the primary timestamp {ts} overflow"))
                        {
                            Ok((HandlingResult::Skip, err)) => {
                                let _ = skip_indices.insert(row, err);
                            }
                            Ok((HandlingResult::Archive, err)) => {
                                let _ = skip_indices.insert(row, err.clone());
                                let _ = archive_indices.insert(row, err);
                            }
                            Ok((HandlingResult::Modify(_), _)) => unreachable!(),
                            Ok((HandlingResult::ModifyAndArchive(_), _)) => unreachable!(),
                            Ok((HandlingResult::Retry, _)) => unreachable!(),
                            Err(e) => {
                                Err(Error::PrimaryTimestampOverflow(format!("{e:#}")))?;
                            }
                        }
                    }
                }
            }

            let name = table.name.replace("${", "{");
            let mut template = TinyTemplate::new();
            template.add_template("name", &name).unwrap();
            let using = table.using.as_ref().map(|using| using.replace("${", "{"));
            if let Some(using) = using.as_ref() {
                template.add_template("using", using).unwrap();
            }

            let skipped: HashSet<usize> = HashSet::from_iter(skip_indices.keys().cloned());
            let tables = (0..transformed_batch.num_rows())
                .filter(|row| !skipped.contains(row))
                .map(|row| {
                    match generate_table_name(
                        self.global.process_on_abnormal.clone(),
                        table,
                        row,
                        &transformed_batch,
                        &table.name,
                        &json_batch,
                    )? {
                        (HandlingResult::Skip, err) => {
                            let _ = skip_indices.insert(row, err);
                            anyhow::Ok((String::default(), row))
                        }
                        (HandlingResult::Archive, err) => {
                            let _ = skip_indices.insert(row, err.clone());
                            let _ = archive_indices.insert(row, err);
                            Ok((String::default(), row))
                        }
                        (HandlingResult::Modify(mut name), _) => {
                            Ok((name.pop().unwrap_or_default(), row))
                        }
                        (HandlingResult::ModifyAndArchive(mut name), err) => {
                            let _ = archive_indices.insert(row, err);
                            Ok((name.pop().unwrap_or_default(), row))
                        }
                        (HandlingResult::Retry, _) => unreachable!(),
                    }
                })
                .try_collect::<_, Vec<_>, _>()?
                .into_iter()
                .into_group_map();

            // 1. archive records
            if !archive_indices.is_empty() {
                let mut archive_indices_vec = Vec::new();
                let mut err_vec = Vec::new();
                let mut err_timestamp_vec = Vec::new();
                archive_indices.iter().for_each(|(row, err)| {
                    archive_indices_vec.push(*row);
                    err_vec.push(err.clone());
                    err_timestamp_vec.push(Utc::now().timestamp_nanos_opt().unwrap());
                });
                let archive_batches = archive_indices_vec
                    .iter()
                    .map(|row| transformed_batch.slice(*row, 1))
                    .collect_vec();
                let archive_batch = concat_batches(&transformed_batch.schema(), &archive_batches)?;
                archive_records(
                    &archive_batch,
                    err_vec,
                    err_timestamp_vec,
                    archive_tx.clone(),
                )?;
            }

            let ts_field_name = table
                .columns
                .as_ref()
                .and_then(|v| v.first())
                .context("ts field not found")?;

            if let Some(metrics) = self.metrics.as_ref() {
                let metrics = metrics.ipc();
                // check_skipped_rows: 写入前检查过滤掉的 rows
                metrics.add_check_skipped_rows(skip_indices.len() as u64);
            }
            let using_expr = table
                .using
                .as_ref()
                .map(|name| FastStrExpr::new(name.clone().into()));
            // name: sub_table_name, indices: group row index
            for (name, indices) in tables {
                // 2. skip records
                let indices = if skip_indices.is_empty() {
                    indices
                } else {
                    indices
                        .into_iter()
                        .filter(|row| !skip_indices.contains_key(row))
                        .collect_vec()
                };

                // 3. if we did not set a useful name or the indices is empty, skip this table
                if name.is_empty() || indices.is_empty() {
                    continue;
                }

                // 4. modify records
                let columns = if use_current_time_indices.is_empty() {
                    columns.clone()
                } else {
                    let time_array: Vec<_> = (0..columns.num_rows())
                        .map(|row| {
                            if use_current_time_indices.contains_key(&row) {
                                Utc::now().timestamp_nanos_opt()
                            } else {
                                match get_primary_timestamp_ns(
                                    all_fields[0].name(),
                                    columns.column(0),
                                    row,
                                ) {
                                    Ok(Some(ts)) => Some(ts),
                                    _ => None,
                                }
                            }
                        })
                        .collect();
                    RecordBatch::try_new(
                        columns.schema(),
                        columns
                            .columns()
                            .iter()
                            .enumerate()
                            .map(|(i, col)| {
                                if i == 0 {
                                    Arc::new(TimestampNanosecondArray::from(time_array.clone()))
                                } else {
                                    col.clone()
                                }
                            })
                            .collect::<Vec<_>>(),
                    )?
                };

                let ranges = indices_to_ranges(&indices);
                let name_row = indices[0];

                let using = using_expr
                    .as_ref()
                    .map(|expr| expr.eval(&transformed_batch, name_row))
                    .transpose()?
                    .map(|using| self.global().canonical_table_name(&using).to_string());

                let tags = tags
                    .as_ref()
                    .map(|batch| Arc::new(batch.slice(name_row, 1)));

                let using = match (&stables, using) {
                    (Some(map), Some(using)) => map
                        .get(&FastStr::from(using))
                        .map(|m| Arc::new(STable::Model(m.clone()))),
                    (None, Some(using)) => Some(Arc::new(STable::Name(using))),
                    (_, None) => None,
                };

                // without pivot
                if pivot_fields.is_empty() {
                    let sub_table_batches = ranges
                        .iter()
                        .map(|range| columns.slice(range.start, range.len()))
                        .collect_vec();
                    let sub_table_batch = arrow::compute::concat_batches(
                        &columns.schema(),
                        sub_table_batches.iter(),
                    )?;
                    if let Some(metrics) = self.metrics.as_ref() {
                        let metrics = metrics.ipc();
                        // 没有 pivot，设置 write_ready_rows
                        metrics.add_write_ready_rows(sub_table_batch.num_rows() as u64);
                    }
                    let meta = MessageTableMeta::new(name, using, tags);
                    let item = MessageArrowRecords {
                        table: meta,
                        records: sub_table_batch,
                        opts: self.global.clone(),
                    };
                    data.push(item);
                } else {
                    let meta = MessageTableMeta::new(name.clone(), using.clone(), tags.clone());

                    let batches = ranges
                        .iter()
                        .map(|range| transformed_batch.slice(range.start, range.len()))
                        .collect_vec();
                    let pivot_batch = arrow::compute::concat_batches(
                        transformed_batch.schema_ref(),
                        batches.iter(),
                    )?;

                    let common_cols = table.columns.as_ref().map(|cols| {
                        cols.iter()
                            .filter(|col| !pivot_fields.iter().any(|(a, b)| a == col || b == col))
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                    });
                    // let common_cols = table.columns.as_ref()
                    let pivot_batches = pivot(
                        &pivot_batch,
                        ts_field_name,
                        &pivot_fields,
                        common_cols.as_deref(),
                    )?;
                    for batch in pivot_batches {
                        if let Some(metrics) = self.metrics.as_ref() {
                            let metrics = metrics.ipc();
                            // 有 pivot，设置 write_ready_rows
                            metrics.add_write_ready_rows(batch.num_rows() as u64);
                        }

                        data.push(MessageArrowRecords {
                            table: meta.clone(),
                            records: batch,
                            opts: self.global.clone(),
                        })
                    }
                }
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

/// get primary timestamp from record
///
/// - name: the name of column
/// - col: the record column
/// - row: the record row
pub fn get_primary_timestamp_ns(
    name: &str,
    col: &ArrayRef,
    row: usize,
) -> Result<Option<i64>, Error> {
    if let DataType::Timestamp(unit, _) = col.data_type() {
        match unit {
            arrow_schema::TimeUnit::Second => {
                let array = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                if array.is_null(row) {
                    Ok(None)
                } else {
                    let ts = array.value(row);
                    let ts = ts * 1_000_000_000;
                    Ok(Some(ts))
                }
            }
            arrow_schema::TimeUnit::Millisecond => {
                let array = col
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                if array.is_null(row) {
                    Ok(None)
                } else {
                    let ts = array.value(row);
                    let ts = ts * 1_000_000;
                    Ok(Some(ts))
                }
            }
            arrow_schema::TimeUnit::Microsecond => {
                let array = col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                if array.is_null(row) {
                    Ok(None)
                } else {
                    let ts = array.value(row);
                    let ts = ts * 1_000;
                    Ok(Some(ts))
                }
            }
            arrow_schema::TimeUnit::Nanosecond => {
                let array = col
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                if array.is_null(row) {
                    Ok(None)
                } else {
                    let ts = array.value(row);
                    Ok(Some(ts))
                }
            }
        }
    } else {
        Err(Error::PrimaryKeyCastError(
            name.to_string(),
            arrow_schema::ArrowError::CastError(col.data_type().to_string()),
        ))
    }
}

/// generate subtable name by template and record value
///
/// - process_on_abnormal: the configuration of abnormal handling method
/// - template: such as 'table_{tag1}'
/// - table_name_org: the original table name
/// - data: the record processed by mutate
fn generate_table_name(
    process_on_abnormal: ProcessOnAbnormal,
    table: &Table,
    row: usize,
    records: &RecordBatch,
    table_name_org: &str,
    json_batch: &OnceCell<Vec<serde_json::Value>>,
) -> anyhow::Result<(HandlingResult, String)> {
    // render table name
    match table.eval_table_name_row(records, row) {
        Ok(name) => {
            // the length of table name should not exceed 192
            if name.len() > 192 {
                return process_on_abnormal.table_name_length_overflow.handle(
                    vec![name.clone()],
                    192,
                    format!("the length of table name '{name}' should not exceed 192"),
                );
            }
            // the table name should not contain illegal characters
            if name.contains('.') {
                return process_on_abnormal.table_name_contains_illegal_char.handle(
                    &name,
                    format!("the table name '{name}' should not contain illegal characters"),
                );
            }
            Ok((HandlingResult::Modify(vec![name]), String::new()))
        }
        Err(e) => {
            let data = match json_batch.get() {
                Some(data) => data[row].clone(),
                None => {
                    let json_batches = to_json_valid_batches(&[records.clone()]);
                    let data: Vec<_> = json_batches
                        .iter()
                        .map(|batch| batch.to_json_rows::<serde_json::Value>())
                        .flatten_ok()
                        .try_collect()?;
                    let data = json_batch.get_or_init(|| data);
                    data[row].clone()
                }
            };
            // render table name failed
            process_on_abnormal
                .variable_not_exist_in_table_name_template
                .handle(
                    table_name_org,
                    &data,
                    format!("render table name '{table_name_org}' failed, e: {:?}", e),
                )
        }
    }
}

/// write record batch to parquet file
///
/// - task_id: the id of task
/// - location: the location of parquet file
/// - batch: the record batch
/// - err_vec: the error message vector
/// - err_timestamp_vec: the error timestamp vector
pub fn archive_records(
    batch: &RecordBatch,
    err_vec: Vec<String>,
    err_timestamp_vec: Vec<i64>,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<()> {
    if batch.num_rows() > 0 {
        // get fields and columns
        let mut fields_vec = batch.schema().fields().to_vec();
        let mut columns_vec = batch.columns().to_vec();

        // add new fields and columns to record
        let new_field_1 = Field::new("_taosx_error_", DataType::Utf8, false);
        let new_field_2 = Field::new(
            "_taosx_error_timestamp_",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        );
        let new_column_1 = Arc::new(StringArray::from(err_vec));
        let new_column_2 = Arc::new(TimestampNanosecondArray::from(err_timestamp_vec));

        fields_vec.push(Arc::new(new_field_1));
        fields_vec.push(Arc::new(new_field_2));
        columns_vec.push(new_column_1);
        columns_vec.push(new_column_2);

        // create a new RecordBatch with the additional column
        let new_schema = Arc::new(Schema::new(fields_vec));
        let new_batch = RecordBatch::try_new(new_schema, columns_vec)?;

        archive_tx.send(ArchiveType::Archive(new_batch))?;
    }
    Ok(())
}

fn pivot(
    batch: &RecordBatch,
    ts_field: &str,
    pivot_fields: &[(&str, &str)],
    common_fields: Option<&[&str]>,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut partitions: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    let schema = batch.schema();

    let ts_array = batch
        .column_by_name(ts_field)
        .context("ts column not found")?;
    let formatter = ArrayFormatter::try_new(ts_array, &FormatOptions::new())
        .context("build ts formatter error")?;
    for row in 0..batch.num_rows() {
        let ts = formatter.value(row).to_string();
        let rows = partitions
            .entry(ts)
            .or_insert_with(|| Vec::with_capacity(200));
        rows.push(row);
    }

    let mut res: HashMap<Arc<Schema>, Vec<RecordBatch>> = HashMap::new();

    let common_fields = match common_fields {
        Some(common_fields) => common_fields
            .iter()
            .filter_map(|name| {
                schema
                    .field_with_name(name)
                    .ok()
                    .map(|f| Arc::new(f.clone()))
            })
            .collect::<Vec<_>>(),
        None => schema
            .fields()
            .iter()
            .filter(|f| {
                !pivot_fields
                    .iter()
                    .any(|(a, b)| f.name() == a || f.name() == b)
            })
            .cloned()
            .collect::<Vec<_>>(),
    };
    let common_arrays = common_fields
        .iter()
        .filter_map(|f| batch.column_by_name(f.name()))
        .cloned()
        .collect::<Vec<_>>();

    // 一个分区的数据，转换成一行，一个 recordbatch
    for rows in partitions.values() {
        let mut fields = Vec::from_iter(common_fields.clone());
        let mut arrays = Vec::from_iter(common_arrays.iter().map(|s| s.slice(rows[0], 1)).clone());
        for (pivot_name, pivot_value) in pivot_fields {
            let (Some(name_col), Some(value_col)) = (
                batch.column_by_name(pivot_name),
                batch.column_by_name(pivot_value),
            ) else {
                unreachable!()
            };

            let name_col = name_col.as_string::<i32>();
            let mut pivot_fields: BTreeMap<&str, (FieldRef, ArrayRef)> = BTreeMap::new();
            for &row in rows {
                let name = name_col.value(row);
                macro_rules! value_array {
                    ($array_type: ty) => {{
                        let value_column = value_col
                            .as_any()
                            .downcast_ref::<$array_type>()
                            .with_context(|| {
                                format!("value column cast to {} failed", value_col.data_type())
                            })?;
                        if value_column.is_null(row) {
                            (
                                Arc::new(Field::new(name, value_column.data_type().clone(), true)),
                                Arc::new(<$array_type>::new_null(1)),
                            )
                        } else {
                            (
                                Arc::new(Field::new(name, value_column.data_type().clone(), true)),
                                Arc::new(<$array_type>::from(vec![value_column.value(row)])),
                            )
                        }
                    }};
                }

                let (field, array): (FieldRef, ArrayRef) = match value_col.data_type() {
                    DataType::Boolean => value_array!(BooleanArray),
                    DataType::Int8 => value_array!(Int8Array),
                    DataType::Int16 => value_array!(Int16Array),
                    DataType::Int32 => value_array!(Int32Array),
                    DataType::Int64 => value_array!(Int64Array),
                    DataType::UInt8 => value_array!(UInt8Array),
                    DataType::UInt16 => value_array!(UInt16Array),
                    DataType::UInt32 => value_array!(UInt32Array),
                    DataType::UInt64 => value_array!(UInt64Array),
                    DataType::Float32 => value_array!(Float32Array),
                    DataType::Float64 => value_array!(Float64Array),
                    DataType::Timestamp(time_unit, _) => match time_unit {
                        TimeUnit::Second => value_array!(TimestampSecondArray),
                        TimeUnit::Millisecond => value_array!(TimestampMillisecondArray),
                        TimeUnit::Microsecond => value_array!(TimestampMicrosecondArray),
                        TimeUnit::Nanosecond => value_array!(TimestampNanosecondArray),
                    },
                    DataType::Binary => value_array!(BinaryArray),
                    DataType::LargeBinary => value_array!(LargeBinaryArray),
                    DataType::BinaryView => value_array!(BinaryViewArray),
                    DataType::Utf8 => value_array!(StringArray),
                    DataType::LargeUtf8 => value_array!(LargeStringArray),
                    DataType::Utf8View => value_array!(StringViewArray),
                    DataType::Decimal128(precision, scale) => {
                        let value_column = value_col
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .context("value column cast to Decimal128 failed")?;
                        if value_column.is_null(row) {
                            (
                                Arc::new(Field::new(name, value_column.data_type().clone(), true)),
                                Arc::new(Decimal128Array::new_null(1)),
                            )
                        } else {
                            (
                                Arc::new(Field::new(name, value_column.data_type().clone(), true)),
                                Arc::new(
                                    Decimal128Array::from(vec![value_column.value(row)])
                                        .with_precision_and_scale(*precision, *scale)
                                        .context("pivot build Decimal128 array error")?,
                                ),
                            )
                        }
                    }
                    dt => unimplemented!("pivot unsupport datatype: {dt}"),
                };
                pivot_fields.insert(name, (field, array));
            }
            for (field, array) in pivot_fields.into_values() {
                fields.push(field);
                arrays.push(array);
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let records = res.entry(schema.clone()).or_default();
        let batch =
            RecordBatch::try_new(schema, arrays).context("build pivot recordbatch error")?;
        records.push(batch);
    }

    res.into_iter()
        .map(|(schema, batches)| concat_batches(&schema, batches.iter()))
        .collect::<Result<Vec<_>, _>>()
        .context("concat batch error")
}

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

#[derive(Debug, Clone, PartialEq)]
pub enum STable {
    Name(String),
    Model(STableModel),
}

impl STable {
    pub fn name(&self) -> &str {
        match self {
            STable::Name(name) => name,
            STable::Model(model) => model.name(),
        }
    }

    pub fn model(&self) -> Option<&STableModel> {
        match self {
            STable::Name(_) => None,
            STable::Model(model) => Some(model),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageTableMeta {
    pub name: Arc<String>,
    pub using: Option<Arc<STable>>,
    pub tags: Option<Arc<RecordBatch>>,
}

impl MessageTableMeta {
    pub fn new(
        name: impl Into<Arc<String>>,
        using: impl Into<Option<Arc<STable>>>,
        tags: impl Into<Option<Arc<RecordBatch>>>,
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
#[derive(Debug, PartialEq)]
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

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WrittenProtocol {
    #[default]
    Auto,
    Sql,
    Stmt,
    Sml,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WrittenMethod {
    #[default]
    Concurrent,
    VgroupConcurrent,
    VgroupSequential,
    Sequential,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
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

    pub minimum_timestamp: Option<DateTime<Utc>>,
    pub maximum_timestamp: Option<DateTime<Utc>>,

    /// How to process on abnormal.
    #[serde(default)]
    #[serde(flatten)]
    pub process_on_abnormal: ProcessOnAbnormal,
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
            minimum_timestamp: None,
            maximum_timestamp: None,
            process_on_abnormal: ProcessOnAbnormal::default(),
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
            arrow::datatypes::DataType::Decimal128(p, _) => {
                if *p <= 18 {
                    taos::Ty::Decimal64
                } else {
                    taos::Ty::Decimal
                }
            }
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
                let cast_to = field.metadata().get("cast_to");
                match (field.data_type(), cast_to) {
                    (
                        DataType::Binary
                        | DataType::Utf8
                        | DataType::LargeBinary
                        | DataType::LargeUtf8,
                        _,
                    ) => {
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
                    (DataType::Decimal128(precision, scale), _) => ColumnMeta::Column(
                        Described::new(field.name(), field.ty(), None).with_origin_ty_name(
                            &format!("{}({},{})", field.ty(), precision, scale),
                        ),
                    ),
                    (DataType::List(field), Some(cast_to))
                        if field.data_type().is_numeric() && cast_to == "VARBINARY" =>
                    {
                        ColumnMeta::Column(Described::new(
                            field.name(),
                            Ty::from_str(cast_to).unwrap(),
                            None,
                        ))
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
        self.table.using.as_ref().map(|using| match using.as_ref() {
            STable::Name(using) => {
                let fields = self.column_meta();
                let columns = fields.iter().map(|f| f.sql_repr()).join(",");
                let tags = self
                    .tag_meta()
                    .unwrap()
                    .iter()
                    .map(|f| f.sql_repr())
                    .join(",");
                format!("create table `{}` ({}) tags ({})", using, columns, tags)
            }
            STable::Model(model) => model.create_stable_sql(),
        })
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
                table_name,
                using.name(),
                names,
                values
            )
        } else {
            let fields = self.column_meta();
            let columns = fields.iter().map(|f| f.sql_repr()).join(",");
            format!("create table if not exists `{}` ({})", table_name, columns)
        }
    }

    pub fn get_full_table_name(&self, database_name: &str) -> String {
        let table_name = self.opts.canonical_table_name(self.table.name.as_str());
        format!("{}.{}", database_name, table_name)
    }

    pub fn sql_insert_part(
        &self,
        precision: taos::Precision,
        with_meta: bool,
        with_field_names: bool,
        database_name: Option<&str>,
    ) -> Vec<(String, usize, Option<String>, usize, usize)> {
        let primary_key_null_count = self.records.column(0).null_count();
        if primary_key_null_count == self.records.num_rows() {
            return vec![];
        }
        if primary_key_null_count > 0 {
            tracing::warn!("Primary key column has null value, count: {primary_key_null_count}");
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
            .map(|(col_values, rows, start, end)| {
                if !with_meta || self.table.using.is_none() {
                    return (
                        format!("`{}` {}", tbname, col_values),
                        rows,
                        None,
                        start,
                        end,
                    );
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

                    let mut full_name_to_cache = None;
                    if unsafe { SQL_TAG_CACHE_CAPACITY > 0 } && database_name.is_some() {
                        // 根据 database.tablename 来判断缓存，如果缓存在则不需要带 using
                        let table_existed = TABLE_TAG_CACHE.get_or_init(|| {
                            tracing::info!("Init tag cache with capacity: {}", unsafe {
                                SQL_TAG_CACHE_CAPACITY
                            });
                            scc::HashSet::with_capacity(unsafe { SQL_TAG_CACHE_CAPACITY })
                        });
                        let tag_key = format!("{}.{}", database_name.unwrap(), tbname);
                        if table_existed.contains(&tag_key) {
                            return (
                                format!("`{}` {}", tbname, col_values),
                                rows,
                                None,
                                start,
                                end,
                            );
                        }
                        full_name_to_cache = Some(tag_key);
                    }

                    (
                        format!(
                            "`{}` using `{}` ({}) tags({}) {}",
                            tbname,
                            using.name(),
                            names,
                            tag_values,
                            col_values
                        ),
                        rows,
                        full_name_to_cache,
                        start,
                        end,
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
                            tbname,
                            using.name(),
                            tag_values,
                            col_values
                        ),
                        rows,
                        None,
                        start,
                        end,
                    )
                }
            })
            .collect()
    }

    pub fn sql_insert_part_skip_null(
        &self,
        target_precision: taos::Precision,
    ) -> Vec<(String, usize, usize, usize)> {
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
        self.table.using.as_ref().map(|s| s.name())
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

#[allow(clippy::enum_variant_names)]
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
    #[error("Primary timestamp value overflow: {0}")]
    PrimaryTimestampOverflow(String),
    #[error("Field `{0}` name length overflow: {1:#}")]
    FieldNameLengthOverflowError(String, anyhow::Error),
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

#[cfg(test)]
mod parser_tests {
    use anyhow::Context;
    use arrow::{
        array::{ArrayRef, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray},
        util::pretty,
    };
    use chrono::Utc;
    use regex::Regex;
    use serde_json as json;
    use std::sync::Arc;
    use std::{cmp::Ordering, sync::atomic::Ordering::SeqCst};
    use tinytemplate::TinyTemplate;

    use super::*;
    use crate::plugins::transform::{
        modeler::Modeler, mutate::Mutate, parse::ParserImpl, Message, ProcessOnAbnormal, STable,
        TableOptions,
    };
    use crate::sink::ipc_metric::IpcMetrics;

    #[test]
    fn test_indices_to_ranges() {
        let indices = vec![0, 1, 2, 3, 5, 6, 7, 8, 10];
        let ranges = indices_to_ranges(&indices);
        dbg!(&ranges);
        assert_eq!(ranges, vec![0..4, 5..9, 10..11]);
    }

    #[test]
    fn test_parser_serde() {
        let global = r#"{
            "identifier_case_insensitive": false,
            "replace_dot_in_table_name": "_",
            "written_protocol": "auto",
            "written_method": "concurrent",
            "written_concurrent": 4,
            "workers_per_vgroup": 4,
            "null_values": "null",
            "cache": {
                "max_size": "0GB",
                "max_size_unit": "GB",
                "max_size_value": 0,
                "location": "cache",
                "on_fail": "skip"
            },
            "archive": {
                "keep_days": "0d",
                "keep_days_unit": "d",
                "keep_days_value": 0,
                "max_size": "0GB",
                "max_size_unit": "GB",
                "max_size_value": 0,
                "location": "",
                "on_fail": "rotate"
            },
            "primary_timestamp_overflow": "archive",
            "primary_timestamp_null": "archive",
            "primary_key_null": "archive",
            "table_name_length_overflow": "archive",
            "table_name_contains_illegal_char": {
                "replace_to": ""
            },
            "variable_not_exist_in_table_name_template": {
                "replace_to": ""
            },
            "field_name_not_found": "add_field",
            "field_name_length_overflow": "archive",
            "field_length_extend": true,
            "field_length_overflow": "archive",
            "ingesting_error": "archive",
            "connection_timeout_in_second": "1s",
            "connection_timeout_in_second_unit": "s",
            "connection_timeout_in_second_value": 1
        }"#;
        let global: TableOptions = serde_json::from_str(global).unwrap();
        dbg!(global);

        let parser = r#"{
            "payload": { "json": ["value::double"] },
            "ts": { "as": "timestamp(ns)", "with": "%F %T%.f", "tz": "UTC" }
        }"#;
        let parser: ParserImpl = serde_json::from_str(parser).unwrap();
        dbg!(parser);

        let model = r#"{
            "name": "{topic}",
            "using": "mqtt",
            "tags": ["topic"],
            "columns": ["ts", "value", "qos"]
        }"#;
        let model: Modeler = serde_json::from_str(model).unwrap();
        dbg!(model);

        let mutate = r#"[
            { "filter": ["a > b && c != 0"] },
            { "map": { "new1": { "sum": ["a","b"], "as": "INT" }, "new2": { "join": ["a","b"], "with":"&&" } } },
            { "extract": { "payload": { "json": "" } } }
        ]"#;
        let mutates: Vec<Mutate> = serde_json::from_str(mutate).unwrap();
        dbg!(mutates);

        let parser = r#"{
            "parse": {
                "payload": { "json": ["value::double"] },
                "ts": { "as": "timestamp(ns)", "with": "%F %T%.f", "tz": "UTC" }
            },
            "mutate": [
                { "filter": ["a > b && c != 0"] },
                { "map": { "new1": { "sum": ["a","b"], "as": "INT" }, "new2": { "join": ["a","b"], "with":"&&" } } },
                { "extract": { "payload": { "json": "" } } }
            ],
            "model": {
                "name": "{topic}",
                "using": "mqtt",
                "tags": ["topic"],
                "columns": ["ts", "value", "qos"]
            },
            "global": {
                "primary_timestamp_overflow": "break",
                "primary_timestamp_null": "use_current_time",
                "primary_key_null": "break"
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let json = serde_json::to_string_pretty(&parser).unwrap();
        println!("{}", json);
    }

    #[tokio::test]
    async fn test_parse_message_from_records_metrics() -> anyhow::Result<()> {
        let parser = json::json!({
            "parse": {
                "payload": { "json": ""}
            },
            "mutate": [
                { "filter": ["a == b"] },
            ],
            "model": {
                "name": "tb",
                "using": "stb",
                "tags": ["id"],
                "columns": ["ts", "a", "b"]
            },
            "global": {
                "primary_timestamp_overflow": "skip",
                "minimum_timestamp": "2020-01-01T00:00:00Z",
            }
        });
        let mut parser: Parser = serde_json::from_value(parser)?;
        let metrics = IpcMetrics::new("stb".to_string(), 1, None);
        parser.metrics = Some(Arc::new(CoreMetrics::IPC(metrics)));
        let (tx, _rx) = flume::bounded(10);

        let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            1000000000000,
            1100000000000,
            1200000000000,
            1300000000000,
            1400000000000,
            1500000000000,
            1600000000000,
            1700000000000,
            1800000000000,
            1900000000000,
        ]));
        let payload: ArrayRef = Arc::new(StringArray::from(vec![
            r#"{"id":1, "a": 0, "b": 0}"#,
            r#"{"id":1, "a": 1, "b": 1}"#,
            r#"{"id":1, "a": 2, "b": 2}"#,
            r#"{"id":1, "a": 3, "b": 3}"#,
            r#"{"id":1, "a": 4, "b": 4}"#,
            r#"{"id":2, "a": 5, "b": 4}"#,
            r#"{"id":2, "a": 6, "b": 6}"#,
            r#"{"id":2, "a": 7, "b": 7}"#,
            r#"{"id":2, "a": 8, "b": 8}"#,
            r#"{"id":2, "a": 9, "b": 9}"#,
        ]));
        let batch = RecordBatch::try_from_iter(vec![("ts", ts), ("payload", payload)])?;

        let new_batch = parser.parse_message_from_records(&batch, true, tx)?;

        // assert batch.size == 4
        match new_batch {
            Message::Records(records) => {
                assert_eq!(records.len(), 1);
                let records = records.first().unwrap();
                assert_eq!(records.records.num_rows(), 4);
            }
            _ => anyhow::bail!("not records"),
        }

        let m = parser.metrics.as_ref().unwrap().ipc();
        assert_eq!(m.parsed_rows.load(SeqCst), 10);
        assert_eq!(m.filter_skipped_rows.load(SeqCst), 1);
        assert_eq!(m.check_skipped_rows.load(SeqCst), 5);
        assert_eq!(m.write_ready_rows.load(SeqCst), 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_parse_message_from_records() -> anyhow::Result<()> {
        let parser = json::json!({
            "parse": {
                "payload": {
                    "json": ""
                }
            },
            "s_model": {
                "name": "site_${point_name}",
                "tags": [
                    {
                        "name": "site_controller_id",
                        "type": "VARCHAR(128)"
                    }
                ],
                "columns": [
                    {
                        "name": "ts",
                        "type": "TIMESTAMP"
                    },
                    {
                        "name": "`value`",
                        "type": "${data_type}"
                    }
                ]
            },
            "model": {
                "name": "site_${point_name}_${site_controller_id}",
                "using": "site_${point_name}",
                "tags": [
                    "site_controller_id"
                ],
                "columns": [
                    "ts",
                    "value"
                ]
            },
            "mutate": [
                {
                    "extract": {
                        "data_type": {
                            "convert": {
                                "boolean": "bool",
                                "float": "double",
                                "string": "varchar(128)"
                            }
                        }
                    }
                },
                {
                    "map": {
                        "ts": {
                            "cast": "ts",
                            "as": "TIMESTAMP(ns)"
                        },
                        "value": {
                            "cast": "value",
                            "as": "VARCHAR"
                        },
                        "site_controller_id": {
                            "cast": "site_controller_id",
                            "as": "VARCHAR"
                        }
                    }
                }
            ]
        });
        let parser: Parser = json::from_value(parser)?;

        let controllers: ArrayRef = Arc::new(StringArray::from(vec!["controller_1"]));
        let points: ArrayRef = Arc::new(StringArray::from(vec!["point_1"]));
        let data_types: ArrayRef = Arc::new(StringArray::from(vec!["string"]));
        let timestamps: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![100]));
        let values: ArrayRef = Arc::new(StringArray::from(vec!["abc"]));
        let batch = RecordBatch::try_from_iter(vec![
            ("site_controller_id", controllers),
            ("point_name", points),
            ("data_type", data_types),
            ("ts", timestamps),
            ("value", values),
        ])?;
        let (tx, _rx) = flume::bounded(10);

        let message = parser.parse_message_from_records(&batch, false, tx)?;
        let Message::Records(mut records) = message else {
            anyhow::bail!("not records")
        };

        records.sort_by(|a, b| match a.table_name().cmp(b.table_name()) {
            o @ (Ordering::Less | Ordering::Greater) => o,
            Ordering::Equal => pretty::pretty_format_batches(&[a.records.clone()])
                .unwrap()
                .to_string()
                .cmp(
                    &pretty::pretty_format_batches(&[b.records.clone()])
                        .unwrap()
                        .to_string(),
                ),
        });

        assert_eq!(records.len(), 1);

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+-------+
| ts                            | value |
+-------------------------------+-------+
| 1970-01-01T00:00:00.000000100 | abc   |
+-------------------------------+-------+"
        );

        let table = message.table;
        assert_eq!(
            table.name,
            Arc::new("site_point_1_controller_1".to_string())
        );
        assert_eq!(
            table.using,
            Some(Arc::new(STable::Model(json::from_value(json::json!({
                "name": "site_point_1",
                "columns":[
                    {
                        "name": "ts",
                        "type": "TIMESTAMP",
                    },{
                        "name": "`value`",
                        "type": "varchar(128)",
                    }
                ],
                "tags": [
                    {
                        "name": "site_controller_id",
                        "type": "VARCHAR(128)"
                    }
                ]
            }))?)))
        );
        assert_eq!(
            table
                .tags
                .as_ref()
                .context("tags not found")?
                .column_by_name("site_controller_id")
                .cloned(),
            {
                let array: ArrayRef = Arc::new(StringArray::from(vec!["controller_1"]));
                Some(array)
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_multi_columns_message_from_records() -> anyhow::Result<()> {
        let parser = json::json!({
            "parse": {
                "payload": {
                    "json": ""
                }
            },
            "s_model": {
                "name": "site",
                "tags": [
                    {
                        "name": "site_controller_id",
                        "type": "VARCHAR(128)"
                    }
                ],
                "columns": [
                    {
                        "name": "ts",
                        "type": "TIMESTAMP"
                    },
                    {
                        "name": "${point_name}",
                        "type": "${data_type}"
                    }
                ]
            },
            "model": {
                "name": "site_${site_controller_id}",
                "using": "site",
                "tags": [
                    "site_controller_id"
                ],
                "columns": [
                    "ts",
                    "${point_name}"
                ]
            },
            "mutate": [
                {
                    "extract": {
                        "data_type": {
                            "convert": {
                                "boolean": "bool",
                                "float": "double",
                                "string": "varchar(128)"
                            }
                        }
                    }
                },
                {
                    "map": {
                        "ts": {
                            "cast": "ts",
                            "as": "TIMESTAMP(ns)"
                        },
                        "${point_name}": {
                            "cast": "value",
                            "as": "VARCHAR"
                        },
                        "site_controller_id": {
                            "cast": "site_controller_id",
                            "as": "VARCHAR"
                        }
                    }
                }
            ]
        });
        let parser: Parser = json::from_value(parser)?;

        let controllers: ArrayRef = Arc::new(StringArray::from(vec![
            "controller_2",
            "controller_2",
            "controller_2",
            "controller_2",
            "controller_1",
            "controller_1",
            "controller_1",
            "controller_1",
        ]));
        let points: ArrayRef = Arc::new(StringArray::from(vec![
            "point_3", "point_5", "point_4", "point_6", "point_1", "point_7", "point_2", "point_8",
        ]));
        let data_types: ArrayRef = Arc::new(StringArray::from(vec![
            "string", "string", "string", "string", "string", "string", "string", "string",
        ]));
        let timestamps: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            100, 101, 100, 101, 100, 101, 100, 101,
        ]));
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "0.1", "0.2", "3", "4", "abc", "def", "true", "false",
        ]));
        let batch = RecordBatch::try_from_iter(vec![
            ("site_controller_id", controllers),
            ("point_name", points),
            ("data_type", data_types),
            ("ts", timestamps),
            ("value", values),
        ])?;
        let (tx, _rx) = flume::bounded(10);

        let message = parser.parse_message_from_records(&batch, false, tx)?;
        let Message::Records(mut records) = message else {
            anyhow::bail!("not records")
        };
        records.sort_by(|a, b| match a.table_name().cmp(b.table_name()) {
            o @ (Ordering::Less | Ordering::Greater) => o,
            Ordering::Equal => pretty::pretty_format_batches(&[a.records.clone()])
                .unwrap()
                .to_string()
                .cmp(
                    &pretty::pretty_format_batches(&[b.records.clone()])
                        .unwrap()
                        .to_string(),
                ),
        });

        assert_eq!(records.len(), 4);

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+---------+---------+
| ts                            | point_1 | point_2 |
+-------------------------------+---------+---------+
| 1970-01-01T00:00:00.000000100 | abc     | true    |
+-------------------------------+---------+---------+"
        );

        let table = message.table;
        assert_eq!(table.name, Arc::new("site_controller_1".to_string()));

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+---------+---------+
| ts                            | point_7 | point_8 |
+-------------------------------+---------+---------+
| 1970-01-01T00:00:00.000000101 | def     | false   |
+-------------------------------+---------+---------+"
        );

        let table = message.table;
        assert_eq!(table.name, Arc::new("site_controller_1".to_string()));
        assert_eq!(
            table
                .tags
                .as_ref()
                .context("tags not found")?
                .column_by_name("site_controller_id")
                .cloned(),
            {
                let array: ArrayRef = Arc::new(StringArray::from(vec!["controller_1"]));
                Some(array)
            }
        );

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+---------+---------+
| ts                            | point_3 | point_4 |
+-------------------------------+---------+---------+
| 1970-01-01T00:00:00.000000100 | 0.1     | 3       |
+-------------------------------+---------+---------+"
        );

        let table = message.table;
        assert_eq!(table.name, Arc::new("site_controller_2".to_string()));
        assert_eq!(
            table
                .tags
                .as_ref()
                .context("tags not found")?
                .column_by_name("site_controller_id")
                .cloned(),
            {
                let array: ArrayRef = Arc::new(StringArray::from(vec!["controller_2"]));
                Some(array)
            }
        );

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+---------+---------+
| ts                            | point_5 | point_6 |
+-------------------------------+---------+---------+
| 1970-01-01T00:00:00.000000101 | 0.2     | 4       |
+-------------------------------+---------+---------+"
        );

        let table = message.table;
        assert_eq!(table.name, Arc::new("site_controller_2".to_string()));
        assert_eq!(
            table
                .tags
                .as_ref()
                .context("tags not found")?
                .column_by_name("site_controller_id")
                .cloned(),
            {
                let array: ArrayRef = Arc::new(StringArray::from(vec!["controller_2"]));
                Some(array)
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_multi_pivot_columns_message_from_records() -> anyhow::Result<()> {
        let parser = json::json!({
            "parse": {
                "payload": {
                    "json": ""
                }
            },
            "s_model": {
                "name": "site",
                "tags": [
                    {
                        "name": "site_controller_id",
                        "type": "VARCHAR(128)"
                    }
                ],
                "columns": [
                    {
                        "name": "ts",
                        "type": "TIMESTAMP",
                    },
                    {
                        "name": "${point_name}",
                        "type": "${data_type}",
                    },
                    {
                        "name": "${controller_name}",
                        "type": "${data_type}",
                    }
                ]
            },
            "model": {
                "name": "site_${site_controller_id}",
                "using": "site",
                "tags": [
                    "site_controller_id"
                ],
                "columns": [
                    "ts",
                    "${point_name}",
                    "${controller_name}"
                ]
            },
            "mutate": [
                {
                    "extract": {
                        "data_type": {
                            "convert": {
                                "boolean": "bool",
                                "float": "double",
                                "string": "varchar(128)"
                            }
                        }
                    }
                },
                {
                    "map": {
                        "ts": {
                            "cast": "ts",
                            "as": "TIMESTAMP(ns)"
                        },
                        "${point_name}": {
                            "cast": "value1",
                            "as": "VARCHAR"
                        },
                        "${controller_name}": {
                            "cast": "value2",
                            "as": "VARCHAR"
                        },
                        "site_controller_id": {
                            "cast": "site_controller_id",
                            "as": "VARCHAR"
                        }
                    }
                }
            ]
        });
        let parser: Parser = json::from_value(parser)?;

        let controllers: ArrayRef = Arc::new(StringArray::from(vec![
            "controller_2",
            "controller_2",
            "controller_2",
            "controller_2",
            "controller_1",
            "controller_1",
            "controller_1",
            "controller_1",
        ]));
        let controller_names: ArrayRef = Arc::new(StringArray::from(vec![
            "controller_name_1",
            "controller_name_3",
            "controller_name_5",
            "controller_name_7",
            "controller_name_2",
            "controller_name_4",
            "controller_name_6",
            "controller_name_8",
        ]));
        let points: ArrayRef = Arc::new(StringArray::from(vec![
            "point_3", "point_5", "point_4", "point_6", "point_1", "point_7", "point_2", "point_8",
        ]));
        let data_types: ArrayRef = Arc::new(StringArray::from(vec![
            "float", "float", "float", "float", "float", "float", "float", "float",
        ]));
        let timestamps: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            100, 101, 100, 101, 100, 101, 100, 101,
        ]));
        let values_1: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.1),
            None,
            Some(0.3),
            Some(0.4),
            Some(0.5),
            Some(0.6),
            Some(0.7),
            Some(0.8),
        ]));
        let values_2: ArrayRef = Arc::new(StringArray::from(vec![
            Some("0.11"),
            Some("0.12"),
            Some("0.13"),
            Some("0.14"),
            Some("0.15"),
            None,
            Some("0.17"),
            Some("0.18"),
        ]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("site_controller_id", controllers, false),
            ("controller_name", controller_names, false),
            ("point_name", points, false),
            ("data_type", data_types, false),
            ("ts", timestamps, false),
            ("value1", values_1, true),
            ("value2", values_2, true),
        ])?;
        let (tx, _rx) = flume::bounded(10);

        let message = parser.parse_message_from_records(&batch, false, tx)?;
        let Message::Records(mut records) = message else {
            anyhow::bail!("not records")
        };
        records.sort_by(|a, b| match a.table_name().cmp(b.table_name()) {
            o @ (Ordering::Less | Ordering::Greater) => o,
            Ordering::Equal => pretty::pretty_format_batches(&[a.records.clone()])
                .unwrap()
                .to_string()
                .cmp(
                    &pretty::pretty_format_batches(&[b.records.clone()])
                        .unwrap()
                        .to_string(),
                ),
        });

        assert_eq!(records.len(), 4);

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+---------+---------+-------------------+-------------------+
| ts                            | point_1 | point_2 | controller_name_2 | controller_name_6 |
+-------------------------------+---------+---------+-------------------+-------------------+
| 1970-01-01T00:00:00.000000100 | 0.5     | 0.7     | 0.15              | 0.17              |
+-------------------------------+---------+---------+-------------------+-------------------+"
        );

        let table = message.table;
        assert_eq!(table.name, Arc::new("site_controller_1".to_string()));
        assert_eq!(
            table
                .tags
                .as_ref()
                .context("tags not found")?
                .column_by_name("site_controller_id")
                .cloned(),
            {
                let array: ArrayRef = Arc::new(StringArray::from(vec!["controller_1"]));
                Some(array)
            }
        );

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+---------+---------+-------------------+-------------------+
| ts                            | point_7 | point_8 | controller_name_4 | controller_name_8 |
+-------------------------------+---------+---------+-------------------+-------------------+
| 1970-01-01T00:00:00.000000101 | 0.6     | 0.8     |                   | 0.18              |
+-------------------------------+---------+---------+-------------------+-------------------+"
        );

        let table = message.table;
        assert_eq!(table.name, Arc::new("site_controller_1".to_string()));
        assert_eq!(
            table
                .tags
                .as_ref()
                .context("tags not found")?
                .column_by_name("site_controller_id")
                .cloned(),
            {
                let array: ArrayRef = Arc::new(StringArray::from(vec!["controller_1"]));
                Some(array)
            }
        );

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+---------+---------+-------------------+-------------------+
| ts                            | point_3 | point_4 | controller_name_1 | controller_name_5 |
+-------------------------------+---------+---------+-------------------+-------------------+
| 1970-01-01T00:00:00.000000100 | 0.1     | 0.3     | 0.11              | 0.13              |
+-------------------------------+---------+---------+-------------------+-------------------+"
        );

        let table = message.table;
        assert_eq!(table.name, Arc::new("site_controller_2".to_string()));
        assert_eq!(
            table
                .tags
                .as_ref()
                .context("tags not found")?
                .column_by_name("site_controller_id")
                .cloned(),
            {
                let array: ArrayRef = Arc::new(StringArray::from(vec!["controller_2"]));
                Some(array)
            }
        );

        let message = records.remove(0);

        assert_eq!(
            pretty::pretty_format_batches(&[message.records])
                .unwrap()
                .to_string(),
            "\
+-------------------------------+---------+---------+-------------------+-------------------+
| ts                            | point_5 | point_6 | controller_name_3 | controller_name_7 |
+-------------------------------+---------+---------+-------------------+-------------------+
| 1970-01-01T00:00:00.000000101 |         | 0.4     | 0.12              | 0.14              |
+-------------------------------+---------+---------+-------------------+-------------------+"
        );

        let table = message.table;
        assert_eq!(table.name, Arc::new("site_controller_2".to_string()));
        assert_eq!(
            table
                .tags
                .as_ref()
                .context("tags not found")?
                .column_by_name("site_controller_id")
                .cloned(),
            {
                let array: ArrayRef = Arc::new(StringArray::from(vec!["controller_2"]));
                Some(array)
            }
        );
        Ok(())
    }

    #[test]
    fn pivot_test() -> anyhow::Result<()> {
        const REPEAT: usize = 2;
        fn repeat<T: Clone>(data: Vec<T>) -> Vec<T> {
            std::iter::repeat_n(data, REPEAT)
                .flat_map(|v| v.into_iter())
                .collect()
        }
        let controllers: ArrayRef = Arc::new(StringArray::from(repeat(vec![
            "controller_2",
            "controller_2",
            "controller_2",
            "controller_2",
            "controller_1",
            "controller_1",
            "controller_1",
            "controller_1",
        ])));
        let points: ArrayRef = Arc::new(StringArray::from(repeat(vec![
            "point_3", "point_5", "point_4", "point_6", "point_1", "point_7", "point_2", "point_8",
        ])));
        let data_types: ArrayRef = Arc::new(StringArray::from(repeat(vec![
            "string", "string", "string", "string", "string", "string", "string", "string",
        ])));
        let timestamps: ArrayRef = Arc::new(TimestampNanosecondArray::from(
            std::iter::repeat_n(vec![100, 101, 100, 101, 100, 101, 100, 101], REPEAT)
                .enumerate()
                .flat_map(|(i, v)| v.into_iter().map(move |a| (a + i) as i64))
                .collect::<Vec<_>>(),
        ));
        let values: ArrayRef = Arc::new(StringArray::from(repeat(vec![
            "0.1", "0.2", "3", "4", "abc", "def", "true", "false",
        ])));
        let batch = RecordBatch::try_from_iter(vec![
            ("site_controller_id", controllers),
            ("point_name", points),
            ("data_type", data_types),
            ("ts", timestamps),
            ("value", values),
        ])?;
        let batches = pivot(&batch, "ts", &[("point_name", "value")], None).unwrap();
        let res = ["\
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+
| site_controller_id | data_type | ts                            | point_1 | point_2 | point_3 | point_4 |
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+
| controller_2       | string    | 1970-01-01T00:00:00.000000100 | abc     | true    | 0.1     | 3       |
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+","\
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+
| site_controller_id | data_type | ts                            | point_5 | point_6 | point_7 | point_8 |
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+
| controller_2       | string    | 1970-01-01T00:00:00.000000102 | 0.2     | 4       | def     | false   |
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+","\
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+---------+---------+---------+---------+
| site_controller_id | data_type | ts                            | point_1 | point_2 | point_3 | point_4 | point_5 | point_6 | point_7 | point_8 |
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+---------+---------+---------+---------+
| controller_2       | string    | 1970-01-01T00:00:00.000000101 | abc     | true    | 0.1     | 3       | 0.2     | 4       | def     | false   |
+--------------------+-----------+-------------------------------+---------+---------+---------+---------+---------+---------+---------+---------+"];
        for batch in batches {
            let f = arrow::util::pretty::pretty_format_batches(&[batch]);
            // println!("{}", f.unwrap());
            assert!(res.contains(&f.unwrap().to_string().as_str()));
        }

        Ok(())
    }

    #[test]
    fn test_process_on_abnormal_serde() {
        let process_on_abnormal = ProcessOnAbnormal::default();
        let json = serde_json::to_string_pretty(&process_on_abnormal).unwrap();
        println!("{}", json);

        let process = r#"{
            "cache": {
                "max_size": "0GB",
                "max_size_unit": "GB",
                "max_size_value": 0,
                "location": "cache",
                "on_fail": "skip"
            },
            "archive": {
                "keep_days": "0d",
                "keep_days_unit": "d",
                "keep_days_value": 0,
                "max_size": "0GB",
                "max_size_unit": "GB",
                "max_size_value": 0,
                "location": "",
                "on_fail": "rotate"
            },
            "primary_timestamp_overflow": "archive",
            "primary_timestamp_null": "archive",
            "primary_key_null": "archive",
            "table_name_length_overflow": "archive",
            "table_name_contains_illegal_char": {
                "replace_to": ""
            },
            "variable_not_exist_in_table_name_template": {
                "replace_to": ""
            },
            "field_name_not_found": "add_field",
            "field_name_length_overflow": "archive",
            "field_length_extend": true,
            "field_length_overflow": "archive",
            "ingesting_error": "archive",
            "connection_timeout_in_second": "1s",
            "connection_timeout_in_second_unit": "s",
            "connection_timeout_in_second_value": 1
        }"#;
        let process: super::ProcessOnAbnormal = serde_json::from_str(process).unwrap();
        dbg!(&process);
    }

    #[tokio::test]
    async fn test_parse_message_from_records_2() {
        let parser = r#"{
            "global": {
                "table_name_length_overflow": "archive",
                "table_name_contains_illegal_char": "break",
                "variable_not_exist_in_table_name_template": "skip"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let (tx, _rx) = flume::bounded(10);

        // test1: normal
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampNanosecondArray::from(vec![None])) as ArrayRef,
            ),
            ("str1", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
            ("int1", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());

        // test2: the length of table name exceeds the limit, and the processing method is 'archive'
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![123])) as ArrayRef,
            ),
            (
                "str1",
                Arc::new(StringArray::from(vec!["1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567"])) as ArrayRef,
            ),
            ("int1", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }

        // test3: table name contains illegal characters, and the processing method is 'break'
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![123])) as ArrayRef,
            ),
            ("str1", Arc::new(StringArray::from(vec!["1.2"])) as ArrayRef),
            ("int1", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Transform error: the table name 'table_1.2' should not contain illegal characters"
        );

        // test4: table name variable mistake, and the processing method is 'skip'
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![123])) as ArrayRef,
            ),
            ("str2", Arc::new(StringArray::from(vec!["1.2"])) as ArrayRef),
            ("int1", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_table_name_length_overflow() {
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampNanosecondArray::from(vec![1])) as ArrayRef,
            ),
            (
                "str1",
                Arc::new(StringArray::from(vec!["1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567"])) as ArrayRef,
            ),
            ("int1", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let (tx, _rx) = flume::bounded(10);

        // test1: archive
        let parser = r#"{
            "global": {
                "table_name_length_overflow": "archive"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }

        // test2: skip
        let parser = r#"{
            "global": {
                "table_name_length_overflow": "skip"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }

        // test3: break
        let parser = r#"{
            "global": {
                "table_name_length_overflow": "break"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Transform error: the length of table name 'table_1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567' should not exceed 192"
        );

        // test4: truncate
        let parser = r#"{
            "global": {
                "table_name_length_overflow": "truncate"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec[0].table_name(), "table_123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456");
            }
            _ => unreachable!(),
        }

        // test5: truncate and archive
        let parser = r#"{
            "global": {
                "table_name_length_overflow": "truncate_and_archive"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec[0].table_name(), "table_123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456");
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_table_name_contains_illegal_char() {
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![123])) as ArrayRef,
            ),
            ("str1", Arc::new(StringArray::from(vec!["1.2"])) as ArrayRef),
            ("int1", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let (tx, _rx) = flume::bounded(10);

        // test1: archive
        let parser = r#"{
            "global": {
                "table_name_contains_illegal_char": "archive"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();

        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }

        // test2: skip
        let parser = r#"{
            "global": {
                "table_name_contains_illegal_char": "skip"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }

        // test3: break
        let parser = r#"{
            "global": {
                "table_name_contains_illegal_char": "break"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Transform error: the table name 'table_1.2' should not contain illegal characters"
        );

        // test4: replace illegal char
        let parser = r#"{
            "global": {
                "table_name_contains_illegal_char": {"replace_to": "_"}
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec[0].table_name(), "table_1_2");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_tiny_template() {
        let mut template = TinyTemplate::new();
        template.add_template("name", "table_{var1}").unwrap();

        // test1: normal
        let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{
            "var1": "value1"
        }"#,
        )
        .unwrap();
        let result = template.render("name", &map);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "table_value1");

        // test2: variable not found
        let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{
            "var2": "value1"
        }"#,
        )
        .unwrap();
        let result = template.render("name", &map);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Failed to find value 'var1' from path 'var1'"));
    }

    #[test]
    fn test_get_variables_from_template() {
        let template = "table_{var1}_{var2}_{var3}";
        let re = Regex::new(r"\{(\w+)\}").unwrap();

        let variables = re
            .captures_iter(template)
            .map(|c| c.get(1).unwrap().as_str())
            .collect::<Vec<_>>();
        assert_eq!(variables, vec!["var1", "var2", "var3"]);
    }

    #[tokio::test]
    async fn test_variable_not_exist_in_table_name_template() {
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![123])) as ArrayRef,
            ),
            (
                "str1",
                Arc::new(StringArray::from(vec!["12345"])) as ArrayRef,
            ),
            ("int1", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let (tx, _rx) = flume::bounded(10);

        // test1: skip
        let parser = r#"{
            "global": {
                "variable_not_exist_in_table_name_template": "skip"
            },
            "parse": {},
            "model": {
                "name": "table_{str2}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }

        // test2: leave blank
        let parser = r#"{
            "global": {
                "variable_not_exist_in_table_name_template": "leave_blank"
            },
            "parse": {},
            "model": {
                "name": "table_{str2}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec[0].table_name(), "table_");
            }
            _ => unreachable!(),
        }

        // test3: replace to
        let parser = r#"{
            "global": {
                "variable_not_exist_in_table_name_template": {"replace_to": "xyz"}
            },
            "parse": {},
            "model": {
                "name": "table_{str2}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec[0].table_name(), "table_xyz");
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_primary_timestamp_null_use_current_time() {
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampNanosecondArray::from(vec![None, None])) as ArrayRef,
            ),
            (
                "str1",
                Arc::new(StringArray::from(vec!["12345", "67890"])) as ArrayRef,
            ),
            ("int1", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ])
        .unwrap();
        let (tx, _rx) = flume::bounded(10);

        // test1: use current time
        let parser = r#"{
            "global": {
                "primary_timestamp_null": "use_current_time"
            },
            "parse": {},
            "model": {
                "name": "table_{str1}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 2);
                let records = vec[0].records.clone();
                assert_eq!(records.column(0).null_count(), 0);
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_field_name_length_overflow() {
        let record = RecordBatch::try_from_iter([
            (
                "ts",
                Arc::new(TimestampNanosecondArray::from(vec![None, None])) as ArrayRef,
            ),
            (
                "str1234567890123456789012345678901234567890123456789012345678901234567890",
                Arc::new(StringArray::from(vec!["12345", "67890"])) as ArrayRef,
            ),
            ("int1", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ])
        .unwrap();
        let (tx, _rx) = flume::bounded(10);

        // test1: columns empty, use the others not in tags & skip
        let parser = r#"{
            "global": {
                "field_name_length_overflow": "skip"
            },
            "parse": {},
            "model": {
                "name": "table_{str1234567890123456789012345678901234567890123456789012345678901234567890}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": []
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }

        // test2: archive
        let parser = r#"{
            "global": {
                "field_name_length_overflow": "archive"
            },
            "parse": {},
            "model": {
                "name": "table_{str1234567890123456789012345678901234567890123456789012345678901234567890}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1234567890123456789012345678901234567890123456789012345678901234567890"]
            }
        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();
        let result = parser.parse_message_from_records(&record, true, tx.clone());
        assert!(result.is_ok());
        match result.unwrap() {
            Message::Records(vec) => {
                assert_eq!(vec.len(), 0);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_modify_maximum_timestamp() {
        let parser = r#"{
            "global": {
                "field_name_length_overflow": "archive"
            },
            "parse": {},
            "model": {
                "name": "table_{str1234567890123456789012345678901234567890123456789012345678901234567890}",
                "using": "stable1",
                "tags": ["int1"],
                "columns": ["ts", "str1234567890123456789012345678901234567890123456789012345678901234567890"]
            }
        }"#;
        let mut parser: Parser = serde_json::from_str(parser).unwrap();

        parser.set_maximum_timestamp(Utc::now());
        dbg!(parser);
    }

    #[tokio::test]
    async fn test_parse_record_to_sql() {
        let parser = r#"{
            "parse": {
                "value": {"json": ""}
            },
            "model": {
                "name": "t_${DEV_ID}",
                "using": "deva",
                "tags": [ "dev_id" ],
                "columns": [ "_ts", "_val0", "_val1" ]
            },
            "mutate": [{
                "map": {
                    "_ts": {
                        "cast": "_ts",
                        "as": "TIMESTAMP(ms)"
                    },
                    "_val0": {
                        "cast": "_val0",
                        "as": "INT"
                    },
                    "_val1": {
                        "cast": "_val1",
                        "as": "INT"
                    },
                    "dev_id": {
                        "cast": "DEV_ID",
                        "as": "VARCHAR"
                    }
                }
            }]

        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();

        let raw_data = arrow::array::record_batch!(
            ("topic", Utf8, ["test", "test", "test"]),
            (
                "value",
                Utf8,
                [
                    r#"{"_ts": "2024-12-02T18:00:00+08:00", "_val0": 12, "DEV_ID": "2212"}"#,
                    r#"{"_ts": "2024-12-02T18:00:00+08:00", "_val1": 13, "DEV_ID": "2213"}"#,
                    r#"{"_ts": "2024-12-02T18:00:01+08:00", "_val0": 14, "DEV_ID": "2212"}"#
                ]
            )
        )
        .unwrap();
        let (tx, _rx) = flume::bounded(10);

        let records = parser
            .parse_message_from_records(&raw_data, true, tx.clone())
            .unwrap();
        // assert_eq!(records.len(), 2);
        if let super::Message::Records(records) = records {
            assert_eq!(records.len(), 2);
            for record in records {
                let sql = record.sql_insert_part(taos::Precision::Millisecond, true, true, None);
                if record.table_name() == "t_2212" {
                    assert_eq!(sql.len(), 1);
                    assert_eq!(
                        sql[0].0,
                        r#"`t_2212` using `deva` (`dev_id`) tags("2212") (`_ts`,`_val0`) values(1733133600000,12)(1733133601000,14)"#
                    );
                    assert_eq!(sql[0].1, 2);
                } else if record.table_name() == "t_2213" {
                    assert_eq!(sql.len(), 1);
                    assert_eq!(
                        sql[0].0,
                        r#"`t_2213` using `deva` (`dev_id`) tags("2213") (`_ts`,`_val1`) values(1733133600000,13)"#
                    );
                    assert_eq!(sql[0].1, 1);
                } else {
                    panic!("unknown table");
                }
            }
        } else {
            panic!("not parsed as records");
        }
    }
}

#[cfg(test)]
mod test {
    use super::Parser;

    #[tokio::test]
    async fn test_sql_insert_part() {
        let parser = r#"{
            "parse": {
                "value": {"json": ""}
            },
            "model": {
                "name": "t_${DEV_ID}",
                "using": "deva",
                "tags": [ "dev_id" ],
                "columns": [ "_ts", "_val0", "_val1" ]
            },
            "mutate": [{
                "map": {
                    "_ts": {
                        "cast": "_ts",
                        "as": "TIMESTAMP(ms)"
                    },
                    "_val0": {
                        "cast": "_val0",
                        "as": "INT"
                    },
                    "_val1": {
                        "cast": "_val1",
                        "as": "INT"
                    },
                    "dev_id": {
                        "cast": "DEV_ID",
                        "as": "VARCHAR"
                    }
                }
            }]

        }"#;
        let parser: Parser = serde_json::from_str(parser).unwrap();

        let raw_data = arrow::array::record_batch!(
            ("topic", Utf8, ["test", "test", "test"]),
            (
                "value",
                Utf8,
                [
                    r#"{"_ts": "2024-12-02T18:00:00+08:00", "_val0": 12, "DEV_ID": "2212"}"#,
                    r#"{"_ts": "2024-12-02T18:00:00+08:00", "_val1": 13, "DEV_ID": "2213"}"#,
                    r#"{"_ts": "2024-12-02T18:00:01+08:00", "DEV_ID": "2212"}"#
                ]
            )
        )
        .unwrap();
        let (tx, _rx) = flume::bounded(10);

        let records = parser
            .parse_message_from_records(&raw_data, false, tx.clone())
            .unwrap();

        if let super::Message::Records(records) = records {
            println!("--sql_insert_part--");
            for record in &records {
                let sql = record.sql_insert_part(taos::Precision::Millisecond, true, true, None);
                dbg!(&sql);
            }
            println!("--sql_insert_part_skip_null--");
            for record in &records {
                let sql = record.sql_insert_part_skip_null(taos::Precision::Millisecond);
                dbg!(&sql);
            }
        }
    }
}
