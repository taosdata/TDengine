use anyhow::Context;
use archive::Archive;
use cache::Cache;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tinytemplate::TinyTemplate;

pub mod archive;
pub mod cache;

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingStrategy {
    #[default]
    Archive,
    Skip,
    Break,
}

impl HandlingStrategy {
    pub fn handle(&self, err: String) -> anyhow::Result<(HandlingResult, String)> {
        match self {
            HandlingStrategy::Archive => {
                tracing::trace!("{err}: archive record");
                Ok((HandlingResult::Archive, err))
            }
            HandlingStrategy::Skip => {
                tracing::warn!("{err}: skip record");
                Ok((HandlingResult::Skip, err))
            }
            HandlingStrategy::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingPrimaryTimestampNull {
    #[default]
    Archive,
    Skip,
    Break,
    UseCurrentTime,
}

impl HandlingPrimaryTimestampNull {
    pub fn handle(&self, err: String) -> anyhow::Result<(HandlingResult, String)> {
        match self {
            HandlingPrimaryTimestampNull::Archive => {
                tracing::trace!("{err}: archive record");
                Ok((HandlingResult::Archive, err))
            }
            HandlingPrimaryTimestampNull::Skip => {
                tracing::warn!("{err}: skip record");
                Ok((HandlingResult::Skip, err))
            }
            HandlingPrimaryTimestampNull::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
            HandlingPrimaryTimestampNull::UseCurrentTime => {
                tracing::debug!("{err}: use current time");
                Ok((HandlingResult::Modify(String::default()), err))
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingDataOverflow {
    #[default]
    Archive,
    Skip,
    Break,
    Truncate,
    TruncateAndArchive,
}

impl HandlingDataOverflow {
    pub fn handle(
        &self,
        data: &String,
        length: usize,
        err: String,
    ) -> anyhow::Result<(HandlingResult, String)> {
        match self {
            HandlingDataOverflow::Archive => {
                tracing::trace!("{err}: archive record");
                Ok((HandlingResult::Archive, err))
            }
            HandlingDataOverflow::Skip => {
                tracing::warn!("{err}: skip record");
                Ok((HandlingResult::Skip, err))
            }
            HandlingDataOverflow::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
            HandlingDataOverflow::Truncate => {
                let data_truncated = data.chars().take(length).collect();
                tracing::warn!("{err}, truncate '{data}' to '{data_truncated}'");
                Ok((HandlingResult::Modify(data_truncated), err))
            }
            HandlingDataOverflow::TruncateAndArchive => {
                let data_truncated = data.chars().take(length).collect();
                tracing::warn!("{err}, truncate '{data}' to '{data_truncated}' and archive record");
                Ok((HandlingResult::ModifyAndArchive(data_truncated), err))
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingTableNameContainsIllegalChar {
    #[default]
    Archive,
    Skip,
    Break,
    ReplaceTo(String),
}

impl HandlingTableNameContainsIllegalChar {
    pub fn handle(
        &self,
        table_name: &String,
        err: String,
    ) -> anyhow::Result<(HandlingResult, String)> {
        match self {
            HandlingTableNameContainsIllegalChar::Archive => {
                tracing::trace!("{err}: archive record");
                Ok((HandlingResult::Archive, err))
            }
            HandlingTableNameContainsIllegalChar::Skip => {
                tracing::warn!("{err}: skip record");
                Ok((HandlingResult::Skip, err))
            }
            HandlingTableNameContainsIllegalChar::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
            HandlingTableNameContainsIllegalChar::ReplaceTo(str) => {
                let table_name_replaced = table_name
                    .chars()
                    .map(|c| if c == '.' { str.clone() } else { c.to_string() })
                    .collect::<String>();
                tracing::warn!(
                    "{err}, convert table name '{table_name}' to '{table_name_replaced}'"
                );
                Ok((HandlingResult::Modify(table_name_replaced), err))
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingTableNameVariableMistake {
    #[default]
    Skip,
    LeaveBlank,
    ReplaceTo(String),
}

impl HandlingTableNameVariableMistake {
    pub fn handle(
        &self,
        table_name_org: &str,
        data: &serde_json::Value,
        err: String,
    ) -> anyhow::Result<(HandlingResult, String)> {
        // get all variables in table name
        let re = Regex::new(r"\{(\w+)\}").unwrap();
        let variables = re
            .captures_iter(table_name_org)
            .map(|c| c.get(1).unwrap().as_str())
            .collect::<Vec<_>>();
        // clone data
        let mut data = data
            .as_object()
            .context("table name mistake handle needs map type data")?
            .clone();
        match self {
            HandlingTableNameVariableMistake::Skip => {
                tracing::warn!("{err}: skip record");
                Ok((HandlingResult::Skip, err))
            }
            HandlingTableNameVariableMistake::LeaveBlank => {
                // fill map with empty string
                variables.iter().for_each(|&variable| {
                    if !data.contains_key(variable) {
                        data.insert(
                            variable.to_string(),
                            serde_json::Value::String(String::default()),
                        );
                    }
                });
                let mut template = TinyTemplate::new();
                template.add_template("name", table_name_org)?;
                match template.render_value("name", &serde_json::Value::from(data)) {
                    Ok(name) => Ok((HandlingResult::Modify(name), err)),
                    Err(e) => {
                        tracing::error!(
                            "{err}, set to left blank, but rendering table name failed: {e:#}"
                        );
                        Ok((HandlingResult::Modify(String::default()), err))
                    }
                }
            }
            HandlingTableNameVariableMistake::ReplaceTo(str) => {
                // fill map with specified string
                variables.iter().for_each(|&variable| {
                    if !data.contains_key(variable) {
                        data.insert(variable.to_string(), serde_json::Value::String(str.clone()));
                    }
                });
                let mut template = TinyTemplate::new();
                template.add_template("name", table_name_org)?;
                match template.render_value("name", &serde_json::Value::from(data)) {
                    Ok(name) => Ok((HandlingResult::Modify(name), err)),
                    Err(e) => {
                        tracing::error!(
                            "{err}, set to replace to specified string, but rendering table name failed: {e:#}"
                        );
                        Ok((HandlingResult::Modify(String::default()), err))
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingFieldNameNotFound {
    #[default]
    Archive,
    Skip,
    Break,
    AddField,
}

impl HandlingFieldNameNotFound {
    pub fn handle(&self, err: String) -> anyhow::Result<(HandlingResult, String)> {
        match self {
            HandlingFieldNameNotFound::Archive => {
                tracing::trace!("{err}: archive record");
                Ok((HandlingResult::Archive, err))
            }
            HandlingFieldNameNotFound::Skip => {
                tracing::warn!("{err}: skip record");
                Ok((HandlingResult::Skip, err))
            }
            HandlingFieldNameNotFound::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
            HandlingFieldNameNotFound::AddField => {
                tracing::warn!("{err}: add field");
                Ok((HandlingResult::Modify(String::default()), err))
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ProcessOnAbnormal {
    #[serde(default)]
    pub primary_timestamp_overflow: HandlingStrategy,
    #[serde(default)]
    pub primary_timestamp_null: HandlingPrimaryTimestampNull,
    #[serde(default)]
    pub primary_key_null: HandlingStrategy,
    #[serde(default)]
    pub table_name_length_overflow: HandlingDataOverflow,
    #[serde(default)]
    pub table_name_contains_illegal_char: HandlingTableNameContainsIllegalChar,
    #[serde(default)]
    pub variable_not_exist_in_table_name_template: HandlingTableNameVariableMistake,
    #[serde(default)]
    pub field_name_not_found: HandlingFieldNameNotFound,
    #[serde(default)]
    pub field_name_length_overflow: HandlingDataOverflow,
    #[serde(default)]
    pub field_length_extend: bool,
    #[serde(default)]
    pub field_length_overflow: HandlingDataOverflow,
    #[serde(default)]
    pub ingesting_error: HandlingStrategy,

    #[serde(default)]
    pub connection_timeout_in_second: u64,

    /// Cache configuration, when the database reports a resource shortage error
    #[serde(default)]
    pub cache: Cache,
    /// Archive configuration, when there is abnormal data
    #[serde(default)]
    pub archive: Archive,
}

impl Default for ProcessOnAbnormal {
    fn default() -> Self {
        Self {
            primary_timestamp_overflow: HandlingStrategy::default(),
            primary_timestamp_null: HandlingPrimaryTimestampNull::default(),
            primary_key_null: HandlingStrategy::default(),
            table_name_length_overflow: HandlingDataOverflow::default(),
            table_name_contains_illegal_char: HandlingTableNameContainsIllegalChar::ReplaceTo(
                "_".to_string(),
            ),
            variable_not_exist_in_table_name_template: HandlingTableNameVariableMistake::ReplaceTo(
                "NULL".to_string(),
            ),
            field_name_not_found: HandlingFieldNameNotFound::AddField,
            field_name_length_overflow: HandlingDataOverflow::default(),
            field_length_extend: true,
            field_length_overflow: HandlingDataOverflow::default(),
            ingesting_error: HandlingStrategy::default(),
            connection_timeout_in_second: 0,
            cache: Cache::default(),
            archive: Archive::default(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub enum HandlingResult {
    #[default]
    Skip,
    Archive,
    Modify(String),
    ModifyAndArchive(String),
}
