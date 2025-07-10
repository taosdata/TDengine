use std::sync::LazyLock;

use anyhow::Context;
use archive::{Archive, Cache};
use regex::Regex;
use serde::{Deserialize, Serialize};
use taos::Itertools;
use tinytemplate::TinyTemplate;

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingStrategy {
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingConnectionError {
    Archive,
    Skip,
    Break,
    Cache,
}

impl HandlingConnectionError {
    pub fn handle(&self, err: String) -> anyhow::Result<(HandlingResult, String)> {
        match self {
            HandlingConnectionError::Archive => {
                tracing::trace!("{err}: archive record");
                Ok((HandlingResult::Archive, err))
            }
            HandlingConnectionError::Skip => {
                tracing::warn!("{err}: skip record");
                Ok((HandlingResult::Skip, err))
            }
            HandlingConnectionError::Cache => {
                tracing::debug!("{err}: write to cache and retry record");
                Ok((HandlingResult::Retry, err))
            }
            HandlingConnectionError::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingTableNotExist {
    Archive,
    Skip,
    Break,
    Retry,
}

impl HandlingTableNotExist {
    pub fn handle(&self, err: String) -> anyhow::Result<(HandlingResult, String)> {
        match self {
            HandlingTableNotExist::Archive => {
                tracing::trace!("{err}: archive record");
                Ok((HandlingResult::Archive, err))
            }
            HandlingTableNotExist::Skip => {
                tracing::warn!("{err}: skip record");
                Ok((HandlingResult::Skip, err))
            }
            HandlingTableNotExist::Retry => {
                tracing::debug!("{err}: retry record");
                Ok((HandlingResult::Retry, err))
            }
            HandlingTableNotExist::Break => {
                tracing::error!("{err}: break task");
                anyhow::bail!(err)
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingPrimaryTimestampNull {
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
                Ok((HandlingResult::Modify(vec![]), err))
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingDataOverflow {
    Archive,
    Skip,
    Break,
    Truncate,
    TruncateAndArchive,
}

impl HandlingDataOverflow {
    pub fn handle(
        &self,
        datas: Vec<String>,
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
                let data_truncated = datas
                    .iter()
                    .map(|data| {
                        let data_truncated = data.chars().take(length).collect();
                        tracing::warn!("{err}, truncate '{data}' to '{data_truncated}'");
                        data_truncated
                    })
                    .collect_vec();
                Ok((HandlingResult::Modify(data_truncated), err))
            }
            HandlingDataOverflow::TruncateAndArchive => {
                let data_truncated = datas
                    .iter()
                    .map(|data| {
                        let data_truncated = data.chars().take(length).collect();
                        tracing::warn!(
                            "{err}, truncate '{data}' to '{data_truncated}' and archive record"
                        );
                        data_truncated
                    })
                    .collect_vec();
                Ok((HandlingResult::ModifyAndArchive(data_truncated), err))
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingTableNameContainsIllegalChar {
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
                Ok((HandlingResult::Modify(vec![table_name_replaced]), err))
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingTableNameVariableMistake {
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
        let template_table_name = table_name_org.replace("${", "{");
        // get all variables in table name
        static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{(\w+)\}").unwrap());
        let variables = RE
            .captures_iter(table_name_org)
            .map(|c| c.get(1).unwrap().as_str())
            .collect::<Vec<_>>();
        // clone data
        let mut map_data = data
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
                    if !map_data.contains_key(variable) {
                        map_data.insert(
                            variable.to_string(),
                            serde_json::Value::String(String::default()),
                        );
                    }
                });
                let mut template = TinyTemplate::new();
                template.add_template("name", &template_table_name)?;
                match template.render_value("name", &serde_json::Value::from(map_data.clone())) {
                    Ok(name) => Ok((HandlingResult::Modify(vec![name]), err)),
                    Err(e) => {
                        tracing::error!(
                            "{err}, set to left blank, but rendering table name failed: {e:#}"
                        );
                        Ok((HandlingResult::Modify(vec![String::new()]), err))
                    }
                }
            }
            HandlingTableNameVariableMistake::ReplaceTo(str) => {
                // fill map with specified string
                variables.iter().for_each(|&variable| {
                    if !map_data.contains_key(variable) {
                        map_data
                            .insert(variable.to_string(), serde_json::Value::String(str.clone()));
                    }
                });
                let mut template = TinyTemplate::new();
                template.add_template("name", &template_table_name)?;
                match template.render_value("name", &serde_json::Value::from(map_data.clone())) {
                    Ok(name) => Ok((HandlingResult::Modify(vec![name]), err)),
                    Err(e) => {
                        tracing::error!(
                            "{err}, set to replace to specified string, but rendering table name failed: {e:#}"
                        );
                        Ok((HandlingResult::Modify(vec![String::new()]), err))
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum HandlingFieldNameNotFound {
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
                tracing::warn!("{err}: add field and retry record");
                Ok((HandlingResult::Retry, err))
            }
        }
    }
}

fn default_database_connection_error() -> HandlingConnectionError {
    HandlingConnectionError::Cache
}

fn default_database_not_exist() -> HandlingStrategy {
    HandlingStrategy::Break
}

fn default_table_not_exist() -> HandlingTableNotExist {
    HandlingTableNotExist::Retry
}

fn default_primary_timestamp_overflow() -> HandlingStrategy {
    HandlingStrategy::Archive
}

fn default_primary_timestamp_null() -> HandlingPrimaryTimestampNull {
    HandlingPrimaryTimestampNull::Archive
}

fn default_primary_key_null() -> HandlingStrategy {
    HandlingStrategy::Archive
}

fn default_table_name_length_overflow() -> HandlingDataOverflow {
    HandlingDataOverflow::Archive
}

fn default_table_name_contains_illegal_char() -> HandlingTableNameContainsIllegalChar {
    HandlingTableNameContainsIllegalChar::ReplaceTo("_".to_string())
}

fn default_variable_not_exist_in_table_name_template() -> HandlingTableNameVariableMistake {
    HandlingTableNameVariableMistake::ReplaceTo("NULL".to_string())
}

fn default_field_name_not_found() -> HandlingFieldNameNotFound {
    HandlingFieldNameNotFound::AddField
}

fn default_field_name_length_overflow() -> HandlingDataOverflow {
    HandlingDataOverflow::Archive
}

fn default_field_length_extend() -> bool {
    true
}

fn default_field_length_overflow() -> HandlingDataOverflow {
    HandlingDataOverflow::Archive
}

fn default_ingesting_error() -> HandlingStrategy {
    HandlingStrategy::Archive
}

fn default_connection_timeout_in_second() -> String {
    "30s".to_string()
}

fn default_connection_timeout_in_second_value() -> usize {
    30
}

fn default_connection_timeout_in_second_unit() -> String {
    "s".to_string()
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ProcessOnAbnormal {
    #[serde(default = "default_database_connection_error")]
    pub database_connection_error: HandlingConnectionError,
    #[serde(default = "default_database_not_exist")]
    pub database_not_exist: HandlingStrategy,
    #[serde(default = "default_table_not_exist")]
    pub table_not_exist: HandlingTableNotExist,
    #[serde(default = "default_primary_timestamp_overflow")]
    pub primary_timestamp_overflow: HandlingStrategy,
    #[serde(default = "default_primary_timestamp_null")]
    pub primary_timestamp_null: HandlingPrimaryTimestampNull,
    #[serde(default = "default_primary_key_null")]
    pub primary_key_null: HandlingStrategy,
    #[serde(default = "default_table_name_length_overflow")]
    pub table_name_length_overflow: HandlingDataOverflow,
    #[serde(default = "default_table_name_contains_illegal_char")]
    pub table_name_contains_illegal_char: HandlingTableNameContainsIllegalChar,
    #[serde(default = "default_variable_not_exist_in_table_name_template")]
    pub variable_not_exist_in_table_name_template: HandlingTableNameVariableMistake,
    #[serde(default = "default_field_name_not_found")]
    pub field_name_not_found: HandlingFieldNameNotFound,
    #[serde(default = "default_field_name_length_overflow")]
    pub field_name_length_overflow: HandlingDataOverflow,
    #[serde(default = "default_field_length_extend")]
    pub field_length_extend: bool,
    #[serde(default = "default_field_length_overflow")]
    pub field_length_overflow: HandlingDataOverflow,
    #[serde(default = "default_ingesting_error")]
    pub ingesting_error: HandlingStrategy,

    #[serde(default = "default_connection_timeout_in_second")]
    pub connection_timeout_in_second: String,
    #[serde(default = "default_connection_timeout_in_second_value")]
    pub connection_timeout_in_second_value: usize,
    #[serde(default = "default_connection_timeout_in_second_unit")]
    pub connection_timeout_in_second_unit: String,

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
            database_connection_error: default_database_connection_error(),
            database_not_exist: default_database_not_exist(),
            table_not_exist: default_table_not_exist(),
            primary_timestamp_overflow: default_primary_timestamp_overflow(),
            primary_timestamp_null: default_primary_timestamp_null(),
            primary_key_null: default_primary_key_null(),
            table_name_length_overflow: default_table_name_length_overflow(),
            table_name_contains_illegal_char: default_table_name_contains_illegal_char(),
            variable_not_exist_in_table_name_template:
                default_variable_not_exist_in_table_name_template(),
            field_name_not_found: default_field_name_not_found(),
            field_name_length_overflow: default_field_name_length_overflow(),
            field_length_extend: default_field_length_extend(),
            field_length_overflow: default_field_length_overflow(),
            ingesting_error: default_ingesting_error(),
            connection_timeout_in_second: default_connection_timeout_in_second(),
            connection_timeout_in_second_value: default_connection_timeout_in_second_value(),
            connection_timeout_in_second_unit: default_connection_timeout_in_second_unit(),
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
    Modify(Vec<String>),
    ModifyAndArchive(Vec<String>),
    Retry,
}

#[non_exhaustive]
pub enum ProcessOnAbnormalEnum<'a> {
    DatabaseConnectionError(&'a HandlingConnectionError),
    DatabaseNotExist(&'a HandlingStrategy),
}
