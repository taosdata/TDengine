use std::str::FromStr;

use anyhow::bail;
use itertools::Itertools;

use taosx_ipc::prelude::IpcDataType;

#[derive(Debug, Clone)]
pub struct CsvColumn {
    pub name: String,
    pub index: usize,
    pub is_tag: bool,
    pub tag_type: Option<IpcDataType>,
    pub is_primary_key: bool,
    pub is_timestamp: bool,
    pub is_expression: bool,
}

impl CsvColumn {
    pub const POINT_ID: &'static str = "point_id";
    pub const TAG_NAME: &'static str = "tag_name";
    pub const ENABLED: &'static str = "enabled";
    pub const STABLE: &'static str = "stable";
    pub const TBNAME: &'static str = "tbname";
    pub const VALUE_COL: &'static str = "value_col";
    pub const QUALITY_COL: &'static str = "quality_col";
    pub const TYPE: &'static str = "type";
    pub const TS_COL: &'static str = "ts_col";
    pub const REQUEST_TS_COL: &'static str = "request_ts_col";
    pub const RECEIVED_TS_COL: &'static str = "received_ts_col";
    pub const VALUE_TRANSFORM: &'static str = "value_transform";
    pub const TS_TRANSFORM: &'static str = "ts_transform";
    pub const REQUEST_TS_TRANSFORM: &'static str = "request_ts_transform";
    pub const RECEIVED_TS_TRANSFORM: &'static str = "received_ts_transform";

    pub fn try_new(name: &str, index: usize) -> anyhow::Result<Self> {
        let col = match name {
            Self::POINT_ID | Self::TAG_NAME | "TagName" => Self::default(name, index),
            Self::ENABLED => Self::default(name, index),
            Self::STABLE | Self::TBNAME => Self::expression_col(name, index),
            Self::VALUE_COL => Self::default(name, index),
            Self::QUALITY_COL => Self::default(name, index),
            Self::TYPE => Self::default(name, index),
            Self::VALUE_TRANSFORM
            | Self::TS_TRANSFORM
            | Self::REQUEST_TS_TRANSFORM
            | Self::RECEIVED_TS_TRANSFORM => Self::transform_col(name, index),
            Self::TS_COL | Self::REQUEST_TS_COL | Self::RECEIVED_TS_COL => {
                Self::timestamp_col(name, index)
            }
            _ => {
                if name.starts_with("tag::") {
                    Self::tag_col(name, index)?
                } else {
                    Self::default(name, index)
                }
            }
        };

        Ok(col)
    }

    fn default(name: &str, index: usize) -> Self {
        Self {
            name: name.to_string(),
            index,
            is_tag: false,
            tag_type: None,
            is_primary_key: false,
            is_timestamp: false,
            is_expression: false,
        }
    }

    fn timestamp_col(name: &str, index: usize) -> Self {
        Self {
            name: name.to_string(),
            index,
            is_tag: false,
            tag_type: None,
            is_primary_key: false,
            is_timestamp: true,
            is_expression: false,
        }
    }

    fn transform_col(name: &str, index: usize) -> Self {
        Self {
            name: name.to_string(),
            index,
            is_tag: false,
            tag_type: None,
            is_primary_key: false,
            is_timestamp: false,
            is_expression: true,
        }
    }

    fn expression_col(name: &str, index: usize) -> Self {
        Self::transform_col(name, index)
    }

    fn tag_col(pattern: &str, index: usize) -> anyhow::Result<Self> {
        // tag pattern is `tag::type::name`, example: tag::varchar(123)::unit
        let split_pattern = pattern.split("::").collect_vec();
        if split_pattern.len() != 3 {
            bail!("invalid tag pattern: {}, col_index: {}", pattern, index);
        }

        let tag_type = IpcDataType::from_str(split_pattern.get(1).unwrap())
            .map_err(|err| anyhow::Error::msg(format!("{err} should be a valid Data Type")))?;
        let tag_name = split_pattern.get(2).unwrap().to_string();

        Ok(Self {
            name: tag_name,
            index,
            is_tag: true,
            tag_type: Some(tag_type),
            is_primary_key: false,
            is_timestamp: false,
            is_expression: false,
        })
    }
}
