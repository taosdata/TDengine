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
    pub fn try_new(name: &str, index: usize) -> anyhow::Result<Self> {
        let col = match name {
            "point_id" | "tag_name" | "TagName" => Self::default(name, index),
            "enabled" => Self::default(name, index),
            "stable" | "tbname" => Self::expression_col(name, index),
            "value_col" => Self::default(name, index),
            "value_transform" | "ts_transform" | "received_ts_transform" => {
                Self::transform_col(name, index)
            }
            "type" => Self::default(name, index),
            "quality_col" => Self::default(name, index),
            "ts_col" | "received_ts_col" => Self::timestamp_col(name, index),
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
