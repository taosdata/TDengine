use std::collections::HashMap;
use std::io::Write;
use std::str::FromStr;

use anyhow::bail;
use base64::engine::general_purpose;
use base64::Engine;
use csv_async::StringRecord;
use itertools::Itertools;
use taos::Dsn;
use tokio_stream::StreamExt;

use taosx_ipc::prelude::IpcDataType;

use crate::runners::opc::config::model::OpcModelConfig;
use crate::runners::opc::config::OPCConfig;
use crate::runners::opc::OpcType;

pub struct CsvParser {
    // dsn: Dsn,
    #[allow(dead_code)]
    opc_type: OpcType,
    #[allow(dead_code)]
    csv_files: Vec<String>,
    model_config: OpcModelConfig,
}

impl CsvParser {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let csv_config_files = OPCConfig::parse_csv_config_file(dsn).ok_or(anyhow::anyhow!(
            "csv_config_file not found in the dsn: {}",
            dsn.to_string()
        ))?;

        let csv_files = csv_config_files
            .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();

        let mut model_config = OpcModelConfig::new();

        for file in csv_files.clone() {
            // open the file
            let mut rdr = if file.starts_with("@") {
                csv_async::AsyncReader::from_reader(tokio::fs::File::open(&file[1..]).await?)
            } else {
                let decoded = general_purpose::STANDARD.decode(&file)?;
                let mut temp_file = tempfile::NamedTempFile::new()?;
                let res = String::from_utf8(decoded)?;
                write!(temp_file, "{}", res)?;
                let path = format!("@{}", temp_file.path().to_str().unwrap());
                let rdr =
                    csv_async::AsyncReader::from_reader(tokio::fs::File::open(&path[1..]).await?);
                temp_file.into_temp_path();
                rdr
            };

            // The first line is comment, skip it. The second line is header.
            let mut records = rdr.records();
            let header = records.next().await;
            if header.is_none() {
                tracing::warn!("file {file} should have 3 lines at least");
                bail!("Config file {file} should not be empty");
            }
            let header = header.unwrap()?;

            // parse header
            let csv_header = CsvHeader::try_new(opc_type.clone(), header).await?;

            // parse lines
            while let Some(record) = records.next().await {
                let csv_line = record?;
                model_config
                    .append(&csv_header, csv_line)
                    .await
                    .map_err(|err| {
                        anyhow::anyhow!(
                            "failed to parse csv in file: {}, error: {:?}",
                            file.clone(),
                            err
                        )
                    })?;
            }
        }

        Ok(Self {
            // dsn: dsn.clone(),
            opc_type,
            csv_files,
            model_config,
        })
    }

    pub fn get_model_config(&self) -> OpcModelConfig {
        self.model_config.clone()
    }

    pub fn get_point_ids(&self) -> Vec<String> {
        let point_config_map = &self.model_config.point_config_map;

        let mut node_config = Vec::new();

        for point_id in point_config_map.keys() {
            let tbname = point_config_map.get(point_id).unwrap().code.clone();
            node_config.push(format!("{}::{}", point_id, tbname));
        }

        node_config
    }

    pub fn get_tables_to_drop(&self) -> Vec<String> {
        let point_config_map = &self.model_config.point_config_map;
        let table_config_map = &self.model_config.table_config_map;

        let mut tables_to_drop = Vec::new();
        for point_id in point_config_map.keys() {
            let table_config = table_config_map.get(point_id).unwrap();
            if table_config.enabled == Some(0i8) {
                let tbname = point_config_map.get(point_id).unwrap().code.clone();
                tables_to_drop.push(tbname);
            }
        }
        tables_to_drop
    }
}

#[cfg(test)]
mod csv_parser_tests {
    use std::str::FromStr;

    use taos::Dsn;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_from_dsn() {
        let dsn = Dsn::from_str("opcua://?csv_config_file=@tests/opc/opcua-template-utf8-bom.csv")
            .unwrap();
        let ua_config = CsvParser::from_dsn(&dsn).await.unwrap();
        assert_eq!(ua_config.opc_type, OpcType::OPCUA);
        let csv_files = ua_config.csv_files;
        assert_eq!(csv_files.len(), 1);
        let path = csv_files.get(0).unwrap();
        assert_eq!(path, "@tests/opc/opcua-template-utf8-bom.csv");

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@tests/opc/opcda-template-zh-utf8bom.csv")
                .unwrap();
        let da_config = CsvParser::from_dsn(&dsn).await.unwrap();
        assert_eq!(da_config.opc_type, OpcType::OPCDA);
        let csv_files = da_config.csv_files;
        assert_eq!(csv_files.len(), 1);
        let path = csv_files.get(0).unwrap();
        assert_eq!(path, "@tests/opc/opcda-template-zh-utf8bom.csv");
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_model_config() {
        let dsn = Dsn::from_str("opcua://?csv_config_file=@tests/opc/opcua-template-utf8-bom.csv")
            .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let ua_config = csv_parser.get_model_config();
        dbg!(ua_config);

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@tests/opc/opcda-template-zh-utf8bom.csv")
                .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let da_config = csv_parser.get_model_config();
        dbg!(da_config);
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_node_config() {
        let dsn = Dsn::from_str("opcua://?csv_config_file=@tests/opc/opcua-template-utf8-bom.csv")
            .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let ua_config = csv_parser.get_point_ids();
        assert_eq!(ua_config.len(), 3);
        dbg!(ua_config);

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@tests/opc/opcda-template-zh-utf8bom.csv")
                .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let da_config = csv_parser.get_point_ids();
        assert_eq!(da_config.len(), 3);
        dbg!(da_config);
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_tables_to_drop() {
        let dsn = Dsn::from_str("opcua://?csv_config_file=@tests/opc/opcua-template-utf8-bom.csv")
            .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let tables_to_drop = csv_parser.get_tables_to_drop();
        assert_eq!(tables_to_drop.len(), 1);
        assert_eq!(tables_to_drop.get(0).unwrap(), "t_3_1007");

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@tests/opc/opcda-template-zh-utf8bom.csv")
                .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let tables_to_drop = csv_parser.get_tables_to_drop();
        assert_eq!(tables_to_drop.len(), 1);
        assert_eq!(tables_to_drop.get(0).unwrap(), "t_pressure");
    }
}

pub struct CsvHeader {
    opc_type: OpcType,
    columns: Vec<CsvColumn>,
    column_map: HashMap<String, usize>,
    primary_timestamp_index: Option<usize>,
}

impl CsvHeader {
    pub async fn try_new(opc_type: OpcType, header: StringRecord) -> anyhow::Result<Self> {
        let mut columns = Vec::new();
        let mut column_map = HashMap::new();
        let mut primary_ts = None;

        for (index, name) in header.iter().enumerate() {
            let mut col = CsvColumn::try_new(name, index).await?;

            // if the header contains ts_col and received_ts_col, use the first one as primary key
            // if neither ts_col nor received_ts_col is found, the primary_ts will be None
            if col.is_timestamp && primary_ts.is_none() {
                col.is_primary_key = true;
                primary_ts = Some(index);
            }

            // push the column into the vector
            let col_name = col.name.clone();
            let is_duplicated = column_map.insert(col_name.clone(), index);

            // check if the column name is duplicated
            if is_duplicated.is_some() {
                bail!("duplicated column name: {}", col_name);
            }

            columns.push(col);
        }

        Ok(Self {
            opc_type,
            columns,
            column_map,
            primary_timestamp_index: primary_ts,
        })
    }

    pub fn get_opc_type(&self) -> &OpcType {
        &self.opc_type
    }

    pub fn get_columns(&self) -> Vec<&CsvColumn> {
        self.columns.iter().collect()
    }

    pub fn get_column(&self, col_name: &str) -> Option<&CsvColumn> {
        self.column_map
            .get(col_name)
            .map(|index| self.columns.get(*index))
            .flatten()
    }

    pub fn get_primary_timestamp(&self) -> Option<&CsvColumn> {
        self.primary_timestamp_index
            .map(|index| self.columns.get(index))
            .flatten()
    }
}

#[cfg(test)]
mod test_csv_header {
    use itertools::Itertools;

    use super::*;

    #[tokio::test]
    async fn test_try_new() {
        let header_line = "0,point_id,enabled,stable,tbname,value_col,value_transform,type,quality_col,ts_col,received_ts_col,ts_transform,received_ts_transform,tag::VARCHAR(200)::name".split(",").collect_vec();
        let header = csv_async::StringRecord::from(header_line);

        let csv_header = CsvHeader::try_new(OpcType::OPCUA, header).await.unwrap();

        let primary_ts = csv_header.get_primary_timestamp().unwrap();
        assert_eq!(primary_ts.index, 9);
        assert_eq!(primary_ts.name, "ts_col");

        assert_eq!(csv_header.column_map.len(), 14);

        let col = csv_header.get_column("point_id").unwrap();
        assert_eq!(col.index, 1);

        let col = csv_header.get_column("enabled").unwrap();
        assert_eq!(col.index, 2);

        let col = csv_header.get_column("name").unwrap();
        assert_eq!(col.index, 13);
        assert_eq!(col.is_tag, true);
        assert_eq!(col.tag_type, Some(IpcDataType::VarChar(200)));
    }
}

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
    pub async fn try_new(name: &str, index: usize) -> anyhow::Result<Self> {
        let col = match name {
            "point_id" | "tag_name" | "TagName" => Self::default(name, index),
            "enabled" => Self::default(name, index),
            "stable" => Self::default(name, index),
            "tbname" => Self::default(name, index),
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

    pub fn transform_col(name: &str, index: usize) -> Self {
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

    pub fn tag_col(pattern: &str, index: usize) -> anyhow::Result<Self> {
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

    pub fn timestamp_col(name: &str, index: usize) -> Self {
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

    pub fn default(name: &str, index: usize) -> Self {
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
}
