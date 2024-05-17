use std::io::Write;

use anyhow::bail;
use base64::engine::general_purpose;
use base64::Engine;
use csv_async::AsyncReader;
use itertools::Itertools;
use taos::Dsn;
use tokio::fs::File;
use tokio_stream::StreamExt;

use crate::runners::opc::config::csv::header::CsvHeader;
use crate::runners::opc::config::model::OpcModelConfig;
use crate::runners::opc::config::OPCConfig;
use crate::runners::opc::OpcType;
use crate::utils::files::{get_encode, get_encode_from_buffer};

pub(crate) mod column;
pub(crate) mod header;

pub struct CsvParser {
    #[allow(dead_code)]
    opc_type: OpcType,
    #[allow(dead_code)]
    csv_files: Vec<String>,
    model_config: OpcModelConfig,
}

impl CsvParser {
    pub async fn is_valid(dsn: &Dsn) -> anyhow::Result<()> {
        let csv_config_files = OPCConfig::parse_csv_config_file(dsn).ok_or(anyhow::anyhow!(
            "csv_config_file not found in the dsn: {}",
            dsn.to_string()
        ))?;
        if csv_config_files.is_empty() {
            bail!("csv_config_file is empty in the dsn: {}", dsn.to_string());
        }

        // check stable, stable is required
        let parser = Self::from_dsn(dsn).await?;
        for (point_id, point_config) in parser.model_config.point_config_map {
            point_config.stable.ok_or(anyhow::anyhow!(
                "stable is required for point_id: {}",
                point_id
            ))?;
        }
        // check ts_col/ received_ts_col
        for (point_id, table_config) in parser.model_config.table_config_map {
            let mut has_primary_key = false;
            for col_config in table_config.column_configs {
                if col_config.is_primary_key == true {
                    has_primary_key = true;
                    break;
                }
            }
            if has_primary_key == false {
                bail!(
                    "ts_col or received_ts_col is required for point_id: {}",
                    point_id
                );
            }
        }

        Ok(())
    }

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

        let files = Self::open_csv_files(csv_files).await?;

        let mut model_config = OpcModelConfig::new();
        let mut csv_files = Vec::new();
        for (file, mut rdr) in files {
            csv_files.push(file.clone());

            // parse header
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(opc_type.clone(), header).await?;
            csv_header.check_required_columns()?;

            // parse lines
            let mut records = rdr.records();
            let mut row_index = 1;
            while let Some(record) = records.next().await {
                let csv_line = record.map_err(|e| {
                    anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
                })?;
                model_config
                    .add_csv_row(&csv_header, csv_line, row_index)
                    .await
                    .map_err(|err| {
                        anyhow::anyhow!(
                            "failed to parse csv at line {}, error: {}",
                            row_index,
                            err.to_string()
                        )
                    })?;

                row_index += 1;
            }
            if row_index == 1 {
                return Err(anyhow::anyhow!("empty csv file"));
            }
        }

        Ok(Self {
            opc_type,
            csv_files,
            model_config,
        })
    }

    async fn open_csv_files(
        csv_files: Vec<String>,
    ) -> anyhow::Result<Vec<(String, AsyncReader<File>)>> {
        let mut readers = Vec::new();
        for file in csv_files {
            // open the file
            let rdr = if file.starts_with("@") {
                let file_path = &file[1..];

                // check the file encoding
                let encoding = get_encode(file_path)?;
                if encoding.name() != "UTF-8" {
                    bail!(
                        "invalid CSV file encoding: {}, only UTF-8 or UTF-8 BOM supported",
                        encoding.name()
                    );
                }

                AsyncReader::from_reader(tokio::fs::File::open(file_path).await?)
            } else {
                let decoded = general_purpose::STANDARD.decode(&file)?;

                // check the file encoding
                let encoding = get_encode_from_buffer(decoded.as_slice())?;
                if encoding.name() != "UTF-8" {
                    bail!(
                        "invalid CSV file encoding: {}, only UTF-8 or UTF-8 BOM supported",
                        encoding.name()
                    );
                }

                let mut temp_file = tempfile::NamedTempFile::new()?;
                let res = String::from_utf8(decoded)?;
                write!(temp_file, "{}", res)?;
                let path = format!("@{}", temp_file.path().to_str().unwrap());
                let rdr = AsyncReader::from_reader(tokio::fs::File::open(&path[1..]).await?);
                temp_file.into_temp_path();
                rdr
            };

            readers.push((file, rdr));
        }
        Ok(readers)
    }

    pub fn get_model_config(&self) -> OpcModelConfig {
        self.model_config.clone()
    }

    pub fn get_point_ids(&self) -> Vec<String> {
        let point_config_map = &self.model_config.point_config_map;
        let table_config_map = &self.model_config.table_config_map;
        let mut node_config = Vec::new();

        for point_id in point_config_map.keys() {
            if let Some(table_config) = table_config_map.get(point_id) {
                if table_config.enabled == Some(0i8) {
                    continue;
                }
            }

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
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_open_csv_files() {
        let files = vec!["@../tests/opc/opcua-utf8bom.csv".to_string()];
        let res = CsvParser::open_csv_files(files).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().0, "@../tests/opc/opcua-utf8bom.csv");

        let files = vec!["@../tests/opc/opcua-utf8.csv".to_string()];
        let res = CsvParser::open_csv_files(files).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().0, "@../tests/opc/opcua-utf8.csv");

        let files = vec!["@../tests/opc/opcua-gbk.csv".to_string()];
        let res = CsvParser::open_csv_files(files).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().to_string(),
            "invalid CSV file encoding: GBK, only UTF-8 or UTF-8 BOM supported"
        );
    }

    #[tokio::test]
    async fn test_from_dsn() {
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-utf8bom.csv").unwrap();
        let ua_config = CsvParser::from_dsn(&dsn).await.unwrap();
        assert_eq!(ua_config.opc_type, OpcType::OPCUA);
        let csv_files = ua_config.csv_files;
        assert_eq!(csv_files.len(), 1);
        let path = csv_files.get(0).unwrap();
        assert_eq!(path, "@../tests/opc/opcua-utf8bom.csv");

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-utf8bom.csv").unwrap();
        let da_config = CsvParser::from_dsn(&dsn).await.unwrap();
        assert_eq!(da_config.opc_type, OpcType::OPCDA);
        let csv_files = da_config.csv_files;
        assert_eq!(csv_files.len(), 1);
        let path = csv_files.get(0).unwrap();
        assert_eq!(path, "@../tests/opc/opcda-utf8bom.csv");
    }

    #[tokio::test]
    async fn test_get_model_config() {
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let ua_config = csv_parser.get_model_config();
        assert_eq!(ua_config.point_config_map.len(), 3);

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let da_config = csv_parser.get_model_config();
        assert_eq!(da_config.point_config_map.len(), 3);
    }

    #[tokio::test]
    async fn test_get_node_config() {
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let ua_config = csv_parser.get_point_ids();
        assert_eq!(ua_config.len(), 2);
        assert_eq!(ua_config.get(0).unwrap(), "ns=3;i=1005::t_3_1005");
        assert_eq!(ua_config.get(1).unwrap(), "ns=3;i=1006::t_3_1006");

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let da_config = csv_parser.get_point_ids();
        assert_eq!(da_config.len(), 2);
        assert_eq!(
            da_config.get(0).unwrap(),
            "root.parent.temperature::t_temperature"
        );
        assert_eq!(
            da_config.get(1).unwrap(),
            "root.parent.current::t_custom_current"
        );
    }

    #[tokio::test]
    async fn test_get_tables_to_drop() {
        // let dsn =
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let tables_to_drop = csv_parser.get_tables_to_drop();
        assert_eq!(tables_to_drop.len(), 1);
        assert_eq!(tables_to_drop.get(0).unwrap(), "t_3_1007");

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();
        let tables_to_drop = csv_parser.get_tables_to_drop();
        assert_eq!(tables_to_drop.len(), 1);
        assert_eq!(tables_to_drop.get(0).unwrap(), "t_pressure");
    }

    #[tokio::test]
    async fn test_empty_csv_file() {
        let dsn = Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-empty.csv").unwrap();

        match CsvParser::from_dsn(&dsn).await {
            Ok(_) => panic!("empty csv file should fail"),
            Err(e) => {
                // println!("error: {}", e.to_string());
                assert_eq!(e.to_string(), "empty csv file")
            }
        }
    }

    #[tokio::test]
    async fn test_csv_file_with_transform_error() {
        let dsn = Dsn::from_str(
            "opcua://?csv_config_file=@../tests/opc/opcua-utf8bom-transform-error.csv",
        )
        .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await;
        assert!(csv_parser.is_err());
        // println!("error: {}", csv_parser.err().unwrap().to_string());
    }

    #[tokio::test]
    async fn test_empty_tbname() {
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-tbname-empty.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await;
        assert!(csv_parser.is_err());
        // println!("error: {}", csv_parser.err().unwrap().to_string());
    }

    #[tokio::test]
    async fn test_error_type() {
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-type-error.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await;
        assert!(csv_parser.is_err());
        // println!("error: {:?}", csv_parser.err().unwrap().to_string());
    }

    #[tokio::test]
    async fn test_error_name() {
        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-name-error.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).await.unwrap();

        let point_config_map = &csv_parser.model_config.point_config_map;

        assert_eq!(3, point_config_map.len());

        let point_id = "root.parent.temperature";
        let point_config = point_config_map.get(point_id).unwrap();
        assert_eq!(point_config.code, "t_temperature");

        let point_id = "root.parent.pressure";
        let point_config = point_config_map.get(point_id).unwrap();
        assert_eq!(point_config.code, "t_pressure");

        let point_id = "root.parent.current";
        let point_config = point_config_map.get(point_id).unwrap();
        assert_eq!(point_config.code, "t_custom_current");
    }
}
