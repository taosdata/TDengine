use crate::get_data_dir;
use crate::runners::opc::config::csv::header::CsvHeader;
use crate::runners::opc::config::model::{
    ColumnConfig, GeneratePointMappingBy, OpcModelConfig, PointConfig, TableConfig,
};
use crate::runners::opc::config::OPCConfig;
use crate::runners::opc::{generate_tbname_from_pattern, OpcType};
use crate::utils::files::{get_encode, get_encode_from_buffer};
use crate::utils::validate_table_column_name;
use anyhow::bail;
use base64::engine::general_purpose;
use base64::Engine;
use csv_async::{AsyncReader, AsyncWriter, StringRecord};
use linked_hash_map::LinkedHashMap;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use taos::Dsn;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

pub mod column;
pub mod header;

/// CsvParser is used to parse csv files and generate model config
#[derive(Debug)]
pub struct CsvParser {
    opc_type: OpcType,
    /// csv files could be file path or utf8 encoded string
    csv_files: Vec<String>,

    csv_origin: Option<String>,
}

impl CsvParser {
    pub fn try_new(opc_type: OpcType, csv_files: Vec<String>) -> anyhow::Result<Self> {
        if csv_files.is_empty() {
            bail!("csv_files is empty");
        }

        Ok(Self {
            opc_type,
            csv_files,
            csv_origin: None,
        })
    }

    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;

        let csv_files = OPCConfig::parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
            "csv_config_file not found in the dsn: {}",
            dsn.to_string()
        ))?;

        Ok(Self {
            opc_type,
            csv_files,
            csv_origin: None,
        })
    }

    pub fn set_csv_origin(&mut self, csv_origin: Option<String>) {
        self.csv_origin = csv_origin;
    }

    pub fn decoded_csv(csv: &str) -> anyhow::Result<String> {
        if csv.starts_with("@") {
            Ok(csv.to_string())
        } else {
            let decoded = general_purpose::STANDARD
                .decode(csv.as_bytes().to_vec())
                .map_err(|err| {
                    anyhow::anyhow!("failed to decode csv content, cause: {}", err.to_string())
                })?;
            Ok(String::from_utf8(decoded)?)
        }
    }

    fn decode_csv_content(content: &str, encoded: bool) -> anyhow::Result<Vec<u8>> {
        let decoded = if encoded {
            general_purpose::STANDARD.decode(content).map_err(|err| {
                anyhow::anyhow!("failed to decode csv content, cause: {}", err.to_string())
            })?
        } else {
            content.as_bytes().to_vec()
        };

        // check the file encoding
        let encoding = get_encode_from_buffer(decoded.as_slice())?;
        if encoding.name() != "UTF-8" {
            bail!(
                "invalid CSV file encoding: {}, only UTF-8 or UTF-8 BOM supported",
                encoding.name()
            );
        };

        Ok(decoded)
    }

    /// 直接解析 csv 文件内容，生成 opc model config
    async fn parse_csv(opc_type: OpcType, content: String) -> anyhow::Result<OpcModelConfig> {
        let rdr = Self::load_csv_with_string(content.as_str(), false).await?;

        let (point_config_map, table_config_map) =
            Self::parse_point_mapping(opc_type.clone(), rdr).await?;

        Ok(OpcModelConfig {
            opc_type,
            generate_rule: None,
            point_config_map,
            table_config_map,
        })
    }

    async fn parse_point_mapping(
        opc_type: OpcType,
        mut rdr: AsyncReader<File>,
    ) -> anyhow::Result<(
        LinkedHashMap<String, PointConfig>,
        LinkedHashMap<String, TableConfig>,
    )> {
        let mut point_config_map = LinkedHashMap::new();
        let mut table_config_map = LinkedHashMap::new();

        // parse header
        let header = rdr
            .headers()
            .await
            .map_err(|e| anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string()))?;
        let csv_header = CsvHeader::try_new(opc_type, header)?;
        csv_header.check_required_columns()?;

        // parse lines
        let mut records = rdr.records();
        let mut row_index = 1;
        while let Some(record) = records.next().await {
            let row = record.map_err(|e| {
                anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
            })?;

            let point_id = Self::parse_point_id(&csv_header, &row)?;
            // parse point config and table config
            let p = PointConfig::from_csv(&csv_header, &row, row_index)?;
            let t = TableConfig::from_csv(&csv_header, &row)?;

            OpcModelConfig::is_conflict(&point_id, &p, &t, &point_config_map, &table_config_map)?;

            point_config_map.insert(point_id.clone(), p);
            table_config_map.insert(point_id.clone(), t);

            row_index += 1;
        }
        if row_index == 1 {
            bail!("empty csv file");
        }

        Ok((point_config_map, table_config_map))
    }

    /// 读取 self.csv_files 的内容，生成 opc model config
    pub async fn parse(&self) -> anyhow::Result<OpcModelConfig> {
        let files = Self::open_csv_many(self.csv_files.clone()).await?;

        let mut point_config_map = LinkedHashMap::new();
        let mut table_config_map = LinkedHashMap::new();

        for (_file, rdr) in files {
            let (point_config, table_config) =
                Self::parse_point_mapping(self.opc_type.clone(), rdr).await?;
            point_config_map.extend(point_config);
            table_config_map.extend(table_config);
        }

        Ok(OpcModelConfig {
            opc_type: self.opc_type.clone(),
            generate_rule: Some(GeneratePointMappingBy::Csv((
                self.csv_files.clone(),
                self.csv_origin.clone(),
            ))),
            point_config_map,
            table_config_map,
        })
    }

    /// get csv headers from csv files
    pub async fn get_all_headers(&self) -> anyhow::Result<HashMap<String, CsvHeader>> {
        if self.csv_files.is_empty() {
            bail!("csv_files is empty");
        }

        let csv_files = self.csv_files.clone();

        let files = Self::open_csv_many(csv_files).await?;

        let mut headers = HashMap::new();

        for (filename, mut rdr) in files {
            // parse header
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(self.opc_type.clone(), header)?;
            // check required columns
            csv_header.check_required_columns()?;

            headers.insert(filename, csv_header);
        }

        Ok(headers)
    }

    /// get headers of the csv file by index
    pub async fn get_headers(&self, csv_index: usize) -> anyhow::Result<CsvHeader> {
        if self.csv_files.is_empty() {
            bail!("csv_files is empty");
        }
        if csv_index >= self.csv_files.len() {
            bail!("csv_file index out of range");
        }
        let csv = self
            .csv_files
            .get(csv_index)
            .ok_or(anyhow::anyhow!("csv_file not found"))?;

        let mut rdr = Self::open_csv(csv.clone()).await?;

        // parse header
        let header = rdr
            .headers()
            .await
            .map_err(|e| anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string()))?;
        let csv_header = CsvHeader::try_new(self.opc_type.clone(), header)?;
        csv_header.check_required_columns()?;

        Ok(csv_header)
    }

    /// 在 csv 文件中追加一行
    pub async fn append_line(&self, line: String) -> anyhow::Result<()> {
        if self.csv_files.is_empty() {
            bail!("csv_files is empty");
        }

        // open the first file
        let csv_file = self
            .csv_files
            .get(0)
            .ok_or(anyhow::anyhow!("csv_file not found"))?;
        tracing::info!("append line to the csv: {}", csv_file);
        let mut rdr = Self::open_csv(csv_file.clone()).await?;

        // read csv to writer
        let mut writer = AsyncWriter::from_writer(vec![]);
        let header = rdr.headers().await?;
        writer.write_record(header.iter()).await?;
        let mut records = rdr.records();
        while let Some(record) = records.next().await {
            let record = record.map_err(|e| {
                anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
            })?;
            writer.write_record(record.iter()).await?;
        }

        // append the new line
        let mut rdr = AsyncReader::from_reader(line.as_bytes());
        let mut records = rdr.records();
        let record = if let Some(record) = records.next().await {
            record
                .map_err(|e| anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string()))?
        } else {
            bail!("empty csv line")
        };
        writer.write_record(record.iter()).await?;

        // write the new csv
        let new_csv = String::from_utf8(writer.into_inner().await?)?;

        let model_config = CsvParser::parse_csv(self.opc_type.clone(), new_csv.clone()).await?;
        model_config.validate()?;

        if csv_file.starts_with("@") {
            let file_path = &csv_file[1..];
            let mut file = File::create(file_path).await?;
            file.write_all(new_csv.as_bytes()).await?;
        } else {
            todo!("write to csv_config_file in dsn")
        }

        Ok(())
    }

    /// 如果 csv 以 @ 开头， 从文件中读， 否则从字符串中读
    pub async fn open_csv(csv: String) -> anyhow::Result<AsyncReader<File>> {
        let rdr = if csv.starts_with("@") {
            let file_path = &csv[1..];
            Self::load_csv_with_path(file_path).await?
        } else {
            Self::load_csv_with_string(&csv, true).await?
        };

        Ok(rdr)
    }

    /// 如果 csv_path 不为空， 从 csv_path 中读， 否则从 csv 中读
    pub async fn open_csv_with_path(
        csv: String,
        csv_path: Option<String>,
    ) -> anyhow::Result<AsyncReader<File>> {
        if let Some(csv_path) = csv_path {
            let path = Path::new(&csv_path);
            if path.exists() {
                return Self::load_csv_with_path(&csv_path).await;
            }
        };
        Self::open_csv(csv).await
    }

    async fn load_csv_with_path(file_path: &str) -> anyhow::Result<AsyncReader<File>> {
        // set current dir to DATA_DIR
        let _ = std::env::set_current_dir(get_data_dir());

        // check the file encoding
        let encoding = get_encode(file_path)?;
        if encoding.name() != "UTF-8" {
            bail!(
                "invalid CSV file encoding: {}, only UTF-8 or UTF-8 BOM supported",
                encoding.name()
            );
        }

        Ok(AsyncReader::from_reader(File::open(file_path).await?))
    }

    /// 将 string 解码，写入临时文件后打开
    async fn load_csv_with_string(
        content: &str,
        encoded: bool,
    ) -> anyhow::Result<AsyncReader<File>> {
        let decoded = Self::decode_csv_content(content, encoded)?;

        let mut temp_file = tempfile::NamedTempFile::new()?;
        let res = String::from_utf8(decoded)?;
        write!(temp_file, "{}", res)?;
        let path = format!("@{}", temp_file.path().to_str().unwrap());
        let rdr = AsyncReader::from_reader(tokio::fs::File::open(&path[1..]).await?);
        temp_file.into_temp_path();

        Ok(rdr)
    }

    /// 打开多个 csv
    async fn open_csv_many(
        csv_files: Vec<String>,
    ) -> anyhow::Result<Vec<(String, AsyncReader<File>)>> {
        let mut readers = Vec::new();
        for file in csv_files.iter() {
            let rdr = Self::open_csv(file.clone()).await.map_err(|err| {
                anyhow::anyhow!("failed to open csv: {}, cause: {}", file, err.to_string())
            })?;
            readers.push((file.clone(), rdr));
        }
        Ok(readers)
    }

    pub async fn parse_all_point_id_and_tbname(&self) -> anyhow::Result<Vec<(String, String)>> {
        let mut point_ids = vec![];

        let files = Self::open_csv_many(self.csv_files.clone()).await?;
        for (_file, mut rdr) in files {
            // parse header
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(self.opc_type.clone(), header)?;
            csv_header.check_required_columns()?;

            // parse lines
            let mut records = rdr.records();
            while let Some(record) = records.next().await {
                let row = record.map_err(|e| {
                    anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
                })?;

                // filter out disabled points
                let enabled = Self::parse_enabled(&csv_header, &row)?.unwrap_or(1i8);
                if enabled == 0 {
                    continue;
                }

                let point_id = Self::parse_point_id(&csv_header, &row)?;
                let tbname = Self::parse_tbname(&csv_header, &row)?;

                point_ids.push((point_id, tbname));
            }
        }

        Ok(point_ids)
    }

    pub async fn parse_all_point_id(&self) -> anyhow::Result<Vec<String>> {
        let mut point_ids = vec![];

        let files = Self::open_csv_many(self.csv_files.clone()).await?;

        for (_file, mut rdr) in files {
            // parse header
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(self.opc_type.clone(), header)?;
            csv_header.check_required_columns()?;

            // parse lines
            let mut records = rdr.records();
            while let Some(record) = records.next().await {
                let row = record.map_err(|e| {
                    anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
                })?;

                // filter out disabled points
                let enabled = Self::parse_enabled(&csv_header, &row)?.unwrap_or(1i8);
                if enabled == 0 {
                    continue;
                }
                let point_id = Self::parse_point_id(&csv_header, &row)?;
                point_ids.push(point_id);
            }
        }

        Ok(point_ids)
    }

    pub async fn parse_transform(
        &self,
        column_name: &str,
    ) -> anyhow::Result<HashMap<String, ColumnConfig>> {
        let mut transform_map = HashMap::new();
        let files = Self::open_csv_many(self.csv_files.clone()).await?;
        for (_file, mut rdr) in files {
            // parse header
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(self.opc_type.clone(), header)?;
            csv_header.check_required_columns()?;

            // parse lines
            let mut records = rdr.records();
            while let Some(record) = records.next().await {
                let row = record.map_err(|e| {
                    anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
                })?;

                let point_id = row
                    .get(csv_header.id_index())
                    .ok_or(anyhow::anyhow!("point id column not found in csv header"))?;
                let t = TableConfig::from_csv(&csv_header, &row)?;
                let column = t
                    .column_configs
                    .iter()
                    .find(|c| c.name == column_name)
                    .ok_or(anyhow::anyhow!(
                        "column {} not found in csv header",
                        column_name
                    ))?;
                transform_map.insert(point_id.to_string(), column.clone());
            }
        }
        Ok(transform_map)
    }

    pub async fn parse_one(
        &self,
        point_id: &str,
    ) -> anyhow::Result<Option<(PointConfig, TableConfig)>> {
        let files = Self::open_csv_many(self.csv_files.clone()).await?;

        for (_file, mut rdr) in files {
            // parse header
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(self.opc_type.clone(), header)?;
            csv_header.check_required_columns()?;

            // parse lines
            let mut records = rdr.records();
            let mut row_index = 1;
            while let Some(record) = records.next().await {
                let row = record.map_err(|e| {
                    anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
                })?;

                let point_id_index = csv_header.id_index();
                let id = row
                    .get(point_id_index)
                    .ok_or(anyhow::anyhow!("point id column not found in csv header"))?;
                if id == point_id {
                    // parse point config and table config
                    let p = PointConfig::from_csv(&csv_header, &row, row_index)?;
                    let t = TableConfig::from_csv(&csv_header, &row)?;
                    return Ok(Some((p, t)));
                }

                row_index += 1;
            }
        }

        Ok(None)
    }

    pub fn parse_point_id(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<String> {
        let point_id_index = header.id_index();
        let point_id = row
            .get(point_id_index)
            .map(|v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.to_string())
                }
            })
            .flatten()
            .ok_or(anyhow::anyhow!("point id cannot be None in csv row"))?;
        Ok(point_id)
    }

    pub fn parse_enabled(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Option<i8>> {
        let enabled = header
            .get_column("enabled")
            .map(|col| row.get(col.index))
            .flatten()
            .map(|val| if val.is_empty() { None } else { Some(val) })
            .flatten()
            .map(|v| {
                if v != "0" && v != "1" {
                    return Err(anyhow::anyhow!(
                        "invalid enabled: {} in csv row, must be 0 or 1",
                        v
                    ));
                }
                v.parse::<i8>().map_err(|_| {
                    anyhow::anyhow!("invalid enabled: {} in csv row, must be 0 or 1", v)
                })
            })
            .transpose()?;
        Ok(enabled)
    }

    pub fn parse_tbname(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<String> {
        let point_id = Self::parse_point_id(header, &row)?;

        let column = header
            .get_column("tbname")
            .ok_or(anyhow::anyhow!("tbname not exist in csv header"))?;

        let value = row
            .get(column.index)
            .ok_or(anyhow::anyhow!("tbname not exist in csv row"))?;

        if value.is_empty() {
            bail!("tbname cannot be empty");
        }

        let tbname = if value.contains("{") {
            // replace {tag_name} or {TagName} in tbname
            let opc_type = header.get_opc_type();
            generate_tbname_from_pattern(opc_type.to_string().as_str(), value, &point_id)
        } else {
            value.to_string()
        };
        validate_table_column_name("table name", &tbname)?;

        match tbname.is_empty() {
            true => bail!("tbname cannot be empty"),
            false => Ok(tbname),
        }
    }

    pub async fn read_to_string(&self) -> anyhow::Result<(Option<String>, String)> {
        if self.csv_files.is_empty() {
            bail!("csv_files is empty");
        }

        let csv = self
            .csv_files
            .get(0)
            .ok_or(anyhow::anyhow!("csv_file not found"))?;

        if csv.starts_with("@") {
            let file_path = &csv[1..];
            let content = tokio::fs::read_to_string(file_path)
                .await
                .map_err(|err| anyhow::anyhow!("failed to read csv file: {}", err))?;

            Ok((Some(file_path.to_string()), content))
        } else {
            let decoded = general_purpose::STANDARD.decode(csv).map_err(|err| {
                anyhow::anyhow!("failed to decode csv content, cause: {}", err.to_string())
            })?;

            // check the file encoding
            let encoding = get_encode_from_buffer(decoded.as_slice())?;
            if encoding.name() != "UTF-8" {
                bail!(
                    "invalid CSV file encoding: {}, only UTF-8 or UTF-8 BOM supported",
                    encoding.name()
                );
            }
            let res = String::from_utf8(decoded)?;
            Ok((None, res))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::StreamExt;
    use std::str::FromStr;

    #[test]
    fn test_decode_csv_content() {
        todo!()
    }

    #[tokio::test]
    async fn test_read_to_string() {
        todo!()
    }

    #[tokio::test]
    async fn test_open_csv_file() {
        // file path
        let file = "@../tests/opc/opcua-utf8bom.csv".to_string();
        let mut rdr = CsvParser::open_csv(file).await.unwrap();
        let headers = rdr.headers().await.unwrap();
        assert_eq!(headers.len(), 14);
        assert_eq!(headers.get(0).unwrap(), "0");
        assert_eq!(headers.get(1).unwrap(), "point_id");
        let mut records = rdr.records();
        let mut count = 0;
        while let Some(record) = records.next().await {
            count += 1;
            let record = record.unwrap();
            assert_eq!(count, record.get(0).unwrap().parse::<i32>().unwrap());
        }
        assert_eq!(count, 3);

        // content
        let content = "a,b,c\n1,2,3".to_string();
        let file = general_purpose::STANDARD.encode(content.as_bytes().to_vec());
        let mut rdr = CsvParser::open_csv(file).await.unwrap();
        let headers = rdr.headers().await.unwrap();
        assert_eq!(headers.len(), 3);
        assert_eq!(headers.get(0).unwrap(), "a");
        assert_eq!(headers.get(1).unwrap(), "b");
        assert_eq!(headers.get(2).unwrap(), "c");
        let mut records = rdr.records();
        assert_eq!(records.next().await.unwrap().unwrap(), vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn test_open_csv_files() {
        let files = vec!["@../tests/opc/opcua-utf8bom.csv".to_string()];
        let res = CsvParser::open_csv_many(files).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().0, "@../tests/opc/opcua-utf8bom.csv");

        let files = vec!["@../tests/opc/opcua-utf8.csv".to_string()];
        let res = CsvParser::open_csv_many(files).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().0, "@../tests/opc/opcua-utf8.csv");

        let files = vec!["@../tests/opc/opcua-gbk.csv".to_string()];
        let res = CsvParser::open_csv_many(files).await;
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
        let ua_config = CsvParser::from_dsn(&dsn).unwrap();
        assert_eq!(ua_config.opc_type, OpcType::OPCUA);
        let csv_files = ua_config.csv_files;
        assert_eq!(csv_files.len(), 1);
        let path = csv_files.get(0).unwrap();
        assert_eq!(path, "@../tests/opc/opcua-utf8bom.csv");

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-utf8bom.csv").unwrap();
        let da_config = CsvParser::from_dsn(&dsn).unwrap();
        assert_eq!(da_config.opc_type, OpcType::OPCDA);
        let csv_files = da_config.csv_files;
        assert_eq!(csv_files.len(), 1);
        let path = csv_files.get(0).unwrap();
        assert_eq!(path, "@../tests/opc/opcda-utf8bom.csv");
    }

    #[tokio::test]
    async fn test_parse() {
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let ua_config = csv_parser.parse().await.unwrap();
        assert_eq!(ua_config.point_config_map.len(), 3);

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let da_config = csv_parser.parse().await.unwrap();
        assert_eq!(da_config.point_config_map.len(), 3);
    }

    #[tokio::test]
    async fn test_parse_point_id_and_tbname() {
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let ua_config = csv_parser.parse_all_point_id_and_tbname().await.unwrap();
        assert_eq!(ua_config.len(), 2);
        let (point_id, tbname) = ua_config.get(0).unwrap();
        assert_eq!(point_id, "ns=3;i=1005");
        assert_eq!(tbname, "t_3_1005");
        let (point_id, tbname) = ua_config.get(1).unwrap();
        assert_eq!(point_id, "ns=3;i=1006");
        assert_eq!(tbname, "t_3_1006");

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let da_config = csv_parser.parse_all_point_id_and_tbname().await.unwrap();
        assert_eq!(da_config.len(), 2);
        let (point_id, tbname) = da_config.get(0).unwrap();
        assert_eq!(point_id, "root.parent.temperature");
        assert_eq!(tbname, "t_temperature");
        let (point_id, tbname) = da_config.get(1).unwrap();
        assert_eq!(point_id, "root.parent.current");
        assert_eq!(tbname, "t_custom_current");
    }

    #[tokio::test]
    async fn test_empty_csv_file() {
        let dsn = Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-empty.csv").unwrap();
        let result = CsvParser::from_dsn(&dsn);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "empty csv file");

        // invalid transform expression
        let dsn = Dsn::from_str(
            "opcua://?csv_config_file=@../tests/opc/opcua-utf8bom-transform-error.csv",
        )
        .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let res = csv_parser.parse().await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid transform expression: invalid expression"
        );

        // tbname is empty
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-tbname-empty.csv").unwrap();
        let parser = CsvParser::from_dsn(&dsn).unwrap();
        let res = parser.parse().await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "tbname cannot be empty");

        // type error
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-type-error.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let res = csv_parser.parse().await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid type: invalid type in csv row, must be INT, FLOAT, BOOL, STRING, DATETIME"
        );
    }

    #[tokio::test]
    async fn test_error_name() {
        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/opcda-name-error.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();

        let model_config = csv_parser.parse().await.unwrap();
        let point_config_map = model_config.point_config_map;

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
