use anyhow::bail;
use base64::Engine;
use base64::engine::general_purpose;
use csv_async::{AsyncReader, AsyncReaderBuilder, StringRecord};
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use taos::Dsn;
use taosx_ipc::prelude::IpcDataType;
use tokio::fs::File;
use tokio_stream::StreamExt;

use crate::sink::point::UpdateMode;
use crate::sink::point::model::{
    ColumnConfig, GeneratePointMappingBy, PointConfig, PointModelConfig, TableConfig,
};
use crate::sink::point::model::{SourceType, generate_tbname_from_pattern};
use crate::utils::files::{get_encode, get_encode_from_buffer};
use crate::utils::{parse_key_in_dsn, validate_table_column_name};
use crate::{get_data_dir, utils};

#[derive(Debug)]
pub struct CsvHeader {
    pub source_type: SourceType,
    pub columns: Vec<CsvColumn>,
    pub column_map: LinkedHashMap<String, usize>,
    pub primary_timestamp_index: Option<usize>,
    pub point_id_index: usize,
    pub enabled_index: usize,
}

impl CsvHeader {
    /// create csv header
    pub fn try_new(source_type: SourceType, header: &StringRecord) -> anyhow::Result<Self> {
        let mut columns = Vec::new();
        let mut column_map = LinkedHashMap::new();
        let mut primary_ts = None;
        let mut point_id_index = None;
        let mut enabled_index = None;

        for (index, name) in header.iter().enumerate() {
            if source_type == SourceType::OPCUA && name == "point_id" {
                point_id_index = Some(index);
            }
            if source_type == SourceType::OPCDA && name == "tag_name" {
                point_id_index = Some(index);
            }
            if source_type == SourceType::KingHistorian && name == "tag_name" {
                point_id_index = Some(index);
            }
            if name == "enabled" {
                enabled_index = Some(index);
            }

            let mut col = CsvColumn::try_new(name, index)?;
            // if the header contains ts_col and received_ts_col, use the first one as primary key
            // if neither ts_col nor received_ts_col is found, the primary_ts will be None
            if col.is_timestamp && primary_ts.is_none() {
                col.is_primary_key = true;
                primary_ts = Some(index);
            }
            let col_name = col.name.clone();
            let is_duplicated = column_map.insert(col_name.clone(), index);
            // check if the column name is duplicated
            if is_duplicated.is_some() {
                bail!("duplicated column name: {}", col_name);
            }
            columns.push(col);
        }

        Ok(Self {
            source_type,
            columns,
            column_map,
            primary_timestamp_index: primary_ts,
            point_id_index: point_id_index.ok_or(anyhow::anyhow!("point_id is required"))?,
            enabled_index: enabled_index.unwrap_or(1),
        })
    }

    pub fn check_required_columns(&self) -> anyhow::Result<()> {
        match self.source_type {
            SourceType::OPCUA => {
                if !self.column_map.contains_key("point_id") {
                    bail!("point_id is required");
                }
            }
            SourceType::OPCDA | SourceType::KingHistorian => {
                if !self.column_map.contains_key("tag_name") {
                    bail!("tag_name is required");
                }
            }
        }

        if !self.column_map.contains_key("stable") {
            bail!("stable is required");
        }

        if !self.column_map.contains_key("tbname") {
            bail!("tbname is required");
        }

        Ok(())
    }

    pub fn id_index(&self) -> usize {
        self.point_id_index
    }

    pub fn enabled_index(&self) -> usize {
        self.enabled_index
    }

    pub fn get_source_type(&self) -> SourceType {
        self.source_type
    }

    /// 返回所有列的只读切片，避免每次分配新的 Vec
    pub fn columns(&self) -> &[CsvColumn] {
        &self.columns
    }

    /// 兼容旧接口：返回一个迭代器，而不是新的 Vec，以避免在热点路径上分配
    pub fn get_columns(&self) -> impl Iterator<Item = &CsvColumn> {
        self.columns.iter()
    }

    pub fn get_column(&self, col_name: &str) -> Option<&CsvColumn> {
        self.column_map
            .get(col_name)
            .and_then(|index| self.columns.get(*index))
    }

    pub fn get_primary_timestamp(&self) -> Option<&CsvColumn> {
        self.primary_timestamp_index
            .and_then(|index| self.columns.get(index))
    }
}

/// CsvParser is used to parse csv files and generate model config
#[derive(Debug)]
pub struct CsvParser<'a> {
    source_type: SourceType,
    update_mode: Option<UpdateMode>,
    /// csv files could be file path or utf8 encoded string
    csv_files: Vec<String>,
    /// csv_origin 是原始的 DSN，其中的 csv_config_file 参数是 URL encoded 的 csv 内容
    csv_origin: Option<String>,
    /// csv_content: csv 文件的内容
    csv_content: Option<&'a str>,
}

impl<'a> CsvParser<'a> {
    pub fn try_new(source_type: SourceType, csv_files: Vec<String>) -> anyhow::Result<Self> {
        if csv_files.is_empty() {
            bail!("csv_files is empty");
        }

        Ok(Self {
            source_type,
            csv_files,
            csv_origin: None,
            csv_content: None,
            update_mode: None,
        })
    }

    pub fn try_from_content(source_type: SourceType, content: &'a str) -> anyhow::Result<Self> {
        if content.is_empty() {
            bail!("csv content is empty");
        }

        Ok(Self {
            source_type,
            csv_files: vec![],
            csv_origin: None,
            csv_content: Some(content),
            update_mode: None,
        })
    }

    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let source_type = SourceType::try_from(dsn)?;

        let csv_files = parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
            "csv_config_file not found in the dsn: {}",
            dsn.to_string()
        ))?;

        let csv_files = csv_files
            .iter()
            .filter(|f| !f.is_empty())
            .map(|f| f.to_string())
            .collect_vec();

        if csv_files.is_empty() {
            bail!("opc csv config files is empty");
        }

        let update_mode = parse_key_in_dsn::<String>(dsn, "update_mode")?
            .map(|m| UpdateMode::from_str(m.as_str()))
            .transpose()?;

        Ok(Self {
            source_type,
            csv_files,
            csv_origin: None,
            csv_content: None,
            update_mode,
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
                .decode(csv.as_bytes())
                .map_err(|err| {
                    anyhow::anyhow!("failed to decode csv content, cause: {}", err.to_string())
                })?;
            Ok(String::from_utf8(decoded)?)
        }
    }

    /// 直接解析 csv 文件内容，生成 opc model config
    pub async fn parse_csv(
        source_type: SourceType,
        content: String,
    ) -> anyhow::Result<PointModelConfig> {
        let rdr = Self::load_csv_with_string(content.as_str(), false).await?;

        let (point_config_map, table_config_map) =
            Self::parse_point_mapping(source_type, rdr).await?;

        Ok(PointModelConfig {
            source_type,
            generate_rule: None,
            point_config_map,
            table_config_map,
            update_mode: None, // 不支持动态点位更新
        })
    }

    async fn parse_point_mapping<'r, R>(
        source_type: SourceType,
        mut rdr: AsyncReader<R>,
    ) -> anyhow::Result<(
        LinkedHashMap<String, PointConfig>,
        LinkedHashMap<String, TableConfig>,
    )>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'r,
    {
        let mut point_config_map: LinkedHashMap<String, PointConfig> = LinkedHashMap::new();
        let mut table_config_map = LinkedHashMap::new();
        // Fast-path de-dup set: (stable, tbname, value_col) -> first point_id
        let mut seen_triplets: HashMap<(String, String, String), String> = HashMap::new();

        // parse header
        let header = rdr
            .headers()
            .await
            .map_err(|e| anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string()))?;
        let csv_header = CsvHeader::try_new(source_type, header)?;
        csv_header.check_required_columns()?;

        // parse lines
        let mut records = rdr.records();
        let mut row_index = 1;
        while let Some(record) = records.next().await {
            let row = record.map_err(|e| {
                anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
            })?;

            let point_id = Self::parse_point_id(&csv_header, &row)?;
            // parse point config and table config (optimized path using existing point_id)
            let p = PointConfig::from_csv_with_point_id(&csv_header, &row, row_index, &point_id)?;
            let t = TableConfig::from_csv(&csv_header, &row, source_type)?;

            // O(1) conflict check when enabled and not using expressions in stable/tbname
            let disabled = t.enabled.is_some_and(|v| v == 0);
            let stable_expr = p.stable.as_ref().is_some_and(|s| s.contains("{type}"));
            let tbname = p.code.as_str();
            let tbname_expr =
                tbname.contains("{id}") || tbname.contains("{ns}") || tbname.contains("{tag_name}");

            if !disabled && !stable_expr && !tbname_expr {
                // value_col alias should always exist per parsing rules
                let value_col = t
                    .column_config(ColumnConfig::VALUE)
                    .and_then(|v| v.alias.as_ref())
                    .map_or("val", |v| v.as_str());

                let key = (
                    p.stable.clone().unwrap_or_default(),
                    tbname.to_string(),
                    value_col.to_string(),
                );
                if let Some(prev_point) = seen_triplets.get(&key) {
                    // Keep error semantics consistent with original is_conflict
                    bail!(
                        "point_id: {} and point_id: {} have same stable: {} and tbname: {}, value_col should be different",
                        prev_point,
                        point_id,
                        p.stable.clone().unwrap_or_default(),
                        tbname,
                    );
                } else {
                    seen_triplets.insert(key, point_id.clone());
                }
            } else {
                // Fallback to existing O(N) check for edge cases (expressions/disabled)
                PointModelConfig::is_conflict(
                    &point_id,
                    &p,
                    &t,
                    &point_config_map,
                    &table_config_map,
                )?;
            }

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
    pub async fn parse(&self) -> anyhow::Result<PointModelConfig> {
        // 点位映射
        let mut point_config_map = LinkedHashMap::new();
        let mut table_config_map = LinkedHashMap::new();

        // 点位映射的生成规则
        let mut generate_rule = Some(GeneratePointMappingBy::Csv((
            self.csv_files.clone(),
            self.csv_origin.clone(),
        )));

        // 解析多个 csv 文件，将点位映射合并
        let files = Self::open_csv_many(self.csv_files.clone()).await?;
        for (_file, rdr) in files {
            let (point_config, table_config) =
                Self::parse_point_mapping(self.source_type, rdr).await?;
            point_config_map.extend(point_config);
            table_config_map.extend(table_config);
        }

        // 如果 csv_files 为空且 csv_content 不为空（King Hisotrian的用法），则直接解析 csv_content
        if let (true, Some(content)) = (self.csv_files.is_empty(), &self.csv_content) {
            let rdr = AsyncReaderBuilder::new()
                .delimiter(b',')
                .create_reader(content.as_bytes());

            let (point_config, table_config) =
                Self::parse_point_mapping(self.source_type, rdr).await?;
            point_config_map.extend(point_config);
            table_config_map.extend(table_config);
            // 使用 csv_content 解析时，不支持“点位映射生成”
            generate_rule = None;
        }

        Ok(PointModelConfig {
            source_type: self.source_type,
            generate_rule,
            point_config_map,
            table_config_map,
            update_mode: self.update_mode,
        })
    }

    /// get csv headers from csv files
    pub async fn get_all_headers(
        source_type: SourceType,
        csv_files: Vec<String>,
    ) -> anyhow::Result<HashMap<String, CsvHeader>> {
        if csv_files.is_empty() {
            bail!("csv_files is empty");
        }

        let files = Self::open_csv_many(csv_files).await?;

        let mut headers = HashMap::new();

        for (filename, mut rdr) in files {
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(source_type, header)?;

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
        let csv_header = CsvHeader::try_new(self.source_type, header)?;
        csv_header.check_required_columns()?;

        Ok(csv_header)
    }

    /// 如果 csv 以 @ 开头， 从文件中读， 否则从字符串中读
    pub async fn open_csv(csv: String) -> anyhow::Result<AsyncReader<File>> {
        let rdr = if let Some(file_path) = csv.strip_prefix("@") {
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
        let file_path = Path::new(file_path);
        let file_path = if file_path.exists() {
            file_path.to_path_buf()
        } else {
            let path = get_data_dir().join(file_path);
            if path.exists() && path.is_file() {
                path
            } else {
                bail!("csv file not found: {}", file_path.display());
            }
        };
        // check the file encoding
        let encoding = get_encode(&file_path)?;
        if encoding.name() != "UTF-8" {
            bail!(
                "invalid CSV file encoding: {}, only UTF-8 or UTF-8 BOM supported",
                encoding.name()
            );
        }

        Ok(AsyncReader::from_reader(File::open(&file_path).await?))
    }

    /// 将 string 解码，写入临时文件后打开
    async fn load_csv_with_string(
        content: &str,
        encoded: bool,
    ) -> anyhow::Result<AsyncReader<File>> {
        let decoded = utils::files::decode_csv_content(content, encoded)?;

        let mut temp_file = tempfile::NamedTempFile::new()?;
        let res = String::from_utf8(decoded)?;
        write!(temp_file, "{}", res)?;
        let path = format!("@{}", temp_file.path().to_str().unwrap());
        let rdr = AsyncReader::from_reader(tokio::fs::File::open(&path[1..]).await?);
        temp_file.into_temp_path();

        Ok(rdr)
    }

    /// 打开多个 csv
    pub async fn open_csv_many(
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

    pub async fn parse_point_id_and_tbname(&self) -> anyhow::Result<LinkedHashMap<String, String>> {
        let mut point_ids = LinkedHashMap::new();

        let files = Self::open_csv_many(self.csv_files.clone()).await?;
        for (_file, mut rdr) in files {
            // parse header
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(self.source_type, header)?;
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

                point_ids.insert(point_id, tbname);
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
            let csv_header = CsvHeader::try_new(self.source_type, header)?;
            csv_header.check_required_columns()?;

            // parse lines
            let mut records = rdr.records();
            while let Some(record) = records.next().await {
                let row = record.map_err(|e| {
                    anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
                })?;
                let point_id = Self::parse_point_id(&csv_header, &row)?;
                point_ids.push(point_id);
            }
        }

        Ok(point_ids)
    }

    pub async fn parse_transform_map(
        source_type: SourceType,
        files: Vec<(String, AsyncReader<File>)>,
        columns: &[&str],
    ) -> anyhow::Result<HashMap<String, HashMap<String, ColumnConfig>>> {
        let mut transform_map = HashMap::new();
        for col in columns {
            transform_map.insert(col.to_string(), HashMap::new());
        }

        for (_file, mut rdr) in files {
            // parse header
            let header = rdr.headers().await.map_err(|e| {
                anyhow::anyhow!("failed to read csv header, cause: {}", e.to_string())
            })?;
            let csv_header = CsvHeader::try_new(source_type, header)?;
            // parse lines
            let mut records = rdr.records();
            while let Some(record) = records.next().await {
                let row = record.map_err(|e| {
                    anyhow::anyhow!("failed to read csv line, cause: {}", e.to_string())
                })?;

                let point_id = row
                    .get(csv_header.id_index())
                    .ok_or(anyhow::anyhow!("point id column not found in csv header"))?;
                let t = TableConfig::from_csv(&csv_header, &row, source_type)?;
                for c in t.column_configs {
                    // 如果 column name 在 columns 中，则加入 transform_map
                    if columns.contains(&c.name.as_str()) {
                        transform_map
                            .entry(c.name.clone())
                            .or_insert(HashMap::new())
                            .insert(point_id.to_string(), c);
                    }
                }
            }
        }

        // 遍历 transform_map, 如果 col 对应的 Hashmap 为空，则删除
        for col in columns {
            if let Some(map) = transform_map.get(*col)
                && map.is_empty()
            {
                transform_map.remove(*col);
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
            let csv_header = CsvHeader::try_new(self.source_type, header)?;
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
                    let p = PointConfig::from_csv_with_point_id(
                        &csv_header,
                        &row,
                        row_index,
                        point_id,
                    )?;
                    let t = TableConfig::from_csv(&csv_header, &row, self.source_type)?;
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
            .and_then(|v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.to_string())
                }
            })
            .ok_or(anyhow::anyhow!("point id cannot be None in csv row"))?;
        Ok(point_id)
    }

    pub fn parse_enabled(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Option<i8>> {
        let enabled = header
            .get_column("enabled")
            .and_then(|col| row.get(col.index))
            .and_then(|val| if val.is_empty() { None } else { Some(val) })
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
        let point_id = Self::parse_point_id(header, row)?;

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
            let opc_type = header.get_source_type();
            generate_tbname_from_pattern(opc_type.as_static_str(), value, &point_id)
        } else {
            value.to_string()
        };
        validate_table_column_name("table name", &tbname)?;

        match tbname.is_empty() {
            true => bail!("tbname cannot be empty"),
            false => Ok(tbname),
        }
    }

    /// 从 csv 文件中读取内容
    /// 如果 csv 在文件中，则返回：(文件路径， 文件内容)
    /// 如果 csv 在 dsn 中，则返回：(None, 文件内容)
    pub async fn read_to_string(&self) -> anyhow::Result<(Option<String>, String)> {
        if self.csv_files.is_empty() {
            bail!("csv_files is empty");
        }

        let csv = self
            .csv_files
            .first()
            .ok_or(anyhow::anyhow!("csv_file not found"))?;

        if let Some(file_path) = csv.strip_prefix("@") {
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

pub fn parse_csv_config_files(dsn: &Dsn) -> Option<Vec<String>> {
    dsn.params.get("csv_config_file").and_then(|v| {
        if v.is_empty() {
            return None;
        }

        let csv_files = v
            .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect_vec();

        Some(csv_files)
    })
}

/// 从 csv_config_files 中获取 csv 文件的 headers
pub async fn get_csv_headers(dsn: &Dsn) -> anyhow::Result<HashMap<String, CsvHeader>> {
    let source_type = SourceType::try_from(dsn)?;
    let csv_files = parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
        "csv_config_file not found in dsn: {}",
        dsn.to_string()
    ))?;
    tracing::debug!("get headers from csv files: {:?}", csv_files);

    let headers = CsvParser::get_all_headers(source_type, csv_files).await?;

    Ok(headers)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::StreamExt;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_try_new() {
        let header = csv_async::StringRecord::from(vec![
            "0",
            "point_id",
            "enabled",
            "stable",
            "tbname",
            "value_col",
            "value_transform",
            "type",
            "quality_col",
            "ts_col",
            "received_ts_col",
            "ts_transform",
            "received_ts_transform",
            "tag::VARCHAR(200)::name",
        ]);

        let csv_header = CsvHeader::try_new(SourceType::OPCUA, &header).unwrap();

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
        assert!(col.is_tag);
        assert_eq!(col.tag_type, Some(IpcDataType::VarChar(200)));
    }

    #[tokio::test]
    async fn test_check_required_columns() {
        let header = csv_async::StringRecord::from(vec!["point_id", "stable", "tbname"]);
        let csv_header = CsvHeader::try_new(SourceType::OPCUA, &header).unwrap();

        let res = csv_header.check_required_columns();
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_get_csv_headers() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-utf8bom.csv").unwrap();

        let headers = get_csv_headers(&dsn).await.unwrap();
        assert_eq!(headers.len(), 1);

        let header = headers.values().next().unwrap();
        let cols = header.columns();
        assert_eq!(cols.len(), 14);
    }

    #[tokio::test]
    async fn test_open_csv_file() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        // file path
        let file = "@./tests/opc/opcua-utf8bom.csv".to_string();
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
        let file = general_purpose::STANDARD.encode(content.as_bytes());
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
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        let files = vec!["@./tests/opc/opcua-utf8bom.csv".to_string()];
        let res = CsvParser::open_csv_many(files).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().0, "@./tests/opc/opcua-utf8bom.csv");

        let files = vec!["@./tests/opc/opcua-utf8.csv".to_string()];
        let res = CsvParser::open_csv_many(files).await.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().0, "@./tests/opc/opcua-utf8.csv");

        let files = vec!["@./tests/opc/opcua-gbk.csv".to_string()];
        let res = CsvParser::open_csv_many(files).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().to_string(),
            "failed to open csv: @./tests/opc/opcua-gbk.csv, cause: invalid CSV file encoding: GBK, only UTF-8 or UTF-8 BOM supported"
        );
    }

    #[tokio::test]
    async fn test_from_dsn() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-utf8bom.csv").unwrap();
        let ua_config = CsvParser::from_dsn(&dsn).unwrap();
        assert_eq!(ua_config.source_type, SourceType::OPCUA);
        let csv_files = ua_config.csv_files;
        assert_eq!(csv_files.len(), 1);
        let path = csv_files.first().unwrap();
        assert_eq!(path, "@./tests/opc/opcua-utf8bom.csv");

        let dsn = Dsn::from_str("opcda://?csv_config_file=@./tests/opc/opcda-utf8bom.csv").unwrap();
        let da_config = CsvParser::from_dsn(&dsn).unwrap();
        assert_eq!(da_config.source_type, SourceType::OPCDA);
        let csv_files = da_config.csv_files;
        assert_eq!(csv_files.len(), 1);
        let path = csv_files.first().unwrap();
        assert_eq!(path, "@./tests/opc/opcda-utf8bom.csv");
    }

    #[tokio::test]
    async fn test_parse() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let ua_config = csv_parser.parse().await.unwrap();
        assert_eq!(ua_config.point_config_map.len(), 3);

        let dsn = Dsn::from_str("opcda://?csv_config_file=@./tests/opc/opcda-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let da_config = csv_parser.parse().await.unwrap();
        assert_eq!(da_config.point_config_map.len(), 3);
    }

    #[tokio::test]
    async fn test_parse_point_id_and_tbname() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let ua_config = csv_parser.parse_point_id_and_tbname().await.unwrap();
        assert_eq!(ua_config.len(), 2);
        let tbname = ua_config.get("ns=3;i=1005").unwrap();
        assert_eq!(tbname, "t_3_1005");
        let tbname = ua_config.get("ns=3;i=1006").unwrap();
        assert_eq!(tbname, "t_3_1006");

        let dsn = Dsn::from_str("opcda://?csv_config_file=@./tests/opc/opcda-utf8bom.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let da_config = csv_parser.parse_point_id_and_tbname().await.unwrap();
        assert_eq!(da_config.len(), 2);
        let tbname = da_config.get("root.parent.temperature").unwrap();
        assert_eq!(tbname, "t_temperature");
        let tbname = da_config.get("root.parent.current").unwrap();
        assert_eq!(tbname, "t_custom_current");
    }

    #[tokio::test]
    async fn test_invalid_csv_file() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        // zero rows
        let dsn = Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-empty.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let res = csv_parser.parse().await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "empty csv file");

        // invalid transform expression
        let dsn = Dsn::from_str(
            "opcua://?csv_config_file=@./tests/opc/opcua-utf8bom-transform-error.csv",
        )
        .unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let res = csv_parser.parse().await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid ts_transform: ts - 6h, cause: Syntax error: Unexpected 'h'"
        );

        // tbname is empty
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-tbname-empty.csv").unwrap();
        let parser = CsvParser::from_dsn(&dsn).unwrap();
        let res = parser.parse().await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "tbname cannot be empty");

        // type error
        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@./tests/opc/opcua-type-error.csv").unwrap();
        let csv_parser = CsvParser::from_dsn(&dsn).unwrap();
        let res = csv_parser.parse().await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid column data type: vacs"
        );
    }

    #[tokio::test]
    async fn test_error_name() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@./tests/opc/opcda-name-error.csv").unwrap();
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
