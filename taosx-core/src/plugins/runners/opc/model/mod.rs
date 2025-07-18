use anyhow::bail;
use csv_async::StringRecord;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use taos::{Dsn, Ty};
use taosx_ipc::prelude::IpcDataType;
use taosx_ipc::types::DataSet;

use crate::plugins::runners::opc::csv::header::CsvHeader;
use crate::plugins::runners::opc::csv::CsvParser;
use crate::runners::opc::config::OPCConfig;
use crate::runners::opc::csv::column::CsvColumn;
use crate::runners::opc::{
    generate_stable_from_pattern, generate_tag_value_from_pattern, generate_tbname_from_pattern,
    OpcType,
};
use crate::utils::rhai_syntax_validator::check_math_expression;
use crate::utils::table_meta::{TableMeta, TableMetaQuerier, TableMetaQueryBuilder};
use crate::utils::validate_table_column_name;

/// OPC 点位与 TDengine 中的表的映射关系
/// point_config_map 和 table_config_map 用来处理预定义的点位，即：通过 csv 文件已经配置好的
/// generate_rule 用来处理未定义的点位，即：动态发现的点位
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpcModelConfig {
    /// OPC 类型
    pub opc_type: OpcType,
    /// 生成点位映射规则的方式
    pub generate_rule: Option<GeneratePointMappingBy>,
    /// key: point_id, value: PointConfig
    pub point_config_map: LinkedHashMap<String, PointConfig>,
    /// key: point_id, value: TableConfig
    pub table_config_map: LinkedHashMap<String, TableConfig>,
}

impl OpcModelConfig {
    /// 检查 csv 文件的合法性
    pub fn validate(&self) -> anyhow::Result<()> {
        tracing::info!("validate model config: {:?}", self);

        // check stable
        for (point_id, point_config) in self.point_config_map.iter() {
            let stable = point_config.stable.as_ref();
            OpcModelConfig::check_stable(stable).map_err(|err| {
                anyhow::anyhow!(
                    "invalid stable of point_id: {}, cause: {}",
                    point_id,
                    err.to_string()
                )
            })?;
        }

        // check tbname
        for (point_id, point_config) in self.point_config_map.iter() {
            let tbname = point_config.code.as_str();
            OpcModelConfig::check_tbname(self.opc_type, tbname).map_err(|err| {
                anyhow::anyhow!(
                    "invalid tbname of point_id: {}, cause: {}",
                    point_id,
                    err.to_string()
                )
            })?;
        }

        // 检查 ts_col 和 received_ts_col：至少有一个
        for (point_id, table_config) in self.table_config_map.iter() {
            let mut has_primary_key = false;
            for col_config in table_config.column_configs.iter() {
                if col_config.is_primary_key {
                    has_primary_key = true;
                    break;
                }
            }
            if !has_primary_key {
                bail!(
                    "ts_col or received_ts_col is required for point_id: {}",
                    point_id
                );
            }
        }

        // 检查 tag_value 应该和 tag_type 匹配
        let joined =
            Self::join_by_point_id(self.point_config_map.clone(), self.table_config_map.clone());
        let joined_tags = Self::fetch_tags(joined);

        for (point_id, tags) in joined_tags {
            for (_tag_name, tag_val, tag_type) in tags {
                Self::check_tag_type(tag_val.as_str(), &tag_type).map_err(|err| {
                    anyhow::anyhow!(
                        "tag value and type not match, point_id: {}, cause: {}",
                        point_id,
                        err.to_string()
                    )
                })?;
            }
        }

        Ok(())
    }

    /// CSV 中 stable 不能为空，且 stable 为任意字符串，如果存在 {}，则{}中间的 string 必须为 type
    fn check_stable(stable: Option<&String>) -> anyhow::Result<()> {
        match stable {
            None => {
                bail!("stable is required");
            }
            Some(stable) => {
                if stable.is_empty() {
                    bail!("stable is required");
                }
                if !stable.contains("{") {
                    return Ok(());
                }
                // stable 为任意字符串，如果存在 {}，则{}中间的 string 必须为 type
                let regex = Regex::new(r"\{([^{}]+)\}")?;
                for cap in regex.captures_iter(stable) {
                    let cap_str = cap.get(1).unwrap().as_str();
                    if cap_str != "type" {
                        bail!("invalid stable expression: {}", stable,);
                    }
                }
            }
        }

        Ok(())
    }

    /// 校验 tbname 配置是否合法，tbname 为任意字符串，如果存在 {}，则{}中间的 string 必须为 ns、id、tag_name
    fn check_tbname(opc_type: OpcType, tbname: &str) -> anyhow::Result<()> {
        if tbname.is_empty() {
            bail!("tbname is required");
        }
        if !tbname.contains("{") {
            return Ok(());
        }
        // tbname 为任意字符串，如果存在 {}，则{}中间的 string 必须为 ns、id、tag_name
        let regex = Regex::new(r"\{([^{}]+)\}")?;
        for cap in regex.captures_iter(tbname) {
            let cap_str = cap.get(1).unwrap().as_str();
            match opc_type {
                OpcType::OPCUA => {
                    if cap_str != "ns" && cap_str != "id" {
                        bail!("invalid tbname expression: {}", tbname);
                    }
                }
                OpcType::OPCDA => {
                    if cap_str != "tag_name" {
                        bail!("invalid tbname expression: {}", tbname);
                    }
                }
                OpcType::FAKE => {
                    // nothing to do
                }
            }
        }

        Ok(())
    }

    /// 校验 tag_value 是否和 tag_type 匹配
    fn check_tag_type<T: AsRef<str>>(tag_value: T, tag_type: &IpcDataType) -> anyhow::Result<()> {
        let tag_value = tag_value.as_ref();
        match tag_type {
            IpcDataType::Bool => {
                let _ = tag_value
                    .parse::<bool>()
                    .map_err(|_e| anyhow::anyhow!("{} is not bool type", tag_value))?;
            }
            IpcDataType::UInt8 => {
                let _ = tag_value
                    .parse::<u8>()
                    .map_err(|_e| anyhow::anyhow!("{} is not u8 type", tag_value))?;
            }
            IpcDataType::UInt16 => {
                let _ = tag_value
                    .parse::<u16>()
                    .map_err(|_e| anyhow::anyhow!("{} is not u16 type", tag_value))?;
            }
            IpcDataType::UInt32 => {
                let _ = tag_value
                    .parse::<u32>()
                    .map_err(|_e| anyhow::anyhow!("{} is not u32 type", tag_value))?;
            }
            IpcDataType::UInt64 => {
                let _ = tag_value
                    .parse::<u64>()
                    .map_err(|_e| anyhow::anyhow!("{} is not u64 type", tag_value))?;
            }
            IpcDataType::Int8 => {
                let _ = tag_value
                    .parse::<i8>()
                    .map_err(|_e| anyhow::anyhow!("{} is not i8 type", tag_value))?;
            }
            IpcDataType::Int16 => {
                let _ = tag_value
                    .parse::<i16>()
                    .map_err(|_e| anyhow::anyhow!("{} is not i16 type", tag_value))?;
            }
            IpcDataType::Int32 => {
                let _ = tag_value
                    .parse::<i32>()
                    .map_err(|_e| anyhow::anyhow!("{} is not i32 type", tag_value))?;
            }
            IpcDataType::Int64 => {
                let _ = tag_value
                    .parse::<i64>()
                    .map_err(|_e| anyhow::anyhow!("{} is not i64 type", tag_value))?;
            }
            IpcDataType::Float32 => {
                let _ = tag_value
                    .parse::<f32>()
                    .map_err(|_e| anyhow::anyhow!("{} is not f32 type", tag_value))?;
            }
            IpcDataType::Float64 => {
                let _ = tag_value
                    .parse::<f64>()
                    .map_err(|_e| anyhow::anyhow!("{} is not f64 type", tag_value))?;
            }
            IpcDataType::NChar(len) => {
                if tag_value.len() > *len as usize {
                    bail!("{} out of range, nchar({})", tag_value, len);
                }
            }
            IpcDataType::VarChar(len) => {
                if tag_value.len() > *len as usize {
                    bail!("{} out of range, varchar({})", tag_value, len);
                }
            }
            IpcDataType::VarBinary(len) => {
                if tag_value.len() > *len as usize {
                    bail!("{} out of range, varbinary({})", tag_value, len);
                }
            }
            _ => {
                // nothing to do
            }
        }
        Ok(())
    }

    /// 检查 csv 文件和 TDengine 的 schema 是否冲突
    pub async fn validate_with_sink(
        &self,
        model_type: ModelType,
        sink: &Dsn,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "validate model config, model_type: {:?}, sink: {:?}",
            model_type,
            sink
        );

        let querier = TableMetaQueryBuilder::new(sink)?.build().await?;

        match model_type {
            // 单列模型
            ModelType::SingleColumn => self.validate_single_column_model(querier).await,
            // 多列模型
            ModelType::MultiColumn => self.validate_multi_column_model(querier).await,
        }
    }

    async fn validate_single_column_model(&self, querier: TableMetaQuerier) -> anyhow::Result<()> {
        let joined =
            Self::join_by_point_id(self.point_config_map.clone(), self.table_config_map.clone());

        for (point_id, (point_config, table_config)) in joined {
            let stable = point_config.stable.unwrap();
            let tbname = point_config.code;

            match (
                PointConfig::is_expr(self.opc_type, "stable", stable.as_str()),
                PointConfig::is_expr(self.opc_type, "tbname", tbname.as_str()),
            ) {
                // stable 为表达式
                (true, _) => {
                    // 不需要校验
                    continue;
                }
                // stable 不是表达式，tbname 是表达式
                (false, true) => {
                    // 如果 stable 在 database 中不存在，不校验
                    if let Some(stable_meta) = querier.super_table_meta(stable.as_str())? {
                        Self::is_column_conflict(
                            &querier,
                            point_id.as_str(),
                            &table_config,
                            stable_meta,
                        )
                        .await?;
                        Self::is_tag_conflict(
                            &querier,
                            point_id.as_str(),
                            &table_config,
                            stable_meta,
                        )
                        .await?;
                    }
                }
                // stable 和 tbname 都不是表达式
                (false, false) => {
                    // stable 在 database 中必须存在；否则，校验失败。
                    Self::is_stable_and_tbname_conflict(
                        &querier,
                        point_id.as_str(),
                        stable.as_str(),
                        tbname.as_str(),
                        &table_config,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    async fn is_stable_and_tbname_conflict(
        querier: &TableMetaQuerier,
        point_id: &str,
        stable: &str,
        tbname: &str,
        table_config: &TableConfig,
    ) -> anyhow::Result<()> {
        let stable_meta = querier.super_table_meta(stable)?;
        let tb_meta = querier.child_table_meta(tbname)?;
        match (stable_meta, tb_meta) {
            (None, None) => {
                bail!(
                    "stable: {} and tbname: {} not exist in database, point_id: {}",
                    stable,
                    tbname,
                    point_id
                );
            }
            (None, Some(_)) => {
                bail!(
                    "stable: {} not exist in database, point_id: {}",
                    stable,
                    point_id
                );
            }
            (Some(_), None) => {
                bail!(
                    "tbname: {} not exist in database, point_id: {}",
                    tbname,
                    point_id
                );
            }
            (Some(stable_meta), Some(_tb_meta)) => {
                if !querier.is_child_of_stable(stable, tbname)? {
                    bail!(
                        "tbname: {} is not child table of super table: {}, point_id: {}",
                        tbname,
                        stable,
                        point_id
                    );
                }
                Self::is_column_conflict(querier, point_id, table_config, stable_meta).await?;
                Self::is_tag_conflict(querier, point_id, table_config, stable_meta).await?;
            }
        }
        Ok(())
    }

    /// csv 中的 val_col/ts_col/request_ts_col/received_ts_col/quality_col 如果有值，则必须在 database 中存在
    async fn is_column_conflict(
        querier: &TableMetaQuerier,
        point_id: &str,
        table_config: &TableConfig,
        stable_meta: &TableMeta,
    ) -> anyhow::Result<()> {
        for col in [
            ColumnConfig::VALUE,
            ColumnConfig::ORIGINAL_TS,
            ColumnConfig::REQUEST_TS,
            ColumnConfig::RECEIVED_TS,
            ColumnConfig::QUALITY,
        ] {
            let col_name = table_config
                .column_config(col)
                .and_then(|v| v.alias.as_ref());
            if let Some(col_name) = col_name {
                if !querier.is_stable_col_exist(stable_meta.tbname.as_str(), col_name)? {
                    bail!(
                        "column: {} not exist in table: {}, point_id: {}",
                        col,
                        stable_meta.tbname.as_str(),
                        point_id
                    );
                }
            }
        }
        Ok(())
    }

    /// CSV 中的 tag 集合为 U1，database 的超级表的 tag 集合为 U2，
    /// 则 U2 必须包含 U1，且 U1 的 tag type 必须与 U2 的一致。
    async fn is_tag_conflict(
        querier: &TableMetaQuerier,
        point_id: &str,
        table_config: &TableConfig,
        stable_meta: &TableMeta,
    ) -> anyhow::Result<()> {
        if let Some(tags) = table_config.tag_configs.as_ref() {
            for tag in tags {
                let tag_name = tag.name.as_str();
                let tag_type = tag.r#type.to_string();
                if !querier.is_stable_tag_exist(
                    stable_meta.tbname.as_str(),
                    tag_name,
                    tag_type.as_str(),
                )? {
                    bail!(
                        "tag: {} not exist in super table: {}, point_id: {}",
                        tag_name,
                        stable_meta.tbname.as_str(),
                        point_id,
                    );
                }
            }
        }
        Ok(())
    }

    async fn validate_multi_column_model(&self, querier: TableMetaQuerier) -> anyhow::Result<()> {
        let joined =
            Self::join_by_point_id(self.point_config_map.clone(), self.table_config_map.clone());

        for (point_id, (point_config, table_config)) in joined {
            let stable = point_config.stable.unwrap();
            let tbname = point_config.code;
            match (
                PointConfig::is_expr(self.opc_type, "stable", stable.as_str()),
                PointConfig::is_expr(self.opc_type, "tbname", tbname.as_str()),
            ) {
                (false, false) => {
                    Self::is_stable_and_tbname_conflict(
                        &querier,
                        point_id.as_str(),
                        stable.as_str(),
                        tbname.as_str(),
                        &table_config,
                    )
                    .await?;
                }
                (_, _) => {
                    // 对于多列模型，stable 和 tbname 都不能是表达式
                    bail!(
                        "stable and tbname should not be an expression, point_id: {}",
                        point_id
                    );
                }
            }
        }

        Ok(())
    }

    pub fn get_point_mapping(
        &self,
        point_id: &str,
    ) -> anyhow::Result<Option<(&PointConfig, &TableConfig)>> {
        let point_config = self.point_config_map.get(point_id);
        let table_config = self.table_config_map.get(point_id);

        match (point_config, table_config) {
            (Some(point_config), Some(table_config)) => Ok(Some((point_config, table_config))),
            (None, None) => Ok(None),
            _ => bail!(
                "point_id: {} not found in point_config_map or table_config_map",
                point_id
            ),
        }
    }

    pub async fn generate_point_mapping(
        &self,
        point_id: &str,
        value_type: &IpcDataType,
    ) -> anyhow::Result<(PointConfig, TableConfig)> {
        if self.point_config_map.len() != self.table_config_map.len() {
            bail!(
                "point_config_map length: {} not equal to table_config_map length: {}",
                self.point_config_map.len(),
                self.table_config_map.len()
            );
        }

        let generate_rule = self
            .generate_rule
            .clone()
            .ok_or(anyhow::anyhow!("generate_rule is required"))?;

        match &generate_rule {
            GeneratePointMappingBy::Rule(rule) => {
                let index = self.point_config_map.len();
                let p =
                    rule.gen_point_config(index, point_id.to_string(), Some(value_type.clone()))?;
                let t = rule.gen_table_config(Some(value_type.clone()))?;
                Ok((p, t))
            }
            GeneratePointMappingBy::Csv((csv_files, csv_origin)) => {
                let parser = match csv_origin {
                    None => CsvParser::try_new(self.opc_type, csv_files.clone())?,
                    Some(csv_origin) => {
                        CsvParser::try_new(self.opc_type, vec![format!("@{}", csv_origin)])?
                    }
                };

                let (p, t) = parser.parse_one(point_id).await?.ok_or(anyhow::anyhow!(
                    "point_id: {} not found in csv files: {:?}",
                    point_id,
                    csv_files
                ))?;
                Ok((p, t))
            }
        }
    }

    pub async fn transform_map(
        &self,
        columns: &[&str],
    ) -> anyhow::Result<HashMap<String, HashMap<String, ColumnConfig>>> {
        match &self.generate_rule {
            None => {
                bail!("generate rule is required")
            }
            Some(GeneratePointMappingBy::Rule(_rule)) => {
                tracing::warn!(
                    "generate transform map by GeneratePointMappingBy::Rule is not supported"
                );
                Ok(HashMap::new())
            }
            Some(GeneratePointMappingBy::Csv((csv, csv_origin))) => match csv_origin {
                None => {
                    let rdr = CsvParser::open_csv_many(csv.clone()).await?;
                    CsvParser::parse_transform_map(self.opc_type, rdr, columns).await
                }
                Some(csv_origin) => {
                    let rdr = CsvParser::open_csv_many(vec![format!("@{}", csv_origin)]).await?;
                    CsvParser::parse_transform_map(self.opc_type, rdr, columns).await
                }
            },
        }
    }

    pub fn get_column_config_map_by_name(&self, col_name: &str) -> HashMap<String, ColumnConfig> {
        let mut column_config_map = HashMap::new();

        for (point_id, table_config) in &self.table_config_map {
            let column_config = table_config.column_config(col_name);
            if let Some(column_config) = column_config {
                column_config_map.insert(point_id.clone(), column_config.clone());
            }
        }

        column_config_map
    }

    pub fn is_conflict(
        point_id: &str,
        point_config: &PointConfig,
        table_config: &TableConfig,
        point_config_map: &LinkedHashMap<String, PointConfig>,
        table_config_map: &LinkedHashMap<String, TableConfig>,
    ) -> anyhow::Result<()> {
        if table_config.enabled.is_some_and(|v| v == 0) {
            return Ok(());
        }

        let stable = point_config.stable.as_ref();
        let tbname = point_config.code.as_str();

        if let Some(stable) = stable {
            if stable.contains("{type}") {
                return Ok(());
            }
        }
        if tbname.contains("{id}") || tbname.contains("{ns}") || tbname.contains("{tag_name}") {
            return Ok(());
        }

        let value_col = table_config
            .column_config(ColumnConfig::VALUE)
            .and_then(|v| v.alias.as_ref());

        // 遍历 self.point_config_map 和 self.table_config_map，当 stable 和 tbname 时，value_col 应该不同，否则报错
        for (id, p_config) in point_config_map {
            if let Some(t_config) = table_config_map.get(id) {
                if p_config.stable.as_ref() == stable && p_config.code.as_str() == tbname {
                    if let Some(v_col) = t_config.column_config(ColumnConfig::VALUE) {
                        if v_col.alias.as_ref() == value_col {
                            bail!(
                                "point_id: {} and point_id: {} have same stable: {} and tbname: {}, value_col should be different",
                                id,
                                point_id,
                                stable.unwrap(),
                                tbname,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 返回一个 LinkedHashMap, key 是 point_id, value 是 (PointConfig, TableConfig)
    /// 通过 point_id 将 point_config_map 和 table_config_map 合并
    fn join_by_point_id(
        point_config_map: LinkedHashMap<String, PointConfig>,
        table_config_map: LinkedHashMap<String, TableConfig>,
    ) -> LinkedHashMap<String, (PointConfig, TableConfig)> {
        let joined: LinkedHashMap<String, (PointConfig, TableConfig)> = point_config_map
            .iter()
            .filter_map(|(point_id, point_config)| {
                table_config_map.get(point_id).map(|table_config| {
                    (
                        point_id.clone(),
                        (point_config.clone(), table_config.clone()),
                    )
                })
            })
            .collect();
        joined
    }

    /// 返回的结果是一个 LinkedHashMap，key 为 point_id，value 为 (tag_name, tag_value, tag_type)
    fn fetch_tags(
        joined: LinkedHashMap<String, (PointConfig, TableConfig)>,
    ) -> LinkedHashMap<String, Vec<(String, String, IpcDataType)>> {
        let mut joined_tags = LinkedHashMap::new();
        for (point_id, (point_config, table_config)) in joined {
            // 如果 point_config.tag_values 为空，或者 table_config.tag_config 为空，跳过
            if point_config.tag_values.is_none() || table_config.tag_configs.is_none() {
                continue;
            }
            let tag_values = point_config.tag_values.as_ref().unwrap();
            let tag_config = table_config.tag_configs.as_ref().unwrap();
            // tag_config 和 tag_values 通过 name join
            let tags = tag_config
                .iter()
                .filter_map(|tag_config| {
                    let tag_name = tag_config.name.as_str();
                    let tag_type = tag_config.r#type.clone();
                    tag_values
                        .get(tag_name)
                        .map(|tag_value| (tag_name.to_string(), tag_value.clone(), tag_type))
                })
                .collect_vec();
            joined_tags.insert(point_id.clone(), tags);
        }

        joined_tags
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PointConfig {
    /// 点位在csv文件中的行号
    pub row_index: usize,
    /// 点位对应的 tbname
    pub code: String,
    /// 点位对应的 stable
    pub stable: Option<String>,
    /// 点位对应的 tag 列的值，key 为 tag_name，value 为 tag_value
    pub tag_values: Option<HashMap<String, String>>,
    /// 点位对应的 type
    pub value_type: Option<IpcDataType>,
}

impl PointConfig {
    /// return true if the column value is an expression
    pub fn is_expr(opc_type: OpcType, col_name: &str, col_value: &str) -> bool {
        match col_name {
            "stable" => col_value.contains("{type}"),
            "tbname" => match opc_type {
                OpcType::OPCUA => col_value.contains("{id}") || col_value.contains("{ns}"),
                OpcType::OPCDA => col_value.contains("{tag_name}"),
                OpcType::FAKE => false,
            },
            _ => false,
        }
    }

    pub fn from_csv(
        header: &CsvHeader,
        row: &StringRecord,
        row_index: usize,
    ) -> anyhow::Result<Self> {
        let code = CsvParser::parse_tbname(header, row)?;
        let value_type = parse_type(header, row)?;
        let stable = parse_stable(header, row);
        let tag_values = parse_tag_values(header, row);
        if stable.is_some() {
            validate_table_column_name("stable name", stable.as_ref().unwrap())?;
        }

        // 遍历tag_values，校验tag_values中的tag_name是否合法
        if tag_values.is_some() {
            for tag_name in tag_values.as_ref().unwrap().keys() {
                validate_table_column_name("tag name", tag_name)?;
            }
        }

        Ok(PointConfig {
            row_index,
            code,
            stable,
            tag_values,
            value_type,
        })
    }
}

/// 解析 csv 中的 type 列
fn parse_type(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Option<IpcDataType>> {
    header
        .get_column("type")
        .and_then(|col| row.get(col.index))
        .map(|val| {
            if val.is_empty() {
                return Ok(None);
            }
            match IpcDataType::from_str(val) {
                Err(_e) => {
                    bail!("invalid column data type: {}", val)
                }
                Ok(value_type) => Ok(Some(value_type)),
            }
        })
        .unwrap_or(Ok(None))
}

fn parse_raw_type(header: &CsvHeader, row: &StringRecord) -> Option<String> {
    header
        .get_column("type")
        .and_then(|col| row.get(col.index))
        .and_then(|val| {
            if val.is_empty() {
                return None;
            }
            match val.find("(") {
                Some(index) => Some(val[..index].to_string().replace(" ", "_")),
                None => Some(val.replace(" ", "_")),
            }
        })
}

fn parse_stable(header: &CsvHeader, row: &StringRecord) -> Option<String> {
    header
        .get_column("stable")
        .and_then(|col| row.get(col.index))
        .and_then(|val| {
            if val.is_empty() {
                return None;
            }
            let val = val.replace(".", "_");
            let val_type = parse_raw_type(header, row);
            // replace {type} with type_value
            let stable_name = match (val.contains("{type}"), val_type) {
                (true, Some(val_type)) => val.replace("{type}", &val_type),
                _ => val,
            };
            Some(stable_name)
        })
}

/// example:
///      tag::VARCHAR(200)::name
///      入库温度
/// tag_value map:
///      name => 入库温度
///
/// ns=2;s=PLC.DEV.SITE
/// example template:
///     tag::VARCHAR(200)::id
///     {id}
///
/// tag_value map:
///     id => "PLC.DEV.SITE"
fn parse_tag_values(header: &CsvHeader, row: &StringRecord) -> Option<HashMap<String, String>> {
    let mut map = HashMap::new();
    let point_id = CsvParser::parse_point_id(header, row).ok()?;

    for col in header.get_columns() {
        if !col.is_tag {
            continue;
        }
        let tag_name = col.name.clone();
        let tag_value = row.get(col.index).unwrap_or("").to_string();
        let tag_value = if tag_value.contains("{") {
            // replace {tag_name} or {TagName} in tbname
            let opc_type = header.get_opc_type();
            generate_tag_value_from_pattern(opc_type.as_static_str(), &tag_value, &point_id)
        } else {
            tag_value
        };
        map.insert(tag_name, tag_value);
    }

    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TableConfig {
    /// enabled: 1 / 0
    pub enabled: Option<i8>,
    pub stable_prefix: Option<String>,
    /// column: original_ts / received_ts / value / quality
    pub column_configs: Vec<ColumnConfig>,
    /// tags(name, type) in csv header
    pub tag_configs: Option<Vec<TagConfig>>,
}

const DEFAULT_STABLE_PREFIX: &str = "opc";

impl TableConfig {
    pub fn from_csv(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Self> {
        let stable = parse_stable(header, row);
        let stable_prefix = match stable {
            None => Some(String::from(DEFAULT_STABLE_PREFIX)),
            Some(_stable) => None,
        };

        let enabled = CsvParser::parse_enabled(header, row)?;
        let column_configs = Self::parse_columns(header, row)?;
        let tag_configs = Self::parse_tags(header);
        let tag_configs = if tag_configs.is_empty() {
            None
        } else {
            Some(tag_configs)
        };

        Ok(Self {
            enabled,
            stable_prefix,
            column_configs,
            tag_configs,
        })
    }

    pub fn column_config(&self, name: &str) -> Option<&ColumnConfig> {
        self.column_configs.iter().find(|c| c.name == name)
    }

    fn parse_columns(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Vec<ColumnConfig>> {
        let mut columns = Vec::new();

        for col in header.get_columns() {
            match col.name.as_str() {
                CsvColumn::VALUE_COL => {
                    let value = Self::parse_value_col(header, row)?;
                    columns.push(value);
                }
                CsvColumn::QUALITY_COL => {
                    let quality = Self::parse_quality_col(header, row)?;
                    if let Some(quality) = quality {
                        columns.push(quality);
                    }
                }
                CsvColumn::TS_COL => {
                    let ts = Self::parse_timestamp_col(
                        header,
                        row,
                        CsvColumn::TS_COL,
                        CsvColumn::TS_TRANSFORM,
                    )?;
                    if let Some(ts) = ts {
                        columns.push(ts);
                    }
                }
                CsvColumn::REQUEST_TS_COL => {
                    let qts = Self::parse_timestamp_col(
                        header,
                        row,
                        CsvColumn::REQUEST_TS_COL,
                        CsvColumn::REQUEST_TS_TRANSFORM,
                    )?;
                    if let Some(qts) = qts {
                        columns.push(qts);
                    }
                }
                CsvColumn::RECEIVED_TS_COL => {
                    let rts = Self::parse_timestamp_col(
                        header,
                        row,
                        CsvColumn::RECEIVED_TS_COL,
                        CsvColumn::RECEIVED_TS_TRANSFORM,
                    )?;
                    if let Some(rts) = rts {
                        columns.push(rts);
                    }
                }
                &_ => {}
            }
        }

        let ts_col = header.get_column(CsvColumn::TS_COL);
        let qts_col = header.get_column(CsvColumn::REQUEST_TS_COL);
        let rts_col = header.get_column(CsvColumn::RECEIVED_TS_COL);

        // 如果 ts_col/request_ts_col/received_ts_col 都不存在，则添加一个默认的 ts_col
        if ts_col.is_none() && qts_col.is_none() && rts_col.is_none() {
            columns.push(ColumnConfig {
                name: ColumnConfig::ORIGINAL_TS.to_string(),
                r#type: Some(Ty::Timestamp),
                alias: Some("ts".to_string()),
                transform: None,
                is_primary_key: true,
            });
        }

        Ok(columns)
    }

    fn parse_tags(header: &CsvHeader) -> Vec<TagConfig> {
        let mut tags = Vec::new();

        for col in header.get_columns() {
            if !col.is_tag {
                continue;
            }

            let tag_name = col.name.clone();
            let tag_type = col.tag_type.clone().unwrap();
            let tag_config = TagConfig {
                name: tag_name,
                r#type: tag_type,
            };
            tags.push(tag_config);
        }

        tags
    }

    fn parse_value_col(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<ColumnConfig> {
        let value_name = header
            .get_column("value_col")
            .and_then(|col| row.get(col.index))
            .map_or(Some("val".to_string()), |val| {
                if val.is_empty() {
                    Some("val".to_string())
                } else {
                    Some(val.to_string())
                }
            });

        let value_type = header
            .get_column("type")
            .and_then(|col| row.get(col.index))
            .and_then(|val| {
                if val.is_empty() {
                    None
                } else {
                    let val_type = IpcDataType::from_str(val)
                        .map(|val_type| val_type.ty())
                        .map_err(|_err| anyhow::anyhow!("invalid column data type: {}", val));
                    Some(val_type)
                }
            })
            .transpose()?;

        let value_transform = header
            .get_column("value_transform")
            .and_then(|col| row.get(col.index))
            .and_then(|val| {
                if val.is_empty() {
                    None
                } else {
                    Some(val.to_string())
                }
            });

        match (value_name.as_ref(), value_transform.as_ref()) {
            (Some(value_name), Some(value_transform)) => {
                // 校验列名
                validate_table_column_name("value column name", value_name)?;
                // 校验表达式
                check_math_expression(value_name, value_transform).map_err(|e| {
                    anyhow::anyhow!(
                        "invalid value_transform: {}, cause: {}",
                        value_transform,
                        e.to_string()
                    )
                })?;
            }
            (Some(value_name), None) => {
                // 校验列名
                validate_table_column_name("value column name", value_name)?;
            }
            (None, _) => {
                panic!("value column name cannot be None");
            }
        }

        Ok(ColumnConfig {
            name: ColumnConfig::VALUE.to_string(),
            r#type: value_type,
            alias: value_name,
            transform: value_transform,
            is_primary_key: false,
        })
    }

    fn parse_quality_col(
        header: &CsvHeader,
        row: &StringRecord,
    ) -> anyhow::Result<Option<ColumnConfig>> {
        let col = header
            .get_column("quality_col")
            .and_then(|col| row.get(col.index));

        if col.is_none() {
            return Ok(None);
        }

        let quality_col = col.unwrap();
        let quality_col = if quality_col.is_empty() {
            "quality".to_string()
        } else {
            quality_col.to_string()
        };

        // todo!("check column name")
        // if quality.is_some() {
        //     let quality_column = quality.unwrap();
        //     let quality_name = quality_column.alias.as_ref().unwrap();
        //     validate_table_column_name("quality column name", quality_name)?;
        // }

        Ok(Some(ColumnConfig {
            name: ColumnConfig::QUALITY.to_string(),
            r#type: Some(Ty::Int),
            alias: Some(quality_col),
            transform: None,
            is_primary_key: false,
        }))
    }

    fn parse_timestamp_col(
        header: &CsvHeader,
        row: &StringRecord,
        col_name: &str,
        col_transform: &str,
    ) -> anyhow::Result<Option<ColumnConfig>> {
        let col = header.get_column(col_name);
        if col.is_none() {
            return Ok(None);
        }
        let col = col.unwrap();

        let col_value = row
            .get(col.index)
            .and_then(|val| if val.is_empty() { None } else { Some(val) });
        if col_value.is_none() {
            return Ok(None);
        }
        let col_value = col_value.unwrap();
        // 校验列名
        validate_table_column_name(col_name, col_value)?;
        // transform
        let transform = header
            .get_column(col_transform)
            .and_then(|col| row.get(col.index))
            .and_then(|val| if val.is_empty() { None } else { Some(val) });
        // 校验表达式
        if let Some(transform) = transform {
            check_math_expression(col_value, transform)
                .map_err(|e| anyhow::anyhow!("invalid {col_transform}: {transform}, cause: {e}"))?;
        }

        let column_config = ColumnConfig {
            name: ColumnConfig::from_csv_column_name(col_name).to_string(),
            r#type: Some(Ty::Timestamp),
            alias: Some(col_value.to_string()),
            transform: transform.map(|v| v.to_string()),
            is_primary_key: col.is_primary_key,
        };

        Ok(Some(column_config))
    }
}

#[derive(Clone, Deserialize, Debug, Serialize, PartialEq)]
pub struct ColumnConfig {
    ///  original_ts / received_ts / value / quality
    pub name: String,
    pub r#type: Option<Ty>,
    /// column name in TDengine
    pub alias: Option<String>,
    pub transform: Option<String>,
    pub is_primary_key: bool,
}

impl ColumnConfig {
    /// OPC Server 的采集时间戳
    pub const ORIGINAL_TS: &'static str = "original_ts";
    /// 查询点位值的发起时间
    pub const REQUEST_TS: &'static str = "request_ts";
    /// 查询点位值的接收时间
    pub const RECEIVED_TS: &'static str = "received_ts";
    pub const VALUE: &'static str = "value";
    pub const QUALITY: &'static str = "quality";

    pub fn from_csv_column_name(csv_column_name: &str) -> &str {
        match csv_column_name {
            CsvColumn::TS_COL => ColumnConfig::ORIGINAL_TS,
            CsvColumn::REQUEST_TS_COL => ColumnConfig::REQUEST_TS,
            CsvColumn::RECEIVED_TS_COL => ColumnConfig::RECEIVED_TS,
            CsvColumn::VALUE_COL => ColumnConfig::VALUE,
            CsvColumn::QUALITY_COL => ColumnConfig::QUALITY,
            _ => {
                unreachable!("invalid csv column name: {}", csv_column_name);
            }
        }
    }
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TagConfig {
    pub name: String,
    pub r#type: IpcDataType,
}

/// 点位映射规则的生成方式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GeneratePointMappingBy {
    /// 通过自定义的规则生成点位映射规则
    Rule(OpcPointMappingRule),
    /// 通过csv文件中的配置生成点位映射规则
    Csv((Vec<String>, Option<String>)),
}

/// 点位映射规则
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpcPointMappingRule {
    pub opc_type: OpcType,
    /// 超级表名的表达式
    pub stable_expression: String,
    /// 字表名的表达式
    pub tbname_expression: String,
    /// 主键
    pub primary_key: String,
    /// 主键的别名
    pub primary_key_alias: String,
}

impl OpcPointMappingRule {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let stable_expression = OPCConfig::parse_stable_expression(dsn)?;
        let tbname_expression = OPCConfig::parse_tbname_expression(dsn)?;
        let primary_key =
            OPCConfig::parse_primary_key(dsn)?.unwrap_or(ColumnConfig::ORIGINAL_TS.to_string());
        let primary_key_alias =
            OPCConfig::parse_primary_key_alias(dsn)?.unwrap_or("ts".to_string());

        Ok(Self {
            opc_type,
            stable_expression,
            tbname_expression,
            primary_key,
            primary_key_alias,
        })
    }

    pub fn generate(
        &self,
        data: Vec<DataSet>,
    ) -> anyhow::Result<(
        LinkedHashMap<String, PointConfig>,
        LinkedHashMap<String, TableConfig>,
    )> {
        let mut point_map = LinkedHashMap::new();
        let mut table_map = LinkedHashMap::new();

        for (index, p) in data.into_iter().enumerate() {
            let point_id = p.id;
            let point_type = p.r#type;

            let value_type = point_type
                .map(|t| {
                    IpcDataType::from_str(t.as_str()).map_err(|_err| {
                        anyhow::anyhow!("failed to convert point type: {} to IpcDataType", t)
                    })
                })
                .transpose()?;

            // point_config
            let point_config =
                self.gen_point_config(index, point_id.clone(), value_type.clone())?;
            point_map.insert(point_id.clone(), point_config);

            // table_config
            let table_config = self.gen_table_config(value_type.clone())?;
            table_map.insert(point_id.clone(), table_config);
        }

        Ok((point_map, table_map))
    }

    pub fn gen_point_config(
        &self,
        index: usize,
        point_id: String,
        point_type: Option<IpcDataType>,
    ) -> anyhow::Result<PointConfig> {
        let driver = self.opc_type.to_string();

        // 生成 tbname
        let tbname = generate_tbname_from_pattern(
            driver.as_str(),
            self.tbname_expression.as_str(),
            point_id.as_str(),
        );
        // 生成 stable
        let stable = generate_stable_from_pattern(&self.stable_expression, &point_type);

        let point_config = PointConfig {
            row_index: index,
            code: tbname,
            stable: Some(stable),
            tag_values: None,
            value_type: point_type,
        };

        Ok(point_config)
    }

    pub fn gen_table_config(&self, point_type: Option<IpcDataType>) -> anyhow::Result<TableConfig> {
        let value_type = point_type.map(|t| t.ty());

        let mut column_configs = vec![];
        column_configs.push(ColumnConfig {
            name: ColumnConfig::VALUE.to_string(),
            r#type: value_type,
            alias: Some(String::from("val")),
            transform: None,
            is_primary_key: false,
        });
        column_configs.push(ColumnConfig {
            name: ColumnConfig::QUALITY.to_string(),
            r#type: Some(Ty::Int),
            alias: None,
            transform: None,
            is_primary_key: false,
        });
        match self.primary_key.as_str() {
            ColumnConfig::ORIGINAL_TS => {
                column_configs.push(ColumnConfig {
                    name: ColumnConfig::ORIGINAL_TS.to_string(),
                    r#type: Some(Ty::Timestamp),
                    alias: Some(self.primary_key_alias.clone()),
                    transform: None,
                    is_primary_key: true,
                });
            }
            ColumnConfig::REQUEST_TS => {
                column_configs.push(ColumnConfig {
                    name: ColumnConfig::REQUEST_TS.to_string(),
                    r#type: Some(Ty::Timestamp),
                    alias: Some(self.primary_key_alias.clone()),
                    transform: None,
                    is_primary_key: true,
                });
            }
            ColumnConfig::RECEIVED_TS => {
                column_configs.push(ColumnConfig {
                    name: ColumnConfig::RECEIVED_TS.to_string(),
                    r#type: Some(Ty::Timestamp),
                    alias: Some(self.primary_key_alias.clone()),
                    transform: None,
                    is_primary_key: true,
                });
            }
            _ => {
                bail!("invalid primary key: {}", self.primary_key);
            }
        }

        let table_config = TableConfig {
            enabled: Some(1),
            stable_prefix: None,
            column_configs,
            tag_configs: None,
        };

        Ok(table_config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModelType {
    /// 单列模型：一个点位对应一张表
    SingleColumn,
    /// 多列模型：多个点位对应一张表
    MultiColumn,
}

impl From<&str> for ModelType {
    fn from(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "multi_column" => ModelType::MultiColumn,
            "single_column" => ModelType::SingleColumn,
            _ => ModelType::SingleColumn,
        }
    }
}

impl ModelType {
    pub fn from_dsn(dsn: &Dsn) -> Option<Self> {
        dsn.params
            .get("model_type")
            .map(|v| ModelType::from(v.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_mapping_generate() {
        let json = r#"[{"id":"ns=3;s=\"数据块_1\".\"Tag1\"","name":"\"数据块_1\".\"Tag1\""}]"#;
        let res: Vec<DataSet> = serde_json::from_slice(json.as_bytes()).unwrap();

        let rule = OpcPointMappingRule {
            opc_type: OpcType::OPCUA,
            stable_expression: "opc_{type}".to_string(),
            tbname_expression: "t_{ns}_{id}".to_string(),
            primary_key: "original_ts".to_string(),
            primary_key_alias: "ts".to_string(),
        };

        let (p, _t) = rule.generate(res).unwrap();

        let points = p
            .iter()
            .map(|(point_id, point_config)| format!("{}::{}", point_id, point_config.code.clone()))
            .join(",");

        assert_eq!(points, r#"ns=3;s="数据块_1"."Tag1"::t_3_"数据块_1"_"Tag1""#);
    }

    /// 这个测试用例和 opcua_sanity_1.csv 的配置相同
    #[test]
    fn test_table_config_from_csv() {
        let csv_header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["tbname", "point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["tb1", "ns=3;i=1015", "stb_int"]);
        let table_config = TableConfig::from_csv(&csv_header, &row).unwrap();

        assert_eq!(table_config.stable_prefix, None);
        assert_eq!(table_config.enabled, None);
        // TODO: 只有一个 ts 列
        assert_eq!(table_config.column_configs.len(), 1);
        assert!(table_config.tag_configs.is_none());
    }

    #[tokio::test]
    async fn test_check_stable() {
        // given
        let stable = Some("opc_{type}".to_string());
        // when
        let res = OpcModelConfig::check_stable(stable.as_ref());
        // then
        assert!(res.is_ok());

        // given
        let stable = Some("opc_abc".to_string());
        // when
        let res = OpcModelConfig::check_stable(stable.as_ref());
        // then
        assert!(res.is_ok());

        // given and when
        let res = OpcModelConfig::check_stable(None);
        // then
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "stable is required");

        // given and when
        let res = OpcModelConfig::check_stable(Some(&"".to_string()));
        // then
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "stable is required");

        // given
        let stable = Some("t_{abc}".to_string());
        // when
        let res = OpcModelConfig::check_stable(stable.as_ref());
        // then
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid stable expression: t_{abc}"
        );
    }

    #[test]
    fn test_check_tbname() {
        let res = OpcModelConfig::check_tbname(OpcType::OPCUA, "t_{ns}_{id}");
        assert!(res.is_ok());

        let res = OpcModelConfig::check_tbname(OpcType::OPCDA, "t_{tag_name}");
        assert!(res.is_ok());

        let res = OpcModelConfig::check_tbname(OpcType::OPCDA, "t_{TagName}");
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid tbname expression: t_{TagName}"
        );

        let res = OpcModelConfig::check_tbname(OpcType::OPCUA, "t_abc");
        assert!(res.is_ok());

        let res = OpcModelConfig::check_tbname(OpcType::OPCUA, "t_{tag_name}");
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid tbname expression: t_{tag_name}"
        );

        let res = OpcModelConfig::check_tbname(OpcType::OPCUA, "");
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "tbname is required");
    }

    /// 检查 OPC csv 文件的合法性
    #[tokio::test]
    async fn test_validate_of_opc_model_config() {
        // given
        let csv = r#"point_id,stable,tbname,val_col,ts_col,tag::INT::id,tag::VARCHAR(20)::name
ns=3;i=1001,opc_{type},t_{ns}_{id},val,ts,abc,123"#
            .to_string();
        // when
        let opc_model = CsvParser::parse_csv(OpcType::OPCUA, csv).await.unwrap();
        let res = opc_model.validate();
        // then
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "tag value and type not match, point_id: ns=3;i=1001, cause: abc is not i32 type"
        );

        // given
        let csv = r#"point_id,stable,tbname,val_col,ts_col,tag::INT::id,tag::VARCHAR(20)::name
ns=3;i=1001,opc_{type},t_{ns}_{id},val,ts,123,abc"#
            .to_string();
        // when
        let opc_model = CsvParser::parse_csv(OpcType::OPCUA, csv).await.unwrap();
        let res = opc_model.validate();
        // then
        assert!(res.is_ok())
    }

    /// 检查 OPC csv 文件和 database 的 schema 是否冲突
    #[tokio::test]
    async fn test_validate_with_sink_of_opc_model_config() {
        //         // given
        //         let csv = r#"point_id,stable,tbname,val_col,ts_col,tag::INT::id,tag::VARCHAR(20)::name
        // ns=3;i=1001,opc_{type},t_{ns}_{id},val,ts,123,abc
        // "#
        //         .to_string();
        //         let sink = format!("taos:///").into_dsn().unwrap();
        //
        //         // when
        //         let model = CsvParser::parse_csv(OpcType::OPCUA, csv).await.unwrap();
        //         let res = model
        //             .validate_with_sink(ModelType::SingleColumn, &sink)
        //             .await;
        //
        //         println!("{:?}", res);
        //         // then
        //         assert!(res.is_err());
    }

    #[test]
    fn test_join_by_point_id() {
        // given
        let mut p = LinkedHashMap::new();
        p.insert(
            "ns=3;i=1001".to_string(),
            PointConfig {
                row_index: 1,
                code: "t_3_1001".to_string(),
                stable: None,
                tag_values: None,
                value_type: None,
            },
        );
        p.insert(
            "ns=3;i=1002".to_string(),
            PointConfig {
                row_index: 2,
                code: "t_3_1002".to_string(),
                stable: None,
                tag_values: Some(HashMap::from([
                    ("tag1".to_string(), "true".to_string()),
                    ("tag2".to_string(), "abc".to_string()),
                    ("tag3".to_string(), "123".to_string()),
                ])),
                value_type: None,
            },
        );
        p.insert(
            "ns=3;i=1003".to_string(),
            PointConfig {
                row_index: 3,
                code: "t_3_1003".to_string(),
                stable: None,
                tag_values: Some(HashMap::from([
                    ("tag1".to_string(), "false".to_string()),
                    ("tag2".to_string(), "abc".to_string()),
                    ("tag3".to_string(), "123".to_string()),
                ])),
                value_type: None,
            },
        );
        let mut t = LinkedHashMap::new();
        let tag_config = TableConfig {
            enabled: Some(1),
            stable_prefix: None,
            column_configs: vec![],
            tag_configs: Some(vec![
                TagConfig {
                    name: "tag1".to_string(),
                    r#type: IpcDataType::Bool,
                },
                TagConfig {
                    name: "tag2".to_string(),
                    r#type: IpcDataType::NChar(120),
                },
                TagConfig {
                    name: "tag3".to_string(),
                    r#type: IpcDataType::Int32,
                },
            ]),
        };
        t.insert("ns=3;i=1002".to_string(), tag_config.clone());
        t.insert("ns=3;i=1003".to_string(), tag_config.clone());

        // when
        let res = OpcModelConfig::join_by_point_id(p, t);

        // then
        assert_eq!(res.len(), 2);

        let (point_config, table_config) = res.get("ns=3;i=1002").unwrap();
        assert_eq!(point_config.row_index, 2);
        assert_eq!(point_config.code, "t_3_1002");
        assert_eq!(point_config.stable, None);
        assert_eq!(point_config.tag_values.as_ref().unwrap().len(), 3);
        assert_eq!(point_config.value_type, None);

        assert_eq!(table_config.enabled, Some(1));
        assert_eq!(table_config.stable_prefix, None);
        assert_eq!(table_config.column_configs.len(), 0);
        assert_eq!(table_config.tag_configs.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn test_fetch_tags() {
        // given
        let mut joined_map = LinkedHashMap::new();
        for i in 1..=3 {
            joined_map.insert(
                format!("ns=3;i=100{}", i),
                (
                    PointConfig {
                        row_index: i,
                        code: "t_{ns}_{id}".to_string(),
                        stable: Some("opc_{type}".to_string()),
                        tag_values: Some(HashMap::from([
                            ("tag1".to_string(), "true".to_string()),
                            ("tag2".to_string(), "abc".to_string()),
                            ("tag3".to_string(), "123".to_string()),
                        ])),
                        value_type: Some(IpcDataType::Int32),
                    },
                    TableConfig {
                        enabled: Some(1),
                        stable_prefix: None,
                        column_configs: vec![],
                        tag_configs: Some(vec![
                            TagConfig {
                                name: "tag1".to_string(),
                                r#type: IpcDataType::Bool,
                            },
                            TagConfig {
                                name: "tag2".to_string(),
                                r#type: IpcDataType::NChar(120),
                            },
                            TagConfig {
                                name: "tag3".to_string(),
                                r#type: IpcDataType::Int32,
                            },
                        ]),
                    },
                ),
            );
        }

        // when
        let res = OpcModelConfig::fetch_tags(joined_map);

        // then
        assert_eq!(res.len(), 3);

        let tags = res.get("ns=3;i=1002").unwrap();
        assert_eq!(tags.len(), 3);
        let (tag_name, tag_value, tag_type) = tags.first().unwrap();
        assert_eq!(tag_name, "tag1");
        assert_eq!(tag_value, "true");
        assert_eq!(tag_type, &IpcDataType::Bool);
        let (tag_name, tag_value, tag_type) = tags.get(1).unwrap();
        assert_eq!(tag_name, "tag2");
        assert_eq!(tag_value, "abc");
        assert_eq!(tag_type, &IpcDataType::NChar(120));
        let (tag_name, tag_value, tag_type) = tags.get(2).unwrap();
        assert_eq!(tag_name, "tag3");
        assert_eq!(tag_value, "123");
        assert_eq!(tag_type, &IpcDataType::Int32);

        let tags = res.get("ns=3;i=1003").unwrap();
        assert_eq!(tags.len(), 3);
        let (tag_name, tag_value, tag_type) = tags.first().unwrap();
        assert_eq!(tag_name, "tag1");
        assert_eq!(tag_value, "true");
        assert_eq!(tag_type, &IpcDataType::Bool);
        let (tag_name, tag_value, tag_type) = tags.get(1).unwrap();
        assert_eq!(tag_name, "tag2");
        assert_eq!(tag_value, "abc");
        assert_eq!(tag_type, &IpcDataType::NChar(120));
        let (tag_name, tag_value, tag_type) = tags.get(2).unwrap();
        assert_eq!(tag_name, "tag3");
        assert_eq!(tag_value, "123");
        assert_eq!(tag_type, &IpcDataType::Int32);
    }

    #[test]
    fn test_check_tag_type() {
        // bool
        assert!(OpcModelConfig::check_tag_type("true", &IpcDataType::Bool).is_ok());
        assert!(OpcModelConfig::check_tag_type("false", &IpcDataType::Bool).is_ok());
        assert!(OpcModelConfig::check_tag_type("ture", &IpcDataType::Bool).is_err());
        // u8
        assert!(OpcModelConfig::check_tag_type("1", &IpcDataType::UInt8).is_ok());
        assert!(OpcModelConfig::check_tag_type("256", &IpcDataType::UInt8).is_err());
        // u16
        assert!(OpcModelConfig::check_tag_type("1", &IpcDataType::UInt16).is_ok());
        assert!(OpcModelConfig::check_tag_type("65536", &IpcDataType::UInt16).is_err());
        // u32
        assert!(OpcModelConfig::check_tag_type("1", &IpcDataType::UInt32).is_ok());
        assert!(OpcModelConfig::check_tag_type("abc", &IpcDataType::UInt32).is_err());
        // u64
        assert!(OpcModelConfig::check_tag_type("1", &IpcDataType::UInt64).is_ok());
        assert!(OpcModelConfig::check_tag_type("abc", &IpcDataType::UInt64).is_err());
        // i8
        assert!(OpcModelConfig::check_tag_type("1", &IpcDataType::Int8).is_ok());
        assert!(OpcModelConfig::check_tag_type("3.14", &IpcDataType::Int8).is_err());
        // i16
        assert!(OpcModelConfig::check_tag_type("1", &IpcDataType::Int16).is_ok());
        assert!(OpcModelConfig::check_tag_type("3.14", &IpcDataType::Int16).is_err());
        // i32
        assert!(OpcModelConfig::check_tag_type("1", &IpcDataType::Int32).is_ok());
        assert!(OpcModelConfig::check_tag_type("3.14", &IpcDataType::Int32).is_err());
        // i64
        assert!(OpcModelConfig::check_tag_type("1", &IpcDataType::Int64).is_ok());
        assert!(OpcModelConfig::check_tag_type("3.14", &IpcDataType::Int64).is_err());
        // f32
        assert!(OpcModelConfig::check_tag_type("3.14", &IpcDataType::Float32).is_ok());
        assert!(OpcModelConfig::check_tag_type("abc", &IpcDataType::Float32).is_err());
        // f64
        assert!(OpcModelConfig::check_tag_type("3.14", &IpcDataType::Float64).is_ok());
        assert!(OpcModelConfig::check_tag_type("abc", &IpcDataType::Float64).is_err());
        // varchar(20)
        assert!(OpcModelConfig::check_tag_type("abc", &IpcDataType::VarChar(10)).is_ok());
        assert!(OpcModelConfig::check_tag_type("12345678901", &IpcDataType::VarChar(10)).is_err());
        assert!(OpcModelConfig::check_tag_type("一二三", &IpcDataType::VarChar(10)).is_ok());
        assert!(OpcModelConfig::check_tag_type("一二三四", &IpcDataType::VarChar(10)).is_err());
    }

    #[test]
    fn test_is_expr() {
        // stable
        assert!(PointConfig::is_expr(OpcType::OPCUA, "stable", "opc_{type}"));
        assert!(!PointConfig::is_expr(OpcType::OPCUA, "stable", "opc"));

        // tbname
        assert!(PointConfig::is_expr(
            OpcType::OPCUA,
            "tbname",
            "t_{ns}_{id}"
        ));
        assert!(PointConfig::is_expr(
            OpcType::OPCDA,
            "tbname",
            "t_{tag_name}"
        ));
        assert!(!PointConfig::is_expr(
            OpcType::OPCUA,
            "tbname",
            "t_{tag_name}"
        ));
        assert!(!PointConfig::is_expr(OpcType::OPCUA, "tbname", "tb123"));
    }

    #[tokio::test]
    async fn test_parse_stable() {
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["point1", "stable1"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("stable1".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["point1", ""]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, None);

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "meters_{type}"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("meters_{type}".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable", "type"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "meters_{type}", ""]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("meters_{type}".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable", "type"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "stable1_{type}", "varchar(200)"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("stable1_varchar".to_string()));
    }

    #[tokio::test]
    async fn test_parse_value_col() {
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "value_col", "value_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["123", "value", "value + 1"]);
        let value_col = TableConfig::parse_value_col(&header, &row).unwrap();
        assert_eq!(value_col.alias.unwrap(), "value");
        assert_eq!(value_col.transform.unwrap(), "value + 1");

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["point_id", "value_col", "value_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["123", "", "value + 1"]);
        let value_col = TableConfig::parse_value_col(&header, &row);
        assert!(value_col.is_err());
        assert_eq!(
            value_col.unwrap_err().to_string(),
            "invalid value_transform: value + 1, cause: Variable not found: value"
        );

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["point_id", "value_col", "value_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["123", "", "val + 1"]);
        let value_col = TableConfig::parse_value_col(&header, &row).unwrap();
        assert_eq!(value_col.alias.unwrap(), "val");
        assert_eq!(value_col.transform.unwrap(), "val + 1");
    }

    #[test]
    fn test_parse_tag_value_col() {
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec![
                "point_id",
                "value_col",
                "value_transform",
                "tag::VARCHAR(200)::id",
            ]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec![
            "ns=2;s=通道 1.设备 1.标记 1",
            "value",
            "value + 1",
            "{id}",
        ]);
        let tags = parse_tag_values(&header, &row).unwrap();
        assert!(!tags.is_empty());
        assert_eq!(tags["id"], "通道 1.设备 1.标记 1");

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec![
                "point_id",
                "value_col",
                "value_transform",
                "tag::VARCHAR(200)::id",
            ]),
        )
        .unwrap();
        let row =
            csv_async::StringRecord::from(vec!["ns=2;i=123", "value", "value + 1", "id.{ns}.{id}"]);
        let tags = parse_tag_values(&header, &row).unwrap();
        assert_eq!(tags["id"], "id.2.123");
    }

    #[test]
    fn test_parse_timestamp_col() {
        // ts_col 有值，ts_transform 有值
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["point_id", "ts_col", "ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ns=1;i=100", "ts", "ts + 1"]);
        let ts_col = TableConfig::parse_timestamp_col(
            &header,
            &row,
            CsvColumn::TS_COL,
            CsvColumn::TS_TRANSFORM,
        )
        .unwrap()
        .unwrap();
        assert_eq!(ts_col.alias.unwrap(), "ts");
        assert_eq!(ts_col.transform.unwrap(), "ts + 1");

        // ts_col 无值，ts_transform 有值
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["point_id", "ts_col", "ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ns=1;i=100", "", "ts + 1"]);
        let ts_col = TableConfig::parse_timestamp_col(
            &header,
            &row,
            CsvColumn::TS_COL,
            CsvColumn::TS_TRANSFORM,
        )
        .unwrap();
        assert!(ts_col.is_none());

        // ts_col 有值，ts_transform 是错误的表达式
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["point_id", "ts_col", "ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ns=1;i=100", "ts", "origin_ts + 1"]);
        let ts_col = TableConfig::parse_timestamp_col(
            &header,
            &row,
            CsvColumn::TS_COL,
            CsvColumn::TS_TRANSFORM,
        );
        assert!(ts_col.is_err());
        assert_eq!(
            ts_col.unwrap_err().to_string(),
            "invalid ts_transform: origin_ts + 1, cause: Variable not found: origin_ts"
        );

        // received_ts_col 有值，received_ts_transform 有值
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec![
                "point_id",
                "received_ts_col",
                "received_ts_transform",
            ]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ns=1;i=100", "rts", "rts + 1"]);
        let received_ts_col = TableConfig::parse_timestamp_col(
            &header,
            &row,
            CsvColumn::RECEIVED_TS_COL,
            CsvColumn::RECEIVED_TS_TRANSFORM,
        )
        .unwrap()
        .unwrap();
        assert_eq!(received_ts_col.alias.unwrap(), "rts");
        assert_eq!(received_ts_col.transform.unwrap(), "rts + 1");

        // received_ts_col 无值，received_ts_transform 有值
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec![
                "point_id",
                "received_ts_col",
                "received_ts_transform",
            ]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ns=1;i=100", "", "rts + 1"]);
        let received_ts_col = TableConfig::parse_timestamp_col(
            &header,
            &row,
            CsvColumn::RECEIVED_TS_COL,
            CsvColumn::RECEIVED_TS_TRANSFORM,
        )
        .unwrap();
        assert!(received_ts_col.is_none());

        // received_ts_col 有值，received_ts_transform 是错误的表达式
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec![
                "point_id",
                "received_ts_col",
                "received_ts_transform",
            ]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ns=1;i=100", "rts", "received_ts + 1"]);
        let received_ts_col = TableConfig::parse_timestamp_col(
            &header,
            &row,
            CsvColumn::RECEIVED_TS_COL,
            CsvColumn::RECEIVED_TS_TRANSFORM,
        );
        assert!(received_ts_col.is_err());
        assert_eq!(
            received_ts_col.unwrap_err().to_string(),
            "invalid received_ts_transform: received_ts + 1, cause: Variable not found: received_ts"
        );

        // request_ts_col 有值，request_ts_transform 有值
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec![
                "point_id",
                "request_ts_col",
                "request_ts_transform",
            ]),
        );
        let row = csv_async::StringRecord::from(vec!["ns=1;i=100", "qts", "qts + 1"]);
        let request_ts_col = TableConfig::parse_timestamp_col(
            &header.unwrap(),
            &row,
            CsvColumn::REQUEST_TS_COL,
            CsvColumn::REQUEST_TS_TRANSFORM,
        )
        .unwrap()
        .unwrap();
        assert_eq!(request_ts_col.alias.unwrap(), "qts");
        assert_eq!(request_ts_col.transform.unwrap(), "qts + 1");
    }
}
