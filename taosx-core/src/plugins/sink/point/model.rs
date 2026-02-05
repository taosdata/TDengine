use anyhow::{Context, bail};
use csv_async::StringRecord;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use regex::Regex;
use scc::HashSet;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::OnceLock;
use taos::{Dsn, Ty};
use taosx_ipc::prelude::IpcDataType;
use taosx_ipc::types::DataSet;

use crate::plugins::sink::point::csv::{CsvColumn, CsvHeader, CsvParser};
use crate::runners::opc::points::OpcNode;
use crate::sink::point::UpdateMode;
use crate::utils::rhai_syntax_validator::check_math_expression;
use crate::utils::table_meta::{TableMeta, TableMetaQuerier, TableMetaQueryBuilder};
use crate::utils::{parse_key_in_dsn, validate_table_column_name};

static REGEX: OnceLock<Regex> = OnceLock::new();

fn get_regex() -> &'static Regex {
    REGEX.get_or_init(|| Regex::new(r"\{([^{}]+)\}").unwrap())
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    OPCUA,
    OPCDA,
    KingHistorian,
}

impl SourceType {
    pub fn as_static_str(&self) -> &str {
        match self {
            SourceType::OPCUA => "opcua",
            SourceType::OPCDA => "opcda",
            SourceType::KingHistorian => "kinghist",
        }
    }
}

impl TryFrom<&Dsn> for SourceType {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        let driver = dsn.driver.to_lowercase();
        let protocol = dsn.protocol.as_deref();

        match (driver.as_str(), protocol) {
            ("opcua", _) => anyhow::Ok(SourceType::OPCUA), // opcua://...
            ("opcda", _) => anyhow::Ok(SourceType::OPCDA), // opcda://...
            ("kinghist", _) => anyhow::Ok(SourceType::KingHistorian), // kinghist://...
            ("opc", Some("ua")) => anyhow::Ok(SourceType::OPCUA), // opc+ua://...
            ("opc", Some("da")) => anyhow::Ok(SourceType::OPCDA), // opc+da://...
            _ => bail!("invalid source type in dsn: {}", dsn),
        }
    }
}

impl TryFrom<&str> for SourceType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "opcua" => Ok(SourceType::OPCUA),
            "opcda" => Ok(SourceType::OPCDA),
            "kinghist" => Ok(SourceType::KingHistorian),
            _ => Err(anyhow::anyhow!("invalid source type: {}", value)),
        }
    }
}

/// 点位与 TDengine 中的表的映射关系
/// point_config_map 和 table_config_map 用来处理预定义的点位，即：通过 csv 文件已经配置好的
/// generate_rule 用来处理未定义的点位，即：动态发现的点位
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PointModelConfig {
    #[serde(alias = "opc_type", alias = "sourceType", alias = "opcType")]
    pub source_type: SourceType, // point model 对应的数据源类型
    pub update_mode: Option<UpdateMode>, // OPC 点位更新模式：none/append/update
    pub generate_rule: Option<GeneratePointMappingBy>, // 生成点位映射规则的方式
    pub point_config_map: LinkedHashMap<String, PointConfig>, // key: point_id, value: PointConfig
    pub table_config_map: LinkedHashMap<String, TableConfig>, // key: point_id, value: TableConfig
    pub node_config_map: Option<LinkedHashMap<String, ObjectNodeConfig>>, // key: node_id, value: ObjectNodeConfig
}

impl PointModelConfig {
    pub fn to_create_table_sqls(&self) -> Vec<String> {
        // 创建超级表的SQL
        let mut sqls = self.to_stable_sqls();

        // 创建子表的SQL
        let sub_table_sqls = self.to_table_sqls();
        sqls.extend(sub_table_sqls);

        sqls
    }

    /// 根据配置生成超级表的建表语句：
    /// CREATE TABLE IF NOT EXISTS `{stable}` (ts timestamp, val {type}, quality int) TAGS(name {tag.type} ...)
    /// 每种 stable 仅创建一条语句。
    fn to_stable_sqls(&self) -> Vec<String> {
        let mut sqls: Vec<String> = Vec::new();

        let seen: HashSet<String> = HashSet::new();

        for (pid, pcfg) in self.point_config_map.iter() {
            // CREATE TABLE IF NOT EXISTS `{stable}` (cols...) TAGS(tags...)
            let stable = match pcfg.stable.as_deref() {
                None => {
                    tracing::warn!("point_id: {} stable is None, skipping", pid);
                    continue;
                }
                Some(stable) => {
                    if PointConfig::is_expr(self.source_type, "stable", stable) {
                        tracing::warn!("point_id: {} stable is expression, skipping", pid);
                        continue;
                    }
                    stable
                }
            };

            if seen.contains(stable) {
                continue;
            }

            let (cols, tags) = match self.table_config_map.get(pid) {
                None => {
                    tracing::warn!("point_id: {} not found in table_config_map, skipping", pid);
                    continue;
                }
                Some(cfg) => {
                    let dynamic_val = cfg.column_configs.iter().any(|c| c.r#type.is_none());
                    if dynamic_val {
                        tracing::warn!("point_id: {} has dynamic value column type, skipping", pid);
                        continue;
                    }
                    match cfg.tag_configs.as_ref() {
                        None => {
                            tracing::warn!("point_id: {} tag_configs is None, skipping", pid);
                            continue;
                        }
                        Some(tags) => (&cfg.column_configs, tags),
                    }
                }
            };

            let mut sql = format!("CREATE TABLE IF NOT EXISTS `{}`", stable);
            // 需要将主键列放在第一个位置，其余列保持原有相对顺序。
            let ordered_cols: Vec<&ColumnConfig> = {
                let mut v: Vec<&ColumnConfig> = Vec::with_capacity(cols.len());
                // push all primary key columns first (normally only one)
                cols.iter()
                    .filter(|c| c.is_primary_key)
                    .for_each(|c| v.push(c));
                // then push the rest
                cols.iter()
                    .filter(|c| !c.is_primary_key)
                    .for_each(|c| v.push(c));
                v
            };
            let col_clause = ordered_cols
                .into_iter()
                .map(|col_cfg| {
                    let col_name = col_cfg.alias.as_deref().unwrap_or(col_cfg.name.as_str());
                    let col_type = col_cfg.r#type.as_ref().unwrap();
                    let col_type = if col_type.is_var_type() {
                        format!("{}(128)", col_type.name())
                    } else {
                        col_type.name().to_string()
                    };
                    format!("`{}` {}", col_name, col_type)
                })
                .collect::<Vec<String>>()
                .join(",");
            sql.push_str(&format!(" ({})", col_clause));

            let tag_clause = tags
                .iter()
                .map(|tag_cfg| {
                    let tag_name = tag_cfg.name.as_str();
                    let tag_type = tag_cfg.r#type.sql_repr();
                    format!("`{}` {}", tag_name, tag_type)
                })
                .collect::<Vec<String>>()
                .join(",");

            sql.push_str(&format!(" TAGS({})", tag_clause));

            sqls.push(sql);

            let _ = seen.insert(stable.to_string());
        }

        tracing::info!("generate {} stable create sqls", sqls.len());
        sqls
    }

    // TDengine supports batch create of multiple child tables in one SQL:
    // ```SQL
    // CREATE TABLE
    //      IF NOT EXISTS tb1 USING stb (tag_names...) TAGS(tag_values...)
    //      IF NOT EXISTS tb2 USING stb (tag_names...) TAGS(tag_values...)
    //      ...
    // ```
    // We will aggregate as many table segments as possible into a single SQL
    // without exceeding the maximum SQL length (1 MiB = 1024 * 1024 chars).
    fn to_table_sqls(&self) -> Vec<String> {
        const MAX_SQL_LEN: usize = 1024 * 1024;

        let mut result: Vec<String> = Vec::new();
        let mut current: String = String::new();
        let mut has_segment_in_current = false;
        // 统计成功生成的子表段（即有效点位）的数量
        let mut point_count: usize = 0;
        // 统计被跳过的点位数量
        let mut skipped_count: usize = 0;

        for (pid, pcfg) in self.point_config_map.iter() {
            let tbname = pcfg.code.as_str();

            // Preconditions: stable must exist and not be an expression; tag values must exist; table config must exist with tag configs.
            let stable = match pcfg.stable.as_deref() {
                None => {
                    skipped_count += 1;
                    tracing::warn!("point_id: {} stable is None, skipping", pid);
                    continue;
                }
                Some(stable) => {
                    if PointConfig::is_expr(self.source_type, "stable", stable) {
                        skipped_count += 1;
                        tracing::warn!("point_id: {} stable is expression, skipping", pid);
                        continue;
                    }
                    stable
                }
            };

            let tag_values = match pcfg.tag_values.as_ref() {
                None => {
                    skipped_count += 1;
                    tracing::warn!("point_id: {} tag_values is None, skipping", pid);
                    continue;
                }
                Some(values) => values,
            };
            let tcfg = match self.table_config_map.get(pid) {
                None => {
                    skipped_count += 1;
                    tracing::warn!("point_id: {} not found in table_config_map, skipping", pid);
                    continue;
                }
                Some(cfg) => cfg,
            };
            let tags = match tcfg.tag_configs.as_ref() {
                None => {
                    skipped_count += 1;
                    tracing::warn!("point_id: {} tag_configs is None, skipping", pid);
                    continue;
                }
                Some(t) => t,
            };

            let tag_names = tags
                .iter()
                .map(|t| format!("`{}`", t.name.as_str()))
                .collect::<Vec<String>>()
                .join(",");
            let tag_vals = tags
                .iter()
                .map(|t| {
                    let val = tag_values.get(&t.name).map(|s| s.as_str()).unwrap_or("");
                    match t.r#type {
                        IpcDataType::NChar(_)
                        | IpcDataType::VarChar(_)
                        | IpcDataType::VarBinary(_) => format!("'{}'", val),
                        _ => val.to_string(),
                    }
                })
                .collect::<Vec<String>>()
                .join(",");

            let segment = format!(
                "IF NOT EXISTS `{}` USING `{}` ({}) TAGS({})",
                tbname, stable, tag_names, tag_vals
            );

            // 记录一次有效点位
            point_count += 1;

            if !has_segment_in_current {
                current = format!("CREATE TABLE {}", segment);
                has_segment_in_current = true;
                continue;
            }

            // If appending this segment would exceed the maximum length, flush current and start new.
            if current.len() + 1 + segment.len() > MAX_SQL_LEN {
                result.push(current.clone());
                current = format!("CREATE TABLE {}", segment);
            } else {
                current.push(' ');
                current.push_str(&segment);
            }
        }

        if has_segment_in_current && !current.is_empty() {
            result.push(current);
        }

        // 在最后打印统计信息：点位计数、被跳过的点位数量和 SQL 条数
        tracing::info!(
            "generate {} child table create sqls, total points: {}, skipped points: {}",
            result.len(),
            point_count,
            skipped_count
        );
        result
    }

    /// 检查 csv 文件的合法性
    pub fn validate(&self) -> anyhow::Result<()> {
        tracing::info!(
            "validate model config, source_type: {:?}, generate_rule: {:?}",
            self.source_type,
            self.generate_rule
        );

        // 1) per-point checks: stable/tbname sanity
        for (point_id, point_config) in self.point_config_map.iter() {
            // 检查 stable
            let stable = point_config.stable.as_ref();
            PointModelConfig::check_stable(stable).map_err(|err| {
                anyhow::anyhow!("invalid stable of point_id: {}, cause: {}", point_id, err)
            })?;

            // 检查 tbname
            let tbname = point_config.code.as_str();
            PointModelConfig::check_tbname(self.source_type, tbname).map_err(|err| {
                anyhow::anyhow!("invalid tbname of point_id: {}, cause: {}", point_id, err)
            })?;
        }

        // 2) per-table checks: ensure a primary timestamp exists
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

        // 3) tag checks: avoid cloning large maps by iterating references directly
        for (point_id, point_config) in self.point_config_map.iter() {
            let Some(table_config) = self.table_config_map.get(point_id) else {
                continue;
            };
            let (Some(tag_values), Some(tag_configs)) = (
                point_config.tag_values.as_ref(),
                table_config.tag_configs.as_ref(),
            ) else {
                continue;
            };

            // Iterate configs and look up values by name; no intermediate allocations
            for tag_cfg in tag_configs {
                if let Some(tag_val) = tag_values.get(&tag_cfg.name) {
                    Self::check_tag_type(tag_val.as_str(), &tag_cfg.r#type).map_err(|err| {
                        anyhow::anyhow!(
                            "tag value and type not match, point_id: {}, cause: {}",
                            point_id,
                            err
                        )
                    })?;
                }
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
                let regex = get_regex();
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

    /// 校验 tbname 配置是否合法, tbname 为任意字符串
    /// 如果存在 {}，则{}中间的 string 可以为 generate_tbname_from_pattern 允许的表达式
    fn check_tbname(opc_type: SourceType, tbname: &str) -> anyhow::Result<()> {
        if tbname.is_empty() {
            bail!("tbname is required");
        }
        if !tbname.contains("{") {
            return Ok(());
        }

        let regex = get_regex();
        for cap in regex.captures_iter(tbname) {
            let cap_str = cap.get(1).unwrap().as_str();
            match opc_type {
                SourceType::OPCUA => {
                    if !cap_str.contains("ns") && !cap_str.contains("id") {
                        bail!("invalid tbname expression: {}", tbname);
                    }
                }
                SourceType::OPCDA | SourceType::KingHistorian => {
                    if !cap_str.contains("tag_name") {
                        bail!("invalid tbname expression: {}", tbname);
                    }
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
                PointConfig::is_expr(self.source_type, "stable", stable.as_str()),
                PointConfig::is_expr(self.source_type, "tbname", tbname.as_str()),
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
            if let Some(col_name) = col_name
                && !querier.is_stable_col_exist(stable_meta.tbname.as_str(), col_name)?
            {
                bail!(
                    "column: {} not exist in table: {}, point_id: {}",
                    col,
                    stable_meta.tbname.as_str(),
                    point_id
                );
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
                PointConfig::is_expr(self.source_type, "stable", stable.as_str()),
                PointConfig::is_expr(self.source_type, "tbname", tbname.as_str()),
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

    // 根据 point_id 和 value_type 生成点位映射关系
    // 任务运行中，发现点位不在 model 的 point_config_map 和 table_config_map 中时，动态生成点位映射关系
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
                    None => CsvParser::try_new(self.source_type, csv_files.clone())?,
                    Some(csv_origin) => {
                        CsvParser::try_new(self.source_type, vec![format!("@{}", csv_origin)])?
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

    pub fn need_transform(&self) -> bool {
        match &self.generate_rule {
            None => false,
            Some(GeneratePointMappingBy::Rule(rule)) => {
                // TODO: 目前仅支持 value_transform，后续要扩展 timestamp transform
                rule.value_transform.is_some()
            }
            Some(GeneratePointMappingBy::Csv((_csv, _csv_origin))) => true,
        }
    }

    pub async fn transform_map(
        &self,
        columns: &[&str],
    ) -> anyhow::Result<HashMap<String, HashMap<String, ColumnConfig>>> {
        // 如果 update_mode 为 None 或 update_mode=none，优先使用内存中的 table_config_map 构建 transform_map
        if matches!(self.update_mode, None | Some(UpdateMode::None)) {
            let mut result: HashMap<String, HashMap<String, ColumnConfig>> = HashMap::new();

            for &col in columns {
                let mut per_point: HashMap<String, ColumnConfig> = HashMap::new();
                for (point_id, table_cfg) in &self.table_config_map {
                    if let Some(col_cfg) = table_cfg.column_config(col) {
                        per_point.insert(point_id.clone(), col_cfg.clone());
                    }
                }
                if !per_point.is_empty() {
                    result.insert(col.to_string(), per_point);
                }
            }
            tracing::debug!("get transform map from table_config_map");
            return Ok(result);
        }

        // table_config_map 为空：需要从 CSV 中解析（动态点位更新或首次加载）。
        match &self.generate_rule {
            None => Ok(HashMap::new()),
            Some(GeneratePointMappingBy::Rule(_rule)) => {
                tracing::warn!(
                    "generate transform map by GeneratePointMappingBy::Rule is not supported"
                );
                Ok(HashMap::new())
            }
            Some(GeneratePointMappingBy::Csv((csv, csv_origin))) => match csv_origin {
                None => {
                    tracing::debug!("get transform map from csv files");
                    let rdr = CsvParser::open_csv_many(csv.clone()).await?;
                    CsvParser::parse_transform_map(self.source_type, rdr, columns).await
                }
                Some(csv_origin) => {
                    tracing::debug!("get transform map from csv origin");
                    let rdr = CsvParser::open_csv_many(vec![format!("@{}", csv_origin)]).await?;
                    CsvParser::parse_transform_map(self.source_type, rdr, columns).await
                }
            },
        }
    }

    pub fn get_column_config_map_by_name<'a>(
        &'a self,
        col_name: &str,
    ) -> HashMap<String, &'a ColumnConfig> {
        let mut column_config_map: HashMap<String, &ColumnConfig> = HashMap::new();

        for (point_id, table_config) in &self.table_config_map {
            if let Some(column_config) = table_config.column_config(col_name) {
                // 仅保存引用，避免在热路径上克隆整个 ColumnConfig
                column_config_map.insert(point_id.clone(), column_config);
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

        if let Some(stable) = stable
            && stable.contains("{type}")
        {
            return Ok(());
        }
        if tbname.contains("{id}") || tbname.contains("{ns}") || tbname.contains("{tag_name}") {
            return Ok(());
        }

        let value_col = table_config
            .column_config(ColumnConfig::VALUE)
            .and_then(|v| v.alias.as_ref());

        // 遍历 self.point_config_map 和 self.table_config_map，当 stable 和 tbname 时，value_col 应该不同，否则报错
        for (id, p_config) in point_config_map {
            if let Some(t_config) = table_config_map.get(id)
                && p_config.stable.as_ref() == stable
                && p_config.code.as_str() == tbname
                && let Some(v_col) = t_config.column_config(ColumnConfig::VALUE)
                && v_col.alias.as_ref() == value_col
            {
                bail!(
                    "point_id: {} and point_id: {} have same stable: {} and tbname: {}, value_col should be different",
                    id,
                    point_id,
                    stable.unwrap(),
                    tbname,
                );
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
    pub fn is_expr(opc_type: SourceType, col_name: &str, col_value: &str) -> bool {
        match col_name {
            "stable" => col_value.contains("{type}"),
            "tbname" => match opc_type {
                SourceType::OPCUA => col_value.contains("{id}") || col_value.contains("{ns}"),
                SourceType::OPCDA | SourceType::KingHistorian => col_value.contains("{tag_name}"),
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
        if let Some(tag_values) = &tag_values {
            for tag_name in tag_values.keys() {
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

    /// 更高效的解析：调用者已经解析过 point_id，避免在 `parse_tbname` 和 `parse_tag_values` 中重复解析 point_id。
    pub fn from_csv_with_point_id(
        header: &CsvHeader,
        row: &StringRecord,
        row_index: usize,
        point_id: &str,
    ) -> anyhow::Result<Self> {
        // tbname 可能依赖 point_id 模板，直接内联展开逻辑，避免再次 parse_point_id
        let tb_col = header
            .get_column("tbname")
            .ok_or(anyhow::anyhow!("tbname not exist in csv header"))?;
        let raw_tbname = row
            .get(tb_col.index)
            .ok_or(anyhow::anyhow!("tbname not exist in csv row"))?;
        if raw_tbname.is_empty() {
            anyhow::bail!("tbname cannot be empty");
        }
        let code = if raw_tbname.contains('{') {
            generate_tbname_from_pattern(
                header.get_source_type().as_static_str(),
                raw_tbname,
                point_id,
            )
        } else {
            raw_tbname.to_string()
        };
        validate_table_column_name("table name", &code)?;
        if code.is_empty() {
            anyhow::bail!("tbname cannot be empty");
        }

        let value_type = parse_type(header, row)?;
        let stable = parse_stable(header, row);
        if let Some(st) = &stable {
            validate_table_column_name("stable name", st)?;
        }

        // 高效 tag 解析：复用 point_id
        let tag_values = parse_tag_values_fast(header, row, point_id);
        if let Some(tv) = &tag_values {
            for tag_name in tv.keys() {
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

/// 解析 csv 中的 stable 列，如果 stable 包含 {type}，则替换为 type 列的值
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

/*
支持的替换占位符如下：
* 当 ty = "opcua"（OPC UA）
    {ns}：替换为 point_id 中的命名空间部分（ns 的值）。例如 point_id="ns=6;s=Foo.Bar" 时，{ns} → "6"。如果 point_id 不含分号（没法拆出 ns），则 {ns} → "", {id} -> "Objects"。
    {id}：替换为 point_id 中 id 的值（去掉 "i=" / "s=" / "g=" / "b=" 等前缀后的实际值）。例如 "ns=6;s=Foo.Bar" 时，{id} → "Foo.Bar"。
    {id#/_}: 将 id 中的所有 '/' 变为 '_'，并去掉开头和结尾可能出现的 '_'（例如 "/Device/Type/TagName/" → "Device_Type_TagName"）。
    {id#-_}: 将 id 中的所有 '-' 变为 '_'，并去掉开头和结尾可能出现的 '_'（例如 "Device-Type-TagName" → "Device_Type_TagName"）。
* 当 ty = "opcda" 或 "kinghist"（OPC DA 或 KingHistorian）
    {tag_name} 或 {TagName}：替换为 point_id 中最后一个 . 之后的部分作为 TagName。例如 point_id="Device.DeviceType.TagName" 时，{tag_name} 或 {TagName} → "TagName"。
    {/tag_name}：替换为 point_id 中最后一个 / 之后的部分作为 TagName。例如 point_id="Device/DeviceType/TagName" 时，{/tag_name} → "TagName"。
    {id}：替换为 point_id 的完整值。例如： point_id="Device.DeviceType.TagName" 时，{id} → "Device.DeviceType.TagName"。
    {_id}：替换为 point_id 中的 / 替换为 _ 之后的值。和 {id#/_} 是等价的，为了兼容历史配置。
    {id#/_}: 将 id 中的所有 '/' 变为 '_'，并去掉开头和结尾可能出现的 '_'（例如 "/Device/Type/TagName/" → "Device_Type_TagName"）。
    {id#-_}: 将 id 中的所有 '-' 变为 '_'，并去掉开头和结尾可能出现的 '_'（例如 "Device-Type-TagName" → "Device_Type_TagName"）。
* 其他 ty 类型，直接返回 tb_name 原始值。
*/
/// OPC UA: <table_prefix>_{ns}_{id}_<table_suffix>
/// OPC DA: <table_prefix>_{tag_name/TagName}_<table_suffix>
pub fn generate_tbname_from_pattern(ty: &str, tb_name: &str, point_id: &str) -> String {
    let tbname = match ty {
        "opcua" => {
            // ns=13;i=1003
            // ns=6;s=Scalar_Instructions
            // ns=6;g=00000000-0000-0000-0000-000000009204
            // ns=6;b=CQIABQ==
            if let Some((ns, id)) = point_id.split_once(";") {
                let ns = if let Some((_, ns)) = ns.split_once('=') {
                    ns
                } else {
                    ns
                };
                let id = if let Some((_, id)) = id.split_once('=') {
                    id
                } else {
                    id
                };
                assert!(!id.is_empty(), "id should not be empty: {}", point_id);
                // helpers for normalized replacements with trimming
                let trim_chars = |s: &str, ch: char| -> String {
                    let s = s.trim_start_matches(ch);
                    s.trim_end_matches(ch).to_string()
                };
                let id_slash_to_underscore = trim_chars(&id.replace('/', "_"), '_');
                let id_dash_to_underscore = trim_chars(&id.replace('-', "_"), '_');

                tb_name
                    .replace("{ns}", ns)
                    .replace("{id}", id)
                    .replace("{id#/_}", &id_slash_to_underscore)
                    .replace("{id#-_}", &id_dash_to_underscore)
            } else {
                assert!(!point_id.is_empty(), "id should not be empty: {}", point_id);
                tb_name
                    .replace("{ns}_{id}", "Objects")
                    .replace("{ns}-{id}", "Objects")
                    .replace("{ns}", "")
                    .replace("{id}", "Objects")
            }
        }
        "opcda" | "kinghist" => {
            // tag_name 等于提取点位 ID 中最后一个 . 之后的部分。例如：point_id="Device.DeviceType.TagName"，则 {tag_name} 或 {TagName} → "TagName"
            let tag_name = point_id
                .rfind(".")
                .map(|idx| &point_id[idx + 1..])
                .unwrap_or(point_id);
            let tag_name_slash = point_id
                .rfind("/")
                .map(|idx| &point_id[idx + 1..])
                .unwrap_or(point_id);
            // 统一处理 id 的替换规则，带修剪
            let trim_chars = |s: &str, ch: char| -> String {
                let s = s.trim_start_matches(ch);
                s.trim_end_matches(ch).to_string()
            };
            let id_slash_to_underscore = trim_chars(&point_id.replace('/', "_"), '_');
            let id_dash_to_underscore = trim_chars(&point_id.replace('-', "_"), '_');

            tb_name
                .replace("{TagName}", tag_name)
                .replace("{tag_name}", tag_name)
                .replace("{/tag_name}", tag_name_slash)
                .replace("{id}", point_id)
                .replace("{_id}", &id_slash_to_underscore)
                .replace("{id#/_}", &id_slash_to_underscore)
                .replace("{id#-_}", &id_dash_to_underscore)
        }
        _ => tb_name.to_string(),
    };

    // 将 tbname 中的 . 和 ` 替换为 _
    tbname.replace(".", "_").replace("`", "_")
}

pub fn generate_stable_from_pattern(stable_expr: &str, value_type: &Option<IpcDataType>) -> String {
    let mut stable = stable_expr.to_string();
    if stable_expr.contains(".") {
        stable = stable.replace(".", "_");
    }

    if let Some(t) = value_type {
        stable = match t {
            IpcDataType::VarChar(_len) => stable.replace("{type}", "varchar"),
            IpcDataType::NChar(_len) => stable.replace("{type}", "nchar"),
            _ => stable.replace("{type}", &t.sql_repr().replace(" ", "_")),
        };
    }

    stable
}

/*
支持的替换占位符如下：
* 当 ty = "opcua"（OPC UA）
    {ns}：替换为 point_id 中的命名空间部分（ns 的值）。例如 point_id="ns=6;s=Foo.Bar" 时，{ns} → "6"。如果 point_id 不含分号；形式（没法拆出 ns），则 {ns} → "", {id} → "Objects"。
    {id}：替换为 point_id 中 id 的值（去掉 "i=" / "s=" / "g=" / "b=" 等前缀后的实际值）。例如 "ns=6;s=Foo.Bar" 时，{id} → "Foo.Bar"。
    {id.}：替换为 id 去掉最后一个点号及其后缀的前缀部分（相当于取最后一个 '.' 之前的部分）。例如 "Foo.Bar.Baz" → "Foo.Bar"，"Foo.Bar" → "Foo"。
    {id/}：替换为 id 去掉最后一个斜杠及其后缀的前缀部分（取最后一个 '/' 之前的部分）。
    {id_}：替换为 id 去掉最后一个下划线及其后缀的前缀部分（取最后一个 '_' 之前的部分）。
    {id..}：替换为 id 去掉最后两个点号段的前缀部分（取倒数第二个 '.' 之前的整段前缀）。例如 "A.B.C.D" → "A.B"。
    {..id.}：替换为 id 被 '.' 分割后的倒数第二段（“倒数第二个片段”）。例如 "A.B.C" → "B"，"Foo.Bar" → "Foo"。
    {id#/.}：将 id 中的所有 '/' 变为 '.'，并去掉开头和结尾可能出现的 '.'（例如 "/Device/Type/TagName/" → "Device.Type.TagName"）。
    {id#-.}: 将 id 中的所有 '-' 变为 '.'，并去掉开头和结尾可能出现的 '.'（例如 "Device-Type-TagName" → "Device.Type.TagName"）。
    {id#/_}: 将 id 中的所有 '/' 变为 '_'，并去掉开头和结尾可能出现的 '_'（例如 "/Device/Type/TagName/" → "Device_Type_TagName"）。
    {id#-_}: 将 id 中的所有 '-' 变为 '_'，并去掉开头和结尾可能出现的 '_'（例如 "Device-Type-TagName" → "Device_Type_TagName"）。
    {id/#/.}: 先执行 {id/}，再将结果中的所有 '/' 变为 '.'。 例如： "Device/Type/TagName" → "Device.Type"。
    {id_#_.}: 先执行 {id_}，再将结果中的所有 '_' 变为 '.'。例如： "Device_Type_TagName" → "Device.Type"。
    说明：当 point_id 不包含分号（无法拆出 ns、id）时，使用 {ns}="0"，{id}=point_id，并对上述 {id.}/{id/}/{id_}/{id..}/{..id.} 规则同样基于 point_id 进行计算。
* 当 ty = "opcda" 或 "kinghist"（OPC DA / KingHistorian）
    {TagName} 或 {tag_name}：替换为 point_id 中最后一个 '.' 之后的片段（末段）。例如 "Device.DeviceType.TagName" → "TagName"。
    {/tag_name}：替换为 point_id 中最后一个 '/' 之后的片段。例如 "Device/DeviceType/TagName" → "TagName"。
    {id}：替换为完整的 point_id。例如： "Device.DeviceType.TagName" → "Device.DeviceType.TagName"。
    {_id}：将 point_id 中的 '/' 替换为 '_' 后。例如： "Device/DeviceType.TagName" → "Device_DeviceType.TagName"。
    {id#/.}：将 id 中的所有 '/' 变为 '.'，并去掉开头和结尾可能出现的 '.'。（例如 "/Device/Type/TagName/" → "Device.Type.TagName"）。
    {id#-.}: 将 id 中的所有 '-' 变为 '.'，并去掉开头和结尾可能出现的 '.'（例如 "Device-Type-TagName" → "Device.Type.TagName"）。
    {id#/_}: 将 id 中的所有 '/' 变为 '_'，并去掉开头和结尾可能出现的 '_'（例如 "/Device/Type/TagName/" → "Device_Type_TagName"）。
    {id#-_}: 将 id 中的所有 '-' 变为 '_'，并去掉开头和结尾可能出现的 '_'（例如 "Device-Type-TagName" → "Device_Type_TagName"）。
* 其他情况
    如果模板中不包含上述任何受支持的占位符，则原样返回 template。
*/
/// OPC UA: {ns} {id}
/// OPC DA: {tag_name/TagName}
pub fn generate_tag_value_from_pattern(ty: &str, template: &str, point_id: &str) -> String {
    match ty {
        "opcua" => {
            // ns=13;i=1003
            // ns=6;s=Scalar_Instructions
            // ns=6;g=00000000-0000-0000-0000-000000009204
            // ns=6;b=CQIABQ==
            if let Some((ns, id)) = point_id.split_once(";") {
                let ns = if let Some((_, ns)) = ns.split_once('=') {
                    ns
                } else {
                    ns
                };
                let id = if let Some((_, id)) = id.split_once('=') {
                    id
                } else {
                    id
                };
                assert!(!id.is_empty(), "id should not be empty: {}", point_id);
                // cache trimmed/sliced versions of id to avoid repeated scans
                let id_trim_dot = id.rsplit_once('.').map_or(id, |(left, _)| left);
                let id_trim_slash = id.rsplit_once('/').map_or(id, |(left, _)| left);
                let id_trim_underscore = id.rsplit_once('_').map_or(id, |(left, _)| left);
                let id_trim_two_dots = id
                    .rsplit_once('.')
                    .map(|(prefix, _)| prefix.rsplit_once('.').map_or(prefix, |(left, _)| left))
                    .unwrap_or(id);
                let id_suffix_two = id
                    .rsplit('.')
                    .tuples()
                    .next()
                    .map_or(id, |(_, suffix)| suffix);
                // normalized transforms with trimming
                let trim_chars = |s: &str, ch: char| -> String {
                    let s = s.trim_start_matches(ch);
                    s.trim_end_matches(ch).to_string()
                };
                let id_slash_to_dot_trim = trim_chars(&id.replace('/', "."), '.');
                let id_dash_to_dot_trim = trim_chars(&id.replace('-', "."), '.');
                let id_slash_to_underscore_trim = trim_chars(&id.replace('/', "_"), '_');
                let id_dash_to_underscore_trim = trim_chars(&id.replace('-', "_"), '_');

                template
                    .replace("{ns}", ns)
                    .replace("{id}", id) // original id
                    .replace("{id.}", id_trim_dot)
                    .replace("{id/}", id_trim_slash)
                    .replace("{id_}", id_trim_underscore)
                    .replace("{id..}", id_trim_two_dots)
                    .replace("{..id.}", id_suffix_two)
                    .replace("{id#/.}", &id_slash_to_dot_trim)
                    .replace("{id#-.}", &id_dash_to_dot_trim)
                    .replace("{id#/_}", &id_slash_to_underscore_trim)
                    .replace("{id#-_}", &id_dash_to_underscore_trim)
                    .replace("{id/#/.}", &id_trim_slash.replace('/', "."))
                    .replace("{id_#_.}", &id_trim_underscore.replace('_', "."))
            } else {
                assert!(!point_id.is_empty(), "id should not be empty: {}", point_id);
                template
                    .replace("{ns}_{id}", "Objects")
                    .replace("{ns}_{id#/.}", "Objects")
                    .replace("{ns}_{id#/_}", "Objects")
                    .replace("{ns}", "")
                    .replace("{id}", "Objects")
            }
        }
        "opcda" | "kinghist" => {
            // derive segments once
            let dot_tag = point_id
                .rfind('.')
                .map(|idx| &point_id[idx + 1..])
                .unwrap_or(point_id);
            let slash_tag = point_id
                .rfind('/')
                .map(|idx| &point_id[idx + 1..])
                .unwrap_or(point_id);
            // normalized transforms with trimming
            let trim_chars = |s: &str, ch: char| -> String {
                let s = s.trim_start_matches(ch);
                s.trim_end_matches(ch).to_string()
            };
            let pid_slash_to_dot_trim = trim_chars(&point_id.replace('/', "."), '.');
            let pid_dash_to_dot_trim = trim_chars(&point_id.replace('-', "."), '.');
            let pid_slash_to_underscore_trim = trim_chars(&point_id.replace('/', "_"), '_');
            let pid_dash_to_underscore_trim = trim_chars(&point_id.replace('-', "_"), '_');

            template
                .replace("{TagName}", dot_tag)
                .replace("{tag_name}", dot_tag)
                .replace("{/tag_name}", slash_tag)
                .replace("{id}", point_id)
                .replace("{_id}", &point_id.replace('/', "_"))
                .replace("{id#/.}", &pid_slash_to_dot_trim)
                .replace("{id#-.}", &pid_dash_to_dot_trim)
                .replace("{id#/_}", &pid_slash_to_underscore_trim)
                .replace("{id#-_}", &pid_dash_to_underscore_trim)
        }
        _ => template.to_string(),
    }
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

    for col in header.columns() {
        if !col.is_tag {
            continue;
        }
        let tag_name = col.name.clone();
        let tag_value = row.get(col.index).unwrap_or("").to_string();
        let tag_value = if tag_value.contains("{") {
            // replace {tag_name} or {TagName} in tbname
            let source_type = header.get_source_type();
            generate_tag_value_from_pattern(source_type.as_static_str(), &tag_value, &point_id)
        } else {
            tag_value
        };
        map.insert(tag_name, tag_value);
    }

    if map.is_empty() { None } else { Some(map) }
}

/// 更高效的 tag_value 解析，复用已经获取的 point_id，避免再次调用 CsvParser::parse_point_id。
fn parse_tag_values_fast(
    header: &CsvHeader,
    row: &StringRecord,
    point_id: &str,
) -> Option<HashMap<String, String>> {
    let mut map = HashMap::new();
    for col in header.columns() {
        if !col.is_tag {
            continue;
        }
        let tag_name = col.name.clone();
        let tag_value = row.get(col.index).unwrap_or("").to_string();
        let tag_value = if tag_value.contains('{') {
            generate_tag_value_from_pattern(
                header.get_source_type().as_static_str(),
                &tag_value,
                point_id,
            )
        } else {
            tag_value
        };
        let tag_value = if matches!(col.tag_type, Some(IpcDataType::Timestamp(_))) {
            // 如果是数字串（epoch），直接返回；
            // 如果是 RFC3339 字符串，解析为毫秒级 i64；
            // 否则，原样返回并记录一次 warn 方便排查。
            if tag_value.is_empty() || tag_value.parse::<i64>().is_ok() {
                tag_value
            } else {
                match chrono::DateTime::parse_from_rfc3339(&tag_value) {
                    Ok(dt) => dt.timestamp_millis().to_string(),
                    Err(_e) => {
                        tracing::warn!(
                            "invalid timestamp tag value (expect digits or rfc3339): {} for tag {}",
                            tag_value,
                            tag_name
                        );
                        tag_value
                    }
                }
            }
        } else {
            tag_value
        };

        map.insert(tag_name, tag_value);
    }
    if map.is_empty() { None } else { Some(map) }
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TableConfig {
    pub enabled: Option<i8>, // enabled: 1 / 0
    pub stable_prefix: Option<String>,
    pub column_configs: Vec<ColumnConfig>, // column: original_ts / received_ts / value / quality
    pub tag_configs: Option<Vec<TagConfig>>, // tags(name, type) in csv header
}

impl TableConfig {
    pub fn from_csv(
        header: &CsvHeader,
        row: &StringRecord,
        source_type: SourceType,
    ) -> anyhow::Result<Self> {
        let stable = parse_stable(header, row);
        let stable_prefix = match stable {
            None => match source_type {
                SourceType::OPCUA | SourceType::OPCDA => Some(String::from("opc")),
                SourceType::KingHistorian => Some("kinghist".to_string()),
            },
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

        for col in header.columns() {
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

        for col in header.columns() {
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
                    anyhow::anyhow!("invalid value_transform: {}, cause: {}", value_transform, e)
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

    /// 从 dsn 中解析参数 stable_expression 参数：超级表名的表达式。
    /// “选择数据点位”时，super_table_expression 参数是必须的
    pub fn parse_stable_expression(dsn: &Dsn, default_prefix: &str) -> anyhow::Result<String> {
        let stable_expression = dsn
            .params
            .get("super_table_expression")
            .map(|v| {
                if v.is_empty() {
                    format!("{default_prefix}_{{type}}")
                } else {
                    v.to_string()
                }
            })
            .unwrap_or(format!("{default_prefix}_{{type}}"));

        // TODO: validate stable_expression
        Ok(stable_expression)
    }

    /// 从 dsn 中解析 child_table_expression 参数：子表名的表达式。
    /// "选择数据点位"时，child_table_expression 参数是必须的
    pub fn parse_tbname_expression(dsn: &Dsn) -> anyhow::Result<String> {
        let expr = dsn
            .params
            .get("child_table_expression")
            .ok_or(anyhow::anyhow!("child_table_expression is required"))?;

        if expr.is_empty() {
            bail!("child_table_expression cannot be empty");
        }
        let tbname_expression = expr.to_string();

        // TODO: validate tbname_expression
        Ok(tbname_expression)
    }

    /// 从 dsn 中解析 table_primary_key 参数：主键列。
    /// "选择数据点位"时，table_primary_key 参数指定主键列，只能是 original_ts/request_ts/received_ts。
    pub fn parse_primary_key(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        dsn.params.get("table_primary_key").map_or(Ok(None), |v| {
            if v.is_empty() {
                return Ok(None);
            }
            match v.as_str() {
                ColumnConfig::ORIGINAL_TS
                | ColumnConfig::REQUEST_TS
                | ColumnConfig::RECEIVED_TS => Ok(Some(v.to_string())),
                _ => {
                    bail!(
                        "invalid table_primary_key: {}, must be {} or {} or {}",
                        v,
                        ColumnConfig::ORIGINAL_TS,
                        ColumnConfig::REQUEST_TS,
                        ColumnConfig::RECEIVED_TS
                    );
                }
            }
        })
    }

    /// 从 dsn 中解析 table_primary_key_alias 参数：主键列名。
    /// "选择数据点位"时，table_primary_key_alias 参数指定主键的 name。
    pub fn parse_primary_key_alias(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        Ok(dsn.params.get("table_primary_key_alias").and_then(|v| {
            if v.is_empty() {
                return None;
            }
            let primary_key_alias = v.to_string();
            validate_table_column_name("primary_key", &primary_key_alias).ok()?;
            Some(primary_key_alias)
        }))
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
    Rule(PointMappingRule),
    /// 通过csv文件中的配置生成点位映射规则
    Csv((Vec<String>, Option<String>)),
}

/// 点位映射规则
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PointMappingRule {
    #[serde(alias = "opc_type", alias = "sourceType", alias = "opcType")]
    pub source_type: SourceType,
    pub stable_expression: String,           // 超级表名的表达式
    pub tbname_expression: String,           // 字表名的表达式
    pub value_col: String,                   // 值列名
    pub value_transform: Option<String>,     // 值列的转换表达式
    pub primary_key: String,                 // 主键
    pub primary_key_alias: String,           // 主键的别名
    pub custom_tags: Option<Vec<CustomTag>>, // 自定义标签
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustomTag {
    pub name: String,           // 标签名
    pub data_type: IpcDataType, // 标签的数据类型
    pub pattern: String,        // 标签值的表达式
}

impl CustomTag {
    // 可以配置多个自定义标签，以";"分隔。每个自定义标签的格式为：<TagType>::<TagName>::<TagPattern>，以"::"做分隔符。
    // 第一项是 Tag 的数据类型，第二项是 Tag 的名称，第三项是 Tag 值的表达式。
    pub fn try_from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Vec<CustomTag>>> {
        if let Some(tags_str) = dsn.params.get("custom_tags") {
            if tags_str.is_empty() {
                return Ok(None);
            }
            let mut custom_tags = Vec::new();
            let tags: Vec<&str> = tags_str
                .split(';')
                .filter(|tag| !tag.trim().is_empty())
                .collect();
            for tag in tags {
                let parts: Vec<&str> = tag.split("::").collect();
                if parts.len() != 3 {
                    bail!("invalid custom_tag format: {}", tag);
                }
                let data_type = IpcDataType::from_str(parts[0]).map_err(|_err| {
                    anyhow::anyhow!("invalid custom_tag data type: {}", parts[0])
                })?;
                let name = parts[1].to_string();
                let pattern = parts[2].to_string();
                custom_tags.push(CustomTag {
                    name,
                    data_type,
                    pattern,
                });
            }
            Ok(Some(custom_tags))
        } else {
            Ok(None)
        }
    }
}

impl PointMappingRule {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let source_type = SourceType::try_from(dsn)?;
        let stable_expression = match source_type {
            SourceType::OPCUA | SourceType::OPCDA => {
                ColumnConfig::parse_stable_expression(dsn, "opc")?
            }
            SourceType::KingHistorian => ColumnConfig::parse_stable_expression(dsn, "kinghist")?,
        };

        let tbname_expression = ColumnConfig::parse_tbname_expression(dsn)?;

        let value_col = parse_key_in_dsn::<String>(dsn, "value_col")
            .ok()
            .flatten()
            .unwrap_or("val".to_string());
        let value_transform = parse_key_in_dsn::<String>(dsn, "value_transform")
            .ok()
            .flatten();

        let primary_key =
            ColumnConfig::parse_primary_key(dsn)?.unwrap_or(ColumnConfig::ORIGINAL_TS.to_string());
        let primary_key_alias =
            ColumnConfig::parse_primary_key_alias(dsn)?.unwrap_or("ts".to_string());

        let custom_tags = CustomTag::try_from_dsn(dsn)?;

        Ok(Self {
            source_type,
            stable_expression,
            tbname_expression,
            value_col,
            value_transform,
            primary_key,
            primary_key_alias,
            custom_tags,
        })
    }

    /// 根据点位数据生成点位映射规则。在 OPC 任务开始执行前调用，用于生成点位映射规则
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
            let point_id = p.id.as_str();

            // 如果 p.type 存在，且不等于 Variable 则跳过
            if let Some(node_class) = &p.r#type
                && node_class != "Variable"
            {
                continue;
            }

            // 处理 BrowseName,DisplayName,Description 等
            let opc_node = OpcNode::try_from(p.clone()).unwrap_or(OpcNode {
                id: point_id.to_string(),
                is_static: Some(false),
                name: None,
                description: None,
                display_name: None,
                node_type: None,
                parent_id: None,
                path: None,
            });

            // point_config
            let mut point_config = self.gen_point_config(index, point_id.to_string(), None)?;
            // handle extra custom tag values
            self.extra_custom_tags(&mut point_config, &opc_node)?;

            point_map.insert(point_id.to_string(), point_config);

            // table_config
            let table_config = self.gen_table_config(None)?;
            table_map.insert(point_id.to_string(), table_config);
        }

        Ok((point_map, table_map))
    }

    pub fn gen_point_config(
        &self,
        index: usize,
        point_id: String,
        point_type: Option<IpcDataType>,
    ) -> anyhow::Result<PointConfig> {
        // 生成 tbname
        let tbname = generate_tbname_from_pattern(
            self.source_type.as_static_str(),
            self.tbname_expression.as_str(),
            point_id.as_str(),
        );
        // 生成 stable
        let stable = generate_stable_from_pattern(&self.stable_expression, &point_type);

        // 生成 tag_values
        let tag_values = if let Some(custom_tags) = &self.custom_tags {
            if custom_tags.is_empty() {
                None
            } else {
                let mut map = HashMap::new();
                for custom_tag in custom_tags {
                    let tag_value = generate_tag_value_from_pattern(
                        self.source_type.as_static_str(),
                        &custom_tag.pattern,
                        &point_id,
                    );
                    map.insert(custom_tag.name.clone(), tag_value);
                }
                Some(map)
            }
        } else {
            None
        };

        let point_config = PointConfig {
            row_index: index,
            code: tbname,
            stable: Some(stable),
            tag_values,
            value_type: point_type,
        };

        Ok(point_config)
    }

    /// 遍历 PointConfig 的 tag_values，对于每个 map<Key,Value>，如果 Value 中存在以下特殊的pattern，进行替换。
    /// {BrowseName}: 替换成 opc_node.name, 如果 opc_node.name 为空，替换为空字符串
    /// {DisplayName}: 替换成 opc_node.display_name, 如果 opc_node.display_name 为空，替换为空字符串
    /// {Description}: 替换成 opc_node.description, 如果 opc_node.description 为空，替换为空字符串
    fn extra_custom_tags(
        &self,
        point_config: &mut PointConfig,
        opc_node: &OpcNode,
    ) -> anyhow::Result<()> {
        if let Some(tag_values) = point_config.tag_values.as_mut() {
            for (_tag_name, tag_value) in tag_values.iter_mut() {
                let browse_name = opc_node.name.as_deref().unwrap_or("");
                let display_name = opc_node.display_name.as_deref().unwrap_or("");
                let description = opc_node.description.as_deref().unwrap_or("");
                let path = opc_node.path.as_deref().unwrap_or("");

                // Replace placeholders; if the corresponding field is empty or None, replace with empty string
                *tag_value = tag_value.replace("{BrowseName}", browse_name);
                *tag_value = tag_value.replace("{DisplayName}", display_name);
                *tag_value = tag_value.replace("{Description}", description);
                *tag_value = tag_value.replace("{Path}", path);
            }
        }

        Ok(())
    }

    pub fn gen_table_config(&self, point_type: Option<IpcDataType>) -> anyhow::Result<TableConfig> {
        let value_type = point_type.map(|t| t.ty());

        let mut column_configs = vec![];
        column_configs.push(ColumnConfig {
            name: ColumnConfig::VALUE.to_string(),
            r#type: value_type,
            alias: Some(self.value_col.clone()),
            transform: self.value_transform.clone(),
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

        let tag_configs = if let Some(custom_tags) = &self.custom_tags {
            if custom_tags.is_empty() {
                None
            } else {
                let tags = custom_tags
                    .iter()
                    .map(|custom_tag| TagConfig {
                        name: custom_tag.name.clone(),
                        r#type: custom_tag.data_type.clone(),
                    })
                    .collect_vec();
                Some(tags)
            }
        } else {
            None
        };

        let table_config = TableConfig {
            enabled: Some(1),
            stable_prefix: None,
            column_configs,
            tag_configs,
        };

        Ok(table_config)
    }

    pub fn generate_node_config_map(
        &self,
        datasets: Vec<DataSet>,
    ) -> anyhow::Result<LinkedHashMap<String, ObjectNodeConfig>> {
        let mut node_map = LinkedHashMap::new();

        for d in datasets {
            let opc_node = OpcNode::try_from(d).context("failed to convert DataSet to OpcNode")?;
            if opc_node.node_type != Some("Object".to_string()) {
                continue;
            }

            // TODO: 让用户可配置 name 的生成规则
            // name 做了特殊处理，使用 {id#/.} 规则替换斜杠和点，避免生成非法的 tag 值
            let name = generate_tag_value_from_pattern(
                self.source_type.as_static_str(),
                "{id#/.}",
                opc_node.id.as_str(),
            );

            let node_config = ObjectNodeConfig {
                id: opc_node.id.clone(),
                name: Some(name),
                browse_name: opc_node.name.clone(), // Node BrowseName
                path: opc_node.path.clone(),
                display_name: opc_node.display_name.clone(),
                description: opc_node.description.clone(),
            };
            node_map.insert(opc_node.id.clone(), node_config);
        }

        Ok(node_map)
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

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct ObjectNodeConfig {
    pub id: String,
    pub name: Option<String>,
    pub browse_name: Option<String>,  // Node BrowseName
    pub display_name: Option<String>, // Node DisplayName
    pub description: Option<String>,  // Node Description
    pub path: Option<String>,         // Node Path
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::*;

    #[test]
    fn test_point_mapping_generate() {
        let json = r#"[{"id":"ns=3;s=\"数据块_1\".\"Tag1\"","name":"\"数据块_1\".\"Tag1\""}]"#;
        let res: Vec<DataSet> = serde_json::from_slice(json.as_bytes()).unwrap();

        let rule = PointMappingRule {
            source_type: SourceType::OPCUA,
            stable_expression: "opc_{type}".to_string(),
            tbname_expression: "t_{ns}_{id}".to_string(),
            value_col: "val".to_string(),
            value_transform: None,
            primary_key: "original_ts".to_string(),
            primary_key_alias: "ts".to_string(),
            custom_tags: None,
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
            SourceType::OPCUA,
            &StringRecord::from(vec!["tbname", "point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["tb1", "ns=3;i=1015", "stb_int"]);
        let table_config = TableConfig::from_csv(&csv_header, &row, SourceType::OPCUA).unwrap();

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
        let res = PointModelConfig::check_stable(stable.as_ref());
        // then
        assert!(res.is_ok());

        // given
        let stable = Some("opc_abc".to_string());
        // when
        let res = PointModelConfig::check_stable(stable.as_ref());
        // then
        assert!(res.is_ok());

        // given and when
        let res = PointModelConfig::check_stable(None);
        // then
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "stable is required");

        // given and when
        let res = PointModelConfig::check_stable(Some(&"".to_string()));
        // then
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "stable is required");

        // given
        let stable = Some("t_{abc}".to_string());
        // when
        let res = PointModelConfig::check_stable(stable.as_ref());
        // then
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid stable expression: t_{abc}"
        );
    }

    #[test]
    fn test_check_tbname() {
        let res = PointModelConfig::check_tbname(SourceType::OPCUA, "t_{ns}_{id}");
        assert!(res.is_ok());

        let res = PointModelConfig::check_tbname(SourceType::OPCDA, "t_{tag_name}");
        assert!(res.is_ok());

        let res = PointModelConfig::check_tbname(SourceType::OPCDA, "t_{TagName}");
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid tbname expression: t_{TagName}"
        );

        let res = PointModelConfig::check_tbname(SourceType::OPCUA, "t_abc");
        assert!(res.is_ok());

        let res = PointModelConfig::check_tbname(SourceType::OPCUA, "t_{tag_name}");
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid tbname expression: t_{tag_name}"
        );

        let res = PointModelConfig::check_tbname(SourceType::OPCUA, "");
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
        let opc_model = CsvParser::parse_csv(SourceType::OPCUA, csv).await.unwrap();
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
        let opc_model = CsvParser::parse_csv(SourceType::OPCUA, csv).await.unwrap();
        let res = opc_model.validate();
        // then
        assert!(res.is_ok())
    }

    /// 检查 OPC csv 文件和 database 的 schema 是否冲突
    #[tokio::test]
    async fn test_validate_with_sink_of_opc_model_config_with_taos() {
        // given
        let csv = r#"point_id,stable,tbname,val_col,ts_col,tag::INT::id,tag::VARCHAR(20)::name
        ns=3;i=1001,opc_{type},t_{ns}_{id},val,ts,123,abc"#
            .to_string();
        let mut sink = "taos:///".into_dsn().unwrap();

        let taos = TaosBuilder::from_dsn(&sink).unwrap().build().await.unwrap();
        taos.exec_many(vec![
            "DROP DATABASE IF EXISTS test_opc_model_config",
            "CREATE DATABASE IF NOT EXISTS test_opc_model_config",
            "USE test_opc_model_config",
            "CREATE STABLE IF NOT EXISTS opc_int (ts TIMESTAMP, val INT, quality INT) TAGS (id INT, name NCHAR(20))",
            "CREATE TABLE IF NOT EXISTS t_3_1001 USING opc_int TAGS (123, 'abc')",
        ]).await.unwrap();

        // when
        let model = CsvParser::parse_csv(SourceType::OPCUA, csv).await.unwrap();
        sink.subject = Some("test_opc_model_config".to_string());
        let res = model
            .validate_with_sink(ModelType::SingleColumn, &sink)
            .await;

        // then
        assert!(res.is_ok());

        // clean up
        taos.exec("DROP DATABASE IF EXISTS test_opc_model_config")
            .await
            .unwrap();
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
        let res = PointModelConfig::join_by_point_id(p, t);

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
    fn test_check_tag_type() {
        // bool
        assert!(PointModelConfig::check_tag_type("true", &IpcDataType::Bool).is_ok());
        assert!(PointModelConfig::check_tag_type("false", &IpcDataType::Bool).is_ok());

        // spellchecker:off
        assert!(PointModelConfig::check_tag_type("ture", &IpcDataType::Bool).is_err());
        // spellchecker:on

        // u8
        assert!(PointModelConfig::check_tag_type("1", &IpcDataType::UInt8).is_ok());
        assert!(PointModelConfig::check_tag_type("256", &IpcDataType::UInt8).is_err());
        // u16
        assert!(PointModelConfig::check_tag_type("1", &IpcDataType::UInt16).is_ok());
        assert!(PointModelConfig::check_tag_type("65536", &IpcDataType::UInt16).is_err());
        // u32
        assert!(PointModelConfig::check_tag_type("1", &IpcDataType::UInt32).is_ok());
        assert!(PointModelConfig::check_tag_type("abc", &IpcDataType::UInt32).is_err());
        // u64
        assert!(PointModelConfig::check_tag_type("1", &IpcDataType::UInt64).is_ok());
        assert!(PointModelConfig::check_tag_type("abc", &IpcDataType::UInt64).is_err());
        // i8
        assert!(PointModelConfig::check_tag_type("1", &IpcDataType::Int8).is_ok());
        assert!(PointModelConfig::check_tag_type("3.14", &IpcDataType::Int8).is_err());
        // i16
        assert!(PointModelConfig::check_tag_type("1", &IpcDataType::Int16).is_ok());
        assert!(PointModelConfig::check_tag_type("3.14", &IpcDataType::Int16).is_err());
        // i32
        assert!(PointModelConfig::check_tag_type("1", &IpcDataType::Int32).is_ok());
        assert!(PointModelConfig::check_tag_type("3.14", &IpcDataType::Int32).is_err());
        // i64
        assert!(PointModelConfig::check_tag_type("1", &IpcDataType::Int64).is_ok());
        assert!(PointModelConfig::check_tag_type("3.14", &IpcDataType::Int64).is_err());
        // f32
        assert!(PointModelConfig::check_tag_type("3.14", &IpcDataType::Float32).is_ok());
        assert!(PointModelConfig::check_tag_type("abc", &IpcDataType::Float32).is_err());
        // f64
        assert!(PointModelConfig::check_tag_type("3.14", &IpcDataType::Float64).is_ok());
        assert!(PointModelConfig::check_tag_type("abc", &IpcDataType::Float64).is_err());
        // varchar(20)
        assert!(PointModelConfig::check_tag_type("abc", &IpcDataType::VarChar(10)).is_ok());
        assert!(
            PointModelConfig::check_tag_type("12345678901", &IpcDataType::VarChar(10)).is_err()
        );
        assert!(PointModelConfig::check_tag_type("一二三", &IpcDataType::VarChar(10)).is_ok());
        assert!(PointModelConfig::check_tag_type("一二三四", &IpcDataType::VarChar(10)).is_err());
    }

    #[test]
    fn test_is_expr() {
        // stable
        assert!(PointConfig::is_expr(
            SourceType::OPCUA,
            "stable",
            "opc_{type}"
        ));
        assert!(!PointConfig::is_expr(SourceType::OPCUA, "stable", "opc"));

        // tbname
        assert!(PointConfig::is_expr(
            SourceType::OPCUA,
            "tbname",
            "t_{ns}_{id}"
        ));
        assert!(PointConfig::is_expr(
            SourceType::OPCDA,
            "tbname",
            "t_{tag_name}"
        ));
        assert!(!PointConfig::is_expr(
            SourceType::OPCUA,
            "tbname",
            "t_{tag_name}"
        ));
        assert!(!PointConfig::is_expr(SourceType::OPCUA, "tbname", "tb123"));
    }

    fn build_point_model_with_two_points() -> PointModelConfig {
        let mut point_config_map = LinkedHashMap::new();
        point_config_map.insert(
            "p1".to_string(),
            PointConfig {
                row_index: 1,
                code: "t1".to_string(),
                stable: Some("opc_int".to_string()),
                tag_values: Some(HashMap::from([("tag".to_string(), "1".to_string())])),
                value_type: Some(IpcDataType::Int32),
            },
        );
        point_config_map.insert(
            "p2".to_string(),
            PointConfig {
                row_index: 2,
                code: "t2".to_string(),
                stable: Some("opc_int".to_string()),
                tag_values: Some(HashMap::from([("tag".to_string(), "2".to_string())])),
                value_type: Some(IpcDataType::Int32),
            },
        );

        let column_configs = vec![
            ColumnConfig {
                name: ColumnConfig::ORIGINAL_TS.to_string(),
                r#type: Some(Ty::Timestamp),
                alias: Some("ts".to_string()),
                transform: None,
                is_primary_key: true,
            },
            ColumnConfig {
                name: ColumnConfig::VALUE.to_string(),
                r#type: Some(Ty::Int),
                alias: Some("val".to_string()),
                transform: None,
                is_primary_key: false,
            },
            ColumnConfig {
                name: ColumnConfig::QUALITY.to_string(),
                r#type: Some(Ty::Int),
                alias: Some("quality".to_string()),
                transform: None,
                is_primary_key: false,
            },
        ];
        let tag_configs = vec![TagConfig {
            name: "tag".to_string(),
            r#type: IpcDataType::Int32,
        }];
        let table_cfg = TableConfig {
            enabled: Some(1),
            stable_prefix: None,
            column_configs: column_configs.clone(),
            tag_configs: Some(tag_configs.clone()),
        };
        let mut table_config_map = LinkedHashMap::new();
        table_config_map.insert("p1".to_string(), table_cfg.clone());
        table_config_map.insert("p2".to_string(), table_cfg);

        PointModelConfig {
            source_type: SourceType::OPCUA,
            update_mode: None,
            generate_rule: None,
            point_config_map,
            table_config_map,
            node_config_map: None,
        }
    }

    #[test]
    fn test_to_stable_sqls_single_stable() {
        let config = build_point_model_with_two_points();
        let sqls = config.to_stable_sqls();

        assert_eq!(sqls.len(), 1);
        let lower = sqls[0].to_lowercase();
        assert!(lower.contains("create table if not exists `opc_int`"));
        assert!(lower.contains("`ts` timestamp"));
        assert!(lower.contains("`val` int"));
        assert!(lower.contains("tags(`tag`"));
    }

    #[test]
    fn test_to_table_sqls_combines_segments() {
        let config = build_point_model_with_two_points();
        let sqls = config.to_table_sqls();

        assert_eq!(sqls.len(), 1);
        let lower = sqls[0].to_lowercase();
        assert!(lower.contains("create table if not exists `t1` using `opc_int` (`tag`) tags(1)"));
        assert!(lower.contains("if not exists `t2` using `opc_int` (`tag`) tags(2)"));
    }

    #[tokio::test]
    async fn test_parse_stable() {
        let header = CsvHeader::try_new(
            SourceType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["point1", "stable1"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("stable1".to_string()));

        let header = CsvHeader::try_new(
            SourceType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["point1", ""]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, None);

        let header = CsvHeader::try_new(
            SourceType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "meters_{type}"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("meters_{type}".to_string()));

        let header = CsvHeader::try_new(
            SourceType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable", "type"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "meters_{type}", ""]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("meters_{type}".to_string()));

        let header = CsvHeader::try_new(
            SourceType::OPCUA,
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
            SourceType::OPCUA,
            &StringRecord::from(vec!["point_id", "value_col", "value_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["123", "value", "value + 1"]);
        let value_col = TableConfig::parse_value_col(&header, &row).unwrap();
        assert_eq!(value_col.alias.unwrap(), "value");
        assert_eq!(value_col.transform.unwrap(), "value + 1");

        let header = CsvHeader::try_new(
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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
            SourceType::OPCUA,
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

    #[test]
    fn test_generate_tbname_from_pattern() {
        // OPC UA
        assert_eq!(
            generate_tbname_from_pattern("opcua", "t_{ns}_{id}", "ns=13;i=10003"),
            "t_13_10003"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcua", "t_{ns}_{id}", "ns=13;b=GCC"),
            "t_13_GCC"
        );
        assert_eq!(
            generate_tbname_from_pattern(
                "opcua",
                "t_{ns}_{id}",
                "ns=13;g=00000000-0000-0000-0000-000000009204"
            ),
            "t_13_00000000-0000-0000-0000-000000009204"
        );
        assert_eq!(
            generate_tbname_from_pattern(
                "opcua",
                "t_{ns}_{id}",
                r#"ns=3;s=Special_\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]}"#
            ),
            r#"t_3_Special_\"!§$%&/()=?_´\\+~*'#_-:_;,<>|@^°€µ{[]}"#
        );

        assert_eq!(
            generate_tbname_from_pattern("opcua", "t_{ns}_{id#/_}", "ns=2;s=/Dev/Type-Name/Tag/"),
            "t_2_Dev_Type-Name_Tag"
        );

        // OPC DA
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{TagName}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{tag_name}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{/tag_name}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_EDCGQ_MP706AT_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{id}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            "t_/ASSETS/AB/EDCGQ_MP706AT_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{TagName}", "02_LI7059.DACA.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{tag_name}", "02_LI7059.DACA.PV"),
            "t_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{/tag_name}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{id}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{_id}", "02_LI7059.DACA.PV"),
            "t_02_LI7059_DACA_PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{id#/_}", "/ASSETS/AB/EDCGQ.MP706AT-PV/"),
            "t_ASSETS_AB_EDCGQ_MP706AT-PV"
        );
        assert_eq!(
            generate_tbname_from_pattern("opcda", "t_{id#-_}", "ASSETS-AB-EDCGQ-MP706AT-PV"),
            "t_ASSETS_AB_EDCGQ_MP706AT_PV"
        );
    }

    #[test]
    fn test_generate_tag_value_from_pattern() {
        let point_id = "ns=2;s=PLC.DETAIL.DEV.METRIC";
        let expects = [
            ("{ns}", "2"),
            ("{id}", "PLC.DETAIL.DEV.METRIC"),
            ("{id.}", "PLC.DETAIL.DEV"),
            ("{id/}", "PLC.DETAIL.DEV.METRIC"),
            ("{id_}", "PLC.DETAIL.DEV.METRIC"),
            ("{id..}", "PLC.DETAIL"),
            ("{..id.}", "DEV"),
            ("constant_value", "constant_value"),
        ];
        for (t, e) in expects {
            let v = generate_tag_value_from_pattern("opcua", t, point_id);
            assert_eq!(v, e);
        }

        // OPC UA additional transforms
        let point_id2 = "ns=2;s=/Dev/Type_Name/Tag-01/";
        let expects2 = [
            ("{id#/.}", "Dev.Type_Name.Tag-01"),
            ("{id#-.}", "/Dev/Type_Name/Tag.01/"),
            ("{id#/_}", "Dev_Type_Name_Tag-01"),
            ("{id#-_}", "/Dev/Type_Name/Tag_01/"),
            ("{id/#/.}", ".Dev.Type_Name.Tag-01"),
            ("{id_#_.}", "/Dev/Type"),
        ];
        for (t, e) in expects2 {
            let v = generate_tag_value_from_pattern("opcua", t, point_id2);
            assert_eq!(v, e);
        }

        // OPC UA, point_id without ';' (ns should fallback to "0")
        let pid_no_ns = "/Dev/Type.Name/Tag";
        let expects3 = [
            ("{ns}", ""),
            ("{id}", "Objects"),
            ("t_{ns}_{id}", "t_Objects"),
            ("t_{ns}_{id#/.}", "t_Objects"),
            ("t_{ns}_{id#/_}", "t_Objects"),
        ];
        for (t, e) in expects3 {
            let v = generate_tag_value_from_pattern("opcua", t, pid_no_ns);
            assert_eq!(v, e);
        }

        let tag_name = "/ASSETS/AB/EDCGQ.MP706AT.PV";
        let expects = [
            ("{TagName}", "PV"),
            ("{tag_name}", "PV"),
            ("{/tag_name}", "EDCGQ.MP706AT.PV"),
            ("{id}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            ("{_id}", "_ASSETS_AB_EDCGQ.MP706AT.PV"),
            ("{id#/.}", "ASSETS.AB.EDCGQ.MP706AT.PV"),
            ("{id#-.}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            ("{id#/_}", "ASSETS_AB_EDCGQ.MP706AT.PV"),
            ("{id#-_}", "/ASSETS/AB/EDCGQ.MP706AT.PV"),
            ("constant_value", "constant_value"),
        ];
        for (t, e) in expects {
            let v = generate_tag_value_from_pattern("opcda", t, tag_name);
            assert_eq!(v, e);
        }

        let tag_name_dash = "Device-Type-Tag-Name";
        let expects_dash = [
            ("{id#-.}", "Device.Type.Tag.Name"),
            ("{id#/.}", "Device-Type-Tag-Name"),
        ];
        for (t, e) in expects_dash {
            let v = generate_tag_value_from_pattern("opcda", t, tag_name_dash);
            assert_eq!(v, e);
        }

        let tag_name_mix = "/Dev/Type-Name/Tag-01";
        let expects_mix = [
            ("{id#/.}", "Dev.Type-Name.Tag-01"),
            ("{id#-.}", "/Dev/Type.Name/Tag.01"),
        ];
        for (t, e) in expects_mix {
            let v = generate_tag_value_from_pattern("opcda", t, tag_name_mix);
            assert_eq!(v, e);
        }

        // Trailing slash should also be trimmed
        let tag_name_trailing = "/Device/Type/TagName/";
        let v = generate_tag_value_from_pattern("opcda", "{id#/.}", tag_name_trailing);
        assert_eq!(v, "Device.Type.TagName");
    }

    #[test]
    fn test_parse_stable_expression() {
        let dsn = "opcua://?super_table_expression=abc_{type}"
            .to_string()
            .into_dsn()
            .unwrap();
        let stable_expression = ColumnConfig::parse_stable_expression(&dsn, "opc").unwrap();
        assert_eq!(stable_expression, "abc_{type}");

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let stable_expression = ColumnConfig::parse_stable_expression(&dsn, "opc").unwrap();
        assert_eq!(stable_expression, "opc_{type}");
    }

    #[test]
    fn test_parse_tbname_expression() {
        let dsn = "opcua://?child_table_expression=t_{ns}_{id}"
            .to_string()
            .into_dsn()
            .unwrap();
        let tbname_expression = ColumnConfig::parse_tbname_expression(&dsn).unwrap();
        assert_eq!(tbname_expression, "t_{ns}_{id}");

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let result = ColumnConfig::parse_tbname_expression(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "child_table_expression is required",
            result.err().unwrap().to_string()
        );
    }

    #[test]
    fn test_parse_primary_key() {
        let dsn = "opcua://?table_primary_key=original_ts"
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key = ColumnConfig::parse_primary_key(&dsn).unwrap();
        assert_eq!(primary_key, Some("original_ts".to_string()));

        let dsn = "opcua://?table_primary_key=received_ts"
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key = ColumnConfig::parse_primary_key(&dsn).unwrap();
        assert_eq!(primary_key, Some("received_ts".to_string()));

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let primary_key = ColumnConfig::parse_primary_key(&dsn).unwrap();
        assert_eq!(primary_key, None);

        let dsn = "opcua://?table_primary_key="
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key = ColumnConfig::parse_primary_key(&dsn).unwrap();
        assert_eq!(primary_key, None);

        let dsn = "opcua://?table_primary_key=invalid"
            .to_string()
            .into_dsn()
            .unwrap();
        let result = ColumnConfig::parse_primary_key(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid table_primary_key: invalid, must be original_ts or request_ts or received_ts",
            result.err().unwrap().to_string()
        );
    }

    #[test]
    fn test_parse_primary_key_alias() {
        let dsn = "opcua://?table_primary_key_alias=ts"
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key_alias = ColumnConfig::parse_primary_key_alias(&dsn).unwrap();
        assert_eq!(primary_key_alias, Some("ts".to_string()));

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let primary_key_alias = ColumnConfig::parse_primary_key_alias(&dsn).unwrap();
        assert_eq!(primary_key_alias, None);

        let dsn = "opcua://?table_primary_key_alias="
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key_alias = ColumnConfig::parse_primary_key_alias(&dsn).unwrap();
        assert_eq!(primary_key_alias, None);
    }

    #[test]
    fn test_point_model_config_deserialize() {
        // given legacy field name `opc_type` should map to `source_type`
        let json = r#"{
            "opc_type": "opcda",
            "point_config_map": {},
            "table_config_map": {}
        }"#;

        // when
        let cfg: PointModelConfig =
            serde_json::from_str(json).expect("should deserialize with alias opc_type");

        // then
        assert_eq!(cfg.source_type, SourceType::OPCDA);
        assert!(cfg.generate_rule.is_none());
        assert!(cfg.point_config_map.is_empty());
        assert!(cfg.table_config_map.is_empty());
        // given camelCase legacy field name `opcType` should also map to `source_type`
        let json = r#"{
            "opcType": "opcua",
            "point_config_map": {},
            "table_config_map": {}
        }"#;

        // when
        let cfg: PointModelConfig =
            serde_json::from_str(json).expect("should deserialize with alias opcType");

        // then
        assert_eq!(cfg.source_type, SourceType::OPCUA);

        // given
        let cfg = PointModelConfig {
            source_type: SourceType::OPCDA,
            update_mode: None,
            generate_rule: None,
            point_config_map: LinkedHashMap::new(),
            table_config_map: LinkedHashMap::new(),
            node_config_map: None,
        };

        // when
        let s = serde_json::to_string(&cfg).expect("serialize ok");
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();

        // then: only `source_type` exists and is lowercase value
        assert_eq!(v.get("source_type").and_then(|x| x.as_str()), Some("opcda"));
        assert!(v.get("opc_type").is_none());
        assert!(v.get("opcType").is_none());
        assert!(v.get("sourceType").is_none());
    }

    #[tokio::test]
    async fn test_build_sub_table_sqls() {
        // given
        let content = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/opc/opcua-utf8.csv"
        ))
        .to_string();

        // when
        let model = CsvParser::parse_csv(SourceType::OPCUA, content)
            .await
            .unwrap();
        let sqls = model.to_table_sqls();

        // then
        assert_eq!(sqls.len(), 1);
        assert_eq!(
            sqls[0],
            r#"CREATE TABLE IF NOT EXISTS `t_3_1005` USING `opc_int` (`name`) TAGS('入库温度')"#
        );
    }

    #[tokio::test]
    async fn test_build_create_table_sql_large() {
        let content = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/kinghist-2k.csv"
        ))
        .to_string();

        let model = CsvParser::parse_csv(SourceType::KingHistorian, content)
            .await
            .unwrap();

        // let sqls = model.to_stable_sqls();
        // assert_eq!(sqls.len(), 4);

        // sqls.iter().for_each(|sql| {
        //     println!("{};", sql);
        // });

        model.to_create_table_sqls().iter().for_each(|sql| {
            println!("{};", sql);
        });
    }

    #[test]
    fn test_custom_tag() {
        let dsn = "opcua://?custom_tags=VARCHAR(20)::location::Beijing{id#/.};INT::age::30"
            .into_dsn()
            .unwrap();

        let custom_tags = CustomTag::try_from_dsn(&dsn).unwrap().unwrap();
        assert_eq!(custom_tags.len(), 2);
        assert_eq!(custom_tags[0].name, "location");
        assert_eq!(custom_tags[0].data_type, IpcDataType::VarChar(20));
        assert_eq!(custom_tags[0].pattern, "Beijing{id#/.}");
        assert_eq!(custom_tags[1].name, "age");
        assert_eq!(custom_tags[1].data_type, IpcDataType::Int32);
        assert_eq!(custom_tags[1].pattern, "30");
    }
}
