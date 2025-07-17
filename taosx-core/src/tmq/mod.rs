use std::{
    ops::{AddAssign, SubAssign},
    str::FromStr,
};

use anyhow::{bail, Context, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use taos::*;

use crate::utils::sql::connect_taos_root;
use crate::{dsv::DataSourceValidation, utils};

pub mod tmq_metric;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TopicTable {
    pub stable: Option<String>,
    pub stable_sql: Option<String>,
    pub table: String,
    pub table_sql: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum TopicType {
    Database,
    DatabaseWithMeta,
    Stable,
    StableWithMeta,
    Query,
    #[doc(hidden)]
    #[default]
    NoneExhaustive,
}

impl TopicType {
    fn from_sql(sql: &str) -> Self {
        let sql = sql.to_lowercase();

        let with_meta = sql.contains("with meta");
        if sql.contains("as database") {
            if with_meta {
                Self::DatabaseWithMeta
            } else {
                Self::Database
            }
        } else if sql.contains("as stable") {
            if with_meta {
                Self::StableWithMeta
            } else {
                Self::Stable
            }
        } else {
            debug_assert!(sql.contains("as select"));
            Self::Query
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Topic {
    pub name: String,
    pub database: String,
    pub vgroups: usize,
    pub database_sql: Option<String>,
    #[serde(flatten)]
    pub table: Option<TopicTable>,
    #[serde(default)]
    pub topic_type: TopicType,
    pub use_table_name: Option<String>,
}

impl Topic {
    pub fn is_query(&self) -> bool {
        matches!(self.topic_type, TopicType::Query)
    }
}

// tmq metrics
pub const METRIC_TMQ_TOPICS: &str = "metrics.tmq.topics";
pub const METRIC_TMQ_WORKERS: &str = "metrics.tmq.workers";
pub const METRIC_TMQ_MESSAGES: &str = "metrics.tmq.messages";
pub const METRIC_TMQ_MESSAGES_OF_META: &str = "metrics.tmq.messages_of_meta";
pub const METRIC_TMQ_WRITE_META_FAILS: &str = "metrics.tmq.write_meta_fails";
pub const METRIC_TMQ_MESSAGES_OF_DATA: &str = "metrics.tmq.messages_of_data";
pub const METRIC_TMQ_BLOCKS: &str = "metrics.tmq.blocks";
// pub const METRIC_TMQ_WRITE_RAW_BLOCK_FAILS: &str = "tmq.write_raw_block_fails";
pub const METRIC_TMQ_RECORDS: &str = "metrics.tmq.records";
pub const METRIC_TMQ_POINTS: &str = "metrics.tmq.points";
// pub const METRIC_TMQ_TIME_COST: &str = "tmq.time_cost";

/// StopAt is a enum to represent the stop time.
/// example:
/// - `now` or `0` means stop at now.
/// - `-1s` means stop at now - 1s.
/// - `+1s` means stop at now + 1s.
/// - `2021-09-01T00:00:00+08:00` means stop at the specific time.
/// - `1000rows` means stop when received 1000 rows.
#[allow(unused)]
#[derive(Debug, Clone)]
pub(crate) enum StopAt {
    DateTime(chrono::DateTime<Local>),
    Rows(usize),
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum StopAtError {
    #[error(transparent)]
    DurationParse(#[from] fundu::ParseError),
    #[error(transparent)]
    DateTimeCalculate(#[from] chrono::OutOfRangeError),
    #[error(transparent)]
    DateTimeParse(#[from] chrono::ParseError),
    #[error("rows parse error: {0}")]
    RowsParse(#[from] std::num::ParseIntError),
}

impl FromStr for StopAt {
    type Err = StopAtError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut at = Local::now();
        match s {
            "" | "0" | "now" => Ok(Self::DateTime(at)),
            s if s.starts_with('-') => {
                let s = s.trim_start_matches('-');
                let duration = utils::parse_duration(s).map_err(StopAtError::DurationParse)?;
                let duration =
                    chrono::Duration::from_std(duration).map_err(StopAtError::DateTimeCalculate)?;
                at.sub_assign(duration);
                Ok(Self::DateTime(at))
            }
            s if s.starts_with('+') => {
                let s = s.trim_start_matches('+');
                let duration = utils::parse_duration(s).map_err(StopAtError::DurationParse)?;
                let duration =
                    chrono::Duration::from_std(duration).map_err(StopAtError::DateTimeCalculate)?;
                at.add_assign(duration);
                Ok(Self::DateTime(at))
            }
            s if s.ends_with("rows") => {
                let s = s.trim_end_matches("rows");
                let rows = s.parse().map_err(StopAtError::RowsParse)?;
                Ok(Self::Rows(rows))
            }
            s => {
                let d = chrono::DateTime::parse_from_rfc3339(s)?;
                Ok(Self::DateTime(d.into()))
            }
        }
    }
}

/// Parse input dsn, returns subscription dsn and a list of topics.
///
/// Steps:
/// 1. extract topics from dsn.
/// 2. check each name in topic list.
/// 3. if the name is in topic list, return topic itself.
/// 4. if the name is not a topic.
///    4.1 if the name is a database, create a database topic with meta as is.
///    4.2 else, if the name is in `database.table` format,
///    4.3       then if the `table` is STable, create a topic named `database_table` with meta as stable.
///    4.4            if the `table` is child table or normal, create a topic named `database_table` as select * from table.
///    4.5            else, bail unexpected input topics error to upstream.
pub async fn check_tmq_dsn(mut from: Dsn) -> Result<(Dsn, TaosBuilder, Vec<Topic>, bool, bool)> {
    // let origin = from.clone();
    let database = from.subject.take().ok_or(RawError::new(
        Code::FAILED,
        format!("requires topic or database in source dsn: {from}"),
    ))?;
    // dbg!(&from, &database);
    let use_topic_name = from.remove("use.topic.name");
    let use_table_name = from.remove("use.table.name");

    let with_meta_delete = from
        .remove("with.meta.delete")
        .and_then(|val| val.parse().ok())
        .unwrap_or(true);
    let with_meta_drop = from
        .remove("with.meta.drop")
        .and_then(|val| val.parse().ok())
        .unwrap_or(true);

    match from.get("timeout") {
        Some(s) => {
            if matches!(s.as_str(), "0" | "-1") {
                from.set("timeout", "never");
            }
        }
        None => {
            from.set("timeout", "5s");
        }
    }

    if let Some(val) = from.get("auto.offset.reset") {
        if val != "latest" && val != "earliest" {
            bail!("`auto.offset.reset` option only support `latest` or `earliest`");
        }
    } else {
        from.set("auto.offset.reset", "earliest");
    }
    if from.get("experimental.snapshot.enable").is_none() {
        from.set("experimental.snapshot.enable", "true");
    }

    if !from.get("enable.auto.commit").is_some_and(|s| {
        matches!(
            s.as_str(),
            "true"
                | ""
                | "1"
                | "yes"
                | "on"
                | "enable"
                | "enabled"
                | "T"
                | "TRUE"
                | "YES"
                | "ON"
                | "ENABLE"
                | "ENABLED"
        )
    }) {
        from.set("auto.commit.interval.ms", i32::MAX.to_string());
    }

    let mut replica = false;
    if let Some(val) = from.get("msg.consume.excluded") {
        let val = val.trim();
        if !val.is_empty() && val != "1" {
            bail!("`msg.consume.excluded` option only support `1`");
        }
        replica = true;
    } else if from.get("replica").is_some() {
        tracing::info!("Active-StandBy mode, set `msg.consume.excluded=1`");
        from.set("msg.consume.excluded", "1");
        replica = true;
    }
    if replica && from.get("group.id").is_none() {
        from.set("group.id", "replica");
    }

    let builder = TaosBuilder::from_dsn(&from)?;
    let version = builder.server_version().await?;
    if version.starts_with("2.") {
        bail!("tmq does not support TDengine Query");
    }

    let source = builder.build().await?;

    let mut topics = database
        .split(",")
        .map(|s| s.trim().to_string())
        .collect_vec();

    if let Some(topic) = use_topic_name {
        if topics.len() > 1 {
            anyhow::bail!("`use.topic.name` option does not work for multi databases, use \"{from}\" directly");
        }
        let database = topics.pop().unwrap();

        source.exec(format!("use `{database}`")).await?;

        // if !source.database_exists(&database).await? {
        //     anyhow::bail!("database(`{database}`) doest not exist, please check DSN: \"{origin}\"");
        // }

        // todo: should check the topic creation as we need.
        let res = source.create_topic_as_database(&topic, &database).await;
        if let Err(err) = res {
            match err.code().into() {
                // WAL retention period is zero
                0x038C => {
                    anyhow::bail!("{err:#}, use `alter database {database} wal_retention_period 3600` to enable it");
                }
                _ => return Err(err.into()),
            }
        }

        let vgroups = source
            .query_one(format!(
                "select `vgroups` from information_schema.ins_databases where name='{database}'"
            ))
            // .await?
            // .expect("database not exists");
            .await
            .ok()
            .unwrap_or_default()
            .unwrap_or(2);

        let database_sql = match source
            .query_one::<_, ((), String)>(format!("SHOW CREATE DATABASE `{}`", database))
            .await
        {
            Ok(Some((_, sql))) => Some(sql),
            Err(err) => {
                tracing::warn!("SHOW CREATE DATABASE `{}` error: {}", database, err);
                None
            }
            _ => unreachable!(),
        };
        return Ok((
            from,
            builder,
            vec![Topic {
                name: topic,
                database: database.to_string(),
                database_sql,
                vgroups,
                table: None,
                use_table_name: None,
                topic_type: TopicType::DatabaseWithMeta,
            }],
            with_meta_delete,
            with_meta_drop,
        ));
    }
    let source_topics = source.topics().await?;
    let mut databases = Vec::new();
    if topics.len() == 1 {
        let topic = topics.first().unwrap();
        if let Some(topic) = source_topics.iter().find(|t| t.name() == topic) {
            databases.push(topic.db_name().to_string());
            let vgroups = source
                .query_one(format!(
                    "SELECT `vgroups` FROM information_schema.ins_databases WHERE name='{}'",
                    topic.db_name()
                ))
                // .await?
                // .expect("database may not exist");
                .await
                .ok()
                .unwrap_or_default()
                .unwrap_or(2);

            let database_sql = match source
                .query_one::<_, ((), String)>(format!("SHOW CREATE DATABASE `{}`", topic.db_name()))
                .await
            {
                Ok(Some((_, sql))) => Some(sql),
                Err(err) => {
                    tracing::warn!("SHOW CREATE DATABASE `{}` error: {}, so that we can't automatically create a same database", topic.db_name(), err);
                    None
                }
                _ => unreachable!(),
            };

            Ok((
                from,
                builder,
                vec![Topic {
                    name: topic.name().to_string(),
                    database: topic.db_name().to_string(),
                    database_sql,
                    vgroups,
                    table: None,
                    use_table_name: None,
                    topic_type: TopicType::from_sql(topic.sql()),
                }],
                with_meta_delete,
                with_meta_drop,
            ))
        } else if source
            .database_exists(topic)
            .await
            .context(format!("check database exists: {topic}"))?
        {
            // treat it as database if the topic not exists.
            let database = topic;
            let res = source.create_topic_as_database(topic, database).await;
            if let Err(err) = res {
                match err.code().into() {
                    // WAL retention period is zero
                    0x038C => {
                        anyhow::bail!("{err:#}, use `alter database {database} wal_retention_period 3600` to enable it");
                    }
                    _ => return Err(err.into()),
                }
            }

            let vgroups = source
                .query_one(format!(
                    "select `vgroups` from information_schema.ins_databases where name='{database}'"
                ))
                // .await?
                // .expect("database not exists");
                .await
                .ok()
                .unwrap_or_default()
                .unwrap_or(2);

            let database_sql = match source
                .query_one::<_, ((), String)>(format!("SHOW CREATE DATABASE `{}`", database))
                .await
            {
                Ok(Some((_, sql))) => Some(sql),
                Err(err) => {
                    tracing::warn!("SHOW CREATE DATABASE `{}` error: {}", database, err);
                    None
                }
                _ => unreachable!(),
            };
            Ok((
                from,
                builder,
                vec![Topic {
                    name: topic.to_string(),
                    database: database.to_string(),
                    database_sql,
                    vgroups,
                    table: None,
                    use_table_name: None,
                    topic_type: TopicType::DatabaseWithMeta,
                }],
                with_meta_delete,
                with_meta_drop,
            ))
        } else if topic.contains('.') {
            // Extract `database.table` in format.
            let (database, table) = topic.split_once('.').unwrap();
            if source
                .database_exists(database)
                .await
                .context(format!("check database exists: {database}"))?
            {
                // check if is STable
                let stable: Option<String> = source
                    .query_one(format!(
                        "select stable_name from information_schema.ins_stables where db_name = '{}' and stable_name = '{}'",
                        database, table
                    ))
                    .await?;
                if stable.is_some() {
                    let topic = format!("x_{}_{}", database, table);
                    if let Some(topic) = source_topics.iter().find(|t| t.name() == topic) {
                        databases.push(topic.db_name().to_string());
                        let vgroups = source
                            .query_one(format!(
                                "select `vgroups` from information_schema.ins_databases where name='{}'",
                                topic.db_name()
                            ))
                            .await?
                            .expect("database not exists");

                        let database_sql = match source
                            .query_one::<_, ((), String)>(format!(
                                "SHOW CREATE DATABASE `{}`",
                                topic.db_name()
                            ))
                            .await
                        {
                            Ok(Some((_, sql))) => Some(sql),
                            Err(err) => {
                                tracing::warn!(
                                    "SHOW CREATE DATABASE `{}` error: {}",
                                    topic.db_name(),
                                    err
                                );
                                None
                            }
                            _ => unreachable!(),
                        };
                        return Ok((
                            from,
                            builder,
                            vec![Topic {
                                name: topic.name().to_string(),
                                database: topic.db_name().to_string(),
                                database_sql,
                                vgroups,
                                table: None,
                                use_table_name: None,
                                topic_type: TopicType::StableWithMeta,
                            }],
                            with_meta_delete,
                            with_meta_drop,
                        ));
                    } else {
                        source
                            .exec(format!(
                                "create topic `{topic}` with meta as stable `{database}`.`{table}`"
                            ))
                            .await
                            .context(format!("create topic for stable `{database}`.`{table}`"))?;
                        databases.push(database.to_string());
                        let vgroups = source
                            .query_one(format!(
                                "select `vgroups` from information_schema.ins_databases where name='{}'",
                                database
                            ))
                            .await?
                            .expect("database not exists");

                        let database_sql = match source
                            .query_one::<_, ((), String)>(format!(
                                "SHOW CREATE DATABASE `{}`",
                                database
                            ))
                            .await
                        {
                            Ok(Some((_, sql))) => Some(sql),
                            Err(err) => {
                                tracing::warn!(
                                    "SHOW CREATE DATABASE `{}` error: {}",
                                    database,
                                    err
                                );
                                None
                            }
                            _ => unreachable!(),
                        };
                        return Ok((
                            from,
                            builder,
                            vec![Topic {
                                name: topic,
                                database: database.to_string(),
                                database_sql,
                                vgroups,
                                table: None,
                                use_table_name: None,
                                topic_type: TopicType::StableWithMeta,
                            }],
                            with_meta_delete,
                            with_meta_drop,
                        ));
                    }
                }
                // check if is table
                let table_exists: Option<(String, Option<String>)> = source
                    .query_one(format!(
                        "select table_name, stable_name from information_schema.ins_tables where db_name = '{}' and table_name = '{}'",
                        database, table
                    ))
                    .await?;
                if let Some((table, stable)) = table_exists {
                    let topic = format!("x_{}_{}", database, table);

                    if let Some(topic) = source_topics.iter().find(|t| t.name() == topic) {
                        let (_, sql): ((), String) = source
                            .query_one(format!("SHOW CREATE TABLE `{}`.`{}`", database, table))
                            .await?
                            .unwrap();

                        // let mut tags = Vec::new();
                        let stable_sql = if let Some(stable) = stable.as_deref() {
                            let (_, sql): ((), _) = source
                                .query_one(format!("SHOW CREATE STABLE `{database}`.`{stable}`"))
                                .await?
                                .unwrap();
                            Some(sql)
                        } else {
                            None
                        };

                        let topic_table = TopicTable {
                            table,
                            table_sql: sql,
                            stable,
                            stable_sql,
                        };

                        databases.push(topic.db_name().to_string());
                        let vgroups = source
                            .query_one(format!(
                                "select `vgroups` from information_schema.ins_databases where name='{}'",
                                topic.db_name()
                            ))
                            .await?
                            .expect("database not exists");

                        let database_sql = match source
                            .query_one::<_, ((), String)>(format!(
                                "SHOW CREATE DATABASE `{}`",
                                topic.db_name()
                            ))
                            .await
                        {
                            Ok(Some((_, sql))) => Some(sql),
                            Err(err) => {
                                tracing::warn!(
                                    "SHOW CREATE DATABASE `{}` error: {}",
                                    topic.db_name(),
                                    err
                                );
                                None
                            }
                            _ => unreachable!(),
                        };
                        return Ok((
                            from,
                            builder,
                            vec![Topic {
                                name: topic.name().to_string(),
                                database: topic.db_name().to_string(),
                                database_sql,
                                vgroups,
                                table: Some(topic_table),
                                use_table_name,
                                topic_type: TopicType::StableWithMeta,
                            }],
                            with_meta_delete,
                            with_meta_drop,
                        ));
                    } else {
                        let (_, sql): ((), String) = source
                            .query_one(format!("SHOW CREATE TABLE `{}`.`{}`", database, &table))
                            .await?
                            .unwrap();
                        let stable_sql = if let Some(stable) = stable.as_deref() {
                            let (_, sql): ((), _) = source
                                .query_one(format!("SHOW CREATE STABLE `{database}`.`{stable}`"))
                                .await?
                                .unwrap();
                            Some(sql)
                        } else {
                            None
                        };

                        source
                            .exec(format!(
                                "create topic `{topic}` as select * from `{database}`.`{table}`"
                            ))
                            .await
                            .context(format!("create topic for query form {database}"))?;

                        let topic_table = TopicTable {
                            table,
                            table_sql: sql,
                            stable,
                            stable_sql,
                        };

                        databases.push(database.to_string());
                        let vgroups = source
                            .query_one(format!(
                                "select `vgroups` from information_schema.ins_databases where name='{}'",
                                database
                            ))
                            .await?
                            .expect("database not exists");

                        let database_sql = match source
                            .query_one::<_, ((), String)>(format!(
                                "SHOW CREATE DATABASE `{}`",
                                database
                            ))
                            .await
                        {
                            Ok(Some((_, sql))) => Some(sql),
                            Err(err) => {
                                tracing::warn!(
                                    "SHOW CREATE DATABASE `{}` error: {}",
                                    database,
                                    err
                                );
                                None
                            }
                            _ => unreachable!(),
                        };

                        return Ok((
                            from,
                            builder,
                            vec![Topic {
                                name: topic,
                                database: database.to_string(),
                                database_sql,
                                vgroups,
                                table: Some(topic_table),
                                use_table_name,
                                topic_type: TopicType::Query,
                            }],
                            with_meta_delete,
                            with_meta_drop,
                        ));
                    }
                } else {
                    bail!("table does not exist: `{database}`.`{table}`");
                }
            } else {
                bail!("database not exist: {database}");
            }
        } else {
            bail!(format!("unknown topic name: {topic}"))
        }
    } else {
        let found = source_topics
            .iter()
            .filter(|t| topics.iter().any(|name| name.as_str() == t.name()))
            .collect_vec();
        let mut out = Vec::new();
        for topic in found {
            let vgroups: usize = source
                .query_one(format!(
                    "SELECT `vgroups` FROM information_schema.ins_databases WHERE name='{}'",
                    topic.db_name()
                ))
                .await?
                .expect("database not exists");
            let database_sql = match source
                .query_one::<_, ((), String)>(format!("SHOW CREATE DATABASE `{}`", topic.db_name()))
                .await
            {
                Ok(Some((_, sql))) => Some(sql),
                Err(err) => {
                    tracing::warn!("SHOW CREATE DATABASE `{}` error: {}", topic.db_name(), err);
                    None
                }
                _ => unreachable!(),
            };

            let topic_type = TopicType::from_sql(topic.sql());

            out.push(Topic {
                name: topic.name().to_string(),
                database: topic.db_name().to_string(),
                database_sql,
                vgroups,
                table: None,
                use_table_name: use_table_name.clone(),
                topic_type,
            });
        }
        if topics.len() == out.len() {
            // ok;
            Ok((from, builder, out, with_meta_delete, with_meta_drop))
        } else {
            let invalids = topics
                .into_iter()
                .filter(|t| !out.iter().any(|topic| topic.name == *t))
                .collect_vec();
            for topic in invalids {
                if !source
                    .database_exists(&topic)
                    .await
                    .context(format!("check database exists: {topic}"))?
                {
                    anyhow::bail!("{} is not either a topic or a database name", topic);
                } else {
                    let res = source.create_topic_as_database(&topic, &topic).await;
                    if let Err(err) = res {
                        match err.code().into() {
                            // WAL retention period is zero
                            0x038C => {
                                anyhow::bail!("{err:#}, use `alter database {database} wal_retention_period 3600` to enable it");
                            }
                            _ => return Err(err.into()),
                        }
                    }
                    let vgroups = source
                        .query_one(format!(
                            "SELECT `vgroups` FROM information_schema.ins_databases WHERE name='{topic}'"
                        ))
                        .await?
                        .expect("database not exists");
                    let database_sql = match source
                        .query_one::<_, ((), String)>(format!("SHOW CREATE DATABASE `{}`", topic))
                        .await
                    {
                        Ok(Some((_, sql))) => Some(sql),
                        Err(err) => {
                            tracing::warn!("SHOW CREATE DATABASE `{}` error: {}", topic, err);
                            None
                        }
                        _ => unreachable!(),
                    };
                    out.push(Topic {
                        name: topic.to_string(),
                        database: topic.to_string(),
                        database_sql,
                        vgroups,
                        table: None,
                        topic_type: TopicType::DatabaseWithMeta,
                        use_table_name: None,
                    });
                }
            }
            Ok((from, builder, out, with_meta_delete, with_meta_drop))
        }
    }
}

pub fn group_id_hash_by(from: &Dsn, to: &Dsn) -> String {
    let data = vec![from.to_string(), to.to_string()];
    generate_hash(data)
}

pub fn generate_hash(data: Vec<String>) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    for s in data {
        hasher.update(s);
    }
    let id = hasher.finalize();
    let mut group_id = format!("x{:x}", id);
    group_id.truncate(12);
    group_id
}

pub async fn is_tmq_valid(dsn: &Dsn) -> DataSourceValidation {
    let mut dsn = dsn.clone();

    // 如果没有设置 group.id, 则自动生成一个
    if !dsn.params.contains_key("group.id") {
        dsn.params.insert(
            "group.id".to_string(),
            generate_hash(vec![dsn.to_string(), Local::now().to_rfc3339().to_string()]),
        );
    }

    let validation = check_tmq_dsn(dsn.clone()).await;
    match validation {
        Err(err) => DataSourceValidation::invalid(
            dsn.driver.clone(),
            format!("failed to check dsn: {}, cause: {}", dsn, err),
        ),
        Ok((_dsn, builder, topics, _, _)) => {
            // check if the source database has enabled wal
            if let Err(err) = check_wal_enabled(&builder, &topics).await {
                tracing::error!("check wal failed: {:#}", err);
                return DataSourceValidation::invalid(
                    dsn.driver.clone(),
                    format!("check wal failed: {}", err),
                );
            }
            let version = builder.server_version().await;
            match version {
                Err(err) => DataSourceValidation::invalid(
                    dsn.driver.clone(),
                    format!("failed to get server version, cause: {}", err),
                ),
                Ok(version) => DataSourceValidation {
                    valid: true,
                    support: true,
                    data_source: dsn.driver,
                    version: Some(version.to_string()),
                    message: None,
                    namespaces: None,
                },
            }
        }
    }
}

pub async fn check_wal_enabled(taos_builder: &TaosBuilder, topics: &[Topic]) -> anyhow::Result<()> {
    let taos = taos_builder.build().await?;
    for topic in topics {
        // get all subscriptions by topic and consumer group
        let wal_retention_period = taos
            .query_one::<_, usize>(format!(
                "SELECT `wal_retention_period` FROM information_schema.ins_databases WHERE `name` = '{}'",
                topic.database
            ))
            .await?;
        // check if wal is enabled
        if let Some(wal_retention_period) = wal_retention_period {
            if wal_retention_period == 0 {
                bail!("wal is not enabled for topic `{}`", topic.name);
            }
        } else {
            bail!("database not found for topic `{}`", topic.name);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub db_name: Option<String>,
    pub db_sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_sql: Option<String>,
}

impl TryFrom<&Dsn> for BackupObject {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> std::result::Result<Self, Self::Error> {
        let task_id = utils::parse_key_in_dsn::<String>(dsn, "task_id")?;
        let topic = utils::parse_key_in_dsn::<String>(dsn, "topic")?;
        let db_name = utils::parse_key_in_dsn::<String>(dsn, "db_name")?;
        let db_sql = utils::parse_key_in_dsn::<String>(dsn, "db_sql")?;
        let stable_name = utils::parse_key_in_dsn::<String>(dsn, "stable_name")?;
        let stable_sql = utils::parse_key_in_dsn::<String>(dsn, "stable_sql")?;
        Ok(Self {
            task_id,
            topic,
            db_name,
            db_sql,
            stable_name,
            stable_sql,
        })
    }
}

impl BackupObject {
    /// 从 taos 中查询出 BackupObject，只支持查询 $DATABASE 和 $STABLE_NAME
    /// dsn 的格式为： taos://$HOST:$PORT/$DATABASE?stable=$STABLE_NAME
    /// $DATABASE 不能为空，$STABLE_NAME 可以为空
    pub async fn try_from_taos(dsn: &Dsn) -> Result<Option<BackupObject>> {
        let taos = connect_taos_root(dsn).await?;

        let mut backup_obj = BackupObject {
            task_id: None,
            topic: None,
            db_name: None,
            db_sql: None,
            stable_name: None,
            stable_sql: None,
        };

        // database
        let db_name = dsn.subject.as_ref();
        if let Some(db_name) = db_name {
            // 查询 database
            let sql = format!(
                "SELECT name FROM information_schema.ins_databases WHERE name = '{}'",
                db_name
            );
            tracing::debug!("query taos with sql: {}", sql);
            let database: Option<String> = taos.query_one(sql).await?;
            if let Some(database) = database {
                backup_obj.db_name = Some(database);

                // 查询 database 的创建语句
                let sql = format!("SHOW CREATE DATABASE `{}`", db_name);
                tracing::debug!("query taos with sql: {}", sql);
                let (_db, db_sql) = taos
                    .query_one::<_, (String, String)>(sql)
                    .await?
                    .ok_or(anyhow::anyhow!("failed to get create database sql"))?;
                backup_obj.db_sql = Some(db_sql);
            }

            // stable
            let stable = utils::parse_key_in_dsn::<String>(dsn, "stable")?;
            if let Some(stable) = stable {
                let sql = format!(
                    "SELECT stable_name FROM information_schema.ins_stables WHERE db_name = '{}' AND stable_name = '{}'",
                    db_name, stable
                );
                tracing::debug!("query taos with sql: {}", sql);
                let stable_name: Option<String> = taos.query_one(sql).await?;

                if let Some(stable_name) = stable_name {
                    backup_obj.stable_name = Some(stable_name.clone());

                    // 查询 stable 的创建语句
                    let sql = format!("SHOW CREATE STABLE `{}`.`{}`", db_name, stable);
                    tracing::debug!("query taos with sql: {}", sql);
                    let (_stable, stable_sql) =
                        taos.query_one::<_, (String, String)>(sql)
                            .await?
                            .ok_or(anyhow::anyhow!("failed to get create stable sql"))?;
                    backup_obj.stable_sql = Some(stable_sql);
                }
            }
        };

        Ok(Some(backup_obj))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_deserialize_backup_object() {
        let topic_meta = r#"{
            "topic": "x123",
            "db_name": "x123",
            "db_sql": "CREATE DATABASE IF NOT EXISTS x123",
            "stable_name": "x123_stable",
            "stable_sql": "CREATE TABLE IF NOT EXISTS x123_stable (ts TIMESTAMP, f1 INT) TAGS(t1 INT)"
        }"#;

        let topic_meta: BackupObject = serde_json::from_str(topic_meta).unwrap();

        assert_eq!(topic_meta.topic, Some("x123".to_string()));
        assert_eq!(topic_meta.db_name, Some("x123".to_string()));
        assert_eq!(
            topic_meta.db_sql,
            Some("CREATE DATABASE IF NOT EXISTS x123".to_string())
        );
        assert_eq!(topic_meta.stable_name, Some("x123_stable".to_string()));
        assert_eq!(
            topic_meta.stable_sql,
            Some(
                "CREATE TABLE IF NOT EXISTS x123_stable (ts TIMESTAMP, f1 INT) TAGS(t1 INT)"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_group_id_hash() {
        let data = vec!["hello".to_string(), "world".to_string()];
        let group_id = generate_hash(data);
        assert_eq!(group_id.len(), 12);
        assert_eq!(group_id, "x936a185caaa");
    }

    #[test]
    fn test_stop_at() {
        let now = Local::now().timestamp();
        if let StopAt::DateTime(dt) = StopAt::from_str("now").unwrap() {
            assert_eq!(now, dt.timestamp());
        } else {
            panic!("stop_at should be StopAt::DateTime");
        }

        let now = Local::now().timestamp();
        if let StopAt::DateTime(dt) = StopAt::from_str("-1s").unwrap() {
            assert_eq!(now - 1, dt.timestamp());
        } else {
            panic!("stop_at should be StopAt::DateTime");
        }

        let now = Local::now().timestamp();
        if let StopAt::DateTime(dt) = StopAt::from_str("+1s").unwrap() {
            assert_eq!(now + 1, dt.timestamp());
        } else {
            panic!("stop_at should be StopAt::DateTime");
        }

        if let StopAt::DateTime(dt) = StopAt::from_str("2021-09-01T00:00:00+08:00").unwrap() {
            assert_eq!(1630425600, dt.timestamp());
        } else {
            panic!("stop_at should be StopAt::DateTime");
        }

        let stop_at = StopAt::from_str("1000rows").unwrap();
        assert!(matches!(stop_at, StopAt::Rows(1000)));

        let stop_at = StopAt::from_str("12abc22rows");
        assert_eq!(
            stop_at.unwrap_err().to_string(),
            "rows parse error: invalid digit found in string"
        );
    }

    #[tokio::test]
    async fn test_invalid() {
        // tmq
        let dsn = Dsn::from_str("tmq+ws://192.168.1.92:6041").unwrap();
        let dsv = is_tmq_valid(&dsn).await;
        assert!(!dsv.valid);
        assert!(!dsv.support);
        assert_eq!("tmq", dsv.data_source);
        assert!(dsv.message.is_some());
    }

    #[tokio::test]
    #[ignore]
    async fn test_replica() {
        let dsn = Dsn::from_str("tmq:///db1?replica&with.meta.delete").unwrap();
        let (dsn, _, topics, _, _) = check_tmq_dsn(dsn).await.unwrap();
        assert!(dsn.params.contains_key("msg.consume.excluded"));
        assert_eq!("1", dsn.params.get("msg.consume.excluded").unwrap());
        assert_eq!("replica", dsn.params.get("group.id").unwrap());
        assert_eq!("db1", topics[0].database);
    }

    /// example:
    /// ```shell
    /// cargo nextest run -p taosx-core test_check_tmq_dsn_with_taos --nocapture --retries 0
    /// ```
    #[tokio::test]
    async fn test_check_tmq_dsn_with_taos() -> anyhow::Result<()> {
        let host = std::env::var("HOST").unwrap_or(String::from("127.0.0.1"));
        let db = format!("test{}", Utc::now().timestamp());

        // create test database
        let dsn = format!("taos://{host}").into_dsn()?;
        let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;
        taos.exec(format!("create database if not exists {db}"))
            .await?;

        // when
        let dsn = format!("tmq://{host}/{db}?timeout=0&prefer=raw").into_dsn()?;
        let (dsn, _, _, _, _) = check_tmq_dsn(dsn).await?;
        assert_eq!("never", dsn.params.get("timeout").unwrap());

        // clean
        taos.exec(format!("drop topic if exists {db}")).await?;
        taos.exec(format!("drop database if exists {db}")).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid() {
        // TDengine 3.X at 192.168.1.92
        let dsn = Dsn::from_str("tmq+ws://192.168.1.92:6041/tmq_test?group.id=test_tmq_is_valid")
            .unwrap();
        let dsv = is_tmq_valid(&dsn).await;
        assert!(dsv.valid);
        assert!(dsv.support);
        assert_eq!("tmq", dsv.data_source);
        assert_eq!("3.1.1.3", dsv.version.unwrap());

        // TDengine 2.X at 192.168.1.40
        let dsn = Dsn::from_str("tmq+ws://192.168.1.40:6041/tmq_test?group.id=test_tmq_is_valid")
            .unwrap();
        let dsv = is_tmq_valid(&dsn).await;
        assert!(!dsv.valid);
        assert!(!dsv.support);
        assert_eq!("tmq", dsv.data_source);
        assert_eq!("failed to check dsn: tmq+ws://192.168.1.40:6041/tmq_test?group.id=test_tmq_is_valid, cause: tmq does not support TDengine Query", dsv.message.unwrap());

        // TDengine 3.X non-exist topic
        let dsn =
            Dsn::from_str("tmq+ws://192.168.1.92:6041/non_exist_topic?group.id=test_tmq_is_valid")
                .unwrap();
        let dsv = is_tmq_valid(&dsn).await;
        assert!(!dsv.valid);
        assert!(!dsv.support);
        assert_eq!("tmq", dsv.data_source);
        assert_eq!("failed to check dsn: tmq+ws://192.168.1.92:6041/non_exist_topic?group.id=test_tmq_is_valid, cause: unknown topic name: non_exist_topic", dsv.message.unwrap());
    }

    async fn drop_topic_and_database(taos: &Taos, topic: &str, database: &str) {
        let _ = taos.exec(format!("drop topic if exists {}", topic)).await;
        let _ = taos
            .exec(format!("drop database if exists {}", database))
            .await;

        // wait for the drop operation to take effect
        loop {
            if !taos.database_exists(database).await.unwrap() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    #[tokio::test]
    async fn test_create_topic_no_wal_with_taos() {
        let taos_builder = taos::TaosBuilder::from_dsn("taos://").unwrap();
        let taos = taos_builder.build().await.unwrap();

        const DB_NAME: &str = "create_topic_no_wal";

        drop_topic_and_database(&taos, DB_NAME, DB_NAME).await;

        // create database without wal, then create a topic
        let res = taos
            .exec_many(vec![
                format!("create database if not exists {DB_NAME} WAL_RETENTION_PERIOD 0"),
                format!("create topic if not exists {DB_NAME} with meta as database {DB_NAME}"),
            ])
            .await;

        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("WAL retention period is zero"));

        // clean
        drop_topic_and_database(&taos, DB_NAME, DB_NAME).await;
    }

    #[tokio::test]
    async fn test_alter_database_no_wal_with_taos() {
        let taos_builder = taos::TaosBuilder::from_dsn("taos://").unwrap();
        let taos = taos_builder.build().await.unwrap();

        const DB_NAME: &str = "alter_database_no_wal";

        // create a database, drop it first if exists
        drop_topic_and_database(&taos, DB_NAME, DB_NAME).await;
        let _ = taos
            .exec(format!("create database if not exists {DB_NAME}"))
            .await;

        loop {
            if taos.database_exists(DB_NAME).await.unwrap() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        // create a topic
        let _ = taos
            .exec(format!(
                "create topic if not exists {DB_NAME} with meta as database {DB_NAME}"
            ))
            .await;
        loop {
            let res: Option<String> =
                taos.query_one(format!("select topic_name from information_schema.ins_topics where topic_name = '{DB_NAME}'"))
                    .await
                    .unwrap();
            if res.is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        // alter database to disable wal, there should be an error
        let alter_database_res = taos
            .exec(format!("alter database {DB_NAME} wal_retention_period 0"))
            .await;

        // assert the result
        assert!(alter_database_res.is_err());
        assert!(alter_database_res
            .unwrap_err()
            .to_string()
            .contains("WAL retention period is zero"));

        // clear the test data
        drop_topic_and_database(&taos, DB_NAME, DB_NAME).await;
    }

    /// Test `check_wal_enabled` function
    ///
    /// only test the normal case, the error case is tested in `test_create_topic_no_wal` and `test_alter_database_no_wal`
    ///
    #[tokio::test]
    async fn test_check_wal_enabled_with_taos() {
        let taos_builder = taos::TaosBuilder::from_dsn("taos://").unwrap();
        let taos = taos_builder.build().await.unwrap();
        const DB_NAME: &str = "check_wal_enabled";

        // create a database, drop it first if exists
        drop_topic_and_database(&taos, DB_NAME, DB_NAME).await;
        let _ = taos
            .exec(format!("create database if not exists {DB_NAME}"))
            .await;

        // create a topic
        let _ = taos
            .exec(format!(
                "create topic if not exists {DB_NAME} with meta as database {DB_NAME}"
            ))
            .await;

        // check if wal is enabled
        let topics = vec![Topic {
            name: DB_NAME.to_string(),
            database: DB_NAME.to_string(),
            database_sql: None,
            vgroups: 2,
            table: None,
            use_table_name: None,
            topic_type: TopicType::DatabaseWithMeta,
        }];
        let res = check_wal_enabled(&taos_builder, &topics).await;

        // assert the result
        assert!(res.is_ok());

        // clear the test data
        drop_topic_and_database(&taos, DB_NAME, DB_NAME).await;
    }
}
