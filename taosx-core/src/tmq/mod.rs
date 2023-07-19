use std::{
    fmt::Display,
    ops::{AddAssign, SubAssign},
    str::FromStr,
    sync::atomic::{AtomicU16, AtomicU64},
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use taos::*;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct TopicTable {
    pub(crate) stable: Option<String>,
    pub(crate) stable_sql: Option<String>,
    pub(crate) table: String,
    pub(crate) table_sql: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TopicType {
    Database,
    DatabaseWithMeta,
    Stable,
    StableWithMeta,
    Query,
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
    // fn is_query(&self) -> bool {
    //     matches!(self, TopicType::Query)
    // }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Topic {
    pub(crate) name: String,
    pub(crate) database: String,
    pub(crate) vgroups: usize,
    pub(crate) database_sql: Option<String>,
    #[serde(flatten)]
    pub(crate) table: Option<TopicTable>,
    pub(crate) topic_type: TopicType,
    pub(crate) use_table_name: Option<String>,
}

impl Topic {
    pub fn is_query(&self) -> bool {
        matches!(self.topic_type, TopicType::Query)
    }
}

#[derive(Debug)]
pub(crate) struct TmqMetrics {
    pub topics: usize,
    pub workers: AtomicU16,
    pub messages: AtomicU64,
    pub messages_of_meta: AtomicU64,
    pub messages_of_data: AtomicU64,
    pub blocks: AtomicU64,
    pub records: AtomicU64,
    pub points: AtomicU64,
    pub time_cost: Instant,
}

impl Default for TmqMetrics {
    fn default() -> Self {
        Self {
            topics: Default::default(),
            workers: Default::default(),
            messages: Default::default(),
            messages_of_meta: Default::default(),
            messages_of_data: Default::default(),
            blocks: Default::default(),
            records: Default::default(),
            points: Default::default(),
            time_cost: Instant::now(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) enum StopAt {
    #[default]
    Now,
    Backward(Duration),
    Forward(Duration),
    At(chrono::DateTime<Local>),
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum StopAtError {
    #[error(transparent)]
    DurationParseError(#[from] parse_duration::parse::Error),
    #[error(transparent)]
    DateTimeParseError(#[from] chrono::ParseError),
}
impl StopAt {
    pub fn to_local_date_time(&self) -> chrono::DateTime<Local> {
        let mut now = Local::now();
        match self {
            StopAt::Now => now,
            StopAt::Backward(duration) => {
                now.sub_assign(chrono::Duration::from_std(*duration).unwrap());
                now
            }
            StopAt::Forward(duration) => {
                now.add_assign(chrono::Duration::from_std(*duration).unwrap());
                now
            }
            StopAt::At(dt) => *dt,
        }
    }
}

impl FromStr for StopAt {
    type Err = StopAtError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let at = StopAt::Now;
        match s {
            "" | "0" | "now" => Ok(at),
            s if s.starts_with('-') => {
                let s = s.trim_start_matches('-');
                let d = parse_duration::parse(s)?;
                Ok(Self::Backward(d))
            }
            s if s.starts_with('+') => {
                let s = s.trim_start_matches('+');
                let d = parse_duration::parse(s)?;
                Ok(Self::Forward(d))
            }
            s => {
                let d = chrono::DateTime::parse_from_rfc3339(s)?;
                Ok(Self::At(d.into()))
            }
        }
    }
}

// pub(crate) struct TmqExtraOpts {
//     stop_at: StopAt,
// }
// impl TmqMetrics {
//     pub fn new() -> Self {
//         Self {
//             time_cost: Instant::now(),
//             ..Default::default()
//         }
//     }
// }

impl Display for TmqMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering::SeqCst;
        let records = self.records.load(SeqCst);
        let points = self.points.load(SeqCst);
        let cost = self.time_cost.elapsed();
        write!(
            f,
            "# Metrics\n\
            topics: {}\n\
            workers: {}\n\
            messages(total): {}\n\
            messages(meta only): {}\n\
            messages(data only): {}\n\
            blocks: {}\n\
            records: {} ({} r/s)\n\
            points: {} ({} p/s)\n\
            time cost: {:?}",
            self.topics,
            self.workers.load(std::sync::atomic::Ordering::SeqCst),
            self.messages.load(std::sync::atomic::Ordering::SeqCst),
            self.messages_of_meta
                .load(std::sync::atomic::Ordering::SeqCst),
            self.messages_of_data
                .load(std::sync::atomic::Ordering::SeqCst),
            self.blocks.load(std::sync::atomic::Ordering::SeqCst),
            records,
            records / cost.as_secs(),
            points,
            points / cost.as_secs(),
            self.time_cost.elapsed()
        )?;
        Ok(())
    }
}
/// Parse input dsn, returns subscription dsn and a list of topics.
///
/// Steps:
/// 1. extract topics from dsn.
/// 2. check each name in topic list.
/// 3. if the name is in topic list, return topic itself.
/// 4. if the name is not a topic.
///     4.1 if the name is a database, create a database topic with meta as is.
///     4.2 else, if the name is in `database.table` format,
///     4.3       then if the `table` is STable, create a topic named `database_table` with meta as stable.
///     4.4            if the `table` is child table or normal, create a topic named `database_table` as select * from table.
///     4.5            else, bail unexpected input topics error to upstream.
pub(crate) async fn check_tmq_dsn(mut from: Dsn) -> Result<(Dsn, TaosBuilder, Vec<Topic>)> {
    // let origin = from.clone();
    let database = from.subject.take().ok_or(RawError::new(
        Code::FAILED,
        format!("requires topic or database in source dsn: {from}"),
    ))?;
    // dbg!(&from, &database);
    let use_topic_name = from.remove("use.topic.name");
    let use_table_name = from.remove("use.table.name");
    if from.get("timeout").is_none() {
        from.set("timeout", "5s");
    }

    let builder = TaosBuilder::from_dsn(&from)?;
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
        source.create_topic_as_database(&topic, &database).await?;

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
                log::warn!("SHOW CREATE DATABASE `{}` error: {}", database, err);
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
        ));
    }
    let source_topics = source.topics().await?;
    let mut databases = Vec::new();
    if topics.len() == 1 {
        let topic = topics.get(0).unwrap();
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
                    log::warn!("SHOW CREATE DATABASE `{}` error: {}, so that we can't automatically create a same database", topic.db_name(), err);
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
                    topic_type: TopicType::from_sql(&topic.sql()),
                }],
            ))
        } else if source
            .database_exists(&topic)
            .await
            .context(format!("check database exists: {topic}"))?
        {
            // treat it as database if the topic not exists.
            let database = topic;
            source.create_topic_as_database(topic, database).await?;

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
                    log::warn!("SHOW CREATE DATABASE `{}` error: {}", database, err);
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
                                log::warn!(
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
                        ));
                    } else {
                        source
                            .exec(format!(
                                "create topic {topic} with meta as stable `{database}`.`{table}`"
                            ))
                            .await
                            .context(format!("create topic for stable {database}"))?;
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
                                log::warn!("SHOW CREATE DATABASE `{}` error: {}", database, err);
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
                                log::warn!(
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
                                "create topic {topic} as select * from `{database}`.`{table}`"
                            ))
                            .await
                            .context(format!("create topic for stable {database}"))?;

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
                                log::warn!("SHOW CREATE DATABASE `{}` error: {}", database, err);
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
            .filter(|t| {
                topics
                    .iter()
                    .find(|name| name.as_str() == t.name())
                    .is_some()
            })
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
                    log::warn!("SHOW CREATE DATABASE `{}` error: {}", topic.db_name(), err);
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
            return Ok((from, builder, out));
        } else {
            let invalids = topics
                .into_iter()
                .filter(|t| out.iter().find(|topic| topic.name == *t).is_none())
                .collect_vec();
            for topic in invalids {
                if !source
                    .database_exists(&topic)
                    .await
                    .context(format!("check database exists: {topic}"))?
                {
                    anyhow::bail!("{} is not either a topic or a database name", topic);
                } else {
                    source.create_topic_as_database(&topic, &topic).await?;
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
                            log::warn!("SHOW CREATE DATABASE `{}` error: {}", topic, err);
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
            return Ok((from, builder, out));
        }
    }
}

pub(crate) fn group_id_hash(from: &Dsn, to: &Dsn) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(from.to_string());
    hasher.update(to.to_string());
    let id = hasher.finalize();
    let mut group_id = format!("x{:x}", id);
    group_id.truncate(12);
    group_id
}
