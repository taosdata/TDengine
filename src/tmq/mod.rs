use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use taos::*;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Topic {
    pub(crate) name: String,
    pub(crate) database: String,
    pub(crate) vgroups: usize,
    pub(crate) sql: String,
    /// the topic is for single table
    pub(crate) table: Option<String>,
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
pub(crate) async fn check_tmq_dsn(mut from: Dsn) -> Result<(Dsn, Vec<Topic>)> {
    let database = from.database.take().ok_or(RawError::new(
        Code::Failed,
        format!("requires topic or database in source dsn: {from}"),
    ))?;
    // dbg!(&from, &database);

    let source = TaosBuilder::from_dsn(&from)?.build()?;
    let source_topics = source.topics().await?;

    let topics = database.split(",").map(|s| s.trim()).collect_vec();
    let mut databases = Vec::new();
    if topics.len() == 1 {
        let topic = topics[0];
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
                .unwrap_or(0);

            let (_, sql): ((), String) = source
                .query_one(format!("SHOW CREATE DATABASE `{}`", topic.db_name()))
                .await?
                .unwrap();
            Ok((
                from,
                vec![Topic {
                    name: topic.name().to_string(),
                    database: topic.db_name().to_string(),
                    sql,
                    vgroups,
                    table: None,
                }],
            ))
        } else if source
            .database_exists(topic)
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
                .unwrap_or(0);

            let (_, sql): ((), String) = source
                .query_one(format!("SHOW CREATE DATABASE `{database}`"))
                .await?
                .unwrap();
            Ok((
                from,
                vec![Topic {
                    name: topic.to_string(),
                    database: database.to_string(),
                    sql,
                    vgroups,
                    table: None,
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

                        let (_, sql): ((), String) = source
                            .query_one(format!("SHOW CREATE DATABASE `{}`", topic.db_name()))
                            .await?
                            .unwrap();
                        return Ok((
                            from,
                            vec![Topic {
                                name: topic.name().to_string(),
                                database: topic.db_name().to_string(),
                                sql,
                                vgroups,
                                table: None,
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

                        let (_, sql): ((), String) = source
                            .query_one(format!("SHOW CREATE DATABASE `{database}`"))
                            .await?
                            .unwrap();
                        return Ok((
                            from,
                            vec![Topic {
                                name: topic,
                                database: database.to_string(),
                                sql,
                                vgroups,
                                table: None,
                            }],
                        ));
                    }
                }
                // check if is table
                let table_exists: Option<String> = source
                    .query_one(format!(
                        "select table_name from information_schema.ins_tables where db_name = '{}' and table_name = '{}'",
                        database, table
                    ))
                    .await?;
                if table_exists.is_some() {
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

                        let (_, sql): ((), String) = source
                            .query_one(format!("SHOW CREATE DATABASE `{}`", topic.db_name()))
                            .await?
                            .unwrap();
                        return Ok((
                            from,
                            vec![Topic {
                                name: topic.name().to_string(),
                                database: topic.db_name().to_string(),
                                sql,
                                vgroups,
                                table: Some(table.to_string()),
                            }],
                        ));
                    } else {
                        source
                            .exec(format!(
                                "create topic {topic} as select * from `{database}`.`{table}`"
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

                        let (_, sql): ((), String) = source
                            .query_one(format!("SHOW CREATE DATABASE `{database}`"))
                            .await?
                            .unwrap();
                        return Ok((
                            from,
                            vec![Topic {
                                name: topic,
                                database: database.to_string(),
                                sql,
                                vgroups,
                                table: Some(table.to_string()),
                            }],
                        ));
                    }
                }
                bail!("table does not exist: `{database}`.`{table}`");
            } else {
                bail!("database not exist: {database}");
            }
        } else {
            bail!(format!("unknown topic name: {topic}"))
        }
    } else {
        let found = source_topics
            .iter()
            .filter(|t| topics.contains(&t.name()))
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
            let (_, sql): ((), String) = source
                .query_one(format!("SHOW CREATE DATABASE `{}`", topic.db_name()))
                .await?
                .unwrap();

            out.push(Topic {
                name: topic.name().to_string(),
                database: topic.db_name().to_string(),
                sql,
                vgroups,
                table: None,
            });
        }
        if topics.len() == out.len() {
            // ok;
            return Ok((from, out));
        } else {
            let invalids = topics
                .into_iter()
                .filter(|t| out.iter().find(|topic| topic.name == *t).is_none())
                .collect_vec();
            for topic in invalids {
                if !source
                    .database_exists(topic)
                    .await
                    .context(format!("check database exists: {topic}"))?
                {
                    anyhow::bail!("{} is not either a topic or a database name", topic);
                } else {
                    source.create_topic_as_database(topic, topic).await?;
                    let vgroups = source
                        .query_one(format!(
                            "SELECT `vgroups` FROM information_schema.ins_databases WHERE name='{topic}'"
                        ))
                        .await?
                        .expect("database not exists");
                    let (_, sql): ((), String) = source
                        .query_one(format!("SHOW CREATE DATABASE `{topic}`"))
                        .await?
                        .unwrap();
                    out.push(Topic {
                        name: topic.to_string(),
                        database: topic.to_string(),
                        sql,
                        vgroups,
                        table: None,
                    });
                }
            }
            return Ok((from, out));
        }
    }
}

// /// Deal with a tmq input DSN, returns tmq subscription DSN and vector of topics.
// ///
// /// Steps:
// /// 1.
// pub(crate) async fn check_tmq_dsn(mut from: Dsn, jobs: usize) -> Result<(Dsn, Vec<String>, usize)> {
//     let database = from.database.take().ok_or(RawError::new(
//         Code::Failed,
//         format!("requires topic or database in source dsn: {from}"),
//     ))?;

//     let source = TaosBuilder::from_dsn(&from)?.build()?;
//     let source_topics = source.topics().await?;

//     let mut topics = database.split(",").map(|s| s.trim()).collect_vec();
//     let mut vgroups = 0usize;
//     let mut databases = Vec::new();
//     if topics.len() == 1 {
//         let topic = topics[0];
//         if let Some(topic) = source_topics.iter().find(|t| t.name() == topic) {
//             databases.push(topic.db_name().to_string());
//             vgroups = source
//                 .query_one(format!(
//                     "select `vgroups` from information_schema.ins_databases where name='{}'",
//                     topic.db_name()
//                 ))
//                 .await?
//                 .expect("database not exists");
//         } else {
//             // treat it as database if the topic not exists.
//             let database = topic;
//             source.create_topic_as_database(topic, database).await?;

//             vgroups = source
//                 .query_one(format!(
//                     "select `vgroups` from information_schema.ins_databases where name='{database}'"
//                 ))
//                 .await?
//                 .expect("database not exists");
//         }
//     } else {
//         let found = source_topics
//             .iter()
//             .filter(|t| topics.contains(&t.name()))
//             .collect_vec();
//         if topics.len() == found.len() {
//             // ok;
//             for topic in found {
//                 databases.push(topic.db_name().to_string());
//             }
//         } else {
//             topics.retain(|t| found.iter().find(|topic| topic.name() == *t).is_none());
//             anyhow::bail!("topics not found: {}", topics.join(","));
//         }
//     }

//     let jobs = if jobs == 0 || jobs >= vgroups {
//         vgroups
//     } else {
//         jobs
//     };

//     Ok((
//         from,
//         topics.into_iter().map(ToString::to_string).collect(),
//         jobs,
//     ))
// }

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
