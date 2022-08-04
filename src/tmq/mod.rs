use anyhow::Result;
use taos::*;

pub(crate) async fn check_tmq_dsn(mut from: Dsn, jobs: usize) -> Result<(Dsn, Vec<String>, usize)> {
    let database = from.database.take().ok_or(RawError::new(
        Code::Failed,
        format!("requires topic or database in source dsn: {from}"),
    ))?;

    let source = TaosBuilder::from_dsn(&from)?.build()?;
    let source_topics = source.topics().await?;

    let mut topics = database.split(",").map(|s| s.trim()).collect_vec();
    let mut vgroups = 0usize;
    let mut databases = Vec::new();
    if topics.len() == 1 {
        let topic = topics[0];
        if let Some(topic) = source_topics.iter().find(|t| t.name() == topic) {
            databases.push(topic.db_name().to_string());
            vgroups = source
                .query_one(format!(
                    "select `vgroups` from information_schema.ins_databases where name='{}'",
                    topic.db_name()
                ))
                .await?
                .expect("database not exists");
        } else {
            // treat it as database if the topic not exists.
            let database = topic;
            source.create_topic_as_database(topic, database).await?;

            vgroups = source
                .query_one(format!(
                    "select `vgroups` from information_schema.ins_databases where name='{database}'"
                ))
                .await?
                .expect("database not exists");
        }
    } else {
        let found = source_topics
            .iter()
            .filter(|t| topics.contains(&t.name()))
            .collect_vec();
        if topics.len() == found.len() {
            // ok;
            for topic in found {
                databases.push(topic.db_name().to_string());
            }
        } else {
            topics.retain(|t| found.iter().find(|topic| topic.name() == *t).is_none());
            anyhow::bail!("topics not found: {}", topics.join(","));
        }
    }

    let jobs = if jobs == 0 || jobs >= vgroups {
        vgroups
    } else {
        jobs
    };

    Ok((
        from,
        topics.into_iter().map(ToString::to_string).collect(),
        jobs,
    ))
}
