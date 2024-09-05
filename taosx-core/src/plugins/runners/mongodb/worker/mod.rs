use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Days, FixedOffset, Utc};
use mongodb::bson::Bson;
use std::collections::HashSet;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::runners::mongodb::appender::to_schema;
use crate::runners::mongodb::config::MongoDBConfig;
use crate::runners::mongodb::query::MongoDBQuery;
use crate::runners::mongodb::worker::consumer::Consumer;
use crate::runners::mongodb::worker::producer::Producer;
use crate::utils::breakpoints;

mod consumer;
mod producer;

const MIGRATE_TASK_PREFIX: &str = "mig";

/// migrate data
pub async fn migrate_history(
    config: MongoDBConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // mark the current time
    let mut now = Utc::now();
    // origin task end
    let origin_end = config.task.end.clone();

    let mut config_clone = config.clone();
    let cancel_clone = cancel.clone();
    // if origin end is None, or origin end is greater than now, set end to now
    if origin_end.is_none() || origin_end.unwrap() > now {
        config_clone.task.end = Some(now);
    }
    // migrate history by subtable
    let future_migrate = migrate_history_by_subtable(config_clone, cancel_clone);
    let cancel_clone = cancel.clone();
    tokio::select! {
        res = future_migrate => {
            res?;
        }
        _ = cancel_clone.cancelled() => {
            tracing::info!("Migrate cancelled");
            return Ok(());
        }
    };

    let cancel_clone = cancel.clone();
    // sync live data
    let future_sync = async move {
        // loop to migrate until the end of the task
        while origin_end.is_none() || origin_end.unwrap() > now {
            // from 'now' marked by the beginning of the task
            let real_start = now - config.task.delay;
            let real_end = Utc::now() - config.task.delay;
            // every 10 seconds
            if real_end - real_start > chrono::Duration::seconds(10) {
                tracing::trace!(
                    "migrate mongodb from live data, start: {}, end: {}",
                    real_start,
                    real_end
                );
                // create a new window
                let mut config_clone = config.clone();
                config_clone.task.start = real_start;
                config_clone.task.end = Some(real_end);
                let _ = migrate_history_by_subtable(config_clone, cancel_clone.clone()).await;
                // move the window
                now = real_end;
            }
            // sleep 2 second
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
        return ();
    };
    tokio::select! {
        _ = future_sync => {}
        _ = cancel.cancelled() => {
            tracing::info!("Migrate cancelled");
        }
    };

    tracing::info!("migrate mongodb finished");
    Ok(())
}

/// migrate data
pub async fn migrate_history_by_subtable(
    config: MongoDBConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // additional filters, get distinct values
    let config_clone = config.clone();
    let cancel_clone = cancel.clone();
    let filters;
    let future_get_distinct = get_all_distinct_values(&config_clone);
    tokio::select! {
        res = future_get_distinct => {
            filters = res?;
        }
        _ = cancel_clone.cancelled() => {
            return Ok(());
        }
    };

    // generate placeholders
    let mut placeholders = HashMap::new();
    for (k, v) in filters.iter() {
        let placeholder = config.task.subtable_fields.get(k);
        if let Some(placeholder) = placeholder {
            let vec = v
                .iter()
                .map(|v| placeholder.replace("${v}", v))
                .collect::<Vec<String>>();
            placeholders.insert(k, vec);
        }
    }
    // generate combinations
    let mut combinations = vec![];
    generate_combinations(
        &placeholders,
        &config.task.sql,
        0,
        BTreeMap::new(),
        &mut combinations,
    );

    // migrate data by combinations
    let concurrency = cmp::max(config.advanced.read_concurrency.unwrap_or(1), 1);
    let cancel_clone = cancel.clone();
    let future_migrate = async move {
        let semaphore = Arc::new(Semaphore::new(concurrency));
        for sub_sql in combinations {
            let semaphore = semaphore.clone();
            // Acquire permit before sending request.
            let _permit = semaphore.acquire_owned().await.unwrap();
            // modify config and produce task
            let cancel_clone = cancel_clone.clone();
            let mut config_clone = config.clone();
            config_clone.task.sql = sub_sql.sql;
            config_clone.sub_task_id =
                Some(format!("{MIGRATE_TASK_PREFIX}-{}", sub_sql.sub_values));
            let _ = tokio::spawn(async move {
                // do migrate
                let _ = migrate_history_by_interval(config_clone, cancel_clone).await;
                // Drop the permit after the request has been sent.
                drop(_permit);
            });
        }
    };
    tokio::select! {
        _ = future_migrate => {}
        _ = cancel.cancelled() => {}
    };

    Ok(())
}

/// migrate data
pub async fn migrate_history_by_interval(
    mut config: MongoDBConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // schema
    let schema = to_schema()?;
    tracing::debug!("schema: {:?}", schema);

    // get break point
    let breakpoint = get_breakpoint(config.task_id, &config.task.sql);
    if breakpoint.is_some() {
        config.task.start = breakpoint.unwrap();
        tracing::info!("migrate mongodb from breakpoint: {}", config.task.start);
    }
    tracing::info!("migrate mongodb start, config: {:?}", config);

    let (tx, rx) = flume::bounded(0);
    // set sub task id
    let config_clone = config.clone();
    // consumer
    let consumer =
        tokio::spawn(async move { Consumer::new(config_clone, schema).consume(rx).await });

    // produce task
    let producer = Producer::new(&config);
    let future_produce = producer.produce(tx);
    tokio::select! {
        _ = future_produce => {}
        _ = cancel.cancelled() => {
            return Ok(());
        }
    };

    // consumer join
    let future_consume = async move {
        consumer.await??;
        anyhow::Ok(())
    };
    tokio::select! {
        res = future_consume => {
            res?;
        }
        _ = cancel.cancelled() => {}
    };

    Ok(())
}

pub async fn get_all_distinct_values(
    config: &MongoDBConfig,
) -> anyhow::Result<HashMap<String, HashSet<String>>> {
    // connect to database
    let mut query = MongoDBQuery::try_new(config.connect.clone()).await?;
    // additional filters, get distinct values
    let mut filters = HashMap::new();

    let start = config.task.start;
    let end = match config.task.end {
        Some(end) => end,
        None => Utc::now(),
    };
    let time_zone = FixedOffset::from_str(&config.task.time_zone.to_string())?;
    let interval = config.task.interval;

    // split the query into multiple windows
    let window_start = start.clone();
    // with time zone
    let mut window_start_with_tz = window_start.with_timezone(&time_zone);
    let end_with_tz = end.with_timezone(&time_zone);

    let mut current_database = String::new();
    let mut current_collection = String::new();

    while window_start_with_tz < end_with_tz {
        // calculate the end of the window
        let mut window_end_with_tz = window_start_with_tz + interval;

        // when the window across days, we need to adjust the end to the start of the next day
        if window_end_with_tz.date_naive() > window_start_with_tz.date_naive() {
            window_end_with_tz = window_start_with_tz
                .date_naive()
                .checked_add_days(Days::new(1))
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_local_timezone(time_zone)
                .unwrap();
        }

        // generate new database and collection
        let mut config = config.clone();
        config.task.start = window_start_with_tz.with_timezone(&Utc);
        config.task.end = Some(window_end_with_tz.with_timezone(&Utc));
        let database = config.task.generate_database()?;
        let collection = config.task.generate_collection()?;

        if current_database != database || current_collection != collection {
            // get distinct values
            if config.task.subtable_fields.len() > 0 {
                // such as: {\"sys_sn\":\"\\\"sys_sn\\\":${v}\",\"sys_so\":\"\\\"sys_so\\\":${v}\"}, use sys_sn as distinct
                for (k, _) in config.task.subtable_fields.iter() {
                    let values = query
                        .select_distinct_values(&database, &collection, k)
                        .await?;
                    // transform to string set
                    let values = values
                        .iter()
                        .map(|v| match v {
                            Bson::String(s) => format!("\"{}\"", s),
                            Bson::Double(d) => d.to_string(),
                            Bson::Int32(i) => i.to_string(),
                            Bson::Int64(i) => i.to_string(),
                            Bson::Boolean(b) => b.to_string(),
                            _ => "".to_string(),
                        })
                        .collect::<HashSet<String>>();
                    filters
                        .entry(k.clone())
                        .or_insert(HashSet::new())
                        .extend(values.clone());
                }
            }
            current_database = database;
            current_collection = collection;
        }

        // move the window
        window_start_with_tz = window_end_with_tz;
    }
    Ok(filters)
}

struct SubSql {
    sql: String,
    sub_values: String,
}

fn generate_combinations(
    data: &HashMap<&String, Vec<String>>,
    template: &String,
    index: usize,
    current_values: BTreeMap<&str, String>,
    result: &mut Vec<SubSql>,
) {
    if index == data.len() {
        let mut filled_template = template.to_string();
        for (key, value) in current_values.iter() {
            filled_template = filled_template.replace(&format!("${{{}}}", key), &value.to_string());
        }
        result.push(SubSql {
            sql: filled_template,
            sub_values: current_values
                .iter()
                .map(|(_, v)| format!("{}", v))
                .collect::<Vec<String>>()
                .join(","),
        });
        return;
    }

    let keys: Vec<&String> = data.keys().cloned().collect();
    let current_key = keys[index];

    for value in &data[current_key] {
        let mut new_values = current_values.clone();
        new_values.insert(current_key, value.clone());
        generate_combinations(data, template, index + 1, new_values, result);
    }
}

pub async fn set_breakpoint(
    config: &MongoDBConfig,
    breakpoint: &DateTime<Utc>,
) -> anyhow::Result<()> {
    let task_id = format!("{}", config.task_id.unwrap_or(0));
    let sub_task_id = config.sub_task_id.clone().unwrap();
    let breakpoint = format!("{}", breakpoint.to_rfc3339());

    // set break point and ignore error
    let _ = breakpoints::breakpoints_set(&task_id, &sub_task_id, &breakpoint);
    Ok(())
}

fn get_breakpoint(task_id: Option<i64>, subtask_sql: &String) -> Option<DateTime<Utc>> {
    // get break point by task_id, if not found, return None
    if task_id.is_none() {
        return None;
    }
    // get all break points by task_id
    let breakpoints = breakpoints::breakpoints_get_all(&format!("{}", task_id.unwrap()));
    // find the earliest break point
    match breakpoints {
        Ok(breakpoints) => {
            let mut earliest = None;
            for (sub_task_id, breakpoint) in breakpoints {
                if sub_task_id.starts_with(format!("{MIGRATE_TASK_PREFIX}-{subtask_sql}").as_str())
                {
                    // parse breakpoint to DateTime
                    let date_time = DateTime::parse_from_rfc3339(&breakpoint)
                        .map(|dt| Some(dt.with_timezone(&Utc)))
                        .unwrap_or(None);
                    // find the earliest break point
                    if date_time.is_some() {
                        earliest = Some(cmp::min(
                            earliest.unwrap_or(date_time.unwrap()),
                            date_time.unwrap(),
                        ));
                    }
                }
            }
            earliest
        }
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

    #[tokio::test]
    #[ignore]
    async fn test_migrate_history() {
        let dsn = Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql_placeholder={\"double\":\"\\\"double\\\":${v}\",\"string\":\"\\\"string\\\":${v}\"}&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}},${double},${string}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let mut config = MongoDBConfig::from_dsn(&dsn).unwrap();
        config.task_id = Some(1);
        config.ipc_port = Some(6666);

        // let _ = migrate_history(config).await;
    }

    #[tokio::test]
    async fn test_get_all_distinct_values() {
        let dsn = Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql_placeholder={\"double\":\"{\\\"double\\\":${v}}\",\"string\":\"{\\\"string\\\":${v}}\"}&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=1970-01-01T00:00:00+00:00&end=2024-09-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        let filters = get_all_distinct_values(&config).await.unwrap();
        dbg!(filters);
    }

    #[tokio::test]
    async fn test_set_breakpoint() {
        let dsn = Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let mut config = MongoDBConfig::from_dsn(&dsn).unwrap();

        config.task_id = Some(1);
        config.sub_task_id = Some(format!("mig-{}-1", config.task.sql));
        let breakpoint = DateTime::parse_from_rfc3339("2024-04-01T00:00:00Z")
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap();

        let result = set_breakpoint(&config, &breakpoint);
        assert_eq!(result.await.is_ok(), true);
    }

    #[test]
    fn test_get_breakpoint() {
        // set breakpoint on 2024-04-01T00:00:00Z
        test_set_breakpoint();
        // get breakpoint
        let task_id = Some(1);
        let breakpoint = get_breakpoint(task_id, &String::new());

        if breakpoint.is_some() {
            assert_eq!(
                DateTime::parse_from_rfc3339("2024-04-01T00:00:00Z")
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap(),
                breakpoint.unwrap()
            );
        }
    }
}
