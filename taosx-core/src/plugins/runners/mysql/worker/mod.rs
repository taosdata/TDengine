use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Days, FixedOffset, Utc};
use sqlx::{Column, Row, TypeInfo};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::runners::mysql::appender::to_schema;
use crate::runners::mysql::config::MySqlConfig;
use crate::runners::mysql::query::MySqlQuery;
use crate::runners::mysql::worker::consumer::Consumer;
use crate::runners::mysql::worker::producer::Producer;
use crate::utils::breakpoints;

mod consumer;
mod producer;

const MIGRATE_TASK_PREFIX: &str = "mig";

/// migrate data
pub async fn migrate_history(config: MySqlConfig, cancel: CancellationToken) -> anyhow::Result<()> {
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
                    "migrate mysql from live data, start: {}, end: {}",
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

    tracing::info!("migrate mysql finished");
    Ok(())
}

/// migrate data
pub async fn migrate_history_by_subtable(
    config: MySqlConfig,
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

    // generate combinations
    let mut combinations = HashSet::new();
    generate_combinations(&filters, &config.task.sql, &mut combinations);
    // if no distinct values, use the original sql
    if combinations.is_empty() {
        combinations.insert(SubSql {
            sql: config.task.sql.clone(),
            sub_values: String::new(),
        });
    }

    // migrate data by combinations
    let concurrency = cmp::max(config.advanced.read_concurrency.unwrap_or(1), 1);
    let cancel_clone = cancel.clone();
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut migrate_join_set = JoinSet::new();
    for sub_sql in combinations {
        let semaphore = semaphore.clone();
        // Acquire permit before sending request.
        let _permit = semaphore.acquire_owned().await.unwrap();
        // modify config and produce task
        let cancel_clone = cancel_clone.clone();
        let mut config_clone = config.clone();
        config_clone.task.sql = sub_sql.sql;
        config_clone.sub_task_id = Some(format!("{MIGRATE_TASK_PREFIX}-{}", sub_sql.sub_values));
        // spawn migrate task
        migrate_join_set.spawn(async move {
            // do migrate
            let _ = migrate_history_by_interval(config_clone, cancel_clone).await;
            // Drop the permit after the request has been sent.
            drop(_permit);
        });
    }
    let futures = async {
        while let Some(_) = migrate_join_set.join_next().await.transpose()? {}
        anyhow::Ok(())
    };

    tokio::select! {
        res = futures => {
            if let Err(err) = &res {
                tracing::error!("do migrate runtime error: {err:#}");
            }
            return res;
        },
        _ = cancel.cancelled() => {}
    };

    Ok(())
}

/// migrate data
pub async fn migrate_history_by_interval(
    mut config: MySqlConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // schema
    let mut query =
        MySqlQuery::try_new(config.connect.clone(), config.task.time_zone.clone()).await?;
    let sql = config.task.generate_sql()?;
    let row = query.select_one_for_schema(&sql).await?;
    let schema = match row {
        Some(row) => to_schema(row).await?,
        None => {
            return Ok(());
        }
    };
    tracing::debug!("schema: {:?}", schema);

    // get break point
    let breakpoint = get_breakpoint(config.task_id, &config.sub_task_id.clone().unwrap());
    if breakpoint.is_some() {
        config.task.start = breakpoint.unwrap();
        tracing::info!("migrate mysql from breakpoint: {}", config.task.start);
    }
    tracing::info!("migrate mysql start, config: {:?}", config);

    let (tx, rx) = flume::bounded(0);
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
    config: &MySqlConfig,
) -> anyhow::Result<Vec<HashMap<String, String>>> {
    // connect to database
    let mut query =
        MySqlQuery::try_new(config.connect.clone(), config.task.time_zone.clone()).await?;
    // additional filters, get distinct values
    let mut filters = Vec::new();

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

    let mut current_distinct_sql = String::new();

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

        // generate new table name
        let mut config = config.clone();
        config.task.start = window_start_with_tz.with_timezone(&Utc);
        config.task.end = Some(window_end_with_tz.with_timezone(&Utc));
        let distinct_sql = config.task.generate_distinct_sql()?;

        // get distinct values
        if !distinct_sql.is_empty() && current_distinct_sql != distinct_sql {
            let values = query.select_distinct_values(&distinct_sql).await;
            let values = match values {
                Ok(values) => values,
                Err(e) => {
                    tracing::error!("get distinct values error: {}", e);
                    Vec::new()
                }
            };
            // transform to string set
            filters.extend(
                values
                    .iter()
                    .map(|v| {
                        // parse row to HashMap
                        v.columns()
                            .iter()
                            .filter_map(|col| {
                                let col_cidx = col.ordinal();
                                let col_name = col.name().to_string();
                                let col_type = col.type_info().name();

                                match col_type {
                                    "TINYINT" => {
                                        let val = v.try_get::<Option<i8>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "TINYINT UNSIGNED" => {
                                        let val = v.try_get::<Option<u8>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "SMALLINT" => {
                                        let val = v.try_get::<Option<i16>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "SMALLINT UNSIGNED" => {
                                        let val = v.try_get::<Option<u16>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "MEDIUMINT" | "INT" => {
                                        let val = v.try_get::<Option<i32>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
                                        let val = v.try_get::<Option<u32>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "BIGINT" => {
                                        let val = v.try_get::<Option<i64>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "BIGINT UNSIGNED" => {
                                        let val = v.try_get::<Option<u64>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "FLOAT" => {
                                        let val = v.try_get::<Option<f32>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "DOUBLE" => {
                                        let val = v.try_get::<Option<f64>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDUIMTEXT"
                                    | "LONGTEXT" => {
                                        let val = v.try_get::<Option<String>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}='{}'", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    "YEAR" => {
                                        let val = v.try_get::<Option<u16>, _>(col_cidx);
                                        if let Ok(Some(col_value)) = val {
                                            Some((
                                                col_name.clone(),
                                                format!("{}={}", col_name, col_value),
                                            ))
                                        } else {
                                            None
                                        }
                                    }
                                    _ => None,
                                }
                            })
                            .collect::<HashMap<String, String>>()
                    })
                    .collect::<Vec<HashMap<String, String>>>(),
            );
            current_distinct_sql = distinct_sql;
        }
        // move the window
        window_start_with_tz = window_end_with_tz;
    }
    Ok(filters)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubSql {
    sql: String,
    sub_values: String,
}

fn generate_combinations(
    filters: &Vec<HashMap<String, String>>,
    template: &String,
    result: &mut HashSet<SubSql>,
) {
    filters.iter().for_each(|filter| {
        let mut distinct_values = BTreeMap::new();
        for (key, value) in filter.iter() {
            distinct_values.insert(key.as_str(), value.clone());
        }
        let mut filled_template = template.to_string();
        for (key, value) in distinct_values.iter() {
            filled_template = filled_template.replace(&format!("${{{}}}", key), &value.to_string());
        }
        result.insert(SubSql {
            sql: filled_template,
            sub_values: distinct_values
                .iter()
                .map(|(_, v)| format!("{}", v))
                .collect::<Vec<String>>()
                .join(","),
        });
    });
}

pub async fn set_breakpoint(
    config: &MySqlConfig,
    breakpoint: &DateTime<Utc>,
) -> anyhow::Result<()> {
    let task_id = format!("{}", config.task_id.unwrap_or(0));
    let sub_task_id = config.sub_task_id.clone().unwrap();
    let breakpoint = format!("{}", breakpoint.to_rfc3339());

    // set break point and ignore error
    let _ = breakpoints::breakpoints_set(&task_id, &sub_task_id, &breakpoint);
    Ok(())
}

fn get_breakpoint(task_id: Option<i64>, sub_task_id: &String) -> Option<DateTime<Utc>> {
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
            for (key, breakpoint) in breakpoints {
                if key.starts_with(format!("{MIGRATE_TASK_PREFIX}-{sub_task_id}").as_str()) {
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
    use crate::runners::mysql::ConnectConfig;
    use sqlx::Executor;
    use std::str::FromStr;
    use taos::Dsn;

    async fn test_create_database() {
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/information_schema").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_database = "create database if not exists test_taosx";
                let _ = query.pool.execute(sql_create_database).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_create_table() {
        let _ = test_create_database().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_table = "create table if not exists t_metric (id int primary key auto_increment, name varchar(255), value double, ts timestamp)";
                let _ = query.pool.execute(sql_create_table).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(len: usize) {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_insert_data =
                    "insert into t_metric (name, value, ts) values ('cpu', 0.8, now())";
                for _ in 0..len {
                    let _ = query.pool.execute(sql_insert_data).await;
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data() {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql = "delete from t_metric where 1 = 1";
                let _ = query.pool.execute(sql).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_migrate_history() {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?sql=select * from t_metric&start=2024-03-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=5d&delay=0")
            .unwrap();
        let mut config = MySqlConfig::from_dsn(&dsn).unwrap();
        config.task_id = Some(1);
        config.ipc_port = Some(6666);

        // let _ = migrate_history(config).await;
    }

    #[tokio::test]
    async fn test_get_all_distinct_values() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(4).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?subtable_fields=select distinct name,value from t_metric&sql=select * from t_metric&start=2024-03-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=5d&delay=0")
            .unwrap();
        let config = MySqlConfig::from_dsn(&dsn).unwrap();
        let filters = get_all_distinct_values(&config).await.unwrap();
        dbg!(filters);

        // clear data
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    async fn test_generate_combinations() {
        let mut filters = Vec::new();
        let mut map = HashMap::new();
        map.insert("name".to_string(), "name='cpu'".to_string());
        map.insert("value".to_string(), "value=0.8".to_string());
        filters.push(map);
        let mut map = HashMap::new();
        map.insert("name".to_string(), "name='mem'".to_string());
        map.insert("value".to_string(), "value=0.2".to_string());
        filters.push(map);

        let template = "select * from t_metric where ${name} and ${value}".to_string();
        let mut result = HashSet::new();
        generate_combinations(&filters, &template, &mut result);

        result.iter().for_each(|sub_sql| {
            println!("sql: {}, sub_values: {}", sub_sql.sql, sub_sql.sub_values);
        });
    }

    #[tokio::test]
    async fn test_set_breakpoint() {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?sql=select * from t_metric&start=2021-01-01T00:00:00Z&end=2021-02-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let mut config = MySqlConfig::from_dsn(&dsn).unwrap();

        config.task_id = Some(1);
        config.sub_task_id = Some(format!(
            "mig-{}-1",
            config.sub_task_id.unwrap_or("sub_task_id".to_string())
        ));
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
