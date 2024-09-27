use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Days, FixedOffset, Utc};
use linked_hash_map::LinkedHashMap;
use oracle::sql_type::OracleType;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::runners::oracle::appender::to_schema;
use crate::runners::oracle::config::OracleConfig;
use crate::runners::oracle::query::OracleQuery;
use crate::runners::oracle::worker::consumer::Consumer;
use crate::runners::oracle::worker::producer::Producer;
use crate::utils::breakpoints;

mod consumer;
mod producer;

const MIGRATE_TASK_PREFIX: &str = "mig";

/// migrate data
pub async fn migrate_history(
    config: OracleConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // mark the current time
    let mut now = Utc::now();
    // origin task end
    let origin_end = config.task.end.clone();

    // if origin end is None, or origin end is greater than now, set end to now
    let mut config_clone = config.clone();
    if origin_end.is_none() || origin_end.unwrap() > now {
        config_clone.task.end = Some(now);
    }

    // create an instance of OracleQuery
    let query = OracleQuery::try_new(config.connect.clone(), config.task.time_zone.clone())?;

    // clone cancel for migrate history
    let cancel_clone = cancel.clone();
    // migrate history by subtable
    let future_migrate = migrate_history_by_subtable(config_clone, cancel.clone(), query.clone());
    tokio::select! {
        res = future_migrate => {
            if let Err(e) = res {
                tracing::error!("migrate oracle from history data error: {e}");
            }
        }
        _ = cancel_clone.cancelled() => {
            tracing::info!("Migrate cancelled");
            return Ok(());
        }
    };

    // clone cancel for sync live
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
                    "migrate oracle from live data, start: {}, end: {}",
                    real_start,
                    real_end
                );
                // create a new window
                let mut config_clone = config.clone();
                config_clone.task.start = real_start;
                config_clone.task.end = Some(real_end);
                let migrate_result =
                    migrate_history_by_subtable(config_clone, cancel_clone.clone(), query.clone())
                        .await;
                if let Err(e) = migrate_result {
                    tracing::error!("migrate oracle from live data error: {e}");
                }
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

    tracing::info!("migrate oracle finished");
    Ok(())
}

/// migrate data
pub async fn migrate_history_by_subtable(
    config: OracleConfig,
    cancel: CancellationToken,
    query: OracleQuery,
) -> anyhow::Result<()> {
    // clone config
    let config_clone = config.clone();
    // additional filters, get distinct values
    let filters = get_all_distinct_values(&config_clone, query.clone())?;

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
    // create a semaphore to limit the number of concurrent requests
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut migrate_join_set = JoinSet::new();
    for sub_sql in combinations {
        let semaphore = semaphore.clone();
        // Acquire permit before sending request.
        let _permit = semaphore.acquire_owned().await.unwrap();
        // modify config and produce task
        let mut config_clone = config.clone();
        config_clone.task.sql = sub_sql.sql;
        config_clone.sub_task_id = Some(format!("{MIGRATE_TASK_PREFIX}-{}", sub_sql.sub_values));
        // clone cancel and query
        let cancel_clone = cancel.clone();
        let query_clone = query.clone();
        // spawn migrate task
        migrate_join_set.spawn(async move {
            // do migrate, if fails, retry 3 times by sleeping 1 second each time
            for i in 1..4 {
                let migrate_result = migrate_history_by_interval(config_clone.clone(), cancel_clone.clone(), query_clone.clone()).await;
                match migrate_result {
                    Ok(_) => break,
                    Err(e) => {
                        tracing::warn!(
                            "migrate oracle, migrate history by interval failed, cause: {}, retrying {i} times...",
                            e
                        );
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
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
                tracing::error!("migrate oracle, do migrate runtime error: {err:#}");
            }
            return res;
        },
        _ = cancel.cancelled() => {}
    };

    Ok(())
}

/// migrate data
pub async fn migrate_history_by_interval(
    mut config: OracleConfig,
    cancel: CancellationToken,
    mut query: OracleQuery,
) -> anyhow::Result<()> {
    // schema
    let sql = config.task.generate_sql()?;
    let col_map = query.select_for_schema(&sql)?;
    let schema = to_schema(col_map)?;
    tracing::debug!("migrate oracle, schema: {:?}", schema);

    // get break point
    let breakpoint = get_breakpoint(config.task_id, &config.sub_task_id.clone().unwrap());
    if breakpoint.is_some() {
        config.task.start = breakpoint.unwrap();
        tracing::info!("migrate oracle from breakpoint: {}", config.task.start);
    }
    tracing::info!("migrate oracle start, config: {:?}", config);

    let (tx, rx) = flume::bounded(0);
    let config_clone = config.clone();
    // consumer
    let consumer = tokio::spawn(async move {
        Consumer::new(config_clone, schema, query.clone())
            .consume(rx)
            .await
    });
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

pub fn get_all_distinct_values(
    config: &OracleConfig,
    mut query: OracleQuery,
) -> anyhow::Result<Vec<HashMap<String, String>>> {
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
            let values = query.select_distinct_values(&distinct_sql);
            let (col_map, values) = match values {
                Ok(values) => values,
                Err(e) => {
                    tracing::error!("get distinct values error: {}", e);
                    (LinkedHashMap::new(), Vec::new())
                }
            };
            // transform to string set
            filters.extend(
                values
                    .iter()
                    .map(|v| {
                        // parse row to HashMap
                        v.sql_values()
                            .iter()
                            .enumerate()
                            .filter_map(|(col_cidx, col)| {
                                let col_name = col_map
                                    .iter()
                                    .nth(col_cidx)
                                    .map(|(key, _)| key.clone())
                                    .unwrap_or_default();
                                let col_type = col.oracle_type();
                                let col_val = col.get::<String>();
                                if let (Ok(col_type), Ok(col_val)) = (col_type, col_val) {
                                    match col_type {
                                        OracleType::Varchar2(_)
                                        | OracleType::NVarchar2(_)
                                        | OracleType::Char(_)
                                        | OracleType::NChar(_)
                                        | OracleType::Rowid
                                        | OracleType::Raw(_) => Some((
                                            col_name.clone(),
                                            format!("{}='{}'", col_name, col_val),
                                        )),
                                        OracleType::BinaryFloat
                                        | OracleType::BinaryDouble
                                        | OracleType::Number(_, _)
                                        | OracleType::Float(_)
                                        | OracleType::Int64
                                        | OracleType::UInt64 => Some((
                                            col_name.clone(),
                                            format!("{}={}", col_name, col_val),
                                        )),
                                        _ => None,
                                    }
                                } else {
                                    None
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
    config: &OracleConfig,
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
    use crate::runners::oracle::config::connect::ConnectConfig;

    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

    fn test_create_table() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                let sql_create_table = "create table t_metric (id NUMBER(10, 0) PRIMARY KEY, name VARCHAR2(255), value NUMBER(10, 2), ts timestamp)";
                let x = conn.execute(sql_create_table, &[]);
                println!("create table: {:?}", x);
                let y = conn.commit();
                println!("commit: {:?}", y);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    fn test_insert_data(len: usize) {
        let _ = test_create_table();

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                for i in 0..len {
                    let sql_insert_data = format!("insert into t_metric (id, name, value, ts) values ({}, 'cpu', 0.8, sysdate)", i);
                    let _ = conn.execute(&sql_insert_data.as_str(), &[]);
                }
                let _ = conn.commit();
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    fn test_clear_data() {
        let _ = test_create_table();

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                let sql = "delete from t_metric where 1 = 1";
                let _ = conn.execute(sql, &[]);
                let _ = conn.commit();
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_migrate_history() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric&start=2024-03-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=5d&delay=0")
            .unwrap();
        let mut config = OracleConfig::from_dsn(&dsn).unwrap();
        config.task_id = Some(1);
        config.ipc_port = Some(6666);

        // let _ = migrate_history(config).await;
    }

    #[tokio::test]
    async fn test_get_all_distinct_values() {
        // prepare data
        let _ = test_create_table();
        let _ = test_clear_data();
        let _ = test_insert_data(4);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?subtable_fields=select distinct name,value from t_metric&sql=select * from t_metric&start=2021-01-01T00:00:00Z&end=2021-02-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let config = OracleConfig::from_dsn(&dsn).unwrap();
        let query =
            OracleQuery::try_new(config.connect.clone(), config.task.time_zone.clone()).unwrap();
        let filters = get_all_distinct_values(&config, query.clone()).unwrap();
        dbg!(filters);

        // clear data
        let _ = test_clear_data();
    }

    #[tokio::test]
    async fn test_set_breakpoint() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric&start=2021-01-01T00:00:00Z&end=2021-02-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let mut config = OracleConfig::from_dsn(&dsn).unwrap();

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
