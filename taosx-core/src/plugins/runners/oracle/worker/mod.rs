use std::cmp;

use chrono::{DateTime, Utc};
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
    mut config: OracleConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // mark the current time
    let now = Utc::now();

    // schema
    let mut query = OracleQuery::try_new(config.connect.clone(), config.task.time_zone.clone())?;
    let sql = config.task.generate_sql()?;
    let col_map = query.select_for_schema(&sql)?;
    let schema = to_schema(col_map)?;
    tracing::debug!("schema: {:?}", schema);

    // get break point
    let breakpoint = get_breakpoint(config.task_id);
    if breakpoint.is_some() {
        config.task.start = breakpoint.unwrap();
        tracing::info!("migrate oracle from breakpoint: {}", config.task.start);
    }
    tracing::info!("migrate oracle start, config: {:?}", config);

    let (tx, rx) = flume::bounded(0);
    let concurrency = cmp::max(config.advanced.read_concurrency.unwrap_or(1), 1);
    // consume task
    let mut consumers = Vec::new();
    for sub_task_index in 1..=concurrency {
        let receiver = rx.clone();
        let mut config_clone = config.clone();
        let schema_clone = schema.clone();
        // set sub task id
        config_clone.sub_task_id = Some(format!("{MIGRATE_TASK_PREFIX}-{sub_task_index}"));

        // consumer
        let consumer = tokio::spawn(async move {
            Consumer::new(config_clone, schema_clone)
                .consume(receiver)
                .await
        });
        consumers.push(consumer);
    }

    // sync live data, if end is None
    if config.task.end.is_none() {
        let config_live = config.clone();
        let tx_live = tx.clone();
        // from 'now' marked by the beginning of the task
        let mut real_start = now - config_live.task.delay;
        // loop to produce task
        let future = async move {
            loop {
                let real_end = Utc::now() - config_live.task.delay;
                // every 10 seconds
                if real_end - real_start > chrono::Duration::seconds(10) {
                    tracing::trace!(
                        "migrate oracle from live data, start: {}, end: {}",
                        real_start,
                        real_end
                    );
                    // create a new window
                    let mut config_clone = config_live.clone();
                    config_clone.task.start = real_start;
                    config_clone.task.end = Some(real_end);

                    // produce task
                    let producer = Producer::new(&config_clone);
                    let _ = producer.produce(tx_live.clone()).await;

                    // move the window
                    real_start = real_end;
                }
                // sleep 2 second
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        };
        tokio::select! {
            _ = future => {}
            _ = cancel.cancelled() => {}
        };
    }

    // produce task
    let producer = Producer::new(&config);
    let _ = producer.produce(tx).await?;

    // consumer join
    for consumer in consumers {
        consumer.await??;
    }

    tracing::info!("migrate oracle finished");
    Ok(())
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

fn get_breakpoint(task_id: Option<i64>) -> Option<DateTime<Utc>> {
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
                if sub_task_id.starts_with(MIGRATE_TASK_PREFIX) {
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
    async fn test_migrate_history() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?sql=select * from TEST&start=2024-03-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=5d&delay=0")
            .unwrap();
        let mut config = OracleConfig::from_dsn(&dsn).unwrap();
        config.task_id = Some(1);
        config.ipc_port = Some(6666);

        // let _ = migrate_history(config).await;
    }

    #[tokio::test]
    async fn test_set_breakpoint() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?sql=select * from TEST&start=2021-01-01T00:00:00Z&end=2021-02-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let mut config = OracleConfig::from_dsn(&dsn).unwrap();

        config.task_id = Some(1);
        config.sub_task_id = Some("mig-1".to_string());
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
        let breakpoint = get_breakpoint(task_id);

        assert_eq!(
            DateTime::parse_from_rfc3339("2024-04-01T00:00:00Z")
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap(),
            breakpoint.unwrap()
        );
    }
}
