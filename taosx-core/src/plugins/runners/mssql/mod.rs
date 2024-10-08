use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveTime;
use chrono::{DateTime, FixedOffset, NaiveDate};
use linked_hash_map::LinkedHashMap;
use serde_json::json;
use taos::Dsn;
use tiberius::ColumnData;
use tokio_util::sync::CancellationToken;

use crate::dsv::DataSourceValidation;
use crate::plugins::transform::sample::DsSampleIn;
use crate::runners::mssql::appender::column_meta::ColumnMeta;
use crate::runners::mssql::config::connect::ConnectConfig;
use crate::runners::mssql::config::MssqlConfig;
use crate::runners::mssql::query::MssqlQuery;
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

use self::worker::migrate_history;

mod appender;
mod config;
mod query;
mod worker;

pub const MSSQL_ID: &str = "mssql";
pub const MSSQL_NAME: &str = "Mssql";

/// check mssql dsn is valid
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            MSSQL_ID.to_string(),
            format!("invalid dsn: {}, cause: {}", dsn, err),
        ),
        Ok(c) => {
            let result = MssqlQuery::try_new(c, String::from("+08:00")).await;
            match result {
                Err(err) => DataSourceValidation::invalid(
                    MSSQL_ID.to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                ),
                Ok(_cli) => DataSourceValidation::valid(MSSQL_ID.to_string(), None),
            }
        }
    }
}

/// get sample data from mssql
/// # Arguments
/// * `dsn` - mssql dsn
/// # Returns
/// * `DsSampleIn` - {
///     "input": [{ "col_name": "xxx", ... }],
///     "parser": {"parse": {
///         "col_name": { "as": col_type }, ...
///     }}
///   }
pub async fn get_sample(dsn: &Dsn) -> anyhow::Result<DsSampleIn> {
    // create mssql query
    let mut config = MssqlConfig::from_dsn(dsn)?;
    let mut query = MssqlQuery::try_new(config.connect, config.task.time_zone.clone()).await?;

    // results
    let mut input_sample: Vec<LinkedHashMap<String, serde_json::Value>> = Vec::new();
    let mut parse_sample: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();

    // replace subtable fields
    let distinct_sql = config.task.generate_distinct_sql()?;
    let values = if !distinct_sql.is_empty() {
        query.select_for_schema(&distinct_sql).await?
    } else {
        LinkedHashMap::new()
    };
    values.iter().for_each(|(key, _)| {
        config.task.sql = config
            .task
            .sql
            .replace(&format!("${{{}}}", key), &format!("{} is not null", key));
    });

    // generate sql
    let sql = config.task.generate_sql()?;
    tracing::info!(
        "get sample data, sql: {}, limit: {}",
        sql,
        config.task.sample_data_limit
    );

    // query sample data
    let (col_map, rows) = query.top_n(&sql, config.task.sample_data_limit).await?;

    if rows.is_empty() {
        return Err(anyhow::anyhow!("no data found"));
    }

    // generate sample data
    for row in rows {
        let mut sample_map: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();
        for (col_cidx, col) in row.into_iter().enumerate() {
            let (col_name, _) = col_map.iter().nth(col_cidx).unwrap();
            let col_val = generate_json_value(col, config.task.time_zone.clone())?;
            sample_map.insert(col_name.clone(), col_val);
        }
        input_sample.push(sample_map);
    }

    // generate parse data
    for (col_name, col_type) in col_map {
        let column_meta = ColumnMeta::try_new(col_name.to_string(), col_type)?;
        parse_sample.insert(
            col_name.to_string(),
            json!({"as": column_meta.get_ipc_type()?}),
        );
    }

    // generate sample data
    let sample_json = json!({
        "input": input_sample,
        "parser": {
            "parse": parse_sample,
        }
    });
    let ds_sample_in: DsSampleIn = serde_json::from_value(sample_json.clone()).map_err(|err| {
        anyhow::anyhow!(
            "failed to parse sample data, cause: {}, value: {:?}",
            err.to_string(),
            sample_json
        )
    })?;

    Ok(ds_sample_in)
}

/// migrate or synchronize data from mssql to taos

pub async fn mssql_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    _transform: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let mut config = MssqlConfig::from_dsn(&from)?;

    // set task_id
    config.task_id = task_id;
    tracing::info!(
        "{MSSQL_NAME} task start, id: {:?}, configuration: {:?}",
        task_id,
        config
    );

    // set ipc port
    let port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for connection"))?;
    let socket = format!("127.0.0.1:{}", port);
    config.ipc_port = Some(port.get());

    // create ipc handler
    let mut ipc = build_ipc(
        &socket,
        parser,
        &to,
        Some(MSSQL_ID),
        None,
        None,
        &cancel,
        with_agent,
        transferred,
        task_id,
        notify,
    )
    .await?;

    // create worker
    let worker = tokio::spawn(migrate_history(config, cancel.clone()));

    // execute worker
    let abort_handle = worker.abort_handle();
    tokio::spawn(async move {
        tokio::select! {
            status = worker => {
                match status? {
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        match ipc.try_recv_error() {
                            Ok(res) => {
                                tracing::error!("IPC Error: {res}");
                                anyhow::bail!("{MSSQL_NAME} exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("{MSSQL_NAME} done successfully");
                                let _ = ipc.send(());
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc.send(());
                        anyhow::bail!("{MSSQL_NAME} exit with error: {:#}", err);
                    }
                }
            },
            err = ipc.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                abort_handle.abort();
                if let Some(err) = err {
                    let _ = ipc.send(());
                    let _ = ipc.close().await;
                    abort_handle.abort();
                    anyhow::bail!("{MSSQL_NAME} writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("{MSSQL_NAME} task cancelled, id: {}", task_id.unwrap_or(-1));
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = ipc.send(());
        // stop the connector
        tracing::info!("{MSSQL_NAME} task done, id: {}", task_id.unwrap_or(-1));
        ipc.close().await?;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

fn generate_json_value(
    col: ColumnData<'static>,
    _time_zone: String,
) -> anyhow::Result<serde_json::Value> {
    match col {
        tiberius::ColumnData::U8(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(val)),
        },
        tiberius::ColumnData::I16(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(val)),
        },
        tiberius::ColumnData::I32(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(val)),
        },
        tiberius::ColumnData::I64(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(val)),
        },
        tiberius::ColumnData::F32(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(format!("{:?}", val))),
        },
        tiberius::ColumnData::F64(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(format!("{:?}", val))),
        },
        tiberius::ColumnData::Bit(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(format!("{:?}", val))),
        },
        tiberius::ColumnData::String(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(val)),
        },
        tiberius::ColumnData::Guid(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(format!("{:?}", val))),
        },
        tiberius::ColumnData::Binary(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(format!("{:?}", val))),
        },
        tiberius::ColumnData::Numeric(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(val.to_string().replace(".-", "."))),
        },
        tiberius::ColumnData::Xml(val) => match val {
            None => Ok(json!(null)),
            Some(val) => Ok(json!(format!("{:?}", val))),
        },
        tiberius::ColumnData::DateTime(val) => match val {
            None => Ok(json!(null)),
            Some(val) => {
                let days = val.days();
                let secs = val.seconds_fragments();
                // convert to seconds and nanoseconds(since 1900-01-01 00:00:00, and seconds are actually 1/300 seconds)
                let secs = (days as u32 - 25567) * 24 * 60 * 60 + secs / 300;
                // convert to datetime with timezone
                let datetime = DateTime::from_timestamp(secs as i64, 0_u32).unwrap();

                Ok(json!(format!("{:?}", datetime)))
            }
        },
        tiberius::ColumnData::SmallDateTime(val) => match val {
            None => Ok(json!(null)),
            Some(val) => {
                let days = val.days();
                let secs = val.seconds_fragments();
                // convert to seconds and nanoseconds(since 1900-01-01 00:00:00, and seconds are actually minutes)
                let secs = (days as i64 - 25567) * 24 * 60 * 60 + (secs as i64) * 60;
                // convert to datetime with timezone
                let datetime = DateTime::from_timestamp(secs, 0_u32).unwrap();

                Ok(json!(format!("{:?}", datetime)))
            }
        },
        tiberius::ColumnData::Time(val) => match val {
            None => Ok(json!(null)),
            Some(val) => {
                // convert to seconds and nanoseconds
                let secs = val.increments() / (10_u64.pow(val.scale() as u32));
                let nsecs = val.increments() % (10_u64.pow(val.scale() as u32))
                    * 10_u64.pow(9 - val.scale() as u32);
                let time = NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nsecs as u32)
                    .unwrap();

                Ok(json!(format!("{:?}", time)))
            }
        },
        tiberius::ColumnData::Date(val) => match val {
            None => Ok(json!(null)),
            Some(val) => {
                // convert to days(since 1st of January, year 1)
                let days = val.days() + 1;
                // convert to naivedate
                let date = NaiveDate::from_num_days_from_ce_opt(days as i32).unwrap();

                Ok(json!(format!("{:?}", date)))
            }
        },
        tiberius::ColumnData::DateTime2(val) => match val {
            None => Ok(json!(null)),
            Some(val) => {
                let date = val.date();
                let time = val.time();
                // convert to seconds and nanoseconds(since 1st of January, year 1)
                let secs = (date.days() as u64 - 719162) * 24 * 60 * 60
                    + time.increments() / (10_u64.pow(time.scale() as u32));
                let nsecs = time.increments() % (10_u64.pow(time.scale() as u32))
                    * 10_u64.pow(9 - time.scale() as u32);
                // convert to datetime with timezone
                let datetime = DateTime::from_timestamp(secs as i64, nsecs as u32).unwrap();

                Ok(json!(format!("{:?}", datetime)))
            }
        },
        tiberius::ColumnData::DateTimeOffset(val) => match val {
            None => Ok(json!(null)),
            Some(val) => {
                let datetime = val.datetime2();
                let date = datetime.date();
                let time = datetime.time();
                // convert to seconds and nanoseconds(since 1st of January, year 1)
                let secs = (date.days() as u64 - 719162) * 24 * 60 * 60
                    + time.increments() / (10_u64.pow(time.scale() as u32));
                let nsecs = time.increments() % (10_u64.pow(time.scale() as u32))
                    * 10_u64.pow(9 - time.scale() as u32);
                // get timezone
                let _offset = FixedOffset::east_opt((val.offset() as i32) * 60).unwrap();
                // convert to datetime(an accurate UTC time)
                let datetime = DateTime::from_timestamp(secs as i64, nsecs as u32).unwrap();

                Ok(json!(datetime))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    async fn test_create_database() {
        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/master?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_database = "create database test_taosx";
                let mut conn = query.pool.get().await.unwrap();
                let _ = conn.execute(sql_create_database, &[]).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_create_table() {
        let _ = test_create_database().await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_table = "create table t_metric (id bigint, name char(10), value float, ts datetimeoffset(7))";
                let mut conn = query.pool.get().await.unwrap();
                let x = conn.execute(sql_create_table, &[]).await;
                println!("create table: {:?}", x);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(len: usize) {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                for i in 0..len {
                    let sql_insert_data = format!("insert into t_metric (id, name, value, ts) values ({}, 'cpu', 0.8, GETDATE())", i);
                    let mut conn = query.pool.get().await.unwrap();
                    let _ = conn.execute(sql_insert_data, &[]).await;
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data() {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql = "delete from t_metric where 1 = 1";
                let mut conn = query.pool.get().await.unwrap();
                let _ = conn.execute(sql, &[]).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_is_valid() {
        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1432/master?encryption=On&trust_cert=true",
        )
        .unwrap();
        let res = is_valid(&dsn).await;
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!("mssql", res.data_source);
        assert_eq!(
            "failed to connect to dsn: mssql://test:123456@192.168.1.66:1432/master?encryption=On&trust_cert=true, cause: failed to connect to mssql, cause: Connection refused (os error 111)",
            res.message.unwrap()
        );

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/master?encryption=On&trust_cert=true",
        )
        .unwrap();
        let res = is_valid(&dsn).await;
        assert!(res.valid);
        assert!(res.support);
        assert_eq!("mssql", res.data_source);
    }

    #[tokio::test]
    async fn test_get_sample() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(4).await;

        let from = Dsn::from_str("mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true&sql=select * from t_metric where ts>=${start} and ts<${end}&start=2024-01-01T00:00:00Z&end=2024-06-01T00:00:00Z&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();

        let res = get_sample(&from).await;
        dbg!(&res);
        assert!(res.is_ok());
        // clear data
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_mssql_to_taos() {
        let from = Dsn::from_str("mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true&sql=select * from t_metric&start=2024-01-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let to = Dsn::from_str("taos://localhost:6030/ms").unwrap();
        let parser = None;
        let transform = vec![];
        let jobs = 1;
        let port_pool = PortPool::default();
        let cancel = CancellationToken::new();
        let with_agent = None;
        let transferred = None;
        let _span = tracing::info_span!("test_mssql_to_taos");
        let task_id = Some(1);
        let (notify, _) = flume::unbounded();

        mssql_to_taos(
            from,
            parser,
            transform,
            to,
            jobs,
            &port_pool,
            cancel,
            with_agent,
            transferred,
            task_id,
            notify,
        )
        .await
        .ok();
        // let _ = res.await;
    }

    #[tokio::test]
    async fn test_generate_json_value() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(4).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.select_all("select * from t_metric").await;
                match query_result {
                    Ok((_, rows)) => {
                        for row in rows {
                            for col in row.into_iter() {
                                let col_val = generate_json_value(col, String::from("+08:00"));
                                dbg!(&col_val);
                            }
                        }
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        let _ = test_clear_data().await;
    }
}
