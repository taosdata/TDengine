use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDateTime;
use linked_hash_map::LinkedHashMap;
use oracle::sql_type::OracleType;
use oracle::SqlValue;
use serde_json::json;
use taos::Dsn;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::dsv::DataSourceValidation;
use crate::plugins::transform::sample::DsSampleIn;
use crate::runners::oracle::appender::column_meta::ColumnMeta;
use crate::runners::oracle::config::connect::ConnectConfig;
use crate::runners::oracle::config::OracleConfig;
use crate::runners::oracle::query::OracleQuery;
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

use self::worker::migrate_history;

mod appender;
mod config;
mod query;
mod worker;

pub const ORACLE_ID: &str = "oracle";
pub const ORACLE_NAME: &str = "Oracle";

/// check oracle dsn is valid
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            ORACLE_ID.to_string(),
            format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let result = OracleQuery::try_new(c, String::from("+08:00"));
            match result {
                Err(err) => DataSourceValidation::invalid(
                    ORACLE_ID.to_string(),
                    format!(
                        "failed to connect to dsn: {}, cause: {}",
                        dsn.to_string(),
                        err.to_string()
                    ),
                ),
                Ok(_cli) => DataSourceValidation::valid(ORACLE_ID.to_string(), None),
            }
        }
    }
}

/// get sample data from oracle
/// # Arguments
/// * `dsn` - oracle dsn
/// # Returns
/// * `DsSampleIn` - {
///     "input": [{ "col_name": "xxx", ... }],
///     "parser": {"parse": {
///         "col_name": { "as": col_type }, ...
///     }}
/// }
pub async fn get_sample(dsn: &Dsn) -> anyhow::Result<DsSampleIn> {
    // create oracle query
    let config = OracleConfig::from_dsn(dsn)?;
    let mut query = OracleQuery::try_new(config.connect, config.task.time_zone.clone())?;

    // results
    let mut input_sample: Vec<LinkedHashMap<String, serde_json::Value>> = Vec::new();
    let mut parse_sample: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();

    // generate sql
    let sql = config.task.generate_sql()?;
    tracing::info!(
        "get sample data, sql: {}, limit: {}",
        sql,
        config.task.sample_data_limit
    );

    // query sample data
    let (col_map, rows) = query.top_n(&sql, config.task.sample_data_limit)?;

    if rows.is_empty() {
        return Err(anyhow::anyhow!("no data found"));
    }

    // generate sample data
    for row in rows {
        let mut sample_map: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();
        for (col_cidx, col) in row.sql_values().iter().enumerate() {
            let col_name = col_map.iter().nth(col_cidx).map(|(key, _)| key);
            let col_type: &OracleType = col.oracle_type()?;
            let col_val = generate_json_value(col, col_type)?;
            sample_map.insert(col_name.unwrap_or(&"unknown".to_string()).clone(), col_val);
        }
        input_sample.push(sample_map);
    }

    // generate parse data
    for (col_name, col_type) in col_map {
        let column_meta = ColumnMeta::try_new(col_name.clone(), col_type)?;
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

/// migrate or synchronize data from oracle to taos
pub async fn oracle_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    _transform: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    span: Span,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let mut config = OracleConfig::from_dsn(&from)?;

    // set task_id
    config.task_id = task_id;
    tracing::info!(
        "{ORACLE_NAME} task start, id: {:?}, configuration: {:?}",
        task_id,
        config
    );

    // set ipc port
    let port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for connection"))?;
    let socket = format!("127.0.0.1:{}", port);
    config.ipc_port = Some(port);

    // create ipc handler
    let mut ipc = build_ipc(
        &socket,
        parser,
        &to,
        Some(ORACLE_ID),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
        task_id.clone(),
        notify,
    )
    .await?;

    // create worker
    let worker = tokio::spawn(migrate_history(config, cancel.clone()));

    // execute worker
    let port_pool = port_pool.clone();
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
                                anyhow::bail!("{ORACLE_NAME} exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("{ORACLE_NAME} done successfully");
                                let _ = ipc.send(());
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc.send(());
                        anyhow::bail!("{ORACLE_NAME} exit with error: {:#}", err);
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
                    anyhow::bail!("{ORACLE_NAME} writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("{ORACLE_NAME} task cancelled, id: {}", task_id.unwrap_or(-1));
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = ipc.send(());
        // stop the connector
        tracing::info!("{ORACLE_NAME} task done, id: {}", task_id.unwrap_or(-1));
        ipc.close().await?;
        // put ipc port back to port pool.
        port_pool.put(port).await;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

fn generate_json_value(col: &SqlValue, col_type: &OracleType) -> anyhow::Result<serde_json::Value> {
    match col_type {
        // 字符串
        OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Rowid
        | OracleType::Raw(_) => {
            let val = col.get::<String>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        // 浮点数
        OracleType::BinaryFloat => {
            let val = col.get::<f32>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        OracleType::BinaryDouble => {
            let val = col.get::<f64>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        OracleType::Number(_, _) | OracleType::Float(_) => {
            let val = col.get::<String>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        // 日期时间
        OracleType::Date => {
            let val = col.get::<String>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        OracleType::Timestamp(_) | OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            let val = col.get::<NaiveDateTime>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        OracleType::IntervalDS(_, _) | OracleType::IntervalYM(_) => {
            let val = col.get::<String>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        // 大文本
        OracleType::CLOB | OracleType::NCLOB | OracleType::BLOB => {
            let val = col.get::<String>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        OracleType::BFILE | OracleType::RefCursor => {
            let val = col.get::<String>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        OracleType::Boolean => {
            let val = col.get::<String>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        OracleType::Object(_) | OracleType::Long | OracleType::LongRaw | OracleType::Json => {
            let val = col.get::<String>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        // 整型数
        OracleType::Int64 => {
            let val = col.get::<i64>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        }
        OracleType::UInt64 => {
            let val = col.get::<u64>();
            match val {
                Err(_) => Ok(json!(null)),
                Ok(val) => Ok(json!(val)),
            }
        } // 其他
          // }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_is_valid() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1522/ORCLPDB1").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(false, res.valid);
        assert_eq!(false, res.support);
        assert_eq!("oracle", res.data_source);
        assert_eq!(
            "failed to connect to dsn: oracle://test_user:123456@192.168.1.40:1522/ORCLPDB1, cause: failed to connect to oracle, cause: OCI Error: ORA-12541: TNS:no listener",
            res.message.unwrap()
        );

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(true, res.valid);
        assert_eq!(true, res.support);
        assert_eq!("oracle", res.data_source);
    }

    #[tokio::test]
    async fn test_get_sample() {
        let from = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?sql=select * from TEST where ts>=${start} and ts<${end}&start=2024-01-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();

        let res = get_sample(&from).await;
        dbg!(&res);
        assert_eq!(true, res.is_ok());
        println!("{}", serde_json::to_string_pretty(&res.unwrap()).unwrap());
    }

    #[test]
    fn test_oracle_to_taos() {
        let from = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?sql=select * from TEST&start=2024-01-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let to = Dsn::from_str("taos://localhost:6030/ms").unwrap();
        let parser = None;
        let transform = vec![];
        let jobs = 1;
        let port_pool = PortPool::default();
        let cancel = CancellationToken::new();
        let with_agent = None;
        let transferred = None;
        let span = tracing::info_span!("test_oracle_to_taos");
        let task_id = Some(1);
        let (notify, _) = flume::unbounded();

        let _ = oracle_to_taos(
            from,
            parser,
            transform,
            to,
            jobs,
            &port_pool,
            cancel,
            with_agent,
            transferred,
            span,
            task_id,
            notify,
        );
        // let _ = res.await;
    }

    #[test]
    fn test_generate_json_value() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let (_, rows) = query.select_all("select * from TEST").unwrap();

        for row in rows {
            for (_, col) in row.sql_values().iter().enumerate() {
                let col_type: &OracleType = col.oracle_type().unwrap();
                let col_val = generate_json_value(col, col_type);
                dbg!(&col_val);
            }
        }
    }
}
