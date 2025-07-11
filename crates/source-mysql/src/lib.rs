use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{FixedOffset, NaiveDateTime};
use linked_hash_map::LinkedHashMap;
use serde_json::json;
use sqlx::mysql::MySqlRow;
use sqlx::{Column, Row, TypeInfo};
use taos::Dsn;
use tokio_util::sync::CancellationToken;

use taosx_core::dsv::DataSourceValidation;
use taosx_core::plugins::transform::sample::DsSampleIn;
use taosx_core::utils::port_pool::PortPool;
use taosx_core::{Parser, TaskNotifySender, Transferred, build_ipc};

use crate::appender::column_meta::ColumnMeta;
use crate::config::MySqlConfig;
use crate::config::connect::ConnectConfig;
use crate::query::MySqlQuery;

use self::worker::migrate_history;

mod appender;
mod config;
mod query;
mod worker;

pub const MYSQL_ID: &str = "mysql";
pub const MYSQL_NAME: &str = "MySQL";

/// check mysql dsn is valid
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            MYSQL_ID.to_string(),
            format!("invalid dsn: {}, cause: {}", dsn, err),
        ),
        Ok(c) => {
            let result = MySqlQuery::try_new(c, String::from("+08:00")).await;
            match result {
                Err(err) => DataSourceValidation::invalid(
                    MYSQL_ID.to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                ),
                Ok(mut _cli) => {
                    let rs: Result<Vec<String>, anyhow::Error> = _cli.show_tables().await;
                    match rs {
                        Err(err) => DataSourceValidation::invalid(
                            MYSQL_ID.to_string(),
                            format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                        ),
                        Ok(_) => DataSourceValidation::valid(MYSQL_ID.to_string(), None),
                    }
                }
            }
        }
    }
}

/// get sample data from mysql
/// # Arguments
/// * `dsn` - mysql dsn
/// # Returns
/// * `DsSampleIn` - {
///   "input": [{ "col_name": "xxx", ... }],
///   "parser": {"parse": {
///   "col_name": { "as": col_type }, ...
///   }}
///   }
pub async fn get_sample(dsn: &Dsn) -> anyhow::Result<DsSampleIn> {
    // create mysql query
    let mut config = MySqlConfig::from_dsn(dsn)?;
    let mut query =
        MySqlQuery::try_new(config.connect.clone(), config.task.time_zone.clone()).await?;

    // results
    let mut input_sample: Vec<LinkedHashMap<String, serde_json::Value>> = Vec::new();
    let mut parse_sample: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();

    // replace subtable fields
    let distinct_sql = config.task.generate_distinct_sql()?;
    let values = if !distinct_sql.is_empty() {
        query.select_one_for_schema(&distinct_sql).await?
    } else {
        None
    };
    if let Some(row) = values {
        for idx in 0..row.len() {
            let col_name = row.column(idx).name();
            config.task.sql = config.task.sql.replace(
                &format!("${{{}}}", col_name),
                &format!("{} is not null", col_name),
            );
        }
    }

    // generate sql
    let sql = config.task.generate_sql()?;
    tracing::info!("get sample data, config: {:?}", &config);
    tracing::info!(
        "get sample data, sql: {}, limit: {}",
        sql,
        config.task.sample_data_limit
    );

    // query sample data
    let rows = query.top_n(&sql, config.task.sample_data_limit).await?;

    if rows.is_empty() {
        return Err(anyhow::anyhow!("no data found"));
    }

    // generate sample data
    for row in &rows {
        let mut sample_map: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();
        for (idx, col) in row.columns().iter().enumerate() {
            let col_name = col.name();
            let col_type = col.type_info().name();
            let col_val = generate_json_value(row, col_type, idx, config.task.time_zone.clone())?;
            sample_map.insert(col_name.to_string(), col_val);
        }
        input_sample.push(sample_map);
    }

    // generate parse data
    for col in rows[0].columns() {
        let col_name = col.name();
        let col_type = col.type_info().name();
        let column_meta = ColumnMeta::try_new(col_name.to_string(), col_type.to_string())?;
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

/// migrate or synchronize data from mysql to taos
pub async fn mysql_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: TaskNotifySender,
) -> anyhow::Result<()> {
    let mut config = MySqlConfig::from_dsn(&from)?;

    // set task_id
    config.task_id = task_id;
    tracing::info!(
        "{MYSQL_NAME} task start, id: {:?}, configuration: {:?}",
        task_id,
        config
    );

    // set ipc port
    let port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for connection"))?;
    let socket = format!("127.0.0.1:{}", port.get());
    config.ipc_port = Some(port.get());

    // create ipc handler
    let (mut ipc, _) = build_ipc(
        Some(&socket),
        parser,
        &to,
        Some(MYSQL_ID),
        None,
        None,
        &cancel,
        with_agent,
        transferred,
        task_id,
        notify,
        None,
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
                                anyhow::bail!("{MYSQL_NAME} exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("{MYSQL_NAME} done successfully");
                                let _ = ipc.send(());
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc.send(());
                        anyhow::bail!("{MYSQL_NAME} exit with error: {:#}", err);
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
                    anyhow::bail!("{MYSQL_NAME} writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("{MYSQL_NAME} task cancelled, id: {}", task_id.unwrap_or(-1));
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = ipc.send(());
        // stop the connector
        tracing::info!("{MYSQL_NAME} task done, id: {}", task_id.unwrap_or(-1));
        ipc.close().await?;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

fn generate_json_value(
    row: &MySqlRow,
    col_type: &str,
    cidx: usize,
    time_zone: String,
) -> anyhow::Result<serde_json::Value> {
    match col_type {
        // 整型数
        "TINYINT" => {
            let val = row.try_get::<Option<i8>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "TINYINT UNSIGNED" => {
            let val = row.try_get::<Option<u8>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "SMALLINT" => {
            let val = row.try_get::<Option<i16>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "SMALLINT UNSIGNED" => {
            let val = row.try_get::<Option<u16>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "MEDIUMINT" | "INT" => {
            let val = row.try_get::<Option<i32>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
            let val = row.try_get::<Option<u32>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "BIGINT" => {
            let val = row.try_get::<Option<i64>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "BIGINT UNSIGNED" => {
            let val = row.try_get::<Option<u64>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        // 浮点数
        "FLOAT" => {
            let val = row.try_get::<Option<f32>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "DOUBLE" => {
            let val = row.try_get::<Option<f64>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "DECIMAL" => {
            let val = row.try_get::<Option<bigdecimal::BigDecimal>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val.to_string())),
            }
        }
        // 字符串
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDUIMTEXT" | "LONGTEXT" => {
            let val = row.try_get::<Option<String>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            let val = row.try_get::<Option<&[u8]>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        // 字节数组
        "BINARY" | "VARBINARY" => {
            let val = row.try_get::<Option<&[u8]>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        // 日期时间
        "DATE" => {
            let val = row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        "TIME" => {
            let val = row.try_get::<Option<sqlx::types::chrono::NaiveTime>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        "DATETIME" => {
            let val = row.try_get::<Option<NaiveDateTime>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        "TIMESTAMP" => {
            let val = row
                .try_get::<Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>, _>(
                    cidx,
                )?;
            match val {
                None => Ok(json!(null)),
                Some(val) => {
                    // mysql 的 timestamp 是基于 session 时区的假 UTC 时间，需要转换为真正的 UTC 时间
                    let time_zone = FixedOffset::from_str(time_zone.as_str()).unwrap();
                    let real_timestamp_utc = val.naive_utc().and_local_timezone(time_zone).unwrap();
                    Ok(json!(real_timestamp_utc))
                }
            }
        }
        "YEAR" => {
            let val = row.try_get::<Option<u16>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        // 二进制
        "BIT" => {
            let val = row.try_get::<Option<u8>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        _ => {
            let val = row.try_get::<Option<String>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::Executor;

    use super::*;

    async fn test_create_database() {
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.45:3306/information_schema").unwrap();
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

    async fn test_create_table(table_name: &str) {
        let _ = test_create_database().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_drop_table = format!("drop table if exists {table_name}");
                let _ = query.pool.execute(sql_drop_table.as_str()).await;
                let sql_create_table = format!(
                    "create table if not exists {table_name} (id int primary key auto_increment, name varchar(255), value double, ts timestamp)"
                );
                let _ = query.pool.execute(sql_create_table.as_str()).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(table_name: &str, len: usize) {
        let _ = test_create_table(table_name).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_insert_data = format!(
                    "insert into {table_name} (name, value, ts) values ('cpu', 0.8, now())"
                );
                for _ in 0..len {
                    let _ = query.pool.execute(sql_insert_data.as_str()).await;
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data(table_name: &str) {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql = format!("delete from {table_name} where 1 = 1");
                let _ = query.pool.execute(sql.as_str()).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_is_valid_with_datasource() {
        // prepare data
        let _ = test_create_database().await;

        // invalid port
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3305/test_ci").unwrap();
        let res = is_valid(&dsn).await;
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!("mysql", res.data_source);
        assert_eq!(
            "failed to connect to dsn: mysql://root:123456@192.168.1.45:3305/test_ci, cause: failed to connect to mysql, cause: pool timed out while waiting for an open connection",
            res.message.unwrap()
        );

        // user: test_ssl_only -- ssl_mode: DISABLED -- Access denied
        // let dsn = Dsn::from_str(
        //     "mysql://test_ssl_only:taosdata@192.168.1.45:3306/test_ci?ssl_mode=DISABLED",
        // )
        // .unwrap();
        // let res = is_valid(&dsn).await;
        // assert!(!res.valid);
        // assert!(!res.support);
        // assert_eq!("mysql", res.data_source);
        // assert_eq!(
        //     "failed to connect to dsn: mysql://test_ssl_only:taosdata@192.168.1.45:3306/test_ci?ssl_mode=DISABLED, cause: failed to connect to mysql, cause: error returned from database: 1045 (28000): Access denied for user 'test_ssl_only'@'192.168.2.13' (using password: YES)",
        //     res.message.unwrap()
        // );

        // user: test_ssl_only -- ssl_mode: REQUIRED -- Access succ
        // let dsn = Dsn::from_str(
        //     "mysql://test_ssl_only:taosdata@192.168.1.45:3306/test_ci?ssl_mode=REQUIRED",
        // )
        // .unwrap();
        // let res = is_valid(&dsn).await;
        // assert!(res.valid);
        // assert!(res.support);
        // assert_eq!("mysql", res.data_source);

        // user: test_disabled_only -- not support

        // normal
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let res = is_valid(&dsn).await;
        assert!(res.valid);
        assert!(res.support);
        assert_eq!("mysql", res.data_source);
    }

    #[tokio::test]
    async fn test_get_sample_with_datasource() {
        // prepare data
        let _ = test_create_table("test_get_sample").await;
        let _ = test_insert_data("test_get_sample", 4).await;

        let from = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?sql=select * from test_get_sample where ts >= ${start} and ts <= ${end}&start=2024-04-08T00:00:00Z&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();

        let res = get_sample(&from).await;
        dbg!(&res);
        assert!(res.is_ok());
        println!("{}", serde_json::to_string_pretty(&res.unwrap()).unwrap());

        // clear data
        let _ = test_clear_data("test_get_sample").await;
    }

    #[tokio::test]
    async fn test_mysql_to_taos() {
        let from = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?sql=select * from t_metric&start=2024-01-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let to = Dsn::from_str("taos://localhost:6030/ms").unwrap();
        let parser = None;
        let port_pool = PortPool::default();
        let cancel = CancellationToken::new();
        let with_agent = None;
        let transferred = None;
        let task_id = Some(1);
        let (notify, _) = flume::unbounded();

        mysql_to_taos(
            from,
            parser,
            to,
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
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select * from tb_test_ci")
            .await
            .unwrap();

        match row {
            Some(row) => {
                for idx in 0..31 {
                    let res = generate_json_value(
                        &row,
                        row.column(idx).type_info().name(),
                        idx,
                        String::from("+08:00"),
                    );
                    dbg!(&res);
                }
            }
            None => {
                println!("no data");
            }
        }
    }
}
