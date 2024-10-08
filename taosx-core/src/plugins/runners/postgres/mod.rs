use std::sync::Arc;
use std::time::Duration;

use linked_hash_map::LinkedHashMap;
use serde_json::json;
use sqlx::{Column, Row, TypeInfo};
use sqlx_postgres::types::PgTimeTz;
use sqlx_postgres::PgRow;
use taos::Dsn;
use tokio_util::sync::CancellationToken;

use crate::dsv::DataSourceValidation;
use crate::plugins::transform::sample::DsSampleIn;
use crate::runners::postgres::appender::column_meta::ColumnMeta;
use crate::runners::postgres::config::connect::ConnectConfig;
use crate::runners::postgres::config::PostgresConfig;
use crate::runners::postgres::query::PostgresQuery;
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

use self::worker::migrate_history;

mod appender;
mod config;
mod query;
mod worker;

pub const POSTGRES_ID: &str = "postgres";
pub const POSTGRES_NAME: &str = "Postgres";

/// check postgres dsn is valid
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            POSTGRES_ID.to_string(),
            format!("invalid dsn: {}, cause: {}", dsn, err),
        ),
        Ok(c) => {
            let result = PostgresQuery::try_new(c, String::from("+08:00")).await;
            match result {
                Err(err) => DataSourceValidation::invalid(
                    POSTGRES_ID.to_string(),
                    format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                ),
                Ok(mut _cli) => {
                    let rs = _cli.select_one_for_schema("select 1 from pg_tables;").await;
                    match rs {
                        Err(err) => DataSourceValidation::invalid(
                            POSTGRES_ID.to_string(),
                            format!("failed to connect to dsn: {}, cause: {}", dsn, err),
                        ),
                        Ok(_) => DataSourceValidation::valid(POSTGRES_ID.to_string(), None),
                    }
                }
            }
        }
    }
}

/// get sample data from postgres
/// # Arguments
/// * `dsn` - postgres dsn
/// # Returns
/// * `DsSampleIn` - {
///     "input": [{ "col_name": "xxx", ... }],
///     "parser": {"parse": {
///         "col_name": { "as": col_type }, ...
///     }}
/// }
pub async fn get_sample(dsn: &Dsn) -> anyhow::Result<DsSampleIn> {
    // create postgres query
    let mut config = PostgresConfig::from_dsn(dsn)?;
    let mut query = PostgresQuery::try_new(config.connect, config.task.time_zone.clone()).await?;

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
            let col_val = generate_json_value(row, col_type, idx)?;
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

/// migrate or synchronize data from postgres to taos
pub async fn postgres_to_taos(
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
    let mut config = PostgresConfig::from_dsn(&from)?;

    // set task_id
    config.task_id = task_id;
    tracing::info!(
        "{POSTGRES_NAME} task start, id: {:?}, configuration: {:?}",
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
        Some(POSTGRES_ID),
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
                                anyhow::bail!("{POSTGRES_NAME} exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("{POSTGRES_NAME} done successfully");
                                let _ = ipc.send(());
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc.send(());
                        anyhow::bail!("{POSTGRES_NAME} exit with error: {:#}", err);
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
                    anyhow::bail!("{POSTGRES_NAME} writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("{POSTGRES_NAME} task cancelled, id: {}", task_id.unwrap_or(-1));
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = ipc.send(());
        // stop the connector
        tracing::info!("{POSTGRES_NAME} task done, id: {}", task_id.unwrap_or(-1));
        ipc.close().await?;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

fn generate_json_value(
    row: &PgRow,
    col_type: &str,
    cidx: usize,
) -> anyhow::Result<serde_json::Value> {
    match col_type {
        // 布尔值
        "BOOL" => {
            let val = row.try_get::<Option<bool>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        // 字符
        "CHAR" => {
            let val = row.try_get::<Option<String>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        // 整型数
        "SMALLINT" | "SMALLSERIAL" | "INT2" => {
            let val = row.try_get::<Option<i16>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "INT" | "SERIAL" | "INT4" => {
            let val = row.try_get::<Option<i32>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "BIGINT" | "BIGSERIAL" | "INT8" => {
            let val = row.try_get::<Option<i64>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        // 浮点数
        "REAL" | "FLOAT4" => {
            let val = row.try_get::<Option<f32>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "DOUBLE PRECISION" | "FLOAT8" => {
            let val = row.try_get::<Option<f64>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "NUMERIC" => {
            let val = row.try_get::<Option<bigdecimal::BigDecimal>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val.to_string())),
            }
        }
        // 字符串
        "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" | "CITEXT" => {
            let val = row.try_get::<Option<String>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        "BYTEA" => {
            let val = row.try_get::<Option<&[u8]>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
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
        "TIMESTAMP" => {
            let val = row.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        "TIMESTAMPTZ" => {
            let val = row
                .try_get::<Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>, _>(
                    cidx,
                )?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val)),
            }
        }
        // uuid
        "UUID" => {
            // TODO
            Ok(json!(""))
        }
        // 二进制数组
        "BIT" | "VARBIT" => {
            let val = row.try_get::<Option<bit_vec::BitVec>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        // json
        "JSON" | "JSONB" => {
            let val = row.try_get::<Option<serde_json::Value>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?}", val))),
            }
        }
        // Others
        "INTERVAL" => {
            let val = row.try_get::<Option<sqlx_postgres::types::PgInterval>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(val.microseconds.to_string())),
            }
        }
        "INT8RANGE" | "INT4RANGE" | "TSRANGE" | "TSTZRANGE" | "DATERANGE" | "NUMRANGE" => {
            // TODO
            Ok(json!(""))
        }
        "MONEY" => {
            // TODO
            Ok(json!(""))
        }
        "LTREE" => {
            // TODO
            Ok(json!(""))
        }
        "LQUERY" => {
            // TODO
            Ok(json!(""))
        }
        "TIMETZ" => {
            let val = row.try_get::<Option<PgTimeTz>, _>(cidx)?;
            match val {
                None => Ok(json!(null)),
                Some(val) => Ok(json!(format!("{:?} {:?}", val.time, val.offset))),
            }
        }
        "INET" | "CIDR" => {
            // TODO
            Ok(json!(""))
        }
        "MACADDR" => {
            // TODO
            Ok(json!(""))
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
    use std::str::FromStr;

    async fn test_create_database() {
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_database = "create database test_taosx";
                let _ = query.pool.execute(sql_create_database).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_create_table() {
        let _ = test_create_database().await;

        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_table = "create table if not exists t_metric (id int primary key, name varchar(255), value FLOAT8, ts timestamp)";
                let _ = query.pool.execute(sql_create_table).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(len: usize) {
        let _ = test_create_table().await;

        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                for i in 0..len {
                    let sql_insert_data = format!("insert into t_metric (id, name, value, ts) values ({}, 'cpu', 0.8, CURRENT_TIMESTAMP)", i);
                    let _ = query.pool.execute(sql_insert_data.as_str()).await;
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data() {
        let _ = test_create_table().await;

        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
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
    async fn test_is_valid() {
        // prepare data
        let _ = test_create_database().await;

        // invalid port
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5433/test_taosx").unwrap();
        let res = is_valid(&dsn).await;
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!("postgres", res.data_source);
        assert_eq!(
            "failed to connect to dsn: postgres://postgres:tbase125%21@192.168.1.40:5433/test_taosx, cause: failed to connect to postgres, cause: pool timed out while waiting for an open connection",
            res.message.unwrap()
        );

        // user: test_ssl_only -- ssl_mode: Disable -- Access denied
        let dsn = Dsn::from_str(
            "postgres://test_ssl_only:taosdata@192.168.1.40:5432/test_ssl_only?ssl_mode=Disable",
        )
        .unwrap();
        let res = is_valid(&dsn).await;
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!("postgres", res.data_source);
        assert_eq!(
            "failed to connect to dsn: postgres://test_ssl_only:taosdata@192.168.1.40:5432/test_ssl_only?ssl_mode=Disable, cause: failed to connect to postgres, cause: error returned from database: no pg_hba.conf entry for host \"192.168.2.13\", user \"test_ssl_only\", database \"test_ssl_only\", no encryption",
            res.message.unwrap()
        );

        // user: test_ssl_only -- ssl_mode: Require -- Access succ
        let dsn = Dsn::from_str(
            "postgres://test_ssl_only:taosdata@192.168.1.40:5432/test_ssl_only?ssl_mode=Require",
        )
        .unwrap();
        let res = is_valid(&dsn).await;
        assert!(res.valid);
        assert!(res.support);
        assert_eq!("postgres", res.data_source);

        // user: test_disable_only -- ssl_mode: Require -- Access denied
        let dsn = Dsn::from_str(
            "postgres://test_disable_only:taosdata@192.168.1.40:5432/test_disable_only?ssl_mode=Require",
        )
        .unwrap();
        let res = is_valid(&dsn).await;
        assert!(!res.valid);
        assert!(!res.support);
        assert_eq!("postgres", res.data_source);
        assert_eq!(
            "failed to connect to dsn: postgres://test_disable_only:taosdata@192.168.1.40:5432/test_disable_only?ssl_mode=Require, cause: failed to connect to postgres, cause: error returned from database: no pg_hba.conf entry for host \"192.168.2.13\", user \"test_disable_only\", database \"test_disable_only\", SSL encryption",
            res.message.unwrap()
        );

        // user: test_disable_only -- ssl_mode: Disable -- Access succ
        let dsn = Dsn::from_str(
            "postgres://test_disable_only:taosdata@192.168.1.40:5432/test_disable_only?ssl_mode=Disable",
        )
        .unwrap();
        let res = is_valid(&dsn).await;
        assert!(res.valid);
        assert!(res.support);
        assert_eq!("postgres", res.data_source);

        // normal
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let res = is_valid(&dsn).await;
        assert!(res.valid);
        assert!(res.support);
        assert_eq!("postgres", res.data_source);
    }

    #[tokio::test]
    async fn test_get_sample() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(4).await;

        let from = Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx?sql=select * from public.t_metric where ts >= ${start} and ts <= ${end}&start=2024-01-01T00:00:00Z&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();

        let res = get_sample(&from).await;
        dbg!(&res);
        assert!(res.is_ok());
        // clear data
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_postgres_to_taos() {
        let from = Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx?sql=select * from public.t_metric&start=2024-01-01T00:00:00Z&end=2024-04-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let to = Dsn::from_str("taos://localhost:6030/ms").unwrap();
        let parser = None;
        let transform = vec![];
        let jobs = 1;
        let port_pool = PortPool::default();
        let cancel = CancellationToken::new();
        let with_agent = None;
        let transferred = None;
        let span = tracing::info_span!("test_postgres_to_taos");
        let task_id = Some(1);
        let (notify, _) = flume::unbounded();

        let _ = postgres_to_taos(
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
        );
        // let _ = res.await;
    }

    #[tokio::test]
    async fn test_generate_json_value() {
        // prepare data
        let _ = test_clear_data().await;
        let _ = test_insert_data(1).await;

        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.select_one_for_schema("select * from t_metric").await;
                match query_result {
                    Ok(row) => match row {
                        Some(row) => {
                            for idx in 0..4 {
                                let res = generate_json_value(
                                    &row,
                                    row.column(idx).type_info().name(),
                                    idx,
                                );
                                dbg!(&res);
                            }
                        }
                        None => {
                            println!("no data found");
                        }
                    },
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
