use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use linked_hash_map::LinkedHashMap;
use mongodb::bson::{Bson, Document};
use serde_json::json;
use taos::Dsn;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::dsv::DataSourceValidation;
use crate::plugins::transform::sample::DsSampleIn;
use crate::runners::mongodb::config::connect::ConnectConfig;
use crate::runners::mongodb::config::MongoDBConfig;
use crate::runners::mongodb::query::MongoDBQuery;
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

use self::worker::migrate_history;

mod appender;
mod config;
mod query;
mod worker;

pub const MONGODB_ID: &str = "mongodb";
pub const MONGODB_NAME: &str = "MongoDB";

/// check mongodb dsn is valid
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = ConnectConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            MONGODB_ID.to_string(),
            format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let query = MongoDBQuery::try_new(c).await;
            match query {
                Err(err) => {
                    let mut err = err.to_string();
                    if err.contains("No available servers") {
                        err = String::from("No available servers");
                    } else if err.contains("Unauthorized") || err.contains("Authentication") {
                        err = String::from("authentication failed");
                    }
                    DataSourceValidation::invalid(
                        MONGODB_ID.to_string(),
                        format!("Failed to connect to dsn: {}", err),
                    )
                }
                Ok(query) => {
                    // 通过查询数据库来判断是否连接成功
                    let result = query.client.list_databases().await;
                    match result {
                        Err(err) => {
                            let mut err = err.to_string();
                            if err.contains("No available servers") {
                                err = String::from("No available servers");
                            } else if err.contains("Unauthorized") || err.contains("Authentication")
                            {
                                err = String::from("authentication failed");
                            }
                            DataSourceValidation::invalid(
                                MONGODB_ID.to_string(),
                                format!("Failed to connect to dsn: {}", err),
                            )
                        }
                        Ok(_cli) => DataSourceValidation::valid(MONGODB_ID.to_string(), None),
                    }
                }
            }
        }
    }
}

/// get sample data from mongodb
/// # Arguments
/// * `dsn` - mongodb dsn
/// # Returns
/// * `DsSampleIn` - {
///     "input": [{ "col_name": "xxx", ... }],
///     "parser": {"parse": {
///         "col_name": { "as": col_type }, ...
///     }}
/// }
pub async fn get_sample(dsn: &Dsn) -> anyhow::Result<DsSampleIn> {
    // create mongodb query
    let mut config = MongoDBConfig::from_dsn(dsn)?;
    let mut query = MongoDBQuery::try_new(config.connect).await?;

    // results
    let mut input_sample: Vec<LinkedHashMap<String, String>> = Vec::new();

    // replace subtable fields
    let placeholders = config
        .task
        .subtable_fields
        .iter()
        .map(|(k, v)| (k.clone(), v.replace("${v}", "{\"$ne\":\"\"}")))
        .collect::<HashMap<String, String>>();
    for (key, value) in placeholders.iter() {
        config.task.sql = config
            .task
            .sql
            .replace(&format!("${{{}}}", key), &value.to_string());
    }

    // generate filter
    let database = config.task.generate_database()?;
    let collection = config.task.generate_collection()?;
    let filter = config.task.generate_filter()?;
    let sort = config.task.generate_sort()?;
    tracing::info!(
        "get sample data, filter: {}, limit: {}",
        filter,
        config.task.sample_data_limit
    );

    // query sample data
    let documents = query
        .top_n(
            &database,
            &collection,
            filter,
            sort,
            config.task.sample_data_limit,
        )
        .await?;

    if documents.is_empty() {
        return Err(anyhow::anyhow!("no data found"));
    }

    // generate sample data
    for document in documents {
        input_sample.push(LinkedHashMap::from_iter(vec![(
            "payload".to_string(),
            generate_payload(document)?,
        )]));
    }

    // generate sample data
    let sample_json = json!({
        "input": input_sample,
        "parser": {
            "parse": {},
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

/// migrate or synchronize data from mongodb to taos
pub async fn mongodb_to_taos(
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
    let mut config = MongoDBConfig::from_dsn(&from)?;

    // set task_id
    config.task_id = task_id;
    tracing::info!(
        "{MONGODB_NAME} task start, id: {:?}, configuration: {:?}",
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
    let mut ipc = build_ipc(
        &socket,
        parser,
        &to,
        Some(MONGODB_ID),
        None,
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
                                anyhow::bail!("{MONGODB_NAME} exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("{MONGODB_NAME} done successfully");
                                let _ = ipc.send(());
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc.send(());
                        anyhow::bail!("{MONGODB_NAME} exit with error: {:#}", err);
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
                    anyhow::bail!("{MONGODB_NAME} writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("{MONGODB_NAME} task cancelled, id: {}", task_id.unwrap_or(-1));
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = ipc.send(());
        // stop the connector
        tracing::info!("{MONGODB_NAME} task done, id: {}", task_id.unwrap_or(-1));
        ipc.close().await?;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

fn generate_payload(document: Document) -> anyhow::Result<String> {
    let mut payload: LinkedHashMap<String, serde_json::Value> = LinkedHashMap::new();
    let keys = document.keys();
    for key in keys {
        let val = document.get(key);
        match val {
            Some(val) => match val {
                Bson::Double(v) => {
                    payload.insert(key.clone(), json!(v));
                }
                Bson::String(v) => {
                    payload.insert(key.clone(), json!(v));
                }
                Bson::Array(v) => {
                    payload.insert(key.clone(), json!(v));
                }
                Bson::Document(v) => {
                    payload.insert(key.clone(), json!(serde_json::to_string(v).unwrap()));
                }
                Bson::Boolean(v) => {
                    payload.insert(key.clone(), json!(v));
                }
                Bson::Null => {
                    payload.insert(key.clone(), json!(null));
                }
                Bson::RegularExpression(v) => {
                    payload.insert(key.clone(), json!(serde_json::to_string(v).unwrap()));
                }
                Bson::JavaScriptCode(v) => {
                    payload.insert(key.clone(), json!(v.to_string()));
                }
                Bson::JavaScriptCodeWithScope(v) => {
                    payload.insert(key.clone(), json!(serde_json::to_string(v).unwrap()));
                }
                Bson::Int32(v) => {
                    payload.insert(key.clone(), json!(v));
                }
                Bson::Int64(v) => {
                    payload.insert(key.clone(), json!(v));
                }
                Bson::Timestamp(v) => {
                    let v = DateTime::from_timestamp(v.time as i64, v.increment).unwrap();
                    payload.insert(key.clone(), json!(v));
                }
                Bson::Binary(v) => {
                    let value: String = v.bytes.iter().map(|b| format!("{:02x}", b)).collect();
                    payload.insert(key.clone(), json!(format!("\\x{}", value)));
                }
                Bson::ObjectId(v) => {
                    payload.insert(key.clone(), json!(v.to_string()));
                }
                Bson::DateTime(v) => {
                    let v = DateTime::from_timestamp_millis(v.timestamp_millis()).unwrap();
                    payload.insert(key.clone(), json!(v));
                }
                Bson::Symbol(v) => {
                    payload.insert(key.clone(), json!(v));
                }
                Bson::Decimal128(v) => {
                    payload.insert(key.clone(), json!(v.to_string()));
                }
                Bson::Undefined => {
                    payload.insert(key.clone(), json!(null));
                }
                Bson::MaxKey => {
                    payload.insert(key.clone(), json!("MaxKey"));
                }
                Bson::MinKey => {
                    payload.insert(key.clone(), json!("MinKey"));
                }
                _ => {
                    payload.insert(key.clone(), json!(null));
                }
            },
            None => {
                payload.insert(key.clone(), json!(null));
            }
        }
    }
    Ok(serde_json::json!(payload).to_string())
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{doc, oid::ObjectId, spec::BinarySubtype, Binary, Decimal128};

    use super::*;
    use std::str::FromStr;

    async fn test_create_table() {
        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(query) => {
                let database = query.client.database("test_taosx");
                let x = database.create_collection("metrics").await;
                println!("create table: {:?}", x);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(len: usize) {
        let _ = test_create_table().await;

        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(query) => {
                for _ in 0..len {
                    let database = query.client.database("test_taosx");
                    let collection = database.collection("metrics");
                    let doc_all = doc! {
                        "double": Bson::Double(3.141592653),
                        "string": Bson::String("abc".to_string()),
                        "array": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]),
                        "document": Bson::Document(doc! {
                            "int32": Bson::Int32(123),
                            "int64": Bson::Int64(123)
                        }),
                        "bool": Bson::Boolean(true),
                        "null": Bson::Null,
                        "regex": Bson::RegularExpression(mongodb::bson::Regex {
                            pattern: "abc".to_string(),
                            options: "i".to_string()
                        }),
                        "javascript": Bson::JavaScriptCode("function() { return 1; }".to_string()),
                        "javascript_with_scope": Bson::JavaScriptCodeWithScope(mongodb::bson::JavaScriptCodeWithScope {
                            code: "function() { return n; }".to_string(),
                            scope: doc! { "n": 1 }
                        }),
                        "int32": Bson::Int32(123),
                        "int64": Bson::Int64(123),
                        "timestamp": Bson::Timestamp(mongodb::bson::Timestamp {
                            time: 123,
                            increment: 456
                        }),
                        "binary": Bson::Binary(mongodb::bson::Binary {
                            subtype: mongodb::bson::spec::BinarySubtype::Generic,
                            bytes: vec![1, 2, 3]
                        }),
                        "object_id": Bson::ObjectId(ObjectId::new()),
                        "datetime": Bson::DateTime(mongodb::bson::DateTime::now()),
                        "symbol": Bson::Symbol("abc".to_string()),
                        "decimal128": Bson::Decimal128(Decimal128::from_str("3.141592653").unwrap()),
                        "undefined": Bson::Undefined,
                        "max_key": Bson::MaxKey,
                        "min_key": Bson::MinKey,
                        // Deprecated
                        // "db_pointer": Bson::DbPointer(DbPointer {
                        //     db: "test".to_string(),
                        //     oid: ObjectId::new()
                        // }),
                    };
                    let _ = collection.insert_one(doc_all).await;
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
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(query) => {
                let database = query.client.database("test_taosx");
                let collection: mongodb::Collection<Document> = database.collection("metrics");

                let _ = collection.delete_many(doc! {}).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_is_valid() {
        // error host
        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.41:27017?source=admin").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(false, res.valid);
        assert_eq!(false, res.support);
        assert_eq!("mongodb", res.data_source);
        assert_eq!(
            "Failed to connect to dsn: No available servers",
            res.message.unwrap()
        );

        // error port
        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27018?source=admin").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(false, res.valid);
        assert_eq!(false, res.support);
        assert_eq!("mongodb", res.data_source);
        assert_eq!(
            "Failed to connect to dsn: No available servers",
            res.message.unwrap()
        );

        // error user
        let dsn =
            Dsn::from_str("mongodb://admin1:tbase125!@192.168.1.40:27017?source=admin").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(false, res.valid);
        assert_eq!(false, res.support);
        assert_eq!("mongodb", res.data_source);
        assert_eq!(
            "Failed to connect to dsn: authentication failed",
            res.message.unwrap()
        );

        // error password
        let dsn =
            Dsn::from_str("mongodb://admin:tbase126!@192.168.1.40:27017?source=admin").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(false, res.valid);
        assert_eq!(false, res.support);
        assert_eq!("mongodb", res.data_source);
        assert_eq!(
            "Failed to connect to dsn: authentication failed",
            res.message.unwrap()
        );

        // error source
        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin1").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(false, res.valid);
        assert_eq!(false, res.support);
        assert_eq!("mongodb", res.data_source);
        assert_eq!(
            "Failed to connect to dsn: authentication failed",
            res.message.unwrap()
        );

        // success
        let dsn =
            Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin").unwrap();
        let res = is_valid(&dsn).await;
        assert_eq!(true, res.valid);
        assert_eq!(true, res.support);
        assert_eq!("mongodb", res.data_source);
    }

    #[tokio::test]
    async fn test_get_sample() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(4).await;

        let from = Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_db6_2023&collection=tb_9&sql={\"createtime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2023-09-01T00:00:00+00:00&end=2023-09-30T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();

        let res = get_sample(&from).await;
        dbg!(&res);
        assert_eq!(true, res.is_ok());
        // clear data
        let _ = test_clear_data().await;
    }

    #[test]
    #[ignore]
    fn test_mongodb_to_taos() {
        let from = Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let to = Dsn::from_str("taos://localhost:6030/ms").unwrap();
        let parser = None;
        let transform = vec![];
        let jobs = 1;
        let port_pool = PortPool::default();
        let cancel = CancellationToken::new();
        let with_agent = None;
        let transferred = None;
        let span = tracing::info_span!("test_mongodb_to_taos");
        let task_id = Some(1);
        let (notify, _) = flume::unbounded();

        let _ = mongodb_to_taos(
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

    #[tokio::test]
    async fn test_generate_payload() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(4).await;

        let dsn = Dsn::from_str(
            "mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(mut query) => {
                let query_result = query
                    .top_n("test_taosx", "metrics", doc! {}, doc! {}, 1)
                    .await;
                match query_result {
                    Ok(documents) => {
                        for document in documents {
                            let payload = generate_payload(document).unwrap();
                            dbg!(&payload);
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

    #[test]
    fn test_binary() {
        let binary = Binary {
            subtype: BinarySubtype::Generic,
            bytes: vec![1, 2, 3],
        };
        let res: String = binary.bytes.iter().map(|b| format!("{:02x}", b)).collect();
        println!("\\x{}", res);
    }
}
