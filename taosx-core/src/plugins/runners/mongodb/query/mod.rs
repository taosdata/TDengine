use arrow::array::RecordBatch;
use flume::Sender;
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::{AuthMechanism, ClientOptions, Compressor, Credential, Tls, TlsOptions};
use mongodb::{Client, Cursor};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use taos::StreamExt;

use crate::runners::mongodb::appender;
use crate::runners::mongodb::config::connect::ConnectConfig;

#[derive(Clone)]
pub struct MongoDBQuery {
    pub client: Client,
}

impl MongoDBQuery {
    pub async fn try_new(config: ConnectConfig) -> anyhow::Result<Self> {
        let client = Self::connect(
            config.host,
            config.port,
            config.load_balanced,
            config.direct_connection,
            config.repl_set_name,
            config.local_threshold,
            config.username,
            config.password,
            config.mechanism,
            config.source,
            config.app_name,
            config.compressors,
            config.tls,
            config.ca_file_path,
            config.cert_key_file_path,
        )
        .await
        .map_err(|err| {
            anyhow::anyhow!("failed to connect to mongodb, cause: {}", err.to_string())
        })?;
        Ok(Self { client })
    }

    async fn connect(
        host: String,
        port: u16,
        load_balanced: bool,
        direct_connection: bool,
        repl_set_name: Option<String>,
        local_threshold: Duration,
        username: Option<String>,
        password: Option<String>,
        mechanism: Option<String>,
        source: Option<String>,
        application_name: Option<String>,
        compressors: Option<String>,
        tls: bool,
        ca_file_path: Option<String>,
        cert_key_file_path: Option<String>,
    ) -> anyhow::Result<Client> {
        // base connection string
        let conn_str = format!("mongodb://{}:{}", host, port);
        let mut client_options = ClientOptions::parse(conn_str).await?;

        // whether to connect to a cluster
        client_options.load_balanced = Some(load_balanced);
        client_options.direct_connection = Some(direct_connection);
        client_options.repl_set_name = repl_set_name;
        client_options.local_threshold = Some(local_threshold);

        // authentication
        client_options.credential = if let (Some(username), Some(password)) = (username, password) {
            let credential_builder = Credential::builder()
                .username(username)
                .password(password)
                .source(source);
            let mechanism = match mechanism {
                Some(mechanism) => match mechanism.as_str() {
                    "MongoDbCr" => Some(AuthMechanism::MongoDbCr),
                    "ScramSha1" => Some(AuthMechanism::ScramSha1),
                    "ScramSha256" => Some(AuthMechanism::ScramSha256),
                    "MongoDbX509" => Some(AuthMechanism::MongoDbX509),
                    "Gssapi" => Some(AuthMechanism::Gssapi),
                    "Plain" => Some(AuthMechanism::Plain),
                    "MongodDbAws" => Some(AuthMechanism::MongoDbAws),
                    "MongoDbOidc" => Some(AuthMechanism::MongoDbOidc),
                    _ => None,
                },
                None => None,
            };
            Some(credential_builder.mechanism(mechanism).build())
        } else {
            None
        };

        // other options
        client_options.app_name = application_name;
        client_options.compressors = if let Some(compressors) = compressors {
            let compressor = Compressor::from_str(compressors.as_str());
            match compressor {
                Ok(compressor) => Some(vec![compressor]),
                Err(_) => None,
            }
        } else {
            None
        };
        // tls: if the path of ca_file and cert_key_file is not empty, enable tls
        let tls = tls || (ca_file_path.is_some() && cert_key_file_path.is_some());
        client_options.tls = if tls {
            let tls_builder = TlsOptions::builder()
                .allow_invalid_certificates(Some(true))
                .ca_file_path(PathBuf::from(ca_file_path.as_ref().unwrap()))
                .cert_key_file_path(PathBuf::from(cert_key_file_path.as_ref().unwrap()))
                .allow_invalid_hostnames(Some(true));
            Some(Tls::Enabled(tls_builder.build()))
        } else {
            Some(Tls::Disabled)
        };
        let client = Client::with_options(client_options);
        match client {
            Ok(client) => Ok(client),
            Err(err) => Err(anyhow::anyhow!("{}", err.to_string())),
        }
    }

    pub async fn select_distinct_values(
        &mut self,
        database: &str,
        collection: &str,
        field: &str,
    ) -> anyhow::Result<Vec<Bson>> {
        // connect to mongodb
        let database = self.client.database(database);
        let collection: mongodb::Collection<Document> = database.collection(collection);
        // select distinct values
        let result = collection.distinct(field, doc! {}).await;
        match result {
            Ok(values) => Ok(values),
            Err(err) => anyhow::bail!("failed to select distinct values, cause: {err:#}"),
        }
    }

    #[allow(unused)]
    pub async fn select_all_and_to_record_batches(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
        batch_size: usize,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        // connect to mongodb
        let database = self.client.database(database);
        let collection: mongodb::Collection<Document> = database.collection(collection);
        // select data
        let result: Result<Cursor<Document>, mongodb::error::Error> = collection.find(filter).await;
        match result {
            Ok(mut cursor) => {
                let mut documents = Vec::new();
                loop {
                    let item = cursor.next().await;
                    match item {
                        Some(Ok(item)) => {
                            documents.push(item);
                        }
                        Some(Err(e)) => {
                            anyhow::bail!("failed to select data, cause: {}", e.to_string());
                        }
                        None => break,
                    }
                }
                let batch = appender::to_record_batches(&documents, batch_size)?;
                Ok(batch)
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
    }

    pub async fn select_all_and_send(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
        sort: Document,
        batch_size: usize,
        tx: Sender<RecordBatch>,
    ) -> anyhow::Result<u64> {
        // connect to mongodb
        let database = self.client.database(database);
        let collection: mongodb::Collection<Document> = database.collection(collection);
        // statistics
        let mut amount = 0;
        // select data
        let result: Result<Cursor<Document>, mongodb::error::Error> =
            collection.find(filter).sort(sort).await;
        match result {
            Ok(mut cursor) => {
                let mut documents = Vec::new();
                loop {
                    let item = cursor.next().await;
                    match item {
                        Some(Ok(item)) => {
                            if documents.len() >= batch_size {
                                send_documents_to_ipc(
                                    &mut documents,
                                    batch_size,
                                    &tx,
                                    &mut amount,
                                )?;
                            }
                            documents.push(item);
                        }
                        Some(Err(e)) => {
                            anyhow::bail!("failed to select data, cause: {}", e.to_string());
                        }
                        None => break,
                    }
                }
                if !documents.is_empty() {
                    send_documents_to_ipc(&mut documents, batch_size, &tx, &mut amount)?;
                }
                Ok(amount)
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
    }

    pub async fn top_n(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
        sort: Document,
        top_n: u32,
    ) -> anyhow::Result<Vec<Document>> {
        // connect to mongodb
        let database = self.client.database(database);
        let collection: mongodb::Collection<Document> = database.collection(collection);
        // select data
        let result: Result<Cursor<Document>, mongodb::error::Error> =
            collection.find(filter).sort(sort).await;
        match result {
            Ok(mut cursor) => {
                let mut documents = Vec::new();
                loop {
                    let item = cursor.next().await;
                    match item {
                        Some(Ok(item)) => {
                            if documents.len() >= top_n as usize {
                                break;
                            }
                            documents.push(item);
                        }
                        Some(Err(e)) => {
                            anyhow::bail!("failed to select data, cause: {}", e.to_string());
                        }
                        None => break,
                    }
                }
                Ok(documents)
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
    }
}

fn send_documents_to_ipc(
    documents: &mut Vec<Document>,
    batch_size: usize,
    tx: &Sender<RecordBatch>,
    amount: &mut u64,
) -> Result<(), anyhow::Error> {
    let batches = appender::to_record_batches(&*documents, batch_size)?;
    for batch in batches {
        if batch.num_rows() > 0 {
            // if the sending fails, retry 3 times by sleeping 1 second each time
            for i in 1..4 {
                let send_result = tx.send(batch.clone());
                match send_result {
                    Ok(_) => break,
                    Err(e) => {
                        tracing::warn!(
                            "migrate mongodb, failed to send record batch to taosx, cause: {}, retrying {i} times...",
                            e
                        );
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        }
    }
    *amount += documents.len() as u64;
    documents.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, oid::ObjectId, Bson, Decimal128};
    use std::str::FromStr;
    use taos::Dsn;

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
                        "double": Bson::Double(1.234567890),
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
                        "decimal128": Bson::Decimal128(Decimal128::from_str("1.234567890").unwrap()),
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
    async fn test_connect() {
        let dsn = Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin");
        let config = ConnectConfig::from_dsn(&dsn.unwrap()).unwrap();
        dbg!(&config);

        let query = MongoDBQuery::try_new(config).await.unwrap();
        dbg!(query.client);
    }

    #[tokio::test]
    async fn test_select_distinct_values() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(3).await;

        let dsn = Dsn::from_str(
            "mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={}",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(mut query) => {
                let query_result = query
                    .select_distinct_values("test_taosx", "metrics", "string")
                    .await;
                match query_result {
                    Ok(values) => {
                        dbg!(&values);
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

    #[tokio::test]
    async fn test_select_all_and_to_record_batches() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str(
            "mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={}",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(mut query) => {
                let query_result = query
                    .select_all_and_to_record_batches("test_taosx", "metrics", doc! {}, 3)
                    .await;
                match query_result {
                    Ok(batches) => {
                        dbg!(&batches);
                        assert_eq!(batches.len(), 3);
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

    #[tokio::test]
    async fn test_top_n() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(3).await;

        let dsn = Dsn::from_str(
            "mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={}",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MongoDBQuery::try_new(config).await;
        match result {
            Ok(mut query) => {
                let query_result = query
                    .top_n("test_taosx", "metrics", doc! {}, doc! {}, 5)
                    .await;
                match query_result {
                    Ok(documents) => {
                        dbg!(&documents);
                        assert_eq!(documents.len(), 3);
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
