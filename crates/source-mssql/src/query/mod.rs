use crate::appender;
use crate::config::connect::ConnectConfig;

use arrow::array::RecordBatch;
use futures::TryStreamExt;
use linked_hash_map::LinkedHashMap;
use tiberius::AuthMethod;
use tiberius::{ColumnType, EncryptionLevel, QueryItem, Row};

#[derive(Clone)]
pub struct MssqlQuery {
    pub pool: deadpool::managed::Pool<deadpool_tiberius::Manager>,
    time_zone: String,
}

impl MssqlQuery {
    pub async fn try_new(config: ConnectConfig, time_zone: String) -> anyhow::Result<Self> {
        let pool = Self::connect(
            &config.host,
            config.port,
            &config.database,
            &config.instance_name,
            &config.application_name,
            config.encryption,
            config.trust_cert,
            &config.trust_cert_ca,
            &config.username,
            &config.password,
            time_zone.clone(),
        )
        .map_err(|err| anyhow::anyhow!("failed to connect to mssql, cause: {}", err.to_string()))?;
        Ok(Self { pool, time_zone })
    }

    fn connect(
        host: &String,
        port: u16,
        database: &String,
        instance_name: &String,
        application_name: &String,
        encryption: String,
        trust_cert: bool,
        trust_cert_ca: &Option<String>,
        username: &String,
        password: &String,
        _time_zone: String,
    ) -> std::result::Result<
        deadpool::managed::Pool<
            deadpool_tiberius::Manager,
            deadpool::managed::Object<deadpool_tiberius::Manager>,
        >,
        deadpool_tiberius::SqlServerError,
    > {
        let mut manager = deadpool_tiberius::Manager::new()
            .host(host)
            .port(port)
            .database(database)
            .instance_name(instance_name)
            .application_name(application_name)
            .max_size(50)
            .wait_timeout(10)
            .pre_recycle_sync(|_client, _metrics| {
                // do sth with client object and pool metrics
                Ok(())
            });
        // The configured encryption level specifying if encryption is required
        manager = match encryption.as_str() {
            "Off" => manager.encryption(EncryptionLevel::Off),
            "On" => manager.encryption(EncryptionLevel::On),
            "NotSupported" => manager.encryption(EncryptionLevel::NotSupported),
            "Required" => manager.encryption(EncryptionLevel::Required),
            _ => manager.encryption(EncryptionLevel::NotSupported),
        };
        // Trust the server certificate
        if trust_cert {
            if let Some(ca) = trust_cert_ca {
                manager = manager.trust_cert_ca(ca.as_str());
            } else {
                manager = manager.trust_cert();
            }
        }
        manager = manager.authentication(AuthMethod::sql_server(username, password));
        // create a pool
        manager.create_pool()
    }

    pub async fn select_distinct_values(
        &mut self,
        sql: &str,
    ) -> anyhow::Result<(LinkedHashMap<String, ColumnType>, Vec<Row>)> {
        // get a connection
        let mut conn = self.pool.get().await?;
        // select data
        let result = conn.query(sql, &[]).await;
        match result {
            Ok(mut stream) => {
                let mut col_map = LinkedHashMap::new();
                let mut rows = Vec::new();
                let columns = stream.columns().await;
                match columns {
                    Ok(Some(columns)) => {
                        for column in columns {
                            col_map.insert(column.name().to_string(), column.column_type());
                        }
                    }
                    Ok(None) => {
                        anyhow::bail!("no columns");
                    }
                    Err(e) => {
                        anyhow::bail!("failed to get columns, cause: {}", e.to_string());
                    }
                }
                loop {
                    let item = stream.try_next().await;
                    match item {
                        Ok(Some(item)) => match item {
                            QueryItem::Row(row) => {
                                rows.push(row);
                            }
                            QueryItem::Metadata(_) => {}
                        },
                        Ok(None) => {
                            break;
                        }
                        Err(e) => anyhow::bail!(
                            "failed to select distinct values, cause: {}",
                            e.to_string()
                        ),
                    }
                }
                Ok((col_map, rows))
            }
            Err(err) => anyhow::bail!(
                "failed to select distinct values, cause: {}",
                err.to_string()
            ),
        }
    }

    pub async fn select_for_schema(
        &mut self,
        sql: &str,
    ) -> anyhow::Result<LinkedHashMap<String, ColumnType>> {
        // get a connection
        let mut conn = self.pool.get().await?;
        // select data
        let result = conn.query(sql, &[]).await;
        match result {
            Ok(mut stream) => {
                let mut col_map = LinkedHashMap::new();
                let columns = stream.columns().await;
                match columns {
                    Ok(Some(columns)) => {
                        for column in columns {
                            col_map.insert(column.name().to_string(), column.column_type());
                        }
                    }
                    Ok(None) => {
                        anyhow::bail!("no columns");
                    }
                    Err(e) => {
                        anyhow::bail!("failed to get columns, cause: {}", e.to_string());
                    }
                }
                Ok(col_map)
            }
            Err(e) => {
                anyhow::bail!("failed to execute query, cause: {}", e.to_string());
            }
        }
    }

    #[allow(dead_code)]
    pub async fn select_all(
        &mut self,
        sql: &str,
    ) -> anyhow::Result<(LinkedHashMap<String, ColumnType>, Vec<Row>)> {
        // get a connection
        let mut conn = self.pool.get().await?;
        // select data
        let result = conn.query(sql, &[]).await;
        match result {
            Ok(mut stream) => {
                let mut col_map = LinkedHashMap::new();
                let mut rows = Vec::new();
                let columns = stream.columns().await;
                match columns {
                    Ok(Some(columns)) => {
                        for column in columns {
                            col_map.insert(column.name().to_string(), column.column_type());
                        }
                    }
                    Ok(None) => {
                        anyhow::bail!("no columns");
                    }
                    Err(e) => {
                        anyhow::bail!("failed to get columns, cause: {}", e.to_string());
                    }
                }
                loop {
                    let item = stream.try_next().await;
                    match item {
                        Ok(Some(item)) => match item {
                            QueryItem::Row(row) => {
                                rows.push(row);
                            }
                            QueryItem::Metadata(_) => {}
                        },
                        Ok(None) => {
                            break;
                        }
                        Err(e) => anyhow::bail!("failed to select data, cause: {}", e.to_string()),
                    }
                }
                Ok((col_map, rows))
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
    }

    pub async fn select_all_and_to_record_batches(
        &mut self,
        sql: &str,
        batch_size: usize,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        // get a connection
        let mut conn = self.pool.get().await?;
        // select data
        let result = conn.query(sql, &[]).await;
        match result {
            Ok(mut stream) => {
                let mut col_map = LinkedHashMap::new();
                let mut rows = Vec::new();
                let columns = stream.columns().await;
                match columns {
                    Ok(Some(columns)) => {
                        for column in columns {
                            col_map.insert(column.name().to_string(), column.column_type());
                        }
                    }
                    Ok(None) => {
                        anyhow::bail!("no columns");
                    }
                    Err(e) => {
                        anyhow::bail!("failed to get columns, cause: {}", e.to_string());
                    }
                }
                loop {
                    let item = stream.try_next().await;
                    match item {
                        Ok(Some(item)) => match item {
                            QueryItem::Row(row) => {
                                rows.push(row);
                            }
                            QueryItem::Metadata(_) => {}
                        },
                        Ok(None) => {
                            break;
                        }
                        Err(e) => anyhow::bail!("failed to select data, cause: {}", e.to_string()),
                    }
                }
                let batch =
                    appender::to_record_batches(col_map, rows, batch_size, self.time_zone.clone())?;
                Ok(batch)
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
    }

    pub async fn top_n(
        &mut self,
        sql: &str,
        top_n: u32,
    ) -> anyhow::Result<(LinkedHashMap<String, ColumnType>, Vec<Row>)> {
        // get a connection
        let mut conn = self.pool.get().await?;
        // select data
        let result = conn.query(sql, &[]).await;
        match result {
            Ok(mut stream) => {
                let mut col_map = LinkedHashMap::new();
                let mut rows = Vec::new();
                let columns = stream.columns().await;
                match columns {
                    Ok(Some(columns)) => {
                        for column in columns {
                            col_map.insert(column.name().to_string(), column.column_type());
                        }
                    }
                    Ok(None) => {
                        anyhow::bail!("no columns");
                    }
                    Err(e) => {
                        anyhow::bail!("failed to get columns, cause: {}", e.to_string());
                    }
                }
                loop {
                    let item = stream.try_next().await;
                    match item {
                        Ok(Some(item)) => match item {
                            QueryItem::Row(row) => {
                                if rows.len() >= top_n as usize {
                                    break;
                                }
                                rows.push(row);
                            }
                            QueryItem::Metadata(_) => {}
                        },
                        Ok(None) => {
                            break;
                        }
                        Err(e) => anyhow::bail!("failed to select data, cause: {}", e.to_string()),
                    }
                }
                Ok((col_map, rows))
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

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
                    let sql_insert_data = format!(
                        "insert into t_metric (id, name, value, ts) values ({}, 'cpu', 0.8, GETDATE())",
                        i
                    );
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
    #[ignore]
    async fn test_connect() {
        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        );
        let config = ConnectConfig::from_dsn(&dsn.unwrap()).unwrap();
        dbg!(&config);

        let query = MssqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        dbg!(query.pool.get().await.is_ok());
    }

    #[ignore]
    #[tokio::test]
    async fn test_select_distinct_values() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query
                    .select_distinct_values("select distinct name,value from t_metric")
                    .await;
                match query_result {
                    Ok((col_map, rows)) => {
                        dbg!(col_map);
                        dbg!(&rows);
                        assert_eq!(rows.len(), 7);
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

    #[ignore]
    #[tokio::test]
    async fn test_select_for_schema() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(1).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.select_for_schema("select * from t_metric").await;
                match query_result {
                    Ok(col_map) => {
                        dbg!(&col_map);
                        assert_eq!(col_map.len(), 4);
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

    #[ignore]
    #[tokio::test]
    async fn test_select_all() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(7).await;

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
                    Ok((col_map, rows)) => {
                        dbg!(col_map);
                        dbg!(&rows);
                        assert_eq!(rows.len(), 7);
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

    #[ignore]
    #[tokio::test]
    async fn test_select_all_and_to_record_batches() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query
                    .select_all_and_to_record_batches("select * from t_metric", 3)
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

    #[ignore]
    #[tokio::test]
    async fn test_top_n() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(3).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.top_n("select * from t_metric", 5).await;
                match query_result {
                    Ok((col_map, rows)) => {
                        dbg!(col_map);
                        dbg!(&rows);
                        assert_eq!(rows.len(), 3);
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

    #[ignore]
    #[tokio::test]
    async fn test_top_n_with_tz() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(3).await;

        let dsn = Dsn::from_str(
            "mssql://test:123456@192.168.1.66:1433/test_taosx?encryption=On&trust_cert=true",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MssqlQuery::try_new(config, String::from("+06:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.top_n("select * from t_metric", 5).await;
                match query_result {
                    Ok((col_map, rows)) => {
                        dbg!(col_map);
                        dbg!(&rows);
                        assert_eq!(rows.len(), 3);
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
    }
}
