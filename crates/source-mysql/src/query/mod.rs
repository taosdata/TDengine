use std::collections::HashMap;
use std::pin::Pin;

use futures::stream::IntoStream;
use futures::{StreamExt, TryStreamExt};
use sqlx::mysql::{MySqlConnectOptions, MySqlRow};
use sqlx::{Column, Error, Executor, MySql, MySqlPool, Pool, Row, TypeInfo};
use taos::Itertools;

use crate::config::connect::ConnectConfig;

use super::generate_json_value;

#[derive(Clone)]
pub struct MySqlQuery {
    pub pool: Pool<MySql>,
}

impl MySqlQuery {
    pub async fn try_new(config: ConnectConfig, time_zone: String) -> anyhow::Result<Self> {
        let pool = Self::connect(
            &config.host,
            config.port,
            &config.subject,
            &config.username,
            &config.password,
            &config.charset,
            &config.ssl_mode,
            &config.ssl_ca,
            &config.ssl_client_cert,
            &config.ssl_client_key,
            time_zone,
        )
        .await
        .map_err(|err| anyhow::anyhow!("failed to connect to mysql, cause: {}", err.to_string()))?;
        Ok(Self { pool })
    }

    async fn connect(
        host: &str,
        port: u16,
        subject: &str,
        username: &str,
        password: &str,
        charset: &str,
        ssl_mode: &str,
        ssl_ca: &Option<String>,
        ssl_client_cert: &Option<String>,
        ssl_client_key: &Option<String>,
        time_zone: String,
    ) -> anyhow::Result<Pool<MySql>> {
        let mut options = MySqlConnectOptions::new()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .database(subject)
            .charset(charset)
            .timezone(time_zone);
        match ssl_mode {
            "DISABLED" => {
                options = options.ssl_mode(sqlx::mysql::MySqlSslMode::Disabled);
                Ok(MySqlPool::connect_with(options).await?)
            }
            "PREFERRED" => {
                options = options.ssl_mode(sqlx::mysql::MySqlSslMode::Preferred);
                Ok(MySqlPool::connect_with(options).await?)
            }
            "REQUIRED" => {
                options = options.ssl_mode(sqlx::mysql::MySqlSslMode::Required);
                Ok(MySqlPool::connect_with(options).await?)
            }
            "VERIFY_CA" => {
                options = options.ssl_mode(sqlx::mysql::MySqlSslMode::VerifyCa);
                if let Some(ca) = ssl_ca {
                    options = options.ssl_ca(ca.as_str());
                }
                Ok(MySqlPool::connect_with(options).await?)
            }
            "VERIFY_IDENTITY" => {
                options = options.ssl_mode(sqlx::mysql::MySqlSslMode::VerifyIdentity);
                if let Some(ca) = ssl_ca {
                    options = options.ssl_ca(ca.as_str());
                }
                if let Some(cert) = ssl_client_cert {
                    options = options.ssl_client_cert(cert.as_str());
                }
                if let Some(key) = ssl_client_key {
                    options = options.ssl_client_key(key.as_str());
                }
                Ok(MySqlPool::connect_with(options).await?)
            }
            _ => Err(anyhow::anyhow!("unsupported ssl mode: {}", ssl_mode)),
        }
    }

    #[allow(dead_code)]
    pub async fn show_tables(&mut self) -> anyhow::Result<Vec<String>> {
        let result = self.pool.fetch_all("SHOW TABLES").await;
        let tables = match result {
            Ok(rows) => rows
                .iter()
                .map(|row| -> anyhow::Result<String> {
                    let col0 = row.column(0);
                    let col0_type = col0.type_info().name();
                    if matches!(col0_type, "BINARY" | "VARBINARY") {
                        let val = row.try_get::<Option<&[u8]>, _>(0)?;
                        return Ok(val
                            .and_then(|s| String::from_utf8(s.to_vec()).ok())
                            .unwrap_or_else(|| "null".to_string()));
                    }
                    let col0_value = generate_json_value(row, col0_type, 0, "".to_string())?;
                    Ok(col0_value.as_str().unwrap().to_string())
                })
                .try_collect()?,
            Err(err) => anyhow::bail!("failed to show tables, cause: {}", err.to_string()),
        };
        Ok(tables)
    }

    #[allow(dead_code)]
    pub async fn show_columns(&mut self, table: &str) -> anyhow::Result<HashMap<String, String>> {
        let result = self
            .pool
            .fetch_all(format!("SHOW COLUMNS FROM {}", table).as_str())
            .await;
        let columns = match result {
            Ok(rows) => rows
                .iter()
                .map(|row| -> anyhow::Result<(String, String)> {
                    let col0 = row.column(0);
                    let col1 = row.column(1);
                    let col0_type = col0.type_info().name();
                    let col1_type = col1.type_info().name();
                    let col0_value = generate_json_value(row, col0_type, 0, "".to_string())?;
                    let col1_value = generate_json_value(row, col1_type, 1, "".to_string())?;
                    Ok((
                        col0_value.as_str().unwrap().to_string(),
                        col1_value.as_str().unwrap().to_string(),
                    ))
                })
                .try_collect()?,
            Err(err) => anyhow::bail!("failed to show columns, cause: {}", err.to_string()),
        };
        Ok(columns)
    }

    pub async fn select_distinct_values(&mut self, sql: &str) -> anyhow::Result<Vec<MySqlRow>> {
        let result = self.pool.fetch_all(sql).await;
        match result {
            Ok(rows) => Ok(rows),
            Err(err) => anyhow::bail!(
                "failed to select distinct values, cause: {}",
                err.to_string()
            ),
        }
    }

    pub async fn select_one_for_schema(&mut self, sql: &str) -> anyhow::Result<Option<MySqlRow>> {
        let result = self.pool.fetch_optional(sql).await;
        Ok(match result {
            Ok(Some(row)) => Some(row),
            Ok(None) => None,
            Err(e) => {
                anyhow::bail!("failed to execute query, cause: {}", e.to_string());
            }
        })
    }

    #[allow(dead_code)]
    pub async fn select_all(&mut self, sql: &str) -> anyhow::Result<Vec<MySqlRow>> {
        let result = self.pool.fetch_all(sql).await;
        match result {
            Ok(rows) => Ok(rows),
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn select_by_stream<'a>(
        &mut self,
        sql: &'a str,
    ) -> IntoStream<Pin<Box<dyn futures::Stream<Item = Result<MySqlRow, Error>> + Send + 'a>>> {
        self.pool.fetch(sql).into_stream()
    }

    pub async fn top_n(&mut self, sql: &str, top_n: u32) -> anyhow::Result<Vec<MySqlRow>> {
        let mut stream = self.pool.fetch(sql).into_stream();

        let mut rows = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(row) => {
                    if rows.len() >= top_n as usize {
                        break;
                    }
                    rows.push(row);
                }
                Err(e) => {
                    anyhow::bail!("error: {:?}", e);
                }
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

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
                    "insert into {table_name} (name, value, ts) values ('中文', 0.8, now())"
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
    async fn test_connect() {
        // prepare data
        let _ = test_create_database().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci");
        let config = ConnectConfig::from_dsn(&dsn.unwrap()).unwrap();
        dbg!(&config);

        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());
    }

    #[tokio::test]
    async fn test_show_tables() {
        // prepare data
        let _ = test_create_table("test_show_tables").await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let tables = query.show_tables().await.unwrap();
        assert!(
            tables.contains(&"test_show_tables".to_string())
                || tables.contains(&"[116, 95, 109, 101, 116, 114, 105, 99]".to_string())
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_show_columns() {
        // prepare data
        let _ = test_create_table("test_show_columns").await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let columns = query.show_columns("test_show_columns").await.unwrap();
        assert!(
            columns.eq(&[
                ("id".to_string(), "int".to_string()),
                ("name".to_string(), "varchar".to_string()),
                ("value".to_string(), "double".to_string()),
                ("ts".to_string(), "timestamp".to_string())
            ]
            .iter()
            .cloned()
            .collect())
                || columns.eq(&[
                    ("id".to_string(), "[105, 110, 116]".to_string()),
                    (
                        "name".to_string(),
                        "[118, 97, 114, 99, 104, 97, 114, 40, 50, 53, 53, 41]".to_string()
                    ),
                    (
                        "value".to_string(),
                        "[100, 111, 117, 98, 108, 101]".to_string()
                    ),
                    (
                        "ts".to_string(),
                        "[116, 105, 109, 101, 115, 116, 97, 109, 112]".to_string()
                    )
                ]
                .iter()
                .cloned()
                .collect())
        );
    }

    #[tokio::test]
    async fn test_select_distinct_values_with_datasource() {
        // prepare data
        let _ = test_create_table("test_select_distinct_values").await;
        let _ = test_insert_data("test_select_distinct_values", 7).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .select_distinct_values("select distinct name,value from test_select_distinct_values")
            .await
            .unwrap();
        dbg!(&rows);
        // clear data
        let _ = test_clear_data("test_select_distinct_values").await;
    }

    #[tokio::test]
    async fn test_select_one_for_schema_with_datasource() {
        // prepare data
        let _ = test_create_table("test_select_one_for_schema").await;
        let _ = test_insert_data("test_select_one_for_schema", 1).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select * from test_select_one_for_schema")
            .await
            .unwrap();
        assert!(row.is_some());
        // clear data
        let _ = test_clear_data("test_select_one_for_schema").await;
    }

    #[tokio::test]
    async fn test_select_all_with_datasource() {
        // prepare data
        let _ = test_create_table("test_select_all").await;
        let _ = test_insert_data("test_select_all", 7).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .select_all("select * from test_select_all")
            .await
            .unwrap();
        dbg!(&rows.len());
        // assert_eq!(rows.len(), 7);
        // clear data
        let _ = test_clear_data("test_select_all").await;
    }

    #[tokio::test]
    async fn test_select_by_stream_with_datasource() {
        // prepare data
        let _ = test_create_table("test_select_by_stream").await;
        let _ = test_insert_data("test_select_by_stream", 7).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let mut stream = query.select_by_stream("select * from test_select_by_stream");

        let mut rows = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(row) => {
                    rows.push(row);
                }
                Err(e) => {
                    println!("error: {:?}", e);
                }
            }
        }
        dbg!(&rows.len());
        // assert_eq!(rows.len(), 7);
        // clear data
        let _ = test_clear_data("test_select_by_stream").await;
    }

    #[tokio::test]
    async fn test_top_n_with_datasource() {
        // prepare data
        let _ = test_create_table("test_top_n").await;
        let _ = test_insert_data("test_top_n", 3).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query.top_n("select * from test_top_n", 5).await.unwrap();
        dbg!(&rows.len());
        // assert_eq!(rows.len(), 3);
        // clear data
        let _ = test_clear_data("test_top_n").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_charset() {
        // prepare data
        let _ = test_create_table("test_charset").await;
        let _ = test_insert_data("test_charset", 3).await;

        // gbk, not match the charset in mysql
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?charset=gbk").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select name from test_charset")
            .await
            .unwrap();
        match row {
            Some(row) => {
                let val = row.try_get::<String, _>(0);
                assert!(val.is_err());
            }
            None => {
                println!("no data");
            }
        }

        // utf8, match the charset in mysql
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?charset=utf8").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select name from test_charset")
            .await
            .unwrap();
        match row {
            Some(row) => {
                let val = row.try_get::<String, _>(0);
                assert!(val.is_ok());
            }
            None => {
                println!("no data");
            }
        }
        // clear data
        let _ = test_clear_data("test_charset").await;
    }

    /// mysql> show variables like 'require_secure_transport'; ---OFF
    #[tokio::test]
    async fn test_ssl_require_secure_off() {
        // test: ssl_mode=DISABLED
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=DISABLED")
            .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=PREFERRED
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=PREFERRED")
            .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=REQUIRED
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=REQUIRED")
            .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=VERIFY_CA
        // let dsn =
        //     Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=VERIFY_CA&ssl_ca=tests/mysql/ca.pem")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // let query = MySqlQuery::try_new(config, String::from("+08:00"))
        //     .await
        //     .unwrap();
        // assert!(!query.pool.is_closed());

        // test: ssl_mode=VERIFY_IDENTITY
        // let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=VERIFY_IDENTITY&ssl_ca=tests/mysql/ca.pem&ssl_client_cert=/tmp/mysql/client-cert.pem&ssl_client_key=tests/mysql/client-key.pem")
        //     .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();

        // let query = MySqlQuery::try_new(config, String::from("+08:00"))
        //     .await
        //     .unwrap();
        // assert!(!query.pool.is_closed());
    }

    /// mysql> show variables like 'require_secure_transport'; ---OFF
    #[tokio::test]
    async fn test_ssl_require_secure_on() {
        // // test: ssl_mode=DISABLED
        // let dsn =
        //     Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=DISABLED")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=PREFERRED
        // let dsn = Dsn::from_str(
        //     "mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=PREFERRED",
        // )
        // .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=REQUIRED
        // let dsn =
        //     Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=REQUIRED")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=VERIFY_CA
        // let dsn =
        //     Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=VERIFY_CA")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=VERIFY_IDENTITY
        // let dsn = Dsn::from_str("mysql://root:123456@192.168.1.45:3306/test_ci?ssl_mode=VERIFY_IDENTITY")
        //     .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());
    }
}
