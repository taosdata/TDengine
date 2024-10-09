use std::collections::HashMap;
use std::pin::Pin;

use futures::stream::IntoStream;
use futures::{StreamExt, TryStreamExt};
use sqlx::mysql::{MySqlConnectOptions, MySqlRow};
use sqlx::{Error, Executor, MySql, MySqlPool, Pool, Row};

use crate::runners::mysql::config::connect::ConnectConfig;

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
                .map(|row| row.try_get::<String, _>(0).unwrap())
                .collect(),
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
                .map(|row| {
                    (
                        row.try_get::<String, _>(0).unwrap(),
                        row.try_get::<String, _>(1).unwrap(),
                    )
                })
                .collect(),
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
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/information_schema").unwrap();
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

    async fn test_create_table() {
        let _ = test_create_database().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_create_table = "create table if not exists t_metric (id int primary key auto_increment, name varchar(255), value double, ts timestamp)";
                let _ = query.pool.execute(sql_create_table).await;
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_insert_data(len: usize) {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(query) => {
                let sql_insert_data =
                    "insert into t_metric (name, value, ts) values ('cpu', 0.8, now())";
                for _ in 0..len {
                    let _ = query.pool.execute(sql_insert_data).await;
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    async fn test_clear_data() {
        let _ = test_create_table().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = MySqlQuery::try_new(config, String::from("+08:00")).await;
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
    async fn test_connect() {
        // prepare data
        let _ = test_create_database().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx");
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
        let _ = test_create_table().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let tables = query.show_tables().await.unwrap();
        assert!(tables.contains(&"t_metric".to_string()));
    }

    #[tokio::test]
    async fn test_show_columns() {
        // prepare data
        let _ = test_create_table().await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let columns = query.show_columns("t_metric").await.unwrap();
        assert_eq!(
            columns,
            [
                ("id".to_string(), "int".to_string()),
                ("name".to_string(), "varchar(255)".to_string()),
                ("value".to_string(), "double".to_string()),
                ("ts".to_string(), "timestamp".to_string())
            ]
            .iter()
            .cloned()
            .collect()
        );
    }

    #[tokio::test]
    async fn test_select_distinct_values() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .select_distinct_values("select distinct name,value from t_metric")
            .await
            .unwrap();
        dbg!(&rows);
        // clear data
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    async fn test_select_one_for_schema() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(1).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select * from t_metric")
            .await
            .unwrap();
        assert!(row.is_some());
        // clear data
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    async fn test_select_all() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query.select_all("select * from t_metric").await.unwrap();
        assert_eq!(rows.len(), 7);
        // clear data
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    async fn test_select_by_stream() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(7).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let mut stream = query.select_by_stream("select * from t_metric");

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
        assert_eq!(rows.len(), 7);
        // clear data
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    async fn test_top_n() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(3).await;

        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query.top_n("select * from t_metric", 5).await.unwrap();
        dbg!(&rows);
        assert_eq!(rows.len(), 3);
        // clear data
        let _ = test_clear_data().await;
    }

    #[tokio::test]
    async fn test_charset() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(3).await;

        // gbk, not match the charset in mysql
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?charset=gbk").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select name from t_metric")
            .await
            .unwrap();
        match row {
            Some(row) => {
                let val = row.try_get::<String, _>(0);
                assert!(val.is_err());
                assert!(val.err().unwrap().to_string().contains("mismatched types"));
            }
            None => {
                println!("no data");
            }
        }

        // utf8, match the charset in mysql
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?charset=utf8").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select name from t_metric")
            .await
            .unwrap();
        match row {
            Some(row) => {
                let val = row.try_get::<String, _>(0);
                assert!(val.is_ok());
                println!("name: {}", val.unwrap());
            }
            None => {
                println!("no data");
            }
        }
        // clear data
        let _ = test_clear_data().await;
    }

    /// mysql> show variables like 'require_secure_transport'; ---OFF
    #[tokio::test]
    #[ignore]
    async fn test_ssl_require_secure_off() {
        // test: ssl_mode=DISABLED
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=DISABLED")
                .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=PREFERRED
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=PREFERRED")
                .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=REQUIRED
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=REQUIRED")
                .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=VERIFY_CA
        let dsn =
            Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=VERIFY_CA&ssl_ca=/tmp/mysql/ca.pem")
                .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=VERIFY_IDENTITY
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=VERIFY_IDENTITY&ssl_ca=/tmp/mysql/ca.pem&ssl_client_cert=/tmp/mysql/client-cert.pem&ssl_client_key=/tmp/mysql/client-key.pem")
            .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = MySqlQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());
    }

    /// mysql> show variables like 'require_secure_transport'; ---OFF
    #[tokio::test]
    #[ignore]
    async fn test_ssl_require_secure_on() {
        // // test: ssl_mode=DISABLED
        // let dsn =
        //     Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=DISABLED")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=PREFERRED
        // let dsn = Dsn::from_str(
        //     "mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=PREFERRED",
        // )
        // .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=REQUIRED
        // let dsn =
        //     Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=REQUIRED")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=VERIFY_CA
        // let dsn =
        //     Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=VERIFY_CA")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=VERIFY_IDENTITY
        // let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?ssl_mode=VERIFY_IDENTITY")
        //     .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = MySqlQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());
    }
}
