use std::pin::Pin;

use futures::stream::IntoStream;
use futures::{StreamExt, TryStreamExt};
use sqlx::{Error, Executor, Pool};
use sqlx_postgres::{PgConnectOptions, PgPool, PgRow, Postgres};

use crate::config::connect::ConnectConfig;

#[derive(Clone)]
pub struct PostgresQuery {
    pub pool: Pool<Postgres>,
}

impl PostgresQuery {
    pub async fn try_new(config: ConnectConfig, time_zone: String) -> anyhow::Result<Self> {
        let pool = Self::connect(
            &config.host,
            config.port,
            &config.subject,
            &config.username,
            &config.password,
            &config.application_name,
            &config.ssl_mode,
            &config.ssl_ca,
            &config.ssl_client_cert,
            &config.ssl_client_key,
            time_zone,
        )
        .await
        .map_err(|err| {
            anyhow::anyhow!("failed to connect to postgres, cause: {}", err.to_string())
        })?;
        Ok(Self { pool })
    }

    async fn connect(
        host: &str,
        port: u16,
        subject: &str,
        username: &str,
        password: &str,
        application_name: &str,
        ssl_mode: &str,
        ssl_ca: &Option<String>,
        ssl_client_cert: &Option<String>,
        ssl_client_key: &Option<String>,
        _time_zone: String,
    ) -> anyhow::Result<Pool<Postgres>> {
        let mut options = PgConnectOptions::new()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .database(subject)
            .application_name(application_name);
        match ssl_mode {
            "DISABLE" => {
                options = options.ssl_mode(sqlx_postgres::PgSslMode::Disable);
                Ok(PgPool::connect_with(options).await?)
            }
            "ALLOW" => {
                options = options.ssl_mode(sqlx_postgres::PgSslMode::Allow);
                Ok(PgPool::connect_with(options).await?)
            }
            "PREFER" => {
                options = options.ssl_mode(sqlx_postgres::PgSslMode::Prefer);
                Ok(PgPool::connect_with(options).await?)
            }
            "REQUIRE" => {
                options = options.ssl_mode(sqlx_postgres::PgSslMode::Require);
                Ok(PgPool::connect_with(options).await?)
            }
            "VERIFY_CA" => {
                options = options.ssl_mode(sqlx_postgres::PgSslMode::VerifyCa);
                if let Some(ca) = ssl_ca {
                    options = options.ssl_root_cert(ca.as_str());
                }
                Ok(PgPool::connect_with(options).await?)
            }
            "VERIFY_FULL" => {
                options = options.ssl_mode(sqlx_postgres::PgSslMode::VerifyFull);
                if let Some(ca) = ssl_ca {
                    options = options.ssl_root_cert(ca.as_str());
                }
                if let Some(cert) = ssl_client_cert {
                    options = options.ssl_client_cert(cert.as_str());
                }
                if let Some(key) = ssl_client_key {
                    options = options.ssl_client_key(key.as_str());
                }
                Ok(PgPool::connect_with(options).await?)
            }
            _ => Err(anyhow::anyhow!("unsupported ssl mode: {}", ssl_mode)),
        }
        // TODO timezone
    }

    pub async fn select_distinct_values(&mut self, sql: &str) -> anyhow::Result<Vec<PgRow>> {
        let result = self.pool.fetch_all(sql).await;
        match result {
            Ok(rows) => Ok(rows),
            Err(err) => anyhow::bail!(
                "failed to select distinct values, cause: {}",
                err.to_string()
            ),
        }
    }

    pub async fn select_one_for_schema(&mut self, sql: &str) -> anyhow::Result<Option<PgRow>> {
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
    pub async fn select_all(&mut self, sql: &str) -> anyhow::Result<Vec<PgRow>> {
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
    ) -> IntoStream<Pin<Box<dyn futures::Stream<Item = Result<PgRow, Error>> + Send + 'a>>> {
        self.pool.fetch(sql).into_stream()
    }

    pub async fn top_n(&mut self, sql: &str, top_n: u32) -> anyhow::Result<Vec<PgRow>> {
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
                    let sql_insert_data = format!(
                        "insert into t_metric (id, name, value, ts) values ({}, 'cpu', 0.8, CURRENT_TIMESTAMP)",
                        i
                    );
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
    async fn test_connect() {
        // prepare data
        let _ = test_create_database().await;

        let dsn = Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx");
        let config = ConnectConfig::from_dsn(&dsn.unwrap()).unwrap();
        dbg!(&config);

        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());
    }

    #[ignore]
    #[tokio::test]
    async fn test_select_one_for_schema() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_insert_data(1).await;

        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let row = query
                    .select_one_for_schema("select * from t_metric")
                    .await
                    .unwrap();
                assert!(row.is_some());
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
        let _ = test_clear_data().await;
        let _ = test_insert_data(3).await;

        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.select_all("select * from t_metric").await;
                match query_result {
                    Ok(rows) => {
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
    async fn test_select_by_stream() {
        // prepare data
        let _ = test_create_table().await;
        let _ = test_clear_data().await;
        let _ = test_insert_data(3).await;

        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
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
                assert_eq!(rows.len(), 3);
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
        let _ = test_clear_data().await;
        let _ = test_insert_data(7).await;

        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = PostgresQuery::try_new(config, String::from("+08:00")).await;
        match result {
            Ok(mut query) => {
                let query_result = query.top_n("select * from t_metric", 3).await;
                match query_result {
                    Ok(rows) => {
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

    /// postgres> show variables like 'require_secure_transport'; ---OFF
    #[tokio::test]
    #[ignore]
    async fn test_ssl_require_secure_off() {
        // test: ssl_mode=DISABLE
        let dsn = Dsn::from_str(
            "postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx?ssl_mode=DISABLE",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=ALLOW
        let dsn = Dsn::from_str(
            "postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx?ssl_mode=ALLOW",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=PREFER
        let dsn = Dsn::from_str(
            "postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx?ssl_mode=PREFER",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=REQUIRE
        let dsn = Dsn::from_str(
            "postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx?ssl_mode=REQUIRE",
        )
        .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=VERIFY_CA
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx?ssl_mode=VERIFY_CA&ssl_ca=/tmp/postgres/ca.pem")
                .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=VERIFY_IDENTITY
        let dsn = Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/test_taosx?ssl_mode=VERIFY_FULL&ssl_ca=/tmp/postgres/ca.pem&ssl_client_cert=/tmp/postgres/client-cert.pem&ssl_client_key=/tmp/postgres/client-key.pem")
            .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());
    }

    /// postgres> show variables like 'require_secure_transport'; ---OFF
    #[tokio::test]
    #[ignore]
    async fn test_ssl_require_secure_on() {
        // // test: ssl_mode=DISABLED
        // let dsn =
        //     Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=DISABLE")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = PostgresQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=PREFERRED
        // let dsn = Dsn::from_str(
        //     "postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=PREFER",
        // )
        // .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = PostgresQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=REQUIRED
        // let dsn =
        //     Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=REQUIRED")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = PostgresQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=VERIFY_CA
        // let dsn =
        //     Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=VERIFY_CA")
        //         .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = PostgresQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());

        // // test: ssl_mode=VERIFY_IDENTITY
        // let dsn = Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=VERIFY_IDENTITY")
        //     .unwrap();
        // let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // // dbg!(&config);
        // let query = PostgresQuery::try_new(config).await.unwrap();
        // assert!(!query.pool.is_closed());
    }
}
