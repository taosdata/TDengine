use std::pin::Pin;

use futures::stream::IntoStream;
use futures::{StreamExt, TryStreamExt};
use sqlx::{Error, Executor, Pool};
use sqlx_postgres::{PgConnectOptions, PgPool, PgRow, Postgres};

use crate::runners::postgres::config::connect::ConnectConfig;

pub struct PostgresQuery {
    pool: Pool<Postgres>,
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
        host: &String,
        port: u16,
        subject: &String,
        username: &String,
        password: &String,
        application_name: &String,
        ssl_mode: &String,
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
        match ssl_mode.as_str() {
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
    use sqlx::Row;
    use std::str::FromStr;
    use taos::Dsn;

    #[tokio::test]
    async fn test_connect() {
        let dsn = Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres");
        let config = ConnectConfig::from_dsn(&dsn.unwrap()).unwrap();
        dbg!(&config);

        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());
    }

    #[tokio::test]
    async fn test_select_one_for_schema() {
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let row = query
            .select_one_for_schema("select * from information_schema.tables")
            .await
            .unwrap();
        match row {
            Some(row) => {
                dbg!(row.len());
            }
            None => {
                println!("no data");
            }
        }
    }

    #[tokio::test]
    async fn test_select_all() {
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .select_all("select * from information_schema.tables")
            .await
            .unwrap();
        dbg!(rows.len());
    }

    #[tokio::test]
    async fn test_select_by_stream() {
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let mut stream = query.select_by_stream("select * from information_schema.tables");
        while let Some(result) = stream.next().await {
            match result {
                Ok(row) => {
                    println!("row: {:?}", row.len());
                }
                Err(e) => {
                    println!("error: {:?}", e);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_top_n() {
        let dsn =
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();

        let rows = query
            .top_n("select * from information_schema.tables", 1)
            .await
            .unwrap();
        // dbg!(&rows);
        assert_eq!(rows.len(), 1);
    }

    /// postgres> show variables like 'require_secure_transport'; ---OFF
    #[tokio::test]
    async fn test_ssl_require_secure_off() {
        // test: ssl_mode=DISABLE
        let dsn = Dsn::from_str(
            "postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=DISABLE",
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
            "postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=ALLOW",
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
            "postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=PREFER",
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
            "postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=REQUIRE",
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
            Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=VERIFY_CA&ssl_ca=/tmp/postgres/ca.pem")
                .unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        // dbg!(&config);
        let query = PostgresQuery::try_new(config, String::from("+08:00"))
            .await
            .unwrap();
        assert!(!query.pool.is_closed());

        // test: ssl_mode=VERIFY_IDENTITY
        let dsn = Dsn::from_str("postgres://postgres:tbase125!@192.168.1.40:5432/postgres?ssl_mode=VERIFY_FULL&ssl_ca=/tmp/postgres/ca.pem&ssl_client_cert=/tmp/postgres/client-cert.pem&ssl_client_key=/tmp/postgres/client-key.pem")
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
