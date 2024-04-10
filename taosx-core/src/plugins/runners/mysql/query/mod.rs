use std::collections::HashMap;
use std::pin::Pin;

use futures::stream::IntoStream;
use futures::{StreamExt, TryStreamExt};
use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::{Error, Executor, MySql, Pool, Row};

use crate::runners::mysql::config::connect::ConnectConfig;

pub struct MySqlQuery {
    pool: Pool<MySql>,
}

impl MySqlQuery {
    pub async fn try_new(config: ConnectConfig) -> anyhow::Result<Self> {
        let pool = Self::connect(
            &config.host,
            config.port,
            &config.subject,
            &config.username,
            &config.password,
        )
        .await
        .map_err(|err| anyhow::anyhow!("failed to connect to mysql, cause: {}", err.to_string()))?;
        Ok(Self { pool })
    }

    async fn connect(
        host: &String,
        port: u16,
        subject: &String,
        username: &String,
        password: &String,
    ) -> anyhow::Result<Pool<MySql>> {
        let db_url = format!(
            "mysql://{}:{}@{}:{}/{}",
            username, password, host, port, subject
        );
        let pool = MySqlPoolOptions::new().connect(&db_url.as_str()).await?;
        Ok(pool)
    }

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

    pub async fn select_all(&mut self, sql: &str) -> anyhow::Result<Vec<MySqlRow>> {
        let result = self.pool.fetch_all(sql).await;
        match result {
            Ok(rows) => Ok(rows),
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
    }

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
                    rows.push(row);
                    if rows.len() >= top_n as usize {
                        break;
                    }
                }
                Err(e) => {
                    println!("error: {:?}", e);
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

    #[tokio::test]
    async fn test_show_tables() {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_connector").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config).await.unwrap();

        let tables = query.show_tables().await.unwrap();
        dbg!(tables);
    }

    #[tokio::test]
    async fn test_show_columns() {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_connector").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config).await.unwrap();

        let columns = query.show_columns("t_full_columns").await.unwrap();
        dbg!(columns);
    }

    #[tokio::test]
    async fn test_select_one_for_schema() {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_connector").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config).await.unwrap();

        let row = query
            .select_one_for_schema("select * from t_full_columns")
            .await
            .unwrap();
        match row {
            Some(row) => {
                dbg!(row);
            }
            None => {
                println!("no data");
            }
        }
    }

    #[tokio::test]
    async fn test_select_all() {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_connector").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config).await.unwrap();

        let rows = query
            .select_all("select * from t_full_columns")
            .await
            .unwrap();
        dbg!(rows);
    }

    #[tokio::test]
    async fn test_select_by_stream() {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_connector").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config).await.unwrap();

        let mut stream = query.select_by_stream("select * from t_full_columns");
        while let Some(result) = stream.next().await {
            match result {
                Ok(row) => {
                    println!("row: {:?}", row);
                }
                Err(e) => {
                    println!("error: {:?}", e);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_top_n() {
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_connector").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = MySqlQuery::try_new(config).await.unwrap();

        let rows = query
            .top_n("select * from t_full_columns", 1)
            .await
            .unwrap();
        dbg!(&rows);
        assert_eq!(rows.len(), 1);
    }
}
