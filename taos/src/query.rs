use std::fmt::Display;

use async_trait::async_trait;

use crate::helpers::*;


#[async_trait]
pub trait AsyncQuery {
    type Error;
    type ResultSet;

    async fn query(&self, sql: &str) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

    async fn describe(&self, table: &str) -> Result<ColumnMeta, Self::Error>;

    async fn exec(&self, sql: &str) -> Result<usize, Self::Error> {
        self.query(sql).await.map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }

    async fn create_database(
        &self,
        name: &str,
        opts: impl Into<DatabaseOption> + Send + 'async_trait,
    ) -> Result<(), Self::Error> {
        let sql = format!("create database {} if not exists {}", name, opts.into());
        self.exec(&sql).await.map(|_| ())
    }

    async fn use_database(&self, database: &str) -> Result<(), Self::Error> {
        let sql = format!("use database {}", database);
        self.exec(&sql).await.map(|_| ())
    }

    async fn create_table(&self, name: &str) -> Result<(), Self::Error> {
        let sql = format!("create table {}", name);
        self.exec(&sql).await.map(|_| ())
    }
}

pub trait SyncQuery {
    type Error;
    type ResultSet;

    fn query(&self, sql: &str) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

    fn describe(&self, table: &str) -> Result<ColumnMeta, Self::Error>;

    fn exec(&self, sql: &str) -> Result<usize, Self::Error> {
        self.query(sql).map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }

    fn create_database(
        &self,
        name: &str,
        opts: impl Into<DatabaseOption>,
    ) -> Result<(), Self::Error> {
        let sql = format!("create database {} if not exists {}", name, opts.into());
        self.exec(&sql).map(|_| ())
    }

    fn use_database(&self, database: &str) -> Result<(), Self::Error> {
        let sql = format!("use database {}", database);
        self.exec(&sql).map(|_| ())
    }

    fn create_table(&self, name: &str) -> Result<(), Self::Error> {
        let sql = format!("create table {}", name);
        self.exec(&sql).map(|_| ())
    }
}
