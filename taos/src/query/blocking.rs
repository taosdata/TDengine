use serde::{
    de::{DeserializeOwned, IntoDeserializer},
    Deserialize,
};

use crate::helpers::*;

use super::ResultSetProps;

pub trait Block<'de>: Sized {
    type Error: serde::de::Error;
    type Row: IntoDeserializer<'de, Self::Error>;
    type RowsIter: Iterator<Item = Self::Row>;

    fn rows_iter(self) -> Self::RowsIter;
}

// pub trait SyncResultSet<'de>: ResultSetProps {
//     type Error: serde::de::Error;
//     type Row: IntoDeserializer<'de, Self::Error>;

//     type Block: IntoIterator<Self::Row>;
//     type Iter: Iterator<Item = Self::Block>;
//     type RowsIter: Iterator<Item = <Self::Block as Block<'de>>::Row>;

//     fn blocks_iter(&self) -> Self::Iter;

//     fn rows_iter(&self) -> SyncResultSetRowsIter<'de, Self> {
//         self.blocks_iter().flat_map(Block::rows_iter)
//     }
// }

pub trait SyncQuery {
    type Error;
    type ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

    fn describe(&self, table: &str) -> Result<ColumnMeta, Self::Error>;

    fn exec<T: AsRef<str>>(&self, sql: T) -> Result<usize, Self::Error> {
        self.query(sql).map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }

    fn create_database<I: Into<DatabaseProperties>>(
        &self,
        name: &str,
        opts: I,
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

    #[inline]
    fn exec_sync(&self, sql: &str) -> Result<usize, Self::Error> {
        self.exec(sql)
    }

    #[inline]
    fn query_sync<T: AsRef<str>>(
        &self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, Self::Error> {
        self.query(sql)
    }
}
