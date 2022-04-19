use async_trait::async_trait;
use futures::Stream;
use serde::{de::IntoDeserializer, Deserialize};

use crate::helpers::*;

pub trait ResultSetProps {
    fn num_of_fields(&self) -> usize;

    fn affected_rows(&self) -> usize;

    fn is_update_query(&self) -> bool;

    fn precision(&self) -> Precision;
}

pub trait BlockCore<'de>: Sized {
    type Error: serde::de::Error;
    type Row: IntoDeserializer<'de, Self::Error>;
    type RowsIter: Iterator<Item = Self::Row>;

    fn rows_iter(self) -> Self::RowsIter;

    fn into_stream(self) -> futures::stream::Iter<Self::RowsIter> {
        futures::stream::iter(BlockCore::rows_iter(self))
    }
}

pub type SyncResultSetRowsIter<'de, T> = std::iter::FlatMap<
    <T as SyncResultSet<'de>>::Iter,
    <<T as SyncResultSet<'de>>::Block as BlockCore<'de>>::RowsIter,
    fn(
        <T as SyncResultSet<'de>>::Block,
    ) -> <<T as SyncResultSet<'de>>::Block as BlockCore<'de>>::RowsIter,
>;

pub trait SyncResultSet<'de>: ResultSetProps {
    type Value;
    type Block: BlockCore<'de>;
    type Iter: Iterator<Item = Self::Block>;
    type RowsIter: Iterator<Item = <Self::Block as BlockCore<'de>>::Row>;

    fn blocks_iter(&self) -> Self::Iter;

    fn rows_iter(&self) -> SyncResultSetRowsIter<'de, Self> {
        self.blocks_iter().flat_map(BlockCore::rows_iter)
    }
}

pub type RowIterStream<'de, T> = futures::stream::Iter<<T as BlockCore<'de>>::RowsIter>;

pub type RowsStream<'de, T> = futures::stream::FlatMap<
    <T as AsyncResultSet<'de>>::BlockStream,
    RowIterStream<'de, <T as AsyncResultSet<'de>>::Block>,
    fn(<T as AsyncResultSet<'de>>::Block) -> RowIterStream<'de, <T as AsyncResultSet<'de>>::Block>,
>;

pub trait AsyncResultSet<'de> {
    type Block: BlockCore<'de>;
    type BlockStream: Stream<Item = Self::Block>;

    fn num_of_fields(&self) -> usize;

    fn affected_rows(&self) -> usize;

    fn is_update_query(&self) -> bool;

    fn precision(&self) -> Precision;

    fn blocks(&mut self) -> Self::BlockStream;

    fn rows(
        &'de mut self,
    ) -> futures::stream::FlatMap<
        Self::BlockStream,
        RowIterStream<Self::Block>,
        fn(Self::Block) -> RowIterStream<'de, Self::Block>,
    > {
        use futures::StreamExt;
        self.blocks().flat_map(BlockCore::into_stream)
    }

    fn deserialize_stream<T>(
        &'de mut self,
    ) -> futures::stream::Map<
        RowsStream<'de, Self>,
        fn(
            <Self::Block as BlockCore<'de>>::Row,
        ) -> std::result::Result<
            T,
            <<Self as AsyncResultSet<'de>>::Block as BlockCore<'de>>::Error,
        >,
    >
    where
        T: Deserialize<'de>,
    {
        use futures::StreamExt;

        self.rows()
            .map(|row| T::deserialize(IntoDeserializer::into_deserializer(row)))
    }
}

#[async_trait]
pub trait AsyncQuery<'query>: Send + Sync {
    type Error;
    type ResultSet: AsyncResultSet<'query>;

    async fn query<T: AsRef<str>>(
        &self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

    async fn describe(&self, table: &str) -> Result<ColumnMeta, Self::Error>;

    async fn exec(&self, sql: &str) -> Result<usize, Self::Error> {
        self.query(sql).await.map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }

    async fn create_database<I: Into<DatabaseProperties> + Send + 'async_trait>(
        &self,
        name: &str,
        opts: I,
    ) -> Result<(), Self::Error> {
        let sql = format!("create database if not exists {} {}", name, opts.into());
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

    fn exec_sync(&self, sql: &str) -> Result<usize, Self::Error> {
        futures::executor::block_on(self.exec(sql))
    }

    fn query_sync<T: AsRef<str>>(
        &self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, Self::Error> {
        futures::executor::block_on(self.query(sql))
    }
}

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
