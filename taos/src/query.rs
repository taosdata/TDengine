use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{
    de::{DeserializeOwned, IntoDeserializer},
    Deserialize,
};

use crate::helpers::*;
use taos_query::common::Precision;

pub trait ResultSetProps {
    fn num_of_fields(&self) -> usize;

    fn affected_rows(&self) -> usize;

    fn is_update_query(&self) -> bool;

    fn precision(&self) -> Precision;
}

pub trait RowWise<R, T: Iterator<Item = R>>: Sized {
    fn into_row_iter(self) -> T {
        todo!()
    }
}

pub trait AsyncColumnWise<C, I>: Send + Sized {
    fn into_column_stream(self) -> futures::stream::Iter<I> {
        todo!()
    }
}

/// Block into row stream.
///
/// R: Row,
/// E: Error,
/// I: Iterator<Item = R>,
pub trait AsyncBlock<'de, R, E, I>: IntoIterator<Item = R, IntoIter = I> + Send + Sized
where
    R: IntoDeserializer<'de, E> + Send + Sized,
    E: serde::de::Error + Send + From<E>,
    I: Iterator<Item = R> + Send,
{
    fn into_rows_stream(self) -> futures::stream::Iter<I> {
        futures::stream::iter(IntoIterator::into_iter(self))
    }
}

// B: Block
pub type RowsIter<'de, B> = <B as IntoIterator>::IntoIter;

// S: ResultSet
pub type RowsStreamExt<'de, S, R, E, I> = futures::stream::FlatMap<
    <S as AsyncResultSet<'de, R, E, I>>::BlockStream,
    futures::stream::Iter<I>,
    fn(<S as AsyncResultSet<'de, R, E, I>>::Block) -> futures::stream::Iter<I>,
>;

// T: Deserialized Type.
pub type DeserializeStreamExt<'de, T, S, R, E, I> =
    futures::stream::Map<RowsStreamExt<'de, S, R, E, I>, fn(R) -> std::result::Result<T, E>>;

pub trait AsyncResultSet<'de, R, E, I>: ResultSetProps + Send + Sized
where
    R: IntoDeserializer<'de, E> + Send + Sized,
    E: serde::de::Error + Send,
    I: Iterator<Item = R> + Send,
{
    type Block: AsyncBlock<'de, R, E, I>;
    type BlockStream: Stream<Item = Self::Block> + Send;

    fn blocks(&mut self) -> Self::BlockStream;

    fn rows<'a>(&'a mut self) -> RowsStreamExt<'de, Self, R, E, I> {
        self.blocks().flat_map(AsyncBlock::into_rows_stream)
    }

    fn deserialize_stream<'a, T>(&'a mut self) -> DeserializeStreamExt<'de, T, Self, R, E, I>
    where
        T: Deserialize<'de>,
    {
        self.rows()
            .map(|row| T::deserialize(IntoDeserializer::into_deserializer(row)))
    }

    fn deserialize_owned_stream<T>(&mut self) -> DeserializeStreamExt<'de, T, Self, R, E, I>
    where
        T: DeserializeOwned,
    {
        self.rows()
            .map(|row| T::deserialize(IntoDeserializer::into_deserializer(row)))
    }
}

#[async_trait]
pub trait AsyncQuery<'query, 'de, R, E, I>: Send + Sync + Sized
where
    R: IntoDeserializer<'de, E> + Send + Sized,
    E: serde::de::Error + Send,
    I: Iterator<Item = R> + Send,
{
    type ResultSet: AsyncResultSet<'de, R, E, I>;

    async fn query<T: AsRef<str>>(
        &'query self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, E>;

    async fn exec(&'query self, sql: &str) -> Result<usize, E> {
        self.query(sql).await.map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }

    async fn create_database<O: Into<DatabaseProperties> + Send + 'async_trait>(
        &'query self,
        name: &str,
        opts: O,
    ) -> Result<(), E> {
        let sql = format!("create database if not exists {} {}", name, opts.into());
        self.exec(&sql).await.map(|_| ())
    }

    async fn use_database(&'query self, database: &str) -> Result<(), E> {
        let sql = format!("use database {}", database);
        self.exec(&sql).await.map(|_| ())
    }

    async fn create_table(&'query self, name: &str) -> Result<(), E> {
        let sql = format!("create table {}", name);
        self.exec(&sql).await.map(|_| ())
    }

    async fn databases(&'query self) -> Result<Vec<ShowDatabase>, E> {
        self.query(format!("show databases"))
            .await?
            .expect("`show databases` must be queryable")
            .deserialize_stream()
            .try_collect()
            .await
    }

    async fn describe(&'query self, table: &str) -> Result<Vec<ColumnMeta>, E> {
        self.query(format!("describe {table}"))
            .await?
            .expect("`describe <table>` must be queryable")
            .deserialize_stream()
            .try_collect()
            .await
    }

    fn exec_sync(&'query self, sql: &str) -> Result<usize, E> {
        futures::executor::block_on(self.exec(sql))
    }

    fn query_sync<T: AsRef<str>>(
        &'query self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, E> {
        futures::executor::block_on(self.query(sql))
    }
}

// pub type AsyncQueryError<T> = T::Error;

// #[async_trait]
// pub trait AsyncQuery<'query, 'de>: Send + Sync + Sized
// {
//     type Error: serde::de::Error + Send + From<Self::Error>;
//     type Row: IntoDeserializer<'de, Self::Error> + Send + Sized;
//     type IntoIter: Iterator<Item = Self::Row> + Send;
//     type ResultSet: AsyncResultSetExt<'de, Self::Row, Self::Error, Self::IntoIter>;

//     async fn query<T: AsRef<str>>(
//         &'query self,
//         sql: T,
//     ) -> Result<Result<Self::ResultSet, usize>, Self::Error>;

//     async fn describe(&self, table: &str) -> Result<ColumnMeta, Self::Error>;

//     async fn exec(&'query self, sql: &str) -> Result<usize, Self::Error> {
//         self.query(sql).await.map(|res| match res {
//             Ok(_) => 0, // todo: if we should get the selected rows if not update query?
//             Err(affected) => affected,
//         })
//     }

//     async fn create_database<I: Into<DatabaseProperties> + Send + 'async_trait>(
//         &'query self,
//         name: &str,
//         opts: I,
//     ) -> Result<(), Self::Error> {
//         let sql = format!("create database if not exists {} {}", name, opts.into());
//         self.exec(&sql).await.map(|_| ())
//     }

//     async fn use_database(
//         &'query self,
//         database: &str,
//     ) -> Result<(), Self::Error> {
//         let sql = format!("use database {}", database);
//         self.exec(&sql).await.map(|_| ())
//     }

//     async fn create_table(&'query self, name: &str) -> Result<(), Self::Error> {
//         let sql = format!("create table {}", name);
//         self.exec(&sql).await.map(|_| ())
//     }

//     async fn databases(&'query self) -> Result<Vec<ShowDatabase>, Self::Error> {
//         self.query(format!("show databases"))
//             .await?
//             .expect("`show databases` must be queryable")
//             .deserialize_owned_stream()
//             .try_collect()
//             .await
//     }

//     fn exec_sync(&'query self, sql: &str) -> Result<usize, Self::Error> {
//         futures::executor::block_on(self.exec(sql))
//     }

//     fn query_sync<T: AsRef<str>>(
//         &'query self,
//         sql: T,
//     ) -> Result<Result<Self::ResultSet, usize>, Self::Error> {
//         futures::executor::block_on(self.query(sql))
//     }
// }

pub mod r#async;
pub mod blocking;
