use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use serde::{
    de::{DeserializeOwned, IntoDeserializer},
    Deserialize,
};

use crate::helpers::*;

use super::ResultSetProps;

/// A `Block` will be both column-wise or row-wise struct.
pub trait Block<'de>: Sized + Send + Sync {
    type Error: serde::de::Error + Send + Sync;

    // type BorrowedRow: IntoDeserializer<'de, Self::Error> + Send;
    // type BorrowedRowsIter: Iterator<Item = Self::BorrowedRow> + Send;
    // fn iter_rows(&self) -> Self::BorrowedRowsIter;

    type Row: IntoDeserializer<'de, Self::Error> + Send;
    type RowsIter: Iterator<Item = Self::Row> + Send;
    fn into_iter_rows(&self) -> Self::RowsIter;

    // type BorrowedColumn: Send;
    // type BorrowedColumnIter: Iterator<Item = Self::Column>;
    // fn iter_columns(&self) -> Self::BorrowedColumnIter;

    // type Column: Send;
    // type ColumnsIter: Iterator<Item = Self::Column> + Send;

    // fn into_iter_columns(self) -> Self::ColumnsIter;

    fn into_stream(self) -> futures::stream::Iter<Self::RowsIter> {
        futures::stream::iter(Block::into_iter_rows(&self))
    }
}

pub type SyncResultSetRowsIter<'de, T> = std::iter::FlatMap<
    <T as SyncResultSet<'de>>::Iter,
    <<T as SyncResultSet<'de>>::Block as Block<'de>>::RowsIter,
    fn(
        <T as SyncResultSet<'de>>::Block,
    ) -> <<T as SyncResultSet<'de>>::Block as Block<'de>>::RowsIter,
>;

pub trait SyncResultSet<'de>: ResultSetProps {
    type Value;
    type Block: Block<'de>;
    type Iter: Iterator<Item = Self::Block>;
    type RowsIter: Iterator<Item = <Self::Block as Block<'de>>::Row>;

    // Block can not roll back, use &mut here.
    fn blocks_iter(&mut self) -> Self::Iter;

    // The rows depend on the blocks iterator, use &mut too. So that one can only deal with blocks/rows at one time.
    fn rows_iter(&mut self) -> SyncResultSetRowsIter<'de, Self> {
        self.blocks_iter().flat_map(|b| Block::into_iter_rows(&b))
    }
}

pub type RowIterStream<'de, T> = futures::stream::Iter<<T as Block<'de>>::RowsIter>;

pub type RowsStream<'de, T> = futures::stream::FlatMap<
    <T as AsyncResultSet<'de>>::BlockStream,
    RowIterStream<'de, <T as AsyncResultSet<'de>>::Block>,
    fn(<T as AsyncResultSet<'de>>::Block) -> RowIterStream<'de, <T as AsyncResultSet<'de>>::Block>,
>;

pub type DeserializeStream<'de, R, T> = futures::stream::Map<
    RowsStream<'de, R>,
    fn(
        <<R as AsyncResultSet<'de>>::Block as Block<'de>>::Row,
    ) -> std::result::Result<T, <<R as AsyncResultSet<'de>>::Block as Block<'de>>::Error>,
>;

pub type AsyncResultSetError<'de, T> = <<T as AsyncResultSet<'de>>::Block as Block<'de>>::Error;

pub trait AsyncResultSet<'de>: ResultSetProps + Sync + Send {
    type Block: Block<'de>;
    type BlockStream: Stream<Item = Self::Block> + Send;

    fn blocks(&mut self) -> Self::BlockStream;

    fn rows<'a>(&'a mut self) -> RowsStream<'de, Self> {
        use futures::StreamExt;
        self.blocks().flat_map(Block::into_stream)
    }

    fn deserialize_stream<'a, T>(&'a mut self) -> DeserializeStream<'de, Self, T>
    where
        T: Deserialize<'de>,
    {
        use futures::StreamExt;

        self.rows()
            .map(|row| T::deserialize(IntoDeserializer::into_deserializer(row)))
    }

    fn deserialize_owned_stream<T>(&mut self) -> DeserializeStream<'de, Self, T>
    where
        T: DeserializeOwned,
    {
        use futures::StreamExt;

        self.rows()
            .map(|row| T::deserialize(IntoDeserializer::into_deserializer(row)))
    }
}

pub type AsyncQueryError<'query, T> =
    AsyncResultSetError<'query, <T as AsyncQuery<'query>>::ResultSet>;

#[async_trait]
pub trait AsyncQuery<'query>: Send + Sync {
    type ResultSet: AsyncResultSet<'query>;

    async fn query<T: AsRef<str>>(
        &'query self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, AsyncQueryError<'query, Self>>;

    async fn describe(&self, table: &str) -> Result<ColumnMeta, AsyncQueryError<'query, Self>>;

    async fn exec(&'query self, sql: &str) -> Result<usize, AsyncQueryError<'query, Self>> {
        self.query(sql).await.map(|res| match res {
            Ok(_) => 0, // todo: if we should get the selected rows if not update query?
            Err(affected) => affected,
        })
    }

    async fn create_database<I: Into<DatabaseProperties> + Send + 'async_trait>(
        &'query self,
        name: &str,
        opts: I,
    ) -> Result<(), AsyncQueryError<'query, Self>> {
        let sql = format!("create database if not exists {} {}", name, opts.into());
        self.exec(&sql).await.map(|_| ())
    }

    async fn use_database(
        &'query self,
        database: &str,
    ) -> Result<(), AsyncQueryError<'query, Self>> {
        let sql = format!("use database {}", database);
        self.exec(&sql).await.map(|_| ())
    }

    async fn create_table(&'query self, name: &str) -> Result<(), AsyncQueryError<'query, Self>> {
        let sql = format!("create table {}", name);
        self.exec(&sql).await.map(|_| ())
    }

    async fn databases(&'query self) -> Result<Vec<ShowDatabase>, AsyncQueryError<'query, Self>> {
        self.query(format!("show databases"))
            .await?
            .expect("`show databases` must be queryable")
            .deserialize_stream()
            .try_collect()
            .await
    }

    fn exec_sync(&'query self, sql: &str) -> Result<usize, AsyncQueryError<'query, Self>> {
        futures::executor::block_on(self.exec(sql))
    }

    fn query_sync<T: AsRef<str>>(
        &'query self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, AsyncQueryError<'query, Self>> {
        futures::executor::block_on(self.query(sql))
    }
}

mod _impl {
    use super::*;
    use crate::Error;

    // impl<'b, 'de> Block<'de> for crate::block::Block<'b> {
    //     type Error = Error;

    //     type Row = crate::block::Row<'b>;

    //     type RowsIter = crate::block::RowsIter<'b>;

    //     fn into_iter_rows(&self) -> Self::RowsIter {
    //         self.rows_iter()
    //     }
    // }
}
