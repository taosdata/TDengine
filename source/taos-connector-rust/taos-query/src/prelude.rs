mod _priv {
    pub use itertools::Itertools;
    pub use mdsn::{Dsn, DsnError, IntoDsn};
    pub use taos_error::{Code, Error as RawError};

    pub use crate::common::{
        AlterType, BorrowedValue, ColumnView, Field, JsonMeta, MetaAlter, MetaCreate, MetaDrop,
        MetaUnit, Precision, RawBlock, RawMeta, TagWithValue, Ty, Value,
    };
    pub use crate::tmq::{IsOffset, MessageSet, Timeout};
    pub use crate::util::{Inlinable, InlinableRead, InlinableWrite};
}

pub use _priv::*;
pub use futures::stream::{Stream, StreamExt, TryStreamExt};
pub use r#async::*;
pub use tokio;

pub use crate::tmq::{AsAsyncConsumer, IsAsyncData, IsAsyncMeta};
#[cfg(any(feature = "deadpool", feature = "r2d2"))]
pub use crate::Pool;
pub use crate::{AsyncTBuilder, RawResult};

pub trait Helpers {
    fn table_vgroup_id(&self, _db: &str, _table: &str) -> Option<i32> {
        None
    }

    fn tables_vgroup_ids<T: AsRef<str>>(&self, _db: &str, _tables: &[T]) -> Option<Vec<i32>> {
        None
    }
}

pub mod sync {
    use std::borrow::Cow;

    use chrono_tz::Tz;
    pub use mdsn::{Address, Dsn, DsnError, IntoDsn};
    #[cfg(feature = "r2d2")]
    pub use r2d2::ManageConnection;
    pub use serde::de::value::Error as DeError;
    use serde::de::DeserializeOwned;

    pub use super::_priv::*;
    use crate::common::*;
    use crate::helpers::*;
    pub use crate::stmt::Bindable;
    pub use crate::stmt2::{Stmt2BindParam, Stmt2Bindable};
    pub use crate::tmq::{AsConsumer, IsData, IsMeta};
    #[cfg(feature = "r2d2")]
    pub use crate::{Pool, PoolBuilder};
    pub use crate::{RawResult, TBuilder};

    pub struct IRowsIter<'a, T>
    where
        T: Fetchable,
    {
        iter: IBlockIter<'a, T>,
        block: Option<RawBlock>,
        rows: Option<RowsIter<'a>>,
        tz: Option<Tz>,
    }

    impl<'a, T> IRowsIter<'a, T>
    where
        T: Fetchable,
    {
        fn fetch(&mut self) -> RawResult<Option<RowView<'a>>> {
            if let Some(block) = self.iter.next().transpose()? {
                self.block = Some(block);
                self.rows = self.block.as_mut().map(|raw| raw.rows());
                let row = self.rows.as_mut().unwrap().next();
                Ok(row)
            } else {
                Ok(None)
            }
        }

        fn next_row(&mut self) -> RawResult<Option<RowView<'a>>> {
            // has block
            if let Some(rows) = self.rows.as_mut() {
                // check if block over.
                if let Some(row) = rows.next() {
                    Ok(Some(row))
                } else {
                    self.fetch()
                }
            } else {
                // no data, start fetching.
                self.fetch()
            }
        }
    }

    impl<'a, T> Iterator for IRowsIter<'a, T>
    where
        T: Fetchable,
    {
        type Item = RawResult<RowView<'a>>;

        fn next(&mut self) -> Option<Self::Item> {
            match self.next_row() {
                Ok(Some(mut row)) => {
                    row.set_timezone(self.tz);
                    Some(Ok(row))
                }
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        }
    }

    pub struct IBlockIter<'a, T>
    where
        T: Fetchable,
    {
        query: &'a mut T,
    }

    impl<T> Iterator for IBlockIter<'_, T>
    where
        T: Fetchable,
    {
        type Item = RawResult<RawBlock>;

        fn next(&mut self) -> Option<Self::Item> {
            self.query
                .fetch_raw_block()
                .map(|raw| {
                    if let Some(raw) = raw {
                        self.query.update_summary(raw.nrows());
                        Some(raw)
                    } else {
                        None
                    }
                })
                .transpose()
        }
    }

    pub trait Fetchable: Sized {
        fn affected_rows(&self) -> i32;

        fn precision(&self) -> Precision;

        fn fields(&self) -> &[Field];

        fn num_of_fields(&self) -> usize {
            self.fields().len()
        }

        fn summary(&self) -> (usize, usize);

        #[doc(hidden)]
        fn update_summary(&mut self, nrows: usize);

        #[doc(hidden)]
        fn fetch_raw_block(&mut self) -> RawResult<Option<RawBlock>>;

        /// Iterator for raw data blocks.
        fn blocks(&mut self) -> IBlockIter<'_, Self> {
            IBlockIter { query: self }
        }

        /// Iterator for querying by rows.
        fn rows(&mut self) -> IRowsIter<'_, Self> {
            let tz = self.timezone();
            IRowsIter {
                iter: self.blocks(),
                block: None,
                rows: None,
                tz,
            }
        }

        fn deserialize<T: DeserializeOwned>(
            &mut self,
        ) -> std::iter::Map<IRowsIter<'_, Self>, fn(RawResult<RowView>) -> RawResult<T>> {
            self.rows().map(|row| T::deserialize(&mut row?))
        }

        fn to_rows_vec(&mut self) -> RawResult<Vec<Vec<Value>>> {
            self.blocks()
                .map_ok(|raw| raw.to_values())
                .flatten_ok()
                .try_collect()
        }

        fn timezone(&self) -> Option<Tz> {
            None
        }
    }

    /// The synchronous query trait for TDengine connection.
    pub trait Queryable {
        type ResultSet: Fetchable;

        fn query<T: AsRef<str>>(&self, sql: T) -> RawResult<Self::ResultSet>;

        fn query_with_req_id<T: AsRef<str>>(
            &self,
            sql: T,
            req_id: u64,
        ) -> RawResult<Self::ResultSet>;

        fn exec<T: AsRef<str>>(&self, sql: T) -> RawResult<usize> {
            self.query(sql).map(|res| res.affected_rows() as _)
        }

        fn write_raw_meta(&self, _: &RawMeta) -> RawResult<()>;

        fn write_raw_block(&self, _: &RawBlock) -> RawResult<()>;

        fn write_raw_block_with_req_id(&self, _: &RawBlock, _: u64) -> RawResult<()>;

        fn exec_many<T: AsRef<str>, I: IntoIterator<Item = T>>(
            &self,
            input: I,
        ) -> RawResult<usize> {
            input
                .into_iter()
                .map(|sql| self.exec(sql))
                .try_fold(0, |mut acc, aff| {
                    acc += aff?;
                    Ok(acc)
                })
        }

        fn query_one<T: AsRef<str>, O: DeserializeOwned>(&self, sql: T) -> RawResult<Option<O>> {
            self.query(sql)?
                .deserialize::<O>()
                .next()
                .map_or(Ok(None), |v| v.map(Some))
        }

        /// Short for `SELECT server_version()` as [String].
        fn server_version(&self) -> RawResult<Cow<'_, str>> {
            Ok(self
                .query_one::<_, String>("SELECT server_version()")?
                .expect("should always has result")
                .into())
        }

        fn create_topic(&self, name: impl AsRef<str>, sql: impl AsRef<str>) -> RawResult<()> {
            let (name, sql) = (name.as_ref(), sql.as_ref());
            let query = format!("create topic if not exists `{name}` as {sql}");

            self.query(query)?;
            Ok(())
        }

        fn create_topic_as_database(
            &self,
            name: impl AsRef<str>,
            db: impl std::fmt::Display,
        ) -> RawResult<()> {
            let name = name.as_ref();
            let query = format!("create topic if not exists `{name}` as database `{db}`");

            self.exec(query)?;
            Ok(())
        }

        fn databases(&self) -> RawResult<Vec<ShowDatabase>> {
            self.query("show databases")?.deserialize().try_collect()
        }

        /// Topics information by `SELECT * FROM information_schema.ins_topics` sql.
        ///
        /// ## Compatibility
        ///
        /// This is a 3.x-only API.
        fn topics(&self) -> RawResult<Vec<Topic>> {
            self.query("SELECT * FROM information_schema.ins_topics")?
                .deserialize()
                .try_collect()
        }

        fn describe(&self, table: &str) -> RawResult<Describe> {
            Ok(Describe(
                self.query(format!("describe `{table}`"))?
                    .deserialize()
                    .try_collect()?,
            ))
        }

        /// Check if database exists
        fn database_exists(&self, name: &str) -> RawResult<bool> {
            Ok(self.exec(format!("show `{name}`.stables")).is_ok())
        }

        fn put(&self, data: &SmlData) -> RawResult<()>;

        fn table_vgroup_id(&self, _db: &str, _table: &str) -> Option<i32> {
            None
        }

        fn tables_vgroup_ids<T: AsRef<str>>(&self, _db: &str, _tables: &[T]) -> Option<Vec<i32>> {
            None
        }
    }
}

mod r#async {
    use std::borrow::Cow;
    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[cfg(feature = "async")]
    use async_trait::async_trait;
    use chrono_tz::Tz;
    pub use futures::stream::{Stream, StreamExt, TryStreamExt};
    pub use mdsn::Address;
    pub use serde::de::value::Error as DeError;
    use serde::de::DeserializeOwned;

    pub use super::_priv::*;
    use crate::common::*;
    use crate::helpers::*;
    pub use crate::stmt::AsyncBindable;
    pub use crate::stmt2::{Stmt2AsyncBindable, Stmt2BindParam};
    pub use crate::util::{AsyncInlinable, AsyncInlinableRead, AsyncInlinableWrite};
    pub use crate::RawResult;

    pub struct AsyncBlocks<'a, T> {
        query: &'a mut T,
    }

    impl<T> Stream for AsyncBlocks<'_, T>
    where
        T: AsyncFetchable,
    {
        type Item = RawResult<RawBlock>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.query.fetch_raw_block(cx).map(|raw| {
                raw.map(|raw| {
                    raw.inspect(|raw| {
                        self.query.update_summary(raw.nrows());
                    })
                })
                .transpose()
            })
        }
    }

    pub struct AsyncRows<'a, T> {
        blocks: AsyncBlocks<'a, T>,
        block: Option<RawBlock>,
        rows: Option<RowsIter<'a>>,
    }

    impl<'a, T> AsyncRows<'a, T>
    where
        T: AsyncFetchable,
    {
        fn fetch(&mut self, cx: &mut Context<'_>) -> Poll<RawResult<Option<RowView<'a>>>> {
            let poll = self.blocks.try_poll_next_unpin(cx);
            match poll {
                Poll::Ready(block) => match block.transpose() {
                    Ok(Some(block)) => {
                        self.block = Some(block);
                        self.rows = self.block.as_mut().map(|raw| raw.rows());
                        let row = self.rows.as_mut().unwrap().next();
                        Poll::Ready(Ok(row))
                    }
                    Ok(None) => Poll::Ready(Ok(None)),
                    Err(err) => Poll::Ready(Err(err)),
                },
                Poll::Pending => Poll::Pending,
            }
        }

        fn next_row(&mut self, cx: &mut Context<'_>) -> Poll<RawResult<Option<RowView<'a>>>> {
            // has block
            if let Some(rows) = self.rows.as_mut() {
                // check if block over.
                if let Some(row) = rows.next() {
                    Poll::Ready(Ok(Some(row)))
                } else {
                    self.fetch(cx)
                }
            } else {
                // no data, start fetching.
                self.fetch(cx)
            }
        }
    }

    impl<'a, T> Stream for AsyncRows<'a, T>
    where
        T: AsyncFetchable,
    {
        type Item = RawResult<RowView<'a>>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.next_row(cx).map(Result::transpose)
        }
    }

    pub struct AsyncDeserialized<'a, T, V> {
        rows: AsyncRows<'a, T>,
        tz: Option<Tz>,
        _marker: PhantomData<V>,
    }

    impl<T, V> Unpin for AsyncDeserialized<'_, T, V> {}

    impl<T, V> Stream for AsyncDeserialized<'_, T, V>
    where
        T: AsyncFetchable,
        V: DeserializeOwned,
    {
        type Item = RawResult<V>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            use futures::stream::*;

            let tz = self.tz;

            Pin::get_mut(self).rows.poll_next_unpin(cx).map(|row| {
                row.map(|row| {
                    row.and_then(|mut row| {
                        row.set_timezone(tz);
                        V::deserialize(&mut row)
                    })
                })
            })
        }
    }

    #[cfg(feature = "async")]
    #[async_trait]
    pub trait AsyncFetchable: Sized + Send + Sync {
        fn affected_rows(&self) -> i32;

        fn precision(&self) -> Precision;

        fn fields(&self) -> &[Field];

        fn filed_names(&self) -> Vec<&str> {
            self.fields().iter().map(Field::name).collect_vec()
        }

        fn num_of_fields(&self) -> usize {
            self.fields().len()
        }

        fn summary(&self) -> (usize, usize);

        #[doc(hidden)]
        fn update_summary(&mut self, nrows: usize);

        #[doc(hidden)]
        fn fetch_raw_block(&mut self, cx: &mut Context<'_>) -> Poll<RawResult<Option<RawBlock>>>;

        fn blocks(&mut self) -> AsyncBlocks<'_, Self> {
            AsyncBlocks { query: self }
        }

        fn rows(&mut self) -> AsyncRows<'_, Self> {
            AsyncRows {
                blocks: self.blocks(),
                block: None,
                rows: None,
            }
        }

        /// Records is a row-based 2-dimension matrix of values.
        async fn to_records(&mut self) -> RawResult<Vec<Vec<Value>>> {
            let future = self.rows().map_ok(RowView::into_values).try_collect();
            future.await
        }

        fn deserialize<R>(&mut self) -> AsyncDeserialized<'_, Self, R>
        where
            R: serde::de::DeserializeOwned,
        {
            let tz = self.timezone();
            AsyncDeserialized {
                rows: self.rows(),
                tz,
                _marker: PhantomData,
            }
        }

        fn timezone(&self) -> Option<Tz> {
            None
        }
    }

    /// The synchronous query trait for TDengine connection.
    #[cfg(feature = "async")]
    #[async_trait]
    pub trait AsyncQueryable: Send + Sync + Sized {
        type AsyncResultSet: AsyncFetchable;

        async fn query<T: AsRef<str> + Send + Sync>(
            &self,
            sql: T,
        ) -> RawResult<Self::AsyncResultSet>;

        async fn put(&self, schemaless_data: &SmlData) -> RawResult<()>;

        async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
            &self,
            sql: T,
            req_id: u64,
        ) -> RawResult<Self::AsyncResultSet>;

        async fn exec<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<usize> {
            let sql = sql.as_ref();
            self.query(sql).await.map(|res| res.affected_rows() as _)
        }

        async fn exec_with_req_id<T: AsRef<str> + Send + Sync>(
            &self,
            sql: T,
            req_id: u64,
        ) -> RawResult<usize> {
            let sql = sql.as_ref();
            self.query_with_req_id(sql, req_id)
                .await
                .map(|res| res.affected_rows() as _)
        }

        async fn write_raw_meta(&self, meta: &RawMeta) -> RawResult<()>;

        async fn write_raw_block(&self, block: &RawBlock) -> RawResult<()>;

        async fn write_raw_block_with_req_id(&self, block: &RawBlock, req_id: u64)
            -> RawResult<()>;

        async fn exec_many<T, I>(&self, input: I) -> RawResult<usize>
        where
            T: AsRef<str> + Send + Sync,
            I::IntoIter: Send,
            I: IntoIterator<Item = T> + Send,
        {
            let mut aff = 0;
            for sql in input {
                aff += self.exec(sql).await?;
            }
            Ok(aff)
        }

        /// To conveniently get first row of the result, useful for queries like
        ///
        /// - `select count(*) from ...`
        /// - `select last(*) from ...`
        ///
        /// Type `T` could be `Vec<Value>`, a tuple, or a struct with serde support.
        ///
        /// ## Example
        ///
        /// ```rust,ignore
        /// let count: u32 = taos.query_one("select count(*) from table1")?.unwrap_or(0);
        ///
        /// let one: (i32, String, Timestamp) =
        ///    taos.query_one("select c1,c2,c3 from table1 limit 1")?.unwrap_or_default();
        /// ```
        async fn query_one<T: AsRef<str> + Send + Sync, O: DeserializeOwned + Send>(
            &self,
            sql: T,
        ) -> RawResult<Option<O>> {
            use futures::StreamExt;
            self.query(sql)
                .await?
                .deserialize::<O>()
                .take(1)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .next()
                .map_or(Ok(None), |v| v.map(Some))
        }

        /// Short for `SELECT server_version()` as [String].
        async fn server_version(&self) -> RawResult<Cow<str>> {
            Ok(self
                .query_one::<_, String>("SELECT server_version()")
                .await?
                .expect("should always has result")
                .into())
        }

        /// Short for `CREATE DATABASE IF NOT EXISTS {name}`.
        async fn create_database<N: AsRef<str> + Send>(&self, name: N) -> RawResult<()> {
            let query = format!("CREATE DATABASE IF NOT EXISTS {}", name.as_ref());

            self.query(query).await?;
            Ok(())
        }

        /// Short for `USE {name}`.
        async fn use_database<N: AsRef<str> + Send>(&self, name: N) -> RawResult<()> {
            let query = format!("USE `{}`", name.as_ref());

            self.query(query).await?;
            Ok(())
        }

        /// Short for `CREATE TOPIC IF NOT EXISTS {name} AS {sql}`.
        async fn create_topic<N: AsRef<str> + Send + Sync, S: AsRef<str> + Send>(
            &self,
            name: N,
            sql: S,
        ) -> RawResult<()> {
            let (name, sql) = (name.as_ref(), sql.as_ref());
            let query = format!("CREATE TOPIC IF NOT EXISTS `{name}` AS {sql}");

            self.query(query).await?;
            Ok(())
        }

        /// Short for `CREATE TOPIC IF NOT EXISTS {name} WITH META AS DATABASE {db}`.
        async fn create_topic_as_database(
            &self,
            name: impl AsRef<str> + Send + Sync + 'async_trait,
            db: impl std::fmt::Display + Send + 'async_trait,
        ) -> RawResult<()> {
            let name = name.as_ref();
            let query = format!("create topic if not exists `{name}` with meta as database `{db}`");
            self.exec(&query).await?;
            Ok(())
        }

        /// Short for `SHOW DATABASES`.
        async fn databases(&self) -> RawResult<Vec<ShowDatabase>> {
            use futures::stream::TryStreamExt;
            Ok(self
                .query("SHOW DATABASES")
                .await?
                .deserialize()
                .try_collect()
                .await?)
        }

        /// Topics information by `SELECT * FROM information_schema.ins_topics` sql.
        ///
        /// ## Compatibility
        ///
        /// This is a 3.x-only API.
        async fn topics(&self) -> RawResult<Vec<Topic>> {
            let sql = "SELECT * FROM information_schema.ins_topics";
            tracing::trace!("query one with sql: {sql}");
            Ok(self.query(sql).await?.deserialize().try_collect().await?)
        }

        /// Get table meta information.
        async fn describe(&self, table: &str) -> RawResult<Describe> {
            Ok(Describe(
                self.query(format!("DESCRIBE `{table}`"))
                    .await?
                    .deserialize()
                    .try_collect()
                    .await?,
            ))
        }

        /// Check if database exists
        async fn database_exists(&self, name: &str) -> RawResult<bool> {
            Ok(self.exec(format!("show `{name}`.stables")).await.is_ok())
        }

        /// Sync version of `exec`.
        fn exec_sync<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<usize> {
            crate::block_in_place_or_global(self.exec(sql))
        }

        /// Sync version of `query`.
        fn query_sync<T: AsRef<str> + Send + Sync>(
            &self,
            sql: T,
        ) -> RawResult<Self::AsyncResultSet> {
            crate::block_in_place_or_global(self.query(sql))
        }

        async fn table_vgroup_id(&self, _db: &str, _table: &str) -> Option<i32> {
            None
        }

        async fn tables_vgroup_ids<T: AsRef<str> + Sync>(
            &self,
            _db: &str,
            _tables: &[T],
        ) -> Option<Vec<i32>> {
            None
        }
    }

    #[test]
    fn test() {
        assert!(true);
    }
}
