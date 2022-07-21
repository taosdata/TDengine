mod _priv {
    pub use crate::common::{BorrowedValue, Precision, Ty, Value};
    pub use crate::Connectable;
    #[cfg(feature = "r2d2")]
    pub use crate::Pool;
    #[cfg(feature = "r2d2")]
    pub use crate::PoolBuilder;
    pub use itertools::Itertools;
    pub use mdsn::{Dsn, DsnError, IntoDsn};
    pub use taos_error::{Code, Error as RawError};
}

pub use _priv::*;
pub use r#async::*;

pub mod sync {
    pub use super::_priv::*;

    pub use crate::stmt::Bindable;

    use itertools::Itertools;
    use serde::de::DeserializeOwned;

    pub use mdsn::{Address, Dsn, DsnError, IntoDsn};
    pub use serde::de::value::Error as DeError;

    use crate::common::*;
    use crate::helpers::*;

    use crate::common::RawData;

    // pub use crate::{Fetchable, Queryable};

    pub struct IRowsIter<'a, T>
    where
        T: Fetchable,
    {
        iter: IBlockIter<'a, T>,
        block: Option<RawData>,
        row: usize,
        rows: Option<RowsIter<'a>>,
    }

    impl<'a, T> IRowsIter<'a, T>
    where
        T: Fetchable,
    {
        fn fetch(&mut self) -> Result<Option<RowView<'a>>, T::Error> {
            if let Some(block) = self.iter.next().transpose()? {
                self.block = Some(block);
                self.rows = self.block.as_mut().map(|raw| raw.rows());
                let row = self.rows.as_mut().unwrap().next();
                return Ok(row);
            } else {
                Ok(None)
            }
        }
        fn next_row(&mut self) -> Result<Option<RowView<'a>>, T::Error> {
            // has block
            if let Some(rows) = self.rows.as_mut() {
                // check if block over.
                if let Some(row) = rows.next() {
                    return Ok(Some(row));
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
        type Item = Result<RowView<'a>, T::Error>;

        fn next(&mut self) -> Option<Self::Item> {
            self.next_row().transpose()
        }
    }

    pub struct IBlockIter<'a, T>
    where
        T: Fetchable,
    {
        query: &'a mut T,
    }

    impl<'a, T> Iterator for IBlockIter<'a, T>
    where
        T: Fetchable,
    {
        type Item = Result<RawData, T::Error>;

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
        type Error: From<taos_error::Error>;

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
        fn fetch_raw_block(&mut self) -> Result<Option<RawData>, Self::Error>;

        /// Iterator for raw data blocks.
        fn blocks(&mut self) -> IBlockIter<'_, Self> {
            IBlockIter { query: self }
        }

        /// Iterator for querying by rows.
        fn rows(&mut self) -> IRowsIter<'_, Self> {
            IRowsIter {
                iter: self.blocks(),
                block: None,
                row: 0,
                rows: None,
            }
        }

        fn deserialize<T: DeserializeOwned>(
            &mut self,
        ) -> std::iter::Map<
            IRowsIter<'_, Self>,
            fn(Result<RowView, Self::Error>) -> Result<T, Self::Error>,
        > {
            self.rows().map(|row| Ok(T::deserialize(&mut row?)?))
        }

        fn to_rows_vec(&mut self) -> Result<Vec<Vec<Value>>, Self::Error> {
            self.blocks()
                .map_ok(|raw| raw.to_values())
                .flatten_ok()
                .try_collect()
        }
    }

    /// The synchronous query trait for TDengine connection.
    pub trait Queryable
    // where
    //     Self::ResultSet: Iterator<Item = Result<RawData, Self::Error>>,
    {
        type Error: From<<Self::ResultSet as Fetchable>::Error>;

        type ResultSet: Fetchable;

        fn query<T: AsRef<str>>(&self, sql: T) -> Result<Self::ResultSet, Self::Error>;

        fn exec<T: AsRef<str>>(&self, sql: T) -> Result<usize, Self::Error> {
            log::info!("execute sql: {}", sql.as_ref());

            self.query(sql).map(|res| res.affected_rows() as _)
        }

        fn exec_many<T: AsRef<str>, I: IntoIterator<Item = T>>(
            &self,
            input: I,
        ) -> Result<usize, Self::Error> {
            input
                .into_iter()
                .map(|sql| self.exec(sql))
                .try_fold(0, |mut acc, aff| {
                    acc += aff?;
                    Ok(acc)
                })
        }

        fn query_one<T: AsRef<str>, O: DeserializeOwned>(
            &self,
            sql: T,
        ) -> Result<Option<O>, Self::Error> {
            log::info!("query one: {}", sql.as_ref());
            self.query(sql)?
                .deserialize::<O>()
                .next()
                .map_or(Ok(None), |v| v.map(Some).map_err(Into::into))
        }

        fn create_topic(
            &self,
            name: impl AsRef<str>,
            sql: impl AsRef<str>,
        ) -> Result<(), Self::Error> {
            let (name, sql) = (name.as_ref(), sql.as_ref());
            let query = format!("create topic if not exists {name} as {sql}");

            self.query(&query)?;
            Ok(())
        }

        fn create_topic_as_database(
            &self,
            name: impl AsRef<str>,
            db: impl std::fmt::Display,
        ) -> Result<(), Self::Error> {
            let name = name.as_ref();
            let query = format!("create topic if not exists {name} as database {db}");

            self.exec(&query)?;
            Ok(())
        }

        fn databases(&self) -> Result<Vec<ShowDatabase>, Self::Error> {
            self.query("show databases")?
                .deserialize()
                .try_collect()
                .map_err(Into::into)
        }

        /// Topics information by `show topics` sql.
        ///
        /// ## Compatibility
        ///
        /// This is a 3.x-only API.
        fn topics(&self) -> Result<Vec<Topic>, Self::Error> {
            self.query("show topics")?
                .deserialize()
                .try_collect()
                .map_err(Into::into)
        }

        fn describe(&self, table: &str) -> Result<Describe, Self::Error> {
            Ok(Describe(
                self.query(format!("describe {table}"))?
                    .deserialize()
                    .try_collect()?,
            ))
        }
    }
}

mod r#async {
    pub use super::_priv::*;
    pub use crate::stmt::AsyncBindable;

    pub use futures::stream::{Stream, StreamExt, TryStreamExt};
    use itertools::{FlattenOk, Itertools, MapOk};
    use serde::{de::DeserializeOwned, Deserialize};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::{cell::UnsafeCell, fmt::Debug, marker::PhantomData, rc::Rc};

    pub use mdsn::{Address, Dsn, DsnError, IntoDsn};
    pub use serde::de::value::Error as DeError;

    use crate::common::*;
    use crate::helpers::*;
    use crate::Connectable;
    // use crate::iter::*;
    #[cfg(feature = "async")]
    use async_trait::async_trait;

    pub struct AsyncBlocks<'a, T> {
        query: Pin<Box<&'a mut T>>,
    }

    impl<'a, T> Stream for AsyncBlocks<'a, T>
    where
        T: AsyncFetchable,
    {
        type Item = Result<RawData, T::Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.query.fetch_raw_block(cx).map(|raw| {
                raw.map(|raw| {
                    raw.map(|raw| {
                        self.query.update_summary(raw.nrows());
                        raw
                    })
                })
                .transpose()
            })
        }
    }

    pub struct AsyncRows<'a, T> {
        blocks: AsyncBlocks<'a, T>,
        block: Option<RawData>,
        rows: Option<RowsIter<'a>>,
    }

    impl<'a, T> AsyncRows<'a, T>
    where
        T: AsyncFetchable,
    {
        fn fetch(
            self: &mut Self,
            cx: &mut Context<'_>,
        ) -> Poll<Result<Option<RowView<'a>>, T::Error>> {
            let poll = self.blocks.try_poll_next_unpin(cx);
            match poll {
                Poll::Ready(block) => match block.transpose() {
                    Ok(Some(block)) => {
                        self.block = Some(block);
                        self.rows = self.block.as_mut().map(|raw| raw.rows());
                        let row = self.rows.as_mut().unwrap().next();
                        return Poll::Ready(Ok(row));
                    }
                    Ok(None) => Poll::Ready(Ok(None)),
                    Err(err) => Poll::Ready(Err(err)),
                },
                Poll::Pending => Poll::Pending,
            }
        }
        fn next_row(
            self: &mut Self,
            cx: &mut Context<'_>,
        ) -> Poll<Result<Option<RowView<'a>>, T::Error>> {
            // has block
            if let Some(rows) = self.rows.as_mut() {
                // check if block over.
                if let Some(row) = rows.next() {
                    return Poll::Ready(Ok(Some(row)));
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
        type Item = Result<RowView<'a>, T::Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.next_row(cx).map(|row| row.transpose())
        }
    }

    pub struct AsyncDeserialized<'a, T, V> {
        rows: AsyncRows<'a, T>,
        _marker: PhantomData<V>,
    }

    impl<'a, T, V> Unpin for AsyncDeserialized<'a, T, V> {}

    impl<'a, T, V> Stream for AsyncDeserialized<'a, T, V>
    where
        T: AsyncFetchable,
        V: DeserializeOwned,
    {
        type Item = Result<V, T::Error>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            use futures::stream::*;
            Pin::get_mut(self).rows.poll_next_unpin(cx).map(|row| {
                row.map(|row| row.and_then(|mut row| V::deserialize(&mut row).map_err(Into::into)))
            })
        }
    }

    #[cfg(feature = "async")]
    pub trait AsyncFetchable: Sized + Send + Sync {
        type Error: From<taos_error::Error> + Send + Sync;

        fn affected_rows(&self) -> i32;

        fn precision(&self) -> Precision;

        fn fields(&self) -> &[Field];

        fn num_of_fields(&self) -> usize {
            self.fields().len()
        }

        fn summary(&self) -> (usize, usize);

        fn blocks_iter(&mut self) -> &mut Self {
            self
        }

        #[doc(hidden)]
        fn update_summary(&mut self, nrows: usize);

        #[doc(hidden)]
        fn fetch_raw_block(
            self: &mut Self,
            cx: &mut Context<'_>,
        ) -> Poll<Result<Option<RawData>, Self::Error>>;

        fn blocks(&mut self) -> AsyncBlocks<'_, Self> {
            AsyncBlocks {
                query: Box::pin(self),
            }
        }

        fn rows(&mut self) -> AsyncRows<'_, Self> {
            AsyncRows {
                blocks: self.blocks(),
                block: None,
                rows: None,
            }
        }

        /// Records is a row-based 2-dimension matrix of values.
        fn to_records(&mut self) -> Result<Vec<Vec<Value>>, Self::Error> {
            futures::executor::block_on_stream(Box::pin(self.rows()))
                .map_ok(|block| block.into_values())
                .try_collect()
        }

        fn deserialize_stream<R>(&mut self) -> AsyncDeserialized<'_, Self, R>
        where
            R: serde::de::DeserializeOwned,
        {
            AsyncDeserialized {
                rows: self.rows(),
                _marker: PhantomData,
            }
        }
    }

    #[cfg(feature = "async")]
    /// The synchronous query trait for TDengine connection.
    #[async_trait]
    pub trait AsyncQueryable: Send + Sync + Sized {
        type Error: Debug + From<<Self::AsyncResultSet as AsyncFetchable>::Error> + Send;
        // type B: for<'b> BlockExt<'b, 'b>;
        type AsyncResultSet: AsyncFetchable;

        async fn query<T: AsRef<str> + Send + Sync>(
            &self,
            sql: T,
        ) -> Result<Self::AsyncResultSet, Self::Error>;

        async fn exec<T: AsRef<str> + Send + Sync>(&self, sql: T) -> Result<usize, Self::Error> {
            let sql = sql.as_ref();
            log::trace!("exec sql: {sql}");
            self.query(sql).await.map(|res| res.affected_rows() as _)
        }

        async fn exec_many<T, I>(&self, input: I) -> Result<usize, Self::Error>
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
        /// Type `T` could be `Vec<taos::query::common::Value>`, a tuple, or a struct with serde support.
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
        ) -> Result<Option<O>, Self::Error> {
            use futures::StreamExt;
            self.query(sql)
                .await?
                .deserialize_stream::<O>()
                .take(1)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .next()
                .map_or(Ok(None), |v| v.map(Some).map_err(Into::into))
        }

        async fn create_topic<N: AsRef<str> + Send + Sync, S: AsRef<str> + Send>(
            &self,
            name: N,
            sql: S,
        ) -> Result<(), Self::Error> {
            let (name, sql) = (name.as_ref(), sql.as_ref());
            let query = format!("create topic if not exists {name} as {sql}");

            self.query(query).await?;
            Ok(())
        }

        async fn create_topic_as_database(
            &self,
            name: impl AsRef<str> + Send + Sync + 'async_trait,
            db: impl std::fmt::Display + Send + 'async_trait,
        ) -> Result<(), Self::Error> {
            let name = name.as_ref();
            let query = format!("create topic if not exists {name} as database {db}");

            self.exec(&query).await?;
            Ok(())
        }

        async fn databases(&self) -> Result<Vec<ShowDatabase>, Self::Error> {
            use futures::stream::TryStreamExt;
            Ok(self
                .query("show databases")
                .await?
                .deserialize_stream()
                .try_collect()
                .await?)
        }

        /// Topics information by `show topics` sql.
        ///
        /// ## Compatibility
        ///
        /// This is a 3.x-only API.
        async fn topics(&self) -> Result<Vec<Topic>, Self::Error> {
            Ok(self
                .query("show topics")
                .await?
                .deserialize_stream()
                .try_collect()
                .await?)
        }

        /// Get table meta information.
        async fn describe(&self, table: &str) -> Result<Describe, Self::Error> {
            Ok(Describe(
                self.query(format!("describe {table}"))
                    .await?
                    .deserialize_stream()
                    .try_collect()
                    .await?,
            ))
        }

        /// Sync version of `exec`.
        fn exec_sync<T: AsRef<str> + Send + Sync>(&self, sql: T) -> Result<usize, Self::Error> {
            futures::executor::block_on(self.exec(sql))
        }

        /// Sync version of `query`.
        fn query_sync<T: AsRef<str> + Send + Sync>(
            &self,
            sql: T,
        ) -> Result<Self::AsyncResultSet, Self::Error> {
            futures::executor::block_on(self.query(sql))
        }
    }

    #[test]
    fn test() {
        assert!(true);
    }
}
