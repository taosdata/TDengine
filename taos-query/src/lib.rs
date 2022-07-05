//! This is the common query traits/types for TDengine connectors.
//!
#![cfg_attr(nightly, feature(const_slice_from_raw_parts))]
#![cfg_attr(nightly, feature(const_slice_index))]
#[cfg(feature = "async")]
use futures::stream::TryStreamExt;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use std::{fmt::Debug, marker::PhantomData, rc::Rc};

pub use mdsn::{Address, Dsn, DsnError, IntoDsn};
pub use serde::de::value::Error as DeError;

pub mod common;
mod de;
pub mod helpers;
mod insert;

mod iter;
pub mod util;

pub use iter::*;
#[cfg(feature = "async")]
use async_trait::async_trait;
use common::*;
use helpers::*;

pub enum CodecOpts {
    Raw,
    Parquet,
}

pub trait BlockCodec {
    fn encode(&self, _codec: CodecOpts) -> Vec<u8>;
    fn decode(from: &[u8], _codec: CodecOpts) -> Self;
}

type DeserializeIter<'b, B, T> =
    std::iter::Map<RowsIter<'b, B>, fn(RowInBlock<'b, B>) -> Result<T, DeError>>;

/// Trait to define a data `Block` to fetch records bulky.
///
/// If query performance is not your main concern, you can just use the deserialize method from result set.
pub trait BlockExt: Debug + Sized {
    /// A block should container number of rows.
    fn num_of_rows(&self) -> usize;

    /// Fields can be queried from a block.
    fn fields(&self) -> &[Field];

    /// Number of fields.
    fn field_count(&self) -> usize {
        self.fields().len()
    }

    fn precision(&self) -> Precision;

    fn is_null(&self, row: usize, col: usize) -> bool;

    /// Get field without column index check.
    ///
    /// # Safety
    ///
    /// This should not be called manually, please use [get_field](#method.get_field).
    unsafe fn get_field_unchecked(&self, col: usize) -> &Field {
        self.fields().get_unchecked(col)
    }

    /// Get field of one column.
    fn get_field(&self, col: usize) -> Option<&Field> {
        self.fields().get(col)
    }

    /// # Safety
    ///
    /// **DO NOT** call it directly.
    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, BorrowedValue);

    unsafe fn get_col_unchecked(&self, col: usize) -> BorrowedColumn;

    /// Query by rows.
    fn iter_rows(&self) -> RowsIter<'_, Self> {
        RowsIter::new(self)
    }

    /// Consume self into rows.
    fn into_iter_rows(self) -> IntoRowsIter<Self> {
        IntoRowsIter::new(self)
    }

    /// Columns iterator with borrowed data from block.
    fn columns_iter(&self) -> ColsIter<'_, Self> {
        ColsIter::new(self)
    }

    fn to_records(&self) -> Vec<Vec<Value>> {
        self.iter_rows()
            .map(|row| row.into_iter().map(|(f, v)| v.into_value()).collect_vec())
            .collect_vec()
    }

    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize<'b, T>(&'b self) -> DeserializeIter<'b, Self, T>
    where
        T: serde::de::Deserialize<'b>,
    {
        self.iter_rows().map(|row| {
            let de = de::RecordDeserializer::from(row);
            T::deserialize(de)
        })
    }

    /// Deserialize a row to a record type(primitive type or a struct).
    ///
    /// Any record could borrow data from the block, so that &[u8], &[str] could be used as record element (if valid).
    fn deserialize_into<T>(
        self,
    ) -> std::iter::Map<IntoRowsIter<Self>, fn(QueryRowIter<Self>) -> Result<T, DeError>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.into_iter_rows().map(|row| {
            let de = de::RecordDeserializer::from(&row);
            T::deserialize(de)
        })
    }

    /// Shortcut version to `.deserialize_into.collect::<Vec<T>>()`
    fn deserialize_into_vec<T>(self) -> Vec<Result<T, DeError>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.deserialize_into().collect()
    }

    #[cfg(feature = "async")]
    /// Rows as [futures::stream::Stream].
    fn rows_stream(&self) -> futures::stream::Iter<RowsIter<'_, Self>> {
        futures::stream::iter(Self::iter_rows(self))
    }

    #[cfg(feature = "async")]
    /// Owned version to rows stream.
    fn into_rows_stream(self) -> futures::stream::Iter<IntoRowsIter<Self>> {
        futures::stream::iter(Self::into_iter_rows(self))
    }

    #[cfg(feature = "async")]
    /// Rows stream to deserialized record.
    fn deserialize_stream<'b, T>(&'b self) -> futures::stream::Iter<DeserializeIter<'b, Self, T>>
    where
        T: serde::de::Deserialize<'b>,
    {
        futures::stream::iter(Self::deserialize(self))
    }
}

pub trait Fetchable
where
    Self: Sized,
    for<'r> &'r mut Self: Iterator,
    for<'b, 'r> <&'r mut Self as Iterator>::Item: BlockExt,
{
    // type Block: for<'b> BlockExt;

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

    fn to_rows_vec(&mut self) -> Vec<Vec<Value>> {
        self.rows_iter()
            .map(|row| row.into_iter().map(|(_, v)| v.into_value()).collect())
            .collect()
    }

    fn rows_iter<'r>(
        &'r mut self,
    ) -> std::iter::FlatMap<
        &'r mut Self,
        IntoRowsIter<<&'r mut Self as Iterator>::Item>,
        fn(<&'r mut Self as Iterator>::Item) -> IntoRowsIter<<&'r mut Self as Iterator>::Item>,
    > {
        self.flat_map(|block| block.into_iter_rows())
    }

    fn deserialize<T>(
        &mut self,
    ) -> std::iter::FlatMap<
        &mut Self,
        Vec<Result<T, DeError>>,
        fn(<&mut Self as Iterator>::Item) -> Vec<Result<T, DeError>>,
    >
    where
        T: serde::de::DeserializeOwned,
    {
        self.flat_map(|block| block.deserialize_into_vec())
    }
}

/// The synchronous query trait for TDengine connection.
pub trait Queryable<'q>: Debug
where
    for<'r> &'r mut Self::ResultSet: Iterator,
    for<'b, 'r> <&'r mut Self::ResultSet as Iterator>::Item: BlockExt,
{
    type Error: Debug + From<serde::de::value::Error>;
    // type B: for<'b> BlockExt<'b, 'b>;
    type ResultSet: Fetchable;

    fn query<T: AsRef<str>>(&'q self, sql: T) -> Result<Self::ResultSet, Self::Error>;

    fn exec<T: AsRef<str>>(&'q self, sql: T) -> Result<usize, Self::Error> {
        self.query(sql).map(|res| res.affected_rows() as _)
    }

    fn exec_many<T: AsRef<str>, I: IntoIterator<Item = T>>(
        &'q self,
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
        &'q self,
        sql: T,
    ) -> Result<Option<O>, Self::Error> {
        log::info!("query one: {}", sql.as_ref());
        self.query(sql)?
            .deserialize::<O>()
            .next()
            .map_or(Ok(None), |v| v.map(Some).map_err(Into::into))
    }

    fn create_topic(
        &'q self,
        name: impl AsRef<str>,
        sql: impl AsRef<str>,
    ) -> Result<(), Self::Error> {
        let (name, sql) = (name.as_ref(), sql.as_ref());
        let query = format!("create topic if not exists {name} as {sql}");

        self.query(&query)?;
        Ok(())
    }

    fn create_topic_as_database(
        &'q self,
        name: impl AsRef<str>,
        db: impl std::fmt::Display,
    ) -> Result<(), Self::Error> {
        let name = name.as_ref();
        let query = format!("create topic if not exists {name} as database {db}");

        self.exec(&query)?;
        Ok(())
    }

    fn databases(&'q self) -> Result<Vec<ShowDatabase>, Self::Error> {
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
    fn topics(&'q self) -> Result<Vec<Topic>, Self::Error> {
        self.query("show topics")?
            .deserialize()
            .try_collect()
            .map_err(Into::into)
    }

    fn describe(&'q self, table: &str) -> Result<Describe, Self::Error> {
        Ok(Describe(
            self.query(format!("describe {table}"))?
                .deserialize()
                .try_collect()?,
        ))
    }
}

#[cfg(feature = "async")]
pub trait AsyncFetchable
where
    Self: Sized + Send,
    Self::BlockStream: futures::stream::Stream + Send,
    <Self::BlockStream as futures::stream::Stream>::Item: BlockExt + Send,
{
    type BlockStream;
    // type Block: for<'b> BlockExt;

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

    fn block_stream(&mut self) -> Self::BlockStream;

    fn into_blocks(mut self) -> Self::BlockStream {
        self.block_stream()
    }

    /// Records is a row-based 2-dimension matrix of values.
    fn to_records(&mut self) -> Vec<Vec<Value>> {
        futures::executor::block_on_stream(Box::pin(self.block_stream()))
            .flat_map(|block| block.to_records())
            .collect()
    }

    fn deserialize_stream<T>(
        &mut self,
    ) -> futures::stream::FlatMap<
        <Self as AsyncFetchable>::BlockStream,
        futures::stream::Iter<std::vec::IntoIter<Result<T, DeError>>>,
        fn(
            <Self::BlockStream as futures::stream::Stream>::Item,
        ) -> futures::stream::Iter<std::vec::IntoIter<Result<T, DeError>>>,
    >
    where
        T: serde::de::DeserializeOwned,
    {
        // self.blocks_iter()
        use futures::stream::StreamExt;
        self.block_stream()
            .flat_map(|block| futures::stream::iter(block.deserialize_into_vec::<T>()))
    }
}


#[cfg(feature = "async")]
/// The synchronous query trait for TDengine connection.
#[async_trait]
pub trait AsyncQueryable<'q>: Send + Sync
where
    <Self::AsyncResultSet as AsyncFetchable>::BlockStream: 'q + futures::stream::Stream,
    for<'b> <<Self::AsyncResultSet as AsyncFetchable>::BlockStream as futures::stream::Stream>::Item:
        BlockExt + Send,
{
    type Error: Debug + From<serde::de::value::Error> + Send;
    // type B: for<'b> BlockExt<'b, 'b>;
    type AsyncResultSet: AsyncFetchable;

    async fn query<T: AsRef<str> + Send + Sync>(
        &'q self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error>;

    async fn exec<T: AsRef<str> + Send + Sync>(&'q self, sql: T) -> Result<usize, Self::Error> {
        let sql = sql.as_ref();
        log::trace!("exec sql: {sql}");
        self.query(sql).await.map(|res| res.affected_rows() as _)
    }

    async fn exec_many<T, I>(&'q self, input: I) -> Result<usize, Self::Error>
    where
        T: AsRef<str> + Send+ Sync,
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
    async fn query_one<T: AsRef<str> + Send+ Sync, O: DeserializeOwned + Send>(
        &'q self,
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

    async fn create_topic<N: AsRef<str> + Send+ Sync, S: AsRef<str> + Send>(
        &'q self,
        name: N,
        sql: S,
    ) -> Result<(), Self::Error> {
        let (name, sql) = (name.as_ref(), sql.as_ref());
        let query = format!("create topic if not exists {name} as {sql}");

        self.query(query).await?;
        Ok(())
    }

    async fn create_topic_as_database(
        &'q self,
        name: impl AsRef<str> + Send+ Sync + 'async_trait,
        db: impl std::fmt::Display + Send + 'async_trait,
    ) -> Result<(), Self::Error> {
        let name = name.as_ref();
        let query = format!("create topic if not exists {name} as database {db}");

        self.exec(&query).await?;
        Ok(())
    }

    async fn databases(&'q self) -> Result<Vec<ShowDatabase>, Self::Error> {
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
    async fn topics(&'q self) -> Result<Vec<Topic>, Self::Error> {
        Ok(self
            .query("show topics")
            .await?
            .deserialize_stream()
            .try_collect()
            .await?)
    }

    async fn describe(&'q self, table: &str) -> Result<Describe, Self::Error> {
        Ok(Describe(
            self.query(format!("describe {table}"))
                .await?
                .deserialize_stream()
                .try_collect()
                .await?,
        ))
    }

    fn exec_sync<T: AsRef<str> + Send+ Sync>(&'q self, sql: T) -> Result<usize, Self::Error> {
        futures::executor::block_on(self.exec(sql))
    }

    fn query_sync<T: AsRef<str> + Send+ Sync>(
        &'q self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        futures::executor::block_on(self.query(sql))
    }
}

pub trait FromDsn: Sized + 'static {
    type Err: std::error::Error;

    /// Validate or hygienize the DSN.
    ///
    /// ## Error
    ///
    /// When there're multi addresses, validate all addresses for connection,
    /// and filter success addresses. When all addresses of DSN are invalid,
    /// return last error. When success, it will return a pair of DSN and
    /// filtered addresses.
    fn hygienize(dsn: Dsn) -> Result<(Dsn, Vec<Address>), DsnError>;

    /// Generate a connection object from DSN.
    fn from_dsn<T: IntoDsn>(dsn: T) -> Result<Self, Self::Err>;

    /// Is the connection available?
    fn ping(dsn: &Dsn) -> Result<(), Self::Err>;
}

/// This is how we manage connections.
pub struct Manager<T: FromDsn> {
    dsn: Dsn,
    marker: PhantomData<T>,
}

impl<T: FromDsn> Default for Manager<T> {
    fn default() -> Self {
        Self {
            dsn: Dsn {
                driver: "taos".to_string(),
                ..Default::default()
            },
            marker: Default::default(),
        }
    }
}

impl<T: FromDsn + Send + Sync> Manager<T> {
    /// Build a connection manager from a DSN.
    #[inline]
    pub fn new(dsn: Dsn) -> Result<Self, DsnError> {
        let (dsn, _) = T::hygienize(dsn)?;

        Ok(Self {
            dsn,
            marker: PhantomData,
        })
    }

    /// Parse a DSN format from str.
    #[inline]
    pub fn parse(dsn: impl AsRef<str>) -> Result<Self, DsnError> {
        let dsn = Dsn::parse(dsn)?;
        Self::new(dsn)
    }

    #[inline]
    pub fn from_dsn(dsn: impl IntoDsn) -> Result<Self, DsnError> {
        let dsn = dsn.into_dsn()?;
        Self::new(dsn)
    }

    /// Open a connection to TDengine.
    #[inline]
    pub fn connect(&self) -> Result<T, <T as FromDsn>::Err> {
        T::from_dsn(&self.dsn)
    }

    #[cfg(feature = "r2d2")]
    #[inline]
    pub fn into_pool(self) -> Result<Pool<T>, r2d2::Error> {
        r2d2::Pool::new(self)
    }

    #[cfg(feature = "r2d2")]
    #[inline]
    pub fn into_pool_with_builder(
        self,
        builder: r2d2::Builder<Self>,
    ) -> Result<Pool<T>, r2d2::Error> {
        builder.build(self)
    }
}

#[cfg(feature = "r2d2")]
pub type Pool<T> = r2d2::Pool<Manager<T>>;

#[cfg(feature = "r2d2")]
impl<T: FromDsn + Send + Sync + 'static> r2d2::ManageConnection for Manager<T> {
    type Connection = T;
    type Error = <T as FromDsn>::Err;

    #[inline]
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.connect()
    }

    #[inline]
    fn is_valid(&self, _: &mut Self::Connection) -> Result<(), Self::Error> {
        <T as FromDsn>::ping(&self.dsn)
    }

    #[inline]
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use serde::forward_to_deserialize_any;
    use std::marker::PhantomData;

    use super::*;
    #[derive(Debug)]
    struct Conn;

    #[derive(Debug)]
    struct Value<'s>(&'s str);

    struct Deserializer;

    impl<'de> serde::de::Deserializer<'de> for Deserializer {
        type Error = serde::de::value::Error;
        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_i32(1)
        }

        forward_to_deserialize_any! {
            bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char str string unit option
            seq bytes byte_buf map unit_struct newtype_struct
            tuple_struct struct tuple enum identifier ignored_any
        }
    }

    impl<'de, 's: 'de> serde::de::Deserializer<'de> for Value<'s> {
        type Error = serde::de::value::Error;
        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_i32(1)
        }

        forward_to_deserialize_any! {
            bool u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 char unit option
            seq byte_buf map unit_struct newtype_struct
            tuple_struct struct tuple enum identifier ignored_any
        }

        fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_borrowed_str(self.0)
        }
        fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            self.deserialize_str(visitor)
        }
        fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            const V: u32 = 0x0f0f0f0f;
            let v: &[u8; 4] = unsafe { std::mem::transmute(&V) };
            visitor.visit_borrowed_bytes(v)
        }
    }

    #[derive(Debug)]
    struct MyResultSet<'q>(PhantomData<&'q u8>);

    #[derive(Debug)]
    struct Block<'r, 'q>(PhantomData<(&'r u8, &'q u8)>);

    impl<'b, 'r, 'q> BlockExt for Block<'r, 'q> {
        fn num_of_rows(&self) -> usize {
            1
        }

        fn fields(&self) -> &[Field] {
            static mut FIELDS: Vec<Field> = Vec::new();
            unsafe {
                if FIELDS.len() == 0 {
                    FIELDS.push(Field::new("ts", Ty::Timestamp, 8));
                    FIELDS.push(Field::new("bin10", Ty::VarChar, 10));
                    FIELDS.push(Field::new("int32", Ty::Int, 4));
                }
                &FIELDS
            }
        }

        fn precision(&self) -> Precision {
            Precision::Microsecond
        }

        fn is_null(&self, _row: usize, _col: usize) -> bool {
            false
        }

        fn field_count(&self) -> usize {
            3
        }

        unsafe fn cell_unchecked(&self, _row: usize, col: usize) -> (&Field, BorrowedValue) {
            match col {
                0 => (
                    self.get_field_unchecked(col) as _,
                    BorrowedValue::Timestamp(crate::Timestamp::Milliseconds(0)),
                ),
                2 => (self.get_field_unchecked(col) as _, BorrowedValue::Int(32)),
                1 => (
                    self.get_field_unchecked(col) as _,
                    BorrowedValue::VarChar("str"),
                ),
                _ => (self.get_field_unchecked(col) as _, BorrowedValue::Int(32)),
            }
        }

        unsafe fn get_col_unchecked(&self, _col: usize) -> BorrowedColumn {
            todo!()
        }
    }

    impl<'r, 'q> Iterator for &'r mut MyResultSet<'q> {
        type Item = Block<'r, 'q>;

        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };

                Some(Block(PhantomData))
            } else {
                None
            }
        }
    }

    impl<'r, 'q> crate::Fetchable for MyResultSet<'q> {
        fn fields(&self) -> &[Field] {
            todo!()
        }

        fn precision(&self) -> Precision {
            todo!()
        }

        fn summary(&self) -> (usize, usize) {
            todo!()
        }

        fn affected_rows(&self) -> i32 {
            todo!()
        }
    }

    #[derive(Debug)]
    struct Error;

    impl<'q> Queryable<'q> for Conn {
        type Error = anyhow::Error;

        type ResultSet = MyResultSet<'q>;

        fn query<T: AsRef<str>>(&'q self, _sql: T) -> Result<MyResultSet, Self::Error> {
            Ok(MyResultSet(PhantomData))
        }

        fn exec<T: AsRef<str>>(&self, _sql: T) -> Result<usize, Self::Error> {
            Ok(1)
        }
    }
    #[test]
    fn query_deserialize() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let mut rs = conn.query("abc").unwrap();

        for record in rs.deserialize::<(i32, String, u8)>() {
            dbg!(record.unwrap());
        }
    }
    #[test]
    fn block_deserialize_borrowed() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let mut set = conn.query("abc").unwrap();
        for block in &mut set {
            for record in block.deserialize::<(i32, &str, u8)>() {
                dbg!(record.unwrap());
            }
        }
    }
    #[test]
    fn block_deserialize_borrowed_bytes() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let mut set = conn.query("abc").unwrap();

        for block in &mut set {
            for record in block.deserialize::<(String, &str, u8)>() {
                dbg!(record.unwrap());
            }
        }
    }
    #[tokio::test]
    async fn block_deserialize_borrowed_bytes_stream() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let mut set = conn.query("abc").unwrap();

        use futures::stream::*;

        for block in &mut set {
            for record in block
                .deserialize_stream::<(String, &str, u8)>()
                .next()
                .await
            {
                dbg!(record.unwrap());
            }
        }
    }
    #[test]
    fn with_iter() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let mut set = conn.query("abc").unwrap();

        for block in &mut set {
            // todo
            for row in block.iter_rows() {
                for value in row {
                    println!("{:?}", value);
                }
            }
        }
    }
}
