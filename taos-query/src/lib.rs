//! This is the common query traits/types for TDengine connectors.
//!
#![cfg_attr(nightly, feature(const_slice_from_raw_parts))]
#![cfg_attr(nightly, feature(const_slice_index))]

use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
    rc::Rc,
};

pub use mdsn::{Address, Dsn, DsnError, IntoDsn};
pub use serde::de::value::Error as DeError;

pub mod common;
mod de;
pub mod helpers;
mod insert;

mod iter;
pub mod util;

use common::*;
pub use iter::*;

pub use common::RawData;

pub mod stmt;

pub mod prelude;

pub use prelude::sync::{Fetchable, Queryable};
pub use prelude::{AsyncFetchable, AsyncQueryable};

pub enum CodecOpts {
    Raw,
    Parquet,
}

pub trait BlockCodec {
    fn encode(&self, _codec: CodecOpts) -> Vec<u8>;
    fn decode(from: &[u8], _codec: CodecOpts) -> Self;
}

#[derive(Debug, thiserror::Error)]
pub struct PingError {
    msg: String,
}
impl Display for PingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

/// A struct is `Connectable` when it can be build from a `Dsn`.
pub trait Connectable: Sized + Send + Sync + 'static {
    type Target: Send + Sync + 'static;
    type Error: std::error::Error + From<DsnError>;

    /// A list of parameters available in DSN.
    fn available_params() -> &'static [&'static str];

    /// Connect with dsn without connection checking.
    fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error>;

    /// Get client version.
    fn client_version() -> &'static str;

    /// Get server version.
    fn server_version(&self) -> &str;

    /// Check a connection is still alive.
    fn ping(&self, _: &mut Self::Target) -> Result<(), Self::Error>;

    /// Check if it's ready to connect.
    ///
    /// In most cases, just return true. `r2d2` will use this method to check if it's valid to create a connection.
    /// Just check the address is ready to connect.
    fn ready(&self) -> bool;

    /// Create a new connection from this struct.
    fn connect(&self) -> Result<Self::Target, Self::Error>;

    /// Build connection pool with [r2d2::Pool]
    #[cfg(feature = "r2d2")]
    fn pool(self) -> Result<r2d2::Pool<Manager<Self>>, r2d2::Error> {
        r2d2::Pool::new(Manager::new(self))
    }

    /// Build connection pool with [r2d2::Builder]
    #[cfg(feature = "r2d2")]
    #[inline]
    fn with_pool_builder(
        self,
        builder: r2d2::Builder<Manager<Self>>,
    ) -> Result<r2d2::Pool<Manager<Self>>, r2d2::Error> {
        builder.build(Manager::new(self))
    }
}

#[cfg(feature = "r2d2")]
impl<T: Connectable> r2d2::ManageConnection for Manager<T> {
    type Connection = T::Target;

    type Error = T::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.deref().connect()
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.deref().ping(conn)
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        !self.deref().ready()
    }
}

/// This is how we manage connections.
pub struct Manager<T> {
    manager: T,
}

impl<T> Deref for Manager<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.manager
    }
}
impl<T> DerefMut for Manager<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.manager
    }
}

impl<T: Connectable> Default for Manager<T> {
    fn default() -> Self {
        Self {
            manager: T::from_dsn("taos:///").expect("connect with empty default TDengine dsn"),
        }
    }
}

impl<T: Connectable> Manager<T> {
    pub fn new(builder: T) -> Self {
        Self { manager: builder }
    }
    /// Build a connection manager from a DSN.
    #[inline]
    pub fn from_dsn<D: IntoDsn>(dsn: D) -> Result<(Self, BTreeMap<String, String>), T::Error> {
        let mut dsn = dsn.into_dsn()?;

        let params = T::available_params();
        let (valid, not): (BTreeMap<_, _>, BTreeMap<_, _>) = dsn
            .params
            .into_iter()
            .partition(|(key, _)| params.contains(&key.as_str()));

        dsn.params = valid;

        T::from_dsn(dsn).map(|builder| (Manager::new(builder), not))
    }

    #[cfg(feature = "r2d2")]
    #[inline]
    pub fn into_pool(self) -> Result<r2d2::Pool<Self>, r2d2::Error> {
        r2d2::Pool::new(self)
    }

    #[cfg(feature = "r2d2")]
    #[inline]
    pub fn into_pool_with_builder(
        self,
        builder: r2d2::Builder<Self>,
    ) -> Result<r2d2::Pool<Self>, r2d2::Error> {
        builder.build(self)
    }
}

#[cfg(feature = "r2d2")]
pub type Pool<T> = r2d2::Pool<Manager<T>>;

#[cfg(feature = "r2d2")]
pub type PoolBuilder<T> = r2d2::Builder<Manager<T>>;

#[cfg(test)]
mod tests {
    use std::{fmt::Display, sync::atomic::AtomicUsize};

    use super::*;
    #[derive(Debug)]
    struct Conn;

    #[derive(Debug)]
    struct MyResultSet;

    impl Iterator for MyResultSet {
        type Item = Result<RawData, Error>;

        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };

                Some(Ok(RawData::parse_from_raw_block_v2(
                    [1].as_slice(),
                    &[Field::new("a", Ty::TinyInt, 1)],
                    &[1],
                    1,
                    Precision::Millisecond,
                )))
            } else {
                None
            }
        }
    }

    impl<'q> crate::Fetchable for MyResultSet {
        type Error = Error;
        fn fields(&self) -> &[Field] {
            static mut F: Option<Vec<Field>> = None;
            unsafe { F.get_or_insert(vec![Field::new("a", Ty::TinyInt, 1)]) };
            unsafe { F.as_ref().unwrap() }
        }

        fn precision(&self) -> Precision {
            Precision::Millisecond
        }

        fn summary(&self) -> (usize, usize) {
            (0, 0)
        }

        fn affected_rows(&self) -> i32 {
            0
        }

        fn update_summary(&mut self, rows: usize) {}

        fn fetch_raw_block(&mut self) -> Result<Option<RawData>, Self::Error> {
            static mut B: AtomicUsize = AtomicUsize::new(4);
            unsafe {
                if B.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    return Ok(None);
                }
            }
            unsafe { B.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) };

            Ok(Some(RawData::parse_from_raw_block_v2(
                [1].as_slice(),
                &[Field::new("a", Ty::TinyInt, 1)],
                &[1],
                1,
                Precision::Millisecond,
            )))
        }
    }

    #[derive(Debug)]
    struct Error;

    impl Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("empty error")
        }
    }

    impl From<taos_error::Error> for Error {
        fn from(_: taos_error::Error) -> Self {
            Error
        }
    }

    impl std::error::Error for Error {}
    impl From<DsnError> for Error {
        fn from(_: DsnError) -> Self {
            Error
        }
    }

    impl Connectable for Conn {
        type Target = MyResultSet;

        type Error = Error;

        fn available_params() -> &'static [&'static str] {
            &[]
        }

        fn from_dsn<D: IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
            Ok(Self)
        }

        fn client_version() -> &'static str {
            "3"
        }

        fn server_version(&self) -> &str {
            "3"
        }

        fn ready(&self) -> bool {
            true
        }

        fn connect(&self) -> Result<Self::Target, Self::Error> {
            Ok(MyResultSet)
        }

        fn ping(&self, _: &mut Self::Target) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    impl Queryable for Conn {
        type Error = anyhow::Error;

        type ResultSet = MyResultSet;

        fn query<T: AsRef<str>>(&self, _sql: T) -> Result<MyResultSet, Self::Error> {
            Ok(MyResultSet)
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
            let block = block.unwrap();
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
            let block = block.unwrap();
            for record in block.deserialize::<(String, &str, u8)>() {
                dbg!(record.unwrap());
            }
        }
    }
    #[cfg(feature = "async")]
    #[tokio::test]
    async fn block_deserialize_borrowed_bytes_stream() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let mut set = conn.query("abc").unwrap();

        use futures::stream::*;

        for row in set.deserialize::<u8>() {
            let row = row.unwrap();
            dbg!(row);
        }
    }
    #[test]
    fn with_iter() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let mut set = conn.query("abc").unwrap();

        for block in set.blocks() {
            // todo
            for row in block.unwrap().rows() {
                for value in row {
                    println!("{:?}", value);
                }
            }
        }
    }
}
