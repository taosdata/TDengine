//! This is the common query traits/types for TDengine connectors.
//!
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::type_complexity)]
#![allow(unknown_lints)]

use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

use async_trait::async_trait;
pub use common::RawBlock;
use common::*;
pub use iter::*;
pub use mdsn::{redact_dsn, value_is_true, Address, Dsn, DsnError, IntoDsn, RedactedDsn};
pub use prelude::sync::{Fetchable, Queryable};
pub use prelude::{AsyncFetchable, AsyncQueryable};
pub use serde::de::value::Error as DeError;
pub use taos_error::Error as RawError;
use util::Edition;

pub mod common;
pub mod helpers;
mod iter;
pub mod prelude;
pub mod stmt;
pub mod stmt2;
pub mod tmq;
pub mod util;

pub type RawResult<T> = std::result::Result<T, RawError>;

lazy_static::lazy_static! {
    static ref GLOBAL_RT: tokio::runtime::Runtime = {
        tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
    };
}

pub fn global_tokio_runtime() -> &'static tokio::runtime::Runtime {
    &GLOBAL_RT
}

pub fn block_in_place_or_global<F: std::future::Future>(fut: F) -> F::Output {
    use tokio::runtime::Handle;
    use tokio::task;

    match Handle::try_current() {
        Ok(handle) => task::block_in_place(move || handle.block_on(fut)),
        Err(_) => global_tokio_runtime().block_on(fut),
    }
}

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
pub trait TBuilder: Sized + Send + Sync + 'static {
    type Target: Send + Sync + 'static;

    /// A list of parameters available in DSN.
    fn available_params() -> &'static [&'static str];

    /// Connect with dsn without connection checking.
    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self>;

    /// Get client version.
    fn client_version() -> &'static str;

    /// Get server version.
    #[doc(hidden)]
    fn server_version(&self) -> RawResult<&str>;

    /// Check if the server is an enterprise edition.
    #[doc(hidden)]
    fn is_enterprise_edition(&self) -> RawResult<bool> {
        Ok(false)
    }

    /// Get the edition.
    #[doc(hidden)]
    fn get_edition(&self) -> RawResult<Edition>;

    /// Assert the server is an enterprise edition.
    #[doc(hidden)]
    fn assert_enterprise_edition(&self) -> RawResult<()> {
        if let Ok(edition) = self.get_edition() {
            edition.assert_enterprise_edition()
        } else {
            Err(RawError::from_string("get edition failed"))
        }
    }

    /// Check a connection is still alive.
    fn ping(&self, _: &mut Self::Target) -> RawResult<()>;

    /// Check if it's ready to connect.
    ///
    /// In most cases, just return true. `r2d2` will use this method to check if it's valid to create a connection.
    /// Just check the address is ready to connect.
    fn ready(&self) -> bool;

    /// Create a new connection from this struct.
    fn build(&self) -> RawResult<Self::Target>;

    /// Build connection pool with [r2d2::Pool]
    ///
    /// Here we will use some default options with [r2d2::Builder]
    ///
    /// - max_lifetime: 12h
    /// - max_size: 500
    /// - min_idle: 2
    /// - connection_timeout: 60s
    #[cfg(feature = "r2d2")]
    fn pool(self) -> RawResult<r2d2::Pool<Manager<Self>>> {
        self.pool_builder()
            .build(Manager::new(self))
            .map_err(RawError::from_any)
    }

    /// [r2d2::Builder] generation from config.
    #[cfg(feature = "r2d2")]
    #[inline]
    fn pool_builder(&self) -> r2d2::Builder<Manager<Self>> {
        r2d2::Builder::new()
            .max_lifetime(Some(std::time::Duration::from_secs(12 * 60 * 60)))
            .min_idle(Some(0))
            .max_size(200)
            .connection_timeout(std::time::Duration::from_secs(60))
    }

    /// Build connection pool with [r2d2::Builder]
    #[cfg(feature = "r2d2")]
    #[inline]
    fn with_pool_builder(
        self,
        builder: r2d2::Builder<Manager<Self>>,
    ) -> RawResult<r2d2::Pool<Manager<Self>>> {
        builder
            .build(Manager::new(self))
            .map_err(RawError::from_any)
    }
}

#[cfg(feature = "r2d2")]
impl<T: TBuilder> r2d2::ManageConnection for Manager<T> {
    type Connection = T::Target;
    type Error = RawError;

    fn connect(&self) -> RawResult<Self::Connection> {
        self.deref().build()
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> RawResult<()> {
        self.deref().ping(conn)
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        !self.deref().ready()
    }
}

/// A struct is `Connectable` when it can be build from a `Dsn`.
#[async_trait]
pub trait AsyncTBuilder: Sized + Send + Sync + 'static {
    type Target: Send + Sync + 'static;

    /// Connect with dsn without connection checking.
    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self>;

    /// Get client version.
    fn client_version() -> &'static str;

    /// Get server version.
    #[doc(hidden)]
    async fn server_version(&self) -> RawResult<&str>;

    /// Check if the server is an enterprise edition.
    #[doc(hidden)]
    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        Ok(false)
    }

    /// Get the edition.
    #[doc(hidden)]
    async fn get_edition(&self) -> RawResult<Edition>;

    /// Assert the server is an enterprise edition.
    #[doc(hidden)]
    async fn assert_enterprise_edition(&self) -> RawResult<()> {
        if let Ok(edition) = self.get_edition().await {
            edition.assert_enterprise_edition()
        } else {
            Err(RawError::from_string("get edition failed"))
        }
    }

    /// Check a connection is still alive.
    async fn ping(&self, _: &mut Self::Target) -> RawResult<()>;

    /// Check if it's ready to connect.
    ///
    /// In most cases, just return true. `r2d2` will use this method to check if it's valid to create a connection.
    /// Just check the address is ready to connect.
    async fn ready(&self) -> bool;

    /// Create a new connection from this struct.
    async fn build(&self) -> RawResult<Self::Target>;

    /// Build connection pool with [deadpool::managed::Pool].
    ///
    /// Default:
    /// - max_size: 500
    #[cfg(feature = "deadpool")]
    fn pool(self) -> RawResult<deadpool::managed::Pool<Manager<Self>>> {
        let config = self.default_pool_config();
        self.pool_builder()
            .config(config)
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .map_err(RawError::from_any)
    }

    /// [deadpool::managed::PoolBuilder] generation from config.
    #[cfg(feature = "deadpool")]
    #[inline]
    fn pool_builder(self) -> deadpool::managed::PoolBuilder<Manager<Self>> {
        deadpool::managed::Pool::builder(Manager { manager: self })
    }

    #[cfg(feature = "deadpool")]
    #[inline]
    fn default_pool_config(&self) -> deadpool::managed::PoolConfig {
        deadpool::managed::PoolConfig {
            max_size: 5000,
            timeouts: deadpool::managed::Timeouts {
                wait: None,
                create: Some(std::time::Duration::from_secs(30)),
                recycle: None,
            },
            queue_mode: deadpool::managed::QueueMode::Fifo,
        }
    }

    /// Build connection pool with [deadpool::managed::PoolBuilder]
    #[cfg(feature = "deadpool")]
    #[inline]
    fn with_pool_config(
        self,
        config: deadpool::managed::PoolConfig,
    ) -> RawResult<deadpool::managed::Pool<Manager<Self>>> {
        deadpool::managed::Pool::builder(Manager { manager: self })
            .config(config)
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .map_err(RawError::from_any)
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

impl<T: TBuilder> Default for Manager<T> {
    fn default() -> Self {
        Self {
            manager: T::from_dsn("taos:///").expect("connect with empty default TDengine dsn"),
        }
    }
}

impl<T: TBuilder> Manager<T> {
    pub fn new(builder: T) -> Self {
        Self { manager: builder }
    }

    /// Build a connection manager from a DSN.
    #[inline]
    pub fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<(Self, BTreeMap<String, String>)> {
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
    pub fn into_pool(self) -> RawResult<r2d2::Pool<Self>> {
        r2d2::Pool::new(self).map_err(RawError::from_any)
    }

    #[cfg(feature = "r2d2")]
    #[inline]
    pub fn into_pool_with_builder(
        self,
        builder: r2d2::Builder<Self>,
    ) -> RawResult<r2d2::Pool<Self>> {
        builder.build(self).map_err(RawError::from_any)
    }
}

#[cfg(all(feature = "r2d2", feature = "deadpool"))]
compile_error!("Use only ONE of r2d2 or deadpool");

#[cfg(feature = "r2d2")]
pub type Pool<T> = r2d2::Pool<Manager<T>>;

#[cfg(feature = "deadpool")]
pub type Pool<T> = deadpool::managed::Pool<Manager<T>>;

#[cfg(feature = "r2d2")]
pub type PoolBuilder<T> = r2d2::Builder<Manager<T>>;

#[cfg(feature = "deadpool")]
#[async_trait]
impl<T: AsyncTBuilder> deadpool::managed::Manager for Manager<T> {
    type Type = <T as AsyncTBuilder>::Target;
    type Error = RawError;

    async fn create(&self) -> RawResult<Self::Type> {
        self.manager.build().await
    }

    async fn recycle(
        &self,
        conn: &mut Self::Type,
        _: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        self.ping(conn).await.map_err(RawError::from_any)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[derive(Debug)]
    struct Conn;

    #[derive(Debug)]
    struct MyResultSet;

    impl Iterator for MyResultSet {
        type Item = RawResult<RawBlock>;

        fn next(&mut self) -> Option<Self::Item> {
            static mut AVAILABLE: bool = true;
            if unsafe { AVAILABLE } {
                unsafe { AVAILABLE = false };

                Some(Ok(RawBlock::parse_from_raw_block_v2(
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
        #[allow(static_mut_refs)]
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

        fn update_summary(&mut self, _rows: usize) {}

        #[allow(static_mut_refs)]
        fn fetch_raw_block(&mut self) -> RawResult<Option<RawBlock>> {
            static mut B: AtomicUsize = AtomicUsize::new(4);
            unsafe {
                if B.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    return Ok(None);
                }
            }
            unsafe { B.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) };

            Ok(Some(RawBlock::parse_from_raw_block_v2(
                [1].as_slice(),
                &[Field::new("a", Ty::TinyInt, 1)],
                &[1],
                1,
                Precision::Millisecond,
            )))
        }
    }

    impl TBuilder for Conn {
        type Target = MyResultSet;

        fn available_params() -> &'static [&'static str] {
            &[]
        }

        fn from_dsn<D: IntoDsn>(_dsn: D) -> RawResult<Self> {
            Ok(Self)
        }

        fn client_version() -> &'static str {
            "3"
        }

        fn ready(&self) -> bool {
            true
        }

        fn build(&self) -> RawResult<Self::Target> {
            Ok(MyResultSet)
        }

        fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
            Ok(())
        }

        fn server_version(&self) -> RawResult<&str> {
            todo!()
        }

        fn is_enterprise_edition(&self) -> RawResult<bool> {
            todo!()
        }

        fn get_edition(&self) -> RawResult<Edition> {
            Ok(Edition::new("community", false))
        }
    }

    impl Queryable for Conn {
        type ResultSet = MyResultSet;

        fn query<T: AsRef<str>>(&self, _sql: T) -> RawResult<MyResultSet> {
            Ok(MyResultSet)
        }

        fn query_with_req_id<T: AsRef<str>>(
            &self,
            _sql: T,
            _req_id: u64,
        ) -> RawResult<Self::ResultSet> {
            Ok(MyResultSet)
        }

        fn exec<T: AsRef<str>>(&self, _sql: T) -> RawResult<usize> {
            Ok(1)
        }

        fn write_raw_meta(&self, _: &RawMeta) -> RawResult<()> {
            Ok(())
        }

        fn write_raw_block(&self, _: &RawBlock) -> RawResult<()> {
            Ok(())
        }

        fn write_raw_block_with_req_id(&self, _: &RawBlock, _: u64) -> RawResult<()> {
            Ok(())
        }

        fn put(&self, _data: &SmlData) -> RawResult<()> {
            Ok(())
        }
    }

    #[test]
    fn query_deserialize() {
        let conn = Conn;

        let aff = conn.exec("nothing").unwrap();
        assert_eq!(aff, 1);

        let mut rs = conn.query("abc").unwrap();

        for record in rs.deserialize::<(i32, String, u8)>() {
            println!("{:?}", record);
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
            for record in block.deserialize::<(i32,)>() {
                assert_eq!(record.unwrap(), (1i32,));
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
            for record in block.deserialize::<String>() {
                assert_eq!(record.unwrap(), "1");
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

        for row in set.deserialize::<u8>() {
            let row = row.unwrap();
            assert_eq!(row, 1);
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
