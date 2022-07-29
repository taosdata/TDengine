use std::sync::Once;

pub use impls::Error;
pub use taos_error::{Code, Error as TaosError};

pub use taos_query as query;
use taos_sys::*;

macro_rules! err {
    (custom $err:expr) => {
        <crate::Error as ::serde::de::Error>::custom($err)
    };
    ('str $err:expr) => {
        <crate::Error as ::serde::de::Error>::custom($err)
    };
}

// pub mod timestamp;

mod options;

pub use options::TaosOptions;

mod util;
use util::*;

// deprecated method.
mod async_query;

pub mod helpers;

pub mod stream;

pub mod tmq;

mod schemaless;

mod impls;

pub type Result<T> = std::result::Result<T, crate::impls::Error>;

#[derive(Debug)]
pub struct Taos(RawTaos);

unsafe impl Send for Taos {}
unsafe impl Sync for Taos {}

impl Taos {
    fn new<'a>(
        host: impl Into<NullableCStr<'a>>,
        user: impl Into<NullableCStr<'a>>,
        pass: impl Into<NullableCStr<'a>>,
        db: impl Into<NullableCStr<'a>>,
        port: u16,
    ) -> Result<Self> {
        Ok(Self(RawTaos::connect(
            host.into().as_ptr(),
            user.into().as_ptr(),
            pass.into().as_ptr(),
            db.into().as_ptr(),
            port,
        )?))
    }

    pub(crate) fn as_raw(&self) -> *mut taos_sys::ffi::TAOS {
        self.0.as_ptr()
    }
}

pub mod block;

pub fn client_info() -> &'static str {
    static ONCE: Once = Once::new();
    static mut VERSION: &str = "";
    ONCE.call_once(|| unsafe {
        VERSION = RawTaos::version()
            .to_str()
            .expect("get client info should always be ok");
    });
    unsafe { VERSION }
}
pub mod stmt;

#[cfg(feature = "r2d2")]
pub mod r2d2;

pub mod prelude {
    //! Preludes for async/await queries.
    //!
    //! ```rust
    //! use taos::prelude::*;
    //! use tokio;
    //!
    //! #[tokio::main]
    //! async fn main() -> anyhow::Result<()> {
    //!     let taos = TaosOptions::default().build()?;
    //!     taos.exec("drop database if exists test_prelude").await?;
    //!     taos.exec("create database test_prelude precision 'us'").await?;
    //!     taos.exec("use test_prelude").await?;
    //!     taos.exec("create stable meters (ts timestamp, current float, voltage int, phase float) \
    //!                tags(gid int, location binary(16))").await?;
    //!     let count: u32 = taos.query_one("select count(*) from meters").await?.unwrap_or(0);
    //!     assert!(count == 0);
    //!
    //!     let results = taos.query("select * from meters").await?;
    //!     assert!(results.precision() == "us");
    //!     assert_eq!(results.num_of_fields(), 6);
    //!     Ok(())
    //! }
    //! ```
    pub use crate::impls::Error;
    pub use crate::impls::ResultSet;
    // pub use crate::impls::SyncBlock;
    pub use crate::options::TaosOptions;
    pub use crate::query::FromDsn;
    pub use crate::schemaless::{SchemalessPrecision, SchemalessProtocol};
    pub use crate::stmt::{TaosBind, TaosMultiBind};
    pub use crate::Taos;
    pub use taos_query::common::{Precision, RawBlock, Timestamp, Ty, Value};
    pub use taos_query::{common, AsyncFetchable, AsyncQueryable, BlockCodec, BlockExt};

    #[cfg(feature = "r2d2")]
    pub use crate::r2d2::TaosPool;

    pub type Manager = taos_query::Manager<Taos>;

    #[cfg(feature = "r2d2")]
    pub type Pool = taos_query::Pool<Taos>;

    pub mod sync {

        pub use crate::impls::Error;
        pub use crate::impls::ResultSet;
        // pub use crate::impls::SyncBlock;
        pub use crate::options::TaosOptions;
        pub use crate::query::FromDsn;
        pub use crate::schemaless::{SchemalessPrecision, SchemalessProtocol};
        pub use crate::stmt::{TaosBind, TaosMultiBind};
        pub use crate::Taos;
        // pub use mdsn::{Dsn, IntoDsn};

        pub use taos_query::common::{Precision, RawBlock, Timestamp, Ty, Value};
        pub use taos_query::{common, BlockCodec, BlockExt, Fetchable, Queryable};

        #[cfg(feature = "r2d2")]
        pub use crate::r2d2::TaosPool;

        pub type Manager = taos_query::Manager<Taos>;

        #[cfg(feature = "r2d2")]
        pub type Pool = taos_query::Pool<Taos>;
    }
}
#[cfg(feature = "test")]
pub use taos_macros::test;

// pub use taos_query::BlockExt;
#[cfg(test)]
mod tests {
    use super::{client_info, Taos, TaosOptions};
    use taos_macros::test;

    use crate::prelude::*;
    use anyhow::Result;

    #[test]
    fn test_invalid_database() {
        let res = TaosOptions::default().database("invalid_database").build();
        assert!(res.is_err());

        let err = res.unwrap_err();
        dbg!(err);
    }

    #[test(log_level = "trace")]
    async fn test_information_schema(taos: &Taos) -> Result<()> {
        let info: Vec<Value> = taos
            .query_one("select * from information_schema.user_databases where name like 'infor%'")
            .await?
            .unwrap();
        dbg!(info);
        Ok(())
    }
    #[test]
    async fn test_describe(taos: &Taos) -> Result<()> {
        let desc = taos.describe("log.logs").await?;
        dbg!(desc);
        Ok(())
    }
    #[tokio::test]
    async fn test_databases() -> Result<()> {
        let taos = TaosOptions::new().build()?;
        let desc = taos.databases().await?;
        println!("done");
        dbg!(desc);
        Ok(())
    }
    #[test(crate)]
    fn test_client_info() {
        let version = client_info();
        dbg!(format!("{version}"));
    }

    #[test(crate)]
    fn test_err() {
        fn err_with_res() -> Result<()> {
            let taos = Taos::new(
                "localhost",
                std::ptr::null() as *const i8,
                "taosdata",
                std::ptr::null() as *const i8,
                0,
            )?;
            taos.query_sync("select * from unknown-db.abc")?;
            Ok(())
        }
        err_with_res().expect_err("");
    }
    #[test(crate)]
    fn query_async_await_future_test() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let taos = Taos::new("localhost", "root", "taosdata", "log", 0).unwrap();
                let mut res = taos.query("show databases").await.unwrap();
                let stream = res.block_stream();

                use futures::stream::StreamExt;
                let lengths = stream
                    .enumerate()
                    .map(|(bi, partial)| {
                        partial
                            .iter_rows()
                            .enumerate()
                            .map(|(ri, values)| {
                                println!("block {bi}, row {ri}: {values:?}");
                                return 1;
                            })
                            .sum::<usize>()
                    })
                    .fold(0, |acc, n| futures::future::ready(acc + n))
                    .await;
                println!("lengths is {lengths}");
            });
    }
}
