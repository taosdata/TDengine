use std::sync::Once;

pub use taos_error::*;
pub use taos_query as query;
use taos_sys::*;

macro_rules! err {
    (custom $err:expr) => {
        <::taos_error::Error as ::serde::de::Error>::custom($err)
    };
    ('str $err:expr) => {
        crate::Error::from_string($err)
    };
    ($err:expr) => {
        todo!()
        // Err(<::taos_error::Error as ::serde::de::Error>::custom($err))
        // Err()
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

mod result;

pub mod tmq;

mod schemaless;

mod impls;

pub type Result<T> = std::result::Result<T, Error>;

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
        RawTaos::connect(
            host.into().as_ptr(),
            user.into().as_ptr(),
            pass.into().as_ptr(),
            db.into().as_ptr(),
            port,
        )
        .map(Self)
        .ok_or_else(|| Error::from_string("invalid connection"))
    }

    /// Asynchronously query with sql
    // pub fn query<'a, 'query>(
    //     &'query self,
    //     sql: impl IntoCStr<'a>,
    // ) -> impl Future<Output = Result<TaosResult<'query>>> {
    //     async_query::QueryFuture::new(self, sql)
    // }

    /// Query without result.
    // pub async fn exec<'a, 'query>(&'query self, sql: impl IntoCStr<'a>) -> Result<usize> {
    //     let res = self.query(sql).await?;
    //     Ok(res.affected_rows() as _)
    // }

    // pub fn exec_sync<'a, 'query>(&'query self, sql: impl IntoCStr<'a>) -> Result<usize> {
    //     futures::executor::block_on(self.exec(sql))
    // }

    // pub fn query_sync<'query>(
    //     &'query self,
    //     sql: impl IntoCStr<'query>,
    // ) -> Result<TaosResult<'query>> {
    //     futures::executor::block_on(self.query(sql))
    // }

    // pub fn query_sync2<'query>(
    //     &'query self,
    //     sql: impl IntoCStr<'query>,
    // ) -> Result<TaosResult<'query>> {
    //     futures::executor::block_on(self.query2(sql))
    // }
    // pub fn query_sync3<'query>(
    //     &'query self,
    //     sql: impl IntoCStr<'query>,
    // ) -> Result<TaosResult<'query>> {
    //     futures::executor::block_on(self.query3(sql))
    // }
    // pub async fn query2<'a, 'q>(&'q self, sql: impl IntoCStr<'a>) -> Result<TaosResult<'q>> {
    //     // use tokio::sync::oneshot;
    //     use oneshot::channel;
    //     use oneshot::Sender;
    //     // use std::sync::mpsc::channel;
    //     // use std::sync::mpsc::Sender;
    //     let (sender, rx) = channel();

    //     pub unsafe extern "C" fn async_query_callback(
    //         param: *mut c_void,
    //         res: *mut c_void,
    //         code: c_int,
    //     ) {
    //         assert!(code == 0);
    //         // let _ = RawRes::from_ptr(res);
    //         let v = TaosResult::try_from_ptr(res);
    //         // let param = param as *mut CallbackArg;
    //         // let args = Box::from_raw(param);
    //         // let CallbackArg { sender } = *args;
    //         // sender.send(v).unwrap();
    //         let sender = param as *mut Sender<_>;
    //         let sender = Box::from_raw(sender);

    //         sender.send(v).unwrap();
    //     }
    //     // let args = CallbackArg { sender };
    //     // let args = Box::new(args);
    //     // let ptr = Box::pin(tx);
    //     self.0.query_a(
    //         sql.into_c_str().as_ptr(),
    //         async_query_callback as _,
    //         Box::into_raw(Box::new(sender)) as *mut _,
    //     );
    //     rx.await.unwrap()
    //     // rx.await.map_err(|e| Error::from_string(format!("{}", e)))
    // }
    // pub async fn query3<'a, 'q>(&'q self, sql: impl IntoCStr<'a>) -> Result<TaosResult<'q>> {
    //     // use tokio::sync::oneshot;
    //     use tokio::sync::oneshot::channel;
    //     use tokio::sync::oneshot::Sender;
    //     // use std::sync::mpsc::channel;
    //     // use std::sync::mpsc::Sender;
    //     let (sender, rx) = channel();

    //     pub unsafe extern "C" fn async_query_callback(
    //         param: *mut c_void,
    //         res: *mut c_void,
    //         code: c_int,
    //     ) {
    //         assert!(code == 0);
    //         // let _ = RawRes::from_ptr(res);
    //         let v = TaosResult::try_from_ptr(res);
    //         // let param = param as *mut CallbackArg;
    //         // let args = Box::from_raw(param);
    //         // let CallbackArg { sender } = *args;
    //         // sender.send(v).unwrap();
    //         let sender = param as *mut Sender<_>;
    //         let sender = Box::from_raw(sender);

    //         sender.send(v).unwrap();
    //     }
    //     // let args = CallbackArg { sender };
    //     // let args = Box::new(args);
    //     // let ptr = Box::pin(tx);
    //     self.0.query_a(
    //         sql.into_c_str().as_ptr(),
    //         async_query_callback as _,
    //         Box::into_raw(Box::new(sender)) as *mut _,
    //     );
    //     rx.await.unwrap()
    //     // rx.await.map_err(|e| Error::from_string(format!("{}", e)))
    // }

    // pub fn query_sync0<'query>(
    //     &'query self,
    //     sql: impl IntoCStr<'query>,
    // ) -> Result<TaosResult<'query>> {
    //     self.0
    //         .query(sql.into_c_str().as_ptr())
    //         .map(TaosResult::from_raw)
    // }

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
    pub use crate::impls::ResultSet;
    pub use crate::options::TaosOptions;
    pub use crate::Taos;
    pub use taos_query::common::{Precision, Timestamp, Ty, Value};
    pub use taos_query::{common, AsyncFetchable, AsyncQueryable, BlockCodec, BlockExt};

    #[cfg(feature = "r2d2")]
    pub use crate::r2d2::TaosPool;

    pub mod sync {

        pub use crate::impls::ResultSet;
        pub use crate::options::TaosOptions;
        pub use crate::Taos;
        pub use taos_query::common::{Precision, Timestamp, Ty, Value};
        pub use taos_query::{common, BlockCodec, BlockExt, Fetchable, Queryable};

        #[cfg(feature = "r2d2")]
        pub use crate::r2d2::TaosPool;
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
            taos.query_sync("select * from log.logs")?;
            Ok(())
        }
        err_with_res().unwrap();
    }
    #[test(crate)]
    fn query_async_await_future_test() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let taos = Taos::new("localhost", "root", "taosdata", "log", 0).unwrap();
                let res = taos
                    .query("select * from log.logs limit 10000")
                    .await
                    .unwrap();
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
