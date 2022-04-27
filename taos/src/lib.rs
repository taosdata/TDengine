use futures::TryStreamExt;
use std::{ffi::c_void, future::Future, os::raw::c_int, sync::Once};

pub use taos_error::*;
use taos_sys::*;

macro_rules! custom_error {
    ($err:expr) => {
        <::taos_error::Error as ::serde::de::Error>::custom($err)
    };
}

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

pub mod timestamp;

mod options;

pub use options::TaosOptions;

pub mod util;
use util::*;

pub mod future;

pub mod async_query;

pub mod helpers;

use helpers::*;

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
    pub fn new<'a>(
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
    pub fn query<'a, 'query>(
        &'query self,
        sql: impl IntoCStr<'a>,
    ) -> impl Future<Output = Result<TaosResult<'query>>> {
        async_query::QueryFuture::new(self, sql)
    }

    /// Query without result.
    pub async fn exec<'a, 'query>(&'query self, sql: impl IntoCStr<'a>) -> Result<usize> {
        let res = self.query(sql).await?;
        Ok(res.affected_rows() as _)
    }

    pub fn exec_sync<'a, 'query>(&'query self, sql: impl IntoCStr<'a>) -> Result<usize> {
        futures::executor::block_on(self.exec(sql))
    }

    pub fn query_sync<'query>(
        &'query self,
        sql: impl IntoCStr<'query>,
    ) -> Result<TaosResult<'query>> {
        futures::executor::block_on(self.query(sql))
    }

    pub fn query_sync2<'query>(
        &'query self,
        sql: impl IntoCStr<'query>,
    ) -> Result<TaosResult<'query>> {
        futures::executor::block_on(self.query2(sql))
    }
    pub fn query_sync3<'query>(
        &'query self,
        sql: impl IntoCStr<'query>,
    ) -> Result<TaosResult<'query>> {
        futures::executor::block_on(self.query3(sql))
    }
    pub async fn query2<'a, 'q>(&'q self, sql: impl IntoCStr<'a>) -> Result<TaosResult<'q>> {
        // use tokio::sync::oneshot;
        use oneshot::channel;
        use oneshot::Sender;
        // use std::sync::mpsc::channel;
        // use std::sync::mpsc::Sender;
        let (sender, rx) = channel();

        pub unsafe extern "C" fn async_query_callback(
            param: *mut c_void,
            res: *mut c_void,
            code: c_int,
        ) {
            assert!(code == 0);
            // let _ = RawRes::from_ptr(res);
            let v = TaosResult::try_from_ptr(res);
            // let param = param as *mut CallbackArg;
            // let args = Box::from_raw(param);
            // let CallbackArg { sender } = *args;
            // sender.send(v).unwrap();
            let sender = param as *mut Sender<_>;
            let sender = Box::from_raw(sender);

            sender.send(v).unwrap();
        }
        // let args = CallbackArg { sender };
        // let args = Box::new(args);
        // let ptr = Box::pin(tx);
        self.0.query_a(
            sql.into_c_str().as_ptr(),
            async_query_callback as _,
            Box::into_raw(Box::new(sender)) as *mut _,
        );
        rx.await.unwrap()
        // rx.await.map_err(|e| Error::from_string(format!("{}", e)))
    }
    pub async fn query3<'a, 'q>(&'q self, sql: impl IntoCStr<'a>) -> Result<TaosResult<'q>> {
        // use tokio::sync::oneshot;
        use tokio::sync::oneshot::channel;
        use tokio::sync::oneshot::Sender;
        // use std::sync::mpsc::channel;
        // use std::sync::mpsc::Sender;
        let (sender, rx) = channel();

        pub unsafe extern "C" fn async_query_callback(
            param: *mut c_void,
            res: *mut c_void,
            code: c_int,
        ) {
            assert!(code == 0);
            // let _ = RawRes::from_ptr(res);
            let v = TaosResult::try_from_ptr(res);
            // let param = param as *mut CallbackArg;
            // let args = Box::from_raw(param);
            // let CallbackArg { sender } = *args;
            // sender.send(v).unwrap();
            let sender = param as *mut Sender<_>;
            let sender = Box::from_raw(sender);

            sender.send(v).unwrap();
        }
        // let args = CallbackArg { sender };
        // let args = Box::new(args);
        // let ptr = Box::pin(tx);
        self.0.query_a(
            sql.into_c_str().as_ptr(),
            async_query_callback as _,
            Box::into_raw(Box::new(sender)) as *mut _,
        );
        rx.await.unwrap()
        // rx.await.map_err(|e| Error::from_string(format!("{}", e)))
    }

    pub fn query_sync0<'query>(
        &'query self,
        sql: impl IntoCStr<'query>,
    ) -> Result<TaosResult<'query>> {
        self.0
            .query(sql.into_c_str().as_ptr())
            .map(TaosResult::from_raw)
    }

    pub(crate) fn as_raw(&self) -> *mut taos_sys::ffi::TAOS {
        self.0.as_ptr()
    }

    pub async fn describe(&self, table: &str) -> Result<Vec<ColumnMeta>> {
        use futures::stream::StreamExt;
        self.query(format!("describe {table}"))
            .await?
            .rows_de_stream::<ColumnMeta>()
            .map(|res| res.map_err(<Error as serde::de::Error>::custom))
            .try_collect()
            .await
    }
    pub async fn databases(&self) -> Result<Vec<ShowDatabase>> {
        use futures::stream::StreamExt;
        self.query(format!("show databases"))
            .await?
            .rows_de_stream()
            .map(|res| res.map_err(<Error as serde::de::Error>::custom))
            .try_collect()
            .await
    }

    pub async fn show_create(&self) -> Result<()> {
        todo!()
    }
}

pub use result::*;

pub mod block;
pub use block::Value;

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
#[cfg(feature = "r2d2")]
pub use crate::r2d2::TaosPool;

pub mod prelude {
    #[cfg(feature = "test")]
    pub use taos_macros::test;
}

pub mod query;

pub use taos_query::BlockExt;
#[cfg(test)]
mod tests {
    use super::{client_info, Result, Taos, TaosOptions};
    use taos_macros::test;
    use taos_query::BlockExt;
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
                let stream = res.fetch_block_stream();

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
