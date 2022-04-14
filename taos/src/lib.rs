use futures::TryStreamExt;
use std::{future::Future, sync::Once};

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

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Taos(*mut TAOS);

unsafe impl Send for Taos {}
unsafe impl Sync for Taos {}

impl Drop for Taos {
    fn drop(&mut self) {
        unsafe {
            taos_close(self.0);
        }
    }
}

impl Taos {
    pub fn new<'a>(
        host: impl Into<NullableCStr<'a>>,
        user: impl Into<NullableCStr<'a>>,
        pass: impl Into<NullableCStr<'a>>,
        db: impl Into<NullableCStr<'a>>,
        port: u16,
    ) -> Result<Self> {
        unsafe {
            taos_connect(
                host.into().as_ptr(),
                user.into().as_ptr(),
                pass.into().as_ptr(),
                db.into().as_ptr(),
                port,
            )
            .as_mut()
        }
        .map(|ptr| Taos(ptr as _))
        .ok_or_else(|| Error::from_string("invalid connection"))
    }

    pub fn query_with_callback<F>(&mut self, _callback: F)
    where
        F: FnOnce(Result<TaosResult>),
    {
        unimplemented!()
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
        TaosResult::try_from_ptr(unsafe { taos_query(self.0, sql.into_c_str().as_ptr()) })
    }

    pub(crate) fn as_raw(&self) -> *mut taos_sys::TAOS {
        self.0
    }

    pub async fn describe(&self, table: &str) -> Result<Vec<ColumnMeta>> {
        self.query(format!("describe {table}"))
            .await?
            .rows_de_stream()
            .try_collect()
            .await
    }
    pub async fn databases(&self) -> Result<Vec<ShowDatabase>> {
        self.query(format!("show databases"))
            .await?
            .rows_de_stream()
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
        VERSION = std::ffi::CStr::from_ptr(taos_get_client_info())
            .to_str()
            .expect("get client info should always be ok");
    });
    unsafe { VERSION }
}
pub mod stmt;

#[cfg(feature = "r2d2")]
pub mod r2d2;

pub mod prelude {
    #[cfg(feature = "test")]
    pub use taos_macros::test;
}

#[cfg(test)]
mod tests {
    use super::{client_info, Result, Taos, TaosOptions, TaosResult};
    #[tokio::test]
    async fn test_describe() -> Result<()> {
        let taos = TaosOptions::new().build()?;
        let desc = taos.describe("log.logs").await?;
        dbg!(desc);
        Ok(())
    }
    #[tokio::test]
    async fn test_databases() -> Result<()> {
        std::env::set_var("RUST_LOG", "TRACE");
        simple_logger::init().unwrap();
        let taos = TaosOptions::new().build()?;
        let desc = taos.databases().await?;
        println!("done");
        dbg!(desc);
        Ok(())
    }
    #[test]
    fn test_client_info() {
        let version = client_info();
        dbg!(format!("{version}"));
    }

    #[test]
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

    #[test]
    #[should_panic]
    fn async_query_callback_test() {
        let mut taos = Taos::new("localhost", "root", "taosdata", "", 0).unwrap();
        let callback = |res: Result<TaosResult>| {
            let res = res.unwrap();
            println!("ptr: {:p}", res.as_raw());
            let rows = res.affected_rows();
            println!("rows: {rows}");
        };
        taos.query_with_callback(callback);
        println!("wait for 10 seconds");
        std::thread::sleep(std::time::Duration::from_secs(10));
        println!("wait finished");
        println!("done");
    }

    #[test]
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
                            .rows_iter()
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
