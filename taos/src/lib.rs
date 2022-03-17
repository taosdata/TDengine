use std::{
    borrow::Cow,
    ffi::{c_void, CStr},
    fmt::{self, Display},
    future::Future,
    os::raw::c_char,
    task::{Poll, Waker},
};
use taos_sys::*;
use thiserror::Error;

pub mod error;
pub mod timestamp;
pub use error::*;

pub mod options;
pub use options::TaosOptions;

pub mod util;
use util::*;

pub mod future;

pub mod async_query;

#[cfg(feature = "tmq")]
pub mod tmq;
#[derive(Error, Debug)]
pub struct TaosError {
    pub code: TaosCode,
    pub err: Cow<'static, str>,
}

impl TaosError {
    pub fn new(code: TaosCode, err: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code,
            err: err.into(),
        }
    }
}
impl Display for TaosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.err)
    }
}

type Result<T> = std::result::Result<T, TaosError>;

#[repr(transparent)]
pub struct Taos(*mut TAOS);

impl Drop for Taos {
    fn drop(&mut self) {
        unsafe {
            taos_close(self.0);
        }
    }
}

impl Taos {
    pub fn new(
        ip: impl ToCString,
        user: impl ToCString,
        pass: impl ToCString,
        db: impl ToCString,
        port: u16,
    ) -> Result<Self> {
        let ip = ip.to_c_string();
        let user = user.to_c_string();
        let pass = pass.to_c_string();
        let db = db.to_c_string();

        unsafe {
            taos_options(TSDB_OPTION_CHARSET, "UTF-8".to_c_string().as_ptr() as _);
        }
        unsafe {
            let null = std::ptr::null_mut() as *mut i8;
            let conn = taos_connect(
                // b"localhost\x00" as *const u8 as *const c_char,
                ip.as_ptr(),
                // b"root\x00" as *const u8 as *const c_char, //null,
                user.as_ptr(),
                pass.as_ptr(), // null,
                db.as_ptr(), // null,
                port,
            );
            // ip.as_ptr(),
            // user.as_ptr(),
            // pass.as_ptr(),
            // db.as_ptr(),
            //     port as u16,
            // )
            // .as_mut();
            if conn.is_null() {
                Err(TaosError::new(
                    TaosCode::TscInvalidConnection,
                    "invalid connection",
                ))
            } else {
                Ok(Taos(conn as _))
            }
        }
    }

    pub fn query_sync<'query>(&'query self, sql: &str) -> Result<TaosResult<'query>> {
        unsafe {
            let res = taos_query(self.0, sql.to_c_string().as_ptr() as _);
            let code = taos_errno(self.0);
            TaosResult::new(res, code)
        }
    }
    pub fn query_sync2<'query>(&'query self, sql: &str) -> Result<TaosResult<'query>> {
        futures::executor::block_on(self.query(sql))
    }

    pub fn query_with_callback<'a, F>(&'a mut self, callback: F)
    where
        F: FnOnce(TaosResult<'a>, i32) -> (),
    {
        let callback = Box::new(callback);
        eprintln!("callback: {:p}", callback);
        let ptr = Box::into_raw(callback) as *mut c_void;
    }

    /// b"select * from log.logs\0" as *const u8 as _
    fn query_c_str<'query>(
        &'query self,
        sql: *const i8,
    ) -> impl Future<Output = Result<TaosResult<'query>>> {
        async_query::QueryFuture::new(self, sql)
    }

    /// Asynchronously query with sql
    pub fn query<'query>(
        &'query self,
        sql: &str,
    ) -> impl Future<Output = Result<TaosResult<'query>>> {
        self.query_c_str(sql.to_c_string().as_ptr() as _)
    }

    pub async fn exec(&self, sql: impl AsRef<str>) -> Result<usize> {
        let res = self.query(sql.as_ref()).await?;
        Ok(res.affected_rows() as _)
    }

    pub(crate) fn as_raw(&self) -> *mut taos_sys::TAOS {
        self.0
    }
}

#[test]
fn async_query_callback_test() {
    let mut taos = Taos::new("localhost", "root", "taosdata", "", 0).unwrap();
    let callback = |res: TaosResult, code| {
        println!("callback in rust with code: {code}");
        println!("ptr: {:p}", res.as_raw());
        let rows = res.affected_rows();
        println!("rows: {rows}");
    };
    use tokio::time::{sleep, Duration};
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
            let res = taos.query("select * from log.logs ").await.unwrap();
            let stream = res.fetch_block_stream();

            use futures::stream::StreamExt;
            let lengths = stream
                .enumerate()
                .map(|(idx, partial)| {
                    partial
                        .rows_iter()
                        .enumerate()
                        .map(|(idx, values)| {
                            println!("block {idx}, row {idx}: {values:?}");
                            return 1;
                        })
                        .sum::<usize>()
                })
                .fold(0, |acc, n| futures::future::ready(acc + n))
                .await;
            println!("lengths is {lengths}");
        });
}

#[derive(Debug)]
pub enum TaosResult<'a> {
    WithFields(*mut TAOS_RES, &'a [TAOS_FIELD]),
    WithoutFields(*mut TAOS_RES),
}

impl<'a> Drop for TaosResult<'a> {
    fn drop(&mut self) {
        eprintln!("free result {:p}", self.as_raw());
        unsafe {
            if !self.as_raw().is_null() {
                taos_free_result(self.as_raw());
            }
        }
    }
}

impl<'a> TaosResult<'a> {
    fn as_raw(&self) -> *mut TAOS_RES {
        match self {
            TaosResult::WithFields(res, _) => *res,
            TaosResult::WithoutFields(res) => *res,
        }
    }

    fn try_from_ptr(result: *mut TAOS_RES) -> Result<Self> {
        Self::new(result, unsafe { taos_errno(result) })
    }

    fn new(result: *mut TAOS_RES, code: i32) -> Result<Self> {
        let code = (code & 0xffff).into();
        if code == TaosCode::Success {
            let num_fields = unsafe { taos_num_fields(result) };
            if num_fields == 0 {
                Ok(TaosResult::WithoutFields(result))
            } else {
                let fields = unsafe {
                    std::slice::from_raw_parts(taos_fetch_fields(result), num_fields as _)
                };
                Ok(TaosResult::WithFields(result, fields))
            }
        } else {
            let err_str = unsafe { CStr::from_ptr(taos_errstr(result)) };
            Err(TaosError::new(code, err_str.to_string_lossy()))
        }
    }
    unsafe fn get_fields_unchecked(&self) -> &[TAOS_FIELD] {
        match self {
            TaosResult::WithFields(_, fields) => fields,
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }

    fn num_fields(&self) -> usize {
        match self {
            TaosResult::WithFields(_, fields) => fields.len(),
            _ => 0,
        }
    }

    fn precision(&self) -> TimestampPrecision {
        unsafe { taos_result_precision(self.as_raw()) }.into()
    }

    pub fn affected_rows(&self) -> i32 {
        unsafe { taos_affected_rows(self.as_raw()) as _ }
    }

    pub fn fetch_block_stream(&self) -> block::BlockStream {
        block::BlockStream::new(self)
    }
}

pub mod block;

pub fn client_info() -> String {
    unsafe { CStr::from_ptr(taos_get_client_info()) }
        .to_string_lossy()
        .to_string()
}

#[test]
fn test_client_info() {
    let version = client_info();
    dbg!("{version}");
}

pub mod schemaless;
