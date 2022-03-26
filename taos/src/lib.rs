use block::{Block, Row};
use futures::Stream;
use serde::de::{self, DeserializeOwned};
use std::{
    borrow::Cow,
    ffi::CStr,
    fmt::{self, Display},
    future::Future,
    rc::Rc,
    str::FromStr,
    sync::Arc,
};
pub use taos_error::{Code, Error};
use taos_sys::*;

pub type TaosError = Error;

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

#[cfg(feature = "tmq")]
pub mod tmq;

type Result<T> = std::result::Result<T, Error>;

pub struct Taos(*mut TAOS);

unsafe impl Send for Taos {}

impl Drop for Taos {
    fn drop(&mut self) {
        unsafe {
            taos_close(self.0);
        }
    }
}

impl Taos {
    pub(crate) fn new<'a>(
        ip: impl Into<NullableCStr<'a>>,
        user: impl Into<NullableCStr<'a>>,
        pass: impl Into<NullableCStr<'a>>,
        db: impl Into<NullableCStr<'a>>,
        port: u16,
    ) -> Result<Self> {
        unsafe {
            taos_connect(
                ip.into().as_ptr(),
                user.into().as_ptr(),
                pass.into().as_ptr(),
                db.into().as_ptr(),
                port,
            )
            .as_mut()
        }
        .map(|p| Taos(p as _))
        .ok_or(TaosError::from_string("invalid connection"))
    }

    pub fn query_sync<'query>(
        &'query self,
        sql: impl IntoCStr<'query>,
    ) -> Result<TaosResult<'query>> {
        TaosResult::try_from_ptr(unsafe { taos_query(self.0, sql.into_c_str().as_ptr()) })
    }

    pub fn query_sync2<'query>(
        &'query self,
        sql: impl IntoCStr<'query>,
    ) -> Result<TaosResult<'query>> {
        futures::executor::block_on(self.query(sql))
    }

    pub fn query_with_callback<'a, F>(&'a mut self, _callback: F)
    where
        F: FnOnce(TaosResult<'a>, i32) -> (),
    {
        unimplemented!()
        // let callback = Box::new(callback);
        // eprintln!("callback: {:p}", callback);
        // let ptr = Box::into_raw(callback) as *mut c_void;
    }

    /// b"select * from log.logs\0" as *const u8 as _
    fn query_c_str<'query>(
        &'query self,
        sql: *const i8,
    ) -> impl Future<Output = Result<TaosResult<'query>>> {
        async_query::QueryFuture::new(self, sql)
    }

    /// Asynchronously query with sql
    pub fn query<'a, 'query>(
        &'query self,
        sql: impl IntoCStr<'a>,
    ) -> impl Future<Output = Result<TaosResult<'query>>> {
        self.query_c_str(sql.into_c_str().as_ptr() as _)
    }

    pub async fn exec<'a, 'query>(&'query self, sql: impl IntoCStr<'a>) -> Result<usize> {
        let res = self.query(sql).await?;
        Ok(res.affected_rows() as _)
    }

    pub fn exec_sync<'a, 'query>(&'query self, sql: impl IntoCStr<'a>) -> Result<usize> {
        futures::executor::block_on(self.exec(sql))
    }

    pub(crate) fn as_raw(&self) -> *mut taos_sys::TAOS {
        self.0
    }

    pub async fn describe(&self, table: &str) -> Result<Vec<ColumnMeta>> {
        todo!()
        // self.query(&format!("describe {}", table))
        //     .await
        //     .map(TaosDescribe::from)
    }
}

#[test]
#[should_panic]
fn async_query_callback_test() {
    let mut taos = Taos::new("localhost", "root", "taosdata", "", 0).unwrap();
    let callback = |res: TaosResult, code| {
        println!("callback in rust with code: {code}");
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

#[derive(Debug)]
pub enum TaosResult<'a> {
    WithFields(*mut TAOS_RES, &'a [TAOS_FIELD]),
    WithoutFields(*mut TAOS_RES),
}

impl<'a> Drop for TaosResult<'a> {
    fn drop(&mut self) {
        unsafe {
            if !self.as_raw().is_null() {
                taos_free_result(self.as_raw());
            }
        }
    }
}

impl<'a> TaosResult<'a> {
    const fn as_raw(&self) -> *mut TAOS_RES {
        match self {
            TaosResult::WithFields(res, _) => *res,
            TaosResult::WithoutFields(res) => *res,
        }
    }

    fn try_from_ptr(result: *mut TAOS_RES) -> Result<Self> {
        Self::new(result, unsafe { taos_errno(result) })
    }

    fn new(result: *mut TAOS_RES, code: i32) -> Result<Self> {
        log::debug!("result code: {code}");
        let code: Code = (code & 0xffff).into();
        if code.success() {
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
            let err_str = err_str.to_string_lossy();
            if err_str == "success" {
                return Self::new(result, 0);
            }
            Err(TaosError::new(code, err_str))
        }
    }
    unsafe fn get_fields_unchecked(&self) -> &'a [TAOS_FIELD] {
        match self {
            TaosResult::WithFields(_, fields) => fields,
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }
    fn get_field_names(&self) -> Vec<&'a CStr> {
        match self {
            TaosResult::WithFields(_, fields) => fields.into_iter().map(|f| f.name()).collect(),
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }
    fn get_field_names_to_string_vec(&self) -> Vec<String> {
        match self {
            TaosResult::WithFields(_, fields) => fields
                .into_iter()
                .map(|f| f.name().to_string_lossy().into_owned())
                .collect(),
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }
    unsafe fn get_field_unchecked(&self, index: usize) -> &TAOS_FIELD {
        match self {
            TaosResult::WithFields(_, fields) => fields.get_unchecked(index),
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }

    const fn num_fields(&self) -> usize {
        match self {
            TaosResult::WithFields(_, fields) => fields.len(),
            _ => 0,
        }
    }

    fn precision(&self) -> TimestampPrecision {
        unsafe { taos_result_precision(self.as_raw()) }.into()
    }

    pub fn affected_rows(&self) -> usize {
        unsafe { taos_affected_rows(self.as_raw()) as _ }
    }

    pub fn fetch_block_stream(&self) -> block::BlockStream {
        block::BlockStream::new(self)
    }

    pub fn rows_stream(&self) -> impl Stream<Item = Row> {
        use futures::StreamExt;
        block::BlockStream::new(self).flat_map(|block| {
            futures::stream::iter(block.into_iter_rows())
            // let size = block.num_of_rows();

            // let block = Rc::new(block);
            // futures::stream::iter(
            //     std::iter::repeat(block)
            //         .take(size as _)
            //         .enumerate()
            //         .map(|(row_i, block)| Row::new(block)),
            // )
        })
    }
    pub fn rows_de_stream<T>(&self) -> impl Stream<Item = Result<T>> + '_
    where
        T: DeserializeOwned,
    {
        use futures::StreamExt;

        self.rows_stream()
            .map(|row| T::deserialize(&mut row.deserializer()))
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
    dbg!(format!("{version}"));
}

#[test]
#[should_panic]
fn test_err() {
    fn err_with_res() -> Result<()> {
        let taos = Taos::new(
            "localhost",
            std::ptr::null() as *const i8,
            "taosdata",
            "",
            0,
        )?;
        taos.query_sync("select * from log.logs where")?;
        Ok(())
    }
    err_with_res().unwrap();
}

// pub mod schemaless;

pub mod stmt;

#[cfg(feature = "r2d2")]
pub mod r2d2;
