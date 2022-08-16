#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]

//! TDengine database connector use native C library.
//!

use std::{
    cell::UnsafeCell,
    ffi::CString,
    fmt::Display,
    task::{Context, Poll},
};

use once_cell::sync::OnceCell;
use query::blocks::SharedState;
use taos_error::Error as RawError;
// use taos_query::{AsyncFetchable, AsyncQueryable, DsnError, Fetchable, Queryable, TBuilder};

mod into_c_str;
pub mod stmt;

use into_c_str::IntoCStr;

pub(crate) mod types;

mod ffi;
// use ffi::*;

mod set_config;

use ffi::taos_options;

pub mod schemaless;
// pub use schemaless::*;

pub mod tmq;
pub use tmq::{Consumer, TmqBuilder};

mod conn;
use conn::RawTaos;

mod query;
use query::RawRes;

pub use taos_query::prelude::*;
pub use types::TaosMultiBind;
pub use stmt::Stmt;

#[macro_export(local_inner_macros)]
macro_rules! err_or {
    ($res:ident, $code:expr, $ret:expr) => {
        unsafe {
            let code: Code = { $code }.into();
            if code.success() {
                Ok($ret)
            } else {
                Err(Error::new(code, $res.err_as_str()))
            }
        }
    };

    ($res:ident, $code:expr) => {{
        err_or!($res, $code, ())
    }};
    ($code:expr, $ret:expr) => {
        unsafe {
            let code: Code = { $code }.into();
            if code.success() {
                Ok($ret)
            } else {
                Err(Error::from_code(code))
            }
        }
    };

    ($code:expr) => {
        err_or!($code, ())
    };
}

#[derive(Debug)]
pub struct Taos {
    raw: RawTaos,
}

impl Drop for Taos {
    fn drop(&mut self) {
        self.raw.close();
    }
}

impl taos_query::Queryable for Taos {
    type Error = RawError;

    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> Result<Self::ResultSet, Self::Error> {
        self.raw.query(sql.as_ref())
    }

    fn write_meta(&self, meta: taos_query::common::RawMeta) -> Result<(), Self::Error> {
        let raw = meta.as_raw_data_t();
        self.raw.write_raw_meta(raw)
    }
}

#[async_trait::async_trait]
impl AsyncQueryable for Taos {
    type Error = RawError;

    type AsyncResultSet = ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
    ) -> Result<Self::AsyncResultSet, Self::Error> {
        self.raw
            .query_async(sql.as_ref())
            .await
            .map(|raw| ResultSet::new(raw))
    }

    async fn write_raw_meta(&self, meta: taos_query::common::RawMeta) -> Result<(), Self::Error> {
        self.raw.write_raw_meta(meta.as_raw_data_t())
    }

    async fn write_raw_block(&self, block: &RawBlock) -> Result<(), Self::Error> {
        self.raw.write_raw_block(block)
    }
}

/// Connection builder.
///
/// ## Examples
///
/// ### Synchronous
///
/// ```rust
/// use taos_sys::Builder;
/// use taos_query::prelude::sync::*;
/// fn main() -> anyhow::Result<()> {
///     let builder = Builder::from_dsn("taos://localhost:6030")?;
///     let taos = builder.connect()?;
///     let mut query = taos.query("show databases")?;
///     for row in query.rows() {
///         println!("{:?}", row?.into_values());
///     }
///     Ok(())
/// }
/// ```
///
/// ### Async
///
/// ```rust
/// use taos_sys::Builder;
/// use taos_query::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let builder = Builder::from_dsn("taos://localhost:6030")?;
///     let taos = builder.connect()?;
///     let mut query = taos.query("show databases").await?;
///
///     while let Some(row) = query.rows().try_next().await? {
///         println!("{:?}", row.into_values());
///     }
///     Ok(())
/// }
/// #
/// ```
#[derive(Debug, Default)]
pub struct TaosBuilder {
    host: Option<CString>,
    user: Option<CString>,
    pass: Option<CString>,
    db: Option<CString>,
    port: u16,
}

#[derive(Debug)]
pub struct Error(taos_error::Error);

impl From<DsnError> for Error {
    fn from(err: DsnError) -> Self {
        Self(RawError::from_string(err.to_string()))
    }
}
impl From<RawError> for Error {
    fn from(err: RawError) -> Self {
        Self(err)
    }
}
impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl TBuilder for TaosBuilder {
    type Target = Taos;

    type Error = Error;

    fn available_params() -> &'static [&'static str] {
        const PARAMS: &'static [&'static str] = &["configDir"];
        &PARAMS
    }

    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> Result<Self, Self::Error> {
        let dsn = dsn.into_dsn()?;
        let mut builder = TaosBuilder::default();
        if let Some(addr) = dsn.addresses.into_iter().next() {
            if let Some(host) = addr.host {
                builder.host.replace(CString::new(host).unwrap());
            }
            if let Some(port) = addr.port {
                builder.port = port;
            }
        }
        if let Some(db) = dsn.database {
            builder.db.replace(CString::new(db).unwrap());
        }
        if let Some(user) = dsn.username {
            builder.user.replace(CString::new(user).unwrap());
        }
        if let Some(pass) = dsn.password {
            builder.pass.replace(CString::new(pass).unwrap());
        }
        let params = dsn.params;
        if let Some(dir) = params.get("configDir") {
            let dir = CString::new(dir.as_bytes()).unwrap();
            unsafe {
                taos_options(types::TSDB_OPTION::ConfigDir, dir.as_ptr() as _);
            }
        }
        // let raw = RawTaos::connect(host, user, pass, db, port)
        Ok(builder)
    }

    fn client_version() -> &'static str {
        RawTaos::version()
    }

    fn ping(&self, conn: &mut Self::Target) -> Result<(), Self::Error> {
        conn.raw.query("select 1")?;
        Ok(())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> Result<Self::Target, Self::Error> {
        let raw = RawTaos::connect(
            self.host
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.user
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.pass
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.db
                .as_ref()
                .map(|v| v.as_ptr())
                .unwrap_or(std::ptr::null()),
            self.port,
        )?;

        Ok(Taos { raw })
    }
}

#[derive(Debug)]
pub struct ResultSet {
    raw: RawRes,
    fields: OnceCell<Vec<Field>>,
    summary: UnsafeCell<(usize, usize)>,
    state: UnsafeCell<SharedState>,
}

impl ResultSet {
    fn new(raw: RawRes) -> Self {
        Self {
            raw,
            fields: OnceCell::new(),
            summary: UnsafeCell::new((0, 0)),
            state: UnsafeCell::new(SharedState::default()),
        }
    }

    pub fn precision(&self) -> Precision {
        self.raw.precision()
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields.get_or_init(|| self.raw.fetch_fields())
    }

    pub fn ncols(&self) -> usize {
        self.raw.field_count()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.fields().iter().map(|f| f.name())
    }

    fn update_summary(&mut self, nrows: usize) {
        let summary = self.summary.get_mut();
        summary.0 += 1;
        summary.1 += nrows;
    }

    // pub(crate) fn blocks(&self) -> Blocks {
    //     self.raw.to_blocks()
    // }

    pub(crate) fn summary(&self) -> &(usize, usize) {
        unsafe { &*self.summary.get() }
    }

    pub(crate) fn affected_rows(&self) -> i32 {
        self.raw.affected_rows() as _
    }

    pub(crate) fn fetch_raw_block(&self) -> Result<Option<RawBlock>, RawError> {
        Ok(self.raw.fetch_raw_block(self.fields())?)
    }

    pub(crate) fn fetch_raw_block_async(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        self.raw
            .fetch_raw_block_async(self.fields(), self.precision(), &self.state, cx)
    }
}

impl Iterator for ResultSet {
    type Item = Result<RawBlock, RawError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw
            .fetch_raw_block(self.fields())
            .transpose()
            .map(|block| {
                block.map(|raw| {
                    let mut summary = unsafe { &mut *self.summary.get() };
                    summary.0 += 1;
                    summary.1 += raw.nrows();
                    raw
                })
            })
    }
}

impl taos_query::Fetchable for ResultSet {
    type Error = RawError;
    fn affected_rows(&self) -> i32 {
        self.affected_rows()
    }

    fn precision(&self) -> Precision {
        self.precision()
    }

    fn fields(&self) -> &[Field] {
        self.fields()
    }

    fn summary(&self) -> (usize, usize) {
        *self.summary()
    }

    fn update_summary(&mut self, nrows: usize) {
        self.update_summary(nrows)
    }

    fn fetch_raw_block(&mut self) -> Result<Option<RawBlock>, Self::Error> {
        self.raw.fetch_raw_block(self.fields())
    }
}

impl AsyncFetchable for ResultSet {
    type Error = RawError;

    fn affected_rows(&self) -> i32 {
        self.affected_rows()
    }

    fn precision(&self) -> Precision {
        self.precision()
    }

    fn fields(&self) -> &[Field] {
        self.fields()
    }

    fn summary(&self) -> (usize, usize) {
        *self.summary()
    }

    fn fetch_raw_block(
        self: &mut Self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<RawBlock>, Self::Error>> {
        self.fetch_raw_block_async(cx)
    }

    fn update_summary(&mut self, nrows: usize) {
        self.update_summary(nrows)
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        self.raw.drop();
    }
}

unsafe impl Send for ResultSet {}
unsafe impl Sync for ResultSet {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_databases() -> Result<(), Error> {
        let null = std::ptr::null();
        let taos = RawTaos::connect(null, null, null, null, 0)?;
        let mut set = taos.query("show databases")?;

        for raw in &mut set {
            let raw = raw?;
            for (col, view) in raw.columns().into_iter().enumerate() {
                for (row, value) in view.iter().enumerate() {
                    println!("Value at (row: {}, col: {}) is: {}", row, col, value);
                }
            }

            for (row, view) in raw.rows().enumerate() {
                for (col, value) in view.enumerate() {
                    println!("Value at (row: {}, col: {}) is: {:?}", row, col, value);
                }
            }
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[test]
    #[cfg(taos_parse_time)]
    fn test_parse_time() {
        use std::ffi::CString;
        let s = CString::new("1970-01-01 00:00:00").unwrap();
        let mut time = 0i64;
        unsafe {
            taos_options(
                TSDB_OPTION_TIMEZONE,
                b"Europe/Landon\0" as *const u8 as *const _,
            )
        };
        let res = unsafe {
            taos_parse_time(
                s.as_ptr(),
                &mut time as _,
                s.to_bytes().len() as _,
                Precision::Microsecond,
                0,
            )
        };
        assert_eq!(res, 0, "success");
        assert_eq!(time, 0, "parse time");

        let s = CString::new("1970-01-01 08:00:00").unwrap(); // CST +8
                                                              // timezone could be set multiple times
        unsafe {
            taos_options(
                TSDB_OPTION_TIMEZONE,
                b"Asia/Shanghai\0" as *const u8 as *const _,
            )
        };
        let res = unsafe {
            taos_parse_time(
                s.as_ptr(),
                &mut time as _,
                s.to_bytes().len() as _,
                Precision::Microsecond,
                0,
            )
        };
        assert_eq!(res, 0, "success");
        assert_eq!(time, 0, "parse time");
    }
}
