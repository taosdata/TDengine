use std::{ffi::CStr, os::raw::*};

use taos_error::{Code, Error};
use taos_query::common::raw_data_t;
use taos_query::RawBlock;

use crate::tmq::taos_write_raw_block;
use crate::{err_or, into_c_str::IntoCStr, query::QueryFuture};
use crate::{ffi::*, tmq::ffi::tmq_write_raw, RawRes, ResultSet};

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RawTaos(*mut TAOS);

unsafe impl Send for RawTaos {}
unsafe impl Sync for RawTaos {}

impl RawTaos {
    /// Client version.
    pub fn version() -> &'static str {
        unsafe {
            CStr::from_ptr(taos_get_client_info())
                .to_str()
                .expect("client version should always be valid utf-8 str")
        }
    }

    #[inline]
    pub fn connect(
        host: *const c_char,
        user: *const c_char,
        pass: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> Result<Self, Error> {
        let ptr = unsafe { taos_connect(host, user, pass, db, port) };
        let null = std::ptr::null_mut();
        let code = unsafe { taos_errno(null) };
        if code != 0 {
            let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
                .to_string_lossy()
                .to_string();
            let err = Error::new(code, err);
        }

        if ptr.is_null() {
            let null = std::ptr::null_mut();
            let code = unsafe { taos_errno(null) };
            let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
                .to_string_lossy()
                .to_string();
            log::trace!("error: {err}");

            Err(Error::new(code, err))
        } else {
            Ok(RawTaos(ptr))
        }
    }
    #[inline]
    pub fn connect_auth(
        host: *const c_char,
        user: *const c_char,
        auth: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> Result<Self, Error> {
        let ptr = unsafe { taos_connect_auth(host, user, auth, db, port) };
        if ptr.is_null() {
            let null = std::ptr::null_mut();
            let code = unsafe { taos_errno(null) };
            let err = unsafe { CStr::from_ptr(taos_errstr(null)) }
                .to_string_lossy()
                .to_string();
            Err(Error::new(code, err))
        } else {
            Ok(RawTaos(ptr))
        }
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS {
        self.0
    }

    #[inline]
    pub fn query<'a, S: IntoCStr<'a>>(&self, sql: S) -> Result<ResultSet, Error> {
        RawRes::from_ptr(unsafe { taos_query(self.as_ptr(), sql.into_c_str().as_ptr()) })
            .map(ResultSet::new)
    }

    #[inline]
    pub fn query_async<'a, S: IntoCStr<'a>>(&self, sql: S) -> QueryFuture<'a> {
        QueryFuture::new(self.clone(), sql)
    }

    #[inline]
    pub fn query_a<'a, S: IntoCStr<'a>>(
        &self,
        sql: S,
        fp: taos_async_query_cb,
        param: *mut c_void,
    ) {
        unsafe { taos_query_a(self.as_ptr(), sql.into_c_str().as_ptr(), fp, param) }
    }

    #[inline]
    pub fn validate_sql(self, sql: *const c_char) -> Result<(), Error> {
        let code: Code = unsafe { taos_validate_sql(self.as_ptr(), sql) }.into();
        if code.success() {
            return Ok(());
        } else {
            let err = unsafe { taos_errstr(std::ptr::null_mut()) };
            let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
            return Err(Error::new(code, err));
        }
    }

    #[inline]
    pub fn reset_current_db(&self) {
        unsafe { taos_reset_current_db(self.as_ptr()) }
    }

    #[inline]
    pub fn server_version(&self) -> &CStr {
        unsafe { CStr::from_ptr(taos_get_server_info(self.as_ptr())) }
    }

    #[inline]
    pub fn load_table_info(&self, list: *const c_char) -> Result<(), Error> {
        err_or!(taos_load_table_info(self.as_ptr(), list))
    }

    #[inline]
    pub fn write_raw_meta(&self, meta: raw_data_t) -> Result<(), Error> {
        err_or!(tmq_write_raw(self.as_ptr(), meta))
    }

    #[inline]
    pub fn write_raw_block(&self, block: &RawBlock) -> Result<(), Error> {
        let nrows = block.nrows();
        let name = block
            .table_name()
            .ok_or(Error::new(Code::Failed, "raw block should have table name"))?;
        let ptr = block.as_raw_bytes().as_ptr();
        err_or!(taos_write_raw_block(
            self.as_ptr(),
            nrows as _,
            ptr as _,
            name.into_c_str().as_ptr()
        ))
    }

    #[inline]
    pub fn close(&mut self) {
        unsafe { taos_close(self.as_ptr()) }
    }
}
