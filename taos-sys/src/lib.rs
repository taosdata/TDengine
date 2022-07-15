#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    ffi::CStr,
    future::Future,
    marker::PhantomData,
    ops::Deref,
    os::raw::*,
    process::Output,
    sync::{Arc, Mutex},
};

use into_c_str::IntoCStr;
use once_cell::sync::OnceCell;
use taos_error::{Code, Error};
use taos_query::common::{Field, Raw, Ty};

pub(crate) mod types;
pub use types::*;
pub mod ffi;
use ffi::*;

mod set_config;
pub use set_config::*;

pub use ffi::taos_options;

mod schemaless;
pub use schemaless::*;

mod tmq;
pub use tmq::*;

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
#[repr(transparent)]
pub struct RawTaos(*mut TAOS);

impl Drop for RawTaos {
    fn drop(&mut self) {
        self.close()
    }
}

impl RawTaos {
    /// Client version.
    pub fn version() -> &'static CStr {
        unsafe { CStr::from_ptr(taos_get_client_info()) }
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
    pub fn query<'a, S: IntoCStr<'a>>(&self, sql: S) -> Result<DroppableRawRes, Error> {
        RawRes::from_ptr(unsafe { taos_query(self.as_ptr(), sql.into_c_str().as_ptr()) })
            .map(DroppableRawRes::new)
    }

    #[inline]
    pub fn query_a(&self, sql: *const i8, fp: taos_async_query_cb, param: *mut c_void) {
        unsafe { taos_query_a(self.as_ptr(), sql, fp, param) }
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
    pub fn close(&mut self) {
        unsafe { taos_close(self.as_ptr()) }
    }
}

#[derive(Debug)]
pub struct RawRes {
    ptr: *mut TAOS_RES,
    msg_type: tmq_res_t,
    fields: OnceCell<Vec<Field>>,
}

unsafe impl Send for RawRes {}
unsafe impl Sync for RawRes {}

#[derive(Debug)]
pub struct DroppableRawRes {
    raw: Arc<RawRes>,
}

impl Deref for DroppableRawRes {
    type Target = RawRes;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl DroppableRawRes {
    pub fn new(raw: RawRes) -> Self {
        Self { raw: Arc::new(raw) }
    }

    pub fn from_ptr_with_code(ptr: *mut TAOS_RES, code: Code) -> Result<Self, Error> {
        RawRes::from_ptr_with_code(ptr, code).map(Self::new)
    }

    pub fn raw(&self) -> Arc<RawRes> {
        self.raw.clone()
    }
}

pub type VGroupId = i32;

impl Drop for DroppableRawRes {
    fn drop(&mut self) {
        if let Some(raw) = Arc::get_mut(&mut self.raw) {
            raw.free_result();
        } else {
            log::error!("there's other result pointer in-use, please check");
            // todo: safely drop result pointer.
            // panic!("there's other result pointer in-use, please check");
        }
    }
}

mod result_set;
pub use result_set::BlockStream;

impl RawRes {
    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS_RES {
        self.ptr
    }

    #[inline]
    pub fn errno(&self) -> Code {
        unsafe { taos_errno(self.as_ptr()) & 0xffff }.into()
    }
    #[inline]
    pub fn errstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(taos_errstr(self.as_ptr())) }
    }
    #[inline]
    pub fn err_as_str(&self) -> &'static str {
        unsafe {
            std::str::from_utf8_unchecked(CStr::from_ptr(taos_errstr(self.as_ptr())).to_bytes())
        }
    }

    #[inline]
    pub fn from_ptr(ptr: *mut TAOS_RES) -> Result<RawRes, Error> {
        let raw = unsafe { Self::from_ptr_unchecked(ptr) };
        let code = raw.errno();
        raw.with_code(code)
    }

    #[inline]
    pub unsafe fn from_ptr_unchecked(ptr: *mut TAOS_RES) -> RawRes {
        RawRes {
            ptr,
            msg_type: tmq_get_res_type(ptr),
            fields: OnceCell::new(),
        }
    }

    #[inline]
    pub fn from_ptr_with_code(ptr: *mut TAOS_RES, code: Code) -> Result<RawRes, Error> {
        unsafe { RawRes::from_ptr_unchecked(ptr) }.with_code(code)
    }

    #[inline]
    fn with_code(self, code: Code) -> Result<Self, Error> {
        if code.success() {
            Ok(self)
        } else {
            Err(Error::new(code, self.err_as_str()))
        }
    }

    #[inline]
    pub fn num_fields(&self) -> usize {
        self.fields().len()
    }
    #[inline]
    pub fn fields<'any>(&self) -> &[Field] {
        let fields = self.fields.get_or_init(|| {
            let len = unsafe { taos_field_count(self.as_ptr()) };
            from_raw_fields(unsafe { taos_fetch_fields(self.as_ptr()) }, len as usize)
        });
        &fields
    }

    pub fn fetch_fields(&self) -> Vec<Field> {
        let len = unsafe { taos_field_count(self.as_ptr()) };
        from_raw_fields(unsafe { taos_fetch_fields(self.as_ptr()) }, len as usize)
    }

    #[inline]
    pub fn fetch_lengths(&self) -> *const i32 {
        unsafe { taos_fetch_lengths(self.as_ptr()) }
    }
    #[inline]
    unsafe fn fetch_lengths_raw(&self) -> *const i32 {
        taos_fetch_lengths(self.as_ptr())
    }

    #[inline]
    pub fn fetch_block(&self) -> Result<Option<(TAOS_ROW, i32, *const i32)>, Error> {
        let block = Box::into_raw(Box::new(std::ptr::null_mut()));
        // let mut num = 0;
        let num = unsafe { taos_fetch_block(self.as_ptr(), block) };
        // taos_fetch_block(res, rows)
        if num > 0 {
            Ok(Some(unsafe { (*block, num, self.fetch_lengths_raw()) }))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn get_column_data_offset(&self, col: usize) -> *const i32 {
        unsafe { taos_get_column_data_offset(self.as_ptr(), col as i32) }
    }

    #[inline]
    pub fn fetch_raw_block(&self) -> Result<Option<Raw>, Error> {
        #[cfg(taos_v3)]
        return self.fetch_raw_block_v3();
        #[cfg(not(taos_v3))]
        self.fetch_raw_block_v2()
    }

    #[inline]
    pub fn fetch_raw_block_v3(&self) -> Result<Option<Raw>, Error> {
        let mut block: *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        err_or!(
            self,
            taos_fetch_raw_block(self.as_ptr(), &mut num as _, &mut block as _),
            if num > 0 {
                match self.msg_type {
                    tmq_res_t::TMQ_RES_INVALID => {
                        let mut raw = Raw::parse_from_ptr(
                            block as _,
                            num as usize,
                            self.num_fields(),
                            self.precision(),
                        );
                        raw.with_fields(self.fields().to_vec());
                        Some(raw)
                    }
                    tmq_res_t::TMQ_RES_DATA => {
                        let fields = self.fetch_fields();

                        let mut raw = Raw::parse_from_ptr(
                            block as _,
                            num as usize,
                            fields.len(),
                            self.precision(),
                        );

                        raw.with_fields(fields);

                        if let Some(name) = self.tmq_db_name() {
                            raw.with_database_name(name);
                        }

                        if let Some(name) = self.tmq_table_name() {
                            raw.with_table_name(name);
                        }

                        Some(raw)
                    }
                    _ => None,
                }
            } else {
                None
            }
        )
    }
    #[inline]
    pub fn fetch_raw_block_v2(&self) -> Result<Option<Raw>, Error> {
        let mut block: *mut *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        let lengths = self.fetch_lengths();
        let cols = self.num_fields();
        let lengths = unsafe { std::slice::from_raw_parts(lengths as *const u32, cols) };
        err_or!(
            self,
            taos_fetch_block_s(self.as_ptr(), &mut num as _, &mut block as _),
            if num > 0 {
                let raw = Raw::parse_from_ptr_v2(
                    block as _,
                    self.fields(),
                    lengths,
                    num as usize,
                    self.precision(),
                );
                Some(raw)
            } else {
                None
            }
        )
    }

    // #[inline]
    pub fn fetch_raw_block_async(&self) -> BlockStream {
        BlockStream::new(self.as_ptr(), self.fields(), self.precision())
    }

    #[inline]
    pub fn is_update_query(&self) -> bool {
        unsafe { taos_is_update_query(self.as_ptr()) }
    }

    #[inline]
    pub fn is_null(&self, row: i32, col: i32) -> bool {
        unsafe { taos_is_null(self.as_ptr(), row, col) }
    }

    #[inline]
    pub fn stop_query(&self) {
        unsafe { taos_stop_query(self.as_ptr()) }
    }

    #[inline]
    pub fn select_db(&self, db: *const i8) -> Result<(), Error> {
        err_or!(self, taos_select_db(self.as_ptr(), db))
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe { taos_affected_rows(self.as_ptr()) }
    }

    #[inline]
    pub fn field_count(&self) -> i32 {
        unsafe { taos_field_count(self.as_ptr()) }
    }

    #[inline]
    pub fn free_result(&mut self) {
        unsafe { taos_free_result(self.as_ptr()) }
    }

    #[inline]
    pub fn precision(&self) -> Precision {
        unsafe { taos_result_precision(self.as_ptr()) }.into()
    }

    #[inline]
    pub fn fetch_row(&self) -> TAOS_ROW {
        unsafe { taos_fetch_row(self.as_ptr()) }
    }

    #[inline]
    pub fn fetch_rows_a(&self, fp: taos_async_fetch_cb, param: *mut c_void) {
        unsafe { taos_fetch_rows_a(self.as_ptr(), fp, param) }
    }

    #[inline]
    pub fn block(&self) -> *mut *mut c_void {
        unsafe { taos_result_block(self.as_ptr()).read() }
    }

    #[inline]
    pub fn tmq_topic_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_topic_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    pub fn tmq_vgroup_id(&self) -> Option<VGroupId> {
        unsafe {
            let c = tmq_get_vgroup_id(self.as_ptr());
            if c == -1 {
                None
            } else {
                Some(c)
            }
        }
    }

    #[inline]
    pub fn tmq_table_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_table_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }
    #[inline]
    fn tmq_db_name(&self) -> Option<&str> {
        unsafe {
            let c = tmq_get_db_name(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }

    #[inline]
    fn message_type(&self) -> Result<MessageType, Error> {
        unsafe {
            let t = tmq_get_res_type(self.as_ptr());
            match t {
                tmq_res_t::TMQ_RES_INVALID => Err(Error::new(Code::Failed, "unknown message type")),
                tmq_res_t::TMQ_RES_DATA => Ok(MessageType::Schema),
                tmq_res_t::TMQ_RES_TABLE_META => Ok(MessageType::Data),
            }
        }
    }
}

pub enum MessageType {
    Schema,
    Data,
}
pub mod into_c_str;
pub mod stmt;
