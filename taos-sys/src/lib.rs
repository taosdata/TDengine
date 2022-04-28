#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{ffi::CStr, marker::PhantomData, ops::Deref, os::raw::*, sync::Arc};

use once_cell::sync::OnceCell;
use taos_error::{Code, Error};
use taos_query::common::{Field, Ty};

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
    ) -> Option<Self> {
        let ptr = unsafe { taos_connect(host, user, pass, db, port) };
        if ptr.is_null() {
            None
        } else {
            Some(RawTaos(ptr))
        }
    }
    #[inline]
    pub fn connect_auth(
        host: *const c_char,
        user: *const c_char,
        auth: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> Option<Self> {
        let ptr = unsafe { taos_connect_auth(host, user, auth, db, port) };
        if ptr.is_null() {
            None
        } else {
            Some(RawTaos(ptr))
        }
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS {
        self.0
    }

    #[inline]
    pub fn query(&self, sql: *const i8) -> Result<DroppableRawRes, Error> {
        RawRes::from_ptr(unsafe { taos_query(self.as_ptr(), sql) }).map(DroppableRawRes::new)
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
    fields: OnceCell<Vec<Field>>,
}

unsafe impl Send for RawRes {}
unsafe impl Sync for RawRes {}

#[derive(Debug)]
pub struct DroppableRawRes<'q> {
    raw: Arc<RawRes>,
    _marker: PhantomData<&'q u8>,
}

impl<'q> Deref for DroppableRawRes<'q> {
    type Target = RawRes;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl<'q> DroppableRawRes<'q> {
    pub fn new(raw: RawRes) -> Self {
        Self {
            raw: Arc::new(raw),
            _marker: PhantomData,
        }
    }

    pub fn from_ptr_with_code(ptr: *mut TAOS_RES, code: Code) -> Result<Self, Error> {
        RawRes::from_ptr_with_code(ptr, code).map(Self::new)
    }

    pub fn raw(&self) -> Arc<RawRes> {
        self.raw.clone()
    }
}

impl<'q> Drop for DroppableRawRes<'q> {
    fn drop(&mut self) {
        if let Some(raw) = Arc::get_mut(&mut self.raw) {
            raw.free_result();
        } else {
            log::error!("there's other result pointer in-use, please check");
            panic!("there's other result pointer in-use, please check");
        }
    }
}

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
    pub const unsafe fn from_ptr_unchecked(ptr: *mut TAOS_RES) -> RawRes {
        RawRes {
            ptr,
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
            let len = unsafe { taos_num_fields(self.as_ptr()) };
            from_raw_fields(unsafe { taos_fetch_fields(self.as_ptr()) }, len as usize)
        });
        &fields
    }

    #[inline]
    pub fn fetch_lengths(&self) -> *const i32 {
        unsafe {
            taos_fetch_lengths(self.as_ptr())
        }
    }
    #[inline]
    unsafe fn fetch_lengths_raw(&self) -> *const i32 {
        dbg!("call fetch  lengths");
        dbg!(taos_fetch_lengths(self.as_ptr()))
    }

    #[inline]
    pub fn fetch_block(&self) -> Result<Option<(TAOS_ROW, i32, *const i32)>, Error> {
        let block = Box::into_raw(Box::new(std::ptr::null_mut()));
        let mut num = 0;
        err_or!(
            self,
            taos_fetch_block_s(self.as_ptr(), &mut num, block),
            if num > 0 {
                Some((*block, num, self.fetch_lengths_raw()))
            } else {
                None
            }
        )
    }

    #[inline]
    pub fn get_column_data_offset(&self, col: usize) -> *const i32 {
        unsafe { taos_get_column_data_offset(self.as_ptr(), col as i32) }
    }

    #[inline]
    pub fn fetch_raw_block(&self) -> Result<(*mut c_void, i32), Error> {
        let block = Box::into_raw(Box::new(std::ptr::null_mut()));
        let mut num = 0;
        err_or!(
            self,
            taos_fetch_raw_block(self.as_ptr(), &mut num as _, block),
            (*block, num as _)
        )
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
}

#[derive(Debug)]
pub struct RawStmt(*mut TAOS_STMT);

impl Drop for RawStmt {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl RawStmt {
    #[inline]
    pub unsafe fn as_ptr(&self) -> *mut TAOS_STMT {
        self.0
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
    pub fn from_raw_taos(taos: &RawTaos) -> RawStmt {
        RawStmt(unsafe { taos_stmt_init(taos.as_ptr()) })
    }
    #[inline]
    pub fn close(&mut self) -> Result<(), Error> {
        err_or!(self, taos_stmt_close(self.as_ptr()))
    }

    #[inline]
    pub fn prepare(&mut self, sql: *const c_char, length: c_ulong) -> Result<(), Error> {
        err_or!(self, taos_stmt_prepare(self.as_ptr(), sql, length))
    }

    #[inline]
    pub fn set_tbname_tags(
        &mut self,
        name: *const c_char,
        tags: *mut TAOS_BIND,
    ) -> Result<(), Error> {
        err_or!(self, taos_stmt_set_tbname_tags(self.as_ptr(), name, tags))
    }

    #[inline]
    pub fn set_tbname(&mut self, name: *const c_char) -> Result<(), Error> {
        err_or!(self, taos_stmt_set_tbname(self.as_ptr(), name))
    }

    #[inline]
    pub fn set_sub_tbname(&mut self, name: *const c_char) -> Result<(), Error> {
        err_or!(self, taos_stmt_set_sub_tbname(self.as_ptr(), name))
    }

    #[inline]
    pub fn use_result(&mut self) -> RawRes {
        unsafe { RawRes::from_ptr_unchecked(taos_stmt_use_result(self.as_ptr())) }
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe { taos_stmt_affected_rows(self.as_ptr()) }
    }

    #[inline]
    pub fn execute(&self) -> Result<(), Error> {
        err_or!(self, taos_stmt_execute(self.as_ptr()))
    }

    #[inline]
    pub fn add_batch(&self) -> Result<(), Error> {
        err_or!(self, taos_stmt_add_batch(self.as_ptr()))
    }

    #[inline]
    pub fn is_insert(&self) -> Result<bool, Error> {
        let mut is_insert = 0;
        err_or!(
            self,
            taos_stmt_is_insert(self.as_ptr(), &mut is_insert as _),
            is_insert != 0
        )
    }

    #[inline]
    pub fn num_params(&self) -> Result<i32, Error> {
        let mut num = 0;
        err_or!(
            self,
            taos_stmt_num_params(self.as_ptr(), &mut num as _),
            num
        )
    }

    #[inline]
    pub fn get_param(&mut self, idx: i32) -> Result<(Ty, i32), Error> {
        let (mut type_, mut bytes) = (0, 0);
        err_or!(
            self,
            taos_stmt_get_param(self.as_ptr(), idx, &mut type_ as _, &mut bytes as _),
            ((type_ as u8).into(), bytes)
        )
    }
    #[inline]
    pub fn bind_param(&mut self, bind: *mut TAOS_BIND) -> Result<(), Error> {
        err_or!(self, taos_stmt_bind_param(self.as_ptr(), bind))
    }

    #[inline]
    pub fn bind_param_batch(&mut self, bind: *mut TAOS_MULTI_BIND) -> Result<(), Error> {
        err_or!(self, taos_stmt_bind_param_batch(self.as_ptr(), bind))
    }

    #[inline]
    pub fn bind_single_param_batch(
        &self,
        bind: *mut TAOS_MULTI_BIND,
        col: i32,
    ) -> Result<(), Error> {
        err_or!(
            self,
            taos_stmt_bind_single_param_batch(self.as_ptr(), bind, col)
        )
    }
}

// #[derive(Debug, Clone, Copy)]
// #[repr(C)]
// enum BlockVer {
//     V2 = 0,
//     V3,
// }
// #[derive(Debug, Clone, Copy)]
// #[repr(C)]

// enum BlockCodec {
//     Bytes,
// }

// #[derive(Debug)]
// enum BlockType<'a> {
//     V2(*mut *mut c_void),
//     Bytes(Cow<'a, [u8]>),
// }

// struct RawCodec<'a> {
//     version: BlockVer,
//     method: BlockCodec,
//     precision: Precision,
//     fields: Cow<'a, [Field]>,
// }

// #[derive(Debug)]
// pub struct RawBlock<'a> {
//     version: BlockVer,
//     codec: BlockCodec,
//     precision: Precision,
//     fields: Cow<'a, [Field]>,
//     num_of_rows: usize,
//     data: BlockType<'a>,
// }

// impl<'a> RawBlock<'a> {
//     fn precision(&self) -> Precision {
//         self.precision
//     }

//     fn to_bytes(&self) -> Cow<[u8]> {
//         todo!()
//     }

//     fn write<T: Write>(&self, mut wtr: T) -> std::io::Result<usize> {
//         wtr.write(&self.to_bytes())
//     }

//     fn write_all<T: Write>(&self, mut wtr: T) -> std::io::Result<usize> {
//         wtr.write(&self.to_bytes())
//     }
// }
