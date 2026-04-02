use std::cell::RefCell;
use std::ffi::{c_void, CStr, CString};
use std::fmt::{Debug, Display};
use std::os::raw::{c_char, c_int};
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use taos_error::Code;
use taos_query::common::{
    Field, Precision, RawBlock as Block, SchemalessPrecision, SchemalessProtocol, SmlDataBuilder,
    SmlDataBuilderError, Timestamp, Ty,
};
use taos_query::prelude::RawError;
use taos_query::util::{hex, InlineBytes, InlineNChar, InlineStr};
use taos_query::{DsnError, Fetchable, Queryable, TBuilder};
use taos_ws::consumer::Offset;
pub use taos_ws::query::asyn::WS_ERROR_NO;
use taos_ws::query::{Error, ResultSet, Taos};
use taos_ws::TaosBuilder;

pub mod stmt;
pub mod tmq;

use tmq::WsTmqResultSet;

const MAX_ERROR_MSG_LEN: usize = 4096;
const EMPTY: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"\0") };
thread_local! {
    static C_ERROR_CONTAINER: RefCell<[u8; MAX_ERROR_MSG_LEN]> = const { RefCell::new([0; MAX_ERROR_MSG_LEN]) };
    static C_ERRNO: RefCell<i32> = const { RefCell::new(0) };
}

fn get_err_code_fromated(err_code: i32) -> i32 {
    if err_code >= 0 {
        let uerror_code: u32 = err_code as _;
        return (uerror_code | 0x80000000) as i32;
    }
    err_code
}

unsafe fn set_error_and_get_code(ws_err: WsError) -> i32 {
    C_ERRNO.with(|c_errno| {
        let err_num: u32 = ws_err.code.into();
        *c_errno.borrow_mut() = (err_num | 0x80000000) as i32;
    });
    C_ERROR_CONTAINER.with(|container| {
        let mut container_ref = container.borrow_mut();
        let bytes = ws_err.message.as_bytes();
        let length = bytes.len().min(MAX_ERROR_MSG_LEN - 1);
        container_ref[..length].copy_from_slice(&bytes[..length]);
        container_ref[length] = 0;
    });
    get_c_errno()
}

fn get_c_errno() -> i32 {
    C_ERRNO.with(|c_errno| *c_errno.borrow())
}

fn get_c_error_str() -> *const c_char {
    C_ERROR_CONTAINER.with(|container| {
        let container = container.borrow();
        let slice = &container[..container.len()];
        slice.as_ptr() as _
    })
}

#[allow(unused)]
unsafe fn clear_error_info() {
    C_ERRNO.with(|c_errno| {
        *c_errno.borrow_mut() = 0;
    });
    C_ERROR_CONTAINER.with(|container| {
        let bytes = b"";
        let length = bytes.len().min(MAX_ERROR_MSG_LEN - 1); // make sure the size max, and keep last byte for '\0'
        container.borrow_mut()[..length].copy_from_slice(&bytes[..length]);
        container.borrow_mut()[length] = 0; // add c str last '\0'
    });
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum WS_TSDB_SML_PROTOCOL_TYPE {
    WS_TSDB_SML_UNKNOWN_PROTOCOL = 0,
    WS_TSDB_SML_LINE_PROTOCOL = 1,
    WS_TSDB_SML_TELNET_PROTOCOL = 2,
    WS_TSDB_SML_JSON_PROTOCOL = 3,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum WS_TSDB_SML_TIMESTAMP_TYPE {
    WS_TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
    WS_TSDB_SML_TIMESTAMP_HOURS,
    WS_TSDB_SML_TIMESTAMP_MINUTES,
    WS_TSDB_SML_TIMESTAMP_SECONDS,
    WS_TSDB_SML_TIMESTAMP_MILLI_SECONDS,
    WS_TSDB_SML_TIMESTAMP_MICRO_SECONDS,
    WS_TSDB_SML_TIMESTAMP_NANO_SECONDS,
}

/// Opaque type definition for websocket connection.
#[allow(non_camel_case_types)]
pub type WS_TAOS = c_void;

/// Opaque type definition for websocket result set.
#[allow(non_camel_case_types)]
pub type WS_RES = c_void;

/// Opaque type definition for websocket row.
#[allow(non_camel_case_types)]
pub type WS_ROW = *const *const c_void;

#[derive(Debug)]
pub struct WsError {
    code: Code,
    message: CString,
    source: Option<Box<dyn std::error::Error + 'static>>,
}

impl WsError {
    fn new(code: Code, message: &str) -> Self {
        Self {
            code,
            message: CString::new(message).unwrap(),
            source: None,
        }
    }

    pub fn from_err(err: Box<dyn std::error::Error + 'static>) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(err.to_string()).unwrap(),
            source: Some(err),
        }
    }
}

#[derive(Debug)]
pub struct WsMaybeError<T> {
    error: Option<WsError>,
    data: *mut T,
    type_id: &'static str,
}

impl<T> Drop for WsMaybeError<T> {
    fn drop(&mut self) {
        if !self.data.is_null() {
            tracing::trace!("dropping obj {}", self.type_id);
            let _ = unsafe { Box::from_raw(self.data) };
        }
    }
}

impl<T> WsMaybeError<T> {
    pub fn errno(&self) -> Option<i32> {
        self.error.as_ref().map(|s| s.code.into())
    }

    pub fn errstr(&self) -> Option<*const c_char> {
        self.error.as_ref().map(|s| s.message.as_ptr())
    }

    pub fn safe_deref(&self) -> Option<&T> {
        unsafe { self.data.as_ref() }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn safe_deref_mut(&self) -> Option<&mut T> {
        unsafe { self.data.as_mut() }
    }
}

impl<T: 'static> From<T> for WsMaybeError<T> {
    fn from(value: T) -> Self {
        Self {
            error: None,
            data: Box::into_raw(Box::new(value)),
            type_id: std::any::type_name::<T>(),
        }
    }
}

impl<T: 'static> From<Box<T>> for WsMaybeError<T> {
    fn from(value: Box<T>) -> Self {
        Self {
            error: None,
            data: Box::into_raw(value),
            type_id: std::any::type_name::<T>(),
        }
    }
}

impl<T: 'static, E> From<Result<T, E>> for WsMaybeError<T>
where
    E: Into<WsError>,
{
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(value) => Self {
                error: None,
                data: Box::into_raw(Box::new(value)),
                type_id: std::any::type_name::<T>(),
            },
            Err(err) => Self {
                error: Some(err.into()),
                data: std::ptr::null_mut(),
                type_id: std::any::type_name::<T>(),
            },
        }
    }
}

impl<T: 'static, E> From<Result<Box<T>, E>> for WsMaybeError<T>
where
    E: Into<WsError>,
{
    fn from(value: Result<Box<T>, E>) -> Self {
        match value {
            Ok(value) => Self {
                error: None,
                data: Box::into_raw(value),
                type_id: std::any::type_name::<T>(),
            },
            Err(err) => Self {
                error: Some(err.into()),
                data: std::ptr::null_mut(),
                type_id: std::any::type_name::<T>(),
            },
        }
    }
}

pub type WsResult<T> = Result<T, WsError>;

impl Display for WsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:#06X}] {}", self.code, self.message.to_str().unwrap())
    }
}

impl std::error::Error for WsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref())
    }
}

impl From<Utf8Error> for WsError {
    fn from(e: Utf8Error) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(format!("{e}")).unwrap(),
            source: Some(Box::new(e)),
        }
    }
}

impl From<FromUtf8Error> for WsError {
    fn from(e: FromUtf8Error) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(format!("{e}")).unwrap(),
            source: Some(Box::new(e)),
        }
    }
}

impl From<SmlDataBuilderError> for WsError {
    fn from(e: SmlDataBuilderError) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(format!("{e}")).unwrap(),
            source: Some(Box::new(e)),
        }
    }
}

impl From<Error> for WsError {
    fn from(e: Error) -> Self {
        Self {
            code: e.errno(),
            message: CString::new(e.errstr()).unwrap(),
            source: None,
        }
    }
}

impl From<&WsError> for WsError {
    fn from(e: &WsError) -> Self {
        Self {
            code: e.code,
            message: e.message.clone(),
            source: None,
        }
    }
}

impl From<taos_ws::Error> for WsError {
    fn from(e: taos_ws::Error) -> Self {
        Self {
            code: e.errno(),
            message: CString::new(e.errstr()).unwrap(),
            source: None,
        }
    }
}

impl From<RawError> for WsError {
    fn from(e: RawError) -> Self {
        Self {
            code: e.code(),
            message: CString::new(e.to_string()).unwrap(),
            source: None,
        }
    }
}

impl From<DsnError> for WsError {
    fn from(e: DsnError) -> Self {
        Self {
            code: WS_ERROR_NO::DSN_ERROR.as_code(),
            message: CString::new(e.to_string()).unwrap(),
            source: None,
        }
    }
}

type WsTaos = Result<Taos, WsError>;

/// Only useful for developers who use along with TDengine 2.x `TAOS_FIELD` struct.
/// It means that the struct has the same memory layout with the `TAOS_FIELD` struct
/// in taos.h of TDengine 2.x
#[repr(C)]
#[derive(Copy, Clone, Debug)]
#[allow(non_camel_case_types)]
pub struct WS_FIELD_V2 {
    pub name: [c_char; 65usize],
    pub r#type: u8,
    pub bytes: u16,
}

impl WS_FIELD_V2 {
    pub fn name(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.name.as_ptr() as _) }
    }

    pub fn r#type(&self) -> Ty {
        self.r#type.into()
    }

    pub fn bytes(&self) -> u32 {
        self.bytes as _
    }
}

impl From<&Field> for WS_FIELD_V2 {
    fn from(field: &Field) -> Self {
        let f_name = field.name();
        let mut name = [0 as c_char; 65usize];
        unsafe {
            std::ptr::copy_nonoverlapping(f_name.as_ptr(), name.as_mut_ptr() as _, f_name.len());
        };
        Self {
            name,
            r#type: field.ty() as u8,
            bytes: field.bytes() as _,
        }
    }
}

/// Field struct that has v3-compatible memory layout, which is recommended.
#[repr(C)]
#[derive(Copy, Clone)]
#[allow(non_camel_case_types)]
pub struct WS_FIELD {
    pub name: [c_char; 65usize],
    pub r#type: u8,
    pub bytes: u32,
}

impl WS_FIELD {
    pub fn name(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.name.as_ptr() as _) }
    }

    pub fn r#type(&self) -> Ty {
        self.r#type.into()
    }

    pub fn bytes(&self) -> u32 {
        self.bytes as _
    }
}

impl Debug for WS_FIELD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WS_FIELD")
            .field("name", &self.name())
            .field("type", &self.r#type)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl From<&Field> for WS_FIELD {
    fn from(field: &Field) -> Self {
        let f_name = field.name();
        let mut name = [0 as c_char; 65usize];
        unsafe {
            std::ptr::copy_nonoverlapping(f_name.as_ptr(), name.as_mut_ptr() as _, f_name.len());
        };
        Self {
            name,
            r#type: field.ty() as u8,
            bytes: field.bytes(),
        }
    }
}

#[derive(Debug)]
pub struct ROW {
    /// Data is used to hold the row data
    data: Vec<*const c_void>,
    /// Current row to get
    current_row: usize,
}

#[derive(Debug)]
pub struct WsSchemalessResultSet {
    affected_rows: i64,
    precision: Precision,
    timing: Duration,
}

impl WsSchemalessResultSet {
    fn new(affected_rows: i64, precision: Precision, timing: Duration) -> Self {
        Self {
            affected_rows,
            precision,
            timing,
        }
    }
}

#[derive(Debug)]
struct WsSqlResultSet {
    rs: ResultSet,
    block: Option<Block>,
    fields: Vec<WS_FIELD>,
    fields_v2: Vec<WS_FIELD_V2>,
    row: ROW,
}

#[derive(Debug)]
enum WsResultSet {
    Sql(WsSqlResultSet),
    Schemaless(WsSchemalessResultSet),
    Tmq(WsTmqResultSet),
}

trait WsResultSetTrait {
    fn tmq_get_topic_name(&self) -> *const c_char;
    fn tmq_get_db_name(&self) -> *const c_char;
    fn tmq_get_table_name(&self) -> *const c_char;
    fn tmq_get_vgroup_offset(&self) -> i64;
    fn tmq_get_vgroup_id(&self) -> i32;
    fn tmq_get_offset(&self) -> Offset;

    fn precision(&self) -> Precision;

    fn affected_rows(&self) -> i32;

    fn affected_rows64(&self) -> i64;

    fn num_of_fields(&self) -> i32;

    fn get_fields(&mut self) -> *const WS_FIELD;
    fn get_fields_v2(&mut self) -> *const WS_FIELD_V2;

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error>;

    unsafe fn fetch_row(&mut self) -> Result<WS_ROW, Error>;

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void);

    fn take_timing(&mut self) -> Duration;

    fn stop_query(&mut self);
}

impl WsResultSetTrait for WsSchemalessResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        std::ptr::null()
    }
    fn tmq_get_db_name(&self) -> *const c_char {
        std::ptr::null()
    }
    fn tmq_get_table_name(&self) -> *const c_char {
        std::ptr::null()
    }
    fn tmq_get_vgroup_offset(&self) -> i64 {
        0
    }
    fn tmq_get_vgroup_id(&self) -> i32 {
        0
    }
    fn tmq_get_offset(&self) -> Offset {
        todo!()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    fn affected_rows(&self) -> i32 {
        self.affected_rows as _
    }

    fn affected_rows64(&self) -> i64 {
        self.affected_rows
    }

    fn num_of_fields(&self) -> i32 {
        0
    }

    fn get_fields(&mut self) -> *const WS_FIELD {
        std::ptr::null()
    }
    fn get_fields_v2(&mut self) -> *const WS_FIELD_V2 {
        std::ptr::null()
    }

    unsafe fn fetch_block(
        &mut self,
        _ptr: *mut *const c_void,
        _rows: *mut i32,
    ) -> Result<(), Error> {
        Err(Error::CommonError(
            "schemaless result set does not support fetch block".to_string(),
        ))
    }

    unsafe fn fetch_row(&mut self) -> Result<WS_ROW, Error> {
        Err(Error::CommonError(
            "schemaless result set does not support fetch row".to_string(),
        ))
    }

    unsafe fn get_raw_value(&mut self, _row: usize, _col: usize) -> (Ty, u32, *const c_void) {
        (Ty::Null, 0, std::ptr::null())
    }

    fn take_timing(&mut self) -> Duration {
        self.timing
    }

    fn stop_query(&mut self) {}
}

impl WsResultSetTrait for WsResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_topic_name(),
            WsResultSet::Tmq(rs) => rs.tmq_get_topic_name(),
            WsResultSet::Schemaless(rs) => rs.tmq_get_topic_name(),
        }
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_db_name(),
            WsResultSet::Tmq(rs) => rs.tmq_get_db_name(),
            WsResultSet::Schemaless(rs) => rs.tmq_get_db_name(),
        }
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_table_name(),
            WsResultSet::Tmq(rs) => rs.tmq_get_table_name(),
            WsResultSet::Schemaless(rs) => rs.tmq_get_table_name(),
        }
    }

    fn tmq_get_offset(&self) -> Offset {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_offset(),
            WsResultSet::Tmq(rs) => rs.tmq_get_offset(),
            WsResultSet::Schemaless(rs) => rs.tmq_get_offset(),
        }
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_vgroup_offset(),
            WsResultSet::Tmq(rs) => rs.tmq_get_vgroup_offset(),
            WsResultSet::Schemaless(rs) => rs.tmq_get_vgroup_offset(),
        }
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        match self {
            WsResultSet::Sql(rs) => rs.tmq_get_vgroup_id(),
            WsResultSet::Tmq(rs) => rs.tmq_get_vgroup_id(),
            WsResultSet::Schemaless(rs) => rs.tmq_get_vgroup_id(),
        }
    }

    fn precision(&self) -> Precision {
        match self {
            WsResultSet::Sql(rs) => rs.precision(),
            WsResultSet::Tmq(rs) => rs.precision(),
            WsResultSet::Schemaless(rs) => rs.precision(),
        }
    }

    fn affected_rows(&self) -> i32 {
        match self {
            WsResultSet::Sql(rs) => rs.affected_rows(),
            WsResultSet::Tmq(rs) => rs.affected_rows(),
            WsResultSet::Schemaless(rs) => rs.affected_rows(),
        }
    }

    fn affected_rows64(&self) -> i64 {
        match self {
            WsResultSet::Sql(rs) => rs.affected_rows64(),
            WsResultSet::Tmq(rs) => rs.affected_rows64(),
            WsResultSet::Schemaless(rs) => rs.affected_rows64(),
        }
    }

    fn num_of_fields(&self) -> i32 {
        match self {
            WsResultSet::Sql(rs) => rs.num_of_fields(),
            WsResultSet::Tmq(rs) => rs.num_of_fields(),
            WsResultSet::Schemaless(rs) => rs.num_of_fields(),
        }
    }

    fn get_fields(&mut self) -> *const WS_FIELD {
        match self {
            WsResultSet::Sql(rs) => rs.get_fields(),
            WsResultSet::Tmq(rs) => rs.get_fields(),
            WsResultSet::Schemaless(rs) => rs.get_fields(),
        }
    }

    fn get_fields_v2(&mut self) -> *const WS_FIELD_V2 {
        match self {
            WsResultSet::Sql(rs) => rs.get_fields_v2(),
            WsResultSet::Tmq(rs) => rs.get_fields_v2(),
            WsResultSet::Schemaless(rs) => rs.get_fields_v2(),
        }
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error> {
        match self {
            WsResultSet::Sql(rs) => rs.fetch_block(ptr, rows),
            WsResultSet::Tmq(rs) => rs.fetch_block(ptr, rows),
            WsResultSet::Schemaless(rs) => rs.fetch_block(ptr, rows),
        }
    }

    unsafe fn fetch_row(&mut self) -> Result<WS_ROW, Error> {
        match self {
            WsResultSet::Sql(rs) => rs.fetch_row(),
            WsResultSet::Tmq(rs) => rs.fetch_row(),
            WsResultSet::Schemaless(rs) => rs.fetch_row(),
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        match self {
            WsResultSet::Sql(rs) => rs.get_raw_value(row, col),
            WsResultSet::Tmq(rs) => rs.get_raw_value(row, col),
            WsResultSet::Schemaless(rs) => rs.get_raw_value(row, col),
        }
    }

    fn take_timing(&mut self) -> Duration {
        match self {
            WsResultSet::Sql(rs) => rs.take_timing(),
            WsResultSet::Tmq(rs) => rs.take_timing(),
            WsResultSet::Schemaless(rs) => rs.take_timing(),
        }
    }

    fn stop_query(&mut self) {
        match self {
            WsResultSet::Sql(rs) => rs.stop_query(),
            WsResultSet::Tmq(rs) => rs.stop_query(),
            WsResultSet::Schemaless(rs) => rs.stop_query(),
        }
    }
}

impl WsSqlResultSet {
    fn new(rs: ResultSet) -> Self {
        let num_of_fields = rs.num_of_fields();
        let mut data_vec = Vec::with_capacity(num_of_fields);
        for _col in 0..num_of_fields {
            data_vec.push(std::ptr::null());
        }

        Self {
            rs,
            block: None,
            fields: Vec::new(),
            fields_v2: Vec::new(),
            row: ROW {
                data: data_vec,
                current_row: 0,
            },
        }
    }
}

impl WsResultSetTrait for WsSqlResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        std::ptr::null()
    }
    fn tmq_get_db_name(&self) -> *const c_char {
        std::ptr::null()
    }
    fn tmq_get_table_name(&self) -> *const c_char {
        std::ptr::null()
    }
    fn tmq_get_offset(&self) -> Offset {
        todo!()
    }
    fn tmq_get_vgroup_offset(&self) -> i64 {
        0
    }
    fn tmq_get_vgroup_id(&self) -> i32 {
        0
    }

    fn precision(&self) -> Precision {
        self.rs.precision()
    }

    fn affected_rows(&self) -> i32 {
        self.rs.affected_rows() as _
    }

    fn affected_rows64(&self) -> i64 {
        self.rs.affected_rows64() as _
    }

    fn num_of_fields(&self) -> i32 {
        self.rs.num_of_fields() as _
    }

    fn get_fields(&mut self) -> *const WS_FIELD {
        if self.fields.len() != self.rs.num_of_fields() {
            self.fields.clear();
            self.fields
                .extend(self.rs.fields().iter().map(WS_FIELD::from));
        }
        self.fields.as_ptr()
    }

    fn get_fields_v2(&mut self) -> *const WS_FIELD_V2 {
        if self.fields_v2.len() != self.rs.num_of_fields() {
            self.fields_v2.clear();
            self.fields_v2
                .extend(self.rs.fields().iter().map(WS_FIELD_V2::from));
        }
        self.fields_v2.as_ptr()
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error> {
        tracing::trace!("fetch block with ptr {ptr:p}");
        self.block = self.rs.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        tracing::trace!("fetch block with ptr {ptr:p} with rows {}", *rows);
        Ok(())
    }

    unsafe fn fetch_row(&mut self) -> Result<WS_ROW, Error> {
        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.rs.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        if let Some(block) = self.block.as_ref() {
            if block.nrows() == 0 {
                return Ok(std::ptr::null());
            }

            for col in 0..block.ncols() {
                let tuple = block.get_raw_value_unchecked(self.row.current_row, col);
                self.row.data[col] = tuple.2;
            }

            self.row.current_row += 1;
            Ok(self.row.data.as_ptr() as _)
        } else {
            Ok(std::ptr::null())
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        tracing::trace!("try to get raw value at ({row}, {col})");
        match self.block.as_ref() {
            Some(block) => {
                if row < block.nrows() && col < block.ncols() {
                    let res = block.get_raw_value_unchecked(row, col);
                    tracing::trace!("got raw value at ({row}, {col}): {:?}", res);
                    res
                } else {
                    tracing::trace!("out of range at ({row}, {col}), return null");
                    (Ty::Null, 0, std::ptr::null())
                }
            }
            None => (Ty::Null, 0, std::ptr::null()),
        }
    }

    fn take_timing(&mut self) -> Duration {
        self.rs.take_timing()
    }

    fn stop_query(&mut self) {
        taos_query::block_in_place_or_global(self.rs.stop());
    }
}

unsafe fn connect_with_dsn(dsn: *const c_char) -> WsTaos {
    let dsn = if dsn.is_null() {
        CStr::from_bytes_with_nul(b"taos://localhost:6041\0").unwrap()
    } else {
        CStr::from_ptr(dsn)
    };

    let dsn = dsn.to_str()?;
    let builder = TaosBuilder::from_dsn(dsn)?;

    if cfg!(all(windows, target_pointer_width = "32")) {
        tracing::debug!(
            "Connecting with dsn: {dsn}, using thread to avoid stack overflow on 32-bit Windows"
        );

        let stack_size = 4 * 1024 * 1024;

        let handle = thread::Builder::new()
            .stack_size(stack_size)
            .spawn(move || {
                let result = builder.build();
                match result {
                    Ok(mut taos) => {
                        builder.ping(&mut taos)?;
                        Ok(taos)
                    }
                    Err(e) => Err(e),
                }
            })
            .expect("Failed to spawn thread");

        let result = handle.join().expect("Thread panicked");
        match result {
            Ok(taos) => Ok(taos),
            Err(e) => Err(WsError::new(Code::FAILED, &format!("{e}"))),
        }
    } else {
        let mut taos = builder.build()?;
        builder.ping(&mut taos)?;
        Ok(taos)
    }
}

/// Enable inner log to stdout with para log_level.
///
/// # Example
///
/// ```c
/// ws_enable_log("debug");
/// ```
#[no_mangle]
pub unsafe extern "C" fn ws_enable_log(log_level: *const c_char) -> i32 {
    static ONCE_INIT: std::sync::Once = std::sync::Once::new();

    let log_level = if log_level.is_null() {
        "info"
    } else if let Ok(log_level_str) = CStr::from_ptr(log_level).to_str() {
        log_level_str
    } else {
        return set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "log_level is not a valid string",
        ));
    };

    ONCE_INIT.call_once(|| {
        let mut builder = pretty_env_logger::formatted_timed_builder();
        builder.format_timestamp_nanos();
        builder.parse_filters(log_level);
        let _ = builder.try_init();
    });

    Code::SUCCESS.into()
}

#[no_mangle]
pub extern "C" fn ws_data_type(r#type: i32) -> *const c_char {
    match Ty::from_u8_option(r#type as _) {
        Some(ty) => ty.tsdb_name(),
        None => std::ptr::null(),
    }
}

/// Connect via dsn string, returns NULL if failed.
///
/// Remember to check the return pointer is null and get error details.
///
/// # Example
///
/// ```c
/// char* dsn = "taos://localhost:6041";
/// WS_TAOS* taos = ws_connect(dsn);
/// if (taos == NULL) {
///   int errno = ws_errno(NULL);
///   char* errstr = ws_errstr(NULL);
///   printf("Connection failed[%d]: %s", errno, errstr);
///   exit(-1);
/// }
/// ```
#[no_mangle]
pub unsafe extern "C" fn ws_connect(dsn: *const c_char) -> *mut WS_TAOS {
    match connect_with_dsn(dsn) {
        Ok(client) => Box::into_raw(Box::new(client)) as _,
        Err(err) => {
            set_error_and_get_code(err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
#[allow(static_mut_refs)]
/// Same to taos_get_server_info, returns server version info.
pub unsafe extern "C" fn ws_get_server_info(taos: *mut WS_TAOS) -> *const c_char {
    static VERSION_INFO: OnceLock<CString> = OnceLock::new();

    if taos.is_null() {
        set_error_and_get_code(WsError::new(Code::INVALID_PARA, "taos is null"));
        return std::ptr::null();
    }

    let version_info = VERSION_INFO.get_or_init(|| {
        if let Some(taos) = (taos as *mut Taos).as_mut() {
            CString::new(taos.version().as_bytes()).unwrap()
        } else {
            CString::new("").unwrap()
        }
    });

    version_info.as_ptr()
}

#[no_mangle]
/// Same to taos_close. This should always be called after everything done with the connection.
pub unsafe extern "C" fn ws_close(taos: *mut WS_TAOS) -> i32 {
    if !taos.is_null() {
        tracing::trace!("close connection {taos:p}");
        let client = Box::from_raw(taos as *mut Taos);
        // client.close();
        drop(client);
        return Code::SUCCESS.into();
    }
    set_error_and_get_code(WsError::new(Code::INVALID_PARA, "taos is null"))
}

unsafe fn get_req_id(taos: *const WS_TAOS) -> u64 {
    let reqid = match (taos as *const Taos).as_ref() {
        Some(client) => client.get_req_id(),
        None => return 0,
    };
    reqid
}

unsafe fn query_with_sql(
    taos: *mut WS_TAOS,
    sql: *const c_char,
    req_id: u64,
) -> WsResult<WsResultSet> {
    let client = (taos as *mut Taos)
        .as_mut()
        .ok_or(WsError::new(Code::INVALID_PARA, "client pointer is null"))?;

    let sql = CStr::from_ptr(sql).to_str()?;
    let rs = client.query_with_req_id(sql, req_id)?;
    Ok(WsResultSet::Sql(WsSqlResultSet::new(rs)))
}

unsafe fn query_with_sql_timeout(
    taos: *mut WS_TAOS,
    sql: *const c_char,
    timeout: Duration,
) -> WsResult<WsResultSet> {
    let _ = timeout;
    let client = (taos as *mut Taos)
        .as_mut()
        .ok_or(WsError::new(Code::INVALID_PARA, "client pointer it null"))?;

    let sql = CStr::from_ptr(sql as _).to_str()?;
    let rs = client.query(sql)?;
    Ok(WsResultSet::Sql(WsSqlResultSet::new(rs)))
}

#[no_mangle]
/// Query with a sql command, returns pointer to result set.
///
/// Please always use `ws_errno` to check it work and `ws_free_result` to free memory.
pub unsafe extern "C" fn ws_query(taos: *mut WS_TAOS, sql: *const c_char) -> *mut WS_RES {
    ws_query_with_reqid(taos, sql, get_req_id(taos))
}

#[no_mangle]
pub unsafe extern "C" fn ws_query_with_reqid(
    taos: *mut WS_TAOS,
    sql: *const c_char,
    req_id: u64,
) -> *mut WS_RES {
    tracing::trace!("query {:?}, req_id {:?}", CStr::from_ptr(sql), req_id);
    let res: WsMaybeError<WsResultSet> = query_with_sql(taos, sql, req_id).into();
    tracing::trace!("query done: {:?}", res);
    Box::into_raw(Box::new(res)) as _
}

#[no_mangle]
pub unsafe extern "C" fn ws_stop_query(rs: *mut WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(rs) => {
            rs.stop_query();
            Code::SUCCESS.into()
        }
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "object is invalid")),
    }
}

#[no_mangle]
/// Query a sql with timeout.
///
/// Please always use `ws_errno` to check it work and `ws_free_result` to free memory.
pub unsafe extern "C" fn ws_query_timeout(
    taos: *mut WS_TAOS,
    sql: *const c_char,
    seconds: u32,
) -> *mut WS_RES {
    let res: WsMaybeError<WsResultSet> =
        query_with_sql_timeout(taos, sql, Duration::from_secs(seconds as _)).into();
    Box::into_raw(Box::new(res)) as _
}

/// Get taosc execution timing duration as nanoseconds.
#[no_mangle]
pub unsafe extern "C" fn ws_take_timing(rs: *mut WS_RES) -> i64 {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(rs) => rs.take_timing().as_nanos() as _,
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "WS_RES is null")) as _,
    }
}

#[no_mangle]
/// Always use this to ensure that the query is executed correctly.
pub unsafe extern "C" fn ws_errno(rs: *mut WS_RES) -> i32 {
    if rs.is_null() {
        return get_c_errno();
    }
    match (rs as *mut WsMaybeError<()>)
        .as_ref()
        .and_then(WsMaybeError::errno)
    {
        Some(c) => get_err_code_fromated(c),
        _ => Code::SUCCESS.into(),
    }
}

#[no_mangle]
/// Use this method to get a formatted error string when query errno is not 0.
pub unsafe extern "C" fn ws_errstr(rs: *mut WS_RES) -> *const c_char {
    if rs.is_null() {
        if get_c_errno() == 0 {
            return EMPTY.as_ptr();
        }
        return get_c_error_str() as _;
    }
    match (rs as *mut WsMaybeError<()>)
        .as_ref()
        .and_then(WsMaybeError::errstr)
    {
        Some(e) => e,
        _ => EMPTY.as_ptr(),
    }
}

#[no_mangle]
/// Works exactly the same to taos_affected_rows.
pub unsafe extern "C" fn ws_affected_rows(rs: *const WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.affected_rows() as _,
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "rs is invallid")),
    }
}

#[no_mangle]
/// Works exactly the same to taos_affected_rows64.
pub unsafe extern "C" fn ws_affected_rows64(rs: *const WS_RES) -> i64 {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.affected_rows64() as _,
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "rs is invallid")) as _,
    }
}

#[no_mangle]
/// use db.
pub unsafe extern "C" fn ws_select_db(taos: *mut WS_TAOS, db: *const c_char) -> i32 {
    tracing::trace!("db {:?}", CStr::from_ptr(db));

    let client = (taos as *mut Taos).as_mut();
    let client = match client {
        Some(t) => t,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "taos is invallid"))
        }
    };

    let db = CStr::from_ptr(db).to_str();
    let db = match db {
        Ok(t) => t,
        Err(_) => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "db is invallid"))
        }
    };

    let sql = format!("use {db}");
    let rs = client.query(sql);

    match rs {
        Err(e) => set_error_and_get_code(WsError::from(e)),
        _ => Code::SUCCESS.into(),
    }
}

#[no_mangle]
/// If the query is update query or not
pub unsafe extern "C" fn ws_get_client_info() -> *const c_char {
    static VERSION_INFO: OnceLock<CString> = OnceLock::new();
    VERSION_INFO
        .get_or_init(|| CString::new(env!("CARGO_PKG_VERSION")).unwrap())
        .as_ptr()
}

#[no_mangle]
/// Returns number of fields in current result set.
pub unsafe extern "C" fn ws_field_count(rs: *const WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.num_of_fields(),
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "rs is invalid")),
    }
}

#[no_mangle]
/// If the element is update query or not
pub unsafe extern "C" fn ws_is_update_query(rs: *const WS_RES) -> bool {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.num_of_fields() == 0,
        _ => true,
    }
}

#[no_mangle]
/// Works like taos_fetch_fields, users should use it along with a `num_of_fields`.
pub unsafe extern "C" fn ws_fetch_fields(rs: *mut WS_RES) -> *const WS_FIELD {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(rs) => rs.get_fields(),
        _ => {
            set_error_and_get_code(WsError::new(Code::INVALID_PARA, "rs is invalid"));
            std::ptr::null()
        }
    }
}

#[no_mangle]
/// To fetch v2-compatible fields structs.
pub unsafe extern "C" fn ws_fetch_fields_v2(rs: *mut WS_RES) -> *const WS_FIELD_V2 {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(rs) => rs.get_fields_v2(),
        _ => {
            set_error_and_get_code(WsError::new(Code::INVALID_PARA, "rs is invalid"));
            std::ptr::null()
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
/// Works like taos_fetch_raw_block, it will always return block with format v3.
pub unsafe extern "C" fn ws_fetch_raw_block(
    rs: *mut WS_RES,
    pData: *mut *const c_void,
    numOfRows: *mut i32,
) -> i32 {
    unsafe fn handle_error(error_message: &str, rows: *mut i32) -> i32 {
        *rows = 0;
        set_error_and_get_code(WsError::new(Code::FAILED, error_message))
    }

    match (rs as *mut WsMaybeError<WsResultSet>).as_mut() {
        Some(rs) => match rs.safe_deref_mut() {
            Some(s) => match s.fetch_block(pData, numOfRows) {
                Ok(()) => Code::SUCCESS.into(),
                Err(err) => {
                    set_error_and_get_code(WsError::new(err.errno(), &err.errstr()));
                    let code = err.errno();
                    rs.error = Some(err.into());
                    code.into()
                }
            },
            None => handle_error("WS_RES data is null", numOfRows),
        },
        _ => handle_error("WS_RES is null", numOfRows),
    }
}

#[no_mangle]
/// If the element in (row, column) is null or not
pub unsafe extern "C" fn ws_is_null(rs: *const WS_RES, row: i32, col: i32) -> bool {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(WsResultSet::Sql(rs)) => match &(rs.block) {
            Some(block) => block.is_null(row as _, col as _),
            _ => true,
        },
        _ => true,
    }
}

#[no_mangle]
/// Works like taos_fetch_row
pub unsafe extern "C" fn ws_fetch_row(rs: *mut WS_RES) -> WS_ROW {
    match (rs as *mut WsMaybeError<WsResultSet>).as_mut() {
        Some(rs) => match rs.safe_deref_mut() {
            Some(s) => match s.fetch_row() {
                Ok(p_row) => p_row,
                Err(err) => {
                    set_error_and_get_code(WsError::new(err.errno(), &err.errstr()));
                    std::ptr::null()
                }
            },
            None => {
                set_error_and_get_code(WsError::new(Code::INVALID_PARA, "WS_RES data is null"));
                std::ptr::null()
            }
        },
        _ => {
            set_error_and_get_code(WsError::new(Code::INVALID_PARA, "WS_RES is null"));
            std::ptr::null()
        }
    }
}

#[no_mangle]
/// Returns number of fields in current result set.
pub unsafe extern "C" fn ws_num_fields(rs: *const WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.num_of_fields(),
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "rs is null")),
    }
}

#[no_mangle]
/// Same to taos_free_result. Every websocket result-set object should be freed with this method.
pub unsafe extern "C" fn ws_free_result(rs: *mut WS_RES) -> i32 {
    if !rs.is_null() {
        let _ = Box::from_raw(rs as *mut WsMaybeError<WsResultSet>);
    }
    Code::SUCCESS.into()
}

#[no_mangle]
/// Same to taos_result_precision.
pub unsafe extern "C" fn ws_result_precision(rs: *const WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(rs) => rs.precision() as i32,
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "rs is null")),
    }
}

/// To get value at (row, col) in a block (as a 2-dimension matrix), input row/col index,
/// it will write the value type in *ty, and data length in *len, return a pointer to the real data.
///
/// For type which is var-data (varchar/nchar/json), the `*len` is the bytes length, others is fixed size of that type.
///
/// ## Example
///
/// ```c
/// u8 ty = 0;
/// int len = 0;
/// void* v = ws_get_value_in_block(rs, 0, 0, &ty, &len);
/// if (ty == TSDB_DATA_TYPE_TIMESTAMP) {
///   int64_t* timestamp = (int64_t*)v;
///   printf("ts: %d\n", *timestamp);
/// }
/// ```
#[no_mangle]
pub unsafe extern "C" fn ws_get_value_in_block(
    rs: *mut WS_RES,
    row: i32,
    col: i32,
    ty: *mut u8,
    len: *mut u32,
) -> *const c_void {
    match (rs as *mut WsMaybeError<WsResultSet>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(rs) => {
            let value = rs.get_raw_value(row as _, col as _);
            *ty = value.0 as u8;
            *len = value.1 as _;
            value.2
        }
        _ => {
            *ty = Ty::Null as u8;
            *len = 0;
            std::ptr::null()
        }
    }
}

/// Convert timestamp to C string.
///
/// This function use a thread-local variable to print, it may works in most cases but not always be thread-safe,
///  use it only if it work as you expected.
#[no_mangle]
pub unsafe extern "C" fn ws_timestamp_to_rfc3339(
    dest: *mut u8,
    raw: i64,
    precision: i32,
    use_z: bool,
) {
    let precision = Precision::from_u8(precision as u8);
    let s = Timestamp::new(raw, precision)
        .to_datetime_with_tz()
        .to_rfc3339_opts(precision.to_seconds_format(), use_z);

    std::ptr::copy_nonoverlapping(s.as_ptr(), dest, s.len());
}

#[no_mangle]
pub unsafe extern "C" fn ws_print_row(
    str: *mut c_char,
    str_len: i32,
    row: WS_ROW,
    fields: *const WS_FIELD,
    num_fields: i32,
) -> i32 {
    // rust string to cstr
    unsafe fn write_to_cstr(remain_space: &mut usize, str: *mut c_char, content: &str) -> i32 {
        if content.len() > *remain_space {
            return 0;
        }
        let cstr = CString::new(content).unwrap();
        std::ptr::copy_nonoverlapping(cstr.as_ptr(), str, content.len());
        *remain_space -= content.len();
        content.len() as i32
    }

    let mut len = 0;
    let mut remain_space = (str_len - 1) as usize;
    if remain_space == 0
        || str_len <= 0
        || str.is_null()
        || row.is_null()
        || fields.is_null()
        || num_fields <= 0
    {
        return Code::SUCCESS.into();
    }

    let row_slice = std::slice::from_raw_parts(row, num_fields as usize);
    let fields_slice = std::slice::from_raw_parts(fields, num_fields as usize);

    for i in 0..num_fields as usize {
        if i > 0 && remain_space > 0 {
            *str.add(len as usize) = ' ' as c_char;
            len += 1;
            remain_space -= 1;
        }

        let field_ptr = row_slice[i];
        if field_ptr.is_null() {
            len += write_to_cstr(&mut remain_space, str.add(len as usize), "NULL");
            continue;
        }

        match Ty::from(fields_slice[i].r#type) {
            Ty::TinyInt => {
                let value = std::ptr::read_unaligned(field_ptr as *const i8);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::UTinyInt => {
                let value = std::ptr::read_unaligned(field_ptr as *const u8);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::SmallInt => {
                let value = std::ptr::read_unaligned(field_ptr as *const i16);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::USmallInt => {
                let value = std::ptr::read_unaligned(field_ptr as *const u16);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::Int => {
                let value = std::ptr::read_unaligned(field_ptr as *const i32);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::UInt => {
                let value = std::ptr::read_unaligned(field_ptr as *const u32);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::BigInt | Ty::Timestamp => {
                let value = std::ptr::read_unaligned(field_ptr as *const i64);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::UBigInt => {
                let value = std::ptr::read_unaligned(field_ptr as *const u64);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::Float => {
                let value = std::ptr::read_unaligned(field_ptr as *const f32);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::Double => {
                let value = std::ptr::read_unaligned(field_ptr as *const f64);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{value}").as_str(),
                );
            }
            Ty::Bool => {
                let value = std::ptr::read_unaligned(field_ptr as *const bool);
                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    format!("{}", value as i32).as_str(),
                );
            }
            Ty::VarBinary | Ty::Geometry => {
                let data = field_ptr.offset(-2) as *const InlineBytes;
                let data = &*data;
                let data = Bytes::from(data.as_bytes());

                len += write_to_cstr(
                    &mut remain_space,
                    str.add(len as usize),
                    &hex::bytes_to_hex_string(data),
                );
            }
            Ty::VarChar => {
                let data = field_ptr.offset(-2) as *const InlineStr;
                let data = &*data;

                len += write_to_cstr(&mut remain_space, str.add(len as usize), data.as_str());
            }
            Ty::NChar => {
                let data = field_ptr.offset(-2) as *const InlineNChar;
                let data = &*data;

                len += write_to_cstr(&mut remain_space, str.add(len as usize), &data.to_string());
            }
            _ => {}
        }
    }

    *str.add(len as usize) = 0; // add the null terminator
    len
}

#[no_mangle]
/// Same to taos_get_current_db.
pub unsafe extern "C" fn ws_get_current_db(
    taos: *mut WS_TAOS,
    database: *mut c_char,
    len: c_int,
    required: *mut c_int,
) -> i32 {
    if taos.is_null() {
        return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "taos is null"));
    }

    let rs = ws_query(taos, b"SELECT DATABASE()\0" as *const u8 as _);
    if rs.is_null() {
        return set_error_and_get_code(WsError::new(Code::FAILED, "query failed"));
    }

    let mut block: *const c_void = std::ptr::null();
    let mut rows = 0;
    let mut code = ws_fetch_raw_block(rs, &mut block, &mut rows);
    if code != 0 {
        ws_free_result(rs);
        return code;
    }

    let mut ty: Ty = Ty::Null;
    let mut len_actual = 0u32;
    let res = ws_get_value_in_block(rs, 0, 0, &mut ty as *mut Ty as _, &mut len_actual);
    if res.is_null() {
        ws_free_result(rs);
        return set_error_and_get_code(WsError::new(Code::FAILED, "get value failed"));
    }

    let copy_len = if len <= 0 {
        0
    } else if len_actual < len as u32 {
        len_actual as usize
    } else {
        (len - 1) as usize
    };

    std::ptr::copy_nonoverlapping(res as _, database, copy_len);
    if copy_len > 0 {
        *database.add(copy_len) = 0;
    }

    if len as u32 <= len_actual {
        *required = len_actual as _;
        code = -1;
    }

    ws_free_result(rs);

    code
}

/*  --------------------------schemaless INTERFACE------------------------------- */
#[no_mangle]
pub unsafe extern "C" fn ws_schemaless_insert_raw(
    taos: *mut WS_TAOS,
    lines: *const c_char,
    len: c_int,
    #[allow(non_snake_case)] totalRows: *mut i32,
    protocal: c_int,
    precision: c_int,
) -> *mut WS_RES {
    ws_schemaless_insert_raw_ttl_with_reqid(
        taos,
        lines,
        len,
        totalRows,
        protocal,
        precision,
        0,
        get_req_id(taos),
    )
}

#[no_mangle]
pub unsafe extern "C" fn ws_schemaless_insert_raw_with_reqid(
    taos: *mut WS_TAOS,
    lines: *const c_char,
    len: c_int,
    #[allow(non_snake_case)] totalRows: *mut i32,
    protocal: c_int,
    precision: c_int,
    reqid: u64,
) -> *mut WS_RES {
    ws_schemaless_insert_raw_ttl_with_reqid(
        taos, lines, len, totalRows, protocal, precision, 0, reqid,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ws_schemaless_insert_raw_ttl(
    taos: *mut WS_TAOS,
    lines: *const c_char,
    len: c_int,
    #[allow(non_snake_case)] totalRows: *mut i32,
    protocal: c_int,
    precision: c_int,
    ttl: c_int,
) -> *mut WS_RES {
    ws_schemaless_insert_raw_ttl_with_reqid(
        taos,
        lines,
        len,
        totalRows,
        protocal,
        precision,
        ttl,
        get_req_id(taos),
    )
}

#[no_mangle]
pub unsafe extern "C" fn ws_schemaless_insert_raw_ttl_with_reqid(
    taos: *mut WS_TAOS,
    lines: *const c_char,
    len: c_int,
    #[allow(non_snake_case)] totalRows: *mut i32,
    protocal: c_int,
    precision: c_int,
    ttl: c_int,
    reqid: u64,
) -> *mut WS_RES {
    match schemaless_insert_raw(taos, lines, len, totalRows, protocal, precision, ttl, reqid) {
        Ok(rs) => {
            tracing::trace!("schemaless insert done: {:?}", rs);
            let rs: WsMaybeError<WsResultSet> = rs.into();
            Box::into_raw(Box::new(rs)) as _
        }
        Err(e) => {
            tracing::trace!("schemaless insert failed: {:?}", e);
            let error_message = format!("schemaless insert failed: {e}");
            set_error_and_get_code(WsError::new(e.code, &error_message));
            std::ptr::null_mut() as _
        }
    }
}

#[allow(clippy::too_many_arguments)]
unsafe fn schemaless_insert_raw(
    taos: *mut WS_TAOS,
    lines: *const c_char,
    len: c_int,
    _total_rows: *mut i32,
    protocal: c_int,
    precision: c_int,
    ttl: c_int,
    reqid: u64,
) -> WsResult<WsResultSet> {
    let client = (taos as *mut Taos)
        .as_mut()
        .ok_or(WsError::new(Code::INVALID_PARA, "client pointer it null"))?;

    let slice = std::slice::from_raw_parts(lines as *const u8, len as usize);

    let data = String::from_utf8(slice.to_vec())?;

    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::from(protocal))
        .precision(SchemalessPrecision::from(precision))
        .data(vec![data])
        .ttl(ttl)
        .req_id(reqid)
        .build()?;

    client.put(&sml_data)?;
    let r = WsResultSet::Schemaless(WsSchemalessResultSet::new(
        0,
        Precision::Millisecond,
        Duration::from_millis(0),
    ));
    Ok(r)
}

#[cfg(test)]
pub fn init_env() {
    unsafe {
        std::env::set_var("LIBTAOSWS_LOG_LEVEL", "info");
        ws_enable_log("info\0".as_ptr() as *const c_char);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result() {
        assert_eq!(
            std::mem::size_of::<WsMaybeError<ResultSet>>(),
            std::mem::size_of::<WsMaybeError<()>>()
        );
    }

    #[test]
    fn test_client_info() {
        unsafe {
            let pclient_info = ws_get_client_info();
            dbg!(CStr::from_ptr(pclient_info));
            assert_eq!(
                CStr::from_ptr(pclient_info).to_str().unwrap(),
                env!("CARGO_PKG_VERSION")
            );
        }
    }

    #[test]
    fn test_server_info() {
        unsafe {
            let taos = ws_connect(c"http://localhost:6041".as_ptr());
            assert!(!taos.is_null());

            let version = ws_get_server_info(taos);
            dbg!(CStr::from_ptr(version as _));

            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    #[ignore]
    fn dsn_error() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://unknown-host:15237\0" as *const u8 as _);
            assert!(taos.is_null(), "connection return NULL when failed");
            // check taos.is_null() when in production use case.
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
        }
    }

    #[test]
    fn query_error() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            assert!(!taos.is_null(), "client pointer is not null when success");

            let sql = b"show x\0" as *const u8 as _;
            let rs = ws_query(taos, sql);
            let code = ws_errno(rs);
            let err = CStr::from_ptr(ws_errstr(rs) as _);
            // Incomplete SQL statement
            assert!(code != 0);
            assert!(err
                .to_str()
                .unwrap()
                .match_indices("Incomplete SQL statement")
                .next()
                .is_some());
            assert_eq!(ws_free_result(rs), 0);

            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn is_update_query() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            assert!(!taos.is_null(), "client pointer is not null when success");

            let sql = b"show databases\0" as *const u8 as _;
            let rs = ws_query(taos, sql);
            let code = ws_errno(rs);
            assert!(code == 0);
            assert!(!ws_is_update_query(rs));
            assert_eq!(ws_free_result(rs), 0);

            let sql = b"create database if not exists ws_is_update\0" as *const u8 as _;
            let rs = ws_query(taos, sql);
            let code = ws_errno(rs);
            assert!(code == 0);
            assert!(ws_is_update_query(rs));
            assert_eq!(ws_free_result(rs), 0);

            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn ts_to_rfc3339() {
        unsafe {
            let mut ts = [0; 192];
            ws_timestamp_to_rfc3339(ts.as_mut_ptr(), 0, 0, true);
            let s = CStr::from_ptr(ts.as_ptr() as _);
            dbg!(s);
        }
    }

    #[test]
    fn connect_with_null() {
        unsafe {
            let taos = ws_connect(std::ptr::null());
            assert!(!taos.is_null());
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn test_affected_row() {
        init_env();

        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_affected_row\0");
            execute!(b"create database ws_affected_row keep 36500\0");
            execute!(b"create table ws_affected_row.s1 (ts timestamp, v int, b binary(100))\0");

            let rs = ws_query(
                taos,
                b"insert into ws_affected_row.s1 values (now, 1, 'hello')\0" as *const u8 as _,
            );
            let row = ws_affected_rows(rs);
            assert_eq!(row, 1);
            assert_eq!(ws_free_result(rs), 0);

            let rs = ws_query(
                taos,
                b"insert into ws_affected_row.s1 values (now, 2, 'world')\0" as *const u8 as _,
            );
            let row = ws_affected_rows64(rs);
            assert_eq!(row, 1);
            assert_eq!(ws_free_result(rs), 0);

            execute!(b"drop database if exists ws_affected_row\0");
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn connect() {
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            let version = ws_get_server_info(taos);
            dbg!(CStr::from_ptr(version as _));

            let sql = b"select ts, phase from test.d0 limit 10\0" as *const u8 as _;
            let rs = ws_query(taos, sql);

            let code = ws_errno(rs);
            let errstr = CStr::from_ptr(ws_errstr(rs));

            if code != 0 {
                dbg!(errstr);
                ws_free_result(rs);
                ws_close(taos);
                return;
            }
            assert_eq!(code, 0, "{errstr:?}");

            let affected_rows = ws_affected_rows(rs);
            assert!(affected_rows == 0);

            let affected_rows64 = ws_affected_rows64(rs);
            assert!(affected_rows64 == 0);

            let cols = ws_field_count(rs);
            dbg!(cols);
            // assert!(num_of_fields == 21);
            let fields = ws_fetch_fields(rs);

            for field in std::slice::from_raw_parts(fields, cols as usize) {
                dbg!(field);
            }

            let mut block: *const c_void = std::ptr::null();
            let mut rows = 0;
            let code = ws_fetch_raw_block(rs, &mut block as *mut *const c_void, &mut rows as _);
            assert_eq!(code, 0);

            let is_null = ws_is_null(rs, 0, 0);
            assert!(!is_null);
            let is_null = ws_is_null(rs, 0, cols);
            assert!(is_null);
            let is_null = ws_is_null(rs, 0, 1);
            assert!(!is_null);

            dbg!(rows);
            for row in 0..rows {
                for col in 0..cols {
                    let mut ty: Ty = Ty::Null;
                    let mut len = 0u32;
                    let v =
                        ws_get_value_in_block(rs, row, col, &mut ty as *mut Ty as _, &mut len as _);
                    print!("({row}, {col}): ");
                    if v.is_null() || ty.is_null() {
                        println!("NULL");
                        continue;
                    }
                    match ty {
                        Ty::Null => println!("NULL"),
                        Ty::Bool => println!("{}", *(v as *const bool)),
                        Ty::TinyInt => println!("{}", *(v as *const i8)),
                        Ty::SmallInt => println!("{}", (v as *const i16).read_unaligned()),
                        Ty::Int => println!("{}", (v as *const i32).read_unaligned()),
                        Ty::BigInt => println!("{}", (v as *const i64).read_unaligned()),
                        Ty::Float => println!("{}", (v as *const f32).read_unaligned()),
                        Ty::Double => println!("{}", (v as *const f64).read_unaligned()),
                        Ty::VarChar => println!(
                            "{}",
                            std::str::from_utf8(std::slice::from_raw_parts(
                                v as *const u8,
                                len as usize
                            ))
                            .unwrap()
                        ),
                        Ty::Timestamp => println!("{}", (v as *const i64).read_unaligned()),
                        Ty::NChar => println!(
                            "{}",
                            std::str::from_utf8(std::slice::from_raw_parts(
                                v as *const u8,
                                len as usize
                            ))
                            .unwrap()
                        ),
                        Ty::UTinyInt => println!("{}", *(v as *const u8)),
                        Ty::USmallInt => println!("{}", *(v as *const u16)),
                        Ty::UInt => println!("{}", *(v as *const u32)),
                        Ty::UBigInt => println!("{}", *(v as *const u64)),
                        Ty::Json => println!(
                            "{}",
                            std::str::from_utf8(std::slice::from_raw_parts(
                                v as *const u8,
                                len as usize
                            ))
                            .unwrap()
                        ),
                        Ty::VarBinary => todo!(),
                        Ty::Decimal => todo!(),
                        Ty::Blob => todo!(),
                        Ty::MediumBlob => todo!(),
                        _ => todo!(),
                    }
                }
            }
        }
    }

    #[test]
    fn kepware_connect() {
        let stack_size = 1024 * 1024;

        let handle = thread::Builder::new()
            .stack_size(stack_size)
            .spawn(|| {
                let dsn = "http://localhost:6041?company=kepware";

                init_env();
                unsafe {
                    let c_dsn = CString::new(dsn).expect("CString::new failed");

                    let taos = ws_connect(c_dsn.as_ptr() as *const u8 as _);
                    if taos.is_null() {
                        let code = ws_errno(taos);
                        assert!(code != 0);
                        let str = ws_errstr(taos);
                        dbg!(CStr::from_ptr(str));
                    }
                    assert!(!taos.is_null());

                    let version = ws_get_server_info(taos);
                    dbg!(CStr::from_ptr(version as _));
                    assert_eq!(ws_close(taos), 0);
                }
            })
            .expect("Failed to spawn thread");

        handle.join().expect("Thread panicked");
    }

    #[test]
    fn test_connect() {
        let dsn = "http://localhost:6041";

        init_env();
        unsafe {
            let c_dsn = CString::new(dsn).expect("CString::new failed");

            let taos = ws_connect(c_dsn.as_ptr() as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            let version = ws_get_server_info(taos);
            dbg!(CStr::from_ptr(version as _));

            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn test_timing() {
        use std::time::Instant;
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0".as_ptr() as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            let sql = b"show databases;\0" as *const u8 as _;

            let start = Instant::now();
            let rs = ws_query(taos, sql);
            let timing1 = ws_take_timing(rs);

            let mut block: *const c_void = std::ptr::null();
            let mut rows = 0;
            let code = ws_fetch_raw_block(rs, &mut block as *mut *const c_void, &mut rows as _);
            assert_eq!(code, 0);

            let end = Instant::now();

            let duration = end.duration_since(start);
            println!("total: {:?}", duration.as_nanos());

            let timing2 = ws_take_timing(rs);
            ws_free_result(rs);
            ws_close(taos);
            dbg!((timing1) as u128);
            dbg!((timing2) as u128);
            dbg!((timing2 + timing1) as u128);
            assert!(((timing1 + timing2) as u64) < duration.as_nanos() as u64);
        }
    }

    #[test]
    fn simple_fetch_row_test() {
        init_env();

        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            let version = ws_get_server_info(taos);
            dbg!(CStr::from_ptr(version as _));

            let sql = b"select ts, groupid, current, location  from test.meters limit 10\0"
                as *const u8 as _;

            let rs = ws_query(taos, sql);

            let code = ws_errno(rs);
            let errstr = CStr::from_ptr(ws_errstr(rs));

            if code != 0 {
                dbg!(errstr);
                ws_free_result(rs);
                ws_close(taos);
                return;
            }
            assert_eq!(code, 0, "{errstr:?}");

            let cols = ws_field_count(rs);
            dbg!(cols);
            // assert!(num_of_fields == 21);
            let fields = ws_fetch_fields(rs);
            let fields = std::slice::from_raw_parts(fields, cols as usize);

            for field in fields {
                dbg!(field);
            }

            let mut line_num = 1;

            loop {
                let row_data = ws_fetch_row(rs);
                let errno = ws_errno(rs);
                if errno != 0 {
                    let errstr = ws_errstr(rs);
                    dbg!(CStr::from_ptr(errstr));
                    break;
                }

                if row_data == std::ptr::null() {
                    break;
                }
                let row_data = std::slice::from_raw_parts(row_data, cols as usize);

                for col in 0..cols {
                    let col_type = fields[col as usize].r#type;
                    let ty: Ty = Ty::from(col_type);

                    let v = row_data[col as usize] as *const c_void;
                    let len = 0u32;
                    if v.is_null() || ty.is_null() {
                        println!("NULL");
                        continue;
                    }
                    match ty {
                        Ty::Null => println!("NULL"),
                        Ty::Bool => println!("{}", *(v as *const bool)),
                        Ty::TinyInt => println!("{}", *(v as *const i8)),
                        Ty::SmallInt => println!("{}", (v as *const i16).read_unaligned()),
                        Ty::Int => println!("{}", (v as *const i32).read_unaligned()),
                        Ty::BigInt => println!("{}", (v as *const i64).read_unaligned()),
                        Ty::Float => println!("{}", (v as *const f32).read_unaligned()),
                        Ty::Double => println!("{}", (v as *const f64).read_unaligned()),
                        Ty::VarChar => println!(
                            "{}",
                            std::str::from_utf8(std::slice::from_raw_parts(
                                v as *const u8,
                                ((v as *const u8).wrapping_sub(2) as *const i16).read_unaligned()
                                    as usize
                            ))
                            .unwrap()
                        ),
                        Ty::Timestamp => println!("{}", (v as *const i64).read_unaligned()),
                        Ty::NChar => println!(
                            "{}",
                            std::str::from_utf8(std::slice::from_raw_parts(
                                v as *const u8,
                                len as usize
                            ))
                            .unwrap()
                        ),
                        Ty::UTinyInt => println!("{}", *(v as *const u8)),
                        Ty::USmallInt => println!("{}", *(v as *const u16)),
                        Ty::UInt => println!("{}", *(v as *const u32)),
                        Ty::UBigInt => println!("{}", *(v as *const u64)),
                        Ty::Json => println!(
                            "{}",
                            std::str::from_utf8(std::slice::from_raw_parts(
                                v as *const u8,
                                len as usize
                            ))
                            .unwrap()
                        ),
                        Ty::VarBinary => todo!(),
                        Ty::Decimal => todo!(),
                        Ty::Blob => todo!(),
                        Ty::MediumBlob => todo!(),
                        _ => todo!(),
                    }
                }

                println!("line num = {}", line_num);
                line_num += 1;
            }

            if get_c_errno() == 0 {
                println!("fetch row done");
            } else {
                let error = ws_errstr(rs);
                dbg!(CStr::from_ptr(error));
            }
        }
    }

    #[test]
    fn test_stop_query() {
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_stop_query\0");
            execute!(b"create database ws_stop_query keep 36500\0");
            execute!(b"create table ws_stop_query.s1 (ts timestamp, v int, b binary(100))\0");

            let version = ws_get_server_info(taos);
            dbg!(CStr::from_ptr(version as _));

            let res = ws_query(
                taos,
                b"select count(*) from ws_stop_query.s1\0" as *const u8 as _,
            );
            let cols = ws_field_count(res);
            dbg!(cols);
            let fields = ws_fetch_fields(res);

            for field in std::slice::from_raw_parts(fields, cols as usize) {
                dbg!(field);
            }

            ws_stop_query(res);
            assert_eq!(ws_free_result(res), 0);

            execute!(b"drop database if exists ws_stop_query\0");
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn test_ws_num_fields() {
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_num_fields\0");
            execute!(b"create database ws_num_fields keep 36500\0");
            execute!(b"create table ws_num_fields.s1 (ts timestamp, v int, b binary(100))\0");

            let version = ws_get_server_info(taos);
            dbg!(CStr::from_ptr(version as _));

            let res = ws_query(
                taos,
                b"select count(*) from ws_num_fields.s1\0" as *const u8 as _,
            );
            let cols = ws_field_count(res);
            dbg!(cols);

            assert_eq!(ws_free_result(res), 0);
            execute!(b"drop database if exists ws_num_fields\0");
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn test_get_current_db() {
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_get_current_db\0");
            execute!(b"create database ws_get_current_db keep 36500\0");
            execute!(b"use ws_get_current_db\0");

            let mut database_buffer = vec![0; 10];
            let database = database_buffer.as_mut_ptr();
            let len = database_buffer.len() as c_int;
            let mut required = 0;
            let code = ws_get_current_db(taos, database, len, &mut required);
            assert_eq!(code, -1);
            assert_eq!(required, 17);

            let database = CStr::from_ptr(database as _);
            assert_eq!(database, CStr::from_bytes_with_nul(b"ws_get_cu\0").unwrap());

            let mut database_buffer = vec![0; 128];
            let database = database_buffer.as_mut_ptr();
            let len = database_buffer.len() as c_int;
            let mut required = 0;
            let code = ws_get_current_db(taos, database, len, &mut required);
            assert_eq!(code, 0);

            let database = CStr::from_ptr(database as _);
            assert_eq!(
                database,
                CStr::from_bytes_with_nul(b"ws_get_current_db\0").unwrap()
            );

            execute!(b"drop database if exists ws_get_current_db\0");
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn test_bi_mode() {
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041?conn_mode=1\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_bi_mode\0");
            execute!(b"create database ws_bi_mode keep 36500\0");
            execute!(b"use ws_bi_mode\0");
            execute!(b"CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int)\0");
            execute!(b"CREATE TABLE d1001 USING meters TAGS ('California.SanFrancisco', 2)\0");
            execute!(b"INSERT INTO d1001 USING meters TAGS ('California.SanFrancisco', 2) VALUES (NOW, 10.2, 219, 0.32)\0");

            let sql = b"select * from meters\0" as *const u8 as _;
            let rs = ws_query(taos, sql);
            let code = ws_errno(rs);
            let errstr = CStr::from_ptr(ws_errstr(rs));

            if code != 0 {
                dbg!(errstr);
                ws_free_result(rs);
                ws_close(taos);
                return;
            }
            assert_eq!(code, 0, "{errstr:?}");

            let cols = ws_field_count(rs);
            dbg!(cols);
            let fields = ws_fetch_fields(rs);
            let fields = std::slice::from_raw_parts(fields, cols as usize);

            for field in fields {
                dbg!(field);
            }

            // bi mode will show tbname
            assert!(fields.len() >= 6);

            assert_eq!(ws_free_result(rs), 0);
            execute!(b"drop database if exists ws_bi_mode\0");
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn test_schemaless() {
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }
            execute!(b"drop database if exists schemaless_test\0");
            execute!(b"create database if not exists schemaless_test\0");
            execute!(b"use schemaless_test\0");

            let data = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639";
            let c_data = CString::new(data).expect("CString::new failed");
            let lines = c_data.as_ptr() as *const c_char;
            let len = data.len() as c_int;

            let _total_rows: *mut i32 = &mut 0;
            let protocal = WS_TSDB_SML_PROTOCOL_TYPE::WS_TSDB_SML_LINE_PROTOCOL as i32;
            let precision = WS_TSDB_SML_TIMESTAMP_TYPE::WS_TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 0;
            let reqid = 123456u64;
            let _rs = schemaless_insert_raw(
                taos,
                lines as *const c_char,
                len as i32,
                _total_rows,
                protocal,
                precision,
                ttl,
                reqid,
            )
            .unwrap();

            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn test_tsdb_type() {
        init_env();
        unsafe {
            let type_null_str = ws_data_type(0);
            let type_null_str = CStr::from_ptr(type_null_str);
            assert_eq!(
                type_null_str,
                CStr::from_bytes_with_nul(b"TSDB_DATA_TYPE_NULL\0").unwrap()
            );

            let type_geo_str = ws_data_type(20);
            let type_geo_str = CStr::from_ptr(type_geo_str);
            assert_eq!(
                type_geo_str,
                CStr::from_bytes_with_nul(b"TSDB_DATA_TYPE_GEOMETRY\0").unwrap()
            );

            let type_invalid = ws_data_type(100);
            assert_eq!(type_invalid, std::ptr::null(),);
        }
    }

    #[test]
    fn test_err_code() {
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
            assert!(!taos.is_null());

            let sql = b"select * from db_not_exsits.tb1\0" as *const u8 as _;
            let rs = ws_query(taos, sql);
            let code = ws_errno(rs);
            assert!(code != 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
            ws_free_result(rs);
            ws_close(taos);

            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
            assert!(!taos.is_null());

            let sql = b"select to_iso8601(0) as ts\0" as *const u8 as _;
            let rs = ws_query(taos, sql);
            let code = ws_errno(rs);
            assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
            ws_free_result(rs);
            ws_close(taos);
        }
    }

    #[test]
    #[ignore]
    fn test_ipv6() {
        unsafe {
            let taos = ws_connect(b"ws://[::1]:6041\0" as *const u8 as _);
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists test_1748918714\0");
            execute!(b"create database test_1748918714\0");
            execute!(b"use test_1748918714\0");
            execute!(b"create table t0 (ts timestamp, c1 int)\0");
            execute!(b"insert into t0 values (1741660079228, 1)\0");

            let res = ws_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = ws_fetch_row(res);
            assert!(!row.is_null());

            let fields = ws_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = ws_num_fields(res);
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = ws_print_row(str.as_mut_ptr(), str.len() as _, row, fields, num_fields);
            assert_eq!(len, 15);
            assert_eq!(
                "1741660079228 1",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );

            ws_free_result(res);
            execute!(b"drop database test_1748918714\0");
            ws_close(taos);
        }
    }
}

#[cfg(test)]
mod cloud_tests {
    use super::*;

    #[test]
    fn test_connect() -> anyhow::Result<()> {
        init_env();

        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_connect");
            return Ok(());
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_connect");
            return Ok(());
        }

        let dsn = format!("{}/rust_test?token={}", url.unwrap(), token.unwrap());

        unsafe {
            let cdsn = CString::new(dsn).unwrap();
            let taos = ws_connect(cdsn.as_ptr() as *const u8 as _);
            assert!(!taos.is_null());

            let version = ws_get_server_info(taos);
            tracing::info!("Server version: {:?}", CStr::from_ptr(version as _));

            assert_eq!(ws_close(taos), 0);
        }

        Ok(())
    }
}
