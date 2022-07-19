use std::{
    any::TypeId,
    ffi::{c_void, CStr, CString},
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
    os::raw::c_char,
    str::Utf8Error,
    time::Duration,
};

use taos_error::Code;

use taos_query::{
    common::{Field, RawData as Block, Timestamp},
    common::{Precision, Ty},
    Fetchable,
};
use taos_ws::sync::*;

pub use taos_ws::sync::WS_ERROR_NO;

pub mod stmt;

const EMPTY: &'static CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"\0") };
static mut C_ERROR_CONTAINER: [u8; 4096] = [0; 4096];
static mut C_ERRNO: Code = Code::Success;

/// Opaque type definition for websocket connection.
#[allow(non_camel_case_types)]
pub type WS_TAOS = c_void;

/// Opaque type definition for websocket result set.
#[allow(non_camel_case_types)]
pub type WS_RES = c_void;

#[derive(Debug)]
pub struct WsError {
    code: Code,
    message: CString,
    source: Option<Box<dyn std::error::Error + 'static>>,
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
            log::debug!("dropping obj {}", self.type_id);
            let _ = unsafe { self.data.read() };
        }
    }
}

#[test]
fn test_result() {
    assert_eq!(
        std::mem::size_of::<WsMaybeError<ResultSet>>(),
        std::mem::size_of::<WsMaybeError<()>>()
    );
}

impl<T> WsMaybeError<T> {
    pub fn errno(&self) -> Option<i32> {
        self.error.as_ref().map(|s| s.code.into())
    }
    pub fn errstr(&self) -> Option<*const c_char> {
        self.error.as_ref().map(|s| s.message.as_ptr())
    }
}

impl<T> Deref for WsMaybeError<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.data.as_ref().unwrap() }
    }
}

impl<T> DerefMut for WsMaybeError<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.data.as_mut().unwrap() }
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

impl WsError {
    fn new(code: Code, message: &str) -> Self {
        Self {
            code,
            message: CString::new(message).unwrap(),
            source: None,
        }
    }
}

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
            code: Code::Failed,
            message: CString::new(format!("{}", e)).unwrap(),
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

impl From<taos_ws::stmt::Error> for WsError {
    fn from(e: taos_ws::stmt::Error) -> Self {
        Self {
            code: e.errno(),
            message: CString::new(e.errstr()).unwrap(),
            source: None,
        }
    }
}

type WsTaos = Result<WsClient, WsError>;

/// Only useful for developers who use along with TDengine 2.x `TAOS_FIELD` struct.
/// It means that the struct has the same memory layout with the `TAOS_FIELD` struct
/// in taos.h of TDengine 2.x
#[repr(C)]
#[derive(Copy, Clone, Debug)]
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
            std::ptr::copy_nonoverlapping(f_name.as_ptr(), name.as_mut_ptr() as _, f_name.len())
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
            std::ptr::copy_nonoverlapping(f_name.as_ptr(), name.as_mut_ptr() as _, f_name.len())
        };
        Self {
            name,
            r#type: field.ty() as u8,
            bytes: field.bytes(),
        }
    }
}

#[derive(Debug)]
struct WsResultSet {
    rs: ResultSet,
    block: Option<Block>,
    fields: Vec<WS_FIELD>,
    fields_v2: Vec<WS_FIELD_V2>,
}

// impl Deref for WsResultSet {
//     type Target = ResultSet;

// }

impl WsResultSet {
    fn new(rs: ResultSet) -> Self {
        Self {
            rs,
            block: None,
            fields: Vec::new(),
            fields_v2: Vec::new(),
        }
    }

    fn precision(&self) -> Precision {
        self.rs.precision()
    }

    fn affected_rows(&self) -> i32 {
        self.rs.affected_rows() as _
    }

    fn num_of_fields(&self) -> i32 {
        self.rs.num_of_fields() as _
    }

    fn get_fields(&mut self) -> *const WS_FIELD {
        if self.fields.len() == self.rs.num_of_fields() {
            self.fields.as_ptr()
        } else {
            self.fields.clear();
            self.fields
                .extend(self.rs.fields().iter().map(WS_FIELD::from));
            self.fields.as_ptr()
        }
    }
    fn get_fields_v2(&mut self) -> *const WS_FIELD_V2 {
        if self.fields_v2.len() == self.rs.num_of_fields() {
            self.fields_v2.as_ptr()
        } else {
            self.fields_v2.clear();
            self.fields_v2
                .extend(self.rs.fields().iter().map(WS_FIELD_V2::from));
            self.fields_v2.as_ptr()
        }
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error> {
        log::debug!("fetch block with ptr {ptr:p}");
        self.block = self.rs.fetch_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        log::debug!("fetch block with ptr {ptr:p} with rows {}", *rows);
        Ok(())
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        log::debug!("try to get raw value at ({row}, {col})");
        match self.block.as_ref() {
            Some(block) => {
                if row < block.nrows() && col < block.ncols() {
                    let res = block.get_raw_value_unchecked(row, col);
                    log::debug!("got raw value at ({row}, {col}): {:?}", res);
                    res
                } else {
                    log::debug!("out of range at ({row}, {col}), return null");
                    (Ty::Null, 0, std::ptr::null())
                }
            }
            None => (Ty::Null, 0, std::ptr::null()),
        }
    }
}

unsafe fn connect_with_dsn(dsn: *const c_char) -> WsTaos {
    let dsn = CStr::from_ptr(dsn).to_str()?;
    Ok(WsClient::from_dsn(dsn)?)
}

/// Enable inner log to stdout with environment RUST_LOG.
///
/// # Example
///
/// ```c
/// ws_enable_log();
/// ```
///
/// To show debug logs:
///
/// ```bash
/// RUST_LOG=debug ./a.out
/// ```
#[no_mangle]
pub unsafe extern "C" fn ws_enable_log() {
    pretty_env_logger::init();
}

/// Connect via dsn string, returns NULL if failed.
///
/// Remember to check the return pointer is null and get error details.
///
/// # Example
///
/// ```c
/// char* dsn = "taos://localhost:6041";
/// WS_TAOS* taos = ws_connect_with_dsn(dsn);
/// if (taos == NULL) {
///   int errno = ws_errno(NULL);
///   char* errstr = ws_errstr(NULL);
///   printf("Connection failed[%d]: %s", errno, errstr);
///   exit(-1);
/// }
/// ```
#[no_mangle]
pub unsafe extern "C" fn ws_connect_with_dsn(dsn: *const c_char) -> *mut WS_TAOS {
    C_ERRNO = Code::Success;
    match connect_with_dsn(dsn) {
        Ok(client) => Box::into_raw(Box::new(client)) as _,
        Err(err) => {
            C_ERRNO = err.code;
            let dst = C_ERROR_CONTAINER.as_mut_ptr();
            let errstr = err.message.as_bytes_with_nul();
            std::ptr::copy_nonoverlapping(errstr.as_ptr(), dst, errstr.len());
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
/// Same to taos_close. This should always be called after everything done with the connection.
pub unsafe extern "C" fn ws_get_server_info(taos: *mut WS_TAOS) -> *const c_char {
    static mut VERSION_INFO: [u8; 128] = [0; 128];
    if taos.is_null() {
        return VERSION_INFO.as_ptr() as *const c_char;
    }
    if VERSION_INFO[0] == 0 {
        if let Some(taos) = (taos as *mut WsClient).as_mut() {
            let v = taos.version();
            std::ptr::copy_nonoverlapping(v.as_ptr(), VERSION_INFO.as_mut_ptr(), v.len());
        }
        VERSION_INFO.as_ptr() as *const c_char
    } else {
        VERSION_INFO.as_ptr() as *const c_char
    }
}

#[no_mangle]
/// Same to taos_close. This should always be called after everything done with the connection.
pub unsafe extern "C" fn ws_close(taos: *mut WS_TAOS) {
    if !taos.is_null() {
        log::debug!("close connection {taos:p}");
        let client = Box::from_raw(taos as *mut WsClient);
        client.close();
        drop(client);
    }
}

unsafe fn query_with_sql(taos: *mut WS_TAOS, sql: *const c_char) -> WsResult<WsResultSet> {
    let client = (taos as *mut WsClient)
        .as_mut()
        .ok_or(WsError::new(Code::Failed, "client pointer it null"))?;

    let sql = CStr::from_ptr(sql as _).to_str()?;
    let rs = client.s_query(sql)?;
    Ok(WsResultSet::new(rs))
}

unsafe fn query_with_sql_timeout(
    taos: *mut WS_TAOS,
    sql: *const c_char,
    timeout: Duration,
) -> WsResult<WsResultSet> {
    let client = (taos as *mut WsClient)
        .as_mut()
        .ok_or(WsError::new(Code::Failed, "client pointer it null"))?;

    let sql = CStr::from_ptr(sql as _).to_str()?;
    let rs = client.s_query_timeout(sql, timeout)?;
    Ok(WsResultSet::new(rs))
}

#[no_mangle]
/// Query with a sql command, returns pointer to result set.
///
/// Please always use `ws_errno` to check it work and `ws_free_result` to free memory.
pub unsafe extern "C" fn ws_query(taos: *mut WS_TAOS, sql: *const c_char) -> *mut WS_RES {
    log::debug!("query {:?}", CStr::from_ptr(sql));
    let res: WsMaybeError<WsResultSet> = query_with_sql(taos, sql).into();
    log::debug!("query done: {:?}", res);
    Box::into_raw(Box::new(res)) as _
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

#[no_mangle]
/// Always use this to ensure that the query is executed correctly.
pub unsafe extern "C" fn ws_errno(rs: *mut WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<()>)
        .as_ref()
        .and_then(|s| s.errno())
    {
        Some(c) => c,
        _ => C_ERRNO.into(),
    }
}

#[no_mangle]
/// Use this method to get a formatted error string when query errno is not 0.
pub unsafe extern "C" fn ws_errstr(rs: *mut WS_RES) -> *const c_char {
    match (rs as *mut WsMaybeError<()>)
        .as_ref()
        .and_then(|s| s.errstr())
    {
        Some(e) => e,
        _ => {
            if C_ERRNO.success() {
                EMPTY.as_ptr()
            } else {
                C_ERROR_CONTAINER.as_ptr() as _
            }
        }
    }
}

#[no_mangle]
/// Works exactly the same to taos_affected_rows.
pub unsafe extern "C" fn ws_affected_rows(rs: *const WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>).as_ref() {
        Some(rs) => rs.affected_rows(),
        _ => 0,
    }
}

#[no_mangle]
/// Returns number of fields in current result set.
pub unsafe extern "C" fn ws_field_count(rs: *const WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>).as_ref() {
        Some(rs) => rs.num_of_fields(),
        _ => 0,
    }
}

#[no_mangle]
/// If the query is update query or not
pub unsafe extern "C" fn ws_is_update_query(rs: *const WS_RES) -> bool {
    match (rs as *mut WsMaybeError<WsResultSet>).as_ref() {
        Some(rs) => rs.num_of_fields() == 0,
        _ => true,
    }
}

#[no_mangle]
/// Works like taos_fetch_fields, users should use it along with a `num_of_fields`.
pub unsafe extern "C" fn ws_fetch_fields(rs: *mut WS_RES) -> *const WS_FIELD {
    match (rs as *mut WsMaybeError<WsResultSet>).as_mut() {
        Some(rs) => rs.get_fields(),
        _ => std::ptr::null(),
    }
}

#[no_mangle]
/// To fetch v2-compatible fields structs.
pub unsafe extern "C" fn ws_fetch_fields_v2(rs: *mut WS_RES) -> *const WS_FIELD_V2 {
    match (rs as *mut WsMaybeError<WsResultSet>).as_mut() {
        Some(rs) => rs.get_fields_v2(),
        _ => std::ptr::null(),
    }
}
#[no_mangle]
/// Works like taos_fetch_raw_block, it will always return block with format v3.
pub unsafe extern "C" fn ws_fetch_block(
    rs: *mut WS_RES,
    ptr: *mut *const c_void,
    rows: *mut i32,
) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>).as_mut() {
        Some(rs) => match rs.fetch_block(ptr, rows) {
            Ok(()) => 0,
            Err(err) => {
                let code = err.errno();
                rs.error = Some(err.into());
                code.into()
            }
        },
        _ => {
            *rows = 0;

            C_ERRNO = Code::Failed;
            let dst = C_ERROR_CONTAINER.as_mut_ptr();
            const NULL_PTR_RES: &'static str = "WS_RES is null";
            std::ptr::copy_nonoverlapping(NULL_PTR_RES.as_ptr(), dst, NULL_PTR_RES.len());
            Code::Failed.into()
        }
    }
}

#[no_mangle]
/// Same to taos_free_result. Every websocket result-set object should be freed with this method.
pub unsafe extern "C" fn ws_free_result(rs: *mut WS_RES) {
    let _ = Box::from_raw(rs as *mut WsMaybeError<WsResultSet>);
}

#[no_mangle]
/// Same to taos_result_precision.
pub unsafe extern "C" fn ws_result_precision(rs: *const WS_RES) -> i32 {
    match (rs as *mut WsMaybeError<WsResultSet>).as_mut() {
        Some(rs) => rs.precision() as i32,
        _ => 0,
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
    match (rs as *mut WsMaybeError<WsResultSet>).as_mut() {
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
    let s = format!(
        "{}",
        Timestamp::new(raw, precision)
            .to_datetime_with_tz()
            .to_rfc3339_opts(precision.to_seconds_format(), use_z)
    );

    std::ptr::copy_nonoverlapping(s.as_ptr(), dest, s.len());
}

#[no_mangle]
/// Unimplemented currently.
pub unsafe fn ws_print_row(rs: *mut WS_RES, row: i32) {
    todo!()
    // match (rs as *mut WsResultSet).as_mut() {
    //     Some(rs) => rs.fetch_block(ptr, rows),
    //     None => {
    //         *rows = 0;
    //         0
    //     },
    // }
}

#[cfg(test)]
pub fn init_env() {
    static ONCE_INIT: std::sync::Once = std::sync::Once::new();
    ONCE_INIT.call_once(|| {
        pretty_env_logger::init();
        std::env::set_var("RUST_DEBUG", "debug");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dsn_error() {
        init_env();
        unsafe {
            let taos = ws_connect_with_dsn(b"ws://unknown-host:15237\0" as *const u8 as _);
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
            let taos = ws_connect_with_dsn(b"ws://localhost:6041\0" as *const u8 as _);
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
        }
    }

    #[test]
    fn is_update_query() {
        init_env();
        unsafe {
            let taos = ws_connect_with_dsn(b"ws://localhost:6041\0" as *const u8 as _);
            assert!(!taos.is_null(), "client pointer is not null when success");

            let sql = b"show databases\0" as *const u8 as _;
            let rs = ws_query(taos, sql);

            let code = ws_errno(rs);
            assert!(code == 0);
            assert!(!ws_is_update_query(rs));

            let sql = b"create database if not exists ws_is_update\0" as *const u8 as _;
            let rs = ws_query(taos, sql);
            let code = ws_errno(rs);
            assert!(code == 0);
            assert!(ws_is_update_query(rs));
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
    fn connect() {
        init_env();
        unsafe {
            let taos = ws_connect_with_dsn(b"http://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            let version = ws_get_server_info(taos);
            dbg!(CStr::from_ptr(version as _));

            let sql = b"select groupid from test.d0 limit 10\0" as *const u8 as _;
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

            let cols = ws_field_count(rs);
            dbg!(cols);
            // assert!(num_of_fields == 21);
            let fields = ws_fetch_fields(rs);

            for field in std::slice::from_raw_parts(fields, cols as usize) {
                dbg!(field);
            }

            let mut block: *const c_void = std::ptr::null();
            let mut rows = 0;
            let code = ws_fetch_block(rs, &mut block as *mut *const c_void, &mut rows as _);
            assert_eq!(code, 0);

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
                        Ty::SmallInt => println!("{}", *(v as *const i16)),
                        Ty::Int => println!("{}", *(v as *const i32)),
                        Ty::BigInt => println!("{}", *(v as *const i64)),
                        Ty::Float => println!("{}", *(v as *const f32)),
                        Ty::Double => println!("{}", *(v as *const f64)),
                        Ty::VarChar => println!(
                            "{}",
                            std::str::from_utf8(std::slice::from_raw_parts(
                                v as *const u8,
                                len as usize
                            ))
                            .unwrap()
                        ),
                        Ty::Timestamp => println!("{}", *(v as *const i64)),
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
}
