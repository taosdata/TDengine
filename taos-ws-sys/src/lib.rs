use std::{
    ffi::{c_void, CStr, CString},
    fmt::{Debug, Display},
    os::raw::c_char,
    str::Utf8Error,
};

use taos_error::Code;

use taos_query::{
    common::{Block, Field},
    common::{Precision, Ty},
    Fetchable,
};
use taos_ws::{sync::*, Error, Ws};

use anyhow::Result;

const EMPTY: &'static CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"\0") };

pub type WS_TAOS = c_void;

pub type WS_RS = c_void;

use std::cell::RefCell;
thread_local! {
    pub static ERROR: RefCell<Option<Error>> = RefCell::new(None);
}

#[derive(Debug)]
struct WsError {
    code: Code,
    message: CString,
    source: Option<Box<dyn std::error::Error + 'static>>,
}

impl WsError {
    fn new(code: Code, message: &str) -> Self {
        Self {
            code: Code::Failed,
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
            code: Code::Failed,
            message: CString::new(format!("{}", e)).unwrap(),
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

impl From<taos_ws::sync::Error> for WsError {
    fn from(e: taos_ws::sync::Error) -> Self {
        Self {
            code: Code::Failed,
            message: CString::new(format!("{}", e)).unwrap(),
            source: None,
        }
    }
}

type WsTaos = Result<WsClient, WsError>;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WS_FIELD {
    pub name: [u8; 65usize],
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
        let mut name = [0; 65usize];
        unsafe { std::ptr::copy_nonoverlapping(f_name.as_ptr(), name.as_mut_ptr(), f_name.len()) };
        Self {
            name,
            r#type: field.ty() as u8,
            bytes: field.bytes(),
        }
    }
}

struct WsResultSet {
    rs: Result<ResultSet, WsError>,
    block: Option<Block>,
    fields: Vec<WS_FIELD>,
}

impl WsResultSet {
    fn new(rs: Result<ResultSet, WsError>) -> Self {
        Self {
            rs,
            block: None,
            fields: Vec::new(),
        }
    }
    fn errno(&self) -> i32 {
        match self.rs.as_ref() {
            Ok(_) => 0,
            Err(err) => err.code as i32,
        }
    }
    fn errstr(&self) -> *const c_char {
        const EMPTY: &'static CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"\0") };
        match self.rs.as_ref() {
            Ok(_) => EMPTY.as_ptr() as _,
            Err(err) => err.message.as_ptr() as _,
        }
    }

    fn precision(&self) -> Precision {
        match self.rs.as_ref() {
            Ok(rs) => rs.precision(),
            Err(_) => Precision::Millisecond,
        }
    }

    fn affected_rows(&self) -> i32 {
        match self.rs.as_ref() {
            Ok(rs) => rs.affected_rows() as _,
            Err(_) => 0,
        }
    }

    fn num_of_fields(&self) -> i32 {
        match self.rs.as_ref() {
            Ok(rs) => rs.num_of_fields() as _,
            Err(_) => 0,
        }
    }

    fn get_fields(&mut self) -> *const WS_FIELD {
        match self.rs.as_ref() {
            Ok(rs) => {
                if self.fields.len() == rs.num_of_fields() {
                    self.fields.as_ptr()
                } else {
                    self.fields.clear();
                    self.fields.extend(rs.fields().iter().map(WS_FIELD::from));
                    self.fields.as_ptr()
                }
            }
            Err(_) => std::ptr::null(),
        }
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> i32 {
        match self.rs.as_mut() {
            Ok(rs) => {
                self.block = rs.next();
                if let Some(block) = self.block.as_ref() {
                    *ptr = block.as_raw_block().as_bytes().as_ptr() as _;
                    *rows = block.nrows() as _;
                } else {
                    *rows = 0;
                }
                0
            }
            Err(err) => err.code as i32,
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        match self.block.as_ref() {
            Some(block) => block.as_raw_block().get_raw_value_unchecked(row, col),
            None => (Ty::Null, 0, std::ptr::null()),
        }
    }
}

unsafe fn connect_with_dsn(dsn: *const c_char) -> WsTaos {
    let dsn = CStr::from_ptr(dsn).to_str()?;
    Ok(WsClient::from_dsn(dsn)?)
}

#[no_mangle]
pub unsafe extern "C" fn ws_connect_with_dsn(dsn: *const c_char) -> *mut WS_TAOS {
    Box::into_raw(Box::new(connect_with_dsn(dsn))) as _
}

#[no_mangle]
pub unsafe extern "C" fn ws_connect_errno(taos: *mut WS_TAOS) -> i32 {
    match (taos as *mut WsTaos).as_ref() {
        Some(Ok(_)) => 0,
        Some(Err(err)) => err.code as i32,
        None => 0,
    }
}
#[no_mangle]
pub unsafe extern "C" fn ws_connect_errstr(taos: *mut WS_TAOS) -> *const c_char {
    const EMPTY: &'static CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"\0") };
    match (taos as *mut WsTaos).as_ref() {
        Some(Ok(_)) => EMPTY.as_ptr(),
        Some(Err(err)) => err.message.as_ptr() as _,
        None => EMPTY.as_ptr(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_close(taos: *mut WS_TAOS) {
    let _ = Box::from_raw(taos as *mut WsTaos);
}

unsafe fn query_with_sql(taos: *mut WS_TAOS, sql: *const c_char) -> Result<ResultSet, WsError> {
    let client = (taos as *mut WsTaos)
        .as_mut()
        .ok_or(WsError::new(Code::Failed, "client pointer it null"))?
        .as_ref()?;

    let sql = CStr::from_ptr(sql as _).to_str()?;
    let rs = client.s_query(sql)?;
    Ok(rs)
}

#[no_mangle]
pub unsafe extern "C" fn ws_query(taos: *mut WS_TAOS, sql: *const c_char) -> *mut WS_RS {
    let res = query_with_sql(taos, sql);
    Box::into_raw(Box::new(WsResultSet::new(res))) as _
}

#[no_mangle]
pub unsafe extern "C" fn ws_query_errno(rs: *mut WS_RS) -> i32 {
    match (rs as *mut WsResultSet).as_ref() {
        Some(rs) => rs.errno(),
        None => 0,
    }
}
#[no_mangle]
pub unsafe extern "C" fn ws_query_errstr(rs: *mut WS_RS) -> *const c_char {
    match (rs as *mut WsResultSet).as_ref() {
        Some(rs) => rs.errstr(),
        None => EMPTY.as_ptr(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_affected_rows(rs: *const WS_RS) -> i32 {
    match (rs as *mut WsResultSet).as_ref() {
        Some(rs) => rs.affected_rows(),
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_num_of_fields(rs: *const WS_RS) -> i32 {
    match (rs as *mut WsResultSet).as_ref() {
        Some(rs) => rs.num_of_fields(),
        None => 0,
    }
}
#[no_mangle]
pub unsafe extern "C" fn ws_fetch_fields(rs: *mut WS_RS) -> *const WS_FIELD {
    match (rs as *mut WsResultSet).as_mut() {
        Some(rs) => rs.get_fields(),
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_fetch_block(
    rs: *mut WS_RS,
    ptr: *mut *const c_void,
    rows: *mut i32,
) -> i32 {
    match (rs as *mut WsResultSet).as_mut() {
        Some(rs) => rs.fetch_block(ptr, rows),
        None => {
            *rows = 0;
            0
        }
    }
}
#[no_mangle]
pub unsafe extern "C" fn ws_free_result(rs: *mut WS_RS) {
    let _ = Box::from_raw(rs as *mut WsResultSet);
}

#[no_mangle]
pub unsafe extern "C" fn ws_result_precision(rs: *const WS_RS) -> i32 {
    match (rs as *mut WsResultSet).as_mut() {
        Some(rs) => rs.precision() as i32,
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_get_value_in_block(
    rs: *mut WS_RS,
    row: i32,
    col: i32,
    ty: *mut u8,
    len: *mut u32,
) -> *const c_void {
    match (rs as *mut WsResultSet).as_mut() {
        Some(rs) => {
            let value = rs.get_raw_value(row as _, col as _);
            *ty = value.0 as u8;
            *len = value.1 as _;
            value.2
        }
        None => {
            *ty = Ty::Null as u8;
            *len = 0;
            std::ptr::null()
        }
    }
}

#[no_mangle]
pub unsafe fn ws_print_row(rs: *mut WS_RS, row: i32) {
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
mod tests {
    use std::{io::Read, num};

    use super::*;

    fn init() {
        std::sync::ONCE_INIT.call_once(|| {
            pretty_env_logger::init();
            std::env::set_var("RUST_DEBUG", "debug");
        });
    }

    #[test]
    fn dsn_error() {
        init();
        unsafe {
            let taos = ws_connect_with_dsn(b"ws://localhost:10\0" as *const u8 as _);
            let code = ws_connect_errno(taos);
            assert!(code != 0);
            let str = ws_connect_errstr(taos);
            dbg!(CStr::from_ptr(str));
        }
    }
    #[test]
    fn connect() {
        init();
        unsafe {
            let taos = ws_connect_with_dsn(b"ws://localhost:6041\0" as *const u8 as _);
            let code = ws_connect_errno(taos);
            assert!(code == 0);

            let sql = b"show databases\0" as *const u8 as _;
            let rs = ws_query(taos, sql);

            let code = ws_query_errno(rs);
            assert!(code == 0);

            let affected_rows = ws_affected_rows(rs);
            assert!(affected_rows == 0);

            let num_of_fields = ws_num_of_fields(rs);
            dbg!(num_of_fields);
            assert!(num_of_fields == 21);
            let fields = ws_fetch_fields(rs);

            for field in std::slice::from_raw_parts(fields, num_of_fields as usize) {
                dbg!(field);
            }

            let mut block: *const c_void = std::ptr::null();
            let mut rows = 0;
            let code = ws_fetch_block(rs, &mut block as *mut *const c_void, &mut rows as _);
            assert_eq!(code, 0);

            dbg!(rows);
            for row in 0..rows {
                for col in 0..num_of_fields {
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
