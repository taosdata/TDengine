use std::cell::RefCell;
use std::ffi::{c_char, c_int, CStr, CString};
use std::ptr;

use taos_error::Code;
use tracing::debug;

use crate::ws::TAOS_RES;

#[no_mangle]
pub unsafe extern "C" fn taos_errno(res: *mut TAOS_RES) -> c_int {
    debug!("taos_errno, res: {res:?}");
    if res.is_null() {
        return errno();
    }
    match (res as *mut TaosMaybeError<()>)
        .as_ref()
        .and_then(TaosMaybeError::errno)
    {
        Some(errno) => format_errno(errno),
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_errstr(res: *mut TAOS_RES) -> *const c_char {
    debug!("taos_errstr, res: {res:?}");
    if res.is_null() {
        if errno() == 0 {
            return EMPTY.as_ptr();
        }
        return errstr();
    }
    match (res as *mut TaosMaybeError<()>)
        .as_ref()
        .and_then(TaosMaybeError::errstr)
    {
        Some(err) => err,
        None => EMPTY.as_ptr(),
    }
}

pub const EMPTY: &CStr = c"";
pub const MAX_ERRSTR_LEN: usize = 4096;

thread_local! {
    static ERRNO: RefCell<i32> = const { RefCell::new(0) };
    static ERRSTR: RefCell<[u8; MAX_ERRSTR_LEN]> = const { RefCell::new([0; MAX_ERRSTR_LEN]) };
}

pub fn set_err_and_get_code(err: TaosError) -> i32 {
    ERRNO.with(|errno| {
        let code: u32 = err.code.into();
        *errno.borrow_mut() = (code | 0x80000000) as i32;
    });

    ERRSTR.with(|errstr| {
        let bytes = err.message.into_bytes();
        let len = bytes.len().min(MAX_ERRSTR_LEN - 1);
        let mut errstr = errstr.borrow_mut();
        errstr[..len].copy_from_slice(&bytes[..len]);
        errstr[len] = 0;
    });

    errno()
}

pub fn errno() -> i32 {
    ERRNO.with(|errno| *errno.borrow())
}

pub fn errstr() -> *const c_char {
    ERRSTR.with(|errstr| errstr.borrow().as_ptr() as _)
}

pub fn format_errno(errno: i32) -> i32 {
    if errno >= 0 {
        return (errno as u32 | 0x80000000) as i32;
    }
    errno
}

pub fn clear_error_info() {
    ERRNO.with(|errno| {
        *errno.borrow_mut() = 0;
    });
    ERRSTR.with(|errstr| {
        errstr.borrow_mut()[0] = 0;
    });
}

pub fn clear_err_and_ret_succ() -> i32 {
    clear_error_info();
    0
}

#[derive(Debug)]
pub struct TaosMaybeError<T> {
    err: Option<TaosError>,
    data: *mut T,
    type_id: &'static str,
}

unsafe impl<T> Send for TaosMaybeError<T> {}
unsafe impl<T> Sync for TaosMaybeError<T> {}

impl<T> TaosMaybeError<T> {
    pub fn with_err(&mut self, err: Option<TaosError>) {
        self.err = err;
    }

    pub fn clear_err(&mut self) {
        self.err = None;
    }

    pub fn errno(&self) -> Option<i32> {
        self.err.as_ref().map(|err| err.code.into())
    }

    pub fn errstr(&self) -> Option<*const c_char> {
        self.err.as_ref().map(|err| err.message.as_ptr())
    }

    pub fn deref(&self) -> Option<&T> {
        unsafe { self.data.as_ref() }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn deref_mut(&self) -> Option<&mut T> {
        unsafe { self.data.as_mut() }
    }
}

impl<T> Drop for TaosMaybeError<T> {
    fn drop(&mut self) {
        if !self.data.is_null() {
            debug!(self.type_id, "drop MaybeError");
            let _ = unsafe { Box::from_raw(self.data) };
        }
    }
}

impl<T> From<T> for TaosMaybeError<T> {
    fn from(value: T) -> Self {
        Self {
            err: None,
            data: Box::into_raw(Box::new(value)),
            type_id: std::any::type_name::<T>(),
        }
    }
}

impl<T> From<Box<T>> for TaosMaybeError<T> {
    fn from(value: Box<T>) -> Self {
        Self {
            err: None,
            data: Box::into_raw(value),
            type_id: std::any::type_name::<T>(),
        }
    }
}

impl<T, E> From<Result<T, E>> for TaosMaybeError<T>
where
    E: Into<TaosError>,
{
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(val) => Self {
                err: None,
                data: Box::into_raw(Box::new(val)),
                type_id: std::any::type_name::<T>(),
            },
            Err(err) => Self {
                err: Some(err.into()),
                data: ptr::null_mut(),
                type_id: std::any::type_name::<T>(),
            },
        }
    }
}

impl<T, E> From<Result<Box<T>, E>> for TaosMaybeError<T>
where
    E: Into<TaosError>,
{
    fn from(value: Result<Box<T>, E>) -> Self {
        match value {
            Ok(val) => Self {
                err: None,
                data: Box::into_raw(val),
                type_id: std::any::type_name::<T>(),
            },
            Err(err) => Self {
                err: Some(err.into()),
                data: ptr::null_mut(),
                type_id: std::any::type_name::<T>(),
            },
        }
    }
}

#[derive(Debug)]
pub struct TaosError {
    code: Code,
    message: CString,
    source: Option<Box<dyn std::error::Error>>,
}

impl TaosError {
    pub fn new(code: Code, message: &str) -> Self {
        Self {
            code,
            message: CString::new(message).unwrap(),
            source: None,
        }
    }

    pub fn code(&self) -> Code {
        self.code
    }
}

impl From<&TaosError> for TaosError {
    fn from(err: &TaosError) -> Self {
        Self {
            code: err.code,
            message: err.message.clone(),
            source: None,
        }
    }
}

impl std::fmt::Display for TaosError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:#06X}] {}", self.code, self.message.to_str().unwrap())
    }
}

impl std::error::Error for TaosError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref())
    }
}

impl From<Box<dyn std::error::Error>> for TaosError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(err.to_string()).unwrap(),
            source: Some(err),
        }
    }
}

impl From<std::str::Utf8Error> for TaosError {
    fn from(err: std::str::Utf8Error) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(err.to_string()).unwrap(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<std::string::FromUtf8Error> for TaosError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(err.to_string()).unwrap(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<taos_query::DsnError> for TaosError {
    fn from(err: taos_query::DsnError) -> Self {
        use taos_ws::query::asyn::WS_ERROR_NO;
        Self {
            code: WS_ERROR_NO::DSN_ERROR.as_code(),
            message: CString::new(err.to_string()).unwrap(),
            source: None,
        }
    }
}

impl From<taos_query::common::SmlDataBuilderError> for TaosError {
    fn from(err: taos_query::common::SmlDataBuilderError) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(err.to_string()).unwrap(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<taos_ws::Error> for TaosError {
    fn from(e: taos_ws::Error) -> Self {
        Self {
            code: e.errno(),
            message: CString::new(e.errstr()).unwrap(),
            source: None,
        }
    }
}

impl From<taos_ws::query::Error> for TaosError {
    fn from(err: taos_ws::query::Error) -> Self {
        Self {
            code: err.errno(),
            message: CString::new(err.errstr()).unwrap(),
            source: None,
        }
    }
}

impl From<taos_error::Error> for TaosError {
    fn from(err: taos_error::Error) -> Self {
        Self {
            code: err.code(),
            message: CString::new(err.to_string()).unwrap(),
            source: None,
        }
    }
}

impl From<serde_json::Error> for TaosError {
    fn from(err: serde_json::Error) -> Self {
        Self {
            code: Code::FAILED,
            message: CString::new(err.to_string()).unwrap(),
            source: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;

    #[test]
    fn test_set_err_and_get_code() {
        unsafe {
            let err = TaosError::new(Code::SUCCESS, "test error");
            let code = set_err_and_get_code(err);
            assert_eq!(code as u32, 0x80000000);

            let code = taos_errno(ptr::null_mut());
            assert_eq!(code as u32, 0x80000000);

            let errstr_ptr = taos_errstr(ptr::null_mut());
            let errstr = CStr::from_ptr(errstr_ptr);
            assert_eq!(errstr, c"test error");
        }
    }

    #[test]
    fn test_taos_error_from_serde_json_error() {
        let invalid_json = "{ invalid json }";
        let err = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
        let taos_err = TaosError::from(err);
        assert_eq!(taos_err.code, Code::FAILED);
    }
}
