use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::sync::OnceLock;
use std::time::Duration;
use std::{ptr, slice};

use taos_error::Code;
use taos_query::common::{Precision, Ty};
use taos_query::util::{generate_req_id, hex, InlineBytes, InlineStr};
use taos_query::{
    block_in_place_or_global, global_tokio_runtime, Fetchable, Queryable, RawBlock as Block,
};
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::{debug, error, Instrument};

use crate::ws::error::{
    clear_err_and_ret_succ, format_errno, set_err_and_get_code, TaosError, TaosMaybeError,
};
use crate::ws::{
    config, ResultSet, ResultSetOperations, Row, SafePtr, TaosResult, __taos_async_fn_t, TAOS,
    TAOS_FIELD, TAOS_FIELD_E, TAOS_RES, TAOS_ROW,
};

pub const TSDB_DATA_TYPE_NULL: usize = 0;
pub const TSDB_DATA_TYPE_BOOL: usize = 1;
pub const TSDB_DATA_TYPE_TINYINT: usize = 2;
pub const TSDB_DATA_TYPE_SMALLINT: usize = 3;
pub const TSDB_DATA_TYPE_INT: usize = 4;
pub const TSDB_DATA_TYPE_BIGINT: usize = 5;
pub const TSDB_DATA_TYPE_FLOAT: usize = 6;
pub const TSDB_DATA_TYPE_DOUBLE: usize = 7;
pub const TSDB_DATA_TYPE_VARCHAR: usize = 8;
pub const TSDB_DATA_TYPE_TIMESTAMP: usize = 9;
pub const TSDB_DATA_TYPE_NCHAR: usize = 10;
pub const TSDB_DATA_TYPE_UTINYINT: usize = 11;
pub const TSDB_DATA_TYPE_USMALLINT: usize = 12;
pub const TSDB_DATA_TYPE_UINT: usize = 13;
pub const TSDB_DATA_TYPE_UBIGINT: usize = 14;
pub const TSDB_DATA_TYPE_JSON: usize = 15;
pub const TSDB_DATA_TYPE_VARBINARY: usize = 16;
pub const TSDB_DATA_TYPE_DECIMAL: usize = 17;
pub const TSDB_DATA_TYPE_BLOB: usize = 18;
pub const TSDB_DATA_TYPE_MEDIUMBLOB: usize = 19;
pub const TSDB_DATA_TYPE_BINARY: usize = TSDB_DATA_TYPE_VARCHAR;
pub const TSDB_DATA_TYPE_GEOMETRY: usize = 20;
pub const TSDB_DATA_TYPE_DECIMAL64: usize = 21;
pub const TSDB_DATA_TYPE_MAX: usize = 22;

#[allow(non_camel_case_types)]
pub type __taos_notify_fn_t = extern "C" fn(param: *mut c_void, ext: *mut c_void, r#type: c_int);

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct TAOS_VGROUP_HASH_INFO {
    pub vgId: i32,
    pub hashBegin: u32,
    pub hashEnd: u32,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct TAOS_DB_ROUTE_INFO {
    pub routeVersion: i32,
    pub hashPrefix: i16,
    pub hashSuffix: i16,
    pub hashMethod: i8,
    pub vgNum: i32,
    pub vgHash: *mut TAOS_VGROUP_HASH_INFO,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum TSDB_SERVER_STATUS {
    TSDB_SRV_STATUS_UNAVAILABLE = 0,
    TSDB_SRV_STATUS_NETWORK_OK = 1,
    TSDB_SRV_STATUS_SERVICE_OK = 2,
    TSDB_SRV_STATUS_SERVICE_DEGRADED = 3,
    TSDB_SRV_STATUS_EXTING = 4,
}

impl From<i32> for TSDB_SERVER_STATUS {
    fn from(value: i32) -> Self {
        match value {
            1 => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_NETWORK_OK,
            2 => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_SERVICE_OK,
            3 => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_SERVICE_DEGRADED,
            4 => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_EXTING,
            _ => TSDB_SERVER_STATUS::TSDB_SRV_STATUS_UNAVAILABLE,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_query(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES {
    taos_query_with_reqid(taos, sql, generate_req_id() as _)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_query_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    reqId: i64,
) -> *mut TAOS_RES {
    debug!("taos_query_with_reqid start, taos: {taos:?}, sql: {sql:?}, req_id: {reqId}");
    match query(taos, sql, reqId as u64) {
        Ok(rs) => {
            let res: TaosMaybeError<ResultSet> = rs.into();
            let res = Box::into_raw(Box::new(res)) as _;
            debug!("taos_query_with_reqid succ, res: {res:?}");
            res
        }
        Err(err) => {
            error!("taos_query_with_reqid failed, err: {err:?}");
            set_err_and_get_code(err);
            ptr::null_mut()
        }
    }
}

unsafe fn query(taos: *mut TAOS, sql: *const c_char, req_id: u64) -> TaosResult<ResultSet> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;
    let sql = CStr::from_ptr(sql).to_str()?;
    debug!("query, sql: {sql:?}");
    let rs = taos.query_with_req_id(sql, req_id)?;
    Ok(ResultSet::Query(QueryResultSet::new(rs)))
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_row(res: *mut TAOS_RES) -> TAOS_ROW {
    fn handle_error(code: Code, msg: &str) -> TAOS_ROW {
        error!("taos_fetch_row failed, code: {code:?}, msg: {msg}");
        set_err_and_get_code(TaosError::new(code, msg));
        ptr::null_mut()
    }

    debug!("taos_fetch_row start, res: {res:?}");

    let maybe_err = match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return handle_error(Code::INVALID_PARA, "res is null"),
    };

    let rs = match maybe_err.deref_mut() {
        Some(rs) => rs,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "res is invalid")));
            return handle_error(Code::INVALID_PARA, "res is invalid");
        }
    };

    match rs.fetch_row() {
        Ok(row) => {
            maybe_err.clear_err();
            debug!("taos_fetch_row succ, row: {row:?}");
            row
        }
        Err(err) => {
            let code = err.errno();
            let msg = err.errstr();
            maybe_err.with_err(Some(TaosError::new(code, &msg)));
            handle_error(code, &msg)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_result_precision(res: *mut TAOS_RES) -> c_int {
    debug!("taos_result_precision start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            debug!("taos_result_precision succ, rs: {rs:?}");
            rs.precision() as _
        }
        None => {
            error!("taos_result_precision failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_free_result(res: *mut TAOS_RES) {
    debug!("taos_free_result, res: {res:?}");
    if !res.is_null() {
        let _ = Box::from_raw(res as *mut TaosMaybeError<ResultSet>);
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_field_count(res: *mut TAOS_RES) -> c_int {
    debug!("taos_field_count start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!(rs=?rs, "taos_field_count done");
            debug!("taos_field_count succ, rs: {rs:?}");
            rs.num_of_fields()
        }
        None => {
            error!("taos_field_count failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_num_fields(res: *mut TAOS_RES) -> c_int {
    taos_field_count(res)
}

#[no_mangle]
pub unsafe extern "C" fn taos_affected_rows(res: *mut TAOS_RES) -> c_int {
    debug!("taos_affected_rows start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("taos_affected_rows succ, rs: {rs:?}");
            rs.affected_rows() as _
        }
        None => {
            error!("taos_affected_rows failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_affected_rows64(res: *mut TAOS_RES) -> i64 {
    debug!("taos_affected_rows64 start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("taos_affected_rows64 succ, rs: {rs:?}");
            rs.affected_rows64() as _
        }
        None => {
            error!("taos_affected_rows64 failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null")) as _
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_fields(res: *mut TAOS_RES) -> *mut TAOS_FIELD {
    debug!("taos_fetch_fields start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            debug!("taos_fetch_fields succ, rs: {rs:?}");
            rs.get_fields()
        }
        None => {
            error!("taos_fetch_fields failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_fields_e(res: *mut TAOS_RES) -> *mut TAOS_FIELD_E {
    debug!("taos_fetch_fields_e start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            debug!("taos_fetch_fields_e succ, rs: {rs:?}");
            rs.get_fields_e()
        }
        None => {
            error!("taos_fetch_fields_e failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_select_db(taos: *mut TAOS, db: *const c_char) -> c_int {
    debug!("taos_select_db start, taos: {taos:?}, db: {db:?}");

    let taos = match (taos as *mut Taos).as_mut() {
        Some(taos) => taos,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null")),
    };

    if db.is_null() {
        error!("taos_select_db failed, db is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "db is null"));
    }

    let db = match CStr::from_ptr(db).to_str() {
        Ok(db) => db,
        Err(_) => {
            error!("taos_select_db failed, db is invalid utf-8");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "db is invalid utf-8"));
        }
    };

    match taos.query(format!("use {db}")) {
        Ok(_) => {
            debug!("taos_select_db succ");
            0
        }
        Err(err) => {
            error!("taos_select_db failed, err: {err:?}");
            set_err_and_get_code(err.into())
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_print_row(
    str: *mut c_char,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    taos_print_row_with_size(str, i32::MAX as _, row, fields, num_fields)
}

#[no_mangle]
pub unsafe extern "C" fn taos_print_row_with_size(
    str: *mut c_char,
    size: u32,
    row: TAOS_ROW,
    fields: *mut TAOS_FIELD,
    num_fields: c_int,
) -> c_int {
    unsafe fn write_to_cstr(size: &mut usize, str: *mut c_char, content: &[u8]) -> i32 {
        if content.len() > *size {
            return -1;
        }
        let cstr = content.as_ptr() as *const c_char;
        ptr::copy_nonoverlapping(cstr, str, content.len());
        *size -= content.len();
        content.len() as _
    }

    debug!("taos_print_row_with_size start, str: {str:?}, size: {size}, row: {row:?}, fields: {fields:?}, num_fields: {num_fields}");

    if str.is_null() || row.is_null() || fields.is_null() || num_fields <= 0 {
        error!("taos_print_row_with_size failed, invalid params");
        return 0;
    }

    let row = slice::from_raw_parts(row, num_fields as usize);
    let fields = slice::from_raw_parts(fields, num_fields as usize);

    let mut len: usize = 0;
    let mut size = (size - 1) as usize;

    let mut int_buf = itoa::Buffer::new();
    let mut float_buf = ryu::Buffer::new();

    for i in 0..num_fields as usize {
        if i > 0 && size > 0 {
            *str.add(len) = ' ' as c_char;
            len += 1;
            size -= 1;
        }

        let write_len = if row[i].is_null() {
            write_to_cstr(&mut size, str.add(len), b"NULL")
        } else {
            macro_rules! read_and_write {
                ($ty:ty) => {{
                    let value = ptr::read_unaligned(row[i] as *const $ty);
                    write_to_cstr(
                        &mut size,
                        str.add(len as usize),
                        int_buf.format(value).as_bytes(),
                    )
                }};
            }

            match Ty::from(fields[i].r#type) {
                Ty::TinyInt => read_and_write!(i8),
                Ty::UTinyInt => read_and_write!(u8),
                Ty::SmallInt => read_and_write!(i16),
                Ty::USmallInt => read_and_write!(u16),
                Ty::Int => read_and_write!(i32),
                Ty::UInt => read_and_write!(u32),
                Ty::BigInt | Ty::Timestamp => read_and_write!(i64),
                Ty::UBigInt => read_and_write!(u64),
                Ty::Float => {
                    let value = ptr::read_unaligned(row[i] as *const f32);
                    write_to_cstr(
                        &mut size,
                        str.add(len),
                        float_buf.format_finite(value).as_bytes(),
                    )
                }
                Ty::Double => {
                    let value = ptr::read_unaligned(row[i] as *const f64);
                    write_to_cstr(
                        &mut size,
                        str.add(len),
                        float_buf.format_finite(value).as_bytes(),
                    )
                }
                Ty::Bool => {
                    let value = ptr::read_unaligned(row[i] as *const bool);
                    let b = if value { b"1" } else { b"0" };
                    write_to_cstr(&mut size, str.add(len), b)
                }
                Ty::VarBinary => {
                    let data = row[i].offset(-2) as *const InlineBytes;
                    let data = (*data).as_bytes();
                    let content = hex::slice_to_hex_upper_with_prefix(data);
                    write_to_cstr(&mut size, str.add(len), &content)
                }
                Ty::Blob => {
                    let data = row[i].offset(-4) as *const InlineBytes<u32>;
                    let data = (*data).as_bytes();
                    let content = hex::slice_to_hex_upper_with_prefix(data);
                    write_to_cstr(&mut size, str.add(len), &content)
                }
                Ty::Geometry => {
                    let data = row[i].offset(-2) as *const InlineBytes;
                    write_to_cstr(&mut size, str.add(len), (*data).as_bytes())
                }
                Ty::VarChar | Ty::NChar => {
                    let data = row[i].offset(-2) as *const InlineStr;
                    write_to_cstr(&mut size, str.add(len), (*data).as_bytes())
                }
                Ty::Decimal | Ty::Decimal64 => {
                    let data = CStr::from_ptr(row[i] as *mut c_char).to_str().unwrap();
                    write_to_cstr(&mut size, str.add(len), data.as_bytes())
                }
                _ => 0,
            }
        };

        if write_len == -1 {
            break;
        }
        len += write_len as usize;
    }

    *str.add(len) = 0;

    debug!("taos_print_row_with_size succ, str: {str:?}, len: {len}");

    len as _
}

#[no_mangle]
pub unsafe extern "C" fn taos_stop_query(res: *mut TAOS_RES) {
    debug!("taos_stop_query start, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        Some(rs) => {
            debug!("taos_stop_query succ, rs: {rs:?}");
            rs.stop_query();
        }
        None => {
            error!("taos_stop_query failed, res is null");
            let _ = set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"));
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_is_null(res: *mut TAOS_RES, row: i32, col: i32) -> bool {
    debug!("taos_is_null start, res: {res:?}, row: {row}, col: {col}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(ResultSet::Query(rs)) => match &rs.block {
            Some(block) => {
                let is_null = block.is_null(row as _, col as _);
                debug!("taos_is_null succ, is_null: {is_null}");
                is_null
            }
            None => true,
        },
        _ => true,
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_is_null_by_column(
    res: *mut TAOS_RES,
    columnIndex: c_int,
    result: *mut bool,
    rows: *mut c_int,
) -> c_int {
    debug!("taos_is_null_by_column start, res: {res:?}, column_index: {columnIndex}, result: {result:?}, rows: {rows:?}");

    if res.is_null() || result.is_null() || rows.is_null() || *rows <= 0 || columnIndex < 0 {
        error!("taos_is_null_by_column failed, invalid params");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid params"));
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    match maybe_err.deref_mut() {
        Some(rs) => match rs.is_null_by_column(columnIndex as usize, result, rows) {
            Ok(()) => {
                debug!("taos_is_null_by_column succ, column_index: {columnIndex}");
                maybe_err.clear_err();
                clear_err_and_ret_succ()
            }
            Err(err) => {
                error!("taos_is_null_by_column failed, err: {err:?}");
                let code = err.code().into();
                maybe_err.with_err(Some(err));
                format_errno(code)
            }
        },
        None => {
            error!("taos_is_null_by_column failed, res is invalid");
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "res is invalid")));
            format_errno(Code::INVALID_PARA.into())
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_is_update_query(res: *mut TAOS_RES) -> bool {
    debug!("taos_is_update_query, res: {res:?}");
    match (res as *mut TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => rs.num_of_fields() == 0,
        None => true,
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_block(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int {
    debug!("taos_fetch_block start, res: {res:?}, rows: {rows:?}");
    let mut num_of_rows = 0;
    let _ = taos_fetch_block_s(res, &mut num_of_rows, rows);
    debug!("taos_fetch_block succ, num_of_rows: {num_of_rows}");
    num_of_rows
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_fetch_block_s(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    rows: *mut TAOS_ROW,
) -> c_int {
    debug!("taos_fetch_block_s start, res: {res:?}, num_of_rows: {numOfRows:?}, rows: {rows:?}");

    if numOfRows.is_null() {
        error!("taos_fetch_block_s failed, num_of_rows is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "numOfRows is null"));
    }

    match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(maybe_err) => match maybe_err.deref_mut() {
            Some(rs) => match rs.fetch_block(rows, numOfRows) {
                Ok(()) => {
                    debug!("taos_fetch_block_s succ, num_of_rows: {}", *numOfRows);
                    0
                }
                Err(err) => {
                    error!("taos_fetch_block_s failed, err: {err:?}");
                    set_err_and_get_code(TaosError::new(err.errno(), &err.errstr()));
                    maybe_err.with_err(Some(TaosError::new(err.errno(), &err.errstr())));
                    err.errno().into()
                }
            },
            None => {
                error!("taos_fetch_block_s failed, res is invalid");
                set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is invalid"))
            }
        },
        None => {
            error!("taos_fetch_block_s failed, res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_fetch_raw_block(
    res: *mut TAOS_RES,
    numOfRows: *mut c_int,
    pData: *mut *mut c_void,
) -> c_int {
    unsafe fn handle_error(message: &str, rows: *mut i32) -> i32 {
        error!("taos_fetch_raw_block failed, message: {message}, rows: {rows:?}");
        *rows = 0;
        set_err_and_get_code(TaosError::new(Code::FAILED, message))
    }

    debug!(
        "taos_fetch_raw_block start, res: {res:?}, num_of_rows: {numOfRows:?}, p_data: {pData:?}"
    );

    match (res as *mut TaosMaybeError<ResultSet>).as_mut() {
        Some(maybe_err) => match maybe_err.deref_mut() {
            Some(rs) => match rs.fetch_raw_block(pData, numOfRows) {
                Ok(()) => {
                    debug!("taos_fetch_raw_block succ, rs: {rs:?}");
                    0
                }
                Err(err) => {
                    error!("taos_fetch_raw_block failed, err: {err:?}");
                    set_err_and_get_code(TaosError::new(err.errno(), &err.errstr()));
                    let code = err.errno();
                    maybe_err.with_err(Some(err.into()));
                    code.into()
                }
            },
            None => handle_error("res is invalid", numOfRows),
        },
        None => handle_error("res is null", numOfRows),
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_raw_block_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    debug!("taos_fetch_raw_block_a start, res: {res:?}, fp: {fp:?}, param: {param:?}");

    let cb = fp as *const ();
    if res.is_null() || cb.is_null() {
        error!("taos_fetch_raw_block_a failed, res or fp is null");
        if !cb.is_null() {
            let code = format_errno(Code::INVALID_PARA.into());
            fp(param, res, code);
        }
        return;
    }

    let res = SafePtr(res);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let res = res;
            let param = param;

            let maybe_err = (res.0 as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
            if maybe_err.deref_mut().is_none() {
                error!("taos_fetch_raw_block_a callback failed, res is invalid");
                maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "res is invalid")));
                let code = format_errno(Code::INVALID_PARA.into());
                fp(param.0, res.0, code);
                return;
            }

            let rs = maybe_err.deref_mut().unwrap();
            if let ResultSet::Query(qrs) = rs {
                match qrs.rs.fetch_raw_block() {
                    Ok(block) => {
                        debug!("taos_fetch_raw_block_a callback succ");
                        qrs.block = block;
                        fp(param.0, res.0, 0);
                    }
                    Err(err) => {
                        error!("taos_fetch_raw_block_a callback failed, err: {err:?}");
                        let code = format_errno(err.code().into());
                        fp(param.0, res.0, code);
                    }
                }
            } else {
                error!("taos_fetch_raw_block_a callback failed, rs is invalid");
                maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "rs is invalid")));
                let code = format_errno(Code::INVALID_PARA.into());
                fp(param.0, res.0, code);
            };
        }
        .in_current_span(),
    );

    debug!("taos_fetch_raw_block_a succ");
}

#[no_mangle]
pub unsafe extern "C" fn taos_result_block(res: *mut TAOS_RES) -> *mut TAOS_ROW {
    debug!("taos_result_block start, res: {res:?}");

    if res.is_null() {
        error!("taos_result_block failed, res is null");
        return ptr::null_mut();
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    if maybe_err.deref_mut().is_none() {
        error!("taos_result_block failed, res is invalid");
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "res is invalid")));
        return ptr::null_mut();
    }

    let rs = maybe_err.deref_mut().unwrap();
    if let ResultSet::Query(qrs) = rs {
        match qrs.fetch_row() {
            Ok(_) => {
                debug!("taos_result_block succ, ptr: {:?}", qrs.row_data_ptr_ptr);
                return qrs.row_data_ptr_ptr;
            }
            Err(err) => {
                error!("taos_result_block failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.errno(), &err.errstr())));
            }
        }
    } else {
        error!("taos_result_block failed, rs is invalid");
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "rs is invalid")));
    };

    ptr::null_mut()
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_get_column_data_offset(
    res: *mut TAOS_RES,
    columnIndex: c_int,
) -> *mut c_int {
    debug!("taos_get_column_data_offset start, res: {res:?}, column_index: {columnIndex}");

    if res.is_null() || columnIndex < 0 {
        error!("taos_get_column_data_offset failed, invalid params");
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid params"));
        return ptr::null_mut();
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    let rs = match maybe_err.deref_mut() {
        Some(rs) => rs,
        None => {
            error!("taos_get_column_data_offset failed, res is invalid");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is invalid"));
            return ptr::null_mut();
        }
    };

    match rs.get_column_data_offset(columnIndex as usize) {
        Ok(offsets) => {
            debug!("taos_get_column_data_offset succ, offsets: {offsets:?}");
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            offsets
        }
        Err(err) => {
            error!("taos_get_column_data_offset failed, err: {err:?}");
            maybe_err.with_err(Some(err));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_validate_sql(taos: *mut TAOS, sql: *const c_char) -> c_int {
    debug!("taos_validate_sql start, taos: {taos:?}, sql: {sql:?}");

    if taos.is_null() {
        error!("taos_validate_sql failed, taos is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
    }

    if sql.is_null() {
        error!("taos_validate_sql failed, sql is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "sql is null"));
    }

    let sql = match CStr::from_ptr(sql).to_str() {
        Ok(sql) => sql,
        Err(_) => {
            error!("taos_validate_sql failed, sql is invalid utf-8");
            return set_err_and_get_code(TaosError::new(
                Code::INVALID_PARA,
                "sql is invalid utf-8",
            ));
        }
    };

    debug!("taos_validate_sql, sql: {sql}");

    let taos = (taos as *mut Taos).as_mut().unwrap();
    if let Err(err) = taos_query::block_in_place_or_global(taos.client().validate_sql(sql)) {
        error!("taos_validate_sql failed, err: {err:?}");
        return set_err_and_get_code(err.into());
    }

    debug!("taos_validate_sql succ");
    0
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_lengths(res: *mut TAOS_RES) -> *mut c_int {
    debug!("taos_fetch_lengths start, res: {res:?}");

    if res.is_null() {
        error!("taos_fetch_lengths failed, res is null");
        return ptr::null_mut();
    }

    let maybe_err = (res as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
    let rs = match maybe_err.deref_mut() {
        Some(rs) => rs,
        None => {
            error!("taos_fetch_lengths failed, res is invalid");
            return ptr::null_mut();
        }
    };

    if let ResultSet::Query(rs) = rs {
        if let Some(block) = rs.block.as_ref() {
            let lengths = if rs.has_called_fetch_row {
                let mut lengths = Vec::with_capacity(block.ncols());
                for col in 0..block.ncols() {
                    tracing::debug!(
                        "taos_fetch_lengths, row: {:?}, col: {:?}",
                        rs.row.current_row - 1,
                        col
                    );
                    let (_, len, _) = block.get_raw_value_unchecked(rs.row.current_row - 1, col);
                    lengths.push(len as i32);
                }
                lengths
            } else {
                block
                    .schemas()
                    .iter()
                    .map(|schema| schema.len() as i32)
                    .collect::<Vec<_>>()
            };

            debug!("taos_fetch_lengths succ, lengths: {lengths:?}");

            if lengths.is_empty() {
                return ptr::null_mut();
            }

            rs.lengths = Some(lengths);
            return rs.lengths.as_ref().unwrap().as_ptr() as *mut _;
        }
        error!("taos_fetch_lengths failed, block is none");
    } else {
        error!("taos_fetch_lengths failed, rs is invalid");
    }

    ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn taos_get_server_info(taos: *mut TAOS) -> *const c_char {
    debug!("taos_get_server_info start, taos: {taos:?}");

    static SERVER_INFO: OnceLock<CString> = OnceLock::new();

    if taos.is_null() {
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
        return ptr::null();
    }

    let server_info = SERVER_INFO.get_or_init(|| {
        if let Some(taos) = (taos as *mut Taos).as_mut() {
            CString::new(taos.version().as_bytes()).unwrap()
        } else {
            CString::new("").unwrap()
        }
    });

    debug!("taos_get_server_info succ, server_info: {server_info:?}");

    server_info.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn taos_get_client_info() -> *const c_char {
    debug!("taos_get_client_info");
    static VERSION: OnceLock<CString> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            let version = env!("TD_VERSION");
            CString::new(version).unwrap()
        })
        .as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn taos_get_current_db(
    taos: *mut TAOS,
    database: *mut c_char,
    len: c_int,
    required: *mut c_int,
) -> c_int {
    debug!("taos_get_current_db start, taos: {taos:?}, database: {database:?}, len: {len}, required: {required:?}");

    if taos.is_null() {
        error!("taos_get_current_db failed, taos is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
    }

    let res = taos_query(taos, c"SELECT DATABASE()".as_ptr());
    if res.is_null() {
        error!("taos_get_current_db failed, query failed");
        return set_err_and_get_code(TaosError::new(Code::FAILED, "query failed"));
    }

    let mut rows = 0;
    let mut data = ptr::null_mut();
    let mut code = taos_fetch_raw_block(res, &mut rows, &mut data);
    if code != 0 {
        taos_free_result(res);
        error!("taos_get_current_db failed, err: fetch raw block failed, code: {code}");
        return code;
    }

    let mut len_actual = 0;
    let mut db = ptr::null();
    if let Some(rs) = (res as *mut TaosMaybeError<ResultSet>)
        .as_mut()
        .and_then(|rs| rs.deref_mut())
    {
        (_, len_actual, db) = rs.get_raw_value(0, 0);
    }

    if db.is_null() {
        taos_free_result(res);
        error!("taos_get_current_db failed, get db failed");
        return set_err_and_get_code(TaosError::new(Code::FAILED, "get db failed"));
    }

    if len_actual < len as u32 {
        ptr::copy_nonoverlapping(db as _, database, len_actual as _);
    } else {
        ptr::copy_nonoverlapping(db as _, database, len as _);
        *required = len_actual as _;
        code = -1;
    }

    taos_free_result(res);

    debug!("taos_get_current_db succ, database: {database:?}");

    code
}

#[no_mangle]
pub extern "C" fn taos_data_type(r#type: c_int) -> *const c_char {
    debug!("taos_data_type, type: {}", r#type);
    match Ty::from_u8_option(r#type as _) {
        Some(ty) => ty.tsdb_name(),
        None => ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_query_a(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    taos_query_a_with_reqid(taos, sql, fp, param, generate_req_id() as _);
}

#[no_mangle]
pub unsafe extern "C" fn taos_query_a_with_reqid(
    taos: *mut TAOS,
    sql: *const c_char,
    fp: __taos_async_fn_t,
    param: *mut c_void,
    reqid: i64,
) {
    debug!("taos_query_a_with_reqid start, taos: {taos:?}, sql: {sql:?}, reqid: {reqid}, fp: {fp:?}, param: {param:?}");

    let taos = SafePtr(taos);
    let sql = SafePtr(sql);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let taos = taos;
            let sql = sql;
            let param = param;

            let cb = fp as *const ();
            if taos.0.is_null() || sql.0.is_null() || cb.is_null() {
                if !cb.is_null() {
                    let code = format_errno(Code::INVALID_PARA.into());
                    fp(param.0, ptr::null_mut(), code);
                }
                return;
            }

            let sql = match CStr::from_ptr(sql.0).to_str() {
                Ok(sql) => sql,
                Err(_) => {
                    error!("taos_query_a_with_reqid failed, err: sql is invalid utf-8");
                    let code = format_errno(Code::INVALID_PARA.into());
                    fp(param.0, ptr::null_mut(), code);
                    return;
                }
            };

            let taos = (taos.0 as *mut Taos).as_mut().unwrap();

            match taos_query::AsyncQueryable::query_with_req_id(taos, sql, reqid as _).await {
                Ok(rs) => {
                    debug!("taos_query_a_with_reqid callback, result_set: {rs:?}");
                    let rs: TaosMaybeError<ResultSet> =
                        ResultSet::Query(QueryResultSet::new(rs)).into();
                    let res = Box::into_raw(Box::new(rs));
                    fp(param.0, res as _, 0);
                }
                Err(err) => {
                    // TODO: use TaosMaybeError to handle error
                    error!("taos_query_a_with_reqid failed, err: {err:?}");
                    let code = format_errno(err.code().into());
                    fp(param.0, ptr::null_mut(), code);
                }
            };
        }
        .in_current_span(),
    );

    debug!("taos_query_a_with_reqid succ");
}

#[no_mangle]
pub unsafe extern "C" fn taos_fetch_rows_a(
    res: *mut TAOS_RES,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) {
    debug!("taos_fetch_rows_a start, res: {res:?}, fp: {fp:?}, param: {param:?}");

    let res = SafePtr(res);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let res = res;
            let param = param;

            let cb = fp as *const ();
            if res.0.is_null() || cb.is_null() {
                if !cb.is_null() {
                    let code = format_errno(Code::INVALID_PARA.into());
                    fp(param.0, ptr::null_mut(), code);
                }
                return;
            }

            let maybe_err = (res.0 as *mut TaosMaybeError<ResultSet>).as_mut().unwrap();
            if maybe_err.deref_mut().is_none() {
                let code = format_errno(Code::INVALID_PARA.into());
                fp(param.0, ptr::null_mut(), code);
                return;
            }

            let rs = maybe_err.deref_mut().unwrap();
            let rows = if let ResultSet::Query(rs) = rs {
                match rs.fetch() {
                    Ok(rows) => rows,
                    Err(err) => {
                        let code = format_errno(err.errno().into());
                        fp(param.0, ptr::null_mut(), code);
                        return;
                    }
                }
            } else {
                let code = format_errno(Code::INVALID_PARA.into());
                fp(param.0, ptr::null_mut(), code);
                return;
            };

            fp(param.0, res.0, rows as _);
        }
        .in_current_span(),
    );

    debug!("taos_fetch_rows_a succ");
}

#[no_mangle]
pub unsafe extern "C" fn taos_get_raw_block(res: *mut TAOS_RES) -> *const c_void {
    debug!("taos_get_raw_block start, res: {res:?}");
    let mut num_of_rows = 0;
    let mut data = ptr::null_mut();
    taos_fetch_raw_block(res, &mut num_of_rows, &mut data);
    debug!("taos_get_raw_block succ, data: {data:?}");
    data
}

#[no_mangle]
pub unsafe extern "C" fn taos_check_server_status(
    fqdn: *const c_char,
    mut port: i32,
    details: *mut c_char,
    maxlen: i32,
) -> TSDB_SERVER_STATUS {
    debug!("taos_check_server_status start, fqdn: {fqdn:?}, port: {port}, details: {details:?}, maxlen: {maxlen}");

    let fqdn = if !fqdn.is_null() {
        match CStr::from_ptr(fqdn).to_str() {
            Ok(fqdn) => Some(fqdn.to_string().into()),
            Err(_) => {
                error!("taos_check_server_status failed, fqdn is invalid utf-8");
                set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "fqdn is invalid utf-8"));
                return TSDB_SERVER_STATUS::TSDB_SRV_STATUS_UNAVAILABLE;
            }
        }
    } else {
        config::fqdn()
    };

    if port == 0 {
        port = config::server_port() as _;
    }

    debug!("taos_check_server_status, fqdn: {fqdn:?}, port: {port}");

    let addr = config::adapter_list()
        .map_or_else(|| "localhost:6041".to_string(), |addr| addr.to_string());

    let dsn = format!("ws://{addr}");

    debug!("taos_check_server_status, dsn: {dsn}");

    let (status, ds) =
        match block_in_place_or_global(taos_ws::query::check_server_status(&dsn, fqdn, port)) {
            Ok(res) => res,
            Err(err) => {
                error!("taos_check_server_status failed, err: {err:?}");
                set_err_and_get_code(err.into());
                return TSDB_SERVER_STATUS::TSDB_SRV_STATUS_UNAVAILABLE;
            }
        };

    debug!("taos_check_server_status succ, status: {status}, details: {ds:?}");

    let len = ds.len().min(maxlen as usize);
    ptr::copy_nonoverlapping(ds.as_ptr() as _, details, len);
    status.into()
}

#[derive(Debug)]
pub struct QueryResultSet {
    rs: taos_ws::ResultSet,
    block: Option<Block>,
    fields: Vec<TAOS_FIELD>,
    fields_e: Vec<TAOS_FIELD_E>,
    row: Row,
    #[allow(dead_code)]
    row_data_ptr: TAOS_ROW,
    row_data_ptr_ptr: *mut TAOS_ROW,
    offsets: Option<Vec<i32>>,
    lengths: Option<Vec<i32>>,
    has_called_fetch_row: bool,
    rows: Vec<*const c_void>,
}

impl Drop for QueryResultSet {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.row_data_ptr_ptr);
        }
    }
}

impl QueryResultSet {
    pub fn new(rs: taos_ws::ResultSet) -> Self {
        let num_of_fields = rs.num_of_fields();
        let mut row = Row {
            data: vec![ptr::null(); num_of_fields],
            current_row: 0,
        };
        let row_data_ptr = row.data.as_mut_ptr() as _;
        let row_data_ptr_ptr = Box::into_raw(Box::new(row_data_ptr));

        Self {
            rs,
            block: None,
            fields: Vec::new(),
            fields_e: Vec::new(),
            row,
            row_data_ptr,
            row_data_ptr_ptr,
            offsets: None,
            lengths: None,
            has_called_fetch_row: false,
            rows: vec![ptr::null(); num_of_fields],
        }
    }

    pub fn fetch(&mut self) -> Result<usize, Error> {
        self.block = self.rs.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            self.row.current_row = 0;
            tracing::trace!("fetch new block with {} rows", block.nrows());
            return Ok(block.nrows());
        }
        tracing::trace!("fetch no more rows");
        Ok(0)
    }
}

impl ResultSetOperations for QueryResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        ptr::null()
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

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        if self.fields.len() != self.rs.num_of_fields() {
            self.fields.clear();
            self.fields
                .extend(self.rs.fields().iter().map(TAOS_FIELD::from));
        }
        self.fields.as_mut_ptr()
    }

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
        if self.fields_e.len() != self.rs.num_of_fields() {
            self.fields_e.clear();
            let precisions = self.rs.fields_precisions().unwrap();
            let scales = self.rs.fields_scales().unwrap();
            for (i, field) in self.rs.fields().iter().enumerate() {
                self.fields_e
                    .push(TAOS_FIELD_E::new(field, precisions[i] as _, scales[i] as _));
            }
        }
        self.fields_e.as_mut_ptr()
    }

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error> {
        self.block = self.rs.fetch_raw_block()?;
        if let Some(block) = self.block.as_ref() {
            *ptr = block.as_raw_bytes().as_ptr() as _;
            *rows = block.nrows() as _;
        } else {
            *rows = 0;
        }
        Ok(())
    }

    unsafe fn fetch_block(&mut self, rows: *mut TAOS_ROW, num: *mut c_int) -> Result<(), Error> {
        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.rs.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        if let Some(block) = self.block.as_ref() {
            if block.nrows() > 0 {
                for (i, col) in block.columns().enumerate() {
                    self.rows[i] = col.as_raw_ptr();
                }
                self.row.current_row = block.nrows();
                *rows = self.rows.as_ptr() as _;
                *num = block.nrows() as _;
            }
        }

        Ok(())
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        if !self.has_called_fetch_row {
            self.has_called_fetch_row = true;
        }

        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.rs.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        if let Some(block) = self.block.as_ref() {
            if block.nrows() == 0 {
                return Ok(ptr::null_mut());
            }

            for col in 0..block.ncols() {
                let res = block.get_raw_value_unchecked(self.row.current_row, col);
                self.row.data[col] = res.2;
            }

            self.row.current_row += 1;
            Ok(self.row.data.as_ptr() as _)
        } else {
            Ok(ptr::null_mut())
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        if let Some(block) = self.block.as_ref() {
            if row < block.nrows() && col < block.ncols() {
                return block.get_raw_value_unchecked(row, col);
            }
        }
        (Ty::Null, 0, ptr::null())
    }

    fn take_timing(&mut self) -> Duration {
        self.rs.take_timing()
    }

    fn stop_query(&mut self) {
        taos_query::block_in_place_or_global(self.rs.stop());
    }

    unsafe fn is_null_by_column(
        &mut self,
        column_index: usize,
        result: *mut bool,
        rows: *mut c_int,
    ) -> Result<(), TaosError> {
        let block = self.block.as_ref().ok_or_else(|| {
            error!("query is_null_by_column failed, block is none");
            TaosError::new(Code::FAILED, "block is none")
        })?;

        let ncols = block.ncols();
        if ncols == 0 || column_index >= ncols {
            error!("query is_null_by_column failed, column_index:{column_index} is invalid, columns_num: {ncols}");
            return Err(TaosError::new(
                Code::INVALID_PARA,
                "column index is invalid",
            ));
        }

        let is_nulls = block.is_null_by_col_unchecked(*rows as _, column_index);
        debug!("query is_null_by_column, column_index: {column_index}, is_nulls: {is_nulls:?}");
        *rows = is_nulls.len() as _;
        let res = slice::from_raw_parts_mut(result, is_nulls.len());
        for (i, is_null) in is_nulls.iter().enumerate() {
            res[i] = *is_null;
        }
        Ok(())
    }

    fn get_column_data_offset(&mut self, column_index: usize) -> Result<*mut c_int, TaosError> {
        let block = self.block.as_ref().ok_or_else(|| {
            error!("query get_column_data_offset failed, block is none");
            TaosError::new(Code::FAILED, "block is none")
        })?;

        let ncols = block.ncols();
        if ncols == 0 || column_index >= ncols {
            error!("query get_column_data_offset failed, column_index:{column_index} is invalid, columns_num: {ncols}");
            return Err(TaosError::new(
                Code::INVALID_PARA,
                "column index is invalid",
            ));
        }

        let offsets = block.get_col_data_offset_unchecked(column_index);
        debug!("query get_column_data_offset, column_index: {column_index}, offsets: {offsets:?}");
        if offsets.is_empty() {
            return Ok(ptr::null_mut());
        }
        self.offsets = Some(offsets);
        Ok(self.offsets.as_mut().unwrap().as_mut_ptr())
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::{ptr, vec};

    use taos_query::common::Precision;

    use super::*;
    use crate::ws::error::{clear_error_info, taos_errno, taos_errstr};
    use crate::ws::{taos_close, taos_connect, taos_init, test_connect, test_exec, test_exec_many};

    #[test]
    fn test_taos_query() {
        let taos = test_connect();
        test_exec_many(
            taos,
            &[
                "drop database if exists test_1737102397",
                "create database test_1737102397",
                "use test_1737102397",
                "create table t0 (ts timestamp, c1 int)",
                "insert into t0 values (now, 1)",
                "select * from t0",
                "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                "create table d0 using s0 tags(1)",
                "insert into d0 values (now, 1)",
                "select * from d0",
                "insert into d1 using s0 tags(2) values(now, 1)",
                "select * from d1",
                "select * from s0",
                "drop database test_1737102397",
            ],
        );

        unsafe { taos_close(taos) };
    }

    #[test]
    fn test_taos_fetch_row() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102398",
                    "create database test_1737102398",
                    "use test_1737102398",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102398");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_row_errno_from_res_after_global_clear() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1775734869",
                    "create database test_1775734869",
                    "use test_1775734869",
                ],
            );

            let line = c"m1,t=t1 c1=1i 1741153642000";
            let mut total_rows = 0;

            let res = crate::ws::sml::taos_schemaless_insert_raw(
                taos,
                line.as_ptr() as *mut c_char,
                line.to_bytes().len() as c_int,
                &mut total_rows,
                crate::ws::sml::TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as c_int,
                crate::ws::sml::TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as c_int,
            );

            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(row.is_null());

            let code_from_res = taos_errno(res);
            assert_ne!(code_from_res, 0);

            clear_error_info();
            assert_eq!(taos_errno(ptr::null_mut()), 0);

            let code_from_res_after_clear = taos_errno(res);
            assert_eq!(code_from_res_after_clear, code_from_res);

            taos_free_result(res);
            test_exec(taos, "drop database if exists test_1775734869");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_result_precision() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102399",
                    "create database test_1737102399",
                    "use test_1737102399",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let precision: Precision = taos_result_precision(res).into();
            assert_eq!(precision, Precision::Millisecond);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102399");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_field_count() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102400",
                    "create database test_1737102400",
                    "use test_1737102400",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let count = taos_field_count(res);
            assert_eq!(count, 2);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102400");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_num_fields() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102401",
                    "create database test_1737102401",
                    "use test_1737102401",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num = taos_num_fields(res);
            assert_eq!(num, 2);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102401");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_affected_rows() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102402",
                    "create database test_1737102402",
                    "use test_1737102402",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 0);

            taos_free_result(res);

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 1);

            taos_free_result(res);

            test_exec(taos, "drop database test_1737102402");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_affected_rows64() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102403",
                    "create database test_1737102403",
                    "use test_1737102403",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows64(res);
            assert_eq!(rows, 0);

            taos_free_result(res);

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let rows = taos_affected_rows(res);
            assert_eq!(rows, 1);

            taos_free_result(res);

            test_exec(taos, "drop database test_1737102403");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_fields() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102404",
                    "create database test_1737102404",
                    "use test_1737102404",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102404");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_select_db() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102405",
                    "create database test_1737102405",
                ],
            );

            let res = taos_select_db(taos, c"test_1737102405".as_ptr());
            assert_eq!(res, 0);

            test_exec_many(
                taos,
                &[
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "drop database test_1737102405",
                ],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_print_row() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102406",
                    "create database test_1737102406",
                    "use test_1737102406",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (1741660079228, 1)",
                    "insert into t0 values (1741660080229, 0)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(len, 15);
            assert_eq!(
                "1741660079228 1",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(len, 15);
            assert_eq!(
                "1741660080229 0",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102406");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741657731",
                    "create database test_1741657731",
                    "use test_1741657731",
                    "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                    c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                    c9 bigint unsigned, c10 float, c11 double, c12 varchar(20), c13 nchar(10), \
                    c14 varbinary(10), c15 decimal(10, 3), c16 decimal(38, 10))",
                    "insert into t0 values (1743557474107, true, 1, 1, 1, 1, 1, 1, 1, 1, 1.1, 1.1, \
                    'hello world', 'hello', 'hello', 12345.123, \
                    12345678901234567890.123456789)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 17);

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()),
                c"1743557474107 1 1 1 1 1 1 1 1 1 1.1 1.1 hello world hello \\x68656C6C6F 12345.123 12345678901234567890.1234567890"
             );

            taos_free_result(res);
            test_exec(taos, "drop database test_1741657731");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1753257608",
                    "create database test_1753257608",
                    "use test_1753257608",
                    "create table t0 (ts timestamp, c2 geometry(50))",
                    "insert into t0 values (1741660079228, 'POINT(1 1)')",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(len, 35);
            assert_eq!(
                &str[..len as _],
                &[
                    49, 55, 52, 49, 54, 54, 48, 48, 55, 57, 50, 50, 56, 32, 1, 1, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63
                ]
            );

            taos_free_result(res);
            test_exec(taos, "drop database test_1753257608");
            taos_close(taos);
        }
    }

    #[test]
    fn test_print_nchar() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1758525503",
                    "create database test_1758525503",
                    "use test_1758525503",
                    "create table t0 (ts timestamp, c1 nchar(50))",
                    "insert into t0 values (1741780784752, null)",
                    "insert into t0 values (1741780784753, 'hello, world')",
                    "insert into t0 values (1741780784754, '你好，世界')",
                    "insert into t0 values (1741780784755, '你好，世界 hello, world 1234567890')",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut bytes = vec![0 as c_char; 1024];
            let len = taos_print_row(bytes.as_mut_ptr(), row, fields, num_fields);
            let str = CStr::from_ptr(bytes.as_ptr()).to_str().unwrap();
            assert_eq!(str, "1741780784752 NULL");

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut bytes = vec![0 as c_char; 1024];
            let len = taos_print_row(bytes.as_mut_ptr(), row, fields, num_fields);
            let str = CStr::from_ptr(bytes.as_ptr()).to_str().unwrap();
            assert_eq!(str, "1741780784753 hello, world");

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut bytes = vec![0 as c_char; 1024];
            let len = taos_print_row(bytes.as_mut_ptr(), row, fields, num_fields);
            let str = CStr::from_ptr(bytes.as_ptr()).to_str().unwrap();
            assert_eq!(str, "1741780784754 你好，世界");

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut bytes = vec![0 as c_char; 1024];
            let len = taos_print_row(bytes.as_mut_ptr(), row, fields, num_fields);
            let str = CStr::from_ptr(bytes.as_ptr()).to_str().unwrap();
            assert_eq!(str, "1741780784755 你好，世界 hello, world 1234567890");

            taos_free_result(res);
            test_exec(taos, "drop database test_1758525503");
            taos_close(taos);
        }
    }

    #[test]
    fn test_write_to_cstr() {
        unsafe fn write_to_cstr(size: &mut usize, str: *mut c_char, content: &str) -> i32 {
            if content.len() > *size {
                return -1;
            }
            let cstr = content.as_ptr() as *const c_char;
            ptr::copy_nonoverlapping(cstr, str, content.len());
            *size -= content.len();
            content.len() as _
        }

        unsafe {
            let mut len = 0;
            let mut size = 1024;
            let mut str = vec![0 as c_char; size];
            let length = write_to_cstr(&mut size, str.as_mut_ptr().add(len), "hello world");
            assert_eq!(length, 11);
            assert_eq!(
                "hello world",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );

            len += length as usize;
            let length = write_to_cstr(&mut size, str.as_mut_ptr().add(len), "hello\0world");
            assert_eq!(length, 11);
            assert_eq!(
                "hello worldhello",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );
        }
    }

    #[test]
    fn test_taos_stop_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102407",
                    "create database test_1737102407",
                    "use test_1737102407",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            taos_stop_query(res);
            let errno = taos_errno(ptr::null_mut());
            assert_eq!(errno, 0);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102407");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_is_null() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102408",
                    "create database test_1737102408",
                    "use test_1737102408",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let is_null = taos_is_null(res, 0, 0);
            assert!(is_null);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let is_null = taos_is_null(res, 0, 0);
            assert!(!is_null);

            let is_null = taos_is_null(ptr::null_mut(), 0, 0);
            assert!(is_null);

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102408");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_is_update_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102409",
                    "create database test_1737102409",
                    "use test_1737102409",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"insert into t0 values (now, 2)".as_ptr());
            assert!(!res.is_null());

            let is_update = taos_is_update_query(res);
            assert!(is_update);

            taos_free_result(res);

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let is_update = taos_is_update_query(res);
            assert!(!is_update);

            taos_free_result(res);

            let is_update = taos_is_update_query(ptr::null_mut());
            assert!(is_update);

            test_exec(taos, "drop database test_1737102409");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_raw_block() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102410",
                    "create database test_1737102410",
                    "use test_1737102410",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 1);
            assert!(!data.is_null());

            taos_free_result(res);
            test_exec(taos, "drop database test_1737102410");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_server_info() {
        unsafe {
            let taos = test_connect();
            let server_info = taos_get_server_info(taos);
            assert!(!server_info.is_null());
            let server_info = CStr::from_ptr(server_info).to_str().unwrap();
            println!("server_info: {server_info}");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_client_info_default() {
        unsafe {
            let client_info = taos_get_client_info();
            assert!(!client_info.is_null());
            let client_info = CStr::from_ptr(client_info).to_str().unwrap();
            assert_eq!(client_info, env!("CARGO_PKG_VERSION"));
        }
    }

    #[test]
    #[ignore]
    fn test_taos_get_client_info_injected() {
        unsafe {
            let client_info = taos_get_client_info();
            assert!(!client_info.is_null());
            let client_info = CStr::from_ptr(client_info).to_str().unwrap();
            assert_eq!(client_info, "3.9.9.9");
        }
    }

    #[test]
    fn test_taos_get_current_db() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737102411",
                    "create database test_1737102411",
                ],
            );
            taos_close(taos);

            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                c"test_1737102411".as_ptr(),
                6041,
            );
            assert!(!taos.is_null());

            let mut db = vec![0 as c_char; 1024];
            let mut required = 0;
            let code = taos_get_current_db(taos, db.as_mut_ptr(), db.len() as _, &mut required);
            assert_eq!(code, 0);
            println!("db: {:?}", CStr::from_ptr(db.as_ptr()));

            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_current_db_without_db() {
        unsafe {
            let taos = test_connect();
            let mut db = vec![0 as c_char; 1024];
            let mut required = 0;
            let code = taos_get_current_db(taos, db.as_mut_ptr(), db.len() as _, &mut required);
            assert!(code != 0);
            let errstr = taos_errstr(ptr::null_mut());
            println!("errstr: {:?}", CStr::from_ptr(errstr));
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_data_type() {
        unsafe {
            let type_null_ptr = taos_data_type(0);
            let type_null = CStr::from_ptr(type_null_ptr);
            assert_eq!(type_null, c"TSDB_DATA_TYPE_NULL");

            let type_geo_ptr = taos_data_type(20);
            let type_geo = CStr::from_ptr(type_geo_ptr);
            assert_eq!(type_geo, c"TSDB_DATA_TYPE_GEOMETRY");

            let type_invalid = taos_data_type(100);
            assert_eq!(type_invalid, ptr::null(),);
        }
    }

    #[test]
    fn test_taos_is_null_by_column() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740644681",
                    "create database test_1740644681",
                    "use test_1740644681",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now+1s, 1)",
                    "insert into t0 values (now+2s, null)",
                    "insert into t0 values (now+3s, 2)",
                    "insert into t0 values (now+4s, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 4);
            assert!(!data.is_null());

            let mut rows = 100;
            let mut result = vec![false; rows as _];
            let code = taos_is_null_by_column(res, 1, result.as_mut_ptr(), &mut rows);
            assert_eq!(code, 0);
            assert_eq!(rows, 4);

            assert!(!result[0]);
            assert!(result[1]);
            assert!(!result[2]);
            assert!(result[3]);

            taos_free_result(res);

            test_exec(taos, "drop database test_1740644681");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_query_a() {
        unsafe {
            extern "C" fn cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    assert_eq!(CStr::from_ptr(param as _), c"hello, world");
                    assert!(!res.is_null());

                    let row = taos_fetch_row(res);
                    assert!(!row.is_null());

                    let fields = taos_fetch_fields(res);
                    assert!(!fields.is_null());

                    let num_fields = taos_num_fields(res);
                    assert_eq!(num_fields, 2);

                    let mut str = vec![0 as c_char; 1024];
                    let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                    assert!(len > 0);
                    println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

                    taos_free_result(res);
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740664844",
                    "create database test_1740664844",
                    "use test_1740664844",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let sql = c"select * from t0";
            let param = c"hello, world";
            taos_query_a(taos, sql.as_ptr(), cb, param.as_ptr() as _);

            sleep(Duration::from_secs(1));

            test_exec(taos, "drop database test_1740664844");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_rows_a() {
        unsafe {
            extern "C" fn fetch_rows_cb(taos: *mut c_void, res: *mut TAOS_RES, num_of_row: c_int) {
                unsafe {
                    println!("fetch_rows_cb, num_of_row: {}", num_of_row);
                    let num_fields = taos_num_fields(res);
                    let fields = taos_fetch_fields(res);
                    if num_of_row > 0 {
                        assert_eq!(num_of_row, 4);
                        for i in 0..num_of_row {
                            let row = taos_fetch_row(res);
                            let mut str = vec![0 as c_char; 1024];
                            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);
                        }
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        println!("fetch_rows_cb, no more data");
                        taos_free_result(res);
                    }
                }
            }

            extern "C" fn query_cb(taos: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    println!("query_cb");
                    if code == 0 && !res.is_null() {
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        taos_free_result(res);
                    }
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740731669",
                    "create database test_1740731669",
                    "use test_1740731669",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "insert into t0 values (now+1s, 2)",
                    "insert into t0 values (now+2s, 3)",
                    "insert into t0 values (now+3s, 4)",
                ],
            );

            let sql = c"select * from t0";
            taos_query_a(taos, sql.as_ptr(), query_cb, taos);

            sleep(Duration::from_secs(1));

            test_exec(taos, "drop database test_1740731669");
            taos_close(taos);
        }
    }

    #[test]
    #[ignore]
    fn test_taos_fetch_rows_a_() {
        unsafe {
            extern "C" fn fetch_rows_cb(taos: *mut c_void, res: *mut TAOS_RES, num_of_row: c_int) {
                unsafe {
                    println!("fetch_rows_cb, num_of_row: {}", num_of_row);
                    let num_fields = taos_num_fields(res);
                    let fields = taos_fetch_fields(res);
                    if num_of_row > 0 {
                        for i in 0..num_of_row {
                            let row = taos_fetch_row(res);
                            let mut str = vec![0 as c_char; 1024];
                            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);
                        }
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        println!("fetch_rows_cb, no more data");
                        taos_free_result(res);
                    }
                }
            }

            extern "C" fn query_cb(taos: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    println!("query_cb");
                    if code == 0 && !res.is_null() {
                        taos_fetch_rows_a(res, fetch_rows_cb, taos);
                    } else {
                        taos_free_result(res);
                    }
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740732937",
                    "create database test_1740732937",
                    "use test_1740732937",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let num = 7000;
            for i in 0..num {
                let sql = format!("insert into t0 values ({}, {})", ts + i, i);
                test_exec(taos, &sql);
            }

            let sql = c"select * from t0";
            taos_query_a(taos, sql.as_ptr(), query_cb, taos);

            sleep(Duration::from_secs(5));

            test_exec(taos, "drop database test_1740732937");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_rows_a_large_batch() {
        #[repr(C)]
        struct Param {
            rows: usize,
            done_sender: flume::Sender<()>,
        }

        unsafe {
            taos_init();
            let taos = test_connect();

            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let num = 10000;
            let mut sql = "insert into t0 values ".to_string();
            for i in 0..num {
                sql.push_str(&format!("({}, {}) ", ts + i, i));
            }

            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1751610087",
                    "create database test_1751610087",
                    "use test_1751610087",
                    "create table t0(ts timestamp, c1 int)",
                    &sql,
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());

            extern "C" fn cb(param: *mut c_void, res: *mut TAOS_RES, num_of_rows: c_int) {
                unsafe {
                    let para = (param as *mut Param).as_mut().unwrap();
                    tracing::trace!("cb, num_of_rows: {}, rows: {}", num_of_rows, para.rows);
                    if num_of_rows > 0 {
                        para.rows += num_of_rows as usize;
                        taos_fetch_rows_a(res, cb, param);
                    } else if num_of_rows < 0 {
                        let errstr = taos_errstr(res);
                        tracing::trace!("failed to fetch rows: {:?}", CStr::from_ptr(errstr));
                    } else {
                        tracing::trace!("no more data, total rows fetched: {}", para.rows);
                        let _ = para.done_sender.send(());
                    }
                }
            }

            let (done_tx, done_rx) = flume::bounded(1);

            let mut param = Param {
                rows: 0,
                done_sender: done_tx,
            };

            taos_fetch_rows_a(res, cb, &mut param as *mut _ as *mut _);

            let _ = done_rx.recv();
            assert_eq!(param.rows, num as usize);

            taos_free_result(res);
            test_exec(taos, "drop database test_1751610087");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_column_data_offset() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740785939",
                    "create database test_1740785939",
                    "use test_1740785939",
                    "create table t0 (ts timestamp, c1 varchar(20), c2 nchar(20), c3 varbinary(20), c4 geometry(50))",
                    "insert into t0 values (now+1s, 'hello', 'hello', 'hello', 'POINT(1.0 1.0)')",
                    "insert into t0 values (now+2s, 'world', 'world', 'world', 'POINT(2.0 2.0)')",
                    "insert into t0 values (now+3s, null, null, null, null)",
                    "insert into t0 values (now+4s, 'hello, world', 'hello, world', 'hello, world', 'POINT(3.0 3.0)')",
                    "insert into t0 values (now+5s, null, null, null, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 5);
            assert!(!data.is_null());

            let offset = taos_get_column_data_offset(res, 0);
            assert!(offset.is_null());

            let offset = taos_get_column_data_offset(res, 1);
            assert!(!offset.is_null());

            let offsets = slice::from_raw_parts(offset, rows as _);
            assert_eq!(offsets, [0, 7, -1, 14, -1]);

            let offset = taos_get_column_data_offset(res, 2);
            assert!(!offset.is_null());

            // TODO: confirm the offsets
            let offsets = slice::from_raw_parts(offset, rows as _);
            assert_eq!(offsets, [0, 22, -1, 44, -1]);

            let offset = taos_get_column_data_offset(res, 3);
            assert!(!offset.is_null());

            let offsets = slice::from_raw_parts(offset, rows as _);
            assert_eq!(offsets, [0, 7, -1, 14, -1]);

            let offset = taos_get_column_data_offset(res, 4);
            assert!(!offset.is_null());

            let offsets = slice::from_raw_parts(offset, rows as _);
            assert_eq!(offsets, [0, 23, -1, 46, -1]);

            taos_free_result(res);

            test_exec(taos, "drop database test_1740785939");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_lengths() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740838806",
                    "create database test_1740838806",
                    "use test_1740838806",
                    "create table t0 (ts timestamp, c1 bool, c2 int, c3 varchar(10), c4 nchar(15))",
                    "insert into t0 values (now, 1, 2025, 'hello', 'helloworld')",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 1);
            assert!(!data.is_null());

            let lengths = taos_fetch_lengths(res);
            assert!(!lengths.is_null());

            // TODO: confirm the lengths
            let lengths = slice::from_raw_parts(lengths, 5);
            assert_eq!(lengths, [8, 1, 4, 12, 62]);

            taos_free_result(res);

            test_exec(taos, "drop database test_1740838806");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740841972",
                    "create database test_1740841972",
                    "use test_1740841972",
                    "create table t0 (ts timestamp, c1 bool, c2 int, c3 varchar(10), c4 nchar(15))",
                    "insert into t0 values (now, 1, 2025, 'hello', 'helloworld')",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let mut rows = 0;
            let mut data = ptr::null_mut();
            let code = taos_fetch_raw_block(res, &mut rows, &mut data);
            assert_eq!(code, 0);
            assert_eq!(rows, 1);
            assert!(!data.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let lengths = taos_fetch_lengths(res);
            assert!(!lengths.is_null());

            let lengths = slice::from_raw_parts(lengths, 5);
            assert_eq!(lengths, [8, 1, 4, 5, 10]);

            taos_free_result(res);

            test_exec(taos, "drop database test_1740841972");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_validate_sql() {
        unsafe {
            let taos = test_connect();
            let sql = c"create database if not exists test_1741339814";
            let code = taos_validate_sql(taos, sql.as_ptr());
            assert_eq!(code, 0);
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_raw_block_a() {
        unsafe {
            extern "C" fn cb(param: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    assert!(!res.is_null());
                    assert!(!param.is_null());
                    assert_eq!(CStr::from_ptr(param as _), c"hello");

                    let row = taos_result_block(res);
                    assert!(!row.is_null());

                    let fields = taos_fetch_fields(res);
                    assert!(!fields.is_null());

                    let num_fields = taos_num_fields(res);
                    assert_eq!(num_fields, 2);

                    let mut str = vec![0 as c_char; 1024];
                    let len = taos_print_row(str.as_mut_ptr(), *row, fields, num_fields);
                    assert!(len > 0);
                    println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741443150",
                    "create database test_1741443150",
                    "use test_1741443150",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let param = c"hello";
            taos_fetch_raw_block_a(res, cb, param.as_ptr() as _);

            sleep(Duration::from_secs(1));

            taos_free_result(res);

            test_exec(taos, "drop database test_1741443150");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_get_raw_block() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741489408",
                    "create database test_1741489408",
                    "use test_1741489408",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 2025)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let block = taos_get_raw_block(res);
            assert!(!block.is_null());

            taos_free_result(res);

            test_exec(taos, "drop database test_1741489408");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_fetch_block_s() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741779708",
                    "create database test_1741779708",
                    "use test_1741779708",
                    "create table t0 (ts timestamp, c1 int, c2 varchar(20))",
                    "insert into t0 values (1741780784749, 2025, 'hello')",
                    "insert into t0 values (1741780784750, 999, null)",
                    "insert into t0 values (1741780784751, null, 'world')",
                    "insert into t0 values (1741780784752, null, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num_fields = taos_num_fields(res);
            let fields = taos_fetch_fields(res);
            let fields = slice::from_raw_parts(fields, num_fields as _);

            loop {
                let mut num_of_rows = 0;
                let mut rows = ptr::null_mut();
                let code = taos_fetch_block_s(res, &mut num_of_rows, &mut rows);
                assert_eq!(code, 0);
                println!("num_of_rows: {}", num_of_rows);

                if num_of_rows == 0 {
                    break;
                }

                let rows = slice::from_raw_parts(rows, num_fields as _);

                for (c, field) in fields.iter().enumerate() {
                    for r in 0..num_of_rows as usize {
                        match field.r#type as usize {
                            TSDB_DATA_TYPE_TIMESTAMP => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 8) as isize) as *mut i64;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_INT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 4) as isize) as *mut i32;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_VARCHAR => {
                                let offsets = taos_get_column_data_offset(res, c as _);
                                let offsets = slice::from_raw_parts(offsets, num_of_rows as _);
                                if offsets[r] == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(offsets[r] as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let val =
                                        rows[c].offset((offsets[r] + 2) as isize) as *mut c_char;
                                    let val = CStr::from_ptr(val).to_str().unwrap();
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            taos_free_result(res);
            test_exec(taos, "drop database test_1741779708");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741782821",
                    "create database test_1741782821",
                    "use test_1741782821",
                    "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                    c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                    c9 bigint unsigned, c10 float, c11 double, c12 varchar(10), c13 nchar(10), \
                    c14 varbinary(10), c15 geometry(50))",
                    "insert into t0 values (1741780784752, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.1, 1.1, \
                    'hello', 'hello', 'hello', 'POINT(1 1)')",
                    "insert into t0 values (1741780784753, null, null, null, null, null, null, \
                    null, null, null, null, null, null, null, null, null)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num_fields = taos_num_fields(res);
            let fields = taos_fetch_fields(res);
            let fields = slice::from_raw_parts(fields, num_fields as _);

            loop {
                let mut num_of_rows = 0;
                let mut rows = ptr::null_mut();
                let code = taos_fetch_block_s(res, &mut num_of_rows, &mut rows);
                assert_eq!(code, 0);
                println!("num_of_rows: {}", num_of_rows);

                if num_of_rows == 0 {
                    break;
                }

                let rows = slice::from_raw_parts(rows, num_fields as _);

                for (c, field) in fields.iter().enumerate() {
                    for r in 0..num_of_rows as usize {
                        match field.r#type as usize {
                            TSDB_DATA_TYPE_TIMESTAMP => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 8) as isize) as *mut i64;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_BOOL => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(r as isize) as *mut bool;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_TINYINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(r as isize) as *mut i8;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_SMALLINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(r as isize) as *mut i16;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_INT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 4) as isize) as *mut i32;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_BIGINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 4) as isize) as *mut i64;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_UTINYINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(r as isize) as *mut u8;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_USMALLINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(r as isize) as *mut u16;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_UINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 4) as isize) as *mut u32;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_UBIGINT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 4) as isize) as *mut u64;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_FLOAT => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 4) as isize) as *mut f32;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_DOUBLE => {
                                if taos_is_null(res, r as _, c as _) {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset((r * 8) as isize) as *mut f64;
                                    let val = ptr::read_unaligned(ptr);
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_VARCHAR => {
                                let offsets = taos_get_column_data_offset(res, c as _);
                                let offsets = slice::from_raw_parts(offsets, num_of_rows as _);
                                if offsets[r] == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(offsets[r] as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let val =
                                        rows[c].offset((offsets[r] + 2) as isize) as *mut c_char;
                                    let val = CStr::from_ptr(val).to_str().unwrap();
                                    println!("col: {}, row: {}, val: {}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_NCHAR => {
                                let offsets = taos_get_column_data_offset(res, c as _);
                                let offsets = slice::from_raw_parts(offsets, num_of_rows as _);
                                if offsets[r] == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(offsets[r] as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let mut bytes = Vec::with_capacity(len as _);
                                    for i in 0..(len / 4) as i32 {
                                        let val = rows[c].offset((offsets[r] + 2 + i * 4) as isize)
                                            as *mut u32;
                                        let val = ptr::read_unaligned(val);
                                        bytes.push(val);
                                    }
                                    println!("col: {}, row: {}, val: {:?}", c, r, bytes);
                                }
                            }
                            TSDB_DATA_TYPE_VARBINARY => {
                                let offsets = taos_get_column_data_offset(res, c as _);
                                let offsets = slice::from_raw_parts(offsets, num_of_rows as _);
                                if offsets[r] == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(offsets[r] as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let val = rows[c].offset((offsets[r] + 2) as isize) as *mut u8;
                                    let val = slice::from_raw_parts(val, len as usize);
                                    println!("col: {}, row: {}, val: {:?}", c, r, val);
                                }
                            }
                            TSDB_DATA_TYPE_GEOMETRY => {
                                let offsets = taos_get_column_data_offset(res, c as _);
                                let offsets = slice::from_raw_parts(offsets, num_of_rows as _);
                                if offsets[r] == -1 {
                                    println!("col: {}, row: {}, val: NULL", c, r);
                                } else {
                                    let ptr = rows[c].offset(offsets[r] as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let val = rows[c].offset((offsets[r] + 2) as isize) as *mut u8;
                                    let val = slice::from_raw_parts(val, len as usize);
                                    println!("col: {}, row: {}, val: {:?}", c, r, val);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            taos_free_result(res);
            test_exec(taos, "drop database test_1741782821");
            taos_close(taos);
        }
    }

    #[test]
    fn test_fetch_block_nchar() {
        fn nchar_bytes_to_string(bytes: &[u8]) -> String {
            bytes
                .chunks_exact(4)
                .filter_map(|chunk| {
                    let code = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                    std::char::from_u32(code)
                })
                .collect()
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1758526794",
                    "create database test_1758526794",
                    "use test_1758526794",
                    "create table t0 (ts timestamp, c1 nchar(50))",
                    "insert into t0 values (1741780784752, null)",
                    "insert into t0 values (1741780784753, 'hello, world')",
                    "insert into t0 values (1741780784754, '你好，世界')",
                    "insert into t0 values (1741780784755, '你好，世界 hello, world 1234567890')",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let num_fields = taos_num_fields(res);
            let fields = taos_fetch_fields(res);
            let fields = slice::from_raw_parts(fields, num_fields as _);

            let mut actual = Vec::with_capacity(8);
            let expected = vec![
                "1741780784752",
                "NULL",
                "1741780784753",
                "hello, world",
                "1741780784754",
                "你好，世界",
                "1741780784755",
                "你好，世界 hello, world 1234567890",
            ];

            loop {
                let mut num_of_rows = 0;
                let mut rows = ptr::null_mut();
                let code = taos_fetch_block_s(res, &mut num_of_rows, &mut rows);
                assert_eq!(code, 0);
                if num_of_rows == 0 {
                    break;
                }

                let rows = slice::from_raw_parts(rows, num_fields as _);

                for r in 0..num_of_rows as usize {
                    for (c, field) in fields.iter().enumerate() {
                        match field.r#type as usize {
                            TSDB_DATA_TYPE_TIMESTAMP => {
                                if taos_is_null(res, r as _, c as _) {
                                    actual.push("NULL".to_string());
                                } else {
                                    let ptr = rows[c].offset((r * 8) as isize) as *const i64;
                                    let val = ptr::read_unaligned(ptr);
                                    actual.push(val.to_string());
                                }
                            }
                            TSDB_DATA_TYPE_NCHAR => {
                                let offsets = taos_get_column_data_offset(res, c as _);
                                let offsets = slice::from_raw_parts(offsets, num_of_rows as _);
                                if offsets[r] == -1 {
                                    actual.push("NULL".to_string());
                                } else {
                                    let ptr = rows[c].offset(offsets[r] as isize) as *const i16;
                                    let len = ptr::read_unaligned(ptr);
                                    let ptr =
                                        rows[c].offset((offsets[r] + 2) as isize) as *const u8;
                                    let slice = slice::from_raw_parts(ptr, len as usize);
                                    let val = nchar_bytes_to_string(slice);
                                    actual.push(val);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            assert_eq!(actual, expected);

            taos_free_result(res);
            test_exec(taos, "drop database test_1758526794");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_check_server_status() {
        unsafe {
            let max_len = 20;
            let mut details = vec![0 as c_char; max_len];
            let fqdn = c"localhost";
            let status =
                taos_check_server_status(fqdn.as_ptr(), 6030, details.as_mut_ptr(), max_len as _);
            assert_eq!(status, TSDB_SERVER_STATUS::TSDB_SRV_STATUS_SERVICE_OK);
            let details = CStr::from_ptr(details.as_ptr());
            println!("status: {status:?}, details: {details:?}");
        }

        unsafe {
            let max_len = 20;
            let mut details = vec![0 as c_char; max_len];
            let status =
                taos_check_server_status(ptr::null(), 0, details.as_mut_ptr(), max_len as _);
            assert_eq!(status, TSDB_SERVER_STATUS::TSDB_SRV_STATUS_SERVICE_OK);
            let details = CStr::from_ptr(details.as_ptr());
            println!("status: {status:?}, details: {details:?}");
        }
    }

    #[test]
    fn test_taos_fetch_fields_e() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1743154970",
                    "create database test_1743154970",
                    "use test_1743154970",
                    "create table t0 (ts timestamp, c1 int, c2 decimal(10, 2), c3 decimal(38, 20))",
                    "insert into t0 values (now, 1, 1234.56, 1234567890.123456789)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields_e(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 4);

            let fields = slice::from_raw_parts(fields, num_fields as _);

            assert_eq!(fields[0].r#type, TSDB_DATA_TYPE_TIMESTAMP as i8);
            assert_eq!(fields[0].bytes, 8);

            assert_eq!(fields[1].r#type, TSDB_DATA_TYPE_INT as i8);
            assert_eq!(fields[1].bytes, 4);

            assert_eq!(fields[2].r#type, TSDB_DATA_TYPE_DECIMAL64 as i8);
            assert_eq!(fields[2].bytes, 8);
            assert_eq!(fields[2].precision, 10);
            assert_eq!(fields[2].scale, 2);

            assert_eq!(fields[3].r#type, TSDB_DATA_TYPE_DECIMAL as i8);
            assert_eq!(fields[3].bytes, 16);
            assert_eq!(fields[3].precision, 38);
            assert_eq!(fields[3].scale, 20);

            taos_free_result(res);
            test_exec(taos, "drop database test_1743154970");
            taos_close(taos);
        }
    }

    #[test]
    #[ignore]
    fn test_ipv6() {
        unsafe {
            let taos = taos_connect(
                c"[::1]".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                6041,
            );
            assert!(!taos.is_null());

            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1748918064",
                    "create database test_1748918064",
                    "use test_1748918064",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (1741660079228, 1)",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 2);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(len, 15);
            assert_eq!(
                "1741660079228 1",
                CStr::from_ptr(str.as_ptr()).to_str().unwrap()
            );

            taos_free_result(res);
            test_exec(taos, "drop database test_1748918064");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_blob() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1753149897",
                    "create database test_1753149897",
                    "use test_1753149897",
                    "create table t0 (ts timestamp, c1 int, c2 blob)",
                    "insert into t0 values (1741660079228, 1, NULL)",
                    "insert into t0 values (1741660079229, 2, '')",
                    "insert into t0 values (1741660079230, 3, 'hello')",
                    "insert into t0 values (1741660079231, 4, '\\x12345678')",
                ],
            );

            let res = taos_query(taos, c"select * from t0".as_ptr());
            assert!(!res.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 3);

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1741660079228 1 NULL",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1741660079229 2 \\x",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1741660079230 3 \\x68656C6C6F",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1741660079231 4 \\x12345678",
            );

            taos_free_result(res);
            test_exec(taos, "drop database test_1753149897");
            taos_close(taos);
        }
    }
}
