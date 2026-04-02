use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::{ptr, slice, str};

use byteorder::{ByteOrder, LittleEndian};
use taos_error::Code;
use taos_query::common::Ty;
use taos_query::stmt2::Stmt2Bindable;
use taos_query::util::generate_req_id;
use taos_query::{block_in_place_or_global, global_tokio_runtime};
use taos_ws::query::{BindType, Stmt2Field};
use taos_ws::{Stmt2, Taos};
use tracing::{debug, error, trace, Instrument};

use crate::ws::error::*;
use crate::ws::query::QueryResultSet;
use crate::ws::{ResultSet, SafePtr, TaosResult, __taos_async_fn_t, TAOS, TAOS_RES};

#[allow(non_camel_case_types)]
pub type TAOS_STMT2 = c_void;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub struct TAOS_STMT2_OPTION {
    pub reqid: i64,
    pub singleStbInsert: bool,
    pub singleTableBindOnce: bool,
    pub asyncExecFn: __taos_async_fn_t,
    pub userdata: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct TAOS_STMT2_BINDV {
    pub count: c_int,
    pub tbnames: *mut *mut c_char,
    pub tags: *mut *mut TAOS_STMT2_BIND,
    pub bind_cols: *mut *mut TAOS_STMT2_BIND,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_ALL {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub field_type: u8,
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_init(
    taos: *mut TAOS,
    option: *mut TAOS_STMT2_OPTION,
) -> *mut TAOS_STMT2 {
    debug!("taos_stmt2_init start, taos: {taos:?}, option: {option:?}");
    let taos_stmt2: TaosMaybeError<TaosStmt2> = match stmt2_init(taos, option) {
        Ok(taos_stmt2) => taos_stmt2.into(),
        Err(err) => {
            error!("taos_stmt2_init failed, err: {err:?}");
            set_err_and_get_code(err);
            return ptr::null_mut();
        }
    };
    debug!("taos_stmt2_init, taos_stmt2: {taos_stmt2:?}");
    let res = Box::into_raw(Box::new(taos_stmt2)) as _;
    debug!("taos_stmt2_init succ, res: {res:?}");
    res
}

unsafe fn stmt2_init(taos: *mut TAOS, option: *mut TAOS_STMT2_OPTION) -> TaosResult<TaosStmt2> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;

    let stmt2 = Stmt2::new(taos.client_cloned());

    let (req_id, single_stb_insert, single_table_bind_once, async_exec_fn, userdata) =
        match option.as_ref() {
            Some(option) => {
                let async_exec_fn = option.asyncExecFn as *const ();
                let async_exec_fn = if !async_exec_fn.is_null() {
                    Some(option.asyncExecFn)
                } else {
                    None
                };

                (
                    option.reqid as _,
                    option.singleStbInsert,
                    option.singleTableBindOnce,
                    async_exec_fn,
                    option.userdata,
                )
            }
            None => (generate_req_id(), false, false, None, ptr::null_mut()),
        };

    debug!("stmt2_init, req_id: 0x{req_id:x}, single_stb_insert: {single_stb_insert}, single_table_bind_once: {single_table_bind_once}, async_exec_fn: {async_exec_fn:?}, userdata: {userdata:?}");

    block_in_place_or_global(stmt2.init_with_options(
        req_id,
        single_stb_insert,
        single_table_bind_once,
    ))?;

    Ok(TaosStmt2::new(stmt2, async_exec_fn, userdata))
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_prepare(
    stmt: *mut TAOS_STMT2,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    debug!("taos_stmt2_prepare start, stmt: {stmt:?}, sql: {sql:?}, length: {length}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if sql.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "sql is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let sql = if length > 0 {
        // TODO: check utf-8
        str::from_utf8_unchecked(slice::from_raw_parts(sql as _, length as _))
    } else {
        match CStr::from_ptr(sql).to_str() {
            Ok(sql) => sql,
            Err(_) => {
                maybe_err.with_err(Some(TaosError::new(
                    Code::INVALID_PARA,
                    "sql is invalid utf-8",
                )));
                return format_errno(Code::INVALID_PARA.into());
            }
        }
    };

    debug!("taos_stmt2_prepare, sql: {sql}");

    match stmt2.prepare(sql) {
        Ok(_) => {
            debug!("taos_stmt2_prepare succ");
            maybe_err.clear_err();
            clear_err_and_ret_succ()
        }
        Err(err) => {
            error!("taos_stmt2_prepare failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            format_errno(err.code().into())
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_bind_param(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
) -> c_int {
    debug!("taos_stmt2_bind_param start, stmt: {stmt:?}, bindv: {bindv:?}, col_idx: {col_idx}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let bindv = match bindv.as_ref() {
        Some(bindv) => bindv,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "bindv is null")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let mut tag_cnt = 0;
    let mut col_cnt = 0;
    if stmt2.is_insert().unwrap() {
        for field in stmt2.fields().unwrap() {
            match field.bind_type {
                BindType::Tag => tag_cnt += 1,
                BindType::Column => col_cnt += 1,
                BindType::TableName => {}
            }
        }
    } else {
        col_cnt = stmt2.fields_count().unwrap();
    }

    let req_id = generate_req_id();
    let bytes = match bindv.to_bytes(req_id, stmt2.stmt_id(), tag_cnt, col_cnt) {
        Ok(bytes) => bytes,
        Err(err) => {
            error!("taos_stmt2_bind_param failed, err: {err:?}");
            let code = err.code();
            maybe_err.with_err(Some(err));
            return format_errno(code.into());
        }
    };

    trace!("taos_stmt2_bind_param, bind bytes: {bytes:?}");

    match block_in_place_or_global(stmt2.bind_bytes(req_id, bytes)) {
        Ok(_) => {
            debug!("taos_stmt2_bind_param succ");
            maybe_err.clear_err();
            0
        }
        Err(err) => {
            error!("taos_stmt2_bind_param failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            format_errno(err.code().into())
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_bind_param_a(
    stmt: *mut TAOS_STMT2,
    bindv: *mut TAOS_STMT2_BINDV,
    col_idx: i32,
    fp: __taos_async_fn_t,
    param: *mut c_void,
) -> c_int {
    0
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_exec(
    stmt: *mut TAOS_STMT2,
    affected_rows: *mut c_int,
) -> c_int {
    debug!("taos_stmt2_exec start, stmt: {stmt:?}, affected_rows: {affected_rows:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => taos_stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if taos_stmt2.async_exec_fn.is_none() {
        if affected_rows.is_null() {
            maybe_err.with_err(Some(TaosError::new(
                Code::INVALID_PARA,
                "affected_rows is null",
            )));
            return format_errno(Code::INVALID_PARA.into());
        }

        match taos_stmt2.stmt2.exec() {
            Ok(rows) => {
                *affected_rows = rows as _;
                debug!("taos_stmt2_exec succ, affected_rows: {rows}");
                maybe_err.clear_err();
                return clear_err_and_ret_succ();
            }
            Err(err) => {
                error!("taos_stmt2_exec failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                return format_errno(err.code().into());
            }
        }
    }

    global_tokio_runtime().spawn(
        async move {
            use taos_query::stmt2::Stmt2AsyncBindable;

            let taos_stmt2 = maybe_err.deref_mut().unwrap();
            let stmt2 = &mut taos_stmt2.stmt2;
            let async_exec_fn = taos_stmt2.async_exec_fn.unwrap();
            let userdata = SafePtr(taos_stmt2.userdata);

            // TODO: add lock
            if let Err(err) = Stmt2AsyncBindable::exec(stmt2).await {
                error!("async taos_stmt2_exec exec failed, err: {err:?}");
                maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                let code = format_errno(err.code().into());
                async_exec_fn(userdata.0, ptr::null_mut(), code);
                return;
            }

            match Stmt2AsyncBindable::result_set(stmt2).await {
                Ok(rs) => {
                    debug!("async taos_stmt2_exec callback, result_set: {rs:?}");
                    let rs: TaosMaybeError<ResultSet> =
                        ResultSet::Query(QueryResultSet::new(rs)).into();
                    let res = Box::into_raw(Box::new(rs));
                    maybe_err.clear_err();
                    async_exec_fn(userdata.0, res as _, 0);
                }
                Err(err) => {
                    error!("async taos_stmt2_exec result failed, err: {err:?}");
                    maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                    let code = format_errno(err.code().into());
                    async_exec_fn(userdata.0, ptr::null_mut(), code as _);
                }
            }
        }
        .in_current_span(),
    );

    debug!("async taos_stmt2_exec succ");
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_close(stmt: *mut TAOS_STMT2) -> c_int {
    debug!("taos_stmt2_close, stmt: {stmt:?}");
    if stmt.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
    }
    let _ = Box::from_raw(stmt as *mut TaosMaybeError<TaosStmt2>);
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_is_insert(stmt: *mut TAOS_STMT2, insert: *mut c_int) -> c_int {
    debug!("taos_stmt2_is_insert start, stmt: {stmt:?}, insert: {insert:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if insert.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "insert is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    match stmt2.is_insert() {
        Some(is_insert) => {
            *insert = is_insert as _;
            debug!("taos_stmt2_is_insert succ, is_insert: {is_insert}");
            maybe_err.clear_err();
            clear_err_and_ret_succ()
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt2_prepare is not called",
            )));
            format_errno(Code::FAILED.into())
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_get_fields(
    stmt: *mut TAOS_STMT2,
    count: *mut c_int,
    fields: *mut *mut TAOS_FIELD_ALL,
) -> c_int {
    debug!("taos_stmt2_get_fields start, stmt: {stmt:?}, count: {count:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => taos_stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if count.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "count is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt2.stmt2;
    if stmt2.is_insert().is_none() {
        maybe_err.with_err(Some(TaosError::new(
            Code::FAILED,
            "taos_stmt2_prepare is not called",
        )));
        return format_errno(Code::FAILED.into());
    }

    if stmt2.is_insert().unwrap() {
        let stmt2_fields = stmt2.fields().unwrap();
        let field_all: Vec<TAOS_FIELD_ALL> = stmt2_fields.iter().map(|f| f.into()).collect();
        if !field_all.is_empty() && !fields.is_null() {
            *fields = Box::into_raw(field_all.into_boxed_slice()) as _;
            taos_stmt2.fields_len = Some(stmt2_fields.len());
        }
        *count = stmt2_fields.len() as _;
    } else {
        *count = stmt2.fields_count().unwrap() as _;
    }

    debug!("taos_stmt2_get_fields succ, fields: {fields:?}, count: {count:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_free_fields(
    stmt: *mut TAOS_STMT2,
    fields: *mut TAOS_FIELD_ALL,
) {
    debug!("taos_stmt2_free_fields start, stmt: {stmt:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return;
        }
    };

    let taos_stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => taos_stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return;
        }
    };

    if taos_stmt2.fields_len.is_none() {
        maybe_err.with_err(Some(TaosError::new(
            Code::FAILED,
            "taos_stmt2_get_fields is not called",
        )));
        return;
    }

    let len = taos_stmt2.fields_len.unwrap();
    let fields = Vec::from_raw_parts(fields, len, len);

    debug!("taos_stmt2_free_fields succ, fields: {fields:?}");

    maybe_err.clear_err();
    clear_error_info();
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_result(stmt: *mut TAOS_STMT2) -> *mut TAOS_RES {
    debug!("taos_stmt2_result start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt2>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return ptr::null_mut();
        }
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt2) => &mut taos_stmt2.stmt2,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return ptr::null_mut();
        }
    };

    match stmt2.result_set() {
        Ok(rs) => {
            let rs: TaosMaybeError<ResultSet> = ResultSet::Query(QueryResultSet::new(rs)).into();
            debug!("taos_stmt2_result succ, result_set: {rs:?}");
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            Box::into_raw(Box::new(rs)) as _
        }
        Err(err) => {
            error!("taos_stmt2_result failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt2_error(stmt: *mut TAOS_STMT2) -> *mut c_char {
    taos_errstr(stmt) as _
}

#[derive(Debug)]
struct TaosStmt2 {
    stmt2: Stmt2,
    async_exec_fn: Option<__taos_async_fn_t>,
    userdata: *mut c_void,
    fields_len: Option<usize>,
}

impl TaosStmt2 {
    fn new(stmt2: Stmt2, async_exec_fn: Option<__taos_async_fn_t>, userdata: *mut c_void) -> Self {
        Self {
            stmt2,
            async_exec_fn,
            userdata,
            fields_len: None,
        }
    }
}

impl TAOS_STMT2_BIND {
    fn ty(&self) -> Ty {
        self.buffer_type.into()
    }
}

const REQ_ID_POS: usize = 0;
const STMT_ID_POS: usize = REQ_ID_POS + 8;
const ACTION_POS: usize = STMT_ID_POS + 8;
const VERSION_POS: usize = ACTION_POS + 8;
const COL_IDX_POS: usize = VERSION_POS + 2;
const HEADER_LEN: usize = COL_IDX_POS + 4;

const TOTAL_LENGTH_POS: usize = 0;
const TABLE_COUNT_POS: usize = TOTAL_LENGTH_POS + 4;
const TAG_COUNT_POS: usize = TABLE_COUNT_POS + 4;
const COL_COUNT_POS: usize = TAG_COUNT_POS + 4;
const TABLE_NAMES_OFFSET_POS: usize = COL_COUNT_POS + 4;
const TAGS_OFFSET_POS: usize = TABLE_NAMES_OFFSET_POS + 4;
const COLS_OFFSET_POS: usize = TAGS_OFFSET_POS + 4;
const DATA_POS: usize = COLS_OFFSET_POS + 4;

const ACTION: u64 = 9;
const VERSION: u16 = 1;
const COL_IDX: i32 = -1;

impl TAOS_STMT2_BINDV {
    fn to_bytes(
        &self,
        req_id: u64,
        stmt_id: u64,
        tag_cnt: usize,
        col_cnt: usize,
    ) -> TaosResult<Vec<u8>> {
        debug!("to_bytes, req_id: 0x{req_id:x}, stmt_id: {stmt_id}, tag_cnt: {tag_cnt}, col_cnt: {col_cnt}");

        let (tbname_buf_len, tbname_lens) = if !self.tbnames.is_null() {
            self.calc_tbname_lens()?
        } else {
            (0, vec![])
        };

        let (tag_buf_len, tag_lens, tag_total_lens, tag_buf_lens) = if !self.tags.is_null() {
            self.calc_tag_or_col_lens(self.tags, tag_cnt)?
        } else {
            (0, vec![], vec![], vec![])
        };

        let (col_buf_len, col_lens, col_total_lens, col_buf_lens) = if !self.bind_cols.is_null() {
            self.calc_tag_or_col_lens(self.bind_cols, col_cnt)?
        } else {
            (0, vec![], vec![], vec![])
        };

        let have_tbname = tbname_buf_len > 0;
        let have_tag = tag_buf_len > 0;
        let have_col = col_buf_len > 0;

        let tbname_total_len = tbname_lens.len() * 2 + tbname_buf_len;
        let tag_total_len = tag_lens.len() * 4 + tag_buf_len as usize;
        let col_total_len = col_lens.len() * 4 + col_buf_len as usize;
        let total_len = 28 + tbname_total_len + tag_total_len + col_total_len;

        trace!("to_bytes, tbname_buf_len: {tbname_buf_len}, tbname_lens: {tbname_lens:?}");
        trace!("to_bytes, tag_buf_len: {tag_buf_len}, tag_lens: {tag_lens:?}, tag_total_lens: {tag_total_lens:?}, tag_buf_lens: {tag_buf_lens:?}");
        trace!("to bytes, col_buf_len: {col_buf_len}, col_lens: {col_lens:?}, col_total_lens: {col_total_lens:?}, col_buf_lens: {col_buf_lens:?}");
        trace!("to bytes, tbname_total_len: {tbname_total_len}, tag_total_len: {tag_total_len}, col_total_len: {col_total_len}, total_len: {total_len}");

        let mut data = vec![0u8; HEADER_LEN + total_len];
        self.write_headers(&mut data, req_id, stmt_id);

        let bytes = &mut data[HEADER_LEN..];
        LittleEndian::write_u32(&mut bytes[TOTAL_LENGTH_POS..], total_len as _);
        LittleEndian::write_i32(&mut bytes[TABLE_COUNT_POS..], self.count);
        LittleEndian::write_i32(&mut bytes[TAG_COUNT_POS..], tag_cnt as _);
        LittleEndian::write_i32(&mut bytes[COL_COUNT_POS..], col_cnt as _);

        if have_tbname {
            LittleEndian::write_u32(&mut bytes[TABLE_NAMES_OFFSET_POS..], DATA_POS as _);
            self.write_tbnames(&mut bytes[DATA_POS..], &tbname_lens);
        }

        if have_tag {
            let tags_offset = DATA_POS + tbname_total_len;
            LittleEndian::write_u32(&mut bytes[TAGS_OFFSET_POS..], tags_offset as _);
            self.write_tags_or_cols(
                &mut bytes[tags_offset..],
                self.tags,
                tag_cnt,
                &tag_lens,
                &tag_total_lens,
                &tag_buf_lens,
            );
        }

        if have_col {
            let cols_offset = DATA_POS + tbname_total_len + tag_total_len;
            LittleEndian::write_u32(&mut bytes[COLS_OFFSET_POS..], cols_offset as _);
            self.write_tags_or_cols(
                &mut bytes[cols_offset..],
                self.bind_cols,
                col_cnt,
                &col_lens,
                &col_total_lens,
                &col_buf_lens,
            );
        }

        Ok(data)
    }

    fn calc_tbname_lens(&self) -> TaosResult<(usize, Vec<u16>)> {
        let mut len = 0;
        let mut lens = vec![0u16; self.count as usize];
        for i in 0..self.count as usize {
            let tbname_ptr = unsafe { self.tbnames.add(i).read() };
            if tbname_ptr.is_null() {
                return Err(TaosError::new(
                    Code::INVALID_PARA,
                    "stmt2 bind tbname is null",
                ));
            }
            lens[i] = unsafe { CStr::from_ptr(tbname_ptr).to_bytes_with_nul().len() } as _;
            len += lens[i] as usize;
        }
        Ok((len, lens))
    }

    #[allow(clippy::type_complexity)]
    fn calc_tag_or_col_lens(
        &self,
        binds_ptr: *mut *mut TAOS_STMT2_BIND,
        tc_cnt: usize,
    ) -> TaosResult<(u32, Vec<u32>, Vec<u32>, Vec<u32>)> {
        let cnt = self.count as usize;
        let mut len = 0;
        let mut data_lens = vec![0; cnt];
        let mut total_lens = vec![0; cnt * tc_cnt];
        let mut buf_lens = vec![0; cnt * tc_cnt];

        for i in 0..cnt {
            let bind_ptr = unsafe { binds_ptr.add(i).read() };
            if bind_ptr.is_null() {
                return Err(TaosError::new(
                    Code::INVALID_PARA,
                    "stmt2 bind tag or column is null",
                ));
            }

            for j in 0..tc_cnt {
                let bind = unsafe { bind_ptr.add(j).read() };
                let have_len = bind.ty().fixed_length() == 0;
                let buf_len = if self.check_tag_or_col_is_null(bind.is_null, bind.num as _) {
                    0
                } else if have_len {
                    if bind.length.is_null() {
                        return Err(TaosError::new(
                            Code::INVALID_PARA,
                            "stmt2 bind tag or column length is null",
                        ));
                    }

                    let lens = unsafe { slice::from_raw_parts(bind.length, bind.num as _) };
                    let is_null = bind.is_null.is_null();
                    let mut buf_len = 0;
                    for (k, len) in lens.iter().enumerate() {
                        if is_null || unsafe { bind.is_null.add(k).read() } == 0 {
                            buf_len += lens[k] as u32;
                        }
                    }
                    buf_len
                } else {
                    bind.num as u32 * bind.ty().fixed_length() as u32
                };

                let k = i * tc_cnt + j;
                buf_lens[k] = buf_len;
                total_lens[k] = self.calc_tag_or_col_header_len(bind.num as _, have_len) + buf_len;
                data_lens[i] += total_lens[k];
                len += total_lens[k];
            }
        }

        Ok((len, data_lens, total_lens, buf_lens))
    }

    fn check_tag_or_col_is_null(&self, is_null: *const c_char, len: usize) -> bool {
        if is_null.is_null() {
            return false;
        }
        let slice = unsafe { slice::from_raw_parts(is_null, len) };
        slice.iter().all(|&v| v == 1)
    }

    fn calc_tag_or_col_header_len(&self, num: u32, have_len: bool) -> u32 {
        let mut len = 17 + num;
        if have_len {
            len += num * 4;
        }
        len
    }

    fn write_headers(&self, bytes: &mut [u8], req_id: u64, stmt_id: u64) {
        LittleEndian::write_u64(&mut bytes[REQ_ID_POS..], req_id);
        LittleEndian::write_u64(&mut bytes[STMT_ID_POS..], stmt_id);
        LittleEndian::write_u64(&mut bytes[ACTION_POS..], ACTION);
        LittleEndian::write_u16(&mut bytes[VERSION_POS..], VERSION);
        LittleEndian::write_i32(&mut bytes[COL_IDX_POS..], COL_IDX);
    }

    fn write_tbnames(&self, bytes: &mut [u8], tbname_lens: &[u16]) {
        let mut offset = 0;
        for len in tbname_lens {
            LittleEndian::write_u16(&mut bytes[offset..], *len);
            offset += 2;
        }
        for i in 0..self.count as usize {
            let tbname_ptr = unsafe { self.tbnames.add(i).read() };
            let tbname = unsafe { CStr::from_ptr(tbname_ptr).to_bytes_with_nul() };
            bytes[offset..offset + tbname.len()].copy_from_slice(tbname);
            offset += tbname.len();
        }
    }

    fn write_tags_or_cols(
        &self,
        bytes: &mut [u8],
        binds_ptr: *mut *mut TAOS_STMT2_BIND,
        tc_cnt: usize,
        tc_lens: &[u32],
        tc_total_lens: &[u32],
        tc_buf_lens: &[u32],
    ) {
        let mut offset = 0;
        for len in tc_lens {
            LittleEndian::write_u32(&mut bytes[offset..], *len);
            offset += 4;
        }

        for i in 0..self.count as usize {
            let bind_ptr = unsafe { binds_ptr.add(i).read() };
            for j in 0..tc_cnt {
                let bind = unsafe { bind_ptr.add(j).read() };
                LittleEndian::write_u32(&mut bytes[offset..], tc_total_lens[i * tc_cnt + j]);
                offset += 4;
                LittleEndian::write_i32(&mut bytes[offset..], bind.ty() as _);
                offset += 4;
                LittleEndian::write_i32(&mut bytes[offset..], bind.num);
                offset += 4;

                if !bind.is_null.is_null() {
                    unsafe {
                        ptr::copy_nonoverlapping(
                            bind.is_null as *const u8,
                            bytes.as_mut_ptr().add(offset),
                            bind.num as _,
                        );
                    }
                }
                offset += bind.num as usize;

                let have_len = bind.ty().fixed_length() == 0;
                if have_len {
                    bytes[offset] = 1;
                }
                offset += 1;

                if have_len {
                    let cnt = bind.num as usize * 4;
                    unsafe {
                        ptr::copy_nonoverlapping(
                            bind.length as *const u8,
                            bytes.as_mut_ptr().add(offset),
                            cnt,
                        );
                    }
                    offset += cnt;
                }

                let buf_len = tc_buf_lens[i * tc_cnt + j];
                LittleEndian::write_u32(&mut bytes[offset..], buf_len);
                offset += 4;

                if !bind.buffer.is_null() {
                    unsafe {
                        ptr::copy_nonoverlapping(
                            bind.buffer as *const u8,
                            bytes.as_mut_ptr().add(offset),
                            buf_len as _,
                        );
                    }
                    offset += buf_len as usize;
                }
            }
        }
    }
}

impl From<&Stmt2Field> for TAOS_FIELD_ALL {
    fn from(field: &Stmt2Field) -> Self {
        let field_name = field.name.as_str();
        let mut name = [0 as c_char; 65];
        unsafe {
            ptr::copy_nonoverlapping(
                field_name.as_ptr(),
                name.as_mut_ptr() as _,
                field_name.len(),
            );
        };

        Self {
            name,
            r#type: field.field_type,
            precision: field.precision,
            scale: field.scale,
            bytes: field.bytes,
            field_type: field.bind_type as _,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;
    use std::sync::Arc;

    use futures::future::try_join_all;
    use rand::Rng;
    use taos_query::util::ws_proxy::{InterceptFn, ProxyAction, WsProxy};
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::ws::{query::*, taos_connect};
    use crate::ws::{taos_close, test_connect, test_exec, test_exec_many};

    macro_rules! new_bind {
        ($ty:expr, $buffer:ident, $length:ident, $is_null:ident) => {
            TAOS_STMT2_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                length: $length.as_mut_ptr(),
                is_null: $is_null.as_mut_ptr(),
                num: $is_null.len() as _,
            }
        };
    }

    macro_rules! new_bind_without_is_null {
        ($ty:expr, $buffer:ident, $length:ident) => {
            TAOS_STMT2_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                length: $length.as_mut_ptr(),
                is_null: ptr::null_mut(),
                num: $length.len() as _,
            }
        };
    }

    #[test]
    fn test_stmt2() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739274502",
                    "create database test_1739274502",
                    "use test_1739274502",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let len = sql.to_bytes().len();
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), len as _);
            assert_eq!(code, 0);

            let mut buffer = vec![1739763276172i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![2];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 1);

            let mut insert = 0;
            let code = taos_stmt2_is_insert(stmt2, &mut insert);
            assert_eq!(code, 0);
            assert_eq!(insert, 1);

            let mut count = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt2_get_fields(stmt2, &mut count, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(count, 2);
            assert!(!fields.is_null());

            taos_stmt2_free_fields(stmt2, fields);

            let error = taos_stmt2_error(stmt2);
            assert_eq!(CStr::from_ptr(error), c"");

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739274502");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_bind_param() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739502440",
                    "create database test_1739502440",
                    "use test_1739502440",
                    "create table s0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                    c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                    c9 bigint unsigned, c10 float, c11 double, c12 varchar(10), c13 nchar(10), \
                    c14 varbinary(10), c15 geometry(50)) \
                    tags (t1 timestamp, t2 bool, t3 tinyint, t4 smallint, t5 int, t6 bigint, \
                    t7 tinyint unsigned, t8 smallint unsigned, t9 int unsigned, \
                    t10 bigint unsigned, t11 float, t12 double, t13 varchar(10), t14 nchar(10), \
                    t15 varbinary(10), t16 geometry(50), t17 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql =
                c"insert into ? using s0 tags(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname1 = c"d0";
            let tbname2 = c"d1";
            let mut tbnames = vec![tbname1.as_ptr() as _, tbname2.as_ptr() as _];

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t1 = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![1i8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t2 = new_bind!(Ty::Bool, buffer, length, is_null);

            let mut buffer = vec![10i8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t3 = new_bind!(Ty::TinyInt, buffer, length, is_null);

            let mut buffer = vec![23i16];
            let mut length = vec![2];
            let mut is_null = vec![0];
            let t4 = new_bind!(Ty::SmallInt, buffer, length, is_null);

            let mut buffer = vec![479i32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t5 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut buffer = vec![1999i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t6 = new_bind!(Ty::BigInt, buffer, length, is_null);

            let mut buffer = vec![27u8];
            let mut length = vec![1];
            let mut is_null = vec![0];
            let t7 = new_bind!(Ty::UTinyInt, buffer, length, is_null);

            let mut buffer = vec![89u16];
            let mut length = vec![2];
            let mut is_null = vec![0];
            let t8 = new_bind!(Ty::USmallInt, buffer, length, is_null);

            let mut buffer = vec![234578u32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t9 = new_bind!(Ty::UInt, buffer, length, is_null);

            let mut buffer = vec![234578u64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t10 = new_bind!(Ty::UBigInt, buffer, length, is_null);

            let mut buffer = vec![1.23f32];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let t11 = new_bind!(Ty::Float, buffer, length, is_null);

            let mut buffer = vec![2345345.99f64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let t12 = new_bind!(Ty::Double, buffer, length, is_null);

            let mut buffer = vec![104u8, 101, 108, 108, 111];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t13 = new_bind!(Ty::VarChar, buffer, length, is_null);

            let mut buffer = vec![104u8, 101, 108, 108, 111];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t14 = new_bind!(Ty::NChar, buffer, length, is_null);

            let mut buffer = vec![118u8, 97, 114, 98, 105, 110, 97, 114, 121];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t15 = new_bind!(Ty::VarBinary, buffer, length, is_null);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t16 = new_bind!(Ty::Geometry, buffer, length, is_null);

            let mut is_null = vec![1];
            let t17 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: ptr::null_mut(),
                length: ptr::null_mut(),
                is_null: is_null.as_mut_ptr(),
                num: is_null.len() as _,
            };

            let mut tag1 = vec![
                t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17,
            ];
            let mut tag2 = tag1.clone();
            let mut tags = vec![tag1.as_mut_ptr(), tag2.as_mut_ptr()];

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![1i8, 1, 0, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c1 = new_bind!(Ty::Bool, buffer, length, is_null);

            let mut buffer = vec![23i8, 0, -23, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c2 = new_bind!(Ty::TinyInt, buffer, length, is_null);

            let mut buffer = vec![34i16, 0, -34, 0];
            let mut length = vec![2, 2, 2, 2];
            let mut is_null = vec![0, 0, 0, 1];
            let c3 = new_bind!(Ty::SmallInt, buffer, length, is_null);

            let mut buffer = vec![45i32, 46, -45, 0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c4 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut buffer = vec![56i64, 57, -56, 0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c5 = new_bind!(Ty::BigInt, buffer, length, is_null);

            let mut buffer = vec![67u8, 68, 67, 0];
            let mut length = vec![1, 1, 1, 1];
            let mut is_null = vec![0, 0, 0, 1];
            let c6 = new_bind!(Ty::UTinyInt, buffer, length, is_null);

            let mut buffer = vec![78u16, 79, 78, 0];
            let mut length = vec![2, 2, 2, 2];
            let mut is_null = vec![0, 0, 0, 1];
            let c7 = new_bind!(Ty::USmallInt, buffer, length, is_null);

            let mut buffer = vec![89u32, 90, 89, 0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c8 = new_bind!(Ty::UInt, buffer, length, is_null);

            let mut buffer = vec![100u64, 101, 100, 0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c9 = new_bind!(Ty::UBigInt, buffer, length, is_null);

            let mut buffer = vec![1.23f32, 1.24, -1.23, 0.0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c10 = new_bind!(Ty::Float, buffer, length, is_null);

            let mut buffer = vec![2345.67f64, 2345.68, -2345.67, 0.0];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 1];
            let c11 = new_bind!(Ty::Double, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c12 = new_bind!(Ty::VarChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c13 = new_bind!(Ty::NChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c14 = new_bind!(Ty::VarBinary, buffer, length, is_null);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![21, 0, 21, 0];
            let mut is_null = vec![0, 1, 0, 1];
            let c15 = new_bind!(Ty::Geometry, buffer, length, is_null);

            let mut col1 = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let ts = new_bind_without_is_null!(Ty::Timestamp, buffer, length);

            let mut buffer = vec![1i8, 1, 0, 0];
            let mut length = vec![1, 1, 1, 1];
            let c1 = new_bind_without_is_null!(Ty::Bool, buffer, length);

            let mut buffer = vec![23i8, 0, -23, 0];
            let mut length = vec![1, 1, 1, 1];
            let c2 = new_bind_without_is_null!(Ty::TinyInt, buffer, length);

            let mut buffer = vec![34i16, 0, -34, 0];
            let mut length = vec![2, 2, 2, 2];
            let c3 = new_bind_without_is_null!(Ty::SmallInt, buffer, length);

            let mut buffer = vec![45i32, 46, -45, 0];
            let mut length = vec![4, 4, 4, 4];
            let c4 = new_bind_without_is_null!(Ty::Int, buffer, length);

            let mut buffer = vec![56i64, 57, -56, 0];
            let mut length = vec![8, 8, 8, 8];
            let c5 = new_bind_without_is_null!(Ty::BigInt, buffer, length);

            let mut buffer = vec![67u8, 68, 67, 0];
            let mut length = vec![1, 1, 1, 1];
            let c6 = new_bind_without_is_null!(Ty::UTinyInt, buffer, length);

            let mut buffer = vec![78u16, 79, 78, 0];
            let mut length = vec![2, 2, 2, 2];
            let c7 = new_bind_without_is_null!(Ty::USmallInt, buffer, length);

            let mut buffer = vec![89u32, 90, 89, 0];
            let mut length = vec![4, 4, 4, 4];
            let c8 = new_bind_without_is_null!(Ty::UInt, buffer, length);

            let mut buffer = vec![100u64, 101, 100, 0];
            let mut length = vec![8, 8, 8, 8];
            let c9 = new_bind_without_is_null!(Ty::UBigInt, buffer, length);

            let mut buffer = vec![1.23f32, 1.24, -1.23, 0.0];
            let mut length = vec![4, 4, 4, 4];
            let c10 = new_bind_without_is_null!(Ty::Float, buffer, length);

            let mut buffer = vec![2345.67f64, 2345.68, -2345.67, 0.0];
            let mut length = vec![8, 8, 8, 8];
            let c11 = new_bind_without_is_null!(Ty::Double, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c12 = new_bind_without_is_null!(Ty::VarChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c13 = new_bind_without_is_null!(Ty::NChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c14 = new_bind_without_is_null!(Ty::VarBinary, buffer, length);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
                0, 0, 0, 0, 0, 0, 240, 63,
            ];
            let mut length = vec![21, 21, 21, 21];
            let c15 = new_bind_without_is_null!(Ty::Geometry, buffer, length);

            let mut col2 = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let mut cols = vec![col1.as_mut_ptr(), col2.as_mut_ptr()];

            let mut bindv = TAOS_STMT2_BINDV {
                count: tbnames.len() as _,
                tbnames: tbnames.as_mut_ptr(),
                tags: tags.as_mut_ptr(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 8);

            let sql = c"select * from s0 where c4 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c4 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c4];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 0);

            let res = taos_stmt2_result(stmt2);
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 33);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            taos_free_result(res);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739502440");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739966021",
                    "create database test_1739966021",
                    "use test_1739966021",
                    "create table s0 (ts timestamp, c1 int) tags (t1 json)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";
            let mut tbnames = vec![tbname.as_ptr() as _];

            let mut buffer = vec![
                123u8, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125,
            ];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t1 = new_bind!(Ty::Json, buffer, length, is_null);

            let mut tag = vec![t1];
            let mut tags = vec![tag.as_mut_ptr()];

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];

            let mut bindv = TAOS_STMT2_BINDV {
                count: tbnames.len() as _,
                tbnames: tbnames.as_mut_ptr(),
                tags: tags.as_mut_ptr(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 1);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739966021");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1754448810",
                    "create database test_1754448810",
                    "use test_1754448810",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";
            let mut tbnames = vec![tbname.as_ptr() as _];

            let mut buffer = vec![101];
            let t1 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: buffer.as_mut_ptr() as _,
                length: ptr::null_mut(),
                is_null: ptr::null_mut(),
                num: buffer.len() as _,
            };

            let mut tag = vec![t1];
            let mut tags = vec![tag.as_mut_ptr()];

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let ts = TAOS_STMT2_BIND {
                buffer_type: Ty::Timestamp as _,
                buffer: buffer.as_mut_ptr() as _,
                length: ptr::null_mut(),
                is_null: ptr::null_mut(),
                num: buffer.len() as _,
            };

            let mut buffer = vec![1, 2, 3, 4];
            let c1 = TAOS_STMT2_BIND {
                buffer_type: Ty::Int as _,
                buffer: buffer.as_mut_ptr() as _,
                length: ptr::null_mut(),
                is_null: ptr::null_mut(),
                num: buffer.len() as _,
            };

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];

            let mut bindv = TAOS_STMT2_BINDV {
                count: tbnames.len() as _,
                tbnames: tbnames.as_mut_ptr(),
                tags: tags.as_mut_ptr(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 4);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1754448810");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_exec_async() {
        unsafe {
            extern "C" fn fp(userdata: *mut c_void, res: *mut TAOS_RES, code: c_int) {
                unsafe {
                    assert_eq!(code, 0);
                    assert!(!res.is_null());

                    let userdata = CStr::from_ptr(userdata as _);
                    assert_eq!(userdata, c"hello, world");

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
                    "drop database if exists test_1739864837",
                    "create database test_1739864837",
                    "use test_1739864837",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 2)",
                ],
            );

            let userdata = c"hello, world";
            let mut option = TAOS_STMT2_OPTION {
                reqid: 1001,
                singleStbInsert: true,
                singleTableBindOnce: false,
                asyncExecFn: fp,
                userdata: userdata.as_ptr() as _,
            };
            let stmt2 = taos_stmt2_init(taos, &mut option);
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![1];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let code = taos_stmt2_exec(stmt2, ptr::null_mut());
            assert_eq!(code, 0);

            std::thread::sleep(std::time::Duration::from_secs(1));

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739864837");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_result() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1739876374",
                    "create database test_1739876374",
                    "use test_1739876374",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 1)",
                    "insert into t0 values(1739762261438, 99)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 0);

            let res = taos_stmt2_result(stmt2);
            assert!(!res.is_null());

            let affected_rows = taos_affected_rows(res);
            assert_eq!(affected_rows, 0);

            let precision = taos_result_precision(res);
            assert_eq!(precision, 0);

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

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert!(len > 0);
            println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

            taos_stop_query(res);

            taos_free_result(res);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1739876374");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_insert_and_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1757667641",
                    "create database test_1757667641",
                    "use test_1757667641",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![45i32, 46, -45, 0];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 1];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 4);

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![45];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut col = vec![c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 0);

            let res = taos_stmt2_result(stmt2);
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

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1757667641");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_stmt2_blob() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1753168041",
                    "create database test_1753168041",
                    "use test_1753168041",
                    "create table t0 (ts timestamp, c1 int, c2 blob)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into t0 values(?, ?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![
                1739521477831i64,
                1739521477832,
                1739521477833,
                1739521477834,
            ];
            let mut length = vec![8, 8, 8, 8];
            let mut is_null = vec![0, 0, 0, 0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![1, 2, 3, 4];
            let mut length = vec![4, 4, 4, 4];
            let mut is_null = vec![0, 0, 0, 0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut buffer = "hello".as_bytes().to_vec();
            buffer.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]);
            let mut length = vec![0, 0, 5, 4];
            let mut is_null = vec![1, 0, 0, 0];
            let c2 = new_bind!(Ty::Blob, buffer, length, is_null);

            let mut col = vec![ts, c1, c2];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 4);

            let sql = c"select * from t0 where ts > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![1739521477830i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut col = vec![ts];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: cols.len() as _,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
            assert_eq!(code, 0);

            let mut affected_rows = 0;
            let code = taos_stmt2_exec(stmt2, &mut affected_rows);
            assert_eq!(code, 0);
            assert_eq!(affected_rows, 0);

            let res = taos_stmt2_result(stmt2);
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
                "1739521477831 1 NULL",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1739521477832 2 \\x",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1739521477833 3 \\x68656C6C6F",
            );

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "1739521477834 4 \\x12345678",
            );

            taos_free_result(res);
            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database test_1753168041");
            taos_close(taos);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_conn() -> anyhow::Result<()> {
        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("stmt") {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                } else if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8818", "ws://localhost:6041/ws", intercept_fn).await;

        let n = 10;
        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            tasks.push(tokio::spawn(async move {
                unsafe {
                    let taos = taos_connect(
                        c"localhost".as_ptr(),
                        ptr::null(),
                        ptr::null(),
                        ptr::null(),
                        8818,
                    );
                    assert!(!taos.is_null());

                    let db = format!("test_{}", 1760161589 + i);

                    test_exec_many(
                        taos,
                        &[
                            &format!("drop database if exists {db}"),
                            &format!("create database {db}"),
                            &format!("use {db}"),
                            "create table t0 (ts timestamp, c1 int)",
                        ],
                    );

                    let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
                    assert!(!stmt2.is_null());

                    let sql = CString::new(format!("insert into {db}.t0 values(?, ?)")).unwrap();
                    let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
                    assert_eq!(code, 0);

                    let mut buffer = vec![1739521477831i64];
                    let mut length = vec![8];
                    let mut is_null = vec![0];
                    let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

                    let mut buffer = vec![1i32];
                    let mut length = vec![4];
                    let mut is_null = vec![0];
                    let c1 = new_bind!(Ty::Int, buffer, length, is_null);

                    let mut col = vec![ts, c1];
                    let mut cols = vec![col.as_mut_ptr()];
                    let mut bindv = TAOS_STMT2_BINDV {
                        count: 1,
                        tbnames: ptr::null_mut(),
                        tags: ptr::null_mut(),
                        bind_cols: cols.as_mut_ptr(),
                    };

                    let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
                    assert_eq!(code, 0);

                    let mut affected_rows = 0;
                    let code = taos_stmt2_exec(stmt2, &mut affected_rows);
                    assert_eq!(code, 0);
                    assert_eq!(affected_rows, 1);

                    let sql = CString::new(format!("select * from {db}.t0 where c1 = ?")).unwrap();
                    let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
                    assert_eq!(code, 0);

                    let mut buffer = vec![1];
                    let mut length = vec![4];
                    let mut is_null = vec![0];
                    let c1 = new_bind!(Ty::Int, buffer, length, is_null);

                    let mut col = vec![c1];
                    let mut cols = vec![col.as_mut_ptr()];
                    let mut bindv = TAOS_STMT2_BINDV {
                        count: 1,
                        tbnames: ptr::null_mut(),
                        tags: ptr::null_mut(),
                        bind_cols: cols.as_mut_ptr(),
                    };

                    let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
                    assert_eq!(code, 0);

                    let mut affected_rows = 0;
                    let code = taos_stmt2_exec(stmt2, &mut affected_rows);
                    assert_eq!(code, 0);
                    assert_eq!(affected_rows, 0);

                    let code = taos_stmt2_close(stmt2);
                    assert_eq!(code, 0);

                    test_exec(taos, format!("drop database if exists {db}"));
                    taos_close(taos);
                }

                Ok::<_, anyhow::Error>(())
            }));
        }

        let results = try_join_all(tasks).await.unwrap();
        for res in results {
            res?;
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_stmt2() -> anyhow::Result<()> {
        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("stmt") {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                } else if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8819", "ws://localhost:6041/ws", intercept_fn).await;

        let taos = unsafe {
            taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                8819,
            )
        };
        assert!(!taos.is_null());

        test_exec_many(
            taos,
            &[
                "drop database if exists test_1760164460",
                "create database test_1760164460",
                "use test_1760164460",
                "create table t0 (ts timestamp, c1 int)",
            ],
        );

        let taos_ptr = SafePtr(taos);

        let n = 10;
        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            let taos_ptr = taos_ptr.clone();
            tasks.push(tokio::spawn(async move {
                unsafe {
                    let taos_ptr = taos_ptr;
                    let taos = taos_ptr.0;
                    let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
                    assert!(!stmt2.is_null());

                    let sql = c"insert into test_1760164460.t0 values(?, ?)";
                    let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
                    assert_eq!(code, 0);

                    let mut buffer = vec![1739521477831i64];
                    let mut length = vec![8];
                    let mut is_null = vec![0];
                    let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

                    let mut buffer = vec![1i32];
                    let mut length = vec![4];
                    let mut is_null = vec![0];
                    let c1 = new_bind!(Ty::Int, buffer, length, is_null);

                    let mut col = vec![ts, c1];
                    let mut cols = vec![col.as_mut_ptr()];
                    let mut bindv = TAOS_STMT2_BINDV {
                        count: 1,
                        tbnames: ptr::null_mut(),
                        tags: ptr::null_mut(),
                        bind_cols: cols.as_mut_ptr(),
                    };

                    let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
                    assert_eq!(code, 0);

                    let mut affected_rows = 0;
                    let code = taos_stmt2_exec(stmt2, &mut affected_rows);
                    assert_eq!(code, 0);
                    assert_eq!(affected_rows, 1);

                    let sql = c"select * from test_1760164460.t0 where c1 = ?";
                    let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
                    assert_eq!(code, 0);

                    let mut buffer = vec![1];
                    let mut length = vec![4];
                    let mut is_null = vec![0];
                    let c1 = new_bind!(Ty::Int, buffer, length, is_null);

                    let mut col = vec![c1];
                    let mut cols = vec![col.as_mut_ptr()];
                    let mut bindv = TAOS_STMT2_BINDV {
                        count: 1,
                        tbnames: ptr::null_mut(),
                        tags: ptr::null_mut(),
                        bind_cols: cols.as_mut_ptr(),
                    };

                    let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
                    assert_eq!(code, 0);

                    let mut affected_rows = 0;
                    let code = taos_stmt2_exec(stmt2, &mut affected_rows);
                    assert_eq!(code, 0);
                    assert_eq!(affected_rows, 0);

                    let code = taos_stmt2_close(stmt2);
                    assert_eq!(code, 0);
                }

                Ok::<_, anyhow::Error>(())
            }));
        }

        let results = try_join_all(tasks).await.unwrap();
        for res in results {
            res?;
        }

        test_exec(taos, "drop database if exists test_1760164460");
        unsafe { taos_close(taos) };

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_stmt2() -> anyhow::Result<()> {
        let intercept_fn: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("stmt") {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                } else if let Message::Binary(bytes) = msg {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    if action == 9 {
                        if rand::rng().random_bool(0.03) {
                            return ProxyAction::Restart;
                        }
                    }
                }
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8821", "ws://localhost:6041/ws", intercept_fn).await;

        unsafe {
            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                8821,
            );
            assert!(!taos.is_null());

            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1760167825",
                    "create database test_1760167825",
                    "use test_1760167825",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into test_1760167825.t0 values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let n = 10;
            let mut ts_buffer = Vec::with_capacity(n);
            let mut c1_buffer = Vec::with_capacity(n);
            for i in 0..n {
                ts_buffer.push(1726803356466 + i as i64);
                c1_buffer.push(100 + i as i32);
            }

            let mut length = vec![8; n];
            let mut is_null = vec![0; n];
            let ts = new_bind!(Ty::Timestamp, ts_buffer, length, is_null);

            let mut length = vec![4; n];
            let mut is_null = vec![0; n];
            let c1 = new_bind!(Ty::Int, c1_buffer, length, is_null);

            let mut col = vec![ts, c1];
            let mut cols = vec![col.as_mut_ptr()];
            let mut bindv = TAOS_STMT2_BINDV {
                count: 1,
                tbnames: ptr::null_mut(),
                tags: ptr::null_mut(),
                bind_cols: cols.as_mut_ptr(),
            };

            for _ in 0..1000 {
                let code = taos_stmt2_bind_param(stmt2, &mut bindv, -1);
                assert_eq!(code, 0);

                let mut affected_rows = 0;
                let code = taos_stmt2_exec(stmt2, &mut affected_rows);
                assert_eq!(code, 0);
                assert_eq!(affected_rows, 10);
            }

            assert_eq!(taos_stmt2_close(stmt2), 0);
            test_exec(taos, "drop database if exists test_1760167825");
            taos_close(taos);
        }

        Ok(())
    }

    #[test]
    fn test_taos_stmt2_get_fields_insert() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1764847750",
                    "create database test_1764847750",
                    "use test_1764847750",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut count = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt2_get_fields(stmt2, &mut count, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(count, 4);
            assert!(!fields.is_null());

            let field_slice = slice::from_raw_parts(fields, count as usize);
            let field_types = field_slice.iter().map(|f| f.field_type).collect::<Vec<_>>();
            assert_eq!(field_types, vec![4, 2, 1, 1]);

            taos_stmt2_free_fields(stmt2, fields);

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1764847750");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt2_get_fields_query() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1765161954",
                    "create database test_1765161954",
                    "use test_1765161954",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt2 = taos_stmt2_init(taos, ptr::null_mut());
            assert!(!stmt2.is_null());

            let sql = c"select * from s0 where tbname = ? and t1 = ? and ts > ?";
            let code = taos_stmt2_prepare(stmt2, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut count = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt2_get_fields(stmt2, &mut count, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(count, 3);
            assert!(fields.is_null());

            let code = taos_stmt2_close(stmt2);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1765161954");
            taos_close(taos);
        }
    }
}
