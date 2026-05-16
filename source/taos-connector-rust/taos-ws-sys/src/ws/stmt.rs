use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::{ptr, slice};

use taos_error::Code;
use taos_query::block_in_place_or_global;
use taos_query::common::{ColumnView, Timestamp, Ty, Value};
use taos_query::stmt2::{Stmt2BindParam, Stmt2Bindable};
use taos_query::util::generate_req_id;
use taos_ws::query::{BindType, Stmt2Field};
use taos_ws::{Stmt2, Taos};
use tracing::{debug, error};

use crate::ws::error::{
    clear_err_and_ret_succ, format_errno, set_err_and_get_code, taos_errstr, TaosError,
    TaosMaybeError,
};
use crate::ws::query::QueryResultSet;
use crate::ws::{ResultSet, TaosResult, TAOS, TAOS_FIELD_E, TAOS_RES};

#[allow(non_camel_case_types)]
pub type TAOS_STMT = c_void;

#[repr(C)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub struct TAOS_STMT_OPTIONS {
    pub reqId: i64,
    pub singleStbInsert: bool,
    pub singleTableBindOnce: bool,
}

#[repr(C)]
#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub struct TAOS_MULTI_BIND {
    pub buffer_type: c_int,
    pub buffer: *mut c_void,
    pub buffer_length: usize,
    pub length: *mut i32,
    pub is_null: *mut c_char,
    pub num: c_int,
}

#[derive(Debug)]
struct TaosStmt {
    stmt2: Stmt2,
    stb_insert: bool,
    tag_fields: Option<Vec<Stmt2Field>>,
    col_fields: Option<Vec<Stmt2Field>>,
    tag_fields_addr: Option<usize>,
    col_fields_addr: Option<usize>,
    bind_params: Vec<Stmt2BindParam>,
    tbname: Option<String>,
    tags: Option<Vec<Value>>,
    cols: Option<Vec<ColumnView>>,
    single_cols: Option<HashMap<usize, ColumnView>>,
}

impl TaosStmt {
    fn new(stmt2: Stmt2, stb_insert: bool) -> Self {
        Self {
            stmt2,
            stb_insert,
            tag_fields: None,
            col_fields: None,
            tag_fields_addr: None,
            col_fields_addr: None,
            bind_params: Vec::new(),
            tbname: None,
            tags: None,
            cols: None,
            single_cols: None,
        }
    }

    fn set_tbname(&mut self, tbname: String) {
        self.tbname = Some(tbname);
    }

    fn set_tags(&mut self, tags: Vec<Value>) {
        self.tags = Some(tags);
    }

    fn set_cols(&mut self, cols: Vec<ColumnView>) {
        self.cols = Some(cols);
    }

    fn set_single_col(&mut self, idx: usize, col: ColumnView) {
        if self.single_cols.is_none() {
            self.single_cols = Some(HashMap::new());
        }
        self.single_cols.as_mut().unwrap().insert(idx, col);
    }

    fn move_to_bind_params(&mut self) -> TaosResult<()> {
        if let Some(cols) = self.cols.take() {
            let param = Stmt2BindParam::new(self.tbname.clone(), self.tags.clone(), Some(cols));
            self.bind_params.push(param);
        }

        if let Some(mut map) = self.single_cols.take() {
            let len = self.col_fields.as_ref().unwrap().len();
            if map.len() != len {
                error!("move_to_bind_params, incorrect number of fields bound");
                return Err(TaosError::new(
                    Code::FAILED,
                    "incorrect number of fields bound",
                ));
            }

            let mut cols = Vec::with_capacity(len);
            for i in 0..len {
                if let Some(col) = map.remove(&i) {
                    debug!("move_to_bind_params, idx: {i}, col: {col:?}");
                    cols.push(col);
                }
            }

            let param = Stmt2BindParam::new(self.tbname.clone(), self.tags.clone(), Some(cols));
            self.bind_params.push(param);
        }

        Ok(())
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_init(taos: *mut TAOS) -> *mut TAOS_STMT {
    taos_stmt_init_with_reqid(taos, generate_req_id() as _)
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_init_with_reqid(taos: *mut TAOS, reqid: i64) -> *mut TAOS_STMT {
    debug!("taos_stmt_init_with_reqid start, taos: {taos:?}, reqid: {reqid}");
    let taos_stmt: TaosMaybeError<TaosStmt> = match stmt_init(taos, reqid as _, false, false) {
        Ok(taos_stmt) => taos_stmt.into(),
        Err(err) => {
            error!("taos_stmt_init_with_reqid failed, err: {err:?}");
            set_err_and_get_code(err);
            return ptr::null_mut();
        }
    };
    debug!("taos_stmt_init_with_reqid, taos_stmt: {taos_stmt:?}");
    let res = Box::into_raw(Box::new(taos_stmt)) as _;
    debug!("taos_stmt_init_with_reqid succ, res: {res:?}");
    res
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_init_with_options(
    taos: *mut TAOS,
    options: *mut TAOS_STMT_OPTIONS,
) -> *mut TAOS_STMT {
    debug!("taos_stmt_init_with_options start, taos: {taos:?}, options: {options:?}");
    let (req_id, single_stb_insert, single_table_bind_once) = match options.as_ref() {
        Some(options) => (
            options.reqId as u64,
            options.singleStbInsert,
            options.singleTableBindOnce,
        ),
        None => (generate_req_id(), false, false),
    };

    let taos_stmt: TaosMaybeError<TaosStmt> =
        match stmt_init(taos, req_id, single_stb_insert, single_table_bind_once) {
            Ok(taos_stmt) => taos_stmt.into(),
            Err(err) => {
                error!("taos_stmt_init_with_options failed, err: {err:?}");
                set_err_and_get_code(err);
                return ptr::null_mut();
            }
        };
    debug!("taos_stmt_init_with_options, taos_stmt: {taos_stmt:?}");
    let res = Box::into_raw(Box::new(taos_stmt)) as _;
    debug!("taos_stmt_init_with_options succ, res: {res:?}");
    res
}

unsafe fn stmt_init(
    taos: *mut TAOS,
    req_id: u64,
    single_stb_insert: bool,
    single_table_bind_once: bool,
) -> TaosResult<TaosStmt> {
    debug!("stmt_init start, req_id: 0x{req_id:x}, single_stb_insert: {single_stb_insert}, single_table_bind_once: {single_table_bind_once}");

    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::FAILED, "taos is null"))?;

    let stmt2 = Stmt2::new(taos.client_cloned());

    block_in_place_or_global(stmt2.init_with_options(
        req_id,
        single_stb_insert,
        single_table_bind_once,
    ))?;

    let taos_stmt = TaosStmt::new(stmt2, single_stb_insert && single_table_bind_once);
    debug!("stmt_init succ, taos_stmt: {taos_stmt:?}");
    Ok(taos_stmt)
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_prepare(
    stmt: *mut TAOS_STMT,
    sql: *const c_char,
    length: c_ulong,
) -> c_int {
    debug!("taos_stmt_prepare start, stmt: {stmt:?}, sql: {sql:?}, length: {length}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
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

    debug!("taos_stmt_prepare, sql: {sql}");

    let stmt2 = &mut taos_stmt.stmt2;

    if let Err(err) = stmt2.prepare(sql) {
        error!("taos_stmt_prepare failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    if stmt2.is_insert().unwrap() {
        let mut tag_fields = Vec::new();
        let mut col_fields = Vec::new();

        for field in stmt2.fields().unwrap() {
            if field.bind_type == BindType::Tag {
                tag_fields.push(field.clone());
            } else if field.bind_type == BindType::Column {
                col_fields.push(field.clone());
            }
        }

        taos_stmt.tag_fields = Some(tag_fields);
        taos_stmt.col_fields = Some(col_fields);
    }

    debug!("taos_stmt_prepare succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_set_tbname_tags(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    debug!("taos_stmt_set_tbname_tags, stmt: {stmt:?}, name: {name:?}, tags: {tags:?}");
    let code = taos_stmt_set_tbname(stmt, name);
    if code != 0 {
        return code;
    }
    taos_stmt_set_tags(stmt, tags)
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_set_tbname(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int {
    debug!("taos_stmt_set_tbname start, stmt: {stmt:?}, name: {name:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let name = match CStr::from_ptr(name).to_str() {
        Ok(name) => name,
        Err(_) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::INVALID_PARA,
                "name is invalid utf-8",
            )));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    taos_stmt.set_tbname(name.to_owned());

    debug!("taos_stmt_set_tbname succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_set_tags(
    stmt: *mut TAOS_STMT,
    tags: *mut TAOS_MULTI_BIND,
) -> c_int {
    debug!("taos_stmt_set_tags start, stmt: {stmt:?}, tags: {tags:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_set_tags can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let tag_cnt = taos_stmt
        .tag_fields
        .as_ref()
        .map_or(0, |fields| fields.len());

    let binds = slice::from_raw_parts(tags, tag_cnt);
    let tags: Result<Vec<Value>, _> = binds.iter().map(|bind| bind.to_value()).collect();
    let tags = match tags {
        Ok(tags) => tags,
        Err(err) => {
            error!("taos_stmt_set_tags failed, err: {err:?}");
            let code = err.code();
            maybe_err.with_err(Some(err));
            return format_errno(code.into());
        }
    };

    taos_stmt.set_tags(tags);
    debug!("taos_stmt_set_tags succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_set_sub_tbname(
    stmt: *mut TAOS_STMT,
    name: *const c_char,
) -> c_int {
    taos_stmt_set_tbname(stmt, name)
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_stmt_get_tag_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    debug!("taos_stmt_get_tag_fields start, stmt: {stmt:?}, field_num: {fieldNum:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if fieldNum.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fieldNum is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    if fields.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fields is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_get_tag_fields can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let tag_fields: Vec<TAOS_FIELD_E> = taos_stmt
        .tag_fields
        .as_ref()
        .unwrap()
        .iter()
        .map(|field| field.into())
        .collect();

    debug!("taos_stmt_get_tag_fields, fields: {tag_fields:?}");

    *fieldNum = tag_fields.len() as _;

    if !tag_fields.is_empty() {
        *fields = Box::into_raw(tag_fields.into_boxed_slice()) as _;
        taos_stmt.tag_fields_addr = Some(*fields as usize);
    }

    debug!("taos_stmt_get_tag_fields succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_stmt_get_col_fields(
    stmt: *mut TAOS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut TAOS_FIELD_E,
) -> c_int {
    debug!("taos_stmt_get_col_fields start, stmt: {stmt:?}, field_num: {fieldNum:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if fieldNum.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fieldNum is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    if fields.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fields is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_get_col_fields can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let col_fields: Vec<TAOS_FIELD_E> = taos_stmt
        .col_fields
        .as_ref()
        .unwrap()
        .iter()
        .map(|field| field.into())
        .collect();

    debug!("taos_stmt_get_col_fields, fields: {col_fields:?}");

    *fieldNum = col_fields.len() as _;

    if !col_fields.is_empty() {
        *fields = Box::into_raw(col_fields.into_boxed_slice()) as _;
        taos_stmt.col_fields_addr = Some(*fields as usize);
    }

    debug!("taos_stmt_get_col_fields succ, taos_stmt: {taos_stmt:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_reclaim_fields(stmt: *mut TAOS_STMT, fields: *mut TAOS_FIELD_E) {
    debug!("taos_stmt_reclaim_fields start, stmt: {stmt:?}, fields: {fields:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return;
        }
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return;
        }
    };

    if fields.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "fields is null")));
        return;
    }

    if let Some(addr) = taos_stmt.tag_fields_addr {
        if addr == fields as usize {
            let len = taos_stmt
                .tag_fields
                .as_ref()
                .map_or(0, |fields| fields.len());
            let _ = Vec::from_raw_parts(fields, len, len);
            taos_stmt.tag_fields_addr = None;
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            return;
        }
    }

    if let Some(addr) = taos_stmt.col_fields_addr {
        if addr == fields as usize {
            let len = taos_stmt
                .col_fields
                .as_ref()
                .map_or(0, |fields| fields.len());
            let _ = Vec::from_raw_parts(fields, len, len);
            taos_stmt.col_fields_addr = None;
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            return;
        }
    }

    maybe_err.with_err(Some(TaosError::new(
        Code::INVALID_PARA,
        "fields is invalid",
    )));
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_is_insert(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int {
    debug!("taos_stmt_is_insert start, stmt: {stmt:?}, insert: {insert:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if insert.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "insert is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    if stmt2.is_insert().is_none() {
        maybe_err.with_err(Some(TaosError::new(
            Code::FAILED,
            "taos_stmt_prepare is not called",
        )));
        return format_errno(Code::FAILED.into());
    }

    let is_insert = stmt2.is_insert().unwrap();
    *insert = is_insert as _;

    debug!("taos_stmt_is_insert succ, is_insert: {is_insert}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_num_params(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int {
    debug!("taos_stmt_num_params start, stmt: {stmt:?}, nums: {nums:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if nums.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "nums is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;
    if let Some(cnt) = stmt2.fields_count() {
        *nums = cnt as _;
    } else {
        maybe_err.with_err(Some(TaosError::new(
            Code::FAILED,
            "taos_stmt_prepare is not called",
        )));
        return format_errno(Code::FAILED.into());
    }

    debug!("taos_stmt_num_params succ");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_get_param(
    stmt: *mut TAOS_STMT,
    idx: c_int,
    r#type: *mut c_int,
    bytes: *mut c_int,
) -> c_int {
    debug!(
        "taos_stmt_get_param start, stmt: {stmt:?}, idx: {idx}, type: {type:?}, bytes: {bytes:?}"
    );

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    if r#type.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "type is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    if bytes.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "bytes is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_get_param can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let col_cnt = taos_stmt
        .col_fields
        .as_ref()
        .map_or(0, |fields| fields.len());

    if idx < 0 || (idx as usize) >= col_cnt {
        maybe_err.with_err(Some(TaosError::new(
            Code::INVALID_PARA,
            "idx is out of range",
        )));
        return format_errno(Code::INVALID_PARA.into());
    }

    let field = taos_stmt
        .col_fields
        .as_ref()
        .unwrap()
        .get(idx as usize)
        .unwrap();

    *r#type = field.field_type as _;
    *bytes = field.bytes as _;

    debug!("taos_stmt_get_param succ, field: {field:?}");

    maybe_err.clear_err();
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_bind_param(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    debug!("taos_stmt_bind_param start, stmt: {stmt:?}, bind: {bind:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    if bind.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "bind is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    let col_cnt = match stmt2.is_insert() {
        Some(true) => taos_stmt
            .col_fields
            .as_ref()
            .map_or(0, |fields| fields.len()),
        Some(false) => stmt2.fields_count().unwrap_or(0),
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    };

    let binds = slice::from_raw_parts(bind, col_cnt);

    let mut cols = Vec::with_capacity(col_cnt);
    for bind in binds {
        cols.push(bind.to_column_view());
    }

    taos_stmt.set_cols(cols);

    if taos_stmt.stb_insert {
        if let Err(err) = taos_stmt.move_to_bind_params() {
            error!("taos_stmt_bind_param failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return format_errno(err.code().into());
        }
    }

    debug!("taos_stmt_bind_param succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_bind_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
) -> c_int {
    debug!("taos_stmt_bind_param_batch start, stmt: {stmt:?}, bind: {bind:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    if bind.is_null() {
        maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "bind is null")));
        return format_errno(Code::INVALID_PARA.into());
    }

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_bind_param_batch can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let col_cnt = taos_stmt
        .col_fields
        .as_ref()
        .map_or(0, |fields| fields.len());

    let binds = slice::from_raw_parts(bind, col_cnt);

    let mut cols = Vec::with_capacity(col_cnt);
    for bind in binds {
        cols.push(bind.to_column_view());
    }

    taos_stmt.set_cols(cols);

    if taos_stmt.stb_insert {
        if let Err(err) = taos_stmt.move_to_bind_params() {
            error!("taos_stmt_bind_param_batch failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            return format_errno(err.code().into());
        }
    }

    debug!("taos_stmt_bind_param_batch succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_stmt_bind_single_param_batch(
    stmt: *mut TAOS_STMT,
    bind: *mut TAOS_MULTI_BIND,
    colIdx: c_int,
) -> c_int {
    debug!(
        "taos_stmt_bind_single_param_batch start, stmt: {stmt:?}, bind: {bind:?}, col_idx: {colIdx}"
    );

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "stmt is invalid")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_bind_single_param_batch can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_bind_single_param_batch is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    let view = match bind.as_ref() {
        Some(bind) => bind.to_column_view(),
        None => {
            maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "bind is null")));
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    debug!("taos_stmt_bind_single_param_batch, idx: {colIdx}, view: {view:?}");

    taos_stmt.set_single_col(colIdx as _, view);

    debug!("taos_stmt_bind_single_param_batch succ, taos_stmt: {taos_stmt:?}");
    maybe_err.with_err(None);
    0
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_add_batch(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_add_batch start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    let stmt2 = &mut taos_stmt.stmt2;

    match stmt2.is_insert() {
        Some(true) => {}
        Some(false) => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_add_batch can only be called for insertion",
            )));
            return format_errno(Code::FAILED.into());
        }
        None => {
            maybe_err.with_err(Some(TaosError::new(
                Code::FAILED,
                "taos_stmt_prepare is not called",
            )));
            return format_errno(Code::FAILED.into());
        }
    }

    if let Err(err) = taos_stmt.move_to_bind_params() {
        error!("taos_stmt_add_batch failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    debug!("taos_stmt_add_batch succ, taos_stmt: {taos_stmt:?}");

    maybe_err.with_err(None);
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_execute(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_execute start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let taos_stmt = match maybe_err.deref_mut() {
        Some(taos_stmt) => taos_stmt,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    if let Err(err) = taos_stmt.move_to_bind_params() {
        error!("taos_stmt_execute failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    debug!("taos_stmt_execute, taos_stmt: {taos_stmt:?}");

    let stmt2 = &mut taos_stmt.stmt2;

    let params = std::mem::take(&mut taos_stmt.bind_params);
    debug!("taos_stmt_execute, params: {params:?}");

    if let Err(err) = stmt2.bind(&params) {
        error!("taos_stmt_execute failed, err: {err:?}");
        maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
        return format_errno(err.code().into());
    }

    match stmt2.exec() {
        Ok(_) => {
            debug!("taos_stmt_execute succ");
            maybe_err.clear_err();
            clear_err_and_ret_succ()
        }
        Err(err) => {
            error!("taos_stmt_execute failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            format_errno(err.code().into())
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_use_result(stmt: *mut TAOS_STMT) -> *mut TAOS_RES {
    debug!("taos_stmt_use_result start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
            return ptr::null_mut();
        }
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt) => &mut taos_stmt.stmt2,
        None => {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid"));
            return ptr::null_mut();
        }
    };

    match stmt2.result_set() {
        Ok(rs) => {
            let rs: TaosMaybeError<ResultSet> = ResultSet::Query(QueryResultSet::new(rs)).into();
            debug!("taos_stmt_use_result succ, result_set: {rs:?}");
            maybe_err.clear_err();
            clear_err_and_ret_succ();
            Box::into_raw(Box::new(rs)) as _
        }
        Err(err) => {
            error!("taos_stmt_use_result failed, err: {err:?}");
            maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_close(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_close, stmt: {stmt:?}");
    if stmt.is_null() {
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null"));
    }
    let _ = Box::from_raw(stmt as *mut TaosMaybeError<TaosStmt>);
    clear_err_and_ret_succ()
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_errstr(stmt: *mut TAOS_STMT) -> *mut c_char {
    taos_errstr(stmt as _) as _
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_affected_rows(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_affected_rows start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt) => &mut taos_stmt.stmt2,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    debug!(
        "taos_stmt_affected_rows succ, affected_rows: {}",
        stmt2.affected_rows()
    );

    stmt2.affected_rows() as _
}

#[no_mangle]
pub unsafe extern "C" fn taos_stmt_affected_rows_once(stmt: *mut TAOS_STMT) -> c_int {
    debug!("taos_stmt_affected_rows_once start, stmt: {stmt:?}");

    let maybe_err = match (stmt as *mut TaosMaybeError<TaosStmt>).as_mut() {
        Some(maybe_err) => maybe_err,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is null")),
    };

    let stmt2 = match maybe_err.deref_mut() {
        Some(taos_stmt) => &mut taos_stmt.stmt2,
        None => return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "stmt is invalid")),
    };

    debug!(
        "taos_stmt_affected_rows_once succ, affected_rows_once: {}",
        stmt2.affected_rows_once(),
    );

    stmt2.affected_rows_once() as _
}

impl TAOS_MULTI_BIND {
    fn to_value(&self) -> TaosResult<Value> {
        debug!("to_value, bind: {self:?}");

        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            let val = Value::Null(self.ty());
            debug!("to_value, value: {val:?}");
            return Ok(val);
        }

        assert!(!self.buffer.is_null());

        let val = match self.ty() {
            Ty::Null => Value::Null(self.ty()),
            Ty::Bool => unsafe { Value::Bool(*(self.buffer as *const _)) },
            Ty::TinyInt => unsafe { Value::TinyInt(*(self.buffer as *const _)) },
            Ty::SmallInt => unsafe { Value::SmallInt(*(self.buffer as *const _)) },
            Ty::Int => unsafe { Value::Int(*(self.buffer as *const _)) },
            Ty::BigInt => unsafe { Value::BigInt(*(self.buffer as *const _)) },
            Ty::UTinyInt => unsafe { Value::UTinyInt(*(self.buffer as *const _)) },
            Ty::USmallInt => unsafe { Value::USmallInt(*(self.buffer as *const _)) },
            Ty::UInt => unsafe { Value::UInt(*(self.buffer as *const _)) },
            Ty::UBigInt => unsafe { Value::UBigInt(*(self.buffer as *const _)) },
            Ty::Float => unsafe { Value::Float(*(self.buffer as *const _)) },
            Ty::Double => unsafe { Value::Double(*(self.buffer as *const _)) },
            Ty::Timestamp => unsafe {
                Value::Timestamp(Timestamp::Milliseconds(*(self.buffer as *const _)))
            },
            Ty::VarChar | Ty::NChar | Ty::Json | Ty::VarBinary | Ty::Geometry => unsafe {
                assert!(!self.length.is_null());
                let slice = slice::from_raw_parts(self.buffer as *const _, self.length.read() as _);
                match self.ty() {
                    Ty::VarChar => str::from_utf8(slice)
                        .map(String::from)
                        .map(Value::VarChar)?,
                    Ty::NChar => str::from_utf8(slice).map(String::from).map(Value::NChar)?,
                    Ty::Json => serde_json::from_slice(slice).map(Value::Json)?,
                    Ty::VarBinary => Value::VarBinary(slice.into()),
                    Ty::Geometry => Value::Geometry(slice.into()),
                    _ => unreachable!("unreachable branch"),
                }
            },
            _ => todo!(),
        };

        debug!("to_value, value: {val:?}");

        Ok(val)
    }

    fn to_column_view(&self) -> ColumnView {
        debug!("to_column_view, bind: {self:?}");

        let ty = self.ty();
        let num = self.num as usize;

        let mut is_nulls = None;
        if !self.is_null.is_null() {
            is_nulls = Some(unsafe { slice::from_raw_parts(self.is_null, num) });
        }
        debug!("to_column_view, ty: {ty}, num: {num}, is_nulls: {is_nulls:?}");

        macro_rules! view {
            ($ty:ty, $from:expr) => {{
                let buf = self.buffer as *const $ty;
                let mut vals = vec![None; num];
                for i in 0..num {
                    if let Some(is_nulls) = is_nulls {
                        for i in 0..num {
                            if is_nulls[i] == 0 {
                                vals[i] = Some(ptr::read_unaligned(buf.add(i)));
                            }
                        }
                    } else {
                        for i in 0..num {
                            vals[i] = Some(ptr::read_unaligned(buf.add(i)));
                        }
                    }
                }
                $from(vals)
            }};
        }

        let view = unsafe {
            match ty {
                Ty::Bool => view!(bool, ColumnView::from_bools),
                Ty::TinyInt => view!(i8, ColumnView::from_tiny_ints),
                Ty::SmallInt => view!(i16, ColumnView::from_small_ints),
                Ty::Int => view!(i32, ColumnView::from_ints),
                Ty::BigInt => view!(i64, ColumnView::from_big_ints),
                Ty::UTinyInt => view!(u8, ColumnView::from_unsigned_tiny_ints),
                Ty::USmallInt => view!(u16, ColumnView::from_unsigned_small_ints),
                Ty::UInt => view!(u32, ColumnView::from_unsigned_ints),
                Ty::UBigInt => view!(u64, ColumnView::from_unsigned_big_ints),
                Ty::Float => view!(f32, ColumnView::from_floats),
                Ty::Double => view!(f64, ColumnView::from_doubles),
                Ty::Timestamp => view!(i64, ColumnView::from_millis_timestamp),
                Ty::VarChar => {
                    if let Some(is_nulls) = is_nulls {
                        let vals = (0..num)
                            .zip(is_nulls)
                            .map(|(i, is_null)| {
                                if *is_null != 0 {
                                    None
                                } else {
                                    let ptr = (self.buffer as *const u8)
                                        .offset(self.buffer_length as isize * i as isize);
                                    let len = *self.length.add(i) as usize;
                                    let bytes = slice::from_raw_parts(ptr, len);
                                    Some(str::from_utf8_unchecked(bytes))
                                }
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_varchar::<&str, _, _, _>(vals)
                    } else {
                        let vals = (0..num)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_varchar::<&str, _, _, _>(vals)
                    }
                }
                Ty::NChar => {
                    if let Some(is_nulls) = is_nulls {
                        let vals = (0..num)
                            .zip(is_nulls)
                            .map(|(i, is_null)| {
                                if *is_null != 0 {
                                    None
                                } else {
                                    let ptr = (self.buffer as *const u8)
                                        .offset(self.buffer_length as isize * i as isize);
                                    let len = *self.length.add(i) as usize;
                                    let bytes = slice::from_raw_parts(ptr, len);
                                    Some(std::str::from_utf8_unchecked(bytes))
                                }
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_nchar::<&str, _, _, _>(vals)
                    } else {
                        let vals = (0..num)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_nchar::<&str, _, _, _>(vals)
                    }
                }
                Ty::VarBinary => {
                    if let Some(is_nulls) = is_nulls {
                        let vals = (0..num)
                            .zip(is_nulls)
                            .map(|(i, is_null)| {
                                if *is_null != 0 {
                                    None
                                } else {
                                    let ptr = (self.buffer as *const u8)
                                        .offset(self.buffer_length as isize * i as isize);
                                    let len = *self.length.add(i) as usize;
                                    Some(slice::from_raw_parts(ptr, len))
                                }
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_bytes::<&[u8], _, _, _>(vals)
                    } else {
                        let vals = (0..num)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                slice::from_raw_parts(ptr, len)
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_bytes::<&[u8], _, _, _>(vals)
                    }
                }
                Ty::Geometry => {
                    if let Some(is_nulls) = is_nulls {
                        let vals = (0..num)
                            .zip(is_nulls)
                            .map(|(i, is_null)| {
                                if *is_null != 0 {
                                    None
                                } else {
                                    let ptr = (self.buffer as *const u8)
                                        .offset(self.buffer_length as isize * i as isize);
                                    let len = *self.length.add(i) as usize;
                                    Some(slice::from_raw_parts(ptr, len))
                                }
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_geobytes::<&[u8], _, _, _>(vals)
                    } else {
                        let vals = (0..num)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                slice::from_raw_parts(ptr, len)
                            })
                            .collect::<Vec<_>>();
                        ColumnView::from_geobytes::<&[u8], _, _, _>(vals)
                    }
                }
                _ => todo!(),
            }
        };

        debug!("to_column_view, view: {view:?}");

        view
    }

    fn ty(&self) -> Ty {
        self.buffer_type.into()
    }

    #[cfg(test)]
    fn from_primitives<T: taos_query::common::itypes::IValue>(
        values: &[T],
        nulls: &[bool],
        lens: &[i32],
    ) -> Self {
        Self {
            buffer_type: T::TY as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<T>(),
            length: lens.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: values.len() as _,
        }
    }

    #[cfg(test)]
    fn from_timestamps(values: &[i64], nulls: &[bool], lens: &[i32]) -> Self {
        Self {
            buffer_type: Ty::Timestamp as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: lens.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: values.len() as _,
        }
    }
}

impl From<&Stmt2Field> for TAOS_FIELD_E {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;
    use crate::ws::query::*;
    use crate::ws::{taos_close, test_connect, test_exec, test_exec_many};

    macro_rules! new_bind {
        ($ty:expr, $buffer:ident, $length:ident, $is_null:ident) => {
            TAOS_MULTI_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                buffer_length: *$length.iter().max().unwrap() as _,
                length: $length.as_mut_ptr(),
                is_null: $is_null.as_mut_ptr(),
                num: $is_null.len() as _,
            }
        };
    }

    macro_rules! new_bind_without_is_null {
        ($ty:expr, $buffer:ident, $length:ident) => {
            TAOS_MULTI_BIND {
                buffer_type: $ty as _,
                buffer: $buffer.as_mut_ptr() as _,
                buffer_length: *$length.iter().max().unwrap() as _,
                length: $length.as_mut_ptr(),
                is_null: ptr::null_mut(),
                num: $length.len() as _,
            }
        };
    }

    #[test]
    fn test_stmt() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740472346",
                    "create database test_1740472346",
                    "use test_1740472346",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"insert into ? using s0 tags (?) values (?, ?)";
            let len = sql.to_bytes().len();
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), len as _);
            assert_eq!(code, 0);

            let name = c"d0";
            let mut tags = vec![TAOS_MULTI_BIND::from_primitives(&[99], &[false], &[4])];
            let code = taos_stmt_set_tbname_tags(stmt, name.as_ptr(), tags.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_set_tbname(stmt, name.as_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_set_sub_tbname(stmt, name.as_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_set_tags(stmt, tags.as_mut_ptr());
            assert_eq!(code, 0);

            let mut field_num = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt_get_tag_fields(stmt, &mut field_num, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(field_num, 1);

            taos_stmt_reclaim_fields(stmt, fields);

            let mut field_num = 0;
            let mut fields = ptr::null_mut();
            let code = taos_stmt_get_col_fields(stmt, &mut field_num, &mut fields);
            assert_eq!(code, 0);
            assert_eq!(field_num, 2);

            taos_stmt_reclaim_fields(stmt, fields);

            let mut insert = 0;
            let code = taos_stmt_is_insert(stmt, &mut insert);
            assert_eq!(code, 0);
            assert_eq!(insert, 1);

            let mut nums = 0;
            let code = taos_stmt_num_params(stmt, &mut nums);
            assert_eq!(code, 0);
            assert_eq!(nums, 4);

            let mut ty = 0;
            let mut bytes = 0;
            let code = taos_stmt_get_param(stmt, 1, &mut ty, &mut bytes);
            assert_eq!(code, 0);
            assert_eq!(ty, Ty::Int as i32);
            assert_eq!(bytes, 4);

            let mut cols = vec![
                TAOS_MULTI_BIND::from_timestamps(&[1738910658659i64], &[false], &[8]),
                TAOS_MULTI_BIND::from_primitives(&[20], &[false], &[4]),
            ];
            let code = taos_stmt_bind_param_batch(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let mut cols = vec![
                TAOS_MULTI_BIND::from_timestamps(&[1738910658659i64], &[false], &[8]),
                TAOS_MULTI_BIND::from_primitives(&[2025], &[false], &[4]),
            ];
            let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let errstr = taos_stmt_errstr(stmt);
            assert_eq!(CStr::from_ptr(errstr), c"");

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 1);

            let affected_rows_once = taos_stmt_affected_rows_once(stmt);
            assert_eq!(affected_rows_once, 1);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740472346");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_init_with_options() {
        unsafe {
            let taos = test_connect();
            let mut option = TAOS_STMT_OPTIONS {
                reqId: 1001,
                singleStbInsert: true,
                singleTableBindOnce: false,
            };

            let stmt = taos_stmt_init_with_options(taos, &mut option);
            assert!(!stmt.is_null());

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            let stmt = taos_stmt_init_with_options(taos, ptr::null_mut());
            assert!(!stmt.is_null());

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_use_result() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740560525",
                    "create database test_1740560525",
                    "use test_1740560525",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values(1739762261437, 1)",
                    "insert into t0 values(1739762261438, 99)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"select * from t0 where c1 > ?";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut bind = vec![c1];
            let code = taos_stmt_bind_param(stmt, bind.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let res = taos_stmt_use_result(stmt);
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

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740560525");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_bind_param_batch() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740563439",
                    "create database test_1740563439",
                    "use test_1740563439",
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

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql =
                c"insert into ? using s0 tags(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname1 = c"d0";
            let tbname2 = c"d1";

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
            let t17 = TAOS_MULTI_BIND {
                buffer_type: Ty::Int as _,
                buffer: ptr::null_mut(),
                buffer_length: 0,
                length: ptr::null_mut(),
                is_null: is_null.as_mut_ptr(),
                num: is_null.len() as _,
            };

            let mut tags = vec![
                t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17,
            ];

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
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c12 = new_bind!(Ty::VarChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c13 = new_bind!(Ty::NChar, buffer, length, is_null);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ];
            let mut length = vec![5, 5, 10, 0];
            let mut is_null = vec![0, 0, 0, 1];
            let c14 = new_bind!(Ty::VarBinary, buffer, length, is_null);

            let mut buffer = vec![
                1u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0,
            ];
            let mut length = vec![21, 0, 21, 0];
            let mut is_null = vec![0, 1, 0, 1];
            let c15 = new_bind!(Ty::Geometry, buffer, length, is_null);

            let mut cols = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let code = taos_stmt_set_tbname(stmt, tbname1.as_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_set_tags(stmt, tags.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

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
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c12 = new_bind_without_is_null!(Ty::VarChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
            ];
            let mut length = vec![5, 5, 10, 10];
            let c13 = new_bind_without_is_null!(Ty::NChar, buffer, length);

            let mut buffer = vec![
                104u8, 101, 108, 108, 111, 0, 0, 0, 0, 0, 119, 111, 114, 108, 100, 0, 0, 0, 0, 0,
                104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 104, 101, 108, 108, 111, 119,
                111, 114, 108, 100,
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

            let mut cols = vec![
                ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15,
            ];

            let code = taos_stmt_set_tbname_tags(stmt, tbname2.as_ptr(), tags.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 8);

            let sql = c"select * from s0 where c4 > ?";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut buffer = vec![0];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c4 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut bind = vec![c4];

            let code = taos_stmt_bind_param(stmt, bind.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows_once = taos_stmt_affected_rows_once(stmt);
            assert_eq!(affected_rows_once, 0);

            let res = taos_stmt_use_result(stmt);
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

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740563439");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1740573608",
                    "create database test_1740573608",
                    "use test_1740573608",
                    "create table s0 (ts timestamp, c1 int) tags (t1 json)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"insert into ? using s0 tags(?) values(?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";

            let mut buffer = vec![
                123u8, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125,
            ];
            let mut length = vec![buffer.len() as _];
            let mut is_null = vec![0];
            let t1 = new_bind!(Ty::Json, buffer, length, is_null);

            let mut tags = vec![t1];

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let mut cols = vec![ts, c1];

            let code = taos_stmt_set_tbname_tags(stmt, tbname.as_ptr(), tags.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_bind_param_batch(stmt, cols.as_mut_ptr());
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 1);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1740573608");
            taos_close(taos);
        }
    }

    #[test]
    fn test_stmt_stb_insert() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741246945",
                    "create database test_1741246945",
                    "use test_1741246945",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let mut option = TAOS_STMT_OPTIONS {
                reqId: 1001,
                singleStbInsert: true,
                singleTableBindOnce: true,
            };
            let stmt = taos_stmt_init_with_options(taos, &mut option);
            assert!(!stmt.is_null());

            let sql = c"insert into s0 (tbname, ts, c1) values(?, ?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let num_of_sub_table = 10;

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let ts_start = 1739521477831i64;

            for i in 0..num_of_sub_table {
                test_exec(taos, format!("create table d{i} using s0 tags({i})"));

                let name = CString::new(format!("d{i}")).unwrap();
                let code = taos_stmt_set_tbname(stmt, name.as_ptr());
                assert_eq!(code, 0);

                let mut cols = vec![ts.clone(), c1.clone()];
                let code = taos_stmt_bind_param(stmt, cols.as_mut_ptr());
                assert_eq!(code, 0);
            }

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows as i64, num_of_sub_table);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1741246945");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741251640",
                    "create database test_1741251640",
                    "use test_1741251640",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                ],
            );

            let mut option = TAOS_STMT_OPTIONS {
                reqId: 1001,
                singleStbInsert: true,
                singleTableBindOnce: true,
            };
            let stmt = taos_stmt_init_with_options(taos, &mut option);
            assert!(!stmt.is_null());

            let sql = c"insert into s0 (tbname, ts, c1) values(?, ?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let num_of_sub_table = 10;

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let ts_start = 1739521477831i64;

            for i in 0..num_of_sub_table {
                test_exec(taos, format!("create table d{i} using s0 tags({i})"));

                let name = CString::new(format!("d{i}")).unwrap();
                let code = taos_stmt_set_tbname(stmt, name.as_ptr());
                assert_eq!(code, 0);

                let mut cols = vec![ts.clone(), c1.clone()];
                let code = taos_stmt_bind_param_batch(stmt, cols.as_mut_ptr());
                assert_eq!(code, 0);
            }

            let code = taos_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows as i64, num_of_sub_table);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1741251640");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_bind_single_param_batch() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741438739",
                    "create database test_1741438739",
                    "use test_1741438739",
                    "create table s0 (ts timestamp, c1 int) tags (t1 int)",
                    "create table d0 using s0 tags(0)",
                    "create table d1 using s0 tags(1)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"insert into s0 (tbname, ts, c1) values(?, ?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let tbname = c"d0";
            let code = taos_stmt_set_tbname(stmt, tbname.as_ptr());
            assert_eq!(code, 0);

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let mut ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let mut c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let code = taos_stmt_bind_single_param_batch(stmt, &mut ts, 0);
            assert_eq!(code, 0);

            let code = taos_stmt_bind_single_param_batch(stmt, &mut c1, 1);
            assert_eq!(code, 0);

            let code = taos_stmt_add_batch(stmt);

            let tbname = c"d1";
            let code = taos_stmt_set_tbname(stmt, tbname.as_ptr());
            assert_eq!(code, 0);

            let mut buffer = vec![1739521477831i64];
            let mut length = vec![8];
            let mut is_null = vec![0];
            let mut ts = new_bind!(Ty::Timestamp, buffer, length, is_null);

            let mut buffer = vec![99];
            let mut length = vec![4];
            let mut is_null = vec![0];
            let mut c1 = new_bind!(Ty::Int, buffer, length, is_null);

            let code = taos_stmt_bind_single_param_batch(stmt, &mut ts, 0);
            assert_eq!(code, 0);

            let code = taos_stmt_bind_single_param_batch(stmt, &mut c1, 1);
            assert_eq!(code, 0);

            let code = taos_stmt_execute(stmt);
            assert_eq!(code, 0);

            let affected_rows = taos_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 2);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database test_1741438739");
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_stmt_num_params() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1760944015",
                    "create database test_1760944015",
                    "use test_1760944015",
                    "create table t0 (ts timestamp, c1 int)",
                ],
            );

            let stmt = taos_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = c"insert into t0 values(?, ?)";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut nums = 0;
            let code = taos_stmt_num_params(stmt, &mut nums);
            assert_eq!(code, 0);
            assert_eq!(nums, 2);

            let sql = c"select * from t0 where ts > ? and c1 > ?";
            let code = taos_stmt_prepare(stmt, sql.as_ptr(), 0);
            assert_eq!(code, 0);

            let mut nums = 0;
            let code = taos_stmt_num_params(stmt, &mut nums);
            assert_eq!(code, 0);
            assert_eq!(nums, 2);

            let code = taos_stmt_close(stmt);
            assert_eq!(code, 0);

            test_exec(taos, "drop database if exists test_1760944015");
            taos_close(taos);
        }
    }
}
