#![allow(non_snake_case)]

use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_ulong, c_void, CStr, CString};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use dlopen2::raw::Library;
use exec_future::ExecFuture;
use taos_query::common::{c_field_t, raw_data_t, JsonMeta, RawData, SmlData};
use taos_query::prelude::{Code, Field, Precision, RawError};
use taos_query::tmq::Assignment;
use taos_query::RawBlock;
use tracing::instrument;

use self::query_future::QueryFuture;
use crate::into_c_str::IntoCStr;
use crate::types::{
    from_raw_fields, taos_async_fetch_cb, taos_async_query_cb, tmq_commit_cb, tmq_conf_res_t,
    tmq_conf_t, tmq_list_t, tmq_res_t, tmq_resp_err_t, tmq_t, TaosMultiBind, TaosStmt2Bindv,
    TaosStmt2Option, TAOS, TAOS_RES, TAOS_ROW, TAOS_STMT, TAOS_STMT2, TSDB_OPTION,
};
use crate::{err_or, Auth};

mod exec_future;
mod query_future;

lazy_static::lazy_static! {
    static ref RAW_LIBRARIES: Mutex<HashMap<PathBuf, Arc<Library>>> = Mutex::new(HashMap::new());
}

#[derive(Debug)]
#[allow(dead_code, non_snake_case)]
pub struct ApiEntry {
    lib: Arc<Library>,
    version: String,
    taos_init: unsafe extern "C" fn() -> c_int,
    taos_cleanup: unsafe extern "C" fn(),
    taos_get_client_info: unsafe extern "C" fn() -> *const c_char,
    taos_options: unsafe extern "C" fn(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int,
    taos_connect: unsafe extern "C" fn(
        ip: *const c_char,
        user: *const c_char,
        pass: *const c_char,
        db: *const c_char,
        port: u16,
    ) -> *mut TAOS,
    taos_connect_totp: Option<
        unsafe extern "C" fn(
            ip: *const c_char,
            user: *const c_char,
            pass: *const c_char,
            totp: *const c_char,
            db: *const c_char,
            port: u16,
        ) -> *mut TAOS,
    >,
    taos_connect_token: Option<
        unsafe extern "C" fn(
            ip: *const c_char,
            token: *const c_char,
            db: *const c_char,
            port: u16,
        ) -> *mut TAOS,
    >,
    taos_close: unsafe extern "C" fn(taos: *mut TAOS),

    // error handler
    taos_errno: unsafe extern "C" fn(taos: *const TAOS) -> c_int,
    taos_errstr: unsafe extern "C" fn(taos: *const TAOS) -> *const c_char,

    // async query
    taos_fetch_rows_a:
        unsafe extern "C" fn(res: *mut TAOS_RES, fp: taos_async_fetch_cb, param: *mut c_void),
    taos_query_a: unsafe extern "C" fn(
        taos: *mut TAOS,
        sql: *const c_char,
        fp: taos_async_query_cb,
        param: *mut c_void,
    ),
    taos_result_block: Option<unsafe extern "C" fn(taos: *mut TAOS_RES) -> *mut *mut c_void>,
    taos_get_raw_block: Option<unsafe extern "C" fn(taos: *mut TAOS_RES) -> *mut c_void>,
    taos_fetch_raw_block_a: Option<
        unsafe extern "C" fn(res: *mut TAOS_RES, fp: taos_async_fetch_cb, param: *mut c_void),
    >,
    tmq_write_raw: Option<unsafe extern "C" fn(taos: *mut TAOS, meta: raw_data_t) -> i32>,
    taos_write_raw_block: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            nrows: i32,
            ptr: *const c_char,
            tbname: *const c_char,
        ) -> i32,
    >,
    taos_write_raw_block_with_reqid: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            nrows: i32,
            ptr: *const c_char,
            tbname: *const c_char,
            req_id: u64,
        ) -> i32,
    >,
    taos_write_raw_block_with_fields: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            nrows: i32,
            ptr: *const c_char,
            tbname: *const c_char,
            fields: *const c_field_t,
            fields_count: i32,
        ) -> i32,
    >,
    taos_write_raw_block_with_fields_with_reqid: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            nrows: i32,
            ptr: *const c_char,
            tbname: *const c_char,
            fields: *const c_field_t,
            fields_count: i32,
            req_id: u64,
        ) -> i32,
    >,

    // query
    taos_query: unsafe extern "C" fn(taos: *mut TAOS, sql: *const c_char) -> *mut TAOS_RES,
    taos_query_with_reqid: Option<
        unsafe extern "C" fn(taos: *mut TAOS, sql: *const c_char, req_id: u64) -> *mut TAOS_RES,
    >,
    taos_free_result: unsafe extern "C" fn(res: *mut TAOS_RES),
    taos_result_precision: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,
    taos_field_count: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,
    taos_affected_rows: unsafe extern "C" fn(res: *mut TAOS_RES) -> c_int,
    taos_fetch_fields: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut c_void,
    taos_fetch_lengths: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut c_int,
    taos_fetch_block: unsafe extern "C" fn(res: *mut TAOS_RES, rows: *mut TAOS_ROW) -> c_int,
    taos_fetch_block_s: Option<
        unsafe extern "C" fn(
            res: *mut TAOS_RES,
            num_of_rows: *mut c_int,
            rows: *mut TAOS_ROW,
        ) -> c_int,
    >,
    taos_fetch_raw_block: Option<
        unsafe extern "C" fn(res: *mut TAOS_RES, num: *mut i32, data: *mut *mut c_void) -> c_int,
    >,

    #[allow(non_snake_case)]
    taos_get_table_vgId: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            db: *const c_char,
            table: *const c_char,
            vgId: *mut i32,
        ) -> c_int,
    >,

    #[allow(non_snake_case)]
    taos_get_tables_vgId: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            db: *const c_char,
            table: *const *const c_char,
            tableNum: c_int,
            vgId: *mut i32,
        ) -> c_int,
    >,

    pub(crate) stmt: StmtApi,
    pub(crate) stmt2: Stmt2Api,
    pub(crate) tmq: Option<TmqApi>,

    taos_schemaless_insert_raw: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            lines: *const c_char,
            len: c_int,
            totalRows: *mut i32,
            protocol: c_int,
            precision: c_int,
        ) -> *mut TAOS_RES,
    >,

    taos_schemaless_insert_raw_with_reqid: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            lines: *const c_char,
            len: c_int,
            totalRows: *mut i32,
            protocol: c_int,
            precision: c_int,
            req_id: u64,
        ) -> *mut TAOS_RES,
    >,

    taos_schemaless_insert_raw_ttl: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            lines: *const c_char,
            len: c_int,
            totalRows: *mut i32,
            protocol: c_int,
            precision: c_int,
            ttl: i32,
        ) -> *mut TAOS_RES,
    >,

    taos_schemaless_insert_raw_ttl_with_reqid: Option<
        unsafe extern "C" fn(
            taos: *mut TAOS,
            lines: *const c_char,
            len: c_int,
            totalRows: *mut i32,
            protocol: c_int,
            precision: c_int,
            ttl: i32,
            req_id: u64,
        ) -> *mut TAOS_RES,
    >,
}

#[derive(Clone, Copy, Debug)]
pub struct TmqListApi {
    tmq_list_new: unsafe extern "C" fn() -> *mut tmq_list_t,
    tmq_list_append: unsafe extern "C" fn(arg1: *mut tmq_list_t, arg2: *const c_char) -> i32,
    tmq_list_destroy: unsafe extern "C" fn(list: *mut tmq_list_t),
    tmq_list_get_size: unsafe extern "C" fn(list: *const tmq_list_t) -> i32,
    tmq_list_to_c_array: unsafe extern "C" fn(list: *const tmq_list_t) -> *const *mut c_char,
}

impl TmqListApi {
    pub(crate) unsafe fn new_list(&self) -> *mut tmq_list_t {
        (self.tmq_list_new)()
    }

    pub(crate) unsafe fn destroy_list(&self, list: *mut tmq_list_t) {
        (self.tmq_list_destroy)(list);
    }

    pub(crate) unsafe fn new_list_from_cstr<'a, T: IntoCStr<'a>>(
        &self,
        iter: impl IntoIterator<Item = T>,
    ) -> Result<*mut tmq_list_t, RawError> {
        let list = self.new_list();
        for item in iter {
            self.append_list(list, item)?;
        }
        Ok(list)
    }

    pub(crate) fn append_list<'a>(
        &self,
        list: *mut tmq_list_t,
        item: impl IntoCStr<'a>,
    ) -> Result<(), RawError> {
        let code = unsafe { (self.tmq_list_append)(list, item.into_c_str().as_ptr()) };
        if code == 0 {
            Ok(())
        } else {
            Err(RawError::new(Code::FAILED, "append tmq list error"))
        }
    }

    pub(crate) fn list_len(&self, list: *mut tmq_list_t) -> i32 {
        unsafe { (self.tmq_list_get_size)(list) }
    }

    pub(crate) fn as_c_str_slice(&self, list: *mut tmq_list_t) -> &[*mut c_char] {
        let len = self.list_len(list) as usize;
        let data = unsafe { (self.tmq_list_to_c_array)(list) };
        unsafe { std::slice::from_raw_parts(data, len) }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TmqConfApi {
    tmq_conf_new: unsafe extern "C" fn() -> *mut tmq_conf_t,

    tmq_conf_destroy: unsafe extern "C" fn(conf: *mut tmq_conf_t),

    tmq_conf_set: unsafe extern "C" fn(
        conf: *mut tmq_conf_t,
        key: *const c_char,
        value: *const c_char,
    ) -> tmq_conf_res_t,

    #[allow(dead_code)]
    tmq_conf_set_auto_commit_cb:
        unsafe extern "C" fn(conf: *mut tmq_conf_t, cb: tmq_commit_cb, param: *mut c_void),

    tmq_consumer_new: unsafe extern "C" fn(
        conf: *mut tmq_conf_t,
        errstr: *mut c_char,
        errstr_len: i32,
    ) -> *mut tmq_t,
}

impl TmqConfApi {
    pub(crate) unsafe fn new_conf(&self) -> *mut tmq_conf_t {
        (self.tmq_conf_new)()
    }

    pub(crate) unsafe fn destroy_conf(&self, conf: *mut tmq_conf_t) {
        (self.tmq_conf_destroy)(conf);
    }

    pub(crate) unsafe fn set_conf(
        &self,
        conf: *mut tmq_conf_t,
        k: &str,
        v: &str,
    ) -> Result<(), RawError> {
        let key = k.into_c_str();
        let value = v.into_c_str();
        (self.tmq_conf_set)(conf, key.as_ptr(), value.as_ptr()).ok(k, v)
    }

    pub(crate) unsafe fn auto_commit_cb(
        &self,
        conf: *mut tmq_conf_t,
        cb: tmq_commit_cb,
        param: *mut c_void,
    ) {
        (self.tmq_conf_set_auto_commit_cb)(conf, cb, param);
    }

    pub(crate) unsafe fn new_consumer(
        &self,
        conf: *mut tmq_conf_t,
    ) -> Result<*mut tmq_t, RawError> {
        let mut err = [0; 256];
        let tmq = (self.tmq_consumer_new)(conf, err.as_mut_ptr() as _, 255);
        if err[0] != 0 {
            Err(RawError::from_string(
                String::from_utf8_lossy(&err).to_string(),
            ))
        } else if tmq.is_null() {
            Err(RawError::from_string("[optin] create new consumer failed"))
        } else {
            Ok(tmq)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TmqApi {
    tmq_get_res_type: unsafe extern "C" fn(res: *mut TAOS_RES) -> tmq_res_t,
    tmq_get_table_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,
    tmq_get_db_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,
    tmq_get_json_meta: unsafe extern "C" fn(res: *mut TAOS_RES) -> *mut c_char,
    tmq_free_json_meta: unsafe extern "C" fn(json: *mut c_char),
    tmq_get_topic_name: unsafe extern "C" fn(res: *mut TAOS_RES) -> *const c_char,
    tmq_get_vgroup_id: unsafe extern "C" fn(res: *mut TAOS_RES) -> i32,
    tmq_get_raw: unsafe extern "C" fn(res: *mut TAOS_RES, raw: *mut raw_data_t) -> i32,
    tmq_free_raw: unsafe extern "C" fn(raw: raw_data_t) -> i32,

    pub(crate) tmq_subscribe:
        unsafe extern "C" fn(tmq: *mut tmq_t, topics: *mut tmq_list_t) -> tmq_resp_err_t,
    pub(crate) tmq_unsubscribe: unsafe extern "C" fn(tmq: *mut tmq_t) -> tmq_resp_err_t,
    #[allow(dead_code)]
    pub(crate) tmq_subscription:
        unsafe extern "C" fn(tmq: *mut tmq_t, topic_list: *mut *mut tmq_list_t) -> tmq_resp_err_t,
    pub(crate) tmq_consumer_poll:
        unsafe extern "C" fn(tmq: *mut tmq_t, blocking_time: i64) -> *mut TAOS_RES,
    pub(crate) tmq_consumer_close: unsafe extern "C" fn(tmq: *mut tmq_t) -> tmq_resp_err_t,
    pub(crate) tmq_commit_sync:
        unsafe extern "C" fn(tmq: *mut tmq_t, msg: *const TAOS_RES) -> tmq_resp_err_t,
    pub(crate) tmq_commit_async: unsafe extern "C" fn(
        tmq: *mut tmq_t,
        msg: *const TAOS_RES,
        cb: tmq_commit_cb,
        param: *mut c_void,
    ),

    pub(crate) tmq_commit_offset_sync: Option<
        unsafe extern "C" fn(
            tmq: *mut tmq_t,
            topic_name: *const c_char,
            vgroup_id: i32,
            offset: i64,
        ) -> tmq_resp_err_t,
    >,

    pub(crate) tmq_commit_offset_async: Option<
        unsafe extern "C" fn(
            tmq: *mut tmq_t,
            topic_name: *const c_char,
            vgroup_id: i32,
            offset: i64,
            cb: tmq_commit_cb,
            param: *mut c_void,
        ),
    >,

    pub(crate) tmq_get_topic_assignment: Option<
        unsafe extern "C" fn(
            tmq: *mut tmq_t,
            topic_name: *const c_char,
            tmq_topic_assignment: *mut *mut Assignment,
            num_of_assignment: *mut i32,
        ) -> tmq_resp_err_t,
    >,
    pub(crate) tmq_free_assignment: Option<unsafe extern "C" fn(assignment: *mut Assignment)>,

    pub(crate) tmq_offset_seek: Option<
        unsafe extern "C" fn(
            tmq: *mut tmq_t,
            topic_name: *const c_char,
            vgroup_id: i32,
            offset: i64,
        ) -> tmq_resp_err_t,
    >,

    pub(crate) tmq_committed: Option<
        unsafe extern "C" fn(
            tmq: *mut tmq_t,
            topic_name: *const c_char,
            vgroup_id: i32,
        ) -> tmq_resp_err_t,
    >,

    pub(crate) tmq_position: Option<
        unsafe extern "C" fn(
            tmq: *mut tmq_t,
            topic_name: *const c_char,
            vgroup_id: i32,
        ) -> tmq_resp_err_t,
    >,

    pub(crate) tmq_err2str: unsafe extern "C" fn(err: tmq_resp_err_t) -> *const c_char,

    pub(crate) conf_api: TmqConfApi,
    pub(crate) list_api: TmqListApi,
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub struct StmtApi {
    pub(crate) taos_stmt_init: unsafe extern "C" fn(taos: *mut TAOS) -> *mut TAOS_STMT,

    pub(crate) taos_stmt_init_with_reqid:
        Option<unsafe extern "C" fn(taos: *mut TAOS, req_id: u64) -> *mut TAOS_STMT>,

    pub(crate) taos_stmt_prepare:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, sql: *const c_char, length: c_ulong) -> c_int,

    pub(crate) taos_stmt_set_tbname_tags: Option<
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, name: *const c_char, tags: *mut c_void) -> c_int,
    >,

    pub(crate) taos_stmt_set_tbname:
        Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int>,

    pub(crate) taos_stmt_set_tags:
        Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT, tags: *mut c_void) -> c_int>,

    pub(crate) taos_stmt_set_sub_tbname:
        Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT, name: *const c_char) -> c_int>,

    pub(crate) taos_stmt_is_insert:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, insert: *mut c_int) -> c_int,

    pub(crate) taos_stmt_num_params:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, nums: *mut c_int) -> c_int,

    pub(crate) taos_stmt_get_param: unsafe extern "C" fn(
        stmt: *mut TAOS_STMT,
        idx: c_int,
        type_: *mut c_int,
        bytes: *mut c_int,
    ) -> c_int,

    pub(crate) taos_stmt_bind_param:
        unsafe extern "C" fn(stmt: *mut TAOS_STMT, bind: *const c_void) -> c_int,

    pub(crate) taos_stmt_bind_param_batch:
        Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT, bind: *const TaosMultiBind) -> c_int>,

    pub(crate) taos_stmt_bind_single_param_batch: Option<
        unsafe extern "C" fn(
            stmt: *mut TAOS_STMT,
            bind: *const TaosMultiBind,
            colIdx: c_int,
        ) -> c_int,
    >,

    pub(crate) taos_stmt_add_batch: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_execute: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_affected_rows: Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int>,

    pub(crate) taos_stmt_use_result: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> *mut TAOS_RES,

    pub(crate) taos_stmt_close: unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> c_int,

    pub(crate) taos_stmt_errstr:
        Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT) -> *const c_char>,
}

#[derive(Debug, Clone, Copy)]
pub struct Stmt2Api {
    taos_stmt2_init: Option<
        unsafe extern "C" fn(taos: *mut TAOS, option: *mut TaosStmt2Option) -> *mut TAOS_STMT2,
    >,

    taos_stmt2_prepare: Option<
        unsafe extern "C" fn(stmt: *mut TAOS_STMT2, sql: *const c_char, length: c_ulong) -> c_int,
    >,

    taos_stmt2_bind_param: Option<
        unsafe extern "C" fn(
            stmt: *mut TAOS_STMT2,
            bindv: *mut TaosStmt2Bindv,
            col_idx: i32,
        ) -> c_int,
    >,

    taos_stmt2_exec:
        Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT2, affected_rows: *mut c_int) -> c_int>,

    taos_stmt2_close: Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT2) -> c_int>,

    taos_stmt2_error: Option<unsafe extern "C" fn(stmt: *mut TAOS_STMT2) -> *mut c_char>,
}

impl Stmt2Api {
    fn check_api<T>(&self, api: Option<T>) -> Result<T, RawError> {
        if let Some(api) = api {
            Ok(api)
        } else {
            Err(RawError::from_string(
                "Stmt2 API missing (requires TDengine >= v3.3.5.0)",
            ))
        }
    }

    pub(crate) fn init(
        &self,
        taos: *mut TAOS,
        option: *mut TaosStmt2Option,
    ) -> Result<*mut TAOS_STMT2, RawError> {
        let taos_stmt2_init = self.check_api(self.taos_stmt2_init)?;
        let stmt = unsafe { taos_stmt2_init(taos, option) };
        if stmt.is_null() {
            return Err(RawError::from_string(
                self.err_as_str(std::ptr::null_mut())?,
            ));
        }
        Ok(stmt)
    }

    pub(crate) fn prepare(
        &self,
        stmt: *mut TAOS_STMT2,
        sql: *const c_char,
        length: c_ulong,
    ) -> Result<(), RawError> {
        let taos_stmt2_prepare = self.check_api(self.taos_stmt2_prepare)?;
        let code = unsafe { taos_stmt2_prepare(stmt, sql, length) };
        if code != 0 {
            return Err(RawError::from_string(self.err_as_str(stmt)?));
        }
        Ok(())
    }

    pub(crate) fn bind(
        &self,
        stmt: *mut TAOS_STMT2,
        bindv: *mut TaosStmt2Bindv,
        col_idx: i32,
    ) -> Result<(), RawError> {
        let taos_stmt2_bind_param = self.check_api(self.taos_stmt2_bind_param)?;
        let code = unsafe { taos_stmt2_bind_param(stmt, bindv, col_idx) };
        if code != 0 {
            return Err(RawError::from_string(self.err_as_str(stmt)?));
        }
        Ok(())
    }

    pub(crate) fn exec(&self, stmt: *mut TAOS_STMT2) -> Result<(), RawError> {
        let taos_stmt2_exec = self.check_api(self.taos_stmt2_exec)?;
        let code = unsafe { taos_stmt2_exec(stmt, std::ptr::null_mut()) };
        if code != 0 {
            return Err(RawError::from_string(self.err_as_str(stmt)?));
        }
        Ok(())
    }

    pub(crate) fn close(&self, stmt: *mut TAOS_STMT2) -> Result<(), RawError> {
        let taos_stmt2_close = self.check_api(self.taos_stmt2_close)?;
        let code = unsafe { taos_stmt2_close(stmt) };
        if code != 0 {
            return Err(RawError::from_string(self.err_as_str(stmt)?));
        }
        Ok(())
    }

    pub(crate) fn err_as_str(&self, stmt: *mut TAOS_STMT2) -> Result<String, RawError> {
        let taos_stmt2_error = self.check_api(self.taos_stmt2_error)?;
        unsafe {
            let err_ptr = taos_stmt2_error(stmt);
            Ok(CStr::from_ptr(err_ptr).to_string_lossy().to_string())
        }
    }
}

const fn default_lib_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "taos.dll"
    } else if cfg!(target_os = "linux") {
        "libtaos.so"
    } else if cfg!(target_os = "macos") {
        "libtaos.dylib"
    } else {
        panic!("current os is not supported")
    }
}

impl ApiEntry {
    pub fn open_default() -> Result<Self, dlopen2::Error> {
        let lib_env = "TAOS_LIBRARY_PATH";
        let path = if let Some(path) = std::env::var_os(lib_env) {
            PathBuf::from(path)
        } else {
            PathBuf::from(default_lib_name())
        };
        Self::dlopen(path)
    }

    pub fn dlopen<S>(path: S) -> Result<Self, dlopen2::Error>
    where
        S: AsRef<Path>,
    {
        let path = path.as_ref();
        let path = if path.is_file() {
            path.to_owned()
        } else if path.is_dir() {
            path.join(default_lib_name())
        } else if path.as_os_str().is_empty() {
            PathBuf::from(default_lib_name())
        } else {
            path.to_path_buf()
        };

        let mut guard = RAW_LIBRARIES.lock().unwrap();
        let lib = if let Some(lib) = guard.get(&path) {
            lib.clone()
        } else {
            let lib = Library::open(path.as_os_str())?;
            let lib = Arc::new(lib);
            guard.insert(path, lib.clone());
            lib
        };

        macro_rules! symbol {
            ($($name:ident),*) => {
                $(let $name = lib.symbol(stringify!($name))?;)*
            };
        }
        macro_rules! optional_symbol {
            ($($name:ident),*) => {
                $(let $name = lib.symbol(stringify!($name)).ok();)*
            };
        }
        unsafe {
            let taos_get_client_info: extern "C" fn() -> *const c_char =
                lib.symbol("taos_get_client_info")?;
            let version = CStr::from_ptr(taos_get_client_info()).to_str().unwrap();
            symbol!(
                taos_init,
                taos_cleanup,
                taos_options,
                taos_connect,
                taos_close,
                taos_errno,
                taos_errstr,
                taos_fetch_rows_a,
                taos_query_a,
                taos_query,
                taos_free_result,
                taos_result_precision,
                taos_field_count,
                taos_affected_rows,
                taos_fetch_fields,
                taos_fetch_lengths,
                taos_fetch_block
            );

            optional_symbol!(
                taos_connect_totp,
                taos_connect_token,
                taos_fetch_block_s,
                taos_fetch_raw_block,
                taos_fetch_raw_block_a,
                tmq_write_raw,
                taos_write_raw_block,
                taos_write_raw_block_with_reqid,
                taos_query_with_reqid,
                taos_schemaless_insert_raw,
                taos_schemaless_insert_raw_with_reqid,
                taos_schemaless_insert_raw_ttl,
                taos_schemaless_insert_raw_ttl_with_reqid,
                taos_write_raw_block_with_fields,
                taos_write_raw_block_with_fields_with_reqid,
                taos_get_raw_block,
                taos_result_block,
                taos_get_table_vgId,
                taos_get_tables_vgId
            );

            // stmt 2.0.22.3
            symbol!(
                taos_stmt_init,
                taos_stmt_prepare,
                taos_stmt_is_insert,
                taos_stmt_num_params,
                taos_stmt_get_param,
                taos_stmt_bind_param,
                taos_stmt_add_batch,
                taos_stmt_execute,
                taos_stmt_use_result,
                taos_stmt_close
            );
            optional_symbol!(
                taos_stmt_set_tags,
                taos_stmt_init_with_reqid,
                taos_stmt_set_tbname_tags,         // 2.6.0.65
                taos_stmt_set_tbname,              // 2.6.0.65
                taos_stmt_set_sub_tbname,          // 2.6.0.65
                taos_stmt_bind_param_batch,        // 2.6.0.65
                taos_stmt_bind_single_param_batch, // 2.6.0.65
                taos_stmt_affected_rows,           // 2.6.0.65
                taos_stmt_errstr                   // 2.6.0.65
            );

            let stmt = StmtApi {
                taos_stmt_init,
                taos_stmt_init_with_reqid,
                taos_stmt_prepare,
                taos_stmt_set_tbname_tags,
                taos_stmt_set_tbname,
                taos_stmt_set_tags,
                taos_stmt_set_sub_tbname,
                taos_stmt_is_insert,
                taos_stmt_num_params,
                taos_stmt_get_param,
                taos_stmt_bind_param,
                taos_stmt_bind_param_batch,
                taos_stmt_bind_single_param_batch,
                taos_stmt_add_batch,
                taos_stmt_execute,
                taos_stmt_affected_rows,
                taos_stmt_use_result,
                taos_stmt_close,
                taos_stmt_errstr,
            };

            optional_symbol!(
                taos_stmt2_init,
                taos_stmt2_prepare,
                taos_stmt2_bind_param,
                taos_stmt2_exec,
                taos_stmt2_close,
                taos_stmt2_error
            );

            let stmt2 = Stmt2Api {
                taos_stmt2_init,
                taos_stmt2_prepare,
                taos_stmt2_bind_param,
                taos_stmt2_exec,
                taos_stmt2_close,
                taos_stmt2_error,
            };

            let tmq = if version.starts_with('3') {
                symbol!(
                    tmq_get_res_type,
                    tmq_get_table_name,
                    tmq_get_db_name,
                    tmq_get_json_meta,
                    tmq_free_json_meta,
                    tmq_get_topic_name,
                    tmq_get_vgroup_id,
                    tmq_get_raw,
                    tmq_free_raw,
                    tmq_conf_new,
                    tmq_conf_destroy,
                    tmq_conf_set,
                    tmq_conf_set_auto_commit_cb,
                    tmq_list_new,
                    tmq_list_append,
                    tmq_list_destroy,
                    tmq_list_get_size,
                    tmq_list_to_c_array,
                    tmq_subscribe,
                    tmq_unsubscribe,
                    tmq_subscription,
                    tmq_consumer_poll,
                    tmq_consumer_close,
                    tmq_commit_sync,
                    tmq_commit_async,
                    tmq_err2str,
                    tmq_consumer_new
                );
                optional_symbol!(
                    tmq_get_topic_assignment,
                    tmq_free_assignment,
                    tmq_offset_seek,
                    tmq_commit_offset_sync,
                    tmq_commit_offset_async,
                    tmq_committed,
                    tmq_position
                );

                let conf_api = TmqConfApi {
                    tmq_conf_new,
                    tmq_conf_destroy,
                    tmq_conf_set,
                    tmq_conf_set_auto_commit_cb,
                    tmq_consumer_new,
                };

                let list_api = TmqListApi {
                    tmq_list_new,
                    tmq_list_append,
                    tmq_list_destroy,
                    tmq_list_get_size,
                    tmq_list_to_c_array,
                };
                Some(TmqApi {
                    tmq_get_res_type,
                    tmq_get_table_name,
                    tmq_get_db_name,
                    tmq_get_json_meta,
                    tmq_free_json_meta,
                    tmq_get_topic_name,
                    tmq_get_vgroup_id,
                    tmq_get_raw,
                    tmq_free_raw,
                    tmq_subscribe,
                    tmq_unsubscribe,
                    tmq_subscription,
                    tmq_consumer_poll,
                    tmq_consumer_close,
                    tmq_commit_sync,
                    tmq_commit_async,
                    tmq_commit_offset_sync,
                    tmq_commit_offset_async,
                    tmq_get_topic_assignment,
                    tmq_free_assignment,
                    tmq_offset_seek,
                    tmq_committed,
                    tmq_position,
                    tmq_err2str,

                    conf_api,
                    list_api,
                })
            } else {
                None
            };

            Ok(Self {
                lib,
                version: version.to_string(),
                taos_init,
                taos_cleanup,
                taos_get_client_info,
                taos_options,
                taos_connect,
                taos_connect_totp,
                taos_connect_token,
                taos_close,

                taos_errno,
                taos_errstr,

                taos_fetch_rows_a,
                taos_query_a,
                taos_query,
                taos_query_with_reqid,
                tmq_write_raw,
                taos_write_raw_block,
                taos_write_raw_block_with_reqid,
                taos_write_raw_block_with_fields,
                taos_write_raw_block_with_fields_with_reqid,
                taos_result_block,

                taos_free_result,
                taos_result_precision,
                taos_field_count,
                taos_affected_rows,
                taos_fetch_fields,
                taos_fetch_lengths,
                taos_fetch_block,
                taos_fetch_block_s,
                taos_fetch_raw_block,
                taos_fetch_raw_block_a,
                taos_get_raw_block,

                taos_get_table_vgId,
                taos_get_tables_vgId,

                stmt,
                stmt2,
                tmq,

                taos_schemaless_insert_raw,
                taos_schemaless_insert_raw_with_reqid,
                taos_schemaless_insert_raw_ttl,
                taos_schemaless_insert_raw_ttl_with_reqid,
            })
        }
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn is_v20(&self) -> bool {
        self.version.starts_with("2.0")
    }

    pub fn is_v3(&self) -> bool {
        self.version.starts_with('3')
    }

    pub fn ge_v3358(&self) -> bool {
        let mut version = [0u32; 4];
        for (i, seg) in self.version.split('.').take(4).enumerate() {
            version[i] = seg.parse().unwrap_or(0);
        }
        version >= [3, 3, 5, 8]
    }

    pub(super) fn options(&self, opt: TSDB_OPTION, val: &str) -> &Self {
        unsafe {
            let val = CString::new(val.as_bytes()).unwrap();
            (self.taos_options)(opt, val.as_ptr() as _, 0);
        }
        self
    }

    fn connect(&self, auth: &Auth) -> *mut TAOS {
        unsafe {
            (self.taos_connect)(
                auth.host_as_ptr(),
                auth.user_as_ptr(),
                auth.password_as_ptr(),
                auth.database_as_ptr(),
                auth.port(),
            )
        }
    }

    fn connect_totp(&self, auth: &Auth) -> Result<*mut TAOS, RawError> {
        unsafe {
            if let Some(taos_connect_totp) = self.taos_connect_totp {
                Ok(taos_connect_totp(
                    auth.host_as_ptr(),
                    auth.user_as_ptr(),
                    auth.password_as_ptr(),
                    auth.totp_as_ptr(),
                    auth.database_as_ptr(),
                    auth.port(),
                ))
            } else {
                Err(RawError::from_string(
                    "taos_connect_totp is only available in TDengine v3.4.0.0 and above",
                ))
            }
        }
    }

    fn connect_token(&self, auth: &Auth) -> Result<*mut TAOS, RawError> {
        unsafe {
            if let Some(taos_connect_token) = self.taos_connect_token {
                Ok(taos_connect_token(
                    auth.host_as_ptr(),
                    auth.token_as_ptr(),
                    auth.database_as_ptr(),
                    auth.port(),
                ))
            } else {
                Err(RawError::from_string(
                    "taos_connect_token is only available in TDengine v3.4.0.0 and above",
                ))
            }
        }
    }

    fn connect_with_auth(&self, auth: &Auth) -> Result<*mut TAOS, RawError> {
        if auth.token().is_some() {
            self.connect_token(auth)
        } else if auth.totp().is_some() {
            self.connect_totp(auth)
        } else {
            Ok(self.connect(auth))
        }
    }

    #[instrument("connect_with_retries", skip(self, auth), fields(host = auth.host().and_then(|s| s.to_str().ok()), user = ?auth.user().and_then(|s| s.to_str().ok())))]
    pub(super) fn connect_with_retries(
        &self,
        auth: &Auth,
        mut retries: u8,
    ) -> Result<*mut TAOS, RawError> {
        if retries == 0 {
            retries = 1;
        }
        loop {
            let now = std::time::Instant::now();
            let ptr = self.connect_with_auth(auth)?;
            let elapsed = now.elapsed();
            if ptr.is_null() {
                tracing::trace!(cost = ?elapsed, "connect failed");
                retries -= 1;
                let err = self.check(ptr).unwrap_err();
                if retries == 0 {
                    break Err(err);
                }
                if err.code() == 0x000B {
                    continue;
                }
                break Err(err);
            }
            tracing::trace!(cost = ?elapsed, "connected");
            break Ok(ptr);
        }
    }

    pub(super) fn check(&self, ptr: *const TAOS_RES) -> Result<(), RawError> {
        let code: Code = unsafe { (self.taos_errno)(ptr as _) & 0xffff }.into();
        if code.success() {
            Ok(())
        } else {
            let message = unsafe {
                std::str::from_utf8_unchecked(CStr::from_ptr((self.taos_errstr)(ptr)).to_bytes())
            };
            Err(RawError::new(code, message))
        }
    }

    pub(crate) fn err_str(&self, res: *mut c_void) -> Cow<'_, str> {
        unsafe { CStr::from_ptr((self.taos_errstr)(res)) }.to_string_lossy()
    }

    pub(crate) fn errno(&self, res: *mut c_void) -> i32 {
        unsafe { (self.taos_errno)(res) }
    }

    pub(crate) fn free_result(&self, res: *mut c_void) {
        unsafe { (self.taos_free_result)(res) }
    }
}

#[derive(Debug, Clone)]
pub struct RawTaos {
    pub(crate) c: Arc<ApiEntry>,
    ptr: *mut TAOS,
}

unsafe impl Send for RawTaos {}
unsafe impl Sync for RawTaos {}

impl RawTaos {
    pub(crate) fn new(c: Arc<ApiEntry>, ptr: *mut TAOS) -> Result<Self, RawError> {
        if ptr.is_null() {
            Err(c.check(ptr).unwrap_err())
        } else {
            Ok(Self { c, ptr })
        }
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS {
        self.ptr
    }

    pub fn get_table_vgroup_id(&self, db: &str, table: &str) -> Result<i32, RawError> {
        if self.c.taos_get_table_vgId.is_none() {
            return Err(RawError::from_string(
                "Current version does not support get table vgId",
            ));
        }
        let db = CString::new(db).unwrap();
        let table = CString::new(table).unwrap();
        let mut vg_id = 0;
        let code = unsafe {
            (self.c.taos_get_table_vgId.unwrap())(
                self.as_ptr(),
                db.as_ptr(),
                table.as_ptr(),
                &mut vg_id,
            )
        };
        if code == 0 {
            Ok(vg_id)
        } else {
            Err(self.c.check(std::ptr::null_mut()).unwrap_err())
        }
    }

    pub fn get_tables_vgroup_ids<T: AsRef<str>>(
        &self,
        db: &str,
        tables: &[T],
    ) -> Result<Vec<i32>, RawError> {
        if self.c.taos_get_tables_vgId.is_none() {
            return Err(RawError::from_string(
                "Current version does not support get tables vgId",
            ));
        }
        let db = CString::new(db).unwrap();
        let tables: Vec<_> = tables
            .iter()
            .map(|t| CString::new(t.as_ref()).unwrap())
            .collect();
        let tables: Vec<_> = tables.iter().map(|t| t.as_ptr()).collect();
        let mut vg_ids = vec![0; tables.len()];
        let code = unsafe {
            (self.c.taos_get_tables_vgId.unwrap())(
                self.as_ptr(),
                db.as_ptr(),
                tables.as_ptr(),
                tables.len() as i32,
                vg_ids.as_mut_ptr(),
            )
        };
        if code == 0 {
            Ok(vg_ids)
        } else {
            Err(self.c.check(std::ptr::null_mut()).unwrap_err())
        }
    }

    #[inline]
    pub fn query<'a, S: IntoCStr<'a>>(&self, sql: S) -> Result<RawRes, RawError> {
        let sql = sql.into_c_str();
        tracing::trace!("query with sql: {:?}", sql);
        let ptr = unsafe { (self.c.taos_query)(self.as_ptr(), sql.as_ptr()) };
        if ptr.is_null() {
            let code = self.c.errno(std::ptr::null_mut());
            let str = self.c.err_str(std::ptr::null_mut());
            return Err(RawError::new_with_context(
                code,
                str.to_string(),
                format!("Query with sql: {sql:?}"),
            ));
        }
        RawRes::from_ptr(self.c.clone(), ptr)
    }

    #[inline]
    pub fn query_with_req_id<'a, S: IntoCStr<'a>>(
        &self,
        sql: S,
        req_id: u64,
    ) -> Result<RawRes, RawError> {
        let sql = sql.into_c_str();
        tracing::trace!("query with sql: {}", sql.to_str().unwrap_or("<...>"));
        if let Some(taos_query_with_req_id) = self.c.taos_query_with_reqid {
            RawRes::from_ptr(self.c.clone(), unsafe {
                (taos_query_with_req_id)(self.as_ptr(), sql.as_ptr(), req_id)
            })
        } else {
            unimplemented!("2.x does not support req_id")
        }
    }

    #[inline]
    pub fn query_async<'a, S: IntoCStr<'a>>(&self, sql: S) -> QueryFuture<'a> {
        let sql = if self.c.is_v20() {
            // remove all backquotes
            sql.into_c_str()
                .to_str()
                .unwrap()
                .chars()
                .filter(|c| *c != '`')
                .collect::<String>()
                .into_c_str()
        } else {
            sql.into_c_str()
        };
        QueryFuture::new(self.clone(), sql.into_owned())
    }

    #[inline]
    pub fn exec_async<'a, S: IntoCStr<'a>>(&self, sql: S) -> ExecFuture<'_, 'a> {
        let sql = if self.c.is_v20() {
            // remove all backquotes
            sql.into_c_str()
                .to_str()
                .unwrap()
                .chars()
                .filter(|c| *c != '`')
                .collect::<String>()
                .into_c_str()
        } else {
            sql.into_c_str()
        };
        ExecFuture::new(self, sql.into_owned())
    }

    #[inline]
    pub fn query_a<'a, S: IntoCStr<'a>>(
        &self,
        sql: S,
        fp: taos_async_query_cb,
        param: *mut c_void,
    ) {
        unsafe { (self.c.taos_query_a)(self.as_ptr(), sql.into_c_str().as_ptr(), fp, param) }
    }

    //     #[inline]
    //     pub fn validate_sql(self, sql: *const c_char) -> Result<(), RawError> {
    //         let code: Code = unsafe { taos_validate_sql(self.as_ptr(), sql) }.into();
    //         if code.success() {
    //             Ok(())
    //         } else {
    //             let err = unsafe { taos_errstr(std::ptr::null_mut()) };
    //             let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
    //             Err(Error::new(code, err))
    //         }
    //     }

    //     #[inline]
    //     pub fn reset_current_db(&self) {
    //         unsafe { taos_reset_current_db(self.as_ptr()) }
    //     }

    //     #[inline]
    //     pub fn server_version(&self) -> &CStr {
    //         unsafe { CStr::from_ptr(taos_get_server_info(self.as_ptr())) }
    //     }

    //     #[inline]
    //     pub fn load_table_info(&self, list: *const c_char) -> Result<(), RawError> {
    //         err_or!(taos_load_table_info(self.as_ptr(), list))
    //     }

    #[inline]
    pub fn write_raw_meta(&self, meta: raw_data_t) -> Result<(), RawError> {
        // try 5 times if write_raw_meta fails with 0x2603 error.
        let tmq_write_raw = self
            .c
            .tmq_write_raw
            .ok_or_else(|| RawError::from_string("2.x does not support write raw meta"))?;
        let tmq_err2str = self
            .c
            .tmq
            .ok_or_else(|| RawError::from_string("tmq api is not available"))?
            .tmq_err2str;
        let mut retries = 2;
        let now = std::time::Instant::now();
        loop {
            tracing::trace!("write raw");
            let raw_code = unsafe { tmq_write_raw(self.as_ptr(), meta) };
            let code = Code::from(raw_code);
            if code.success() {
                let elapsed = now.elapsed();
                if elapsed > std::time::Duration::from_secs(30) {
                    tracing::warn!(tmq.write_raw.cost = ?elapsed, "write raw cost too long");
                } else {
                    tracing::trace!(tmq.write_raw.cost = ?elapsed, "write raw success");
                }
                tracing::trace!(tmq.write_raw.cost = ?now.elapsed(), "write raw success");
                return Ok(());
            }
            if code != Code::from(0x2603) {
                let err = unsafe { tmq_err2str(tmq_resp_err_t(raw_code)) };
                let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
                tracing::trace!(error.code = %code, error.message = err, tmq.write_raw.cost = ?now.elapsed(), "write raw failed");
                if err == "success" {
                    return Err(RawError::from_code(code));
                }
                return Err(taos_query::prelude::RawError::new(code, err));
            }
            tracing::trace!("received error code 0x2603, try once");
            retries -= 1;
            if retries == 0 {
                let err = unsafe { tmq_err2str(tmq_resp_err_t(raw_code)) };
                let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
                if err == "success" {
                    return Err(RawError::from_code(code));
                }
                return Err(taos_query::prelude::RawError::new(code, err));
            }
        }
    }

    #[inline]
    pub fn write_raw_block(&self, block: &RawBlock) -> Result<(), RawError> {
        let nrows = block.nrows();
        let name = block
            .table_name()
            .ok_or_else(|| RawError::new(Code::FAILED, "raw block should have table name"))?;
        let ptr = block.as_raw_bytes().as_ptr();
        if let Some(f) = self.c.taos_write_raw_block_with_fields {
            let tmq_err2str = self
                .c
                .tmq
                .ok_or_else(|| RawError::from_string("tmq api is not available"))?
                .tmq_err2str;
            let fields: Vec<_> = block.fields().into_iter().map(|f| f.to_c_field()).collect();
            let code = unsafe {
                f(
                    self.as_ptr(),
                    nrows as _,
                    ptr as _,
                    name.into_c_str().as_ptr(),
                    fields.as_ptr() as _,
                    fields.len() as _,
                )
            };
            if code == 0 {
                Ok(())
            } else {
                let err = unsafe { tmq_err2str(tmq_resp_err_t(code)) };
                let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
                if err == "success" {
                    return Err(RawError::from_code(code));
                }
                Err(taos_query::prelude::RawError::new(Code::from(code), err))
            }
        } else if let Some(f) = self.c.taos_write_raw_block {
            err_or!(f(
                self.as_ptr(),
                nrows as _,
                ptr as _,
                name.into_c_str().as_ptr()
            ))
        } else {
            unimplemented!("2.x does not support write raw block")
        }
    }

    #[inline]
    pub fn write_raw_block_with_req_id(
        &self,
        block: &RawBlock,
        req_id: u64,
    ) -> Result<(), RawError> {
        let nrows = block.nrows();
        let name = block
            .table_name()
            .ok_or_else(|| RawError::new(Code::FAILED, "raw block should have table name"))?;
        let ptr = block.as_raw_bytes().as_ptr();
        if let Some(f) = self.c.taos_write_raw_block_with_fields_with_reqid {
            let fields: Vec<_> = block.fields().into_iter().map(|f| f.to_c_field()).collect();
            let code = unsafe {
                f(
                    self.as_ptr(),
                    nrows as _,
                    ptr as _,
                    name.into_c_str().as_ptr(),
                    fields.as_ptr() as _,
                    fields.len() as _,
                    req_id,
                )
            };

            if code == 0 {
                Ok(())
            } else {
                let tmq_err2str = self
                    .c
                    .tmq
                    .ok_or_else(|| RawError::from_string("tmq api is not available"))?
                    .tmq_err2str;
                let err = unsafe { tmq_err2str(tmq_resp_err_t(code)) };
                let err = unsafe { std::str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes()) };
                if err == "success" {
                    return Err(RawError::from_code(code));
                }
                Err(taos_query::prelude::RawError::new(Code::from(code), err))
            }
        } else if let Some(f) = self.c.taos_write_raw_block_with_reqid {
            err_or!(f(
                self.as_ptr(),
                nrows as _,
                ptr as _,
                name.into_c_str().as_ptr(),
                req_id
            ))
        } else {
            self.write_raw_block(block)
        }
    }

    #[inline]
    pub fn put(&self, sml: &SmlData) -> Result<(), RawError> {
        let data = sml.data().join("\n");
        tracing::trace!("sml insert with data: {}", data);
        let length = data.len() as i32;
        let mut total_rows: i32 = 0;
        let res;

        if sml.req_id().is_some() && sml.ttl().is_some() {
            tracing::trace!(
                "sml insert with req_id: {} and ttl {}",
                sml.req_id().unwrap(),
                sml.ttl().unwrap()
            );
            if let Some(taos_schemaless_insert_raw_ttl_with_reqid) =
                self.c.taos_schemaless_insert_raw_ttl_with_reqid
            {
                res = RawRes::from_ptr(self.c.clone(), unsafe {
                    taos_schemaless_insert_raw_ttl_with_reqid(
                        self.as_ptr(),
                        data.into_c_str().as_ptr(),
                        length,
                        &mut total_rows,
                        sml.protocol() as c_int,
                        sml.precision() as c_int,
                        sml.ttl().unwrap(),
                        sml.req_id().unwrap(),
                    )
                });
            } else {
                unimplemented!("does not support schemaless")
            }
        } else if sml.req_id().is_some() {
            tracing::trace!("sml insert with req_id: {}", sml.req_id().unwrap());
            if let Some(taos_schemaless_insert_raw_with_reqid) =
                self.c.taos_schemaless_insert_raw_with_reqid
            {
                res = RawRes::from_ptr(self.c.clone(), unsafe {
                    taos_schemaless_insert_raw_with_reqid(
                        self.as_ptr(),
                        data.into_c_str().as_ptr(),
                        length,
                        &mut total_rows,
                        sml.protocol() as c_int,
                        sml.precision() as c_int,
                        sml.req_id().unwrap(),
                    )
                });
            } else {
                unimplemented!("does not support schemaless")
            }
        } else if sml.ttl().is_some() {
            tracing::trace!("sml insert with ttl: {}", sml.ttl().unwrap());
            if let Some(taos_schemaless_insert_raw_ttl) = self.c.taos_schemaless_insert_raw_ttl {
                res = RawRes::from_ptr(self.c.clone(), unsafe {
                    taos_schemaless_insert_raw_ttl(
                        self.as_ptr(),
                        data.into_c_str().as_ptr(),
                        length,
                        &mut total_rows,
                        sml.protocol() as c_int,
                        sml.precision() as c_int,
                        sml.ttl().unwrap(),
                    )
                });
            } else {
                unimplemented!("does not support schemaless")
            }
        } else {
            tracing::trace!("sml insert without req_id and ttl");
            if let Some(taos_schemaless_insert_raw) = self.c.taos_schemaless_insert_raw {
                res = RawRes::from_ptr(self.c.clone(), unsafe {
                    taos_schemaless_insert_raw(
                        self.as_ptr(),
                        data.into_c_str().as_ptr(),
                        length,
                        &mut total_rows,
                        sml.protocol() as c_int,
                        sml.precision() as c_int,
                    )
                });
            } else {
                unimplemented!("does not support schemaless")
            }
        }

        tracing::trace!("sml total rows: {}", total_rows);
        match res {
            Ok(mut raw_res) => {
                tracing::trace!("sml insert success");
                raw_res.free_result();
                Ok(())
            }
            Err(e) => {
                tracing::trace!("sml insert failed: {e:?}");
                Err(e)
            }
        }
    }

    #[inline]
    pub fn close(&mut self) {
        tracing::trace!("call taos_close");
        unsafe { (self.c.as_ref().taos_close)(self.as_ptr()) }
    }
}

#[derive(Debug, Clone)]
pub struct RawRes {
    c: Arc<ApiEntry>,
    ptr: *mut TAOS_RES,
    owned: bool,
}

unsafe impl Send for RawRes {}
unsafe impl Sync for RawRes {}

impl RawRes {
    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS_RES {
        self.ptr
    }

    #[inline]
    fn errno(&self) -> Code {
        unsafe { (self.c.taos_errno)(self.as_ptr()) & 0xffff }.into()
    }

    #[inline]
    pub fn err_as_str(&self) -> &'static str {
        unsafe {
            std::str::from_utf8_unchecked(
                CStr::from_ptr((self.c.taos_errstr)(self.as_ptr())).to_bytes(),
            )
        }
    }

    #[inline]
    pub fn with_code(self, code: Code) -> Result<Self, RawError> {
        if code.success() {
            Ok(self)
        } else {
            Err(RawError::new(code, self.err_as_str()))
        }
    }

    #[inline]
    pub fn from_ptr(c: Arc<ApiEntry>, ptr: *mut TAOS_RES) -> Result<Self, RawError> {
        let raw = unsafe { Self::from_ptr_unchecked(c, ptr) };
        let code = raw.errno();
        raw.with_code(code)
    }

    #[inline]
    pub unsafe fn from_ptr_unchecked(c: Arc<ApiEntry>, ptr: *mut TAOS_RES) -> Self {
        Self {
            c,
            ptr,
            owned: true,
        }
    }

    #[inline]
    pub unsafe fn from_ptr_unowned(c: Arc<ApiEntry>, ptr: *mut TAOS_RES) -> Self {
        RawRes {
            c,
            ptr,
            owned: false,
        }
    }

    #[inline]
    pub fn from_ptr_with_code(
        c: Arc<ApiEntry>,
        ptr: *mut TAOS_RES,
        code: Code,
    ) -> Result<RawRes, RawError> {
        unsafe { RawRes::from_ptr_unchecked(c, ptr) }.with_code(code)
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe { (self.c.taos_affected_rows)(self.as_ptr()) }
    }

    #[inline]
    pub fn free_result(&mut self) {
        if self.owned {
            unsafe { (self.c.taos_free_result)(self.as_ptr()) }
        }
    }

    #[inline]
    pub fn precision(&self) -> Precision {
        unsafe { (self.c.taos_result_precision)(self.as_ptr()) }.into()
    }

    #[inline]
    pub fn field_count(&self) -> usize {
        unsafe { (self.c.taos_field_count)(self.as_ptr()) as _ }
    }

    pub fn fetch_fields(&self) -> Vec<Field> {
        let len = unsafe { (self.c.taos_field_count)(self.as_ptr()) };
        from_raw_fields(
            self.c.version(),
            unsafe { (self.c.taos_fetch_fields)(self.as_ptr()) },
            len as usize,
        )
    }

    #[inline]
    pub fn fetch_lengths(&self) -> &[u32] {
        unsafe {
            std::slice::from_raw_parts(
                (self.c.taos_fetch_lengths)(self.as_ptr()) as *const u32,
                self.field_count(),
            )
        }
    }

    #[inline]
    unsafe fn fetch_lengths_raw(&self) -> *const i32 {
        (self.c.taos_fetch_lengths)(self.as_ptr())
    }

    #[inline]
    pub fn fetch_block(&self) -> Result<Option<(TAOS_ROW, i32, *const i32)>, RawError> {
        let block = Box::into_raw(Box::new(std::ptr::null_mut()));
        let num = unsafe { (self.c.taos_fetch_block)(self.as_ptr(), block) };
        if num > 0 {
            Ok(Some(unsafe { (*block, num, self.fetch_lengths_raw()) }))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn fetch_raw_block(&self, fields: &[Field]) -> Result<Option<RawBlock>, RawError> {
        if self.c.is_v3() {
            self.fetch_raw_block_v3(fields)
        } else {
            self.fetch_raw_block_v2(fields)
        }
    }

    #[inline]
    fn fetch_raw_block_v2(&self, fields: &[Field]) -> Result<Option<RawBlock>, RawError> {
        let mut block: *mut *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        if let Some(taos_fetch_block_s) = self.c.taos_fetch_block_s {
            let fetch =
                unsafe { (taos_fetch_block_s)(self.as_ptr(), &mut num as _, &mut block as _) };
            let lengths = self.fetch_lengths();
            if fetch == 0 {
                if num > 0 {
                    let raw = unsafe {
                        RawBlock::parse_from_ptr_v2(
                            block as _,
                            fields,
                            lengths,
                            num as usize,
                            self.precision(),
                        )
                    };
                    Ok(Some(raw))
                } else {
                    Ok(None)
                }
            } else {
                let code: Code = fetch.into();
                let err = self.err_as_str();
                Err(RawError::new(code, err))
            }
        } else {
            num = unsafe { (self.c.taos_fetch_block)(self.as_ptr(), &mut block as _) };
            let lengths = self.fetch_lengths();
            if num > 0 {
                let raw = unsafe {
                    RawBlock::parse_from_ptr_v2(
                        block as _,
                        fields,
                        lengths,
                        num as usize,
                        self.precision(),
                    )
                };
                Ok(Some(raw))
            } else {
                Ok(None)
            }
        }
    }

    #[inline]
    fn fetch_raw_block_v3(&self, fields: &[Field]) -> Result<Option<RawBlock>, RawError> {
        let mut block: *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        crate::err_or!(
            self,
            (self.c.taos_fetch_raw_block.unwrap())(self.as_ptr(), &mut num as _, &mut block as _),
            if num > 0 {
                match self.tmq_message_type() {
                    tmq_res_t::TMQ_RES_INVALID => {
                        let mut raw = RawBlock::parse_from_ptr(block as _, self.precision());
                        raw.with_field_names(fields.iter().map(Field::name));
                        Some(raw)
                    }
                    tmq_res_t::TMQ_RES_DATA | tmq_res_t::TMQ_RES_METADATA => {
                        let fields = self.fetch_fields();
                        let mut raw = RawBlock::parse_from_ptr(block as _, self.precision());
                        raw.with_field_names(fields.iter().map(Field::name));
                        if let Some(name) = self.tmq_db_name() {
                            raw.with_database_name(name);
                        }
                        if let Some(name) = self.tmq_table_name() {
                            raw.with_table_name(name);
                        }
                        Some(raw)
                    }
                    ty => unreachable!("unknown tmq message type: {ty:?}"),
                }
            } else {
                None
            }
        )
    }

    pub fn fetch_raw_block_async(
        &self,
        fields: &[Field],
        precision: Precision,
        state: &Rc<UnsafeCell<BlockState>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        if self.c.is_v3() {
            self.fetch_raw_block_async_v3(fields, precision, state, cx)
        } else if self.c.is_v20() {
            self.fetch_raw_block_async_v20(fields, precision, state, cx)
        } else {
            self.fetch_raw_block_async_v2(fields, precision, state, cx)
        }
    }

    pub fn fetch_raw_block_async_v20(
        &self,
        fields: &[Field],
        precision: Precision,
        _state: &Rc<UnsafeCell<BlockState>>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        let block = Box::into_raw(Box::new(std::ptr::null_mut()));
        let res = unsafe { (self.c.taos_fetch_block)(self.as_ptr(), block) };
        if res > 0 {
            let block = unsafe { *block };
            let raw = unsafe {
                RawBlock::parse_from_ptr_v2(
                    block as _,
                    fields,
                    self.fetch_lengths(),
                    res as usize,
                    precision,
                )
            };
            Poll::Ready(Ok(Some(raw)))
        } else {
            Poll::Ready(Ok(None))
        }
    }

    pub fn fetch_raw_block_async_v2(
        &self,
        fields: &[Field],
        precision: Precision,
        state: &Rc<UnsafeCell<BlockState>>,
        cx: &Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        let current = unsafe { &mut *state.get() };
        if current.in_use {
            tracing::trace!("call back in use");
            return Poll::Pending;
        }

        if let Some(res) = current.result.take() {
            let item = res.map(|block| {
                block.map(|(ptr, rows)| {
                    assert!(rows > 0);
                    tracing::trace!("{:p} current block has {} rows", self.as_ptr(), rows);
                    // has next block.
                    let mut raw = unsafe {
                        RawBlock::parse_from_ptr_v2(
                            ptr as _,
                            fields,
                            self.fetch_lengths(),
                            rows,
                            precision,
                        )
                    };
                    raw.with_field_names(fields.iter().map(Field::name));
                    raw
                })
            });
            Poll::Ready(item)
        } else {
            current.in_use = true;
            let param = Box::new((
                Rc::downgrade(state),
                Arc::downgrade(&self.c),
                cx.waker().clone(),
            ));
            #[no_mangle]
            unsafe extern "C" fn taos_optin_fetch_rows_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                num_of_rows: c_int,
            ) {
                // use weak pointer in case that the result is freed earlier than callback received.
                let param = param as *mut (Weak<UnsafeCell<BlockState>>, Weak<ApiEntry>, Waker);
                let param = Box::from_raw(param);

                if let (Some(state), Some(api)) = (param.0.upgrade(), param.1.upgrade()) {
                    let state = &mut *state.get();
                    let api = &*api;
                    // state.done = true;
                    state.in_use = false;
                    if num_of_rows < 0 {
                        // error
                        let old = state.result.replace(Err(RawError::new_with_context(
                            num_of_rows,
                            api.err_str(res),
                            "fetch_rows_a",
                        )));
                        drop(old);
                    } else {
                        // success
                        if num_of_rows > 0 {
                            // has a block
                            let block = match api.taos_result_block {
                                Some(f) => (f)(res).read() as _,
                                None => todo!(),
                            };
                            state
                                .result
                                .replace(Ok(Some((block, num_of_rows as usize))));
                        } else {
                            // retrieving completed
                            state.result.replace(Ok(None));
                        }
                    }
                    param.2.wake();
                }
            }
            unsafe {
                (self.c.taos_fetch_rows_a)(
                    self.as_ptr(),
                    taos_optin_fetch_rows_callback as _,
                    Box::into_raw(param) as *mut _ as _,
                );
            };
            Poll::Pending
        }
    }

    pub fn fetch_raw_block_async_v3(
        &self,
        fields: &[Field],
        precision: Precision,
        state: &Rc<UnsafeCell<BlockState>>,
        cx: &Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, RawError>> {
        let current = unsafe { &mut *state.get() };
        // Do not do anything until callback received.
        if current.in_use {
            return Poll::Pending;
        }

        if let Some(res) = current.result.take() {
            let item = res.map(|block| {
                block.map(|(ptr, rows)| {
                    debug_assert!(rows > 0);
                    // has next block.
                    let mut raw = unsafe { RawBlock::parse_from_ptr(ptr as _, precision) };
                    raw.with_field_names(fields.iter().map(Field::name));
                    raw
                })
            });
            Poll::Ready(item)
        } else {
            current.in_use = true;
            let param = Box::new((
                Rc::downgrade(state),
                Arc::downgrade(&self.c),
                cx.waker().clone(),
            ));
            #[no_mangle]
            unsafe extern "C" fn taos_optin_fetch_raw_block_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                num_of_rows: c_int,
            ) {
                // use weak pointer in case that the result is freed earlier than callback received.
                let param: Box<(Weak<UnsafeCell<BlockState>>, Weak<ApiEntry>, Waker)> =
                    Box::from_raw(param as _);
                if let Some(state) = param.0.upgrade() {
                    if let Some(api) = param.1.upgrade() {
                        let state = &mut *state.get();
                        let api = &*api;
                        // state.done = true;
                        state.in_use = false;
                        if num_of_rows < 0 {
                            // state.code = num_of_rows;
                            // state.num = 0;
                            state.result.replace(Err(RawError::new_with_context(
                                num_of_rows,
                                api.err_str(res),
                                "taos_fetch_raw_block_a",
                            )));
                        } else {
                            // state.num = num_of_rows as _;
                            if num_of_rows > 0 {
                                let block = (api.taos_get_raw_block.unwrap())(res) as _;
                                state
                                    .result
                                    .replace(Ok(Some((block, num_of_rows as usize))));
                            } else {
                                state.result.replace(Ok(None));
                            }
                        }
                        param.2.wake();
                    }
                }
            }
            unsafe {
                (self.c.taos_fetch_raw_block_a.unwrap())(
                    self.as_ptr(),
                    taos_optin_fetch_raw_block_callback as _,
                    Box::into_raw(param) as *mut _ as _,
                );
            };
            Poll::Pending
        }
    }

    /// Only for tmq.
    pub(crate) fn fetch_raw_message(&self) -> Option<RawBlock> {
        let mut block: *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        unsafe {
            (self.c.taos_fetch_raw_block.unwrap())(self.as_ptr(), &mut num as _, &mut block as _)
        };
        let fields = self.fetch_fields();

        if num == 0 || block.is_null() {
            return None;
        }
        let mut raw = unsafe { RawBlock::parse_from_ptr(block as _, self.precision()) };

        raw.with_field_names(fields.iter().map(Field::name));

        if let Some(name) = self.tmq_table_name() {
            raw.with_table_name(name);
        }

        Some(raw)
    }

    #[inline]
    pub(crate) fn tmq_message_type(&self) -> tmq_res_t {
        unsafe { (self.c.tmq.as_ref().unwrap().tmq_get_res_type)(self.as_ptr()) }
    }

    #[inline]
    pub fn tmq_table_name(&self) -> Option<&str> {
        unsafe {
            let c = (self.c.tmq.as_ref().unwrap().tmq_get_table_name)(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }

    #[inline]
    pub(crate) fn tmq_db_name(&self) -> Option<&str> {
        unsafe {
            let c = (self.c.tmq.as_ref().unwrap().tmq_get_db_name)(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }

    #[inline]
    pub fn tmq_topic_name(&self) -> Option<&str> {
        unsafe {
            let c = (self.c.tmq.as_ref().unwrap().tmq_get_topic_name)(self.as_ptr());
            if c.is_null() {
                None
            } else {
                CStr::from_ptr(c).to_str().ok()
            }
        }
    }

    #[inline]
    pub fn tmq_vgroup_id(&self) -> Option<i32> {
        unsafe {
            let c = (self.c.tmq.as_ref().unwrap().tmq_get_vgroup_id)(self.as_ptr());
            if c == -1 {
                None
            } else {
                Some(c)
            }
        }
    }

    #[inline]
    pub(crate) fn tmq_get_json_meta(&self) -> Result<JsonMeta, RawError> {
        unsafe {
            let meta = (self.c.tmq.as_ref().unwrap().tmq_get_json_meta)(self.as_ptr());
            if meta.is_null() {
                return Err(RawError::from_string("tmq_get_json_meta returns null"));
            }

            let meta_cstr = CStr::from_ptr(meta);
            match serde_json::from_slice(meta_cstr.to_bytes()) {
                Ok(json_meta) => {
                    tracing::trace!(json = %meta_cstr.to_string_lossy(), "Received TMQ json meta");
                    (self.c.tmq.as_ref().unwrap().tmq_free_json_meta)(meta);
                    Ok(json_meta)
                }
                Err(err) => {
                    (self.c.tmq.as_ref().unwrap().tmq_free_json_meta)(meta);
                    Err(RawError::from_string(err.to_string()))
                }
            }
        }
    }

    #[inline]
    pub(crate) fn tmq_get_raw(&self) -> Result<RawData, RawError> {
        let mut meta = raw_data_t {
            raw: std::ptr::null_mut(),
            raw_len: 0,
            raw_type: 0,
        };
        unsafe {
            let code = (self.c.tmq.as_ref().unwrap().tmq_get_raw)(self.as_ptr(), &mut meta as _);
            if code == 0 {
                return Ok(RawData::new(
                    meta,
                    self.c.tmq.as_ref().unwrap().tmq_free_raw,
                ));
            }
            let c_str = (self.c.tmq.as_ref().unwrap().tmq_err2str)(tmq_resp_err_t(code));
            let err = CStr::from_ptr(c_str).to_string_lossy().into_owned();
            Err(RawError::new_with_context(code, err, "tmq_get_raw error"))
        }
    }
}

/// Shared block state in a ResultSet.
#[derive(Default)]
pub struct BlockState {
    pub result: Option<Result<Option<(*mut c_void, usize)>, RawError>>,
    pub in_use: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_taos() {
        let c = Arc::new(ApiEntry::open_default().unwrap());
        unsafe { (c.taos_init)() };
        let taos = RawTaos::new(c, 1 as _).unwrap();
        let raw = raw_data_t {
            raw: std::ptr::null_mut(),
            raw_len: 0,
            raw_type: 2,
        };
        let err = taos.write_raw_meta(raw).unwrap_err();

        dbg!(&err);
        assert!(
            err.to_string().contains("[0x0118]"),
            "Error message does not contain the expected substring"
        );
    }
}
