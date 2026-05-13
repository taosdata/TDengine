use std::collections::HashMap;
use std::ffi::{c_void, CStr, CString};
use std::fmt::Debug;
use std::os::raw::{c_char, c_int};
use std::str::FromStr;
use std::time::Duration;

use taos_error::Code;
use taos_query::common::{Precision, RawBlock as Block, Ty};
use taos_query::tmq::{self, AsConsumer, IsData, IsOffset};
use taos_query::{redact_dsn, Dsn, TBuilder};
use taos_ws::consumer::{Data, Offset};
use taos_ws::query::Error;
use taos_ws::{Consumer, TmqBuilder};

use super::*;

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum ws_tmq_conf_res_t {
    WS_TMQ_CONF_UNKNOWN = -2,
    WS_TMQ_CONF_INVALID = -1,
    WS_TMQ_CONF_OK = 0,
}
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum ws_tmq_res_t {
    WS_TMQ_RES_INVALID = -1,   // invalid
    WS_TMQ_RES_DATA = 1,       // 数据
    WS_TMQ_RES_TABLE_META = 2, // 元数据
    WS_TMQ_RES_METADATA = 3,   // 既有元数据又有数据，即自动建表
}

#[repr(C)]
#[derive(Debug, Clone)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub struct ws_tmq_topic_assignment {
    vgId: i32,
    currentOffset: i64,
    begin: i64,
    end: i64,
}

impl From<Code> for ws_tmq_conf_res_t {
    fn from(value: Code) -> Self {
        match value {
            Code::SUCCESS => ws_tmq_conf_res_t::WS_TMQ_CONF_OK,
            _ => ws_tmq_conf_res_t::WS_TMQ_CONF_INVALID,
        }
    }
}

struct TmqConf {
    hsmap: HashMap<String, String>,
}

struct WsTmqList {
    topics: Vec<String>,
}

#[derive(Debug)]
pub struct WsTmqResultSet {
    block: Option<Block>,
    fields: Vec<WS_FIELD>,
    num_of_fields: i32,
    precision: Precision,
    offset: Offset,
    row: ROW,
    data: Data,
    table_name: Option<CString>,
    topic_name: Option<CString>,
    db_name: Option<CString>,
}

impl WsTmqResultSet {
    fn new(block: Block, offset: Offset, data: Data) -> Self {
        let num_of_fields = block.ncols();
        let mut data_vec = Vec::with_capacity(num_of_fields);
        for _col in 0..num_of_fields {
            data_vec.push(std::ptr::null());
        }

        let mut fields = Vec::new();
        fields.extend(block.fields().iter().map(WS_FIELD::from));
        let precision = block.precision();
        let table_name = match block.table_name() {
            Some(name) => CString::new(name).ok(),
            None => None,
        };

        let topic_name = CString::new(offset.topic()).ok();
        let db_name = CString::new(offset.database()).ok();

        Self {
            block: Some(block),
            fields,
            num_of_fields: num_of_fields as i32,
            precision,
            offset,
            row: ROW {
                data: data_vec,
                current_row: 0,
            },
            data,
            table_name,
            topic_name,
            db_name,
        }
    }
}

impl WsResultSetTrait for WsTmqResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        match self.topic_name {
            Some(ref name) => name.as_ptr() as _,
            None => std::ptr::null(),
        }
    }
    fn tmq_get_db_name(&self) -> *const c_char {
        match self.db_name {
            Some(ref name) => name.as_ptr() as _,
            None => std::ptr::null(),
        }
    }
    fn tmq_get_table_name(&self) -> *const c_char {
        match self.table_name {
            Some(ref name) => name.as_ptr() as _,
            None => std::ptr::null(),
        }
    }

    fn tmq_get_offset(&self) -> Offset {
        self.offset.clone()
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        self.offset.offset()
    }
    fn tmq_get_vgroup_id(&self) -> i32 {
        self.offset.vgroup_id()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    fn affected_rows(&self) -> i32 {
        0
    }

    fn affected_rows64(&self) -> i64 {
        0
    }

    fn num_of_fields(&self) -> i32 {
        self.num_of_fields
    }

    fn get_fields(&mut self) -> *const WS_FIELD {
        self.fields.as_ptr()
    }
    fn get_fields_v2(&mut self) -> *const WS_FIELD_V2 {
        std::ptr::null()
    }

    unsafe fn fetch_block(&mut self, ptr: *mut *const c_void, rows: *mut i32) -> Result<(), Error> {
        tracing::trace!("fetch block with ptr {ptr:p}");
        self.block = self.data.fetch_raw_block()?;
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
            self.block = self.data.fetch_raw_block()?;
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
        Duration::from_millis(self.offset.timing() as u64)
    }

    fn stop_query(&mut self) {
        // do nothing
    }
}

#[allow(non_camel_case_types)]
pub type ws_tmq_list_t = c_void;
#[allow(non_camel_case_types)]
pub type ws_tmq_conf_t = c_void;

#[no_mangle]
/// Create a new TMQ configuration object.
pub unsafe extern "C" fn ws_tmq_conf_new() -> *mut ws_tmq_conf_t {
    let tmq_conf: WsMaybeError<TmqConf> = TmqConf {
        hsmap: HashMap::new(),
    }
    .into();
    Box::into_raw(Box::new(tmq_conf)) as _
}

unsafe fn tmq_conf_set(
    conf: *mut ws_tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> WsResult<()> {
    let key = CStr::from_ptr(key).to_str()?.to_string();
    let value = CStr::from_ptr(value).to_str()?.to_string();

    match (conf as *mut WsMaybeError<TmqConf>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(tmq_conf) => {
            match key.to_lowercase().as_str() {
                "group.id" | "client.id" | "td.connect.db" => {}
                "enable.auto.commit" | "msg.with.table.name" => match value.to_lowercase().as_str()
                {
                    "true" | "false" => {}
                    _ => {
                        tracing::trace!("set tmq conf failed, key: {}, value: {}", &key, &value);
                        return Err(WsError::new(Code::INVALID_PARA, "invalid value"));
                    }
                },
                "auto.commit.interval.ms" => match i32::from_str(&value) {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::trace!("set tmq conf failed, key: {}, value: {}", &key, &value);
                        return Err(WsError::new(Code::INVALID_PARA, "invalid value"));
                    }
                },
                "auto.offset.reset" => match value.to_lowercase().as_str() {
                    "none" | "earliest" | "latest" => {}
                    _ => {
                        tracing::trace!("set tmq conf failed, key: {}, value: {}", &key, &value);
                        return Err(WsError::new(Code::INVALID_PARA, "invalid value"));
                    }
                },
                _ => {
                    tracing::trace!("set tmq conf failed, unknow key: {}", &key);
                    return Err(WsError::new(Code::FAILED, "unknow key"));
                }
            }
            tracing::trace!("set tmq conf sucess, key: {}, value: {}", &key, &value);
            tmq_conf.hsmap.insert(key, value);
            Ok(())
        }
        _ => Err(WsError::new(
            Code::OBJECT_IS_NULL,
            "invalid tmq conf Object",
        )),
    }
}

#[no_mangle]
/// Set a configuration property for the TMQ configuration object.
pub unsafe extern "C" fn ws_tmq_conf_set(
    conf: *mut ws_tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> ws_tmq_conf_res_t {
    if conf.is_null() || key.is_null() || value.is_null() {
        return ws_tmq_conf_res_t::WS_TMQ_CONF_INVALID;
    }

    match tmq_conf_set(conf, key, value) {
        Ok(_) => ws_tmq_conf_res_t::WS_TMQ_CONF_OK,
        Err(e) => match e.code {
            Code::INVALID_PARA => ws_tmq_conf_res_t::WS_TMQ_CONF_INVALID,
            _ => ws_tmq_conf_res_t::WS_TMQ_CONF_UNKNOWN,
        },
    }
}

#[no_mangle]
/// Destroy the TMQ configuration object.
pub unsafe extern "C" fn ws_tmq_conf_destroy(conf: *mut ws_tmq_conf_t) -> i32 {
    if !conf.is_null() {
        let _boxed_conf = Box::from_raw(conf as *mut WsMaybeError<TmqConf>);
        return Code::SUCCESS.into();
    }
    get_err_code_fromated(Code::FAILED.into())
}

#[no_mangle]
/// Create a new TMQ topic list object.
pub unsafe extern "C" fn ws_tmq_list_new() -> *mut ws_tmq_list_t {
    let tmq_list: WsMaybeError<WsTmqList> = WsTmqList { topics: Vec::new() }.into();
    Box::into_raw(Box::new(tmq_list)) as _
}

unsafe fn tmq_list_append(list: *mut ws_tmq_list_t, src: *const c_char) -> WsResult<()> {
    let src = CStr::from_ptr(src).to_str()?.to_string();

    match (list as *mut WsMaybeError<WsTmqList>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(list) => {
            if !list.topics.is_empty() {
                tracing::trace!("only support one topic in this websocket library");
                return Err(WsError::new(
                    Code::TMQ_TOPIC_APPEND_ERR,
                    "only support one topic",
                ));
            }

            list.topics.push(src);
            Ok(())
        }
        _ => Err(WsError::new(Code::FAILED, "invalid tmq list Object")),
    }
}

#[no_mangle]
/// Append a topic to the TMQ topic list.
pub unsafe extern "C" fn ws_tmq_list_append(list: *mut ws_tmq_list_t, topic: *const c_char) -> i32 {
    if list.is_null() || topic.is_null() {
        return get_err_code_fromated(Code::OBJECT_IS_NULL.into());
    }

    match tmq_list_append(list, topic) {
        Ok(_) => Code::SUCCESS.into(),
        Err(e) => set_error_and_get_code(e),
    }
}

#[no_mangle]
/// Destroy the TMQ topic list object.
pub unsafe extern "C" fn ws_tmq_list_destroy(list: *mut ws_tmq_list_t) -> i32 {
    if !list.is_null() {
        let _boxed_conf = Box::from_raw(list as *mut WsMaybeError<WsTmqList>);
        return Code::SUCCESS.into();
    }
    set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq list Object"))
}

#[no_mangle]
///  Get the size of the TMQ topic list.
pub unsafe extern "C" fn ws_tmq_list_get_size(list: *mut ws_tmq_list_t) -> i32 {
    match (list as *mut WsMaybeError<WsTmqList>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(list) => list.topics.len() as i32,
        _ => set_error_and_get_code(WsError::new(Code::FAILED, "invalid tmq list Object")),
    }
}

#[no_mangle]
/// Convert the TMQ topic list to a C array.
pub unsafe extern "C" fn ws_tmq_list_to_c_array(
    list: *const ws_tmq_list_t,
    topic_num: *mut u32,
) -> *mut *mut c_char {
    if list.is_null() || topic_num.is_null() {
        return std::ptr::null_mut();
    }

    match (list as *const WsMaybeError<WsTmqList>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(list) => {
            if !list.topics.is_empty() {
                *topic_num = list.topics.len() as u32;
                let mut raw_ptrs: Vec<*mut c_char> = list
                    .topics
                    .iter()
                    .map(|s| CString::new(&**s).expect("CString::new failed"))
                    .map(CString::into_raw)
                    .collect();

                let ptr = raw_ptrs.as_mut_ptr();
                std::mem::forget(raw_ptrs);

                ptr
            } else {
                std::ptr::null_mut()
            }
        }
        _ => std::ptr::null_mut(),
    }
}

#[no_mangle]
/// Free the C array of topic strings.
pub unsafe extern "C" fn ws_tmq_list_free_c_array(
    c_str_arry: *mut *mut c_char,
    topic_num: u32,
) -> i32 {
    if c_str_arry.is_null() {
        return get_err_code_fromated(Code::INVALID_PARA.into());
    }

    for i in 0..topic_num {
        let _ = CString::from_raw(*c_str_arry.offset(i as isize));
    }
    let _ = Vec::from_raw_parts(c_str_arry, 0, topic_num as usize);

    Code::SUCCESS.into()
}

#[allow(non_camel_case_types)]
pub type ws_tmq_t = c_void;

struct WsTmq {
    consumer: Option<Consumer>,
}

impl Drop for WsTmq {
    fn drop(&mut self) {
        if self.consumer.is_some() {
            let _ = self.consumer.take();
        }
    }
}

unsafe fn tmq_consumer_new(conf: *mut ws_tmq_conf_t, dsn: *const c_char) -> WsResult<WsTmq> {
    let dsn = if dsn.is_null() {
        CStr::from_bytes_with_nul(b"taos://localhost:6041\0").unwrap()
    } else {
        CStr::from_ptr(dsn)
    };
    let dsn = dsn.to_str()?;

    tracing::trace!("ws_tmq_consumer_new dsn: {}", redact_dsn(dsn));
    let mut dsn = Dsn::from_str(dsn)?;

    if !conf.is_null() {
        match (conf as *mut WsMaybeError<TmqConf>)
            .as_mut()
            .and_then(|s| s.safe_deref_mut())
        {
            Some(tmq_conf) => {
                let map = &tmq_conf.hsmap;

                for (key, value) in map.iter() {
                    dsn.params.insert(key.clone(), value.clone());
                }
            }
            _ => return Err(WsError::new(Code::FAILED, "invalid tmq conf Object")),
        }
    }

    let builder = TmqBuilder::from_dsn(&dsn)?;
    let consumer = builder.build()?;
    let ws_tmq = WsTmq {
        consumer: Some(consumer),
    };
    Ok(ws_tmq)
}

#[no_mangle]
/// Create a new TMQ consumer.
pub unsafe extern "C" fn ws_tmq_consumer_new(
    conf: *mut ws_tmq_conf_t,
    dsn: *const c_char,
    errstr: *mut c_char,
    errstr_len: c_int,
) -> *mut ws_tmq_t {
    match tmq_consumer_new(conf, dsn) {
        Ok(ws_tmq) => {
            let ws_tmq: WsMaybeError<WsTmq> = ws_tmq.into();
            Box::into_raw(Box::new(ws_tmq)) as _
        }
        Err(e) => {
            if errstr_len > 0 && !errstr.is_null() {
                let error_message = CString::new(e.to_string()).expect("CString::new failed");
                let bytes_to_copy = error_message.to_bytes().len().min(errstr_len as usize - 1);
                std::ptr::copy(error_message.as_ptr(), errstr as *mut c_char, bytes_to_copy);
                // add null terminator
                *errstr.add(bytes_to_copy) = 0;
            }
            set_error_and_get_code(e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
/// Close the TMQ consumer.
pub unsafe extern "C" fn ws_tmq_consumer_close(tmq: *mut ws_tmq_t) -> i32 {
    if tmq.is_null() {
        return get_err_code_fromated(Code::INVALID_PARA.into());
    }

    let _ = Box::from_raw(tmq as *mut WsMaybeError<WsTmq>);
    0
}

#[no_mangle]
/// Subscribe the TMQ consumer to a list of topics.
pub unsafe extern "C" fn ws_tmq_subscribe(
    tmq: *mut ws_tmq_t,
    topic_list: *const ws_tmq_list_t,
) -> i32 {
    if tmq.is_null() || topic_list.is_null() {
        return get_err_code_fromated(Code::INVALID_PARA.into());
    }

    let topic_list = match (topic_list as *const WsMaybeError<WsTmqList>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(topic_list) => topic_list,
        _ => return get_err_code_fromated(Code::INVALID_PARA.into()),
    };

    match (tmq as *mut WsMaybeError<WsTmq>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(ws_tmq) => {
            if let Some(consumer) = &mut ws_tmq.consumer {
                match consumer.subscribe(topic_list.topics.as_slice()) {
                    Ok(_) => Code::SUCCESS.into(),
                    Err(e) => {
                        set_error_and_get_code(WsError::new(Code::FAILED, e.message().as_str()))
                    }
                }
            } else {
                set_error_and_get_code(WsError::new(Code::FAILED, "invalid consumer"))
            }
        }
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object")),
    }
}

#[no_mangle]
/// Unsubscribe the TMQ consumer from all topics.
pub unsafe extern "C" fn ws_tmq_unsubscribe(tmq: *mut ws_tmq_t) -> i32 {
    if tmq.is_null() {
        return get_err_code_fromated(Code::INVALID_PARA.into());
    }

    match (tmq as *mut WsMaybeError<WsTmq>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(ws_tmq) => {
            if let Some(to_drop) = ws_tmq.consumer.take() {
                to_drop.unsubscribe();
            }
            Code::SUCCESS.into()
        }
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object")),
    }
}

unsafe fn tmq_consumer_poll(tmq: *mut ws_tmq_t, timeout: i64) -> WsResult<Option<WsResultSet>> {
    let timeout = match timeout {
        0 => tmq::Timeout::Never,
        n if n < 0 => tmq::Timeout::from_millis(1000),
        _ => tmq::Timeout::from_millis(timeout as u64),
    };

    match (tmq as *mut WsMaybeError<WsTmq>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(ws_tmq) => {
            if let Some(consumer) = &ws_tmq.consumer {
                let r = consumer.recv_timeout(timeout)?;
                match r {
                    Some((offset, message_set)) => {
                        if message_set.has_meta() {
                            return Err(WsError::new(
                                Code::FAILED,
                                "message has meta, only support topic created with select sql",
                            ));
                        }
                        let data = message_set.into_data().unwrap();
                        match data.fetch_raw_block()? {
                            Some(block) => {
                                let rs = WsResultSet::Tmq(WsTmqResultSet::new(block, offset, data));
                                Ok(Some(rs))
                            }
                            None => Ok(None),
                        }
                    }
                    None => Ok(None),
                }
            } else {
                Err(WsError::new(Code::FAILED, "invalid consumer"))
            }
        }
        _ => Err(WsError::new(Code::INVALID_PARA, "invalid tmq Object")),
    }
}

#[no_mangle]
/// Poll the TMQ consumer for messages.
pub unsafe extern "C" fn ws_tmq_consumer_poll(tmq: *mut ws_tmq_t, timeout: i64) -> *mut WS_RES {
    if tmq.is_null() {
        set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object"));
        return std::ptr::null_mut();
    }
    match tmq_consumer_poll(tmq, timeout) {
        Ok(Some(rs)) => {
            let rs: WsMaybeError<WsResultSet> = rs.into();
            Box::into_raw(Box::new(rs)) as _
        }
        Ok(None) => std::ptr::null_mut(),
        Err(e) => {
            set_error_and_get_code(e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
/// Get the topic name from the result object.
pub unsafe extern "C" fn ws_tmq_get_topic_name(rs: *const WS_RES) -> *const c_char {
    if rs.is_null() {
        return std::ptr::null();
    }

    match (rs as *const WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.tmq_get_topic_name(),
        None => std::ptr::null(),
    }
}

#[no_mangle]
/// Get the database name from the result object.
pub unsafe extern "C" fn ws_tmq_get_db_name(rs: *const WS_RES) -> *const c_char {
    if rs.is_null() {
        return std::ptr::null();
    }

    match (rs as *const WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.tmq_get_db_name(),
        None => std::ptr::null(),
    }
}

#[no_mangle]
/// Get the table name from the result object.
pub unsafe extern "C" fn ws_tmq_get_table_name(rs: *const WS_RES) -> *const c_char {
    if rs.is_null() {
        return std::ptr::null();
    }

    match (rs as *const WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.tmq_get_table_name(),
        None => std::ptr::null(),
    }
}

#[no_mangle]
/// Get the vgroup ID from the result object.
pub unsafe extern "C" fn ws_tmq_get_vgroup_id(rs: *const WS_RES) -> i32 {
    if rs.is_null() {
        return get_err_code_fromated(Code::INVALID_PARA.into());
    }

    match (rs as *const WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.tmq_get_vgroup_id(),
        None => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid result Object")),
    }
}
#[no_mangle]
/// Get the vgroup offset from the result object.
pub unsafe extern "C" fn ws_tmq_get_vgroup_offset(rs: *const WS_RES) -> i64 {
    if rs.is_null() {
        return get_err_code_fromated(Code::INVALID_PARA.into()) as _;
    }

    match (rs as *const WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
    {
        Some(rs) => rs.tmq_get_vgroup_offset(),
        None => {
            set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid result Object")) as _
        }
    }
}

#[no_mangle]
/// Get the result type from the result object.
pub unsafe extern "C" fn ws_tmq_get_res_type(rs: *const WS_RES) -> ws_tmq_res_t {
    if rs.is_null() {
        return ws_tmq_res_t::WS_TMQ_RES_INVALID;
    }
    ws_tmq_res_t::WS_TMQ_RES_DATA
}

#[no_mangle]
#[allow(non_snake_case, unused_variables)]
/// Get the topic assignment for the TMQ consumer.
pub unsafe extern "C" fn ws_tmq_get_topic_assignment(
    tmq: *mut ws_tmq_t,
    pTopicName: *const c_char,
    assignment: *mut *mut ws_tmq_topic_assignment,
    numOfAssignment: *mut i32,
) -> i32 {
    if tmq.is_null() {
        return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object"));
    }

    match (tmq as *mut WsMaybeError<WsTmq>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(ws_tmq) => {
            if let Some(consumer) = &mut ws_tmq.consumer {
                match consumer.assignments() {
                    Some(vec) => {
                        if vec.is_empty() {
                            *assignment = std::ptr::null_mut();
                            *numOfAssignment = 0;
                        } else {
                            let (_, assignment_vec) = vec.first().unwrap().clone();

                            *numOfAssignment = assignment_vec.len() as _;
                            *assignment = Box::into_raw(assignment_vec.into_boxed_slice()) as _;
                        }
                        Code::SUCCESS.into()
                    }
                    None => {
                        *assignment = std::ptr::null_mut();
                        *numOfAssignment = 0;
                        Code::SUCCESS.into()
                    }
                }
            } else {
                set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid consumer"))
            }
        }
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid consumer")),
    }
}

#[no_mangle]
#[allow(non_snake_case, unused_variables)]
/// Free the topic assignment.
pub unsafe extern "C" fn ws_tmq_free_assignment(
    pAssignment: *mut ws_tmq_topic_assignment,
    numOfAssignment: i32,
) -> i32 {
    let _ = Vec::from_raw_parts(
        pAssignment,
        numOfAssignment as usize,
        numOfAssignment as usize,
    );
    0
}

unsafe fn tmq_commit_sync(tmq: *mut ws_tmq_t, rs: *const WS_RES) -> WsResult<()> {
    let offset = (rs as *const WsMaybeError<WsResultSet>)
        .as_ref()
        .and_then(|s| s.safe_deref())
        .map(WsResultSetTrait::tmq_get_offset);

    match (tmq as *mut WsMaybeError<WsTmq>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(ws_tmq) => {
            if let Some(consumer) = &ws_tmq.consumer {
                match offset {
                    Some(offset) => match consumer.commit(offset) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(WsError::new(e.code(), &e.message())),
                    },
                    None => match consumer.commit_all() {
                        Ok(_) => Ok(()),
                        Err(e) => Err(WsError::new(e.code(), &e.message())),
                    },
                }
            } else {
                Err(WsError::new(Code::FAILED, "invalid consumer"))
            }
        }
        _ => Err(WsError::new(Code::INVALID_PARA, "invalid tmq Object")),
    }
}
#[no_mangle]
/// Commit the current offset synchronously.
pub unsafe extern "C" fn ws_tmq_commit_sync(tmq: *mut ws_tmq_t, rs: *const WS_RES) -> i32 {
    if tmq.is_null() {
        set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object"))
    } else {
        match tmq_commit_sync(tmq, rs) {
            Ok(_) => Code::SUCCESS.into(),
            Err(e) => set_error_and_get_code(e),
        }
    }
}

#[no_mangle]
/// Commit a specific topic and vgroup offset synchronously.
pub unsafe extern "C" fn ws_tmq_commit_offset_sync(
    tmq: *mut ws_tmq_t,
    #[allow(non_snake_case)] pTopicName: *const c_char,
    #[allow(non_snake_case)] vgId: i32,
    offset: i64,
) -> i32 {
    if tmq.is_null() {
        return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object")) as _;
    }
    let ws_tmq_may_err = match (tmq as *mut WsMaybeError<WsTmq>).as_mut() {
        Some(ws_tmq_may_err) => ws_tmq_may_err,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq object"))
                as _;
        }
    };

    let ws_tmq = match ws_tmq_may_err.safe_deref_mut() {
        Some(ws_tmq) => ws_tmq,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq object"))
                as _;
        }
    };

    let consumer = match &mut ws_tmq.consumer {
        Some(consumer) => consumer,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid consumer"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid topic name"))
                as _;
        }
    };
    match consumer.commit_offset(topic_name, vgId, offset) {
        Ok(_) => Code::SUCCESS.into(),
        Err(e) => {
            ws_tmq_may_err.error = Some(WsError::new(e.code(), &e.to_string()));
            set_error_and_get_code(WsError::new(e.code(), &e.message()))
        }
    }
}

#[no_mangle]
/// Get the committed offset for a specific topic and vgroup.
pub unsafe extern "C" fn ws_tmq_committed(
    tmq: *mut ws_tmq_t,
    #[allow(non_snake_case)] pTopicName: *const c_char,
    #[allow(non_snake_case)] vgId: i32,
) -> i64 {
    if tmq.is_null() {
        return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object")) as _;
    }
    let ws_tmq_may_err = match (tmq as *mut WsMaybeError<WsTmq>).as_mut() {
        Some(ws_tmq_may_err) => ws_tmq_may_err,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq object"))
                as _;
        }
    };

    let ws_tmq = match ws_tmq_may_err.safe_deref_mut() {
        Some(ws_tmq) => ws_tmq,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq object"))
                as _;
        }
    };

    let consumer = match &mut ws_tmq.consumer {
        Some(consumer) => consumer,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid consumer"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid topic name"))
                as _;
        }
    };

    match consumer.committed(topic_name, vgId) {
        Ok(offset) => offset,
        Err(e) => {
            ws_tmq_may_err.error = Some(WsError::new(e.code(), &e.to_string()));
            set_error_and_get_code(WsError::new(e.code(), &e.message())) as _
        }
    }
}

#[no_mangle]
/// Seek to a specific offset for a specific topic and vgroup.
pub unsafe extern "C" fn ws_tmq_offset_seek(
    tmq: *mut ws_tmq_t,
    #[allow(non_snake_case)] pTopicName: *const c_char,
    #[allow(non_snake_case)] vgId: i32,
    offset: i64,
) -> i32 {
    if tmq.is_null() {
        return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object")) as _;
    }
    let ws_tmq_may_err = match (tmq as *mut WsMaybeError<WsTmq>).as_mut() {
        Some(ws_tmq_may_err) => ws_tmq_may_err,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq object"))
                as _;
        }
    };

    let ws_tmq = match ws_tmq_may_err.safe_deref_mut() {
        Some(ws_tmq) => ws_tmq,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq object"))
                as _;
        }
    };

    let consumer = match &mut ws_tmq.consumer {
        Some(consumer) => consumer,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid consumer"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid topic name"))
                as _;
        }
    };

    match consumer.offset_seek(topic_name, vgId, offset) {
        Ok(_) => Code::SUCCESS.into(),
        Err(e) => {
            ws_tmq_may_err.error = Some(WsError::new(e.code(), &e.to_string()));
            set_error_and_get_code(WsError::new(e.code(), &e.message()))
        }
    }
}

#[no_mangle]
/// Get the current position for a specific topic and vgroup.
pub unsafe extern "C" fn ws_tmq_position(
    tmq: *mut ws_tmq_t,
    #[allow(non_snake_case)] pTopicName: *const c_char,
    #[allow(non_snake_case)] vgId: i32,
) -> i64 {
    if tmq.is_null() {
        return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq Object")) as _;
    }
    let ws_tmq_may_err = match (tmq as *mut WsMaybeError<WsTmq>).as_mut() {
        Some(ws_tmq_may_err) => ws_tmq_may_err,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq object"))
                as _;
        }
    };

    let ws_tmq = match ws_tmq_may_err.safe_deref_mut() {
        Some(ws_tmq) => ws_tmq,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid tmq object"))
                as _;
        }
    };

    let consumer = match &mut ws_tmq.consumer {
        Some(consumer) => consumer,
        None => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid consumer"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            return set_error_and_get_code(WsError::new(Code::INVALID_PARA, "invalid topic name"))
                as _;
        }
    };

    match consumer.position(topic_name, vgId) {
        Ok(offset) => offset,
        Err(e) => {
            ws_tmq_may_err.error = Some(WsError::new(e.code(), &e.to_string()));
            set_error_and_get_code(WsError::new(e.code(), &e.message())) as _
        }
    }
}

/// Equivalent to ws_errstr
#[no_mangle]
#[allow(unused_variables)]
pub unsafe extern "C" fn ws_tmq_errstr(tmq: *mut ws_tmq_t) -> *const c_char {
    ws_errstr(std::ptr::null_mut())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tmq_conf() {
        init_env();
        unsafe {
            let taos = ws_connect(b"http://localhost:6041\0" as *const u8 as _);
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

            execute!(b"create database if not exists tmq_test\0");
            execute!(b"use tmq_test\0");
            execute!(b"CREATE STABLE if not exists meters (ts timestamp, current float, voltage int, phase float) TAGS (location varchar(64), group_id int);\0");
            execute!(b"CREATE topic if not exists topic_ws_test as select * from meters;\0");
            execute!(
                b"INSERT INTO d1001 USING meters TAGS ('California.SanFrancisco', 2) VALUES 
                        ('2018-10-03 14:38:05', 10.2, 220, 0.23),
                        ('2018-10-03 14:38:15', 12.6, 218, 0.33),
                        ('2018-10-03 14:38:25', 12.3, 221, 0.31) 
                    d1002 USING meters TAGS ('California.SanFrancisco', 3) VALUES 
                        ('2018-10-03 14:38:04', 10.2, 220, 0.23),
                        ('2018-10-03 14:38:14', 10.3, 218, 0.25),
                        ('2018-10-03 14:38:24', 10.1, 220, 0.22)
                    d1003 USING meters TAGS ('California.LosAngeles', 2) VALUES
                        ('2018-10-03 14:38:06', 11.5, 221, 0.35),
                        ('2018-10-03 14:38:16', 10.4, 220, 0.36),
                        ('2018-10-03 14:38:26', 10.3, 220, 0.33)
                    ;\0"
            );

            let conf = ws_tmq_conf_new();
            let r = ws_tmq_conf_set(
                conf,
                b"group.id\0" as *const u8 as _,
                b"abc\0" as *const u8 as _,
            );
            assert_eq!(r as i32, ws_tmq_conf_res_t::WS_TMQ_CONF_OK as i32);

            let r = ws_tmq_conf_set(
                conf,
                b"client.id\0" as *const u8 as _,
                b"abc\0" as *const u8 as _,
            );
            assert_eq!(r as i32, ws_tmq_conf_res_t::WS_TMQ_CONF_OK as i32);

            let r = ws_tmq_conf_set(
                conf,
                b"auto.offset.reset\0" as *const u8 as _,
                b"earliest\0" as *const u8 as _,
            );
            assert_eq!(r as i32, ws_tmq_conf_res_t::WS_TMQ_CONF_OK as i32);

            let list = ws_tmq_list_new();
            let r = ws_tmq_list_append(list, b"topic_ws_test\0" as *const u8 as *const c_char);
            assert_eq!(r, 0);

            let mut topic_num = 0;
            let c_str_arry = ws_tmq_list_to_c_array(list, &mut topic_num as *mut u32);
            assert!(!c_str_arry.is_null());

            let r = ws_tmq_list_free_c_array(c_str_arry, topic_num);
            assert_eq!(r, 0);

            let consumer = ws_tmq_consumer_new(
                conf,
                b"tmq+ws://localhost:6041\0" as *const u8 as *const c_char,
                std::ptr::null_mut(),
                0,
            );
            assert!(!consumer.is_null());

            let r = ws_tmq_subscribe(consumer, list);
            assert_eq!(r, 0);

            let mut row_count = 0;
            for _i in 0..10 {
                let r = ws_tmq_consumer_poll(consumer, 100);
                if r.is_null() {
                    continue;
                }

                assert_ne!(r, std::ptr::null_mut());

                let rs = Box::from_raw(r as *mut WsMaybeError<WsResultSet>);
                let rs = rs.safe_deref_mut().unwrap();

                if !rs.tmq_get_table_name().is_null() {
                    let table_name = CStr::from_ptr(rs.tmq_get_table_name()).to_str().unwrap();
                    println!("table_name: {}", table_name);
                }

                if !rs.tmq_get_db_name().is_null() {
                    let db_name = CStr::from_ptr(rs.tmq_get_db_name()).to_str().unwrap();
                    println!("db_name: {}", db_name);
                }

                if !rs.tmq_get_topic_name().is_null() {
                    let topic_name = CStr::from_ptr(rs.tmq_get_topic_name()).to_str().unwrap();
                    println!("topic_name: {}", topic_name);
                }

                let offset = rs.tmq_get_vgroup_offset();
                println!("offset: {}", offset);
                let vgroup_id = rs.tmq_get_vgroup_id();
                println!("vgroup_id: {}", vgroup_id);
                let num_of_fields = rs.num_of_fields();
                println!("num_of_fields: {}", num_of_fields);
                let precision = rs.precision();
                println!("precision: {:?}", precision);

                let fields = rs.get_fields();

                let fields_slice = std::slice::from_raw_parts(fields, num_of_fields as usize);

                for field in fields_slice {
                    println!("{:?}", field);
                }

                let mut buffer = Vec::with_capacity(4096);
                buffer.resize(4096, 0);
                let buffer_ptr = buffer.as_mut_ptr();

                loop {
                    let row = ws_fetch_row(r);
                    if row.is_null() {
                        break;
                    }

                    row_count += 1;
                    let rlen =
                        ws_print_row(buffer_ptr, buffer.len() as i32, row, fields, num_of_fields);
                    if rlen > 0 {
                        let row_str = CStr::from_ptr(buffer_ptr).to_str().unwrap();
                        println!("{}", row_str);
                    }
                }
            }

            println!("row_count == {}", row_count);

            let r = ws_tmq_list_destroy(list);
            assert_eq!(r, 0);

            let r = ws_tmq_conf_destroy(conf);
            assert_eq!(r, 0);

            let r = ws_tmq_unsubscribe(consumer);
            assert_eq!(r, 0);

            let r = ws_tmq_consumer_close(consumer);
            assert_eq!(r, 0);

            execute!(b"drop topic if exists topic_ws_test;\0");
            execute!(b"drop database if exists tmq_test\0");
            assert_eq!(ws_close(taos), 0);
        }
    }
}
