use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::str::FromStr;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use taos_error::Code;
use taos_query::common::{Precision, RawBlock as Block, Ty};
use taos_query::tmq::{self, AsConsumer, IsData, IsOffset};
use taos_query::{global_tokio_runtime, Dsn, TBuilder};
use taos_ws::consumer::Data;
use taos_ws::query::Error;
use taos_ws::{Consumer, Offset, TmqBuilder};
use tracing::{debug, error, warn, Instrument};

use crate::ws::error::{
    errno, errstr, format_errno, set_err_and_get_code, TaosError, TaosMaybeError, EMPTY,
};
use crate::ws::{
    self, config, util, ResultSet, ResultSetOperations, Row, SafePtr, TaosResult, TAOS_FIELD,
    TAOS_FIELD_E, TAOS_RES, TAOS_ROW,
};

#[allow(non_camel_case_types)]
pub type tmq_t = c_void;

#[allow(non_camel_case_types)]
pub type tmq_conf_t = c_void;

#[allow(non_camel_case_types)]
pub type tmq_list_t = c_void;

#[allow(non_camel_case_types)]
pub type tmq_commit_cb = extern "C" fn(tmq: *mut tmq_t, code: i32, param: *mut c_void);

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Eq)]
pub enum tmq_conf_res_t {
    TMQ_CONF_UNKNOWN = -2,
    TMQ_CONF_INVALID = -1,
    TMQ_CONF_OK = 0,
}

#[repr(C)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[derive(Debug)]
pub struct tmq_topic_assignment {
    pub vgId: i32,
    pub currentOffset: i64,
    pub begin: i64,
    pub end: i64,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Eq)]
pub enum tmq_res_t {
    TMQ_RES_INVALID = -1,
    TMQ_RES_DATA = 1,
    TMQ_RES_TABLE_META = 2,
    TMQ_RES_METADATA = 3,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct tmq_raw_data {
    pub raw: *mut c_void,
    pub raw_len: u32,
    pub raw_type: u16,
}

pub const TSDB_CLIENT_ID_LEN: usize = 256;
pub const TSDB_CGROUP_LEN: usize = 193;
pub const TSDB_USER_LEN: usize = 24;
pub const TSDB_FQDN_LEN: usize = 128;
pub const TSDB_PASSWORD_LEN: usize = 32;
pub const TSDB_VERSION_LEN: usize = 32;

pub const TSDB_ACCT_ID_LEN: usize = 11;
pub const TSDB_DB_NAME_LEN: usize = 65;
pub const TSDB_NAME_DELIMITER_LEN: usize = 1;
pub const TSDB_DB_FNAME_LEN: usize = TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN;

pub const TSDB_MAX_REPLICA: usize = 5;

#[no_mangle]
pub extern "C" fn tmq_conf_new() -> *mut tmq_conf_t {
    let tmq_conf: TaosMaybeError<TmqConf> = TmqConf::new().into();
    let tmq_conf = Box::into_raw(Box::new(tmq_conf)) as _;
    debug!("tmq_conf_new, tmq_conf: {tmq_conf:?}");
    tmq_conf
}

#[no_mangle]
pub unsafe extern "C" fn tmq_conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> tmq_conf_res_t {
    debug!("tmq_conf_set start, conf: {conf:?}, key: {key:?}, value: {value:?}");

    if conf.is_null() || key.is_null() || value.is_null() {
        error!("tmq_conf_set failed, err: invalid params");
        return tmq_conf_res_t::TMQ_CONF_INVALID;
    }

    match conf_set(conf, key, value) {
        Ok(_) => {
            debug!("tmq_conf_set succ");
            tmq_conf_res_t::TMQ_CONF_OK
        }
        Err(err) => {
            error!("tmq_conf_set failed, err: {err:?}");
            match err.code() {
                Code::INVALID_PARA => tmq_conf_res_t::TMQ_CONF_INVALID,
                _ => tmq_conf_res_t::TMQ_CONF_UNKNOWN,
            }
        }
    }
}

unsafe fn conf_set(
    conf: *mut tmq_conf_t,
    key: *const c_char,
    value: *const c_char,
) -> TaosResult<()> {
    let key = CStr::from_ptr(key).to_str()?.to_string();
    let val = CStr::from_ptr(value).to_str()?.to_string();

    debug!("conf_set, key: {key}, val: {val}");

    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|maybe_err| maybe_err.deref_mut())
    {
        Some(conf) => {
            match key.to_lowercase().as_str() {
                // TODO: TmqInit add user and pass
                // "td.connect.ip" | "td.connect.user" | "td.connect.pass" | "td.connect.db"
                // | "group.id" | "client.id" => {}
                "enable.auto.commit" | "msg.with.table.name" | "enable.replay" => {
                    match val.to_lowercase().as_str() {
                        "true" | "false" => {}
                        _ => {
                            error!("tmq_conf_set failed, err: invalid bool value");
                            return Err(TaosError::new(Code::INVALID_PARA, "invalid bool value"));
                        }
                    }
                }
                "td.connect.port" | "auto.commit.interval.ms" | "session.timeout.ms" => {
                    match u32::from_str(&val) {
                        Ok(_) => {}
                        Err(_) => {
                            error!("tmq_conf_set failed, err: invalid integer value");
                            return Err(TaosError::new(
                                Code::INVALID_PARA,
                                "invalid integer value",
                            ));
                        }
                    }
                }
                // "session.timeout.ms" => match u32::from_str(&val) {
                //     Ok(val) => {
                //         if !(6000..=1800000).contains(&val) {
                //             error!("tmq_conf_set failed, err: session.timeout.ms out of range");
                //             return Err(TaosError::new(
                //                 Code::INVALID_PARA,
                //                 "session.timeout.ms out of range",
                //             ));
                //         }
                //     }
                //     Err(_) => {
                //         error!("tmq_conf_set failed, err: session.timeout.ms is invalid");
                //         return Err(TaosError::new(
                //             Code::INVALID_PARA,
                //             "session.timeout.ms is invalid",
                //         ));
                //     }
                // },
                "max.poll.interval.ms" => match i32::from_str(&val) {
                    Ok(val) => {
                        if val < 1000 {
                            error!("tmq_conf_set failed, err: max.poll.interval.ms out of range");
                            return Err(TaosError::new(
                                Code::INVALID_PARA,
                                "max.poll.interval.ms out of range",
                            ));
                        }
                    }
                    Err(_) => {
                        error!("tmq_conf_set failed, err: max.poll.interval.ms is invalid");
                        return Err(TaosError::new(
                            Code::INVALID_PARA,
                            "max.poll.interval.ms is invalid",
                        ));
                    }
                },
                "auto.offset.reset" => match val.to_lowercase().as_str() {
                    "none" | "earliest" | "latest" => {}
                    _ => {
                        error!("tmq_conf_set failed, err: auto.offset.reset is invalid");
                        return Err(TaosError::new(
                            Code::INVALID_PARA,
                            "auto.offset.reset is invalid",
                        ));
                    }
                },
                "ws.tls.mode" => {
                    let _ = val.parse::<config::WsTlsMode>()?;
                }
                _ => {}
            }
            conf.map.insert(key, val);
            Ok(())
        }
        None => Err(TaosError::new(Code::INVALID_PARA, "conf is null")),
    }
}

#[no_mangle]
pub extern "C" fn tmq_conf_destroy(conf: *mut tmq_conf_t) {
    debug!("tmq_conf_destroy, conf: {conf:?}");
    if !conf.is_null() {
        let _ = unsafe { Box::from_raw(conf as *mut TaosMaybeError<TmqConf>) };
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_conf_set_auto_commit_cb(
    conf: *mut tmq_conf_t,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    debug!("tmq_conf_set_auto_commit_cb start, conf: {conf:?}, cb: {cb:?}, param: {param:?}");

    let fp = cb as *const ();
    if fp.is_null() {
        debug!("tmq_conf_set_auto_commit_cb succ, cb is null");
        return;
    }

    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|maybe_err| maybe_err.deref_mut())
    {
        Some(conf) => {
            conf.auto_commit_cb = Some((cb, param));
            debug!("tmq_conf_set_auto_commit_cb succ");
        }
        None => {
            error!("tmq_conf_set_auto_commit_cb failed, err: conf is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "conf is null"));
        }
    }
}

#[no_mangle]
pub extern "C" fn tmq_list_new() -> *mut tmq_list_t {
    let tmq_list: TaosMaybeError<TmqList> = TmqList::new().into();
    let tmq = Box::into_raw(Box::new(tmq_list)) as _;
    debug!("tmq_list_new, tmq: {tmq:?}");
    tmq
}

#[no_mangle]
pub unsafe extern "C" fn tmq_list_append(list: *mut tmq_list_t, value: *const c_char) -> i32 {
    debug!("tmq_list_append start, list: {list:?}, value: {value:?}");

    if list.is_null() || value.is_null() {
        return format_errno(Code::OBJECT_IS_NULL.into());
    }

    debug!("tmq_list_append, value: {:?}", CStr::from_ptr(value));

    match list_append(list, value) {
        Ok(_) => {
            debug!("tmq_list_append succ");
            0
        }
        Err(err) => {
            error!("tmq_list_append failed, err: {err:?}");
            set_err_and_get_code(err)
        }
    }
}

unsafe fn list_append(list: *mut tmq_list_t, value: *const c_char) -> TaosResult<()> {
    let topic = CStr::from_ptr(value).to_str()?.to_string();
    match (list as *mut TaosMaybeError<TmqList>)
        .as_mut()
        .and_then(|list| list.deref_mut())
    {
        Some(list) => {
            list.topics.push(topic);
            Ok(())
        }
        None => Err(TaosError::new(Code::FAILED, "conf is null")),
    }
}

#[no_mangle]
pub extern "C" fn tmq_list_destroy(list: *mut tmq_list_t) {
    debug!("tmq_list_destroy start, list: {list:?}");
    if !list.is_null() {
        let list = unsafe { Box::from_raw(list as *mut TaosMaybeError<TmqList>) };
        debug!("tmq_list_destroy succ");
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_list_get_size(list: *const tmq_list_t) -> i32 {
    debug!("tmq_list_get_size start, list: {list:?}");
    match (list as *mut TaosMaybeError<TmqList>)
        .as_mut()
        .and_then(|list| list.deref_mut())
    {
        Some(list) => {
            let size = list.topics.len() as i32;
            debug!("tmq_list_get_size succ, size: {size}");
            size
        }
        None => {
            error!("tmq_list_get_size failed, err: list is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "list is null"))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_list_to_c_array(list: *const tmq_list_t) -> *mut *mut c_char {
    debug!("tmq_list_to_c_array start, list: {list:?}");

    match (list as *const TaosMaybeError<TmqList>)
        .as_ref()
        .and_then(|list| list.deref_mut())
    {
        Some(list) => {
            if !list.topics.is_empty() {
                let arr = list
                    .topics
                    .iter()
                    .map(|s| CString::new(&**s).unwrap().into_raw())
                    .collect::<Vec<_>>();
                debug!("tmq_list_to_c_array succ, arr: {:?}", list.topics);
                list.set_c_array(arr);
                list.c_array.as_mut().unwrap().as_mut_ptr()
            } else {
                debug!("tmq_list_to_c_array succ, topics is empty");
                ptr::null_mut()
            }
        }
        None => {
            error!("tmq_list_to_c_array failed, err: list is null");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_consumer_new(
    conf: *mut tmq_conf_t,
    errstr: *mut c_char,
    errstrLen: i32,
) -> *mut tmq_t {
    debug!("tmq_consumer_new start, conf: {conf:?}, errstr: {errstr:?}, errstr_len: {errstrLen}");
    match consumer_new(conf) {
        Ok(tmq) => {
            let tmq: TaosMaybeError<Tmq> = tmq.into();
            let tmq = Box::into_raw(Box::new(tmq)) as _;
            debug!("tmq_consumer_new succ, tmq: {tmq:?}");
            tmq
        }
        Err(err) => {
            error!("tmq_consumer_new failed, err: {err:?}");
            if errstrLen > 0 && !errstr.is_null() {
                let message = CString::new(err.to_string()).unwrap();
                let count = message.to_bytes().len().min(errstrLen as usize - 1);
                ptr::copy(message.as_ptr(), errstr, count);
                *errstr.add(count) = 0;
            }
            set_err_and_get_code(err);
            ptr::null_mut()
        }
    }
}

unsafe fn consumer_new(conf: *mut tmq_conf_t) -> TaosResult<Tmq> {
    match (conf as *mut TaosMaybeError<TmqConf>)
        .as_mut()
        .and_then(|conf| conf.deref_mut())
    {
        Some(conf) => {
            debug!("consumer_new, conf: {conf:?}");

            let ip = conf.map.get("td.connect.ip");

            let port = conf
                .map
                .get("td.connect.port")
                .map_or(0, |s| s.parse().unwrap());

            let user = conf.map.get("td.connect.user");
            let pass = conf.map.get("td.connect.pass");

            let addr = if let Some(ip) = ip {
                let port = util::resolve_port(ip, port);
                format!("{ip}:{port}")
            } else if let Some(addr) = config::adapter_list() {
                addr.to_string()
            } else {
                let host = ws::DEFAULT_HOST;
                let port = util::resolve_port(host, port);
                format!("{host}:{port}")
            };

            let ws_tls_mode = conf
                .map
                .get("ws.tls.mode")
                .map(|s| s.parse::<config::WsTlsMode>())
                .transpose()?;
            let ws_tls_version = conf.map.get("ws.tls.version");
            let ws_tls_ca = conf.map.get("ws.tls.ca");

            let dsn = util::DsnBuilder::new()
                .addr(Some(&addr))
                .user(user.map(|s| s.as_str()))
                .pass(pass.map(|s| s.as_str()))
                .ws_tls_mode(ws_tls_mode)
                .ws_tls_version(ws_tls_version.map(|s| s.as_str()))
                .ws_tls_ca(ws_tls_ca.map(|s| s.as_str()))
                .build();
            let mut dsn = Dsn::from_str(&dsn)?;

            let mut auto_commit = false;
            let mut auto_commit_interval_ms = 5000;

            for (key, val) in conf.map.iter() {
                match key.as_str() {
                    "enable.auto.commit" => {
                        auto_commit = val.to_lowercase().parse().unwrap_or(false);
                        dsn.params.insert(key.clone(), "false".to_string());
                    }
                    "auto.commit.interval.ms" => {
                        auto_commit_interval_ms = val.parse().unwrap_or(5000);
                    }
                    _ => {
                        dsn.params.insert(key.clone(), val.clone());
                    }
                }
            }

            debug!("consumer_new, dsn: {}", dsn.redacted());

            let consumer = TmqBuilder::from_dsn(&dsn)?.build()?;

            Ok(Tmq::new(
                consumer,
                auto_commit,
                auto_commit_interval_ms,
                conf.auto_commit_cb,
            ))
        }
        None => Err(TaosError::new(Code::INVALID_PARA, "conf is null")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_subscribe(tmq: *mut tmq_t, topic_list: *const tmq_list_t) -> i32 {
    debug!("tmq_subscribe start, tmq: {tmq:?}, topic_list: {topic_list:?}");

    let tmq_list = match (topic_list as *const TaosMaybeError<TmqList>)
        .as_ref()
        .and_then(|list| list.deref())
    {
        Some(tmq_list) => tmq_list,
        None => {
            error!("tmq_subscribe failed, err: topic_list is null");
            return format_errno(Code::INVALID_PARA.into());
        }
    };

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => {
            if let Some(consumer) = &mut tmq.consumer {
                match consumer.subscribe(&tmq_list.topics) {
                    Ok(_) => {
                        debug!("tmq_subscribe succ");
                        0
                    }
                    Err(err) => {
                        error!("tmq_subscribe failed, err: {err:?}");
                        set_err_and_get_code(TaosError::new(Code::FAILED, &err.message()))
                    }
                }
            } else {
                error!("tmq_subscribe failed, err: consumer is none");
                set_err_and_get_code(TaosError::new(Code::FAILED, "consumer is none"))
            }
        }
        None => {
            error!("tmq_subscribe failed, err: tmq is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_unsubscribe(tmq: *mut tmq_t) -> i32 {
    debug!("tmq_unsubscribe start, tmq: {tmq:?}");

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => {
            if let Some(consumer) = tmq.consumer.take() {
                consumer.unsubscribe();
            }
            debug!("tmq_unsubscribe succ");
            0
        }
        None => {
            error!("tmq_unsubscribe failed, err: tmq is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_subscription(tmq: *mut tmq_t, topics: *mut *mut tmq_list_t) -> i32 {
    debug!("tmq_subscription start, tmq: {tmq:?}, topics: {topics:?}");

    if topics.is_null() {
        error!("tmq_subscription failed, err: topics is null");
        return format_errno(Code::INVALID_PARA.into());
    }

    if (*topics).is_null() {
        *topics = tmq_list_new();
    }

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => {
            if let Some(consumer) = &mut tmq.consumer {
                let topic_list = consumer.list_topics().unwrap();
                for topic in topic_list {
                    let value = CString::new(topic).unwrap();
                    let code = tmq_list_append(*topics, value.as_ptr());
                    if code != 0 {
                        error!(
                            "tmq_subscription failed, err: tmq_list_append failed, code: {code}"
                        );
                        return code;
                    }
                }
                debug!("tmq_subscription succ");
                0
            } else {
                error!("tmq_subscription failed, err: consumer is none");
                set_err_and_get_code(TaosError::new(Code::FAILED, "consumer is none"))
            }
        }
        None => {
            error!("tmq_subscription failed, err: tmq is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_consumer_poll(tmq: *mut tmq_t, timeout: i64) -> *mut TAOS_RES {
    debug!("tmq_consumer_poll start, tmq: {tmq:?}, timeout: {timeout}");

    if tmq.is_null() {
        error!("tmq_consumer_poll failed, err: tmq is null");
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        return ptr::null_mut();
    }

    match consumer_poll(tmq, timeout) {
        Ok(Some(rs)) => {
            let rs: TaosMaybeError<ResultSet> = rs.into();
            debug!("tmq_consumer_poll succ, rs: {rs:?}");
            Box::into_raw(Box::new(rs)) as _
        }
        Ok(None) => {
            debug!("tmq_consumer_poll succ, rs is none");
            ptr::null_mut()
        }
        Err(err) => {
            error!("tmq_consumer_poll failed, err: {err:?}");
            set_err_and_get_code(err);
            ptr::null_mut()
        }
    }
}

unsafe fn consumer_poll(tmq_ptr: *mut tmq_t, timeout: i64) -> TaosResult<Option<ResultSet>> {
    let timeout = match timeout {
        0 => tmq::Timeout::Never,
        n if n < 0 => tmq::Timeout::from_millis(1000),
        _ => tmq::Timeout::from_millis(timeout as u64),
    };

    match (tmq_ptr as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => match &tmq.consumer {
            Some(consumer) => {
                if tmq.auto_commit {
                    if let Some(offset) = tmq.auto_commit_offset.0.take() {
                        let now = Instant::now();
                        if now - tmq.auto_commit_offset.1
                            > Duration::from_millis(tmq.auto_commit_interval_ms)
                        {
                            tmq.auto_commit_offset.1 = now;
                            debug!("poll auto commit, commit offset: {offset:?}");
                            if let Err(err) = consumer.commit(offset) {
                                error!("poll auto commit failed, err: {err:?}");

                                if let Some((cb, param)) = tmq.auto_commit_cb {
                                    debug!("poll auto commit failed, callback");
                                    cb(tmq_ptr, format_errno(err.code().into()), param);
                                }
                            } else if let Some((cb, param)) = tmq.auto_commit_cb {
                                debug!("poll auto commit succ, callback");
                                cb(tmq_ptr, 0, param);
                            }
                        }
                    }
                }

                let res = consumer.recv_timeout(timeout)?;
                match res {
                    Some((offset, message_set)) => {
                        if tmq.auto_commit {
                            debug!("poll auto commit, set offset: {offset:?}");
                            tmq.auto_commit_offset.0 = Some(offset.clone());
                        }

                        if message_set.has_meta() {
                            return Err(TaosError::new(
                                Code::FAILED,
                                "message has meta, only support topic created with select sql",
                            ));
                        }

                        let data = message_set.into_data().unwrap();
                        match data.fetch_raw_block()? {
                            Some(block) => {
                                Ok(Some(ResultSet::Tmq(TmqResultSet::new(block, offset, data))))
                            }
                            None => Ok(None),
                        }
                    }
                    None => Ok(None),
                }
            }
            None => Err(TaosError::new(Code::FAILED, "consumer is none")),
        },
        None => Err(TaosError::new(Code::INVALID_PARA, "tmq is null")),
    }
}

#[no_mangle]
pub extern "C" fn tmq_consumer_close(tmq: *mut tmq_t) -> i32 {
    debug!("tmq_consumer_close, tmq: {tmq:?}");
    if tmq.is_null() {
        error!("tmq_consumer_close failed, err: tmq is null");
        return format_errno(Code::INVALID_PARA.into());
    }
    let _ = unsafe { Box::from_raw(tmq as *mut TaosMaybeError<Tmq>) };
    0
}

#[no_mangle]
pub unsafe extern "C" fn tmq_commit_sync(tmq: *mut tmq_t, msg: *const TAOS_RES) -> i32 {
    debug!("tmq_commit_sync start, tmq: {tmq:?}, msg: {msg:?}");

    if tmq.is_null() {
        error!("tmq_commit_sync failed, err: tmq is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
    }

    match commit_sync(tmq, msg) {
        Ok(_) => {
            debug!("tmq_commit_sync succ");
            0
        }
        Err(err) => {
            error!("tmq_commit_sync failed, err: {err:?}");
            set_err_and_get_code(err)
        }
    }
}

unsafe fn commit_sync(tmq: *mut tmq_t, res: *const TAOS_RES) -> TaosResult<()> {
    let offset = (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
        .map(ResultSetOperations::tmq_get_offset);

    debug!("commit_sync, offset: {offset:?}");

    let tmq = match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => tmq,
        None => return Err(TaosError::new(Code::INVALID_PARA, "tmq is null")),
    };

    if tmq.consumer.is_none() {
        return Err(TaosError::new(Code::FAILED, "consumer is none"));
    }

    let consumer = tmq.consumer.as_mut().unwrap();

    match offset {
        Some(offset) => match consumer.commit(offset) {
            Ok(_) => Ok(()),
            Err(err) => Err(TaosError::new(err.code(), &err.message())),
        },
        None => match consumer.commit_all() {
            Ok(_) => Ok(()),
            Err(err) => Err(TaosError::new(err.code(), &err.message())),
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_commit_async(
    tmq: *mut tmq_t,
    msg: *const TAOS_RES,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    debug!("tmq_commit_async start, tmq: {tmq:?}, msg: {msg:?}, cb: {cb:?}, param: {param:?}");

    if tmq.is_null() {
        error!("tmq_commit_async failed, err: tmq is null");
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        return;
    }

    let tmq = SafePtr(tmq);
    let res = SafePtr(msg);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let tmq = tmq;
            let res = res;
            let param = param;

            let offset = (res.0 as *const TaosMaybeError<ResultSet>)
                .as_ref()
                .and_then(|rs| rs.deref())
                .map(ResultSetOperations::tmq_get_offset);

            debug!("tmq_commit_async, offset: {offset:?}");

            let maybe_err = (tmq.0 as *mut TaosMaybeError<Tmq>).as_mut().unwrap();

            let tmq1 = match maybe_err.deref_mut() {
                Some(tmq) => tmq,
                None => {
                    error!("tmq_commit_async failed, err: tmq is invalid");
                    maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "tmq is invalid")));
                    let code = format_errno(Code::INVALID_PARA.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            let consumer = match &mut tmq1.consumer {
                Some(consumer) => consumer,
                None => {
                    error!("tmq_commit_async failed, err: consumer is none");
                    maybe_err.with_err(Some(TaosError::new(Code::FAILED, "consumer is none")));
                    let code = format_errno(Code::FAILED.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            match offset {
                Some(offset) => {
                    match taos_query::tmq::AsAsyncConsumer::commit(consumer, offset).await {
                        Ok(_) => {
                            debug!("tmq_commit_async commit callback");
                            cb(tmq.0, 0, param.0);
                        }
                        Err(err) => {
                            error!("tmq_commit_async commit failed, err: {err:?}");
                            let code = format_errno(err.code().into());
                            cb(tmq.0, code, param.0);
                        }
                    }
                }
                None => match taos_query::tmq::AsAsyncConsumer::commit_all(consumer).await {
                    Ok(_) => {
                        debug!("tmq_commit_async commit all callback, commit all succ");
                        cb(tmq.0, 0, param.0);
                    }
                    Err(err) => {
                        error!("tmq_commit_async commit all failed, err: {err:?}");
                        let code = format_errno(err.code().into());
                        cb(tmq.0, code, param.0);
                    }
                },
            }
        }
        .in_current_span(),
    );

    debug!("tmq_commit_async succ");
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_commit_offset_sync(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    debug!("tmq_commit_offset_sync start, tmq: {tmq:?}, p_topic_name: {pTopicName:?}, vg_id: {vgId}, offset: {offset}");

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_commit_offset_sync failed, err: tmq is null");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_commit_offset_sync failed, err: tmq is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid"));
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_commit_offset_sync failed, err: consumer is none");
            return set_err_and_get_code(TaosError::new(Code::FAILED, "consumer is none"));
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_commit_offset_sync failed, err: topic name is invalid");
            return set_err_and_get_code(TaosError::new(
                Code::INVALID_PARA,
                "topic name is invalid",
            ));
        }
    };

    match consumer.commit_offset(topic_name, vgId, offset) {
        Ok(_) => {
            debug!("tmq_commit_offset_sync succ");
            0
        }
        Err(err) => {
            error!("tmq_commit_offset_sync failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message()))
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_commit_offset_async(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
    cb: tmq_commit_cb,
    param: *mut c_void,
) {
    debug!(
        "tmq_commit_offset_async start, tmq: {tmq:?}, p_topic_name: {pTopicName:?}, vg_id: {vgId}, offset: {offset}, cb: {cb:?}, param: {param:?}"
    );

    if tmq.is_null() {
        error!("tmq_commit_offset_async failed, err: tmq is null");
        return;
    }

    if pTopicName.is_null() {
        error!("tmq_commit_offset_async failed, err: p_topic_name is null");
        return;
    }

    let tmq = SafePtr(tmq);
    let p_topic_name = SafePtr(pTopicName);
    let param = SafePtr(param);

    global_tokio_runtime().spawn(
        async move {
            let tmq = tmq;
            let p_topic_name = p_topic_name;
            let param = param;

            let maybe_err = (tmq.0 as *mut TaosMaybeError<Tmq>).as_mut().unwrap();

            let tmq1 = match maybe_err.deref_mut() {
                Some(tmq) => tmq,
                None => {
                    error!("tmq_commit_offset_async failed, err: tmq is invalid");
                    maybe_err.with_err(Some(TaosError::new(Code::INVALID_PARA, "tmq is invalid")));
                    let code = format_errno(Code::INVALID_PARA.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            let consumer = match &mut tmq1.consumer {
                Some(consumer) => consumer,
                None => {
                    error!("tmq_commit_offset_async failed, err: consumer is none");
                    maybe_err.with_err(Some(TaosError::new(Code::FAILED, "consumer is none")));
                    let code = format_errno(Code::FAILED.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            let topic = match CStr::from_ptr(p_topic_name.0).to_str() {
                Ok(topic) => topic,
                Err(_) => {
                    error!("tmq_commit_offset_async failed, err: p_topic_name is invalid utf-8");
                    maybe_err.with_err(Some(TaosError::new(
                        Code::INVALID_PARA,
                        "pTopicName is invalid utf-8",
                    )));
                    let code = format_errno(Code::INVALID_PARA.into());
                    cb(tmq.0, code, param.0);
                    return;
                }
            };

            match taos_query::tmq::AsAsyncConsumer::commit_offset(consumer, topic, vgId, offset)
                .await
            {
                Ok(_) => {
                    debug!("tmq_commit_offset_async callback");
                    cb(tmq.0, 0, param.0);
                }
                Err(err) => {
                    error!("tmq_commit_offset_async failed, err: {err:?}");
                    maybe_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
                    let code = format_errno(err.code().into());
                    cb(tmq.0, code, param.0);
                }
            }
        }
        .in_current_span(),
    );

    debug!("tmq_commit_offset_async succ");
}

static TOPIC_ASSIGNMETN_MAP: Lazy<DashMap<usize, usize>> = Lazy::new(DashMap::new);

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_get_topic_assignment(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    assignment: *mut *mut tmq_topic_assignment,
    numOfAssignment: *mut i32,
) -> i32 {
    debug!("tmq_get_topic_assignment start, tmq: {tmq:?}, p_topic_name: {pTopicName:?}, assignment: {assignment:?}, num_of_assignment: {numOfAssignment:?}");

    match (tmq as *mut TaosMaybeError<Tmq>)
        .as_mut()
        .and_then(|tmq| tmq.deref_mut())
    {
        Some(tmq) => match &mut tmq.consumer {
            Some(consumer) => match consumer.assignments() {
                Some(assigns) => {
                    if assigns.is_empty() {
                        *assignment = ptr::null_mut();
                        *numOfAssignment = 0;
                    } else {
                        let (_, assigns) = assigns.first().unwrap().clone();
                        let len = assigns.len();
                        debug!("tmq_get_topic_assignment, assigns: {assigns:?}, len: {len}");
                        *numOfAssignment = len as _;
                        *assignment = Box::into_raw(assigns.into_boxed_slice()) as _;
                        TOPIC_ASSIGNMETN_MAP.insert(*assignment as usize, len);
                    }
                    debug!("tmq_get_topic_assignment succ",);
                    0
                }
                None => {
                    *assignment = ptr::null_mut();
                    *numOfAssignment = 0;
                    debug!("tmq_get_topic_assignment succ, no assignment");
                    0
                }
            },
            None => {
                error!("tmq_get_topic_assignment failed, err: consumer is none");
                set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "consumer is none"))
            }
        },
        None => {
            error!("tmq_get_topic_assignment failed, err: tmq is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"))
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_free_assignment(pAssignment: *mut tmq_topic_assignment) {
    debug!("tmq_free_assignment start, p_assignment: {pAssignment:?}");
    if pAssignment.is_null() {
        error!("tmq_free_assignment failed, p_assignment is null");
        return;
    }

    if let Some((_, len)) = TOPIC_ASSIGNMETN_MAP.remove(&(pAssignment as usize)) {
        let assigns = Vec::from_raw_parts(pAssignment, len, len);
        debug!("tmq_free_assignment succ, assigns: {assigns:?}, len: {len}");
    } else {
        error!("tmq_free_assignment failed, err: p_assignment is invalid");
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_offset_seek(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
    offset: i64,
) -> i32 {
    debug!(
        "tmq_offset_seek start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}, offset: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId,
        offset
    );

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_offset_seek failed, err: tmq is null");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null"));
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_offset_seek failed, err: tmq is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid"));
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_offset_seek failed, err: consumer is none");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "consumer is none"));
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_offset_seek failed, err: topic name is invalid");
            return set_err_and_get_code(TaosError::new(
                Code::INVALID_PARA,
                "topic name is invalid",
            ));
        }
    };

    match consumer.offset_seek(topic_name, vgId, offset) {
        Ok(_) => {
            debug!("tmq_offset_seek succ");
            0
        }
        Err(err) => {
            error!("tmq_offset_seek failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message()))
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_position(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
) -> i64 {
    debug!(
        "tmq_position start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId
    );

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_position failed, err: tmq is null");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null")) as _;
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_position failed, err: tmq is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid")) as _;
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_position failed, err: consumer is none");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "consumer is none"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_position failed, err: topic name is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "topic name is invalid"))
                as _;
        }
    };

    match consumer.position(topic_name, vgId) {
        Ok(offset) => {
            debug!("tmq_position succ, offset: {offset}");
            offset
        }
        Err(err) => {
            error!("tmq_position failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message())) as _
        }
    }
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn tmq_committed(
    tmq: *mut tmq_t,
    pTopicName: *const c_char,
    vgId: i32,
) -> i64 {
    debug!(
        "tmq_committed start, tmq: {:?}, p_topic_name: {:?}, vg_id: {}",
        tmq,
        CStr::from_ptr(pTopicName),
        vgId
    );

    let may_err = match (tmq as *mut TaosMaybeError<Tmq>).as_mut() {
        Some(may_err) => may_err,
        None => {
            error!("tmq_committed failed, err: tmq is null");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is null")) as _;
        }
    };

    let tmq = match may_err.deref_mut() {
        Some(tmq) => tmq,
        None => {
            error!("tmq_committed failed, err: tmq is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "tmq is invalid")) as _;
        }
    };

    let consumer = match &mut tmq.consumer {
        Some(consumer) => consumer,
        None => {
            error!("tmq_committed failed, err: consumer is none");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "consumer is none"))
                as _;
        }
    };

    let topic_name = match CStr::from_ptr(pTopicName).to_str() {
        Ok(name) => name,
        Err(_) => {
            error!("tmq_committed failed, err: topic name is invalid");
            return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "topic name is invalid"))
                as _;
        }
    };

    match consumer.committed(topic_name, vgId) {
        Ok(offset) => {
            debug!("tmq_committed succ, offset: {offset}");
            offset
        }
        Err(err) => {
            error!("tmq_committed failed, err: {err:?}");
            may_err.with_err(Some(TaosError::new(err.code(), &err.to_string())));
            set_err_and_get_code(TaosError::new(err.code(), &err.message())) as _
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_get_table_name(res: *mut TAOS_RES) -> *const c_char {
    debug!("tmq_get_table_name start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_table_name succ, rs: {rs:?}");
            rs.tmq_get_table_name()
        }
        None => {
            warn!("tmq_get_table_name failed, err: res is null");
            ptr::null()
        }
    }
}

#[no_mangle]
pub extern "C" fn tmq_get_res_type(res: *mut TAOS_RES) -> tmq_res_t {
    debug!("tmq_get_res_type start, res: {res:?}");
    if res.is_null() {
        debug!("tmq_get_res_type succ, res is null");
        return tmq_res_t::TMQ_RES_INVALID;
    }
    debug!("tmq_get_res_type succ");
    tmq_res_t::TMQ_RES_DATA
}

#[no_mangle]
pub unsafe extern "C" fn tmq_get_topic_name(res: *mut TAOS_RES) -> *const c_char {
    debug!("tmq_get_topic_name start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_topic_name succ, rs: {rs:?}");
            rs.tmq_get_topic_name()
        }
        None => {
            error!("tmq_get_topic_name failed, err: res is null");
            ptr::null()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_get_db_name(res: *mut TAOS_RES) -> *const c_char {
    debug!("tmq_get_db_name start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_db_name succ, rs: {rs:?}");
            rs.tmq_get_db_name()
        }
        None => {
            error!("tmq_get_db_name failed, err: res is null");
            ptr::null()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_get_vgroup_id(res: *mut TAOS_RES) -> i32 {
    debug!("tmq_get_vgroup_id start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_vgroup_id succ, rs: {rs:?}");
            rs.tmq_get_vgroup_id()
        }
        None => {
            error!("tmq_get_vgroup_id failed, err: res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null"))
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_get_vgroup_offset(res: *mut TAOS_RES) -> i64 {
    debug!("tmq_get_vgroup_offset start, res: {res:?}");
    match (res as *const TaosMaybeError<ResultSet>)
        .as_ref()
        .and_then(|rs| rs.deref())
    {
        Some(rs) => {
            debug!("tmq_get_vgroup_offset succ, rs: {rs:?}");
            rs.tmq_get_vgroup_offset()
        }
        None => {
            error!("tmq_get_vgroup_offset failed, err: res is null");
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "res is null")) as _
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tmq_err2str(code: i32) -> *const c_char {
    debug!("tmq_err2str, code: {code}");

    let err = match code {
        0 => TaosError::new(Code::SUCCESS, "success"),
        -1 => TaosError::new(Code::FAILED, "fail"),
        _ if code == format_errno(errno()) => return errstr(),
        _ => return EMPTY.as_ptr(),
    };

    set_err_and_get_code(err);
    errstr()
}

#[derive(Debug)]
pub struct TmqResultSet {
    block: Option<Block>,
    fields: Vec<TAOS_FIELD>,
    num_of_fields: i32,
    precision: Precision,
    offset: Offset,
    row: Row,
    data: Data,
    table_name: Option<CString>,
    topic_name: Option<CString>,
    db_name: Option<CString>,
    rows: Vec<*const c_void>,
    offsets: Option<Vec<i32>>,
}

impl TmqResultSet {
    fn new(block: Block, offset: Offset, data: Data) -> Self {
        let mut fields = Vec::new();
        fields.extend(block.fields().iter().map(TAOS_FIELD::from));

        let num_of_fields = block.ncols();
        let row_data = vec![ptr::null(); num_of_fields];
        let precision = block.precision();
        let table_name = block.table_name().and_then(|name| CString::new(name).ok());
        let topic_name = CString::new(offset.topic()).ok();
        let db_name = CString::new(offset.database()).ok();

        Self {
            block: Some(block),
            fields,
            num_of_fields: num_of_fields as i32,
            precision,
            offset,
            row: Row::new(row_data, 0),
            data,
            table_name,
            topic_name,
            db_name,
            rows: vec![ptr::null(); num_of_fields],
            offsets: None,
        }
    }
}

impl ResultSetOperations for TmqResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        match self.topic_name {
            Some(ref name) => name.as_ptr(),
            None => ptr::null(),
        }
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        match self.db_name {
            Some(ref name) => name.as_ptr(),
            None => ptr::null(),
        }
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        match self.table_name {
            Some(ref name) => name.as_ptr(),
            None => ptr::null(),
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

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        self.fields.as_mut_ptr()
    }

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
        ptr::null_mut()
    }

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error> {
        self.block = self.data.fetch_raw_block()?;
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
            self.block = self.data.fetch_raw_block()?;
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
        if self.block.is_none() || self.row.current_row >= self.block.as_ref().unwrap().nrows() {
            self.block = self.data.fetch_raw_block()?;
            self.row.current_row = 0;
        }

        match self.block.as_ref() {
            Some(block) => {
                if block.nrows() == 0 {
                    return Ok(ptr::null_mut());
                }

                for col in 0..block.ncols() {
                    let res = block.get_raw_value_unchecked(self.row.current_row, col);
                    self.row.data[col] = res.2;
                }

                self.row.current_row += 1;
                Ok(self.row.data.as_ptr() as _)
            }
            None => Ok(ptr::null_mut()),
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        if let Some(block) = &self.block {
            if row < block.nrows() && col < block.ncols() {
                return block.get_raw_value_unchecked(row, col);
            }
        }
        (Ty::Null, 0, ptr::null())
    }

    fn take_timing(&mut self) -> Duration {
        Duration::from_millis(self.offset.timing() as u64)
    }

    fn stop_query(&mut self) {}

    unsafe fn is_null_by_column(
        &mut self,
        column_index: usize,
        result: *mut bool,
        rows: *mut c_int,
    ) -> Result<(), TaosError> {
        let block = self.block.as_ref().ok_or_else(|| {
            error!("tmq is_null_by_column failed, block is none");
            TaosError::new(Code::FAILED, "block is none")
        })?;

        let ncols = block.ncols();
        if ncols == 0 || column_index >= ncols {
            error!("tmq is_null_by_column failed, column_index:{column_index} is invalid, columns_num: {ncols}");
            return Err(TaosError::new(
                Code::INVALID_PARA,
                "column index is invalid",
            ));
        }

        unsafe {
            let is_nulls = block.is_null_by_col_unchecked(*rows as _, column_index);
            debug!("tmq is_null_by_column, column_index: {column_index}, is_nulls: {is_nulls:?}");
            *rows = is_nulls.len() as _;
            let res = std::slice::from_raw_parts_mut(result, is_nulls.len());
            for (i, is_null) in is_nulls.iter().enumerate() {
                res[i] = *is_null;
            }
        }
        Ok(())
    }

    fn get_column_data_offset(&mut self, column_index: usize) -> Result<*mut c_int, TaosError> {
        let block = self.block.as_ref().ok_or_else(|| {
            error!("tmq get_column_data_offset failed, block is none");
            TaosError::new(Code::FAILED, "block is none")
        })?;

        let ncols = block.ncols();
        if ncols == 0 || column_index >= ncols {
            error!("tmq get_column_data_offset failed, column_index:{column_index} is invalid, columns_num: {ncols}");
            return Err(TaosError::new(
                Code::INVALID_PARA,
                "column index is invalid",
            ));
        }

        let offsets = block.get_col_data_offset_unchecked(column_index);
        debug!("tmq get_column_data_offset, column_index: {column_index}, offsets: {offsets:?}");
        if offsets.is_empty() {
            return Ok(ptr::null_mut());
        }
        self.offsets = Some(offsets);
        Ok(self.offsets.as_mut().unwrap().as_mut_ptr())
    }
}

#[derive(Debug)]
struct TmqConf {
    map: HashMap<String, String>,
    auto_commit_cb: Option<(tmq_commit_cb, *mut c_void)>,
}

impl TmqConf {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            auto_commit_cb: None,
        }
    }
}

#[derive(Debug)]
struct TmqList {
    topics: Vec<String>,
    c_array: Option<Vec<*mut c_char>>,
}

impl Drop for TmqList {
    fn drop(&mut self) {
        self.free_c_array();
    }
}

impl TmqList {
    fn new() -> Self {
        Self {
            topics: Vec::new(),
            c_array: None,
        }
    }

    fn free_c_array(&mut self) {
        if let Some(arr) = self.c_array.take() {
            for ch in arr {
                let _ = unsafe { CString::from_raw(ch) };
            }
        }
    }

    fn set_c_array(&mut self, arr: Vec<*mut c_char>) {
        self.free_c_array();
        self.c_array = Some(arr);
    }
}

struct Tmq {
    consumer: Option<Consumer>,
    auto_commit: bool,
    auto_commit_interval_ms: u64,
    auto_commit_offset: (Option<Offset>, Instant),
    auto_commit_cb: Option<(tmq_commit_cb, *mut c_void)>,
}

impl Tmq {
    fn new(
        consumer: Consumer,
        auto_commit: bool,
        auto_commit_interval_ms: u64,
        auto_commit_cb: Option<(tmq_commit_cb, *mut c_void)>,
    ) -> Self {
        Self {
            consumer: Some(consumer),
            auto_commit,
            auto_commit_interval_ms,
            auto_commit_offset: (None, Instant::now()),
            auto_commit_cb,
        }
    }
}

impl Drop for Tmq {
    fn drop(&mut self) {
        if self.consumer.is_some() {
            let _ = self.consumer.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use taos_query::util::test_utils::{test_password, test_username};

    use super::*;
    use crate::ws::query::{
        taos_fetch_block, taos_fetch_fields, taos_fetch_row, taos_free_result,
        taos_get_column_data_offset, taos_is_null_by_column, taos_num_fields, taos_print_row,
    };
    use crate::ws::{taos_close, test_connect, test_exec, test_exec_many};

    #[test]
    fn test_tmq_conf() {
        unsafe {
            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"td.connect.ip";
            let val = c"localhost";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.user";
            let val = CString::new(test_username()).unwrap();
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.pass";
            let val = CString::new(test_password()).unwrap();
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"group.id";
            let val = c"1";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"client.id";
            let val = c"1";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"enable.auto.commit";
            let val = c"true";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"msg.with.table.name";
            let val = c"true";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"enable.replay";
            let val = c"true";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.port";
            let val = c"6041";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.commit.interval.ms";
            let val = c"5000";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"session.timeout.ms";
            let val = c"10000";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"max.poll.interval.ms";
            let val = c"10000";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset";
            let val = c"earliest";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            tmq_consumer_close(tmq);
            tmq_conf_destroy(conf);
        }

        unsafe {
            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id";
            let val = c"10";
            let res = tmq_conf_set(conf, key.as_ptr(), val.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            tmq_consumer_close(tmq);
            tmq_conf_destroy(conf);
        }
    }

    #[test]
    fn test_tmq_list() {
        unsafe {
            let list = tmq_list_new();
            assert!(!list.is_null());

            let val = c"topic".as_ptr();
            let res = tmq_list_append(list, val);
            assert_eq!(res, 0);

            let size = tmq_list_get_size(list);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(list);
            assert!(!arr.is_null());

            let arr = tmq_list_to_c_array(list);
            assert!(!arr.is_null());

            tmq_list_destroy(list);
        }
    }

    #[test]
    fn test_tmq_subscribe() {
        unsafe {
            let db = "test_1737357513";
            let topic = "topic_1737357513";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    &format!("create topic {topic} as select * from t0"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let res = tmq_consumer_poll(consumer, 1000);
            let errno = tmq_commit_sync(consumer, res);
            assert_eq!(errno, 0);

            taos_free_result(res);

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_get_topic_assignment() {
        unsafe {
            let db = "test_1737423043";
            let topic = "topic_1737423043";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    &format!("create topic {topic} as select * from t0"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let topic_name = CString::from_str(topic).unwrap();
            let mut assignment = ptr::null_mut();
            let mut num_of_assignment = 0;

            let errno = tmq_get_topic_assignment(
                consumer,
                topic_name.as_ptr(),
                &mut assignment,
                &mut num_of_assignment,
            );
            assert_eq!(errno, 0);
            println!("assignment: {assignment:?}, num_of_assignment: {num_of_assignment}");

            tmq_free_assignment(assignment);

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_offset_seek() {
        unsafe {
            let db = "test_1737440249";
            let topic = "topic_1737440249";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    &format!("create topic {topic} as select * from t0"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let topic_name = CString::from_str(topic).unwrap();
            let mut assignment = ptr::null_mut();
            let mut num_of_assignment = 0;

            let errno = tmq_get_topic_assignment(
                consumer,
                topic_name.as_ptr(),
                &mut assignment,
                &mut num_of_assignment,
            );
            assert_eq!(errno, 0);

            let (_, len) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
            let assigns = Vec::from_raw_parts(assignment, len, len);

            for assign in assigns {
                let offset = tmq_position(consumer, topic_name.as_ptr(), assign.vgId);
                assert!(offset >= 0);

                let errno =
                    tmq_offset_seek(consumer, topic_name.as_ptr(), assign.vgId, assign.begin);
                assert_eq!(errno, 0);
            }

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                let row = taos_fetch_row(res);
                assert!(!row.is_null());

                let fields = taos_fetch_fields(res);
                assert!(!fields.is_null());

                let num_fields = taos_num_fields(res);
                assert_eq!(num_fields, 2);

                let mut str = vec![0 as c_char; 1024];
                let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                println!("str: {:?}, len: {}", CStr::from_ptr(str.as_ptr()), len);

                let errno = tmq_commit_sync(consumer, res);
                assert_eq!(errno, 0);

                taos_free_result(res);
            }

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_commit_offset_sync() {
        unsafe {
            let db = "test_1737444552";
            let topic = "topic_1737444552";
            let table = "t0";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    &format!("create table {table} (ts timestamp, c1 int)"),
                    &format!("insert into {table} values (now, 1)"),
                    &format!("insert into {table} values (now, 2)"),
                    &format!("create topic {topic} as database {db}"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let topic_name = CString::from_str(topic).unwrap();
            let mut assignment = ptr::null_mut();
            let mut num_of_assignment = 0;

            let errno = tmq_get_topic_assignment(
                consumer,
                topic_name.as_ptr(),
                &mut assignment,
                &mut num_of_assignment,
            );
            assert_eq!(errno, 0);

            let (_, len) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
            let assigns = Vec::from_raw_parts(assignment, len, len);

            let mut vg_ids = Vec::new();
            for assign in &assigns {
                vg_ids.push(assign.vgId);
            }

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                let table_name = tmq_get_table_name(res);
                assert!(!table_name.is_null());
                assert_eq!(
                    CStr::from_ptr(table_name),
                    CString::new(table).unwrap().as_c_str()
                );

                let db_name = tmq_get_db_name(res);
                assert!(!db_name.is_null());
                assert_eq!(
                    CStr::from_ptr(db_name),
                    CString::new(db).unwrap().as_c_str()
                );

                let res_type = tmq_get_res_type(res);
                assert_eq!(res_type, tmq_res_t::TMQ_RES_DATA);

                let topic_name = tmq_get_topic_name(res);
                assert!(!topic_name.is_null());
                assert_eq!(
                    CStr::from_ptr(topic_name),
                    CString::new(topic).unwrap().as_c_str()
                );

                let vg_id = tmq_get_vgroup_id(res);
                assert!(vg_ids.contains(&vg_id));

                let offset = tmq_get_vgroup_offset(res);
                assert_eq!(offset, 0);

                let mut current_offset = 0;
                for assign in &assigns {
                    if assign.vgId == vg_id {
                        current_offset = assign.currentOffset;
                        println!("current_offset: {current_offset}");
                        break;
                    }
                }

                let errno = tmq_commit_offset_sync(consumer, topic_name, vg_id, current_offset);
                assert_eq!(errno, 0);

                let committed_offset = tmq_committed(consumer, topic_name, vg_id);
                assert_eq!(committed_offset, current_offset);

                taos_free_result(res);
            }

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_subscription() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1741260674",
                    "drop database if exists test_1741260674",
                    "create database test_1741260674",
                    "use test_1741260674",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "insert into t0 values (now+1s, 2)",
                    "insert into t0 values (now+2s, 3)",
                    "insert into t0 values (now+3s, 4)",
                    "create topic topic_1741260674 as database test_1741260674",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let val = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let val = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = "topic_1741260674";
            let val = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, val.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(tmq, list);
            assert_eq!(errno, 0);

            let mut topics = ptr::null_mut();
            let code = tmq_subscription(tmq, &mut topics);
            assert_eq!(code, 0);
            assert!(!topics.is_null());

            let size = tmq_list_get_size(topics);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(topics);
            assert!(!arr.is_null());

            let slice = std::slice::from_raw_parts(arr, size as usize);
            assert_eq!(CStr::from_ptr(slice[0]), c"topic_1741260674");

            tmq_list_destroy(topics);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            sleep(Duration::from_secs(3));

            let errno = tmq_unsubscribe(tmq);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(tmq);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1741260674",
                    "drop database test_1741260674",
                ],
            );

            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1765179197",
                    "drop database if exists test_1765179197",
                    "create database test_1765179197",
                    "use test_1765179197",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "insert into t0 values (now+1s, 2)",
                    "insert into t0 values (now+2s, 3)",
                    "insert into t0 values (now+3s, 4)",
                    "create topic topic_1765179197 as database test_1765179197",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let val = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let val = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = "topic_1765179197";
            let val = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, val.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(tmq, list);
            assert_eq!(errno, 0);

            let mut topics = ptr::null_mut();
            let code = tmq_subscription(tmq, &mut topics);
            assert_eq!(code, 0);
            assert!(!topics.is_null());

            let size = tmq_list_get_size(topics);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(topics);
            assert!(!arr.is_null());

            let slice = std::slice::from_raw_parts(arr, size as usize);
            assert_eq!(CStr::from_ptr(slice[0]), c"topic_1765179197");

            tmq_list_destroy(topics);

            let mut topics = ptr::null_mut();
            let code = tmq_subscription(tmq, &mut topics);
            assert_eq!(code, 0);
            assert!(!topics.is_null());

            let size = tmq_list_get_size(topics);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(topics);
            assert!(!arr.is_null());

            let slice = std::slice::from_raw_parts(arr, size as usize);
            assert_eq!(CStr::from_ptr(slice[0]), c"topic_1765179197");

            tmq_list_destroy(topics);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            sleep(Duration::from_secs(3));

            let errno = tmq_unsubscribe(tmq);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(tmq);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1765179197",
                    "drop database test_1765179197",
                ],
            );

            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1742281773",
                    "drop database if exists test_1742281773",
                    "create database test_1742281773",
                    "use test_1742281773",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "insert into t0 values (now+1s, 2)",
                    "insert into t0 values (now+2s, 3)",
                    "insert into t0 values (now+3s, 4)",
                    "create topic topic_1742281773 as database test_1742281773",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let val = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let val = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1742281773";
            let errno = tmq_list_append(list, topic.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(tmq, list);
            assert_eq!(errno, 0);

            let mut topics = tmq_list_new();
            let code = tmq_subscription(tmq, &mut topics);
            assert_eq!(code, 0);
            assert!(!topics.is_null());

            let size = tmq_list_get_size(topics);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(topics);
            assert!(!arr.is_null());

            let slice = std::slice::from_raw_parts(arr, size as usize);
            assert_eq!(CStr::from_ptr(slice[0]), c"topic_1742281773");

            tmq_list_destroy(topics);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            sleep(Duration::from_secs(3));

            let errno = tmq_unsubscribe(tmq);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(tmq);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1742281773",
                    "drop database test_1742281773",
                ],
            );

            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1765179295",
                    "drop database if exists test_1765179295",
                    "create database test_1765179295",
                    "use test_1765179295",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    "insert into t0 values (now+1s, 2)",
                    "insert into t0 values (now+2s, 3)",
                    "insert into t0 values (now+3s, 4)",
                    "create topic topic_1765179295 as database test_1765179295",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let val = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let val = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1765179295";
            let errno = tmq_list_append(list, topic.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(tmq, list);
            assert_eq!(errno, 0);

            let mut topics = tmq_list_new();
            let code = tmq_subscription(tmq, &mut topics);
            assert_eq!(code, 0);
            assert!(!topics.is_null());

            let size = tmq_list_get_size(topics);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(topics);
            assert!(!arr.is_null());

            let slice = std::slice::from_raw_parts(arr, size as usize);
            assert_eq!(CStr::from_ptr(slice[0]), c"topic_1765179295");

            tmq_list_destroy(topics);

            let mut topics = tmq_list_new();
            let code = tmq_subscription(tmq, &mut topics);
            assert_eq!(code, 0);
            assert!(!topics.is_null());

            let size = tmq_list_get_size(topics);
            assert_eq!(size, 1);

            let arr = tmq_list_to_c_array(topics);
            assert!(!arr.is_null());

            let slice = std::slice::from_raw_parts(arr, size as usize);
            assert_eq!(CStr::from_ptr(slice[0]), c"topic_1765179295");

            tmq_list_destroy(topics);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            sleep(Duration::from_secs(3));

            let errno = tmq_unsubscribe(tmq);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(tmq);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1765179295",
                    "drop database test_1765179295",
                ],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_commit_async() {
        unsafe {
            extern "C" fn cb(tmq: *mut tmq_t, code: i32, param: *mut c_void) {
                unsafe {
                    assert!(!tmq.is_null());
                    assert_eq!(code, 0);

                    assert!(!param.is_null());
                    assert_eq!("hello", CStr::from_ptr(param as *const _).to_str().unwrap());
                }
            }

            let db = "test_1741264926";
            let topic = "topic_1741264926";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (now, 1)",
                    &format!("create topic {topic} as select * from t0"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let res = tmq_consumer_poll(consumer, 1000);

            let param = c"hello";
            tmq_commit_async(consumer, res, cb, param.as_ptr() as _);

            sleep(Duration::from_secs(1));

            taos_free_result(res);

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_commit_offset_async() {
        unsafe {
            extern "C" fn cb(tmq: *mut tmq_t, code: i32, param: *mut c_void) {
                unsafe {
                    assert!(!tmq.is_null());
                    assert_eq!(code, 0);

                    assert!(!param.is_null());
                    assert_eq!("hello", CStr::from_ptr(param as *const _).to_str().unwrap());
                }
            }

            let db = "test_1741271535";
            let topic = "topic_1741271535";
            let table = "t0";

            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    &format!("drop topic if exists {topic}"),
                    &format!("drop database if exists {db}"),
                    &format!("create database {db}"),
                    &format!("use {db}"),
                    &format!("create table {table} (ts timestamp, c1 int)"),
                    &format!("insert into {table} values (now, 1)"),
                    &format!("insert into {table} values (now, 2)"),
                    &format!("create topic {topic} as database {db}"),
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str(topic).unwrap();
            let errno = tmq_list_append(list, value.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let topic_name = CString::from_str(topic).unwrap();
            let mut assignment = ptr::null_mut();
            let mut num_of_assignment = 0;

            let errno = tmq_get_topic_assignment(
                consumer,
                topic_name.as_ptr(),
                &mut assignment,
                &mut num_of_assignment,
            );
            assert_eq!(errno, 0);

            let (_, len) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
            let assigns = Vec::from_raw_parts(assignment, len, len);

            let mut vg_ids = Vec::new();
            for assign in &assigns {
                vg_ids.push(assign.vgId);
            }

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                let topic_name = tmq_get_topic_name(res);
                assert!(!topic_name.is_null());
                assert_eq!(
                    CStr::from_ptr(topic_name),
                    CString::new(topic).unwrap().as_c_str()
                );

                let vg_id = tmq_get_vgroup_id(res);
                assert!(vg_ids.contains(&vg_id));

                let mut current_offset = 0;
                for assign in &assigns {
                    if assign.vgId == vg_id {
                        current_offset = assign.currentOffset;
                        println!("current_offset: {current_offset}");
                        break;
                    }
                }

                let param = c"hello";
                tmq_commit_offset_async(
                    consumer,
                    topic_name,
                    vg_id,
                    current_offset,
                    cb,
                    param.as_ptr() as _,
                );

                sleep(Duration::from_secs(1));

                let committed_offset = tmq_committed(consumer, topic_name, vg_id);
                assert_eq!(committed_offset, current_offset);

                taos_free_result(res);
            }

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[format!("drop topic {topic}"), format!("drop database {db}")],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_err2str() {
        unsafe {
            let errstr = tmq_err2str(0);
            assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "success");
        }

        unsafe {
            let errstr = tmq_err2str(-1);
            assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "fail");
        }

        unsafe {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid para"));
            let errstr = tmq_err2str(format_errno(Code::INVALID_PARA.into()));
            assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "invalid para");
        }

        unsafe {
            set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "invalid para"));
            let errstr = tmq_err2str(format_errno(Code::COLUMN_EXISTS.into()));
            assert_eq!(CStr::from_ptr(errstr).to_str().unwrap(), "");
        }
    }

    #[test]
    fn test_poll_auto_commit() {
        unsafe {
            extern "C" fn cb(tmq: *mut tmq_t, code: i32, param: *mut c_void) {
                unsafe {
                    println!("call auto commit callback");

                    assert!(!tmq.is_null());
                    assert_eq!(code, 0);

                    assert!(!param.is_null());
                    assert_eq!("hello", CStr::from_ptr(param as *const _).to_str().unwrap());
                }
            }

            let taos = test_connect();
            test_exec_many(
                taos,
                [
                    "drop topic if exists topic_1741333066",
                    "drop database if exists test_1741333066",
                    "create database test_1741333066 wal_retention_period 3600",
                    "use test_1741333066",
                    "create table t0 (ts timestamp, c1 int)",
                    "create topic topic_1741333066 as select * from t0",
                ],
            );

            let num = 9000;
            let ts = 1741336467000i64;
            for i in 0..num {
                test_exec(taos, format!("insert into t0 values ({}, 1)", ts + i));
            }

            test_exec_many(
                taos,
                [
                    "drop database if exists test_1741333142",
                    "create database test_1741333142 wal_retention_period 3600",
                    "use test_1741333142",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let val = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let val = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"enable.auto.commit".as_ptr();
            let val = c"true".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.commit.interval.ms".as_ptr();
            let val = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, val);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let param = c"hello";
            tmq_conf_set_auto_commit_cb(conf, cb, param.as_ptr() as _);

            let mut errstr = [0; 256];
            let tmq = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!tmq.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let value = CString::from_str("topic_1741333066").unwrap();
            let code = tmq_list_append(list, value.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(tmq, list);
            assert_eq!(code, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let topic = CString::from_str("topic_1741333066").unwrap();
            let mut assignment = ptr::null_mut();
            let mut num_of_assignment = 0;

            let code = tmq_get_topic_assignment(
                tmq,
                topic.as_ptr(),
                &mut assignment,
                &mut num_of_assignment,
            );
            assert_eq!(code, 0);

            let (_, len) = TOPIC_ASSIGNMETN_MAP.remove(&(assignment as usize)).unwrap();
            let assigns = Vec::from_raw_parts(assignment, len, len);

            let mut vg_ids = Vec::new();
            for assign in &assigns {
                vg_ids.push(assign.vgId);
            }

            let topic = topic.as_ptr();
            let mut pre_vg_id = None;
            let mut pre_offset = 0;

            loop {
                let res = tmq_consumer_poll(tmq, 1000);
                println!("poll res: {res:?}");
                if res.is_null() {
                    break;
                }

                let vg_id = tmq_get_vgroup_id(res);
                assert!(vg_ids.contains(&vg_id));

                if let Some(pre_vg_id) = pre_vg_id {
                    let offset = tmq_committed(tmq, topic, pre_vg_id);
                    assert!(offset >= pre_offset);
                }

                pre_vg_id = Some(vg_id);
                pre_offset = tmq_get_vgroup_offset(res);

                taos_free_result(res);
            }

            let code = tmq_unsubscribe(tmq);
            assert_eq!(code, 0);

            let code = tmq_consumer_close(tmq);
            assert_eq!(code, 0);

            sleep(Duration::from_secs(3));

            test_exec_many(
                taos,
                [
                    "drop topic topic_1741333066",
                    "drop database test_1741333066",
                ],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_show_consumers() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1744006630",
                    "drop topic if exists topic_1744008820",
                    "drop database if exists test_1744006630",
                    "create database test_1744006630 wal_retention_period 3600",
                    "create topic topic_1744006630 with meta as database test_1744006630",
                    "create topic topic_1744008820 with meta as database test_1744006630",
                    "use test_1744006630",
                    "create table t0(ts timestamp, c1 int)",
                    "insert into t0 values(now, 1)",
                    "insert into t0 values(now+1s, 1)",
                    "insert into t0 values(now+2s, 1)",
                    "insert into t0 values(now+3s, 1)",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1744006630";
            let errno = tmq_list_append(list, topic.as_ptr());
            assert_eq!(errno, 0);

            let topic = c"topic_1744008820";
            let errno = tmq_list_append(list, topic.as_ptr());
            assert_eq!(errno, 0);

            let errno = tmq_subscribe(consumer, list);
            assert_eq!(errno, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                tracing::debug!("poll res: {res:?}");
                if res.is_null() {
                    break;
                }

                if !res.is_null() {
                    let errno = tmq_commit_sync(consumer, res);
                    assert_eq!(errno, 0);

                    taos_free_result(res);
                }
            }

            let errno = tmq_unsubscribe(consumer);
            assert_eq!(errno, 0);

            let errno = tmq_consumer_close(consumer);
            assert_eq!(errno, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1744006630",
                    "drop topic topic_1744008820",
                    "drop database test_1744006630",
                ],
            );

            taos_close(taos);
        }
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_poll_blob() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1753174098",
                    "drop database if exists test_1753174098",
                    "create database test_1753174098",
                    "create topic topic_1753174098 as database test_1753174098",
                    "use test_1753174098",
                    "create table t0 (ts timestamp, c1 int, c2 blob)",
                    "insert into t0 values(1753174694276, 1, '\\x12345678')",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1753174098";
            let code = tmq_list_append(list, topic.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(consumer, list);
            assert_eq!(code, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                tracing::debug!("poll res: {res:?}");
                if res.is_null() {
                    break;
                }

                if !res.is_null() {
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
                        "1753174694276 1 \\x12345678"
                    );

                    let code = tmq_commit_sync(consumer, res);
                    assert_eq!(code, 0);

                    taos_free_result(res);
                }
            }

            let code = tmq_unsubscribe(consumer);
            assert_eq!(code, 0);

            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic topic_1753174098",
                    "drop database test_1753174098",
                ],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_tmq_consumer_new_wss() {
        unsafe {
            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.ip".as_ptr();
            let value = c"localhost".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.port".as_ptr();
            let value = c"6445".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"ws.tls.mode".as_ptr();
            let value = c"1".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            tmq_conf_destroy(conf);
            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);
        }
    }

    #[test]
    fn test_tmq_consumer_new_wss_verify_ca() {
        unsafe {
            let ca_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/certs/ca_verify_ca.crt");
            let tls_ca_path = ca_path.to_str().unwrap();

            let cases = [
                include_str!("../../tests/certs/ca_verify_ca.crt"),
                tls_ca_path,
            ];

            for tls_ca in cases {
                let conf = tmq_conf_new();
                assert!(!conf.is_null());

                let key = c"group.id".as_ptr();
                let value = c"10".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"td.connect.ip".as_ptr();
                let value = c"localhost".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"td.connect.port".as_ptr();
                let value = c"6446".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"ws.tls.mode".as_ptr();
                let value = c"2".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"ws.tls.version".as_ptr();
                let value = c"TLSv1.3".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"ws.tls.ca".as_ptr();
                let value = CString::new(tls_ca).unwrap();
                let res = tmq_conf_set(conf, key, value.as_ptr());
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let mut errstr = [0; 256];
                let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
                assert!(!consumer.is_null());

                tmq_conf_destroy(conf);
                let code = tmq_consumer_close(consumer);
                assert_eq!(code, 0);
            }
        }
    }

    #[test]
    fn test_tmq_consumer_new_wss_verify_identity() {
        unsafe {
            let ca_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/certs/ca_verify_identity.crt");
            let tls_ca_path = ca_path.to_str().unwrap();

            let cases = [
                include_str!("../../tests/certs/ca_verify_identity.crt"),
                tls_ca_path,
            ];

            for tls_ca in cases {
                let conf = tmq_conf_new();
                assert!(!conf.is_null());

                let key = c"group.id".as_ptr();
                let value = c"10".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"td.connect.ip".as_ptr();
                let value = c"localhost".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"td.connect.port".as_ptr();
                let value = c"6445".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"ws.tls.mode".as_ptr();
                let value = c"3".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"ws.tls.version".as_ptr();
                let value = c"TLSv1.3".as_ptr();
                let res = tmq_conf_set(conf, key, value);
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let key = c"ws.tls.ca".as_ptr();
                let value = CString::new(tls_ca).unwrap();
                let res = tmq_conf_set(conf, key, value.as_ptr());
                assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

                let mut errstr = [0; 256];
                let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
                assert!(!consumer.is_null());

                tmq_conf_destroy(conf);
                let code = tmq_consumer_close(consumer);
                assert_eq!(code, 0);
            }
        }
    }

    #[test]
    fn test_taos_fetch_block() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1766654629",
                    "drop database if exists test_1766654629",
                    "create database test_1766654629",
                    "create topic topic_1766654629 as database test_1766654629",
                    "use test_1766654629",
                    "create table t0 (ts timestamp, c1 int, c2 varchar(20))",
                    "insert into t0 values (1741780784749, 2025, 'hello')",
                    "insert into t0 values (1741780784750, 999, null)",
                    "insert into t0 values (1741780784751, null, 'world')",
                    "insert into t0 values (1741780784752, null, null)",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1766654629";
            let code = tmq_list_append(list, topic.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(consumer, list);
            assert_eq!(code, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let mut total_rows = 0;
            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                loop {
                    let mut rows = ptr::null_mut();
                    let num_of_rows = taos_fetch_block(res, &mut rows);
                    if num_of_rows == 0 {
                        break;
                    }
                    total_rows += num_of_rows;
                }

                taos_free_result(res);
            }
            assert_eq!(total_rows, 4);

            let code = tmq_unsubscribe(consumer);
            assert_eq!(code, 0);

            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1766654629",
                    "drop database if exists test_1766654629",
                ],
            );

            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_is_null_by_column() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1766656091",
                    "drop database if exists test_1766656091",
                    "create database test_1766656091",
                    "create topic topic_1766656091 as database test_1766656091",
                    "use test_1766656091",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (1741780784749, 1)",
                    "insert into t0 values (1741780784750, null)",
                    "insert into t0 values (1741780784751, 2)",
                    "insert into t0 values (1741780784752, null)",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1766656091";
            let code = tmq_list_append(list, topic.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(consumer, list);
            assert_eq!(code, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let mut actual_rows = 0;
            let mut actual_results = vec![];

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                loop {
                    let mut rows = ptr::null_mut();
                    let num_of_rows = taos_fetch_block(res, &mut rows);
                    if num_of_rows == 0 {
                        break;
                    }

                    let mut rows = num_of_rows;
                    let mut result = vec![false; num_of_rows as _];
                    let code = taos_is_null_by_column(res, 1, result.as_mut_ptr(), &mut rows);
                    assert_eq!(code, 0);

                    actual_rows += num_of_rows;
                    actual_results.extend_from_slice(&result);
                }

                taos_free_result(res);
            }

            assert_eq!(actual_rows, 4);
            assert_eq!(actual_results, vec![false, true, false, true]);

            let code = tmq_unsubscribe(consumer);
            assert_eq!(code, 0);

            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1766656091",
                    "drop database if exists test_1766656091",
                ],
            );

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
                    "drop topic if exists topic_1766661127",
                    "drop database if exists test_1766661127",
                    "create database test_1766661127",
                    "create topic topic_1766661127 as database test_1766661127",
                    "use test_1766661127",
                    "create table t0 (ts timestamp, c1 varchar(20))",
                    "insert into t0 values (1741780784749, 'hello')",
                    "insert into t0 values (1741780784750, null)",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"topic_1766661127";
            let code = tmq_list_append(list, topic.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(consumer, list);
            assert_eq!(code, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                loop {
                    let mut rows = ptr::null_mut();
                    let num_of_rows = taos_fetch_block(res, &mut rows);
                    if num_of_rows == 0 {
                        break;
                    }

                    let offset = taos_get_column_data_offset(res, 1);
                    assert!(!offset.is_null());
                    let offsets = std::slice::from_raw_parts(offset, num_of_rows as _);
                    println!("offsets: {offsets:?}");
                }

                taos_free_result(res);
            }

            let code = tmq_unsubscribe(consumer);
            assert_eq!(code, 0);

            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1766661127",
                    "drop database if exists test_1766661127",
                ],
            );

            taos_close(taos);
        }
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_tmq_connect_with_token() {
        use crate::ws::query;

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop token if exists token_1772709156",
                    "drop topic if exists topic_1772709156",
                    "drop database if exists test_1772709156",
                    "create database test_1772709156",
                    "create topic topic_1772709156 as database test_1772709156",
                    "use test_1772709156",
                    "create table t0 (ts timestamp, c1 int)",
                    "insert into t0 values (1726803356466, 1)",
                    "insert into t0 values (1726803357466, 2)",
                    "insert into t0 values (1726803358466, 3)",
                    "insert into t0 values (1726803359466, 4)",
                ],
            );

            let sql = CString::new(format!(
                "create token token_1772709156 from user {}",
                test_username()
            ))
            .unwrap();
            let res = query::taos_query(taos, sql.as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 1);

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            let token = CStr::from_ptr(str.as_ptr()).to_str().unwrap();

            taos_free_result(res);

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"1005".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.user".as_ptr();
            let value = c"invalid_user".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.pass".as_ptr();
            let value = c"invalid_pass".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.token".as_ptr();
            let value = CString::new(token).unwrap();
            let res = tmq_conf_set(conf, key, value.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = CString::new("topic_1772709156").unwrap();
            let code = tmq_list_append(list, topic.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(consumer, list);
            assert_eq!(code, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let mut cnt = 0;

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                if res.is_null() {
                    break;
                }

                let fields = taos_fetch_fields(res);
                assert!(!fields.is_null());

                let num_fields = taos_num_fields(res);
                assert_eq!(num_fields, 2);

                loop {
                    let row = taos_fetch_row(res);
                    if row.is_null() {
                        break;
                    }

                    cnt += 1;

                    let mut str = vec![0 as c_char; 1024];
                    let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                    tracing::debug!("{:?}", CStr::from_ptr(str.as_ptr()).to_str().unwrap());
                }

                taos_free_result(res);
            }

            assert_eq!(cnt, 4);

            let code = tmq_unsubscribe(consumer);
            assert_eq!(code, 0);
            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);

            std::thread::sleep(std::time::Duration::from_secs(3));

            test_exec_many(
                taos,
                &[
                    "drop token if exists token_1772709156",
                    "drop topic if exists topic_1772709156",
                    "drop database if exists test_1772709156",
                ],
            );
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_tmq_connect_with_invalid_token() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1772704812",
                    "drop database if exists test_1772704812",
                    "create database test_1772704812",
                    "create topic topic_1772704812 as database test_1772704812",
                ],
            );

            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let key = c"group.id".as_ptr();
            let value = c"10".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.user".as_ptr();
            let value = c"invalid_user".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.pass".as_ptr();
            let value = c"invalid_pass".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.token".as_ptr();
            let value = c"invalid_token".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = CString::new("topic_1772704812").unwrap();
            let code = tmq_list_append(list, topic.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(consumer, list);
            assert_ne!(code, 0);

            let errstr = tmq_err2str(code);
            let errstr = CStr::from_ptr(errstr).to_str().unwrap();
            assert!(errstr.contains("init tscObj with token failed"));

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);

            test_exec_many(
                taos,
                &[
                    "drop topic if exists topic_1772704812",
                    "drop database if exists test_1772704812",
                ],
            );
            taos_close(taos);
        }
    }
}

#[cfg(test)]
mod cloud_tests {
    use std::ffi::CString;

    use super::*;
    use crate::ws::{
        query::{
            taos_fetch_fields, taos_fetch_row, taos_free_result, taos_num_fields, taos_print_row,
        },
        taos_close, taos_connect, test_exec,
    };

    #[test]
    fn test_tmq_poll() {
        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_tmq_poll");
            return;
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_tmq_poll");
            return;
        }

        let url = url.unwrap().strip_prefix("https://").unwrap().to_string();
        let url = CString::new(url).unwrap();
        let token = CString::new(token.unwrap()).unwrap();

        unsafe {
            let conf = tmq_conf_new();
            assert!(!conf.is_null());

            let group_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            let key = c"group.id".as_ptr();
            let value = CString::new(group_id.to_string()).unwrap();
            let res = tmq_conf_set(conf, key, value.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"auto.offset.reset".as_ptr();
            let value = c"earliest".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.ip".as_ptr();
            let res = tmq_conf_set(conf, key, url.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.port".as_ptr();
            let value = c"0".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.user".as_ptr();
            let value = c"token".as_ptr();
            let res = tmq_conf_set(conf, key, value);
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let key = c"td.connect.pass".as_ptr();
            let res = tmq_conf_set(conf, key, token.as_ptr());
            assert_eq!(res, tmq_conf_res_t::TMQ_CONF_OK);

            let mut errstr = [0; 256];
            let consumer = tmq_consumer_new(conf, errstr.as_mut_ptr(), errstr.len() as _);
            assert!(!consumer.is_null());

            let list = tmq_list_new();
            assert!(!list.is_null());

            let topic = c"rust_tmq_test_topic";
            let code = tmq_list_append(list, topic.as_ptr());
            assert_eq!(code, 0);

            let code = tmq_subscribe(consumer, list);
            assert_eq!(code, 0);

            tmq_conf_destroy(conf);
            tmq_list_destroy(list);

            loop {
                let res = tmq_consumer_poll(consumer, 1000);
                tracing::debug!("poll res: {res:?}");
                if res.is_null() {
                    break;
                }

                if !res.is_null() {
                    let fields = taos_fetch_fields(res);
                    assert!(!fields.is_null());

                    let num_fields = taos_num_fields(res);
                    assert_eq!(num_fields, 2);

                    let mut cnt = 0;

                    loop {
                        let row = taos_fetch_row(res);
                        if row.is_null() {
                            break;
                        }

                        cnt += 1;

                        let mut str = vec![0 as c_char; 1024];
                        let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
                        tracing::debug!("{:?}", CStr::from_ptr(str.as_ptr()).to_str().unwrap());
                    }

                    assert_eq!(cnt, 100);

                    let code = tmq_commit_sync(consumer, res);
                    assert_eq!(code, 0);

                    taos_free_result(res);
                }
            }

            let code = tmq_unsubscribe(consumer);
            assert_eq!(code, 0);

            let code = tmq_consumer_close(consumer);
            assert_eq!(code, 0);

            let taos = taos_connect(
                url.as_ptr(),
                c"token".as_ptr(),
                token.as_ptr(),
                ptr::null(),
                0,
            );
            assert!(!taos.is_null());

            test_exec(
                taos,
                format!("drop consumer group `{group_id}` on rust_tmq_test_topic"),
            );

            taos_close(taos);
        }
    }
}
