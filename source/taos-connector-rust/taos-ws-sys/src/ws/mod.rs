#![allow(unused_variables)]

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::ptr;
use std::sync::OnceLock;
use std::time::Duration;

use error::{set_err_and_get_code, TaosError};
use faststr::FastStr;
use query::QueryResultSet;
use sml::SchemalessResultSet;
use taos_error::Code;
use taos_log::QidManager;
use taos_query::common::{Field, Precision, Ty};
use taos_query::util::generate_req_id;
use taos_query::{redact_dsn, TBuilder};
use taos_ws::query::{ConnOption, Error};
use taos_ws::{Offset, Taos, TaosBuilder};
use tmq::TmqResultSet;
use tracing::{debug, error, instrument, trace, warn};

use crate::ws::config::WsTlsMode;

mod config;
pub mod error;
pub mod query;
pub mod sml;
pub mod stmt;
pub mod stmt2;
pub mod stub;
pub mod tmq;
pub mod util;

pub type TAOS = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_RES = c_void;

#[allow(non_camel_case_types)]
pub type TAOS_ROW = *mut *mut c_void;

#[allow(non_camel_case_types)]
pub type __taos_async_fn_t = extern "C" fn(param: *mut c_void, res: *mut TAOS_RES, code: c_int);

const DEFAULT_DSN: &str = "ws://localhost:6041";
const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: u16 = 6041;
const DEFAULT_CLOUD_PORT: u16 = 443;
const DEFAULT_USER: &str = "root";
const DEFAULT_PASS: &str = "taosdata";
const DEFAULT_DB: &str = "";

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum TSDB_OPTION {
    TSDB_OPTION_LOCALE,
    TSDB_OPTION_CHARSET,
    TSDB_OPTION_TIMEZONE,
    TSDB_OPTION_CONFIGDIR,
    TSDB_OPTION_SHELL_ACTIVITY_TIMER,
    TSDB_OPTION_USE_ADAPTER,
    TSDB_OPTION_DRIVER,
    TSDB_MAX_OPTIONS,
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum TSDB_OPTION_CONNECTION {
    TSDB_OPTION_CONNECTION_CLEAR = -1,
    TSDB_OPTION_CONNECTION_CHARSET,
    TSDB_OPTION_CONNECTION_TIMEZONE,
    TSDB_OPTION_CONNECTION_USER_IP,
    TSDB_OPTION_CONNECTION_USER_APP,
    TSDB_MAX_OPTIONS_CONNECTION,
}

type TaosResult<T> = Result<T, TaosError>;

#[derive(Debug, Clone)]
pub struct SafePtr<T>(pub T);

unsafe impl<T> Send for SafePtr<T> {}
unsafe impl<T> Sync for SafePtr<T> {}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD_E {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
}

impl TAOS_FIELD_E {
    pub fn new(field: &Field, precision: u8, scale: u8) -> Self {
        let mut name = [0 as c_char; 65];
        let field_name = field.name();
        unsafe {
            ptr::copy_nonoverlapping(
                field_name.as_ptr(),
                name.as_mut_ptr() as _,
                field_name.len(),
            );
        };

        Self {
            name,
            r#type: field.ty() as _,
            precision,
            scale,
            bytes: field.bytes() as _,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct TAOS_FIELD {
    pub name: [c_char; 65],
    pub r#type: i8,
    pub bytes: i32,
}

impl From<&Field> for TAOS_FIELD {
    fn from(field: &Field) -> Self {
        let mut name = [0 as c_char; 65];
        let field_name = field.name();
        unsafe {
            ptr::copy_nonoverlapping(
                field_name.as_ptr(),
                name.as_mut_ptr() as _,
                field_name.len(),
            );
        };

        Self {
            name,
            r#type: field.ty() as i8,
            bytes: field.bytes() as i32,
        }
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_connect(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    taos_init();
    match connect(ip, user, pass, db, port, ptr::null(), ptr::null()) {
        Ok(taos) => Box::into_raw(Box::new(taos)) as _,
        Err(err) => {
            error!("taos_connect failed, err: {err:?}");
            handle_connect_error(err)
        }
    }
}

unsafe fn connect(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    port: u16,
    totp_code: *const c_char,
    bearer_token: *const c_char,
) -> TaosResult<Taos> {
    let addr = if !ip.is_null() {
        let ip = CStr::from_ptr(ip).to_str()?;
        let port = util::resolve_port(ip, port);
        format!("{ip}:{port}")
    } else if let Some(addr) = config::adapter_list() {
        addr.to_string()
    } else {
        let host = DEFAULT_HOST;
        let port = util::resolve_port(host, port);
        format!("{host}:{port}")
    };

    let user = util::c_char_to_str(user)?;
    let pass = util::c_char_to_str(pass)?;
    let db = util::c_char_to_str(db)?;
    let totp_code = util::c_char_to_str(totp_code)?;
    let bearer_token = util::c_char_to_str(bearer_token)?;

    let dsn = util::DsnBuilder::new()
        .addr(Some(&addr))
        .user(user)
        .pass(pass)
        .db(db)
        .totp_code(totp_code)
        .bearer_token(bearer_token)
        .build();

    connect_from_dsn(&dsn)
}

fn handle_connect_error(mut err: TaosError) -> *mut TAOS {
    use taos_ws::query::asyn::WS_ERROR_NO;
    if err.code() == WS_ERROR_NO::WEBSOCKET_ERROR.as_code() {
        err = TaosError::new(Code::new(0x000B), "Unable to establish connection");
    }
    set_err_and_get_code(err);
    ptr::null_mut()
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_connect_with_dsn(dsn: *const c_char) -> *mut TAOS {
    match connect_with_dsn(dsn) {
        Ok(taos) => Box::into_raw(Box::new(taos)) as _,
        Err(err) => {
            error!("taos_connect_with_dsn failed, err: {err:?}");
            handle_connect_error(err)
        }
    }
}

unsafe fn connect_with_dsn(dsn: *const c_char) -> TaosResult<Taos> {
    let dsn = if !dsn.is_null() {
        CStr::from_ptr(dsn).to_str()?
    } else {
        DEFAULT_DSN
    };
    connect_from_dsn(dsn)
}

fn connect_from_dsn(dsn: &str) -> TaosResult<Taos> {
    debug!("Connecting with dsn: {}", redact_dsn(dsn));
    let builder = TaosBuilder::from_dsn(dsn)?;

    #[cfg(all(windows, target_pointer_width = "32"))]
    {
        tracing::debug!("Spawning thread to avoid stack overflow on 32-bit Windows");
        const STACK_SIZE: usize = 4 * 1024 * 1024;
        let handle = std::thread::Builder::new()
            .stack_size(STACK_SIZE)
            .spawn(move || {
                builder.build().and_then(|mut taos| {
                    builder.ping(&mut taos)?;
                    Ok(taos)
                })
            })
            .map_err(|e| TaosError::new(Code::FAILED, &format!("spawn thread failed: {e}")))?;
        match handle.join() {
            Ok(Ok(taos)) => Ok(taos),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(TaosError::new(Code::FAILED, "connect thread panicked")),
        }
    }

    #[cfg(not(all(windows, target_pointer_width = "32")))]
    {
        let mut taos = builder.build()?;
        builder.ping(&mut taos)?;
        Ok(taos)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_close(taos: *mut TAOS) {
    if taos.is_null() {
        set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
        return;
    }
    let _ = Box::from_raw(taos as *mut Taos);
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_options(option: TSDB_OPTION, arg: *const c_void, _: ...) -> c_int {
    match option {
        TSDB_OPTION::TSDB_OPTION_CONFIGDIR => {
            if arg.is_null() {
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "taos cfg dir is null",
                ));
            }
            let dir = CStr::from_ptr(arg as _);
            if let Ok(dir) = dir.to_str() {
                config::set_config_dir(FastStr::new(dir));
                0
            } else {
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "taos cfg dir is invalid CStr",
                ));
            }
        }
        TSDB_OPTION::TSDB_OPTION_TIMEZONE => {
            if arg.is_null() {
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "taos timezone is null",
                ));
            }
            let tz = CStr::from_ptr(arg as _);
            if let Ok(tz) = tz.to_str() {
                config::set_timezone(FastStr::new(tz));
                0
            } else {
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "taos timezone is invalid CStr",
                ));
            }
        }
        _ => 0,
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_options_connection(
    taos: *mut TAOS,
    option: TSDB_OPTION_CONNECTION,
    arg: *const c_void,
) -> c_int {
    if taos.is_null() {
        error!("taos_options_connection failed, taos is null");
        return set_err_and_get_code(TaosError::new(Code::INVALID_PARA, "taos is null"));
    }

    let value = if !arg.is_null() {
        Some(match CStr::from_ptr(arg as *const c_char).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                error!("taos_options_connection failed, arg is invalid utf-8");
                return set_err_and_get_code(TaosError::new(
                    Code::INVALID_PARA,
                    "arg is invalid utf-8",
                ));
            }
        })
    } else {
        None
    };

    if option == TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_CHARSET {
        let valid = value
            .as_deref()
            .is_none_or(|v| v.eq_ignore_ascii_case("utf-8"));

        if !valid {
            let charset = value.unwrap();
            error!("taos_options_connection failed, unsupported charset: {charset}",);
            return set_err_and_get_code(TaosError::new(
                Code::INVALID_PARA,
                &format!("unsupported charset: {charset}"),
            ));
        }
        return 0;
    }

    let taos = &mut *(taos as *mut Taos);
    let option = ConnOption {
        option: option as i32,
        value,
    };

    match taos_query::block_in_place_or_global(taos.client().options_connection(&[option])) {
        Ok(()) => {
            debug!("taos_options_connection succ");
            0
        }
        Err(err) => {
            error!("taos_options_connection failed, err: {err:?}");
            set_err_and_get_code(err.into())
        }
    }
}

#[derive(Clone)]
struct Qid(u64);

impl QidManager for Qid {
    fn init() -> Self {
        Self(generate_req_id())
    }

    fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub extern "C" fn taos_init() -> c_int {
    static ONCE: OnceLock<c_int> = OnceLock::new();
    *ONCE.get_or_init(|| {
        if let Err(e) = taos_init_impl() {
            error!("taos_init failed, err: {e:?}");
            eprintln!("taos_init failed, err: {e:?}");
        }
        0
    })
}

fn taos_init_impl() -> Result<(), Box<dyn std::error::Error>> {
    use taos_log::layer::TaosLayer;
    use taos_log::writer::RollingFileAppender;
    use tracing_log::LogTracer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;

    unsafe {
        let locale = util::get_system_locale();
        let locale = std::ffi::CString::new(locale).unwrap();
        let locale_ptr = libc::setlocale(libc::LC_CTYPE, locale.as_ptr());
        if locale_ptr.is_null() {
            return Err(TaosError::new(Code::FAILED, "setlocale failed").into());
        }
    }

    if let Err(err) = config::init() {
        return Err(TaosError::new(Code::FAILED, &err).into());
    }

    util::set_tz_env(config::timezone().as_str());

    let mut layers = Vec::new();
    let log_dir = config::log_dir();

    let appender = RollingFileAppender::builder(log_dir.as_str())
        .keep_days(config::log_keep_days())
        .rotation_size(&config::rotation_size())
        .build()?;

    layers.push(
        TaosLayer::<Qid>::new(appender)
            .with_location()
            .with_filter(config::log_level())
            .boxed(),
    );

    if config::log_output_to_screen() {
        layers.push(
            TaosLayer::<Qid, _, _>::new(std::io::stdout)
                .with_location()
                .with_filter(config::log_level())
                .boxed(),
        );
    }

    tracing_subscriber::registry().with(layers).init();
    LogTracer::init()?;
    debug!("taos_init, config: {:?}", config::config());
    config::print();
    Ok(())
}

#[repr(C)]
pub struct OPTIONS {
    pub keys: [*const c_char; 256],
    pub values: [*const c_char; 256],
    pub count: u16,
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_set_option(
    options: *mut OPTIONS,
    key: *const c_char,
    value: *const c_char,
) {
    if options.is_null() || key.is_null() || value.is_null() {
        let _ = set_err_and_get_code(TaosError::new(
            Code::INVALID_PARA,
            "options, key or value is null",
        ));
        return;
    }

    let opts = &mut *options;
    let count = opts.count as usize;
    if count >= opts.keys.len() {
        warn!(
            "taos_set_option overflow, count: {}, capacity: {}",
            opts.count,
            opts.keys.len()
        );
        let _ = set_err_and_get_code(TaosError::new(
            Code::INVALID_PARA,
            "options capacity exceeded",
        ));
        return;
    }

    trace!(
        "taos_set_option insert, index: {count}, key: {}",
        CStr::from_ptr(key).to_string_lossy(),
    );

    opts.keys[opts.count as usize] = key;
    opts.values[opts.count as usize] = value;
    opts.count += 1;
}

const IP: &str = "ip";
const PORT: &str = "port";
const USER: &str = "user";
const PASS: &str = "pass";
const DB: &str = "db";

const COMPRESSION: &str = "compression";
const ADAPTER_LIST: &str = "adapterList";
const CONN_RETRIES: &str = "connRetries";
const RETRY_BACKOFF_MS: &str = "retryBackoffMs";
const RETRY_BACKOFF_MAX_MS: &str = "retryBackoffMaxMs";
const WS_TLS_MODE: &str = "wsTlsMode";
const WS_TLS_VERSION: &str = "wsTlsVersion";
const WS_TLS_CA: &str = "wsTlsCa";
const TOKEN: &str = "token";

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_connect_with(options: *const OPTIONS) -> *mut TAOS {
    taos_init();
    match connect_with(options) {
        Ok(taos) => Box::into_raw(Box::new(taos)) as _,
        Err(err) => {
            error!("taos_connect_with failed, err: {err:?}");
            handle_connect_error(err)
        }
    }
}

unsafe fn connect_with(options: *const OPTIONS) -> TaosResult<Taos> {
    let dsn = build_dsn_from_options(options)?;
    connect_from_dsn(&dsn)
}

unsafe fn build_dsn_from_options(options: *const OPTIONS) -> TaosResult<String> {
    if options.is_null() {
        let dsn = util::DsnBuilder::new().build();
        debug!(
            "build_dsn_from_options, options is null, use default dsn: {}",
            redact_dsn(&dsn)
        );
        return Ok(dsn);
    }

    let opts = &*options;
    let count = opts.count as usize;
    if count > opts.keys.len() || count > opts.values.len() {
        error!(
            "build_dsn_from_options, invalid count: {count}, exceeds arrays len: {}",
            opts.keys.len()
        );
        return Err(TaosError::new(
            Code::INVALID_PARA,
            "options count out of range",
        ));
    }

    let mut map = HashMap::with_capacity(count);
    for i in 0..count {
        if opts.keys[i].is_null() || opts.values[i].is_null() {
            warn!("build_dsn_from_options, index {i} has null key/value, skip");
            continue;
        }
        let key = CStr::from_ptr(opts.keys[i]).to_str()?;
        let value = CStr::from_ptr(opts.values[i]).to_str()?;
        if let Some(value) = map.insert(key, value) {
            debug!("build_dsn_from_options, duplicate key: {key}, overwrite previous value");
        }
    }
    trace!("build_dsn_from_options, raw options: {:?}", SecretMap(&map));

    let user = map.remove(USER).unwrap_or(DEFAULT_USER);
    let pass = map.remove(PASS).unwrap_or(DEFAULT_PASS);
    let db = map.remove(DB).unwrap_or(DEFAULT_DB);

    let port = map
        .remove(PORT)
        .map_or(0, |s| s.parse::<u16>().unwrap_or(0));

    let addr = match map.remove(ADAPTER_LIST) {
        Some(addr) => {
            debug!("build_dsn_from_options, use adapterList option: {addr}");
            addr.to_string()
        }
        None => {
            match map.remove(IP) {
                Some(host) => {
                    let port = util::resolve_port(host, port);
                    let addr = format!("{host}:{port}");
                    debug!("build_dsn_from_options, use host: {host}, address: {addr}");
                    addr
                }
                None => match config::adapter_list() {
                    Some(addr) => {
                        debug!("build_dsn_from_options, use adapterList config: {addr}");
                        addr.to_string()
                    }
                    None => {
                        let host = DEFAULT_HOST;
                        let port = util::resolve_port(host, port);
                        let addr = format!("{host}:{port}");
                        debug!("build_dsn_from_options, fallback default host: {host}, address: {addr}");
                        addr
                    }
                },
            }
        }
    };

    let conn_retries = config::conn_retries().to_string();
    let retry_backoff_ms = config::retry_backoff_ms().to_string();
    let retry_backoff_max_ms = config::retry_backoff_max_ms().to_string();

    if !map.contains_key(CONN_RETRIES) {
        map.insert(CONN_RETRIES, &conn_retries);
    }
    if !map.contains_key(RETRY_BACKOFF_MS) {
        map.insert(RETRY_BACKOFF_MS, &retry_backoff_ms);
    }
    if !map.contains_key(RETRY_BACKOFF_MAX_MS) {
        map.insert(RETRY_BACKOFF_MAX_MS, &retry_backoff_max_ms);
    }

    let compression = match map.remove(COMPRESSION) {
        Some(s) => match s.trim() {
            "1" => "true".to_string(),
            "0" => "false".to_string(),
            _ => {
                return Err(TaosError::new(
                    Code::INVALID_PARA,
                    &format!("invalid value for {COMPRESSION}: {s}"),
                ))
            }
        },
        None => config::compression().to_string(),
    };
    map.insert(COMPRESSION, &compression);

    let ws_tls_mode = map
        .remove(WS_TLS_MODE)
        .map(|s| s.parse::<WsTlsMode>())
        .transpose()?
        .unwrap_or(config::ws_tls_mode());

    let protocol = match ws_tls_mode {
        WsTlsMode::Disabled => "ws",
        _ => "wss",
    };

    if ws_tls_mode == WsTlsMode::VerifyCa {
        map.insert("tls_mode", "verify_ca");
    } else if ws_tls_mode == WsTlsMode::VerifyIdentity {
        map.insert("tls_mode", "verify_identity");
    };

    let tls_version = map
        .remove(WS_TLS_VERSION)
        .map_or_else(|| config::ws_tls_version().to_string(), |s| s.to_string());
    map.insert("tls_version", &tls_version);

    let tls_ca = match map.remove(WS_TLS_CA) {
        Some(ca) => Some(ca.to_string()),
        None => config::ws_tls_ca().map(|ca| ca.to_string()),
    };
    if let Some(ca) = &tls_ca {
        map.insert("tls_ca", ca);
    }

    let mut params = String::with_capacity(64);
    for (i, (&key, &value)) in map.iter().enumerate() {
        let key = util::camel_to_snake(key);
        if i > 0 {
            params.push('&');
        }
        params.push_str(&key);
        params.push('=');
        params.push_str(value);
    }

    Ok(format!("{protocol}://{user}:{pass}@{addr}/{db}?{params}"))
}

struct SecretMap<'a>(&'a HashMap<&'a str, &'a str>);

impl<'a> std::fmt::Debug for SecretMap<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg = f.debug_map();
        for (k, v) in self.0.iter() {
            if matches!(*k, PASS | TOKEN) {
                dbg.entry(k, &"[REDACTED]");
            } else {
                dbg.entry(k, v);
            }
        }
        dbg.finish()
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_connect_totp(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    totp: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    taos_init();
    match connect(ip, user, pass, db, port, totp, ptr::null()) {
        Ok(taos) => Box::into_raw(Box::new(taos)) as _,
        Err(err) => {
            error!("taos_connect_totp failed, err: {err:?}");
            handle_connect_error(err)
        }
    }
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_connect_test(
    ip: *const c_char,
    user: *const c_char,
    pass: *const c_char,
    totp: *const c_char,
    db: *const c_char,
    port: u16,
) -> c_int {
    let taos = taos_connect_totp(ip, user, pass, totp, db, port);
    if taos.is_null() {
        return error::errno();
    }
    taos_close(taos);
    0
}

#[no_mangle]
#[instrument(level = "debug", ret)]
pub unsafe extern "C" fn taos_connect_token(
    ip: *const c_char,
    token: *const c_char,
    db: *const c_char,
    port: u16,
) -> *mut TAOS {
    taos_init();
    match connect(ip, ptr::null(), ptr::null(), db, port, ptr::null(), token) {
        Ok(taos) => Box::into_raw(Box::new(taos)) as _,
        Err(err) => {
            error!("taos_connect_token failed, err: {err:?}");
            handle_connect_error(err)
        }
    }
}

#[derive(Debug)]
struct Row {
    data: Vec<*const c_void>,
    current_row: usize,
}

impl Row {
    fn new(data: Vec<*const c_void>, current_row: usize) -> Self {
        Self { data, current_row }
    }
}

pub trait ResultSetOperations {
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

    fn get_fields(&mut self) -> *mut TAOS_FIELD;

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E;

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error>;

    unsafe fn fetch_block(&mut self, rows: *mut TAOS_ROW, num: *mut c_int) -> Result<(), Error>;

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error>;

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void);

    fn take_timing(&mut self) -> Duration;

    fn stop_query(&mut self);

    unsafe fn is_null_by_column(
        &mut self,
        column_index: usize,
        result: *mut bool,
        rows: *mut c_int,
    ) -> Result<(), TaosError>;

    fn get_column_data_offset(&mut self, column_index: usize) -> Result<*mut c_int, TaosError>;
}

#[derive(Debug)]
pub enum ResultSet {
    Query(QueryResultSet),
    Schemaless(SchemalessResultSet),
    Tmq(TmqResultSet),
}

impl ResultSetOperations for ResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_topic_name(),
            ResultSet::Schemaless(rs) => rs.tmq_get_topic_name(),
            ResultSet::Tmq(rs) => rs.tmq_get_topic_name(),
        }
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_db_name(),
            ResultSet::Schemaless(rs) => rs.tmq_get_db_name(),
            ResultSet::Tmq(rs) => rs.tmq_get_db_name(),
        }
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_table_name(),
            ResultSet::Schemaless(rs) => rs.tmq_get_table_name(),
            ResultSet::Tmq(rs) => rs.tmq_get_table_name(),
        }
    }

    fn tmq_get_offset(&self) -> Offset {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_offset(),
            ResultSet::Schemaless(rs) => rs.tmq_get_offset(),
            ResultSet::Tmq(rs) => rs.tmq_get_offset(),
        }
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_vgroup_offset(),
            ResultSet::Schemaless(rs) => rs.tmq_get_vgroup_offset(),
            ResultSet::Tmq(rs) => rs.tmq_get_vgroup_offset(),
        }
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.tmq_get_vgroup_id(),
            ResultSet::Schemaless(rs) => rs.tmq_get_vgroup_id(),
            ResultSet::Tmq(rs) => rs.tmq_get_vgroup_id(),
        }
    }

    fn precision(&self) -> Precision {
        match self {
            ResultSet::Query(rs) => rs.precision(),
            ResultSet::Schemaless(rs) => rs.precision(),
            ResultSet::Tmq(rs) => rs.precision(),
        }
    }

    fn affected_rows(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.affected_rows(),
            ResultSet::Schemaless(rs) => rs.affected_rows(),
            ResultSet::Tmq(rs) => rs.affected_rows(),
        }
    }

    fn affected_rows64(&self) -> i64 {
        match self {
            ResultSet::Query(rs) => rs.affected_rows64(),
            ResultSet::Schemaless(rs) => rs.affected_rows64(),
            ResultSet::Tmq(rs) => rs.affected_rows64(),
        }
    }

    fn num_of_fields(&self) -> i32 {
        match self {
            ResultSet::Query(rs) => rs.num_of_fields(),
            ResultSet::Schemaless(rs) => rs.num_of_fields(),
            ResultSet::Tmq(rs) => rs.num_of_fields(),
        }
    }

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        match self {
            ResultSet::Query(rs) => rs.get_fields(),
            ResultSet::Schemaless(rs) => rs.get_fields(),
            ResultSet::Tmq(rs) => rs.get_fields(),
        }
    }

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
        match self {
            ResultSet::Query(rs) => rs.get_fields_e(),
            ResultSet::Schemaless(rs) => rs.get_fields_e(),
            ResultSet::Tmq(rs) => rs.get_fields_e(),
        }
    }

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error> {
        match self {
            ResultSet::Query(rs) => rs.fetch_raw_block(ptr, rows),
            ResultSet::Schemaless(rs) => rs.fetch_raw_block(ptr, rows),
            ResultSet::Tmq(rs) => rs.fetch_raw_block(ptr, rows),
        }
    }

    unsafe fn fetch_block(&mut self, rows: *mut TAOS_ROW, num: *mut c_int) -> Result<(), Error> {
        match self {
            ResultSet::Query(rs) => rs.fetch_block(rows, num),
            ResultSet::Schemaless(rs) => rs.fetch_block(rows, num),
            ResultSet::Tmq(rs) => rs.fetch_block(rows, num),
        }
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        match self {
            ResultSet::Query(rs) => rs.fetch_row(),
            ResultSet::Schemaless(rs) => rs.fetch_row(),
            ResultSet::Tmq(rs) => rs.fetch_row(),
        }
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        match self {
            ResultSet::Query(rs) => rs.get_raw_value(row, col),
            ResultSet::Schemaless(rs) => rs.get_raw_value(row, col),
            ResultSet::Tmq(rs) => rs.get_raw_value(row, col),
        }
    }

    fn take_timing(&mut self) -> Duration {
        match self {
            ResultSet::Query(rs) => rs.take_timing(),
            ResultSet::Schemaless(rs) => rs.take_timing(),
            ResultSet::Tmq(rs) => rs.take_timing(),
        }
    }

    fn stop_query(&mut self) {
        match self {
            ResultSet::Query(rs) => rs.stop_query(),
            ResultSet::Schemaless(rs) => rs.stop_query(),
            ResultSet::Tmq(rs) => rs.stop_query(),
        }
    }

    unsafe fn is_null_by_column(
        &mut self,
        column_index: usize,
        result: *mut bool,
        rows: *mut c_int,
    ) -> Result<(), TaosError> {
        match self {
            ResultSet::Query(rs) => rs.is_null_by_column(column_index, result, rows),
            ResultSet::Schemaless(rs) => rs.is_null_by_column(column_index, result, rows),
            ResultSet::Tmq(rs) => rs.is_null_by_column(column_index, result, rows),
        }
    }

    fn get_column_data_offset(&mut self, column_index: usize) -> Result<*mut c_int, TaosError> {
        match self {
            ResultSet::Query(rs) => rs.get_column_data_offset(column_index),
            ResultSet::Schemaless(rs) => rs.get_column_data_offset(column_index),
            ResultSet::Tmq(rs) => rs.get_column_data_offset(column_index),
        }
    }
}

#[cfg(test)]
pub fn test_connect() -> *mut TAOS {
    unsafe {
        let taos = taos_connect(
            c"localhost".as_ptr(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            6041,
        );
        assert!(!taos.is_null());
        taos
    }
}

#[cfg(test)]
pub fn test_exec<S: AsRef<str>>(taos: *mut TAOS, sql: S) {
    unsafe {
        let sql = std::ffi::CString::new(sql.as_ref()).unwrap();
        let res = query::taos_query(taos, sql.as_ptr());
        if res.is_null() {
            let code = error::taos_errno(res);
            let err = error::taos_errstr(res);
            println!("code: {}, err: {:?}", code, CStr::from_ptr(err));
        }
        assert!(!res.is_null());
        query::taos_free_result(res);
    }
}

#[cfg(test)]
pub fn test_exec_many<T, S>(taos: *mut TAOS, sqls: S)
where
    T: AsRef<str>,
    S: IntoIterator<Item = T>,
{
    for sql in sqls {
        test_exec(taos, sql);
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;

    use faststr::FastStr;
    use taos_query::util::test_utils::{test_password, test_username};

    use super::*;
    use crate::ws::error::{taos_errno, taos_errstr};
    use crate::ws::query::*;

    #[test]
    fn test_taos_connect() {
        unsafe {
            let user = CString::new(test_username()).unwrap();
            let pass = CString::new(test_password()).unwrap();

            let taos = taos_connect(
                c"localhost".as_ptr(),
                user.as_ptr(),
                pass.as_ptr(),
                ptr::null(),
                6041,
            );
            assert!(!taos.is_null());
            taos_close(taos);

            let taos = taos_connect(ptr::null(), ptr::null(), ptr::null(), ptr::null(), 0);
            assert!(!taos.is_null());
            taos_close(taos);

            let invalid_utf8 = CString::new([0xff, 0xfe, 0xfd]).unwrap();
            let invalid_utf8_ptr = invalid_utf8.as_ptr();

            let taos = taos_connect(invalid_utf8_ptr, ptr::null(), ptr::null(), ptr::null(), 0);
            assert!(taos.is_null());

            let taos = taos_connect(ptr::null(), invalid_utf8_ptr, ptr::null(), ptr::null(), 0);
            assert!(taos.is_null());

            let taos = taos_connect(ptr::null(), ptr::null(), invalid_utf8_ptr, ptr::null(), 0);
            assert!(taos.is_null());

            let taos = taos_connect(ptr::null(), ptr::null(), ptr::null(), invalid_utf8_ptr, 0);
            assert!(taos.is_null());
        }
    }

    #[test]
    fn test_taos_connect_unable_to_establish_connection() {
        unsafe {
            let user = CString::new(test_username()).unwrap();
            let pass = CString::new(test_password()).unwrap();

            let taos = taos_connect(
                c"invalid_host".as_ptr(),
                user.as_ptr(),
                pass.as_ptr(),
                ptr::null(),
                6041,
            );
            assert!(taos.is_null());

            let code = taos_errno(ptr::null_mut());
            let errstr = taos_errstr(ptr::null_mut());
            assert_eq!(Code::from(code), Code::new(0x000B));
            assert_eq!(
                CStr::from_ptr(errstr).to_str().unwrap(),
                "Unable to establish connection"
            );
        }
    }

    #[test]
    fn test_taos_connect_auth_failure() {
        unsafe {
            let taos = taos_connect(
                c"localhost".as_ptr(),
                c"root".as_ptr(),
                c"hello".as_ptr(),
                ptr::null(),
                6041,
            );
            assert!(taos.is_null());

            let code = taos_errno(ptr::null_mut());
            let errstr = taos_errstr(ptr::null_mut());
            let errstr = CStr::from_ptr(errstr).to_str().unwrap();
            assert_eq!(Code::from(code), Code::new(0x0357));
            assert!(errstr.contains("Authentication failure"));
        }
    }

    #[test]
    fn test_taos_options() {
        unsafe {
            let p = taos_options(TSDB_OPTION::TSDB_OPTION_CONFIGDIR, ptr::null());
            assert!(p != 0);
            let code = taos_errno(ptr::null_mut());
            let str = taos_errstr(ptr::null_mut());
            assert_eq!(Code::from(code), Code::INVALID_PARA);
            assert_eq!(
                CStr::from_ptr(str).to_str().unwrap(),
                "taos cfg dir is null"
            );

            let p = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                CString::new("invalid_path").unwrap().as_ptr() as *const c_void,
            );
            assert_eq!(p, 0);

            let p = taos_options(
                TSDB_OPTION::TSDB_OPTION_CONFIGDIR,
                CString::new("tests/").unwrap().as_ptr() as *const c_void,
            );
            assert_eq!(p, 0);
            taos_options(TSDB_OPTION::TSDB_OPTION_TIMEZONE, ptr::null());
            taos_options(
                TSDB_OPTION::TSDB_OPTION_TIMEZONE,
                CString::new("invalid_timezone").unwrap().as_ptr() as *const c_void,
            );
            taos_options(
                TSDB_OPTION::TSDB_OPTION_TIMEZONE,
                CString::new("UTC").unwrap().as_ptr() as *const c_void,
            );
            taos_options(
                TSDB_OPTION::TSDB_OPTION_TIMEZONE,
                CString::new("invalid_timezone").unwrap().as_ptr() as *const c_void,
            );
        }
    }

    #[test]
    fn test_taos_options_release_arg_memory() {
        unsafe {
            let arg = CString::new("/etc/taos").unwrap();
            let code = taos_options(TSDB_OPTION::TSDB_OPTION_CONFIGDIR, arg.as_ptr() as *const _);
            assert_eq!(code, 0);
            drop(arg);
            let cfg_dir = config::config_dir();
            assert_eq!(cfg_dir, FastStr::new("/etc/taos"));
        }
    }

    #[test]
    fn test_taos_options_connection() {
        let taos = test_connect();

        unsafe {
            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_CLEAR,
                ptr::null(),
            );
            assert_eq!(code, 0);

            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_CHARSET,
                c"UTF-8".as_ptr() as *const c_void,
            );
            assert_eq!(code, 0);

            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_CHARSET,
                ptr::null(),
            );
            assert_eq!(code, 0);

            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_USER_IP,
                c"127.0.0.1".as_ptr() as *const c_void,
            );
            assert_eq!(code, 0);

            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_USER_APP,
                c"test".as_ptr() as *const c_void,
            );
            assert_eq!(code, 0);

            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_USER_APP,
                ptr::null(),
            );
            assert_eq!(code, 0);

            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_CLEAR,
                c"clear".as_ptr() as *const c_void,
            );
            assert_eq!(code, 0);
        }

        unsafe {
            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_CHARSET,
                c"GB2312".as_ptr() as *const c_void,
            );
            assert_eq!(code, 0x80000118u32 as i32);

            let errstr = taos_errstr(ptr::null_mut());
            assert_eq!(
                CStr::from_ptr(errstr).to_str().unwrap(),
                "unsupported charset: GB2312"
            );
        }

        unsafe { taos_close(taos) };
    }

    #[test]
    fn test_timezone_default() {
        unsafe {
            let taos = test_connect();

            let res = taos_query(taos, c"select timezone()".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 1);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!(
                CStr::from_ptr(str.as_ptr()).to_str().unwrap(),
                "Asia/Shanghai (CST, +0800)"
            );

            taos_free_result(res);
            taos_close(taos);
        }
    }

    #[test]
    fn test_timezone_custom() {
        unsafe {
            let taos = test_connect();

            let code = taos_options_connection(
                taos,
                TSDB_OPTION_CONNECTION::TSDB_OPTION_CONNECTION_TIMEZONE,
                c"America/New_York".as_ptr() as *const c_void,
            );
            assert_eq!(code, 0);

            let res = taos_query(taos, c"select timezone()".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 1);

            let mut str = vec![0 as c_char; 1024];
            let len = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            let tz = CStr::from_ptr(str.as_ptr()).to_str().unwrap();
            assert!(matches!(
                tz,
                "America/New_York (EST, -0500)" | "America/New_York (EDT, -0400)"
            ));

            taos_free_result(res);
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_connect_with_dsn() {
        unsafe {
            let taos = taos_connect_with_dsn(ptr::null());
            assert!(!taos.is_null());
            taos_close(taos);

            let dsn = CString::new(format!(
                "ws://{}:{}@localhost:6041",
                test_username(),
                test_password()
            ))
            .unwrap();
            let taos = taos_connect_with_dsn(dsn.as_ptr());
            assert!(!taos.is_null());
            taos_close(taos);

            let dsn = CString::new(format!(
                "ws://{}:{}@localhost:6041?read_timeout=1",
                test_username(),
                test_password()
            ))
            .unwrap();
            let taos = taos_connect_with_dsn(dsn.as_ptr());
            assert!(!taos.is_null());
            taos_close(taos);

            let invalid_utf8 = CString::new([0xff, 0xfe, 0xfd]).unwrap();
            let taos = taos_connect_with_dsn(invalid_utf8.as_ptr());
            assert!(taos.is_null());
        }
    }

    #[test]
    fn test_taos_set_option_with_null_options() {
        unsafe {
            taos_set_option(ptr::null_mut(), c"ip".as_ptr(), c"localhost".as_ptr());
            let code = taos_errno(ptr::null_mut());
            assert_eq!(Code::from(code), Code::INVALID_PARA);
        }
    }

    #[test]
    fn test_taos_set_option_with_null_key() {
        let mut opts = OPTIONS {
            keys: [ptr::null(); 256],
            values: [ptr::null(); 256],
            count: 0,
        };
        unsafe { taos_set_option(&mut opts, ptr::null(), c"localhost".as_ptr()) };
        assert_eq!(opts.count, 0);
        let code = unsafe { taos_errno(ptr::null_mut()) };
        assert_eq!(Code::from(code), Code::INVALID_PARA);
    }

    #[test]
    fn test_taos_set_option_with_null_value() {
        let mut opts = super::OPTIONS {
            keys: [ptr::null(); 256],
            values: [ptr::null(); 256],
            count: 0,
        };
        unsafe { taos_set_option(&mut opts, c"ip".as_ptr(), ptr::null()) };
        assert_eq!(opts.count, 0);
        let code = unsafe { taos_errno(ptr::null_mut()) };
        assert_eq!(Code::from(code), Code::INVALID_PARA);
    }

    #[test]
    fn test_taos_set_option_overflow_count() {
        let mut opts = OPTIONS {
            keys: [ptr::null(); 256],
            values: [ptr::null(); 256],
            count: 256,
        };

        unsafe { taos_set_option(&mut opts, c"ip".as_ptr(), c"localhost".as_ptr()) };
        assert_eq!(opts.count, 256);
        assert!(opts.keys[0].is_null());
        assert!(opts.values[0].is_null());
        assert!(opts.keys[255].is_null());
        assert!(opts.values[255].is_null());

        let code = unsafe { taos_errno(ptr::null_mut()) };
        assert_eq!(Code::from(code), Code::INVALID_PARA);
    }

    #[test]
    fn test_build_dsn_from_options_skip_null_key_or_value() {
        let mut opts = OPTIONS {
            keys: [ptr::null(); 256],
            values: [ptr::null(); 256],
            count: 3,
        };
        opts.keys[1] = c"ip".as_ptr();
        opts.keys[2] = c"port".as_ptr();
        opts.values[2] = c"6041".as_ptr();

        unsafe {
            let dsn = build_dsn_from_options(&opts as *const _).unwrap();
            assert!(dsn.starts_with("ws://root:taosdata@localhost:6041/"));
        }
    }

    #[test]
    fn test_build_dsn_from_options_duplicate_key_overwrite() {
        let mut opts = OPTIONS {
            keys: [ptr::null(); 256],
            values: [ptr::null(); 256],
            count: 0,
        };
        unsafe {
            taos_set_option(&mut opts, c"ip".as_ptr(), c"127.0.0.1".as_ptr());
            taos_set_option(&mut opts, c"ip".as_ptr(), c"localhost".as_ptr());
            taos_set_option(&mut opts, c"port".as_ptr(), c"6041".as_ptr());
        }

        unsafe {
            let dsn = build_dsn_from_options(&opts as *const _).unwrap();
            assert!(dsn.starts_with("ws://root:taosdata@localhost:6041/"));
        }
    }

    pub(super) fn make_options(kvs: &[(&str, &str)]) -> OPTIONS {
        let mut opts = OPTIONS {
            keys: [ptr::null(); 256],
            values: [ptr::null(); 256],
            count: 0,
        };
        for (k, v) in kvs {
            let key = CString::new(*k).unwrap().into_raw();
            let value = CString::new(*v).unwrap().into_raw();
            unsafe { taos_set_option(&mut opts, key, value) };
        }
        opts
    }

    pub(super) fn free_options(opts: &OPTIONS) {
        for i in 0..opts.count as usize {
            unsafe {
                if !opts.keys[i].is_null() {
                    let _ = CString::from_raw(opts.keys[i] as *mut c_char);
                }
                if !opts.values[i].is_null() {
                    let _ = CString::from_raw(opts.values[i] as *mut c_char);
                }
            }
        }
    }

    #[test]
    fn test_taos_connect_with_default() {
        unsafe {
            let taos = taos_connect_with(ptr::null());
            assert!(!taos.is_null(),);
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_connect_with_ip_port() {
        unsafe {
            let opts = make_options(&[(IP, "localhost"), (PORT, "6041")]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_adapter_list_priority() {
        unsafe {
            let opts = make_options(&[(ADAPTER_LIST, "localhost:6041"), (IP, "invalid_host")]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_user_pass_db() {
        unsafe {
            let conn = test_connect();
            test_exec_many(
                conn,
                &[
                    "drop database if exists test_1765508846",
                    "create database test_1765508846",
                ],
            );

            let user = test_username();
            let pass = test_password();

            let opts = make_options(&[
                (IP, "localhost"),
                (PORT, "6041"),
                (USER, &user),
                (PASS, &pass),
                (DB, "test_1765508846"),
            ]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);

            test_exec(conn, "drop database if exists test_1765508846");
            taos_close(conn);
        }
    }

    #[test]
    fn test_taos_connect_with_invalid_port() {
        unsafe {
            let opts = make_options(&[(IP, "localhost"), (PORT, "not_a_number")]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_compression() {
        unsafe {
            let opts = make_options(&[(IP, "localhost"), (PORT, "6041"), (COMPRESSION, "1")]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }

        unsafe {
            let opts = make_options(&[(IP, "localhost"), (PORT, "6041"), (COMPRESSION, "0")]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_invalid_compression() {
        unsafe {
            let opts = make_options(&[(IP, "localhost"), (PORT, "6041"), (COMPRESSION, "maybe")]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(taos.is_null());
            let code = taos_errno(ptr::null_mut());
            assert_eq!(Code::from(code), Code::INVALID_PARA);
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_unreachable_host() {
        unsafe {
            let opts = make_options(&[(IP, "invalid_host.example"), (PORT, "6041")]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(taos.is_null());
            let code = taos_errno(ptr::null_mut());
            assert_eq!(Code::from(code), Code::new(0x000B));
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_invalid_utf8_items() {
        let mut opts = OPTIONS {
            keys: [ptr::null(); 256],
            values: [ptr::null(); 256],
            count: 0,
        };

        let bad = CString::new([0xff, 0xfe, 0xfd]).unwrap();
        let good_k = CString::new(IP).unwrap();
        let good_v = CString::new("localhost").unwrap();

        unsafe {
            taos_set_option(&mut opts, bad.as_ptr(), good_v.as_ptr());
            taos_set_option(&mut opts, good_k.as_ptr(), bad.as_ptr());

            let taos = taos_connect_with(&opts as *const _);
            assert!(taos.is_null());

            let errstr = taos_errstr(ptr::null_mut());
            let errstr = CStr::from_ptr(errstr).to_str().unwrap();
            assert!(errstr.contains("invalid utf-8"));
        }
    }

    #[test]
    fn test_taos_connect_with_count_out_of_range() {
        let opts = OPTIONS {
            keys: [ptr::null(); 256],
            values: [ptr::null(); 256],
            count: 300,
        };

        unsafe {
            let taos = taos_connect_with(&opts as *const _);
            assert!(taos.is_null());

            let code = taos_errno(ptr::null_mut());
            assert_eq!(Code::from(code), Code::INVALID_PARA);

            let errstr = taos_errstr(ptr::null_mut());
            let errstr = CStr::from_ptr(errstr).to_str().unwrap();
            assert_eq!(errstr, "options count out of range");
        }
    }

    #[test]
    fn test_taos_connect_with_ws_tls_mode_disabled() {
        unsafe {
            let opts = make_options(&[(WS_TLS_MODE, "0"), (IP, "localhost"), (PORT, "6041")]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_verify_ca() {
        unsafe {
            let opts = make_options(&[
                (WS_TLS_MODE, "2"),
                (
                    WS_TLS_CA,
                    include_str!("../../tests/certs/ca_verify_ca.crt"),
                ),
                (IP, "localhost"),
                (PORT, "6446"),
                (WS_TLS_VERSION, "TLSv1.3,TLSv1.2"),
            ]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }

        unsafe {
            let ca_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/certs/ca_verify_ca.crt");
            let tls_ca = ca_path.to_str().unwrap();

            let opts = make_options(&[
                (WS_TLS_MODE, "2"),
                (WS_TLS_CA, tls_ca),
                (IP, "localhost"),
                (PORT, "6446"),
                (WS_TLS_VERSION, "TLSv1.3,TLSv1.2"),
            ]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_verify_identity() {
        unsafe {
            let opts = make_options(&[
                (WS_TLS_MODE, "3"),
                (
                    WS_TLS_CA,
                    include_str!("../../tests/certs/ca_verify_identity.crt"),
                ),
                (IP, "localhost"),
                (PORT, "6445"),
                (WS_TLS_VERSION, "TLSv1.3,TLSv1.2"),
            ]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }

        unsafe {
            let ca_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/certs/ca_verify_identity.crt");
            let tls_ca = ca_path.to_str().unwrap();

            let opts = make_options(&[
                (WS_TLS_MODE, "3"),
                (WS_TLS_CA, tls_ca),
                (IP, "localhost"),
                (PORT, "6445"),
                (WS_TLS_VERSION, "TLSv1.3,TLSv1.2"),
            ]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_verify_ca_missing_ca() {
        unsafe {
            let opts = make_options(&[
                (WS_TLS_MODE, "2"),
                (IP, "localhost"),
                (PORT, "6041"),
                (WS_TLS_VERSION, "TLSv1.3"),
            ]);

            let taos = taos_connect_with(&opts as *const _);
            assert!(taos.is_null());

            let errstr = taos_errstr(ptr::null_mut());
            let errstr = CStr::from_ptr(errstr).to_str().unwrap();
            assert!(errstr.contains("requires parameter: tls_ca"));

            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_verify_identity_missing_ca() {
        unsafe {
            let opts = make_options(&[
                (WS_TLS_MODE, "3"),
                (IP, "localhost"),
                (PORT, "6041"),
                (WS_TLS_VERSION, "TLSv1.3"),
            ]);

            let taos = taos_connect_with(&opts as *const _);
            assert!(taos.is_null());

            let errstr = taos_errstr(ptr::null_mut());
            let errstr = CStr::from_ptr(errstr).to_str().unwrap();
            assert!(errstr.contains("requires parameter: tls_ca"));

            free_options(&opts);
        }
    }

    #[test]
    fn test_taos_connect_with_all_params() {
        let conn = test_connect();
        test_exec_many(
            conn,
            &[
                "drop database if exists test_1765519301",
                "create database test_1765519301",
            ],
        );

        let user = test_username();
        let pass = test_password();

        unsafe {
            let opts = make_options(&[
                (IP, "localhost"),
                (PORT, "6445"),
                (USER, &user),
                (PASS, &pass),
                (DB, "test_1765519301"),
                (WS_TLS_MODE, "3"),
                (WS_TLS_VERSION, "TLSv1.3,TLSv1.2"),
                (
                    WS_TLS_CA,
                    include_str!("../../tests/certs/ca_verify_identity.crt"),
                ),
                (COMPRESSION, "1"),
                (CONN_RETRIES, "5"),
                (RETRY_BACKOFF_MS, "200"),
                (RETRY_BACKOFF_MAX_MS, "5000"),
                ("token", "my_token"),
                ("timezone", "UTC"),
                ("userIp", "192.168.1.1"),
                ("userApp", "my_app"),
            ]);

            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);

            free_options(&opts);
        }

        test_exec(conn, "drop database if exists test_1765519301");
        unsafe { taos_close(conn) };
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_taos_connect_totp() {
        use taos_query::util::totp::*;

        unsafe {
            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                6041,
            );
            assert!(!taos.is_null());

            let _ = taos_query(taos, c"drop user c_totp_user".as_ptr());

            let totp_seed = generate_totp_seed(64);
            test_exec(
                taos,
                format!("create user c_totp_user pass 'totp_pass_1' totpseed '{totp_seed}'"),
            );

            let totp_secret = generate_totp_secret(totp_seed.as_bytes());
            let totp_code = generate_totp_code(&totp_secret);

            let totp = CString::new(totp_code).unwrap();
            let taost = taos_connect_totp(
                c"localhost".as_ptr(),
                c"c_totp_user".as_ptr(),
                c"totp_pass_1".as_ptr(),
                totp.as_ptr(),
                ptr::null(),
                6041,
            );
            assert!(!taost.is_null());

            let res = taos_query(taost, c"select 1".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 1);

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!("1", CStr::from_ptr(str.as_ptr()).to_str().unwrap());

            taos_free_result(res);
            taos_close(taost);

            let code = taos_connect_test(
                c"localhost".as_ptr(),
                c"c_totp_user".as_ptr(),
                c"totp_pass_1".as_ptr(),
                totp.as_ptr(),
                ptr::null(),
                6041,
            );
            assert_eq!(code, 0);

            test_exec(taos, "drop user c_totp_user");
            taos_close(taos);
        }
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_taos_connect_token() {
        unsafe {
            let taos = taos_connect(
                c"localhost".as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                6041,
            );
            assert!(!taos.is_null());

            let _ = taos_query(taos, c"drop user c_token_user".as_ptr());
            test_exec(taos, "create user c_token_user pass 'token_pass_1'");

            let res = taos_query(
                taos,
                c"create token test_c_bearer_token from user c_token_user".as_ptr(),
            );
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

            let token = CString::new(token).unwrap();
            let taost =
                taos_connect_token(c"localhost".as_ptr(), token.as_ptr(), ptr::null(), 6041);
            assert!(!taost.is_null());

            let res = taos_query(taost, c"select 1".as_ptr());
            assert!(!res.is_null());

            let row = taos_fetch_row(res);
            assert!(!row.is_null());

            let fields = taos_fetch_fields(res);
            assert!(!fields.is_null());

            let num_fields = taos_num_fields(res);
            assert_eq!(num_fields, 1);

            let mut str = vec![0 as c_char; 1024];
            let _ = taos_print_row(str.as_mut_ptr(), row, fields, num_fields);
            assert_eq!("1", CStr::from_ptr(str.as_ptr()).to_str().unwrap());

            taos_free_result(res);
            taos_close(taost);

            test_exec(taos, "drop user c_token_user");
            taos_close(taos);
        }
    }

    #[test]
    fn test_secret_map_debug_redacts_sensitive_values() {
        let mut map = HashMap::new();
        map.insert(IP, "localhost");
        map.insert(PASS, "secret-pass");
        map.insert(TOKEN, "secret-token");

        let output = format!("{:?}", SecretMap(&map));
        assert!(output.contains("\"ip\": \"localhost\""));
        assert!(output.contains("\"pass\": \"[REDACTED]\""));
        assert!(output.contains("\"token\": \"[REDACTED]\""));
        assert!(!output.contains("secret-pass"));
        assert!(!output.contains("secret-token"));
    }

    #[test]
    fn test_secret_map_debug_keeps_non_sensitive_values() {
        let mut map = HashMap::new();
        map.insert(USER, "root");
        map.insert(DB, "test_db");

        let output = format!("{:?}", SecretMap(&map));
        assert!(output.contains("\"user\": \"root\""));
        assert!(output.contains("\"db\": \"test_db\""));
        assert!(!output.contains("[REDACTED]"));
    }
}

#[cfg(test)]
mod cloud_tests {
    use std::ffi::CString;

    use super::*;

    #[test]
    fn test_taos_connect() {
        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_taos_connect");
            return;
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_taos_connect");
            return;
        }

        let url = url.unwrap().strip_prefix("https://").unwrap().to_string();
        let url = CString::new(url).unwrap();
        let token = CString::new(token.unwrap()).unwrap();

        unsafe {
            let taos = taos_connect(
                url.as_ptr(),
                c"token".as_ptr(),
                token.as_ptr(),
                ptr::null(),
                0,
            );
            assert!(!taos.is_null());
            taos_close(taos);

            let taos = taos_connect(
                url.as_ptr(),
                c"token".as_ptr(),
                token.as_ptr(),
                ptr::null(),
                443,
            );
            assert!(!taos.is_null());
            taos_close(taos);
        }
    }

    #[test]
    fn test_taos_connect_with_dsn() -> anyhow::Result<()> {
        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_taos_connect_with_dsn");
            return Ok(());
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_taos_connect_with_dsn");
            return Ok(());
        }

        let dsn = format!("{}?token={}", url.unwrap(), token.unwrap());
        let dsn = CString::new(dsn).unwrap();
        let taos = unsafe { taos_connect_with_dsn(dsn.as_ptr()) };
        assert!(!taos.is_null());
        unsafe { taos_close(taos) };

        Ok(())
    }

    #[test]
    fn test_taos_connect_with() {
        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_taos_connect_with");
            return;
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_taos_connect_with");
            return;
        }

        let url = url.unwrap().strip_prefix("https://").unwrap().to_string();
        let token = token.unwrap();

        unsafe {
            let opts = tests::make_options(&[
                (IP, &url),
                ("token", &token),
                (WS_TLS_MODE, "1"),
                (WS_TLS_VERSION, "TLSv1.3,TLSv1.2"),
            ]);
            let taos = taos_connect_with(&opts as *const _);
            assert!(!taos.is_null());
            taos_close(taos);
            tests::free_options(&opts);
        }
    }
}
