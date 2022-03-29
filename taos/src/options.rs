use std::{
    collections::BTreeMap,
    ffi::{CStr, CString},
    path::PathBuf,
    sync::Once,
};

use crate::{Result, Taos};

use taos_sys::*;

#[derive(Debug, Default)]
pub struct TaosOptions {
    host: Option<String>,
    port: Option<u16>, // make it optional with concern for REST.
    username: Option<String>,
    password: Option<String>,
    database: Option<String>,

    locale: Option<String>,
    charset: Option<String>,
    timezone: Option<String>,
    config_dir: Option<PathBuf>,
    shell_activity_timer: Option<u16>,
    params: BTreeMap<String, String>,
}

macro_rules! _build_opt {
    ($option:ident) => {
        _build_opt!($option, String);
    };
    ($option:ident, $ty:ty) => {
        pub fn $option<T: Into<$ty>>(mut self, $option: T) -> Self {
            self.$option = Some($option.into());
            self
        }
    };
    ($option:ident, $setter:block) => {
        pub fn $option<T: Into<String>>(mut self, $option: T) -> Self {
            $setter;
            self
        }
    };
    ($option:ident, $ty:ty, $setter:block) => {
        pub fn $option<T: Into<$ty>>(mut self, $option: T) -> Self {
            let $option = $option.into();
            $setter;
            self.$option = Some($option);
            self
        }
    };
}

impl TaosOptions {
    pub fn new() -> Self {
        Self::default()
    }
    _build_opt!(host);
    _build_opt!(username);
    _build_opt!(password);
    _build_opt!(database);
    _build_opt!(port, u16);

    _build_opt!(locale, String, {
        let cstr = CString::new(locale.clone()).expect("invalid locale");
        unsafe { taos_options(TSDB_OPTION_LOCALE, cstr.as_c_str().as_ptr() as _) };
    });
    _build_opt!(charset, String, {
        let cstr = CString::new(charset.clone()).expect("invalid charset");
        unsafe { taos_options(TSDB_OPTION_CHARSET, cstr.as_c_str().as_ptr() as _) };
    });
    _build_opt!(timezone, String, {
        let cstr = CString::new(timezone.clone()).expect("invalid timezone");
        unsafe { taos_options(TSDB_OPTION_TIMEZONE, cstr.as_c_str().as_ptr() as _) };
    });
    _build_opt!(config_dir, PathBuf, {
        let config_dir = config_dir
            .canonicalize()
            .expect("invalid path for config dir");
        let cstr = CString::new(config_dir.to_string_lossy().as_bytes()).expect("path to c string");
        unsafe { taos_options(TSDB_OPTION_CONFIGDIR, cstr.as_c_str().as_ptr() as _) };
    });

    _build_opt!(shell_activity_timer, u16, {
        let cstr = CString::new(format!("{}", shell_activity_timer))
            .expect("invalid shell activity timer");
        unsafe {
            taos_options(
                TSDB_OPTION_SHELL_ACTIVITY_TIMER,
                cstr.as_c_str().as_ptr() as _,
            )
        };
    });

    pub fn set_config_json(&self, json: &str) {
        let c_str = CString::new(json).expect("json to c string");
        unsafe {
            let res = taos_set_config(c_str.as_ptr());
            if res.code != SET_CONF_RET_SUCC {
                let msg = CStr::from_ptr(&res.msg as _);
                panic!("set config failed: {}", msg.to_string_lossy());
            }
        }
    }

    pub fn set_param(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.params.insert(key.into(), value.into());
        self
    }

    pub fn build(&self) -> Result<Taos> {
        static SET_CONFIG: Once = Once::new();
        SET_CONFIG.call_once(|| {
            println!("initialize taos options");
            if !self.params.is_empty() {
                let json = serde_json::to_string(&self.params).expect("params to json");
                self.set_config_json(&json);
            }
        });

        Taos::new(
            &self.host,
            &self.username,
            &self.password,
            &self.database,
            self.port.unwrap_or(0),
        )
    }
}

#[test]
fn test_options_builder() {
    let opts = TaosOptions::new();
    let _taos = opts.build().unwrap();
}

#[test]
fn test_options_builder_all() {
    let opts = TaosOptions::new()
        .locale("en_US")
        .charset("UTF-8")
        .timezone("Asia/Chongqing")
        .config_dir("/etc/taos")
        .host("localhost")
        .port(6030u16)
        .username("root")
        .password("taosdata")
        .database("log")
        ;
    let taos = opts.build().unwrap();
    let _res = futures::executor::block_on(taos.query("show databases")).unwrap();
}
