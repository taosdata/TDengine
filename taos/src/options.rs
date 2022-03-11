use std::{
    collections::HashMap,
    ffi::{CStr, CString},
    path::{Path, PathBuf},
};

use derive_builder::Builder;

use taos_sys::*;

#[derive(Builder, Default)]
#[builder(setter(into, strip_option))]
pub struct TaosOptions {
    host: Option<String>,
    user: Option<String>,
    pass: Option<String>,
    db: Option<String>,
    port: Option<u16>,

    locale: Option<String>,
    charset: Option<String>,
    timezone: Option<String>,
    config_dir: Option<String>,
    shell_activity_timer: Option<u16>,

    #[builder(setter(skip))]
    config: Vec<(String, String)>,
}

macro_rules! option_builder_str {
    ($option:ident) => {
        option_builder_str!($option, String);
    };
    ($option:ident, $ty:ty) => {
        pub fn $option<T: Into<$ty>>(&mut self, $option: T) -> &mut Self {
            self.$option = Some($option.into());
            self
        }
    };
    ($option:ident, $setter:block) => {
        pub fn $option<T: Into<String>>(&mut self, $option: T) -> &mut Self {
            $setter;
            self
        }
    };
}
impl TaosOptions {
    pub fn new() -> Self {
        Self::default()
    }
    option_builder_str!(host);
    option_builder_str!(user);
    option_builder_str!(pass);
    option_builder_str!(db);
    option_builder_str!(port, u16);

    pub fn locale<T: AsRef<str>>(&mut self, locale: T) -> &mut Self {
        let locale = locale.as_ref();
        let cstr = CString::new(locale).expect("invalid locale");
        unsafe { taos_options(TSDB_OPTION_LOCALE, cstr.as_c_str().as_ptr() as _) };
        self
    }
    pub fn charset<T: AsRef<str>>(&mut self, charset: T) -> &mut Self {
        let charset = charset.as_ref();
        let cstr = CString::new(charset).expect("invalid locale");
        unsafe { taos_options(TSDB_OPTION_CHARSET, cstr.as_c_str().as_ptr() as _) };
        self
    }
    pub fn timezone<T: AsRef<str>>(&mut self, timezone: T) -> &mut Self {
        let timezone = timezone.as_ref();
        let cstr = CString::new(timezone).expect("invalid locale");
        unsafe { taos_options(TSDB_OPTION_TIMEZONE, cstr.as_c_str().as_ptr() as _) };
        self
    }

    pub fn config_dir<T: AsRef<Path>>(&mut self, path: T) -> &mut Self {
        let path = path.as_ref();
        let cstr = CString::new(
            path.canonicalize()
                .expect("invalid path for config_dir")
                .to_string_lossy()
                .as_bytes(),
        )
        .expect("invalid config dir");
        let res = unsafe { taos_options(TSDB_OPTION::ConfigDir, cstr.as_c_str().as_ptr() as _) };
        println!("set config dir return: {res}");
        self
    }

    pub fn shell_activity_timer(&mut self, shell_activity_timer: u16) -> &mut Self {
        let c_str =
            CString::new(format!("{}", shell_activity_timer)).expect("u16 cannot format to string");
        unsafe { taos_options(TSDB_OPTION_SHELL_ACTIVITY_TIMER, c_str.as_ptr() as _) };
        self
    }

    pub fn set_config_json(&mut self, json: &str) -> &mut Self {
        let c_str = CString::new(json).expect("json to c string");
        unsafe {
            let res = taos_set_config(c_str.as_ptr());
            if res.code == SET_CONF_RET_SUCC {
                return self;
            } else {
                let msg = CStr::from_ptr(&res.msg as _);
                panic!("set config failed: {}", msg.to_string_lossy());
            }
        }
    }
}

// TODO: options should only be set once

#[test]
fn test_options_builder() {
    let opts = TaosOptionsBuilder::default().host("localhost").build();
}
