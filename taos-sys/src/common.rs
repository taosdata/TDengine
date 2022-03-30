use std::os::raw::*;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum TSDB_OPTION {
    Locale = 0,
    Charset,
    Timezone,
    ConfigDir,
    ShellActivityTimer,
    MaxOptions,
}
pub const TSDB_OPTION_LOCALE: TSDB_OPTION = TSDB_OPTION::Locale;
pub const TSDB_OPTION_CHARSET: TSDB_OPTION = TSDB_OPTION::Charset;
pub const TSDB_OPTION_TIMEZONE: TSDB_OPTION = TSDB_OPTION::Timezone;
pub const TSDB_OPTION_CONFIGDIR: TSDB_OPTION = TSDB_OPTION::Locale;
pub const TSDB_OPTION_SHELL_ACTIVITY_TIMER: TSDB_OPTION = TSDB_OPTION::ShellActivityTimer;
pub const TSDB_MAX_OPTIONS: TSDB_OPTION = TSDB_OPTION::MaxOptions;

extern "C" {
    pub fn taos_cleanup();

    pub fn taos_options(option: TSDB_OPTION, arg: *const c_void, ...) -> c_int;

    pub fn taos_get_client_info() -> *const c_char;

    pub fn taos_data_type(type_: c_int) -> *const c_char;
}
