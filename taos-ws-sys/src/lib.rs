use std::{ffi::c_void, os::raw::c_char};

use taos_ws::sync::*;

pub type WS_TAOS = c_void;

use std::cell::RefCell;
thread_local! {
    pub static ERROR: RefCell<Option<Error>> = RefCell::new(None);
}

pub fn ws_errno(taos: *const WS_TAOS) {

}

#[no_mangle]
pub fn ws_connect_legacy(
    host: *mut c_char,
    user: *const c_char,
    pass: *const c_char,
    db: *const c_char,
    port: u16,
) -> WS_TAOS {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {

    }
}
