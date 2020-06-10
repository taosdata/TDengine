#[path = "bindings.rs"]
mod bindings;
use bindings::*;

#[path = "utils.rs"]
mod utils;
use utils::*;

use std::os::raw::c_void;
use std::os::raw::c_char;
use std::os::raw::c_int;
use std::os::raw::c_long;

pub struct Tdengine {
    conn: *mut c_void,
}

/// - **TODO**:  doc
impl Tdengine {

    //! - **TODO**: implement default param.
    //! 
    //! > refer to https://stackoverflow.com/questions/24047686/default-function-arguments-in-rust
    pub fn new(ip: &str, username: &str, passwd: &str, db: &str, port: i32) -> Result<Tdengine, &'static str> {
        unsafe {
            taos_init();
            let mut conn = taos_connect(str_into_raw(ip),
                                        str_into_raw(username),
                                        str_into_raw(passwd),
                                        str_into_raw(db),
                                        port as c_int);
            if conn.is_null() {
                Err("connect error")
            } else {
                println!("connected to {}:{} user:{}, db:{}", ip, port, username, db);
                Ok(Tdengine {conn})
            }
        }
    }

    // - **TODO**: check error code
    pub fn query(self: &Tdengine, s: &str) {
        unsafe {
            if taos_query(self.conn, str_into_raw(s)) == 0 {
                println!("query '{}' ok", s);
            } else {
                println!("query '{}' error: {}", s, raw_into_str(taos_errstr(self.conn)));
            }
        }
    }
}

impl Drop for Tdengine {
    fn drop(&mut self) {
        unsafe {taos_close(self.conn);}
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}