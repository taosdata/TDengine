use taos_sys::ffi::taos_get_client_info;

use std::ffi::CStr;

#[test]
fn test_server_info() {
    let info = unsafe { CStr::from_ptr(taos_get_client_info()) }.to_string_lossy();
    println!("{}", dbg!(&info));
    assert!(info.contains("."))
}
