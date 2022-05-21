use taos_sys::{ffi::*, *};

#[test]
#[cfg(taos_parse_time)]
fn test_parse_time() {
    use std::ffi::CString;
    let s = CString::new("1970-01-01 00:00:00").unwrap();
    let mut time = 0i64;
    unsafe {
        taos_options(
            TSDB_OPTION_TIMEZONE,
            b"Europe/Landon\0" as *const u8 as *const _,
        )
    };
    let res = unsafe {
        taos_parse_time(
            s.as_ptr(),
            &mut time as _,
            s.to_bytes().len() as _,
            Precision::Microsecond,
            0,
        )
    };
    assert_eq!(res, 0, "success");
    assert_eq!(time, 0, "parse time");

    let s = CString::new("1970-01-01 08:00:00").unwrap(); // CST +8
                                                          // timezone could be set multiple times
    unsafe {
        taos_options(
            TSDB_OPTION_TIMEZONE,
            b"Asia/Shanghai\0" as *const u8 as *const _,
        )
    };
    let res = unsafe {
        taos_parse_time(
            s.as_ptr(),
            &mut time as _,
            s.to_bytes().len() as _,
            Precision::Microsecond,
            0,
        )
    };
    assert_eq!(res, 0, "success");
    assert_eq!(time, 0, "parse time");
}
