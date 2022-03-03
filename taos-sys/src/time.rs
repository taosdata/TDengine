use std::os::raw::*;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum TimestampPrecision {
    Millisecond = 0,
    Microsecond,
    Nanosecond,
}

pub const TIMESTAMP_MILLISECOND: TimestampPrecision = TimestampPrecision::Millisecond;
pub const TIMESTAMP_MICROSECOND: TimestampPrecision = TimestampPrecision::Microsecond;
pub const TIMESTAMP_NANOSECOND: TimestampPrecision = TimestampPrecision::Nanosecond;

#[cfg(c_parse_time)]
extern "C" {
    pub fn taos_parse_time(
        time_str: *const c_char,
        time: *mut i64,
        len: i32,
        time_precision: TimestampPrecision,
        daylight: i8, // if in daylight saving time (DST) { 1 } else { 0 }
    ) -> i32;
}
#[cfg(all(not(c_parse_time), feature = "backport"))]
#[no_mangle]
pub fn taos_parse_time(
    _time_str: *const c_char,
    _time: *mut i64,
    _len: i32,
    _time_precision: TimestampPrecision,
    _daylight: i8, // if in daylight saving time (DST) { 1 } else { 0 }
) -> i32 {
    unimplemented!("the function is backport to old version but not implemented!")
}
#[test]
#[cfg(c_parse_time)]
fn test_parse_time() {
    use std::ffi::CString;
    let s = CString::new("2020-02-22 20:20:20").unwrap();
    let mut time = 0i64;
    let res = unsafe {
        taos_parse_time(
            s.as_ptr(),
            &mut time as _,
            s.to_bytes().len() as _,
            TIMESTAMP_MICROSECOND,
            0,
        )
    };
    assert_eq!(time, 1582402820000000, "parse time");
}
