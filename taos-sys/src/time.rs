use num_enum::FromPrimitive;
use std::os::raw::*;

#[repr(i32)]
#[derive(Debug, Copy, Clone, FromPrimitive)]
#[cfg_attr(
    feature = "serde",
    derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)
)]
pub enum TimestampPrecision {
    #[num_enum(default)]
    Millisecond = 0,
    Microsecond,
    Nanosecond,
}

pub const TIMESTAMP_MILLISECOND: TimestampPrecision = TimestampPrecision::Millisecond;
pub const TIMESTAMP_MICROSECOND: TimestampPrecision = TimestampPrecision::Microsecond;
pub const TIMESTAMP_NANOSECOND: TimestampPrecision = TimestampPrecision::Nanosecond;

#[cfg(taos_parse_time)]
extern "C" {
    pub fn taos_parse_time(
        time_str: *const c_char,
        time: *mut i64,
        len: i32,
        time_precision: TimestampPrecision,
        daylight: i8, // if in daylight saving time (DST) { 1 } else { 0 }
    ) -> i32;
}
#[cfg(all(not(taos_parse_time), feature = "backport"))]
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
#[cfg(taos_parse_time)]
fn test_parse_time() {
    use std::ffi::CString;
    let s = CString::new("1970-01-01 00:00:00").unwrap();
    let mut time = 0i64;
    unsafe {
        crate::taos_options(
            crate::TSDB_OPTION_TIMEZONE,
            b"Europe/Landon\0" as *const u8 as *const _,
        )
    };
    let res = unsafe {
        taos_parse_time(
            s.as_ptr(),
            &mut time as _,
            s.to_bytes().len() as _,
            TIMESTAMP_MICROSECOND,
            0,
        )
    };
    assert_eq!(res, 0, "success");
    assert_eq!(time, 0, "parse time");

    let s = CString::new("1970-01-01 08:00:00").unwrap(); // CST +8
                                                          // timezone could be set multiple times
    unsafe {
        crate::taos_options(
            crate::TSDB_OPTION_TIMEZONE,
            b"Asia/Shanghai\0" as *const u8 as *const _,
        )
    };
    let res = unsafe {
        taos_parse_time(
            s.as_ptr(),
            &mut time as _,
            s.to_bytes().len() as _,
            TIMESTAMP_MICROSECOND,
            0,
        )
    };
    assert_eq!(res, 0, "success");
    assert_eq!(time, 0, "parse time");
}
