use crate::ffi::{TAOS, TAOS_RES};
use std::os::raw::*;

///
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum SchemalessProtocol {
    #[non_exhaustive]
    Unknown = 0,
    Line,
    Telnet,
    Json,
}
pub type TSDB_SML_PROTOCOL_TYPE = SchemalessProtocol;
pub const TSDB_SML_UNKNOWN_PROTOCOL: SchemalessProtocol = SchemalessProtocol::Unknown;
pub const TSDB_SML_LINE_PROTOCOL: SchemalessProtocol = SchemalessProtocol::Line;
pub const TSDB_SML_TELNET_PROTOCOL: SchemalessProtocol = SchemalessProtocol::Telnet;
pub const TSDB_SML_JSON_PROTOCOL: SchemalessProtocol = SchemalessProtocol::Json;

/// Timestamp precision to parse from schemaless input.
///
/// Accepted timestamp precision options are:
/// - NonConfigured: let the parse detect precision from input
/// - Hours/minutes/seconds/milliseconds/microseconds/nanoseconds: specified precision to use
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum SchemalessPrecision {
    NonConfigured = 0,
    Hours,
    Minutes,
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}
pub type TSDB_SML_TIMESTAMP_TYPE = SchemalessPrecision;
pub const TSDB_SML_TIMESTAMP_NOT_CONFIGURED: TSDB_SML_TIMESTAMP_TYPE =
    TSDB_SML_TIMESTAMP_TYPE::NonConfigured;
pub const TSDB_SML_TIMESTAMP_HOURS: TSDB_SML_TIMESTAMP_TYPE = TSDB_SML_TIMESTAMP_TYPE::Hours;
pub const TSDB_SML_TIMESTAMP_MINUTES: TSDB_SML_TIMESTAMP_TYPE = TSDB_SML_TIMESTAMP_TYPE::Minutes;
pub const TSDB_SML_TIMESTAMP_SECONDS: TSDB_SML_TIMESTAMP_TYPE = TSDB_SML_TIMESTAMP_TYPE::Seconds;
pub const TSDB_SML_TIMESTAMP_MILLISECONDS: TSDB_SML_TIMESTAMP_TYPE =
    TSDB_SML_TIMESTAMP_TYPE::Milliseconds;
pub const TSDB_SML_TIMESTAMP_MICROSECONDS: TSDB_SML_TIMESTAMP_TYPE =
    TSDB_SML_TIMESTAMP_TYPE::Microseconds;
pub const TSDB_SML_TIMESTAMP_NANOSECONDS: TSDB_SML_TIMESTAMP_TYPE =
    TSDB_SML_TIMESTAMP_TYPE::Nanoseconds;

extern "C" {
    pub fn taos_schemaless_insert(
        taos: *mut TAOS,
        lines: *mut *mut c_char,
        numLines: c_int,
        protocol: SchemalessProtocol,
        precision: TSDB_SML_TIMESTAMP_TYPE,
    ) -> *mut TAOS_RES;
}

#[test]
#[cfg(taos_v2)] // TODO: SML in v3 is unimplemented.
fn test_sml() {
    use std::ptr;
    unsafe {
        let null = ptr::null();
        let mut lines = [
            b"st,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"a, abc\",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000\0\0" as *const u8 as *mut c_char
        ];
        let taos = crate::taos_connect(ptr::null(), ptr::null(), null, null, 0);
        let res = crate::taos_query(taos, b"create database _rs_sml_\0" as *const u8 as _);
        crate::taos_free_result(res);
        let _ = crate::taos_query(taos, b"use _rs_sml_\0" as *const u8 as _);

        let res = crate::taos_schemaless_insert(
            taos,
            &mut lines as _,
            1,
            SchemalessProtocol::Line,
            TSDB_SML_TIMESTAMP_TYPE::NonConfigured,
        );
        assert!(crate::taos_errno(res) == 0);
        crate::taos_free_result(res);
        let res = crate::taos_query(taos, b"select * from st\0" as *const u8 as _);

        crate::taos_free_result(res);
        assert!(!taos.is_null());
        crate::taos_close(taos);
    }
}
