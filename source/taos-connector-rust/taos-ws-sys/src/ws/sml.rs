use std::ffi::{c_char, c_int, c_void, CStr};
use std::time::Duration;
use std::{ptr, slice};

use taos_error::Code;
use taos_query::block_in_place_or_global;
use taos_query::common::{Precision, SchemalessPrecision, SchemalessProtocol, SmlDataBuilder, Ty};
use taos_query::util::generate_req_id;
use taos_ws::query::Error;
use taos_ws::{Offset, Taos};
use tracing::{debug, error};

use crate::ws::error::{set_err_and_get_code, TaosError, TaosMaybeError};
use crate::ws::{
    ResultSet, ResultSetOperations, TaosResult, TAOS, TAOS_FIELD, TAOS_FIELD_E, TAOS_RES, TAOS_ROW,
};

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_PROTOCOL_TYPE {
    TSDB_SML_UNKNOWN_PROTOCOL,
    TSDB_SML_LINE_PROTOCOL,
    TSDB_SML_TELNET_PROTOCOL,
    TSDB_SML_JSON_PROTOCOL,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum TSDB_SML_TIMESTAMP_TYPE {
    TSDB_SML_TIMESTAMP_NOT_CONFIGURED,
    TSDB_SML_TIMESTAMP_HOURS,
    TSDB_SML_TIMESTAMP_MINUTES,
    TSDB_SML_TIMESTAMP_SECONDS,
    TSDB_SML_TIMESTAMP_MILLI_SECONDS,
    TSDB_SML_TIMESTAMP_MICRO_SECONDS,
    TSDB_SML_TIMESTAMP_NANO_SECONDS,
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_schemaless_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        0,
        generate_req_id() as i64,
        ptr::null_mut(),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        0,
        reqid,
        ptr::null_mut(),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        ttl,
        generate_req_id() as i64,
        ptr::null_mut(),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
        taos,
        lines,
        len,
        totalRows,
        protocol,
        precision,
        ttl,
        reqid,
        ptr::null_mut(),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    debug!("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key start, taos: {taos:?}, lines: {lines:?}, len: {len}, total_rows: {totalRows:?}, protocol: {protocol}, precision: {precision}, ttl: {ttl}, reqid: {reqid}, tbname_key: {tbnameKey:?}");
    match sml_insert_raw(
        taos, lines, len, totalRows, protocol, precision, ttl, reqid, tbnameKey,
    ) {
        Ok(rs) => {
            debug!("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key succ, rs: {rs:?}");
            let res: TaosMaybeError<ResultSet> = rs.into();
            Box::into_raw(Box::new(res)) as _
        }
        Err(err) => {
            error!("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key failed, err: {err:?}");
            set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
            ptr::null_mut()
        }
    }
}

#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
unsafe fn sml_insert_raw(
    taos: *mut TAOS,
    lines: *mut c_char,
    len: c_int,
    totalRows: *mut i32,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> TaosResult<ResultSet> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;

    if lines.is_null() {
        return Err(TaosError::new(Code::INVALID_PARA, "lines is null"));
    }

    if len < 0 {
        return Err(TaosError::new(Code::INVALID_PARA, "len is less than 0"));
    }

    let slice: &[u8] = slice::from_raw_parts(lines as _, len as _);

    let data = String::from_utf8(slice.to_vec())?;

    debug!("sml_insert_raw, data: {data:?}");

    let sml_data = if !tbnameKey.is_null() {
        let tbname_key = CStr::from_ptr(tbnameKey)
            .to_str()
            .map_err(|_| TaosError::new(Code::INVALID_PARA, "tbnameKey is invalid utf-8"))?;

        SmlDataBuilder::default()
            .protocol(SchemalessProtocol::from(protocol))
            .precision(SchemalessPrecision::from(precision))
            .data(vec![data])
            .ttl(ttl)
            .req_id(reqid as u64)
            .table_name_key(tbname_key.to_string())
            .build()?
    } else {
        SmlDataBuilder::default()
            .protocol(SchemalessProtocol::from(protocol))
            .precision(SchemalessPrecision::from(precision))
            .data(vec![data])
            .ttl(ttl)
            .req_id(reqid as u64)
            .build()?
    };

    debug!("sml_insert_raw, sml_data: {sml_data:?}");

    let (affected_rows, total_rows) = block_in_place_or_global(taos.client().s_put(&sml_data))?;

    if !totalRows.is_null() {
        *totalRows = total_rows.unwrap_or(0) as _;
    }

    Ok(ResultSet::Schemaless(SchemalessResultSet::new(
        affected_rows.unwrap_or(0) as _,
        Precision::Millisecond,
        Duration::from_millis(0),
    )))
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_schemaless_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
) -> *mut TAOS_RES {
    taos_schemaless_insert_ttl_with_reqid_tbname_key(
        taos,
        lines,
        numLines,
        protocol,
        precision,
        0,
        generate_req_id() as i64,
        ptr::null_mut(),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_schemaless_insert_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    reqid: i64,
) -> *mut TAOS_RES {
    taos_schemaless_insert_ttl_with_reqid_tbname_key(
        taos,
        lines,
        numLines,
        protocol,
        precision,
        0,
        reqid,
        ptr::null_mut(),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_schemaless_insert_ttl(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
) -> *mut TAOS_RES {
    taos_schemaless_insert_ttl_with_reqid_tbname_key(
        taos,
        lines,
        numLines,
        protocol,
        precision,
        ttl,
        generate_req_id() as i64,
        ptr::null_mut(),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn taos_schemaless_insert_ttl_with_reqid(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
) -> *mut TAOS_RES {
    taos_schemaless_insert_ttl_with_reqid_tbname_key(
        taos,
        lines,
        numLines,
        protocol,
        precision,
        ttl,
        reqid,
        ptr::null_mut(),
    )
}

#[no_mangle]
#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn taos_schemaless_insert_ttl_with_reqid_tbname_key(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> *mut TAOS_RES {
    debug!("taos_schemaless_insert_ttl_with_reqid_tbname_key start, taos: {taos:?}, lines: {lines:?}, num_lines: {numLines}, protocol: {protocol}, precision: {precision}, ttl: {ttl}, reqid: {reqid}, tbname_key: {tbnameKey:?}");
    match sml_insert(
        taos, lines, numLines, protocol, precision, ttl, reqid, tbnameKey,
    ) {
        Ok(rs) => {
            debug!("taos_schemaless_insert_ttl_with_reqid_tbname_key succ, rs: {rs:?}");
            let res: TaosMaybeError<ResultSet> = rs.into();
            Box::into_raw(Box::new(res)) as _
        }
        Err(err) => {
            error!("taos_schemaless_insert_ttl_with_reqid_tbname_key failed, err: {err:?}");
            set_err_and_get_code(TaosError::new(err.code(), &err.to_string()));
            ptr::null_mut()
        }
    }
}

#[allow(non_snake_case)]
#[allow(clippy::too_many_arguments)]
unsafe fn sml_insert(
    taos: *mut TAOS,
    lines: *mut *mut c_char,
    mut numLines: c_int,
    protocol: c_int,
    precision: c_int,
    ttl: i32,
    reqid: i64,
    tbnameKey: *mut c_char,
) -> TaosResult<ResultSet> {
    let taos = (taos as *mut Taos)
        .as_mut()
        .ok_or(TaosError::new(Code::INVALID_PARA, "taos is null"))?;

    if lines.is_null() {
        return Err(TaosError::new(Code::INVALID_PARA, "lines is null"));
    }

    if numLines < 0 {
        return Err(TaosError::new(
            Code::INVALID_PARA,
            "numLines is less than 0",
        ));
    }

    if SchemalessProtocol::from(protocol) == SchemalessProtocol::Json {
        numLines = 1;
    }

    let lines: &[*mut c_char] = slice::from_raw_parts(lines as _, numLines as _);
    let mut datas = Vec::with_capacity(numLines as _);

    for line in lines {
        if line.is_null() {
            return Err(TaosError::new(Code::INVALID_PARA, "line is null"));
        }
        let data = CStr::from_ptr(*line)
            .to_str()
            .map_err(|_| TaosError::new(Code::INVALID_PARA, "line is invalid utf-8"))?;
        datas.push(data.to_string());
    }

    debug!("sml_insert, datas: {datas:?}");

    let sml_data = if !tbnameKey.is_null() {
        let tbname_key = CStr::from_ptr(tbnameKey)
            .to_str()
            .map_err(|_| TaosError::new(Code::INVALID_PARA, "tbnameKey is invalid utf-8"))?;

        SmlDataBuilder::default()
            .protocol(SchemalessProtocol::from(protocol))
            .precision(SchemalessPrecision::from(precision))
            .data(datas)
            .ttl(ttl)
            .req_id(reqid as u64)
            .table_name_key(tbname_key.to_string())
            .build()?
    } else {
        SmlDataBuilder::default()
            .protocol(SchemalessProtocol::from(protocol))
            .precision(SchemalessPrecision::from(precision))
            .data(datas)
            .ttl(ttl)
            .req_id(reqid as u64)
            .build()?
    };

    debug!("sml_insert, sml_data: {sml_data:?}");

    let (affected_rows, _) = block_in_place_or_global(taos.client().s_put(&sml_data))?;

    Ok(ResultSet::Schemaless(SchemalessResultSet::new(
        affected_rows.unwrap_or(0) as _,
        Precision::Millisecond,
        Duration::from_millis(0),
    )))
}

#[derive(Debug)]
pub struct SchemalessResultSet {
    affected_rows: i64,
    precision: Precision,
    timing: Duration,
}

impl SchemalessResultSet {
    fn new(affected_rows: i64, precision: Precision, timing: Duration) -> Self {
        Self {
            affected_rows,
            precision,
            timing,
        }
    }
}

impl ResultSetOperations for SchemalessResultSet {
    fn tmq_get_topic_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_db_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_table_name(&self) -> *const c_char {
        ptr::null()
    }

    fn tmq_get_vgroup_offset(&self) -> i64 {
        0
    }

    fn tmq_get_vgroup_id(&self) -> i32 {
        0
    }

    fn tmq_get_offset(&self) -> Offset {
        todo!()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    fn affected_rows(&self) -> i32 {
        self.affected_rows as _
    }

    fn affected_rows64(&self) -> i64 {
        self.affected_rows
    }

    fn num_of_fields(&self) -> i32 {
        0
    }

    fn get_fields(&mut self) -> *mut TAOS_FIELD {
        ptr::null_mut()
    }

    fn get_fields_e(&mut self) -> *mut TAOS_FIELD_E {
        ptr::null_mut()
    }

    unsafe fn fetch_raw_block(
        &mut self,
        ptr: *mut *mut c_void,
        rows: *mut i32,
    ) -> Result<(), Error> {
        Err(Error::CommonError(
            "schemaless result set does not support fetch block".to_owned(),
        ))
    }

    unsafe fn fetch_block(&mut self, rows: *mut TAOS_ROW, num: *mut c_int) -> Result<(), Error> {
        todo!()
    }

    unsafe fn fetch_row(&mut self) -> Result<TAOS_ROW, Error> {
        Err(Error::CommonError(
            "schemaless result set does not support fetch row".to_owned(),
        ))
    }

    unsafe fn get_raw_value(&mut self, row: usize, col: usize) -> (Ty, u32, *const c_void) {
        (Ty::Null, 0, ptr::null())
    }

    fn take_timing(&mut self) -> Duration {
        self.timing
    }

    fn stop_query(&mut self) {}

    unsafe fn is_null_by_column(
        &mut self,
        column_index: usize,
        result: *mut bool,
        rows: *mut c_int,
    ) -> Result<(), TaosError> {
        Err(TaosError::new(
            Code::FAILED,
            "schemaless result set does not support is_null_by_column",
        ))
    }

    fn get_column_data_offset(&mut self, column_index: usize) -> Result<*mut c_int, TaosError> {
        Err(TaosError::new(
            Code::FAILED,
            "schemaless result set does not support get_column_data_offset",
        ))
    }
}

#[cfg(test)]
mod tests {
    use taos_query::util::generate_req_id;

    use super::*;
    use crate::ws::query::{taos_affected_rows, taos_free_result};
    use crate::ws::sml::{TSDB_SML_PROTOCOL_TYPE, TSDB_SML_TIMESTAMP_TYPE};
    use crate::ws::{taos_close, test_connect, test_exec, test_exec_many};

    #[test]
    fn test_sml_insert_raw() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1737291333",
                    "create database test_1737291333",
                    "use test_1737291333",
                ],
            );

            let data = c"measurement,host=host1 field1=2i,field2=2.0 1741153642000";
            let len = data.to_bytes().len() as i32;
            let lines = data.as_ptr() as *mut _;
            let total_rows = ptr::null_mut();
            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 0;

            let res = taos_schemaless_insert_raw(taos, lines, len, total_rows, protocol, precision);
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_raw_with_reqid(
                taos, lines, len, total_rows, protocol, precision, req_id,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let res = taos_schemaless_insert_raw_ttl(
                taos, lines, len, total_rows, protocol, precision, ttl,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_raw_ttl_with_reqid(
                taos, lines, len, total_rows, protocol, precision, ttl, req_id,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let tbname_key = c"host".as_ptr() as *mut _;
            let res = taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
                taos, lines, len, total_rows, protocol, precision, ttl, req_id, tbname_key,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
                taos,
                lines,
                len,
                total_rows,
                protocol,
                precision,
                ttl,
                req_id,
                ptr::null_mut(),
            );
            assert!(!res.is_null());
            taos_free_result(res);

            test_exec(taos, "drop database test_1737291333");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741168851",
                    "create database test_1741168851",
                    "use test_1741168851",
                ],
            );

            let data = "measurement field1=2i,field2=2.0 1741153642000\n#measurement field1=2i,field2=2.0 1741153643000\nmeasurement field1=2i,field2=2.0 1741153644000\n#comment";
            let data = data.as_bytes();
            let len = data.len() as i32;
            let lines = data.as_ptr() as *mut _;
            let mut total_rows = 0;
            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 0;

            let res =
                taos_schemaless_insert_raw(taos, lines, len, &mut total_rows, protocol, precision);
            assert!(!res.is_null());
            assert_eq!(total_rows, 2);

            let affected_rows = taos_affected_rows(res);
            assert_eq!(affected_rows, 2);

            taos_free_result(res);
            test_exec(taos, "drop database test_1741168851");
            taos_close(taos);
        }

        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741608340",
                    "create database test_1741608340",
                    "use test_1741608340",
                ],
            );

            let data = r#"[{"metric":"metric_json","timestamp":1626846400,"value":10.3,"tags":{"groupid":2,"location":"California.SanFrancisco","id":"d1001"}}]"#;

            let data = data.as_bytes();
            let len = data.len() as i32;
            let lines = data.as_ptr() as *mut _;
            let mut total_rows = 0;
            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_JSON_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 0;

            let res =
                taos_schemaless_insert_raw(taos, lines, len, &mut total_rows, protocol, precision);
            assert!(!res.is_null());
            assert_eq!(total_rows, 1);

            taos_free_result(res);
            test_exec(taos, "drop database test_1741608340");
            taos_close(taos);
        }
    }

    #[test]
    fn test_sml_insert() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1741166212",
                    "create database test_1741166212",
                    "use test_1741166212",
                ],
            );

            let line1 = c"measurement,host=host1 field1=2i,field2=2.0 1741153642000";
            let line2 = c"measurement,host=host2 field1=3i,field2=3.0 1741153643000";
            let mut lines = vec![line1.as_ptr() as _, line2.as_ptr() as _];

            let num_lines = lines.len() as i32;
            let lines = lines.as_mut_ptr();

            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_LINE_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;
            let ttl = 20;

            let res = taos_schemaless_insert(taos, lines, num_lines, protocol, precision);
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_with_reqid(
                taos, lines, num_lines, protocol, precision, req_id,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let res = taos_schemaless_insert_ttl(taos, lines, num_lines, protocol, precision, ttl);
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_ttl_with_reqid(
                taos, lines, num_lines, protocol, precision, ttl, req_id,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let tbname_key = c"host".as_ptr() as *mut _;
            let res = taos_schemaless_insert_ttl_with_reqid_tbname_key(
                taos, lines, num_lines, protocol, precision, ttl, req_id, tbname_key,
            );
            assert!(!res.is_null());
            taos_free_result(res);

            let req_id = generate_req_id() as _;
            let res = taos_schemaless_insert_ttl_with_reqid_tbname_key(
                taos,
                lines,
                num_lines,
                protocol,
                precision,
                ttl,
                req_id,
                ptr::null_mut(),
            );
            assert!(!res.is_null());
            taos_free_result(res);

            test_exec(taos, "drop database test_1741166212");
            taos_close(taos);
        }
    }

    #[test]
    fn test_sml_telnet() {
        unsafe {
            let taos = test_connect();
            test_exec_many(
                taos,
                &[
                    "drop database if exists test_1742376152",
                    "create database test_1742376152",
                    "use test_1742376152",
                ],
            );

            let line1 = cr#"stb13 1742375795074 L"wuxX"  t0=-12859061i32 t1=-876196531i64 t2=783043840.000000f32 t3=851331526.930689f64 t4=-5047i16 t5=102i8 t6=false t7=L"QSfm7v""#;
            let line2 = cr#"stb13 1742375795075 L""  t0=-12859061i32 t1=-876196531i64 t2=783043840.000000f32 t3=851331526.930689f64 t4=-5047i16 t5=102i8 t6=false t7=L"QSfm7v""#;
            let mut lines = vec![line1.as_ptr() as _, line2.as_ptr() as _];

            let num_lines = lines.len() as i32;
            let lines = lines.as_mut_ptr();

            let protocol = TSDB_SML_PROTOCOL_TYPE::TSDB_SML_TELNET_PROTOCOL as i32;
            let precision = TSDB_SML_TIMESTAMP_TYPE::TSDB_SML_TIMESTAMP_MILLI_SECONDS as i32;

            let res = taos_schemaless_insert(taos, lines, num_lines, protocol, precision);
            assert!(!res.is_null());

            let affected_rows = taos_affected_rows(res);
            assert_eq!(affected_rows, 2);

            taos_free_result(res);
            test_exec(taos, "drop database test_1742376152");
            taos_close(taos);
        }
    }

    #[test]
    fn test_is_null_by_column_error() {
        let mut rs = SchemalessResultSet::new(
            0,
            Precision::Millisecond,
            std::time::Duration::from_millis(0),
        );
        unsafe {
            let err = rs
                .is_null_by_column(0, std::ptr::null_mut(), std::ptr::null_mut())
                .unwrap_err();
            assert_eq!(err.code(), Code::FAILED);
        }
    }

    #[test]
    fn test_get_column_data_offset_error() {
        let mut rs = SchemalessResultSet::new(
            0,
            Precision::Millisecond,
            std::time::Duration::from_millis(0),
        );
        let err = rs.get_column_data_offset(0).unwrap_err();
        assert_eq!(err.code(), Code::FAILED);
    }
}
