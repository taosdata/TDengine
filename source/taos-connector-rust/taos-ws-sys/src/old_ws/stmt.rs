use std::ffi::c_void;
use std::fmt::Debug;
use std::os::raw::*;

use taos_query::common::Value;
use taos_query::prelude::Itertools;
use taos_query::stmt::Bindable;
use taos_ws::stmt::{StmtField as WsStmtField, WsFieldsable};
use taos_ws::Stmt;

use super::*;

#[repr(C)]
#[derive(Debug, Clone)]
pub struct StmtField {
    name: [c_char; 65usize],
    r#type: i8,
    precision: u8,
    scale: u8,
    bytes: i32,
}

impl From<WsStmtField> for StmtField {
    fn from(f: WsStmtField) -> Self {
        let f_name = f.name.as_str();
        let mut name = [0 as c_char; 65usize];
        unsafe {
            std::ptr::copy_nonoverlapping(f_name.as_ptr(), name.as_mut_ptr() as _, f_name.len());
        };

        Self {
            name,
            r#type: f.field_type,
            precision: f.precision,
            scale: f.scale,
            bytes: f.bytes,
        }
    }
}

/// Opaque STMT type alias.
#[allow(non_camel_case_types)]
pub type WS_STMT = c_void;

unsafe fn stmt_init(taos: *const WS_TAOS, req_id: u64) -> WsResult<Stmt> {
    let client = (taos as *mut Taos)
        .as_mut()
        .ok_or(WsError::new(Code::FAILED, "client pointer it null"))?;
    Ok(taos_ws::Stmt::init_with_req_id(client, req_id)?)
}

/// Create new stmt object.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_init(taos: *const WS_TAOS) -> *mut WS_STMT {
    ws_stmt_init_with_reqid(taos, get_req_id(taos))
}

/// Create new stmt object with req_id.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_init_with_reqid(
    taos: *const WS_TAOS,
    req_id: u64,
) -> *mut WS_STMT {
    let stmt: WsMaybeError<Stmt> = stmt_init(taos, req_id).into();
    Box::into_raw(Box::new(stmt)) as _
}

/// Prepare with sql command
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_prepare(
    stmt: *mut WS_STMT,
    sql: *const c_char,
    len: c_ulong,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let sql = if len > 0 {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(sql as _, len as _))
            } else {
                CStr::from_ptr(sql).to_str().expect(
                    "ws_stmt_prepare with a sql len 0 means the input should always be valid utf-8",
                )
            };

            if let Some(no) = stmt.errno() {
                return get_err_code_fromated(no);
            }

            if let Err(e) = stmt
                .safe_deref_mut()
                .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
                .and_then(|stmt| stmt.prepare(sql))
            {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            } else {
                stmt.error = None;
                clear_error_info();
                Code::SUCCESS.into()
            }
        }
        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

/// Set table name.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_set_tbname(stmt: *mut WS_STMT, name: *const c_char) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let name = CStr::from_ptr(name).to_str().unwrap();

            if let Err(e) = stmt
                .safe_deref_mut()
                .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
                .and_then(|stmt| stmt.set_tbname(name))
            {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            } else {
                stmt.error = None;
                clear_error_info();
                0
            }
        }
        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

/// Set sub table name.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_set_sub_tbname(stmt: *mut WS_STMT, name: *const c_char) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let name = CStr::from_ptr(name).to_str().unwrap();

            if let Err(e) = stmt
                .safe_deref_mut()
                .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
                .and_then(|stmt| stmt.set_tbname(name))
            {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            } else {
                stmt.error = None;
                clear_error_info();
                0
            }
        }
        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

/// Set table name and tags.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_set_tbname_tags(
    stmt: *mut WS_STMT,
    name: *const c_char,
    bind: *const WS_MULTI_BIND,
    len: u32,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let name = CStr::from_ptr(name).to_str().unwrap();
            let tags = std::slice::from_raw_parts(bind, len as usize)
                .iter()
                .map(TaosMultiBind::to_tag_value)
                .collect_vec();

            if let Err(e) = stmt
                .safe_deref_mut()
                .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
                .and_then(|stmt| stmt.set_tbname_tags(name, &tags))
            {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            } else {
                stmt.error = None;
                clear_error_info();
                0
            }
        }
        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

/// Get tag fields.
///
/// Please always use `ws_stmt_reclaim_fields` to free memory.
#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_get_tag_fields(
    stmt: *mut WS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut StmtField,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => match stmt
            .safe_deref_mut()
            .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
            .and_then(WsFieldsable::get_tag_fields)
        {
            Ok(fields_vec) => {
                let fields_vec: Vec<StmtField> = fields_vec.into_iter().map(|f| f.into()).collect();

                *fieldNum = fields_vec.len() as _;

                if fields_vec.is_empty() {
                    *fields = std::ptr::null_mut();
                } else {
                    *fields = Box::into_raw(fields_vec.into_boxed_slice()) as _;
                }

                clear_error_info();
                stmt.error = None;
                0
            }

            Err(e) => {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            }
        },

        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

/// Get col fields.
///
/// Please always use `ws_stmt_reclaim_fields` to free memory.
#[allow(non_snake_case)]
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_get_col_fields(
    stmt: *mut WS_STMT,
    fieldNum: *mut c_int,
    fields: *mut *mut StmtField,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => match stmt
            .safe_deref_mut()
            .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
            .and_then(WsFieldsable::get_col_fields)
        {
            Ok(fields_vec) => {
                let fields_vec: Vec<StmtField> = fields_vec.into_iter().map(|f| f.into()).collect();
                *fieldNum = fields_vec.len() as _;
                *fields = Box::into_raw(fields_vec.into_boxed_slice()) as _;
                stmt.error = None;
                clear_error_info();
                0
            }

            Err(e) => {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            }
        },

        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

/// Free memory of fields that was allocated by `ws_stmt_get_tag_fields` or `ws_stmt_get_col_fields`.
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_reclaim_fields(
    stmt: *mut WS_STMT,
    fields: *mut *mut StmtField,
    fieldNum: c_int,
) -> c_int {
    let _ = Vec::from_raw_parts(*fields, fieldNum as usize, fieldNum as usize);
    0
}

/// Currently only insert sql is supported.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_is_insert(stmt: *mut WS_STMT, insert: *mut c_int) -> c_int {
    let _ = stmt;
    *insert = 1;
    0
}

#[derive(Debug)]
#[repr(transparent)]
#[allow(non_camel_case_types)]
pub struct WS_BIND(TaosMultiBind);

#[repr(C)]
#[derive(Debug, Clone)]
pub struct TaosMultiBind {
    pub buffer_type: c_int,
    pub buffer: *const c_void,
    pub buffer_length: usize,
    pub length: *const i32,
    pub is_null: *const c_char,
    pub num: c_int,
}

#[allow(non_camel_case_types)]
pub type WS_MULTI_BIND = TaosMultiBind;

impl TaosMultiBind {
    pub fn new(ty: Ty) -> Self {
        Self {
            buffer_type: ty as _,
            buffer: std::ptr::null_mut(),
            buffer_length: 0,
            length: std::ptr::null_mut(),
            is_null: std::ptr::null_mut(),
            num: 1,
        }
    }

    pub fn ty(&self) -> Ty {
        self.buffer_type.into()
    }

    pub fn first_to_json(&self) -> serde_json::Value {
        self.to_json().as_array().unwrap().first().unwrap().clone()
    }

    pub fn to_tag_value(&self) -> Value {
        if !self.is_null.is_null() && unsafe { self.is_null.read() != 0 } {
            return Value::Null(self.ty());
        }
        match Ty::from(self.buffer_type) {
            Ty::Null => Value::Null(self.ty()),
            Ty::Bool => unsafe { Value::Bool(*(self.buffer as *const bool)) },
            Ty::TinyInt => unsafe { Value::TinyInt(*(self.buffer as *const i8)) },
            Ty::SmallInt => unsafe { Value::SmallInt(*(self.buffer as *const i16)) },
            Ty::Int => unsafe { Value::Int(*(self.buffer as *const i32)) },
            Ty::BigInt => unsafe { Value::BigInt(*(self.buffer as *const _)) },
            Ty::UTinyInt => unsafe { Value::UTinyInt(*(self.buffer as *const _)) },
            Ty::USmallInt => unsafe { Value::USmallInt(*(self.buffer as *const _)) },
            Ty::UInt => unsafe { Value::UInt(*(self.buffer as *const _)) },
            Ty::UBigInt => unsafe { Value::UBigInt(*(self.buffer as *const _)) },
            Ty::Float => unsafe { Value::Float(*(self.buffer as *const _)) },
            Ty::Double => unsafe { Value::Double(*(self.buffer as *const _)) },
            Ty::Timestamp => unsafe {
                Value::Timestamp(Timestamp::Milliseconds(*(self.buffer as *const _)))
            },
            Ty::VarChar => unsafe {
                assert!(!self.length.is_null());
                assert!(!self.buffer.is_null());
                let slice =
                    std::slice::from_raw_parts(self.buffer as _, self.length.read() as usize);
                let v = std::str::from_utf8_unchecked(slice);
                Value::VarChar(v.to_string())
            },
            Ty::NChar => unsafe {
                assert!(!self.length.is_null());
                assert!(!self.buffer.is_null());
                let slice =
                    std::slice::from_raw_parts(self.buffer as _, self.length.read() as usize);
                let v = std::str::from_utf8_unchecked(slice);
                Value::NChar(v.to_string())
            },
            Ty::Json => unsafe {
                assert!(!self.length.is_null());
                assert!(!self.buffer.is_null());
                let slice =
                    std::slice::from_raw_parts(self.buffer as _, self.length.read() as usize);
                Value::Json(serde_json::from_slice(slice).unwrap())
            },
            _ => todo!(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::{json, Value};
        assert!(self.num > 0, "invalid bind value");
        let len = self.num as usize;

        macro_rules! _nulls {
            () => {
                json!(std::iter::repeat(Value::Null).take(len).collect::<Vec<_>>())
            };
        }
        if self.buffer.is_null() {
            return _nulls!();
        }

        macro_rules! _impl_primitive {
            ($t:ty) => {{
                let slice = std::slice::from_raw_parts(self.buffer as *const $t, len);
                match self.is_null.is_null() {
                    true => json!(slice),
                    false => {
                        let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
                        let column: Vec<_> = slice
                            .iter()
                            .zip(nulls)
                            .map(
                                |(value, is_null)| {
                                    if *is_null {
                                        None
                                    } else {
                                        Some(*value)
                                    }
                                },
                            )
                            .collect();
                        json!(column)
                    }
                }
            }};
        }

        unsafe {
            match Ty::from(self.buffer_type) {
                Ty::Null => _nulls!(),
                Ty::Bool => _impl_primitive!(bool),
                Ty::TinyInt => _impl_primitive!(i8),
                Ty::SmallInt => _impl_primitive!(i16),
                Ty::Int => _impl_primitive!(i32),
                Ty::BigInt => _impl_primitive!(i64),
                Ty::Float => _impl_primitive!(f32),
                Ty::Double => _impl_primitive!(f64),
                Ty::VarChar => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        return json!(column);
                    }
                    let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
                    let column = (0..len)
                        .zip(nulls)
                        .map(|(i, is_null)| {
                            if *is_null {
                                None
                            } else {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(std::str::from_utf8_unchecked(bytes))
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                Ty::Timestamp => _impl_primitive!(i64),
                Ty::NChar => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        return json!(column);
                    }
                    let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
                    let column = (0..len)
                        .zip(nulls)
                        .map(|(i, is_null)| {
                            if *is_null {
                                None
                            } else {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(std::str::from_utf8_unchecked(bytes))
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                Ty::UTinyInt => _impl_primitive!(u8),
                Ty::USmallInt => _impl_primitive!(u16),
                Ty::UInt => _impl_primitive!(u32),
                Ty::UBigInt => _impl_primitive!(u64),
                Ty::Json => {
                    let len = self.num as usize;
                    if self.is_null.is_null() {
                        let column = (0..len)
                            .map(|i| {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                std::str::from_utf8_unchecked(bytes)
                            })
                            .collect::<Vec<_>>();
                        return json!(column);
                    }
                    let nulls = std::slice::from_raw_parts(self.is_null as *const bool, len);
                    let column = (0..len)
                        .zip(nulls)
                        .map(|(i, is_null)| {
                            if *is_null {
                                None
                            } else {
                                let ptr = (self.buffer as *const u8)
                                    .offset(self.buffer_length as isize * i as isize);
                                let len = *self.length.add(i) as usize;
                                let bytes = std::slice::from_raw_parts(ptr, len);
                                Some(serde_json::from_slice::<serde_json::Value>(bytes).unwrap())
                            }
                        })
                        .collect::<Vec<_>>();
                    json!(column)
                }
                Ty::VarBinary => todo!(),
                Ty::Decimal => todo!(),
                Ty::Blob => todo!(),
                Ty::MediumBlob => todo!(),
                _ => todo!(),
            }
        }
    }
}

#[cfg(test)]
impl TaosMultiBind {
    pub(crate) fn from_primitives<T: taos_query::common::itypes::IValue>(
        nulls: Vec<bool>,
        values: &[T],
    ) -> Self {
        TaosMultiBind {
            buffer_type: T::TY as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<T>(),
            length: values.len() as _,
            is_null: std::mem::ManuallyDrop::new(nulls).as_ptr() as _,
            num: values.len() as _,
        }
    }

    pub(crate) fn from_raw_timestamps(nulls: Vec<bool>, values: &[i64]) -> Self {
        TaosMultiBind {
            buffer_type: Ty::Timestamp as _,
            buffer: values.as_ptr() as _,
            buffer_length: std::mem::size_of::<i64>(),
            length: values.len() as _,
            is_null: std::mem::ManuallyDrop::new(nulls).as_ptr() as _,
            num: values.len() as _,
        }
    }

    pub(crate) fn from_binary_vec(values: &[Option<impl AsRef<[u8]>>]) -> Self {
        let mut buffer_length = 0;
        let num = values.len();
        let mut nulls = std::mem::ManuallyDrop::new(Vec::with_capacity(num));
        nulls.resize(num, false);
        let mut length: std::mem::ManuallyDrop<Vec<i32>> =
            std::mem::ManuallyDrop::new(Vec::with_capacity(num));
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                length.push(v.len() as _);
                if v.len() > buffer_length {
                    buffer_length = v.len();
                }
            } else {
                length.push(-1);
                nulls[i] = true;
            }
        }
        let buffer_size = buffer_length * values.len();
        let mut buffer: std::mem::ManuallyDrop<Vec<u8>> =
            std::mem::ManuallyDrop::new(Vec::with_capacity(buffer_size));
        unsafe { buffer.set_len(buffer_size) };
        buffer.fill(0);
        for (i, v) in values.iter().enumerate() {
            if let Some(v) = v {
                let v = v.as_ref();
                unsafe {
                    let dst = buffer.as_mut_ptr().add(buffer_length * i);
                    std::ptr::copy_nonoverlapping(v.as_ptr(), dst, v.len());
                }
            }
        }
        TaosMultiBind {
            buffer_type: Ty::VarChar as _,
            buffer: buffer.as_ptr() as _,
            buffer_length,
            length: length.as_ptr() as _,
            is_null: nulls.as_ptr() as _,
            num: num as _,
        }
    }

    pub(crate) fn from_string_vec(values: &[Option<impl AsRef<str>>]) -> Self {
        let values: Vec<_> = values
            .iter()
            .map(|f| f.as_ref().map(|s| s.as_ref().as_bytes()))
            .collect();
        let mut s = Self::from_binary_vec(&values);
        s.buffer_type = Ty::NChar as _;
        s
    }
}

impl Drop for TaosMultiBind {
    fn drop(&mut self) {
        unsafe { Vec::from_raw_parts(self.is_null as *mut i8, self.num as _, self.num as _) };

        let ty = Ty::from(self.buffer_type);
        if ty == Ty::VarChar || ty == Ty::NChar {
            let buffer_size = self.buffer_length * self.num as usize;
            unsafe {
                Vec::from_raw_parts(self.buffer as *mut u8, buffer_size, buffer_size);
                Vec::from_raw_parts(self.length as *mut i32, self.num as _, self.num as _);
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_stmt_set_tags(
    stmt: *mut WS_STMT,
    bind: *const WS_MULTI_BIND,
    len: u32,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let columns = std::slice::from_raw_parts(bind, len as usize)
                .iter()
                .map(TaosMultiBind::to_tag_value)
                .collect_vec();

            if let Err(e) = stmt
                .safe_deref_mut()
                .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
                .and_then(|s| s.set_tags(&columns))
            {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            } else {
                0
            }
        }
        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_stmt_bind_param_batch(
    stmt: *mut WS_STMT,
    bind: *const WS_MULTI_BIND,
    len: u32,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            let columns = std::slice::from_raw_parts(bind, len as usize)
                .iter()
                .map(TaosMultiBind::to_json)
                .collect();

            if let Err(e) = stmt
                .safe_deref_mut()
                .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
                .and_then(|s| taos_query::block_in_place_or_global(s.stmt_bind(columns)))
            {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            } else {
                0
            }
        }
        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ws_stmt_add_batch(stmt: *mut WS_STMT) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => {
            if let Err(e) = stmt
                .safe_deref_mut()
                .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
                .and_then(|stmt| stmt.add_batch())
            {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            } else {
                0
            }
        }
        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

/// Execute the bind batch, get inserted rows in `affected_row` pointer.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_execute(stmt: *mut WS_STMT, affected_rows: *mut i32) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => match stmt
            .safe_deref_mut()
            .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
            .and_then(Bindable::execute)
        {
            Ok(rows) => {
                *affected_rows = rows as _;
                0
            }
            Err(e) => {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            }
        },
        _ => set_error_and_get_code(WsError::new(
            Code::INVALID_PARA,
            "stmt ptr should not be null",
        )),
    }
}

/// Get inserted rows in current statement.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_affected_rows(stmt: *mut WS_STMT) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(stmt) => stmt.affected_rows() as _,
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "stmt ptr is invalid")),
    }
}

/// Get inserted rows in current statement.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_affected_rows_once(stmt: *mut WS_STMT) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>)
        .as_mut()
        .and_then(|s| s.safe_deref_mut())
    {
        Some(stmt) => stmt.affected_rows_once() as _,
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "stmt ptr is invalid")),
    }
}

/// Get num_params in current statement.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_num_params(stmt: *mut WS_STMT, nums: *mut c_int) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => match stmt
            .safe_deref_mut()
            .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
            .and_then(taos_ws::Stmt::s_num_params)
        {
            Ok(n) => {
                *nums = n as _;
                stmt.error = None;
                clear_error_info();
                0
            }
            Err(e) => {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            }
        },
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "stmt ptr is invalid")),
    }
}

/// Get param by index in current statement.
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_get_param(
    stmt: *mut WS_STMT,
    idx: c_int,
    r#type: *mut c_int,
    bytes: *mut c_int,
) -> c_int {
    match (stmt as *mut WsMaybeError<Stmt>).as_mut() {
        Some(stmt) => match stmt
            .safe_deref_mut()
            .ok_or_else(|| RawError::from_string("stmt ptr should not be null"))
            .and_then(|stmt| stmt.s_get_param(idx as _))
        {
            Ok(param) => {
                *r#type = param.data_type as _;
                *bytes = param.length as _;
                stmt.error = None;
                clear_error_info();
                0
            }
            Err(e) => {
                let errno = e.code();
                stmt.error = Some(WsError::new(errno, &e.to_string()));
                set_error_and_get_code(WsError::new(errno, &e.to_string()))
            }
        },
        _ => set_error_and_get_code(WsError::new(Code::INVALID_PARA, "stmt ptr is invalid")),
    }
}

/// Equivalent to ws_errstr
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_errstr(stmt: *mut WS_STMT) -> *const c_char {
    ws_errstr(stmt as _)
}

/// Same to taos_stmt_close
#[no_mangle]
pub unsafe extern "C" fn ws_stmt_close(stmt: *mut WS_STMT) -> i32 {
    let _ = Box::from_raw(stmt as *mut WsMaybeError<Stmt>);
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stmt_common() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! query {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            query!(b"drop database if exists ws_stmt_i\0");
            query!(b"create database ws_stmt_i keep 36500\0");
            query!(b"create table ws_stmt_i.s1 (ts timestamp, v int, b binary(100))\0");

            let stmt = ws_stmt_init(taos);

            let sql = "insert into ? values(?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            let code = ws_stmt_set_tbname(stmt, b"ws_stmt_i.`s1`\0".as_ptr() as _);

            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }
            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            ws_stmt_close(stmt);
            query!(b"drop database ws_stmt_i\0");
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn stmt_child() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! query {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            query!(b"drop database if exists ws_stmt_i_child\0");
            query!(b"create database ws_stmt_i_child keep 36500\0");
            query!(b"use ws_stmt_i_child\0");
            query!(b"CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(16))\0");
            query!(b"CREATE TABLE `d0` USING `meters` (`groupid`, `location`) TAGS (7, \"San Francisco\")\0");

            let stmt = ws_stmt_init(taos);

            let sql = "insert into ? values(?, ?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            let code = ws_stmt_set_tbname(stmt, b"ws_stmt_i_child.`d0`\0".as_ptr() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[0.0f32, 0.1f32]),
                TaosMultiBind::from_primitives(vec![false, false], &[0, 0]),
                TaosMultiBind::from_primitives(vec![false, false], &[0.0f32, 0.1f32]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            ws_stmt_close(stmt);
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn stmt_tiny_int_null() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! query {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            query!(b"drop database if exists ws_stmt_i_null\0");
            query!(b"create database ws_stmt_i_null keep 36500\0");
            query!(b"use ws_stmt_i_null\0");
            query!(b"create table st(ts timestamp, c1 TINYINT UNSIGNED) tags(utntag TINYINT UNSIGNED)\0");
            query!(b"create table t1 using st tags(0)\0");
            query!(b"create table t2 using st tags(255)\0");
            query!(b"create table t3 using st tags(NULL)\0");

            let stmt = ws_stmt_init(taos);

            let sql = "insert into ? values(?,?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                panic!()
            }

            for tbname in ["t1", "t2", "t3"] {
                let name = format!("ws_stmt_i_null.`{}`\0", tbname);
                let code = ws_stmt_set_tbname(stmt, name.as_ptr() as _);
                if code != 0 {
                    dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                    panic!()
                }

                let params = [
                    TaosMultiBind::from_raw_timestamps(vec![false], &[0]),
                    TaosMultiBind::from_primitives(vec![true], &[0u8]),
                ];
                let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
                if code != 0 {
                    dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
                    panic!()
                }

                ws_stmt_add_batch(stmt);
                let mut rows = 0;
                ws_stmt_execute(stmt, &mut rows);
                assert_eq!(rows, 1);

                let sql = format!("select * from st where tbname = '{tbname}'\0");
                let rs = ws_query(taos, sql.as_bytes().as_ptr() as _);
                let code = ws_errno(rs);
                assert!(code == 0);

                loop {
                    let mut ptr = std::ptr::null();
                    let mut rows = 0;
                    ws_fetch_raw_block(rs, &mut ptr, &mut rows);
                    if rows == 0 {
                        break;
                    }

                    for row in 0..rows {
                        print!("{tbname} row {row}: ");
                        for col in 0..3 {
                            let mut ty = Ty::Null;
                            let mut len = 0;
                            let v = ws_get_value_in_block(
                                rs,
                                row,
                                col,
                                &mut ty as *mut _ as _,
                                &mut len,
                            );
                            if v.is_null() {
                                print!(",NULL");
                            } else {
                                match ty {
                                    Ty::Timestamp => print!("ts: {}", *(v as *const i64)),
                                    _ => print!(",{}", *(v as *const u8)),
                                }
                            }
                        }
                        println!();
                    }
                }

                assert_eq!(ws_free_result(rs), 0);
            }

            ws_stmt_close(stmt);
            assert_eq!(ws_close(taos), 0);
        }
    }

    #[test]
    fn stmt_with_tags() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_stmt_with_tags\0");
            execute!(b"create database ws_stmt_with_tags keep 36500\0");
            execute!(
                b"create table ws_stmt_with_tags.s1 (ts timestamp, v int, b binary(100)) tags(jt json)\0"
            );

            let stmt = ws_stmt_init(taos);
            let sql = "insert into ? using ws_stmt_with_tags.s1 tags(?) values(?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }
            ws_stmt_set_tbname(stmt, b"ws_stmt_with_tags.t1\0".as_ptr() as _);

            let tags = [TaosMultiBind::from_string_vec(&[Some(
                r#"{"name":"姓名"}"#.to_string(),
            )])];

            ws_stmt_set_tags(stmt, tags.as_ptr(), tags.len() as _);

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            ws_stmt_close(stmt);
            execute!(b"drop database if exists ws_stmt_with_tags\0");
            ws_close(taos);
        }
    }

    #[test]
    fn test_stmt_affected_rows() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let c_string = CString::new($sql).expect("CString conversion failed");
                    let c_string_ptr = c_string.as_ptr();
                    let rs = ws_query(taos, c_string_ptr);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }
            let db = "ws_stmt_affected_rows";
            execute!(format!("drop database if exists {db}"));
            execute!(format!("create database {db} keep 36500"));
            execute!(format!("use {db}"));
            execute!("create STABLE s1 (ts timestamp, v int, b binary(100)) tags(jt json)");

            let stmt = ws_stmt_init(taos);
            let sql = "insert into ? using s1 tags(?) values(?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }
            ws_stmt_set_tbname(stmt, b"t1\0".as_ptr() as _);

            let tags = [TaosMultiBind::from_string_vec(&[Some(
                r#"{"name":"姓名"}"#.to_string(),
            )])];

            ws_stmt_set_tags(stmt, tags.as_ptr(), tags.len() as _);

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            let affected_rows_once = ws_stmt_affected_rows_once(stmt);
            assert_eq!(affected_rows_once, 2);

            let affected_rows = ws_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 2);

            // add batch again, affected_rows_once should be 2, affected_rows should be 4

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[2, 3]),
                TaosMultiBind::from_primitives(vec![false, false], &[4, 5]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);

            assert_eq!(rows, 2);

            let affected_rows_once = ws_stmt_affected_rows_once(stmt);

            assert_eq!(affected_rows_once, 2);

            let affected_rows = ws_stmt_affected_rows(stmt);

            assert_eq!(affected_rows, 4);

            // add batch again, affected_rows_once should be 2, affected_rows should be 6

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[4, 5]),
                TaosMultiBind::from_primitives(vec![false, false], &[6, 7]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];

            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);

            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            let affected_rows_once = ws_stmt_affected_rows_once(stmt);
            assert_eq!(affected_rows_once, 2);

            let affected_rows = ws_stmt_affected_rows(stmt);
            assert_eq!(affected_rows, 6);

            ws_stmt_close(stmt);
            ws_close(taos);
        }
    }

    #[test]
    fn stmt_with_sub_table() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_stmt_with_sub_table\0");
            execute!(b"create database ws_stmt_with_sub_table keep 36500\0");
            execute!(b"use ws_stmt_with_sub_table\0");
            execute!(b"create STABLE s1 (ts timestamp, v int, b binary(100)) tags(jt json)\0");

            let stmt = ws_stmt_init(taos);
            let sql = "insert into ? using s1 tags(?) values(?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_set_sub_tbname(stmt, b"sub_t1\0".as_ptr() as _);

            let tags = [TaosMultiBind::from_string_vec(&[Some(
                r#"{"name":"姓名"}"#.to_string(),
            )])];
            ws_stmt_set_tags(stmt, tags.as_ptr(), tags.len() as _);

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            ws_stmt_close(stmt);
            execute!(b"drop database if exists ws_stmt_with_sub_table\0");
            ws_close(taos);
        }
    }

    #[test]
    fn get_tag_and_col_fields() {
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_stmt_tag_and_col\0");
            execute!(b"create database ws_stmt_tag_and_col keep 36500\0");
            execute!(
                b"create table ws_stmt_tag_and_col.s1 (ts timestamp, v int, b binary(100)) tags(jt json)\0"
            );

            let stmt = ws_stmt_init(taos);
            let sql = "insert into ? using ws_stmt_tag_and_col.s1 tags(?) values(?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            // get tag fields before set tbname
            let mut tag_fields_before = std::ptr::null_mut();
            let mut tag_fields_len_before = 0;
            let code =
                ws_stmt_get_tag_fields(stmt, &mut tag_fields_len_before, &mut tag_fields_before);
            tracing::debug!("tag_fields_before code: {}", code);
            if code != 0 {
                tracing::debug!(
                    "tag_fields_before errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let tag_fields_before_rs =
                    std::slice::from_raw_parts(tag_fields_before, tag_fields_len_before as _);
                tracing::debug!("tag_fields_before: {:?}", tag_fields_before_rs);
                ws_stmt_reclaim_fields(stmt, &mut tag_fields_before, tag_fields_len_before);
                tracing::trace!(
                    "tag_fields_before after reclaim: {:?}",
                    tag_fields_before_rs
                );
            }

            // get col fields before set tbname
            let mut col_fields_before = std::ptr::null_mut();
            let mut col_fields_len_before = 0;
            let code =
                ws_stmt_get_col_fields(stmt, &mut col_fields_len_before, &mut col_fields_before);
            tracing::debug!("col_fields_before code: {}", code);
            if code != 0 {
                tracing::debug!(
                    "col_fields_before errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let col_fields_before_rs =
                    std::slice::from_raw_parts(col_fields_before, col_fields_len_before as _);
                tracing::trace!("col_fields_before: {:?}", col_fields_before_rs);
                ws_stmt_reclaim_fields(stmt, &mut col_fields_before, col_fields_len_before);
                tracing::trace!(
                    "col_fields_before after reclaim: {:?}",
                    col_fields_before_rs
                );
            }

            ws_stmt_set_tbname(stmt, b"ws_stmt_tag_and_col.t1\0".as_ptr() as _);

            // get tag fields after set tbname
            let mut tag_fields_after = std::ptr::null_mut();
            let mut tag_fields_len_after = 0;
            let code =
                ws_stmt_get_tag_fields(stmt, &mut tag_fields_len_after, &mut tag_fields_after);
            tracing::debug!("tag_fields_after code: {}", code);
            if code != 0 {
                tracing::error!(
                    "tag_fields_after errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let tag_fields_after_rs =
                    std::slice::from_raw_parts(tag_fields_after, tag_fields_len_after as _);
                tracing::debug!("tag_fields_after: {:?}", tag_fields_after_rs);
                ws_stmt_reclaim_fields(stmt, &mut tag_fields_after, tag_fields_len_after);
                tracing::trace!("tag_fields_after after reclaim: {:?}", tag_fields_after_rs);
            }

            let tags = [TaosMultiBind::from_string_vec(&[Some(
                r#"{"name":"姓名"}"#.to_string(),
            )])];

            ws_stmt_set_tags(stmt, tags.as_ptr(), tags.len() as _);

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);

            assert_eq!(rows, 2);

            // get stmt tag fields
            let mut tag_fields = std::ptr::null_mut();
            let mut tag_fields_len = 0;
            let code = ws_stmt_get_tag_fields(stmt, &mut tag_fields_len, &mut tag_fields);
            if code != 0 {
                tracing::debug!(
                    "tag_fields errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let tag_fields_rs = std::slice::from_raw_parts(tag_fields, tag_fields_len as _);
                tracing::debug!("tag_fields: {:?}", tag_fields_rs);
                ws_stmt_reclaim_fields(stmt, &mut tag_fields, tag_fields_len);
                tracing::trace!("tag_fields after reclaim: {:?}", tag_fields_rs);
            }

            // get stmt column fields
            let mut col_fields = std::ptr::null_mut();
            let mut col_fields_len = 0;
            let code = ws_stmt_get_col_fields(stmt, &mut col_fields_len, &mut col_fields);
            if code != 0 {
                tracing::debug!(
                    "col_fields errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let col_fields_rs = std::slice::from_raw_parts(col_fields, col_fields_len as _);
                tracing::debug!("col_fields: {:?}", col_fields_rs);
                ws_stmt_reclaim_fields(stmt, &mut col_fields, col_fields_len);
                tracing::debug!("col_fields after reclaim: {:?}", col_fields_rs);
            }

            ws_stmt_close(stmt);
            ws_close(taos);
        }
    }

    #[test]
    fn stmt_num_params_and_get_param() {
        init_env();
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            macro_rules! exec_string {
                ($s:expr) => {
                    let sql = CString::new($s).expect("CString conversion failed");
                    execute!(sql.as_ptr());
                };
            }

            let db = "ws_stmt_num_params";

            exec_string!(format!("drop database if exists {db}"));
            exec_string!(format!("create database {db} keep 36500"));
            exec_string!(format!(
                "create table {db}.s1 (ts timestamp, v int, b binary(100)) tags(jt json)"
            ));

            let stmt = ws_stmt_init(taos);
            let sql = format!("insert into ? using {db}.s1 tags(?) values(?, ?, ?)");
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            let table_name = CString::new(format!("{db}.t1")).unwrap();
            ws_stmt_set_tbname(stmt, table_name.as_ptr() as _);

            let tags = [TaosMultiBind::from_string_vec(&[Some(
                r#"{"name":"姓名"}"#.to_string(),
            )])];
            ws_stmt_set_tags(stmt, tags.as_ptr(), tags.len() as _);

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            ws_stmt_add_batch(stmt);
            let mut rows = 0;
            ws_stmt_execute(stmt, &mut rows);
            assert_eq!(rows, 2);

            let mut num_params = 0;
            let code = ws_stmt_num_params(stmt, &mut num_params);
            if code != 0 {
                tracing::debug!(
                    "num_params errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                tracing::debug!("num_params: {}", num_params);
            }

            for i in 0..num_params {
                let mut r#type = 0;
                let mut bytes = 0;
                let code = ws_stmt_get_param(stmt, i, &mut r#type, &mut bytes);
                if code != 0 {
                    tracing::debug!(
                        "param errstr: {}",
                        CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                    );
                } else {
                    tracing::debug!("param type: {} bytes: {}", r#type, bytes);
                }
            }

            ws_stmt_close(stmt);
            ws_close(taos);
        }
    }

    #[test]
    #[should_panic]
    fn test_stmt_api_false_usage() {
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            if taos.is_null() {
                let code = ws_errno(taos);
                assert!(code != 0);
                let str = ws_errstr(taos);
                dbg!(CStr::from_ptr(str));
            }
            assert!(!taos.is_null());

            macro_rules! execute {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let rs = ws_query(taos, sql);
                    let code = ws_errno(rs);
                    assert!(code == 0, "{:?}", CStr::from_ptr(ws_errstr(rs)));
                    ws_free_result(rs);
                };
            }

            execute!(b"drop database if exists ws_stmt_false_usage\0");
            execute!(b"create database ws_stmt_false_usage keep 36500\0");
            execute!(b"create table ws_stmt_false_usage.s1 (ts timestamp, v int, b binary(100))\0");

            let stmt = ws_stmt_init(taos);
            let sql = "insert into ws_stmt_false_usage.s1 (ts, v, b) values(?, ?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }

            // get tag fields before set tbname
            let mut tag_fields_before = std::ptr::null_mut();
            let mut tag_fields_len_before = 0;
            let code =
                ws_stmt_get_tag_fields(stmt, &mut tag_fields_len_before, &mut tag_fields_before);
            tracing::debug!("tag_fields_before code: {}", code);
            if code != 0 {
                tracing::error!(
                    "tag_fields_before errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let tag_fields_before_rs =
                    std::slice::from_raw_parts(tag_fields_before, tag_fields_len_before as _);
                tracing::debug!("tag_fields_before: {:?}", tag_fields_before_rs);
                ws_stmt_reclaim_fields(stmt, &mut tag_fields_before, tag_fields_len_before);
                tracing::trace!(
                    "tag_fields_before after reclaim: {:?}",
                    tag_fields_before_rs
                );
            }

            // get col fields before set tbname
            let mut col_fields_before = std::ptr::null_mut();
            let mut col_fields_len_before = 0;
            let code =
                ws_stmt_get_col_fields(stmt, &mut col_fields_len_before, &mut col_fields_before);
            tracing::debug!("col_fields_before code: {}", code);
            if code != 0 {
                tracing::error!(
                    "col_fields_before errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let col_fields_before_rs =
                    std::slice::from_raw_parts(col_fields_before, col_fields_len_before as _);
                tracing::trace!("col_fields_before: {:?}", col_fields_before_rs);
                ws_stmt_reclaim_fields(stmt, &mut col_fields_before, col_fields_len_before);
                tracing::trace!(
                    "col_fields_before after reclaim: {:?}",
                    col_fields_before_rs
                );
            }

            ws_stmt_set_tbname(stmt, b"ws_stmt_false_usage.t1\0".as_ptr() as _);

            // get tag fields after set tbname
            let mut tag_fields_after = std::ptr::null_mut();
            let mut tag_fields_len_after = 0;
            let code =
                ws_stmt_get_tag_fields(stmt, &mut tag_fields_len_after, &mut tag_fields_after);
            tracing::debug!("tag_fields_after code: {}", code);
            if code != 0 {
                tracing::error!(
                    "tag_fields_after errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let tag_fields_after_rs =
                    std::slice::from_raw_parts(tag_fields_after, tag_fields_len_after as _);
                tracing::debug!("tag_fields_after: {:?}", tag_fields_after_rs);
                ws_stmt_reclaim_fields(stmt, &mut tag_fields_after, tag_fields_len_after);
                tracing::trace!("tag_fields_after after reclaim: {:?}", tag_fields_after_rs);
            }

            let tags = [TaosMultiBind::from_string_vec(&[Some(
                r#"{"name":"姓名"}"#.to_string(),
            )])];
            ws_stmt_set_tags(stmt, tags.as_ptr(), tags.len() as _);

            let params = [
                TaosMultiBind::from_raw_timestamps(vec![false, false], &[0, 1]),
                TaosMultiBind::from_primitives(vec![false, false], &[2, 3]),
                TaosMultiBind::from_binary_vec(&[None, Some("涛思数据")]),
            ];
            let code = ws_stmt_bind_param_batch(stmt, params.as_ptr(), params.len() as _);
            if code != 0 {
                dbg!(CStr::from_ptr(ws_errstr(stmt)).to_str().unwrap());
            }
            assert_eq!(code, 0);

            let code = ws_stmt_add_batch(stmt);
            assert_eq!(code, 0);

            let mut rows = 0;
            let code = ws_stmt_execute(stmt, &mut rows);
            if code != 0 {
                assert_eq!(ws_stmt_close(stmt), 0);
                assert_eq!(ws_close(taos), 0);
            }
            assert_eq!(rows, 2);

            // get stmt tag fields
            let mut tag_fields = std::ptr::null_mut();
            let mut tag_fields_len = 0;
            let code = ws_stmt_get_tag_fields(stmt, &mut tag_fields_len, &mut tag_fields);
            if code != 0 {
                tracing::debug!(
                    "tag_fields errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let tag_fields_rs = std::slice::from_raw_parts(tag_fields, tag_fields_len as _);
                tracing::debug!("tag_fields: {:?}", tag_fields_rs);
                ws_stmt_reclaim_fields(stmt, &mut tag_fields, tag_fields_len);
                tracing::trace!("tag_fields after reclaim: {:?}", tag_fields_rs);
            }

            // get stmt column fields
            let mut col_fields = std::ptr::null_mut();
            let mut col_fields_len = 0;
            let code = ws_stmt_get_col_fields(stmt, &mut col_fields_len, &mut col_fields);
            if code != 0 {
                tracing::debug!(
                    "col_fields errstr: {}",
                    CStr::from_ptr(ws_stmt_errstr(stmt)).to_str().unwrap()
                );
            } else {
                let col_fields_rs = std::slice::from_raw_parts(col_fields, col_fields_len as _);
                tracing::debug!("col_fields: {:?}", col_fields_rs);
                ws_stmt_reclaim_fields(stmt, &mut col_fields, col_fields_len);
                tracing::trace!("col_fields after reclaim: {:?}", col_fields_rs);
            }

            ws_stmt_close(stmt);
            ws_close(taos);
        }
    }

    #[test]
    fn test_ws_stmt_get_tag_fields() {
        unsafe {
            let taos = ws_connect(b"ws://localhost:6041\0" as *const u8 as _);
            assert!(!taos.is_null());

            macro_rules! exec {
                ($sql:expr) => {
                    let sql = $sql as *const u8 as _;
                    let res = ws_query(taos, sql);
                    let code = ws_errno(res);
                    assert!(code == 0);
                    ws_free_result(res);
                };
            }

            exec!(b"drop database if exists db_202412051738\0");
            exec!(b"create database db_202412051738\0");
            exec!(b"create table db_202412051738.ctb (ts timestamp, c1 int)\0");

            let stmt = ws_stmt_init(taos);
            assert!(!stmt.is_null());

            let sql = "insert into db_202412051738.ctb values(?, ?)";
            let code = ws_stmt_prepare(stmt, sql.as_ptr() as _, sql.len() as _);
            assert!(code == 0);

            let mut fields = std::ptr::null_mut();
            let mut field_num = 0;
            let code = ws_stmt_get_tag_fields(stmt, &mut field_num, &mut fields);
            assert!(code != 0);
            assert!(fields.is_null());

            assert_eq!(ws_stmt_close(stmt), 0);
            assert_eq!(ws_close(taos), 0);
        }
    }
}

#[test]
fn json_test() {
    use serde_json::json;

    let s = json!(vec![Option::<u8>::None]);
    assert_eq!(dbg!(serde_json::to_string(&s).unwrap()), "[null]");
}
