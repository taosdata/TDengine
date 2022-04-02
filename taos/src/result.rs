use block::Row;
use futures::Stream;
use serde::de::DeserializeOwned;
use std::ffi::CStr;

use taos_sys::*;

use crate::*;

#[derive(Debug)]
pub enum TaosResult<'a> {
    WithFields(*mut TAOS_RES, &'a [TAOS_FIELD]),
    WithoutFields(*mut TAOS_RES),
}

unsafe impl<'a> Send for TaosResult<'a> {}
unsafe impl<'a> Sync for TaosResult<'a> {}

impl<'a> Drop for TaosResult<'a> {
    fn drop(&mut self) {
        unsafe {
            if !self.as_raw().is_null() {
                taos_free_result(self.as_raw());
            }
        }
    }
}

impl<'a> TaosResult<'a> {
    pub(crate) const fn as_raw(&self) -> *mut TAOS_RES {
        match self {
            TaosResult::WithFields(res, _) => *res,
            TaosResult::WithoutFields(res) => *res,
        }
    }

    pub(crate) fn try_from_ptr(result: *mut TAOS_RES) -> Result<Self> {
        Self::new(result, unsafe { taos_errno(result) })
    }

    pub(crate) fn new(result: *mut TAOS_RES, code: i32) -> Result<Self> {
        let code: Code = (code & 0xffff).into();
        if code.success() {
            let num_fields = unsafe { taos_num_fields(result) };
            if num_fields == 0 {
                Ok(TaosResult::WithoutFields(result))
            } else {
                let fields = unsafe {
                    std::slice::from_raw_parts(taos_fetch_fields(result), num_fields as _)
                };
                Ok(TaosResult::WithFields(result, fields))
            }
        } else {
            let err_str = unsafe { CStr::from_ptr(taos_errstr(result)) };
            let err_str = err_str.to_string_lossy();
            if err_str == "success" {
                return Self::new(result, 0);
            }
            Err(Error::new(code, err_str))
        }
    }

    pub(crate) unsafe fn get_fields_unchecked(&self) -> &'a [TAOS_FIELD] {
        match self {
            TaosResult::WithFields(_, fields) => fields,
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }

    pub(crate) fn get_field_names_to_string_vec(&self) -> Vec<String> {
        match self {
            TaosResult::WithFields(_, fields) => fields
                .iter()
                .map(|f| f.name().to_string_lossy().into_owned())
                .collect(),
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }
    pub(crate) unsafe fn get_field_unchecked(&self, index: usize) -> &TAOS_FIELD {
        match self {
            TaosResult::WithFields(_, fields) => fields.get_unchecked(index),
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }

    pub const fn num_of_fields(&self) -> usize {
        match self {
            TaosResult::WithFields(_, fields) => fields.len(),
            _ => 0,
        }
    }

    pub fn precision(&self) -> TimestampPrecision {
        unsafe { taos_result_precision(self.as_raw()) }.into()
    }

    pub fn affected_rows(&self) -> usize {
        unsafe { taos_affected_rows(self.as_raw()) as _ }
    }

    pub fn fetch_block_stream(&self) -> block::BlockStream {
        block::BlockStream::new(self)
    }

    pub fn rows_stream(&self) -> impl Stream<Item = Row> {
        use futures::StreamExt;
        block::BlockStream::new(self)
            .flat_map(|block| futures::stream::iter(block.into_iter_rows()))
    }
    pub fn rows_de_stream<T>(&self) -> impl Stream<Item = Result<T>> + '_
    where
        T: DeserializeOwned,
    {
        use futures::StreamExt;

        self.rows_stream()
            .map(|row| T::deserialize(&mut row.deserializer()))
    }
}
