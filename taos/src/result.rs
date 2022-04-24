use block::Row;
use futures::Stream;
use serde::de::DeserializeOwned;
use std::{borrow::Cow, ffi::CStr};
use taos_query::common::Field;

use taos_sys::{ffi::*, *};

use crate::*;

#[derive(Debug)]
pub enum TaosResult<'a> {
    WithFields(RawRes<'a>, Cow<'a, [Field]>),
    WithoutFields(RawRes<'a>),
}

unsafe impl<'a> Send for TaosResult<'a> {}
unsafe impl<'a> Sync for TaosResult<'a> {}

impl<'a> TaosResult<'a> {
    pub(crate) const fn as_raw(&self) -> &RawRes {
        match self {
            TaosResult::WithFields(res, _) => res,
            TaosResult::WithoutFields(res) => res,
        }
    }
    pub(crate) fn from_raw(raw: RawRes<'a>) -> Self {
        match raw.fetch_fields() {
            Some(fields) => TaosResult::WithFields(raw, fields),
            None => TaosResult::WithoutFields(raw),
        }
    }

    pub(crate) fn try_from_ptr(result: *mut TAOS_RES) -> Result<Self> {
        Self::new(result, unsafe { taos_errno(result) })
    }

    pub(crate) fn new(result: *mut TAOS_RES, code: i32) -> Result<Self> {
        let code: Code = (code & 0xffff).into();
        RawRes::from_ptr_with_code(result, code).map(Self::from_raw)
        // if code.success() {
        //     let res = RawRes::from_ptr(result);
        //     match res.fetch_fields() {
        //         Some(fields) => Ok(TaosResult::WithFields(res, fields)),
        //         None => Ok(TaosResult::WithoutFields(res))
        //     }
        // } else {
        //     let err_str = unsafe { CStr::from_ptr(taos_errstr(result)) };
        //     let err_str = err_str.to_string_lossy();
        //     if err_str == "success" {
        //         return Self::new(result, 0);
        //     }
        //     Err(Error::new(code, err_str))
        // }
    }

    pub(crate) unsafe fn get_fields_unchecked(&self) -> &[Field] {
        match self {
            TaosResult::WithFields(_, fields) => &fields,
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }

    pub(crate) fn get_field_names_to_string_vec(&self) -> Vec<String> {
        match self {
            TaosResult::WithFields(_, fields) => {
                fields.iter().map(|f| f.name().into_owned()).collect()
            }
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }
    pub(crate) unsafe fn get_field_unchecked(&self, index: usize) -> &Field {
        match self {
            TaosResult::WithFields(_, fields) => &fields[index],
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }

    pub fn num_of_fields(&self) -> usize {
        match self {
            TaosResult::WithFields(_, fields) => fields.len(),
            _ => 0,
        }
    }

    pub fn precision(&self) -> Precision {
        self.as_raw().precision()
    }

    pub fn affected_rows(&self) -> usize {
        self.as_raw().affected_rows() as _
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
