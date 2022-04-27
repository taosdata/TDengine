// use block::Row;
use futures::Stream;
use serde::de::DeserializeOwned;
use std::{borrow::Cow, ffi::CStr};
use taos_query::{common::Field, IntoRowsIter, RowInBlock};

use taos_sys::{ffi::*, DroppableRawRes as RawRes, *};

use crate::{impls::SyncBlock, *};

#[derive(Debug)]
pub enum TaosResult<'a> {
    WithFields(RawRes<'a>),
    WithoutFields(RawRes<'a>),
}

unsafe impl<'a> Send for TaosResult<'a> {}
unsafe impl<'a> Sync for TaosResult<'a> {}

impl<'a> TaosResult<'a> {
    pub(crate) const fn as_raw(&self) -> &RawRes {
        match self {
            TaosResult::WithFields(res) => res,
            TaosResult::WithoutFields(res) => res,
        }
    }
    pub(crate) fn from_raw(raw: RawRes<'a>) -> Self {
        match raw.num_fields() {
            0 => TaosResult::WithoutFields(raw),
            _ => TaosResult::WithFields(raw),
        }
    }

    pub(crate) fn try_from_ptr(result: *mut TAOS_RES) -> Result<Self> {
        Self::new(result, unsafe { taos_errno(result) })
    }

    pub(crate) fn new(result: *mut TAOS_RES, code: i32) -> Result<Self> {
        if code < 0 {
            let code: Code = (code & 0xffff).into();
            RawRes::from_ptr_with_code(result, code).map(Self::from_raw)
        } else {
            RawRes::from_ptr_with_code(result, Code::Success).map(Self::from_raw)
        }
    }

    pub(crate) fn get_field_names_to_string_vec(&self) -> Vec<String> {
        match self {
            TaosResult::WithFields(raw) => {
                raw.fields().iter().map(|f| f.name().to_string()).collect()
            }
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }
    pub(crate) unsafe fn get_field_unchecked(&self, index: usize) -> &Field {
        match self {
            TaosResult::WithFields(raw) => &raw.fields()[index],
            _ => unreachable!("do not fetch fields in a result without fields"),
        }
    }

    pub fn num_of_fields(&self) -> usize {
        match self {
            TaosResult::WithFields(raw) => raw.fields().len(),
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

    // pub fn rows_stream(&self) -> impl Stream<Item = RowInBlock<SyncBlock<'_>>> {
    //     use futures::StreamExt;

    //     use taos_query::{BlockExt, ResultSet};
    //     block::BlockStream::new(self)
    //         .flat_map(|block| futures::stream::iter(block.deserialize_into_vec()))
    // }
    pub fn rows_de_stream<'b, T: 'a + 'b>(
        &self,
    ) -> impl Stream<Item = std::result::Result<T, serde::de::value::Error>> + '_
    where
        T: DeserializeOwned,
    {
        use futures::StreamExt;
        use taos_query::BlockExt;
        block::BlockStream::new(self)
            .flat_map(|block| futures::stream::iter(block.deserialize_into_vec()))
    }
}
