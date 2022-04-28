// // use block::Row;
// use futures::Stream;
// use serde::de::DeserializeOwned;

// use taos_sys::{ffi::*, DroppableRawRes as RawRes, *};

// use crate::*;

// #[derive(Debug)]
// pub enum TaosResult<'a> {
//     WithFields(RawRes<'a>),
//     WithoutFields(RawRes<'a>),
// }

// unsafe impl<'a> Send for TaosResult<'a> {}
// unsafe impl<'a> Sync for TaosResult<'a> {}

// impl<'a> TaosResult<'a> {
//     pub(crate) const fn as_raw(&self) -> &RawRes {
//         match self {
//             TaosResult::WithFields(res) => res,
//             TaosResult::WithoutFields(res) => res,
//         }
//     }
//     pub(crate) fn from_raw(raw: RawRes<'a>) -> Self {
//         match raw.num_fields() {
//             0 => TaosResult::WithoutFields(raw),
//             _ => TaosResult::WithFields(raw),
//         }
//     }

//     pub(crate) fn new(result: *mut TAOS_RES, code: i32) -> Result<Self> {
//         if code < 0 {
//             let code: Code = (code & 0xffff).into();
//             RawRes::from_ptr_with_code(result, code).map(Self::from_raw)
//         } else {
//             RawRes::from_ptr_with_code(result, Code::Success).map(Self::from_raw)
//         }
//     }

//     pub fn num_of_fields(&self) -> usize {
//         self.as_raw().fields().len()
//     }

//     pub fn precision(&self) -> Precision {
//         self.as_raw().precision()
//     }

//     pub fn affected_rows(&self) -> usize {
//         self.as_raw().affected_rows() as _
//     }

//     pub fn block_stream(&self) -> block::BlockStream {
//         block::BlockStream::from_raw(self.as_raw().raw(), Default::default())
//     }

//     pub fn deserialize_stream<'b, T: 'a + 'b>(
//         &self,
//     ) -> impl Stream<Item = std::result::Result<T, serde::de::value::Error>> + '_
//     where
//         T: DeserializeOwned,
//     {
//         use futures::StreamExt;
//         use taos_query::BlockExt;
//         self.block_stream()
//             .flat_map(|block| futures::stream::iter(block.deserialize_into_vec()))
//     }
// }
