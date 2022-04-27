use std::ffi::{CStr};

use taos_sys::*;

use super::Consumer;

// todo: to be removed
pub struct Message<'tmq> {
    tmq: &'tmq Consumer,
    ptr: *mut tmq_message_t,
}

impl<'tmq> Message<'tmq> {
    pub fn new(tmq: &'tmq Consumer, ptr: *mut tmq_message_t) -> Self {
        Self { tmq, ptr }
    }

    pub fn topic_name<'a>(&'a self) -> &'a CStr {
        unsafe { CStr::from_ptr(tmq_get_topic_name(self.ptr)) }
    }

    pub fn vgroup_id(&self) -> i32 {
        unsafe { tmq_get_vgroup_id(self.ptr) }
    }

    pub fn request_offset(&self) -> i64 {
        unsafe { tmq_get_request_offset(self.ptr) }
    }

    pub fn response_offset(&self) -> i64 {
        unsafe { tmq_get_response_offset(self.ptr) }
    }

    pub fn fields(&self) -> &[TAOS_FIELD] {
        let topic = self.topic_name();
        let tmq = self.tmq.as_raw();
        let len = unsafe { tmq_field_count(tmq, topic.as_ptr()) } as usize;
        let ptr = unsafe { tmq_get_fields(tmq, topic.as_ptr()) };
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    #[cfg(test)]
    pub(crate) fn show_raw(&self) {
        unsafe { tmqShowMsg(self.ptr) }
    }
}

impl<'tmq> Drop for Message<'tmq> {
    fn drop(&mut self) {
        unsafe {
            tmq_message_destroy(self.ptr);
        }
    }
}

// pub struct RowsIter<'msg, 'tmq> {
//     msg: &'msg Message<'tmq>,
//     fields: &'msg [TAOS_FIELD],
// }

// impl<'msg, 'tmq> Iterator for RowsIter<'msg, 'tmq> {
//     type Item = RawRow<'msg>;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

// pub struct RawRow<'a> {
//     ptr: &'a [*const c_void],
//     fields: &'a [TAOS_FIELD],
// }

// impl<'a> RawRow<'a> {
//     const fn num_of_fields(&self) -> usize {
//         self.fields.len()
//     }

//     pub fn to_values(&self) -> Vec<BorrowedValue<'a>> {
//         (0..self.num_of_fields())
//             .map(|col| unsafe { self.get_unchecked(col) })
//             .collect()
//     }
//     pub fn into_values(self) -> Vec<Value> {
//         (0..self.num_of_fields())
//             .map(|col| unsafe { self.get_unchecked(col) })
//             .map(|v| v.to_value())
//             .collect()
//     }

//     pub unsafe fn get_unchecked(&self, col: usize) -> BorrowedValue<'a> {
//         let inner = { self.ptr.get_unchecked(col) };
//         let field = self.fields.get_unchecked(col);
//         let is_null = false;
//         if is_null {
//             return BorrowedValue::Null;
//         }

//         macro_rules! parse_cell {
//             ($f:ident, $t:ty) => {
//                 paste::paste! {
//                     BorrowedValue::$f({
//                         (*inner as *const $t).read()
//                     })
//                 }
//             };
//         }

//         match field.type_() {
//             TaosDataType::Null => BorrowedValue::Null,
//             TaosDataType::Bool => parse_cell!(Bool, bool),
//             TaosDataType::TinyInt => parse_cell!(TinyInt, i8),
//             TaosDataType::SmallInt => parse_cell!(SmallInt, i16),
//             TaosDataType::Int => parse_cell!(Int, i32),
//             TaosDataType::BigInt => parse_cell!(BigInt, i64),
//             TaosDataType::UTinyInt => parse_cell!(UTinyInt, u8),
//             TaosDataType::USmallInt => parse_cell!(USmallInt, u16),
//             TaosDataType::UInt => parse_cell!(UInt, u32),
//             TaosDataType::UBigInt => parse_cell!(UBigInt, u64),
//             TaosDataType::Float => parse_cell!(Float, f32),
//             TaosDataType::Double => parse_cell!(Double, f64),
//             TaosDataType::Timestamp => {
//                 let raw = (*inner as *const i64).read();
//                 BorrowedValue::Timestamp(TimestampValue::new(raw, Precision::Millisecond))
//             }
//             TSDB_DATA_TYPE_BINARY => {
//                 todo!()
//                 // let length = self.get_length_unchecked(col);
//                 // let ptr = *inner as *const u8;
//                 // let len = ptr.cast::<i16>().read();
//                 // let start = ptr.offset(2);

//                 // BorrowedValue::Binary(slice::from_raw_parts(start, len as _))
//             }
//             TaosDataType::NChar => {
//                 todo!()
//                 // let length = self.get_length_unchecked(col);

//                 // let ptr = (*inner as *const u8).add(row * length as usize);
//                 // let len = ptr.cast::<i16>().read();
//                 // let start = ptr.offset(2);

//                 // BorrowedValue::NChar(std::str::from_utf8_unchecked(slice::from_raw_parts(
//                 //     start as _, len as _,
//                 // )))
//             }
//             TaosDataType::Json => {
//                 todo!()
//                 // let length = self.get_length_unchecked(col);
//                 // let ptr = (*inner as *const u8).add(row * length as usize);
//                 // let len = ptr.cast::<i16>().read();
//                 // let start = ptr.offset(2);

//                 // BorrowedValue::Json(slice::from_raw_parts(start, len as _))
//             }
//             _ => unreachable!("unknown data type"),
//         }
//     }

//     // pub deserialize()
// }
