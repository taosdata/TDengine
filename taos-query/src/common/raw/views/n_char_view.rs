use std::{
    cell::{RefCell, UnsafeCell},
    ffi::c_void,
    fmt::Debug, sync::Arc,
};

use super::{Offsets, Version};

use crate::{
    common::{layout::Layout, BorrowedValue, Ty},
    util::{InlineNChar, InlineStr},
};

use bytes::Bytes;

#[derive(Debug)]
pub struct NCharView {
    // version: Version,
    pub(crate) offsets: Offsets,
    pub(crate) data: Bytes,
    /// TDengine v3 raw block use [char] for NChar data type, it's [str] in v2 websocket block.
    pub is_chars: UnsafeCell<bool>,
    pub(crate) version: Version,
    /// Layout should set as NCHAR_DECODED when raw data decoded.
    pub(crate) layout: Arc<RefCell<Layout>>,

}

impl NCharView {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Check if the value at `row` index is NULL or not.
    pub fn is_null(&self, row: usize) -> bool {
        if row < self.len() {
            unsafe { self.is_null_unchecked(row) }
        } else {
            false
        }
    }

    /// Unsafe version for [methods.is_null]
    pub unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        *self.offsets.get_unchecked(row) < 0
    }

    pub unsafe fn nchar_to_utf8(&self) {
        if self.version == Version::V3 && *self.is_chars.get() {
            let mut ptr: *const u8 = std::ptr::null();
            for offset in &self.offsets {
                if *offset >= 0 {
                    if ptr.is_null() {
                        ptr = self.data.as_ptr().offset(*offset as isize);
                        InlineNChar::<u16>::from_ptr(self.data.as_ptr().offset(*offset as isize))
                            .into_inline_str();
                    } else {
                        let next = self.data.as_ptr().offset(*offset as isize);
                        if ptr != next {
                            ptr = next;
                            InlineNChar::<u16>::from_ptr(
                                self.data.as_ptr().offset(*offset as isize),
                            )
                            .into_inline_str();
                        }
                    }
                }
            }
            *self.is_chars.get() = false;
            self.layout.borrow_mut().with_nchar_decoded();
        }
    }

    /// Get UTF-8 string at `row`.
    ///
    /// In this method, InlineNChar will directly converted to InlineStr, which means v3 raw block
    /// will be changed in-place.
    #[inline]
    pub unsafe fn get_inline_str_unchecked(&self, row: usize) -> Option<&InlineStr> {
        let offset = self.offsets.get_unchecked(row);
        if *offset >= 0 {
            self.nchar_to_utf8();
            //     // let me: &mut Self = unsafe { std::mem::transmute(&self) };
            //     let is_chars = &mut *self.is_chars.get();
            //     *is_chars = false;
            //     Some(
            //         InlineNChar::<u16>::from_ptr(self.data.as_ptr().offset(*offset as isize))
            //             .into_inline_str(),
            //     )
            // } else {
            Some(InlineStr::<u16>::from_ptr(
                self.data.as_ptr().offset(*offset as isize),
            ))
            // }
        } else {
            None
        }
    }

    /// Get UTF-8 string at `row`.
    #[inline]
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<&str> {
        self.get_inline_str_unchecked(row).map(|s| s.as_str())
    }

    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map(|s| BorrowedValue::NChar(s.into()))
            .unwrap_or(BorrowedValue::Null)
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        match self.get_unchecked(row) {
            Some(s) => (Ty::NChar, s.len() as _, s.as_ptr() as _),
            None => (Ty::NChar, 0, std::ptr::null()),
        }
    }

    /// Iterator for NCharView.
    #[inline]
    pub fn iter(&self) -> NCharViewIter {
        NCharViewIter { view: self, row: 0 }
    }

    /// Collection to `str`s.
    pub fn to_vec(&self) -> Vec<Option<&str>> {
        self.iter().collect()
    }
}

pub struct NCharViewIter<'a> {
    view: &'a NCharView,
    row: usize,
}

impl<'a> Iterator for NCharViewIter<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row < self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.get_unchecked(row) })
        } else {
            None
        }
    }
}
