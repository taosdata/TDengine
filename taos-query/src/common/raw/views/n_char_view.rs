use std::{
    cell::{RefCell, UnsafeCell},
    ffi::c_void,
    fmt::Debug,
    sync::Arc,
};

use super::{Offsets, Version};

use crate::{
    common::{layout::Layout, BorrowedValue, Ty},
    prelude::InlinableWrite,
    util::{InlineNChar, InlineStr},
};

use bytes::Bytes;
use itertools::Itertools;

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

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        if self.layout.borrow().nchar_is_decoded() {
            let mut offsets = Vec::new();
            let mut bytes: Vec<u8> = Vec::new();
            for v in self.iter() {
                if let Some(v) = v {
                    let chars = v.chars().collect_vec();
                    offsets.push(bytes.len() as i32);
                    let chars = unsafe {
                        std::slice::from_raw_parts(chars.as_ptr() as *mut u8, chars.len() * std::mem::size_of::<char>())
                    };
                    bytes.write_inlined_bytes::<2>(chars).unwrap();
                } else {
                    offsets.push(-1);
                }
            }
            unsafe {
                let offsets_bytes = std::slice::from_raw_parts(
                    offsets.as_ptr() as *const u8,
                    offsets.len() * std::mem::size_of::<i32>(),
                );
                let data_bytes = std::slice::from_raw_parts(
                    bytes.as_ptr() as *const u8,
                    bytes.len() * std::mem::size_of::<char>(),
                );
                wtr.write_all(offsets_bytes)?;
                wtr.write_all(data_bytes)?;
                return Ok(offsets_bytes.len() + data_bytes.len());
            }
        }
        let offsets = self.offsets.as_bytes();
        wtr.write_all(offsets)?;
        wtr.write_all(&self.data)?;
        Ok(offsets.len() + self.data.len())
    }

    pub fn from_iter<
        S: AsRef<str>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        let mut offsets = Vec::new();
        let mut data = Vec::new();

        for i in iter.into_iter().map(|v| v.into()) {
            if let Some(s) = i {
                let s: &str = s.as_ref();
                offsets.push(data.len() as i32);
                data.write_inlined_str::<2>(&s).unwrap();
            } else {
                offsets.push(-1);
            }
        }
        let offsets_bytes = unsafe {
            Vec::from_raw_parts(
                offsets.as_mut_ptr() as *mut u8,
                offsets.len() * 4,
                offsets.capacity() * 4,
            )
        };
        std::mem::forget(offsets);
        NCharView {
            offsets: Offsets(offsets_bytes.into()),
            data: data.into(),
            is_chars: UnsafeCell::new(false),
            version: Version::V2,
            layout: Arc::new(RefCell::new({
                let mut layout = Layout::default();
                layout.with_nchar_decoded();
                layout
            })),
        }
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

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.row < self.view.len() {
            let len = self.view.len() - self.row;
            (len, Some(len))
        } else {
            (0, Some(0))
        }
    }
}

impl<'a> ExactSizeIterator for NCharViewIter<'a> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}
