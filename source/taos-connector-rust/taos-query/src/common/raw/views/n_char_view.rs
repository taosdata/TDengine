use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use itertools::Itertools;

use super::{IsColumnView, Offsets, Version};
use crate::common::layout::Layout;
use crate::common::{BorrowedValue, Ty};
use crate::prelude::InlinableWrite;
use crate::util::{InlineNChar, InlineStr};

#[derive(Debug)]
pub struct NCharView {
    pub(crate) offsets: Offsets,
    pub(crate) data: Bytes,
    /// TDengine v3 raw block use [char] for NChar data type, it's [str] in v2 websocket block.
    pub(crate) is_chars: UnsafeCell<bool>,
    pub(crate) version: Version,
    /// Layout should set as NCHAR_DECODED when raw data decoded.
    pub(crate) layout: Rc<AtomicU32>, // Rc<RefCell<Layout>>
}

impl Clone for NCharView {
    fn clone(&self) -> Self {
        unsafe {
            self.nchar_to_utf8();
        }
        Self {
            offsets: self.offsets.clone(),
            data: self.data.clone(),
            is_chars: UnsafeCell::new(false),
            version: self.version,
            layout: self.layout.clone(),
        }
    }
}

impl IsColumnView for NCharView {
    fn ty(&self) -> Ty {
        Ty::NChar
    }

    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
        Self::from_iter::<String, _, _, _>(
            iter.map(|v| v.to_str().map(|v| v.into_owned()))
                .collect_vec(),
        )
    }
}

impl NCharView {
    fn layout(&self) -> &Layout {
        unsafe { &*(self.layout.as_ptr() as *const Layout) }
    }
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn as_raw_ptr(&self) -> *const u8 {
        self.data.as_ptr() as _
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
        self.offsets.get_unchecked(row) < 0
    }

    #[inline]
    pub unsafe fn nchar_to_utf8(&self) {
        if self.version == Version::V3 && *self.is_chars.get() {
            let mut ptr: *const u8 = std::ptr::null();
            for offset in self.offsets.iter() {
                if offset >= 0 {
                    if ptr.is_null() {
                        ptr = self.data.as_ptr().offset(offset as isize);
                        InlineNChar::<u16>::from_ptr(self.data.as_ptr().offset(offset as isize))
                            .into_inline_str();
                    } else {
                        let next = self.data.as_ptr().offset(offset as isize);
                        if ptr != next {
                            ptr = next;
                            InlineNChar::<u16>::from_ptr(
                                self.data.as_ptr().offset(offset as isize),
                            )
                            .into_inline_str();
                        }
                    }
                }
            }
            *self.is_chars.get() = false;
            let mut layout = *self.layout();
            layout.with_nchar_decoded();
            self.layout.store(layout.as_inner(), Ordering::SeqCst);
        }
    }

    /// Get UTF-8 string at `row`.
    ///
    /// In this method, InlineNChar will directly converted to InlineStr, which means v3 raw block
    /// will be changed in-place.
    #[inline]
    pub unsafe fn get_inline_str_unchecked(&self, row: usize) -> Option<&InlineStr> {
        let offset = self.offsets.get_unchecked(row);
        if offset >= 0 {
            self.nchar_to_utf8();
            Some(InlineStr::<u16>::from_ptr(
                self.data.as_ptr().offset(offset as isize),
            ))
        } else {
            None
        }
    }

    #[inline]
    pub unsafe fn get_length_unchecked(&self, row: usize) -> Option<usize> {
        let offset = self.offsets.get_unchecked(row);
        if offset >= 0 {
            self.nchar_to_utf8();
            Some(InlineStr::<u16>::from_ptr(self.data.as_ptr().offset(offset as isize)).len())
        } else {
            None
        }
    }

    #[inline]
    pub fn lengths(&self) -> Vec<Option<usize>> {
        (0..self.len())
            .map(|i| unsafe { self.get_length_unchecked(i) })
            .collect_vec()
    }

    #[inline]
    pub fn max_length(&self) -> usize {
        (0..self.len())
            .filter_map(|i| unsafe { self.get_length_unchecked(i) })
            .max()
            .unwrap_or(0)
    }

    /// Get UTF-8 string at `row`.
    #[inline]
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<&str> {
        self.get_inline_str_unchecked(row).map(|s| s.as_str())
    }

    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue<'_> {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::NChar), |s| {
                BorrowedValue::NChar(s.into())
            })
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        self.nchar_to_utf8();
        match self.get_unchecked(row) {
            Some(s) => (Ty::NChar, s.len() as _, s.as_ptr() as _),
            None => (Ty::NChar, 0, std::ptr::null()),
        }
    }

    pub fn slice(&self, mut range: std::ops::Range<usize>) -> Option<Self> {
        if range.start >= self.len() {
            return None;
        }
        if range.end > self.len() {
            range.end = self.len();
        }
        if range.is_empty() {
            return None;
        }
        let (offsets, range) = unsafe { self.offsets.slice_unchecked(range) };
        let range = if let Some(range) = range {
            range.0 as usize..range.1.map_or(self.data.len(), |v| v as usize)
        } else {
            0..0
        };
        let data = self.data.slice(range);
        Some(Self {
            offsets,
            data,
            is_chars: UnsafeCell::new(false),
            version: self.version,
            layout: self.layout.clone(),
        })
    }

    /// Iterator for NCharView.
    #[inline]
    pub fn iter(&self) -> NCharViewIter<'_> {
        NCharViewIter { view: self, row: 0 }
    }

    /// Collection to `str`s.
    pub fn to_vec(&self) -> Vec<Option<&str>> {
        self.iter().collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let mut offsets = Vec::new();
        let mut bytes: Vec<u8> = Vec::new();
        for v in self.iter() {
            if let Some(v) = v {
                let chars = v.chars().collect_vec();
                offsets.push(bytes.len() as i32);
                let chars = unsafe {
                    std::slice::from_raw_parts(
                        chars.as_ptr() as *const u8,
                        chars.len() * std::mem::size_of::<char>(),
                    )
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
            wtr.write_all(offsets_bytes)?;
            wtr.write_all(&bytes)?;
            Ok(offsets_bytes.len() + bytes.len())
        }
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
                data.write_inlined_str::<2>(s).unwrap();
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
            layout: Rc::new(AtomicU32::new({
                let mut layout = Layout::default();
                layout.with_nchar_decoded();
                layout.as_inner()
            })),
        }
    }

    pub fn concat(&self, rhs: &Self) -> Self {
        Self::from_iter::<&str, _, _, _>(self.iter().chain(rhs.iter()).collect_vec())
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

impl ExactSizeIterator for NCharViewIter<'_> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

#[test]
fn test_slice() {
    let data = [None, Some(""), Some("abc"), Some("中文"), None, None, Some("a loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooog string")];
    let view = NCharView::from_iter::<&str, _, _, _>(data);
    let slice = view.slice(0..0);
    assert!(slice.is_none());
    let slice = view.slice(100..1000);
    assert!(slice.is_none());

    for start in 0..data.len() {
        let end = start + 1;
        for end in end..data.len() {
            let slice = view.slice(start..end).unwrap();
            assert_eq!(slice.to_vec().as_slice(), &data[start..end]);
        }
    }
}
