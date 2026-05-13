use std::ffi::c_void;
use std::fmt::Debug;

use bytes::Bytes;
use itertools::Itertools;

use super::{IsColumnView, Offsets};
use crate::common::{BorrowedValue, Ty};
use crate::prelude::InlinableWrite;
use crate::util::InlineStr;

#[derive(Debug, Clone)]
pub struct VarCharView {
    pub(crate) offsets: Offsets,
    pub(crate) data: Bytes,
}

impl IsColumnView for VarCharView {
    fn ty(&self) -> Ty {
        Ty::VarChar
    }

    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
        Self::from_iter::<String, _, _, _>(
            iter.map(|v| v.to_str().map(|v| v.into_owned()))
                .collect_vec(),
        )
    }
}

impl VarCharView {
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn as_raw_ptr(&self) -> *const u8 {
        self.data.as_ptr() as _
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> VarCharNullsIter<'_> {
        VarCharNullsIter { view: self, row: 0 }
    }

    /// Build a nulls vector.
    pub fn to_nulls_vec(&self) -> Vec<bool> {
        self.is_null_iter().collect()
    }

    /// Check if the value at `row` index is NULL or not.
    ///
    /// Returns null when `row` index out of bound.
    pub fn is_null(&self, row: usize) -> bool {
        if row < self.len() {
            unsafe { self.is_null_unchecked(row) }
        } else {
            false
        }
    }

    /// Unsafe version for [is_null](#method.is_null)
    pub(crate) unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        self.offsets.get_unchecked(row) < 0
    }

    pub(crate) unsafe fn get_unchecked(&self, row: usize) -> Option<&InlineStr> {
        let offset = self.offsets.get_unchecked(row);
        if offset >= 0 {
            Some(InlineStr::<u16>::from_ptr(
                self.data.as_ptr().offset(offset as isize),
            ))
        } else {
            None
        }
    }

    pub(crate) unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue<'_> {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::VarChar), |s| {
                BorrowedValue::VarChar(s.as_str())
            })
    }

    pub(crate) unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        match self.get_unchecked(row) {
            Some(s) => (Ty::VarChar, s.len() as _, s.as_ptr() as _),
            None => (Ty::VarChar, 0, std::ptr::null()),
        }
    }

    #[inline]
    pub unsafe fn get_length_unchecked(&self, row: usize) -> Option<usize> {
        let offset = self.offsets.get_unchecked(row);
        if offset >= 0 {
            Some(InlineStr::<u16>::from_ptr(self.data.as_ptr().offset(offset as isize)).len())
        } else {
            None
        }
    }

    #[inline]
    pub fn lengths(&self) -> Vec<Option<usize>> {
        (0..self.len())
            .map(|i| unsafe { self.get_length_unchecked(i) })
            .collect()
    }

    #[inline]
    pub fn max_length(&self) -> usize {
        (0..self.len())
            .filter_map(|i| unsafe { self.get_length_unchecked(i) })
            .max()
            .unwrap_or(0)
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
        if let Some(range) = range {
            let range = range.0 as usize..range.1.map_or(self.data.len(), |v| v as usize);
            let data = self.data.slice(range);
            Some(Self { offsets, data })
        } else {
            let data = self.data.slice(0..0);
            Some(Self { offsets, data })
        }
    }

    pub fn iter(&self) -> VarCharIter<'_> {
        VarCharIter { view: self, row: 0 }
    }

    pub fn to_vec(&self) -> Vec<Option<String>> {
        (0..self.len())
            .map(|row| unsafe { self.get_unchecked(row) }.map(|s| s.to_string()))
            .collect()
    }

    pub fn iter_as_bytes(&self) -> impl Iterator<Item = Option<&[u8]>> {
        (0..self.len()).map(|row| unsafe { self.get_unchecked(row) }.map(|s| s.as_bytes()))
    }

    pub fn to_bytes_vec(&self) -> Vec<Option<&[u8]>> {
        self.iter_as_bytes().collect_vec()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let mut offsets = Vec::with_capacity(self.len());
        let mut bytes: Vec<u8> = Vec::new();
        for v in self.iter() {
            if let Some(v) = v {
                offsets.push(bytes.len() as i32);
                bytes.write_inlined_str::<2>(v.as_str()).unwrap();
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
        let iter = iter.into_iter();
        let mut offsets = Vec::with_capacity(iter.len());
        let mut data = Vec::new();

        for i in iter.map(|v| v.into()) {
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
        VarCharView {
            offsets: Offsets(offsets_bytes.into()),
            data: data.into(),
        }
    }

    pub fn concat(&self, rhs: &Self) -> Self {
        Self::from_iter::<&InlineStr, _, _, _>(self.iter().chain(rhs.iter()).collect_vec())
    }
}

pub struct VarCharIter<'a> {
    view: &'a VarCharView,
    row: usize,
}

impl<'a> Iterator for VarCharIter<'a> {
    type Item = Option<&'a InlineStr>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row < self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.get_unchecked(row) })
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.view.len() - self.row;
        (len, Some(len))
    }
}

impl ExactSizeIterator for VarCharIter<'_> {}

pub struct VarCharNullsIter<'a> {
    view: &'a VarCharView,
    row: usize,
}

impl Iterator for VarCharNullsIter<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row < self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.is_null_unchecked(row) })
        } else {
            None
        }
    }
}

impl ExactSizeIterator for VarCharNullsIter<'_> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice() {
        let data = [None, Some(""), Some("abc"), Some("中文"), None, None, Some("a loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooog string")];
        let view = VarCharView::from_iter::<&str, _, _, _>(data);
        let slice = view.slice(0..0);
        assert!(slice.is_none());
        let slice = view.slice(100..1000);
        assert!(slice.is_none());

        for start in 0..data.len() {
            let end = start + 1;
            for end in end..data.len() {
                let slice = view.slice(start..end).unwrap();
                assert_eq!(
                    slice.to_vec().as_slice(),
                    &data[start..end]
                        .iter()
                        .map(|s| s.map(ToString::to_string))
                        .collect_vec()
                );
            }
        }
    }
}
