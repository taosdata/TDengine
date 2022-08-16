use std::{ffi::c_void, io::Write};

use crate::common::{BorrowedValue, Ty};

use super::{NullBits, NullsIter, NullsMut};

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct BoolView {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
}

impl BoolView {
    /// Rows
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Raw slice of `bool` type.
    pub fn as_raw_slice(&self) -> &[bool] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const bool, self.len()) }
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> NullsIter {
        NullsIter {
            nulls: &self.nulls,
            row: 0,
            len: self.len(),
        }
    }

    /// Build a nulls vector.
    pub fn to_nulls_vec(&self) -> Vec<bool> {
        self.is_null_iter().collect()
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
        self.nulls.is_null_unchecked(row)
    }

    /// Get nullable value at `row` index.
    pub fn get(&self, row: usize) -> Option<bool> {
        if row < self.len() {
            unsafe { self.get_unchecked(row) }
        } else {
            None
        }
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<bool> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(*self.as_raw_slice().get_unchecked(row))
        }
    }

    pub unsafe fn get_ref_unchecked(&self, row: usize) -> Option<*const bool> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(self.as_raw_slice().get_unchecked(row))
        }
    }

    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map(|v| BorrowedValue::Bool(v))
            .unwrap_or(BorrowedValue::Null)
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        if self.nulls.is_null_unchecked(row) {
            (Ty::Bool, std::mem::size_of::<bool>() as _, std::ptr::null())
        } else {
            (
                Ty::Bool,
                std::mem::size_of::<bool>() as _,
                self.as_raw_slice().get_unchecked(row) as *const bool as _,
            )
        }
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> BoolViewIter {
        BoolViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<bool>> {
        self.iter().collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        let nulls = self.nulls.0.as_ref();
        wtr.write_all(nulls)?;
        wtr.write_all(&self.data)?;
        Ok(nulls.len() + self.data.len())
    }
}

pub struct BoolViewIter<'a> {
    view: &'a BoolView,
    row: usize,
}

impl<'a> Iterator for BoolViewIter<'a> {
    type Item = Option<bool>;

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

impl<'a> ExactSizeIterator for BoolViewIter<'a> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

impl<A: Into<Option<bool>>> FromIterator<A> for BoolView {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let (nulls, mut values): (Vec<bool>, Vec<_>) = iter
            .into_iter()
            .map(|v| match v.into() {
                Some(v) => (false, v),
                None => (true, false),
            })
            .unzip();
        Self {
            nulls: NullBits::from_iter(nulls),
            data: Bytes::from({
                let (ptr, len, cap) = (values.as_mut_ptr(), values.len(), values.capacity());
                std::mem::forget(values);
                unsafe { Vec::from_raw_parts(ptr as *mut u8, len, cap) }
            }),
        }
    }
}
