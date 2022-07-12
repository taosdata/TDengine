use std::ffi::c_void;

use crate::common::{BorrowedValue, Ty};

use super::{NullBits, NullsIter};

use bytes::Bytes;

type Target = u64;

#[derive(Debug)]
pub struct UBigIntView {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
}

impl UBigIntView {
    /// Rows
    pub fn len(&self) -> usize {
        self.data.len() / std::mem::size_of::<Target>()
    }

    /// Raw slice of target type.
    unsafe fn as_raw_slice(&self) -> &[Target] {
        std::slice::from_raw_parts(self.data.as_ptr() as *const Target, self.len())
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> NullsIter {
        NullsIter {
            nulls: &self.nulls,
            row: 0,
            len: self.len(),
        }
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
    pub fn get(&self, row: usize) -> Option<Target> {
        if row < self.len() {
            unsafe { self.get_unchecked(row) }
        } else {
            None
        }
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<Target> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(*self.as_raw_slice().get_unchecked(row))
        }
    }

    pub unsafe fn get_ref_unchecked(&self, row: usize) -> Option<*const Target> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(self.as_raw_slice().get_unchecked(row))
        }
    }

    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue {
        self.get_unchecked(row)
            .map(|v| BorrowedValue::UBigInt(v))
            .unwrap_or(BorrowedValue::Null)
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        if self.nulls.is_null_unchecked(row) {
            (
                Ty::Null,
                std::mem::size_of::<Target>() as _,
                std::ptr::null(),
            )
        } else {
            (
                Ty::UBigInt,
                std::mem::size_of::<Target>() as _,
                self.as_raw_slice().get_unchecked(row) as *const Target as _,
            )
        }
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> UBigIntViewIter {
        UBigIntViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<Target>> {
        self.iter().collect()
    }
}

pub struct UBigIntViewIter<'a> {
    view: &'a UBigIntView,
    row: usize,
}

impl<'a> Iterator for UBigIntViewIter<'a> {
    type Item = Option<Target>;

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
