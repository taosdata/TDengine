use std::ffi::c_void;

use crate::common::{BorrowedValue, Ty};

use super::{NullBits, NullsIter};

use bytes::Bytes;

type Item = u64;
const ITEM_SIZE: usize = std::mem::size_of::<Item>();

#[derive(Debug, Clone)]
pub struct UBigIntView {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
}

impl UBigIntView {
    /// Rows
    pub fn len(&self) -> usize {
        self.data.len() / std::mem::size_of::<Item>()
    }

    /// Raw slice of target type.
    pub fn as_raw_slice(&self) -> &[Item] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const Item, self.len()) }
    }

    /// Build a nulls vector.
    pub fn to_nulls_vec(&self) -> Vec<bool> {
        self.is_null_iter().collect()
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
    pub fn get(&self, row: usize) -> Option<Item> {
        if row < self.len() {
            unsafe { self.get_unchecked(row) }
        } else {
            None
        }
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<Item> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(*self.as_raw_slice().get_unchecked(row))
        }
    }

    pub unsafe fn get_ref_unchecked(&self, row: usize) -> Option<*const Item> {
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
                Ty::UBigInt,
                std::mem::size_of::<Item>() as _,
                std::ptr::null(),
            )
        } else {
            (
                Ty::UBigInt,
                std::mem::size_of::<Item>() as _,
                self.as_raw_slice().get_unchecked(row) as *const Item as _,
            )
        }
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> UBigIntViewIter {
        UBigIntViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<Item>> {
        self.iter().collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let nulls = self.nulls.0.as_ref();
        wtr.write_all(nulls)?;
        wtr.write_all(&self.data)?;
        Ok(nulls.len() + self.data.len())
    }
}

pub struct UBigIntViewIter<'a> {
    view: &'a UBigIntView,
    row: usize,
}

impl<'a> Iterator for UBigIntViewIter<'a> {
    type Item = Option<Item>;

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

impl<'a> ExactSizeIterator for UBigIntViewIter<'a> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

impl<A: Into<Option<Item>>> FromIterator<A> for UBigIntView {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let (nulls, mut values): (Vec<bool>, Vec<_>) = iter
            .into_iter()
            .map(|v| match v.into() {
                Some(v) => (false, v),
                None => (true, Item::default()),
            })
            .unzip();
        Self {
            nulls: NullBits::from_iter(nulls),
            data: Bytes::from({
                let (ptr, len, cap) = (values.as_mut_ptr(), values.len(), values.capacity());
                std::mem::forget(values);
                unsafe { Vec::from_raw_parts(ptr as *mut u8, len * ITEM_SIZE, cap * ITEM_SIZE) }
            }),
        }
    }
}
