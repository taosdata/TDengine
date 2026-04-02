use std::ffi::c_void;
use std::io::Write;
use std::ops::Range;

use bytes::Bytes;

use super::{IsColumnView, NullBits, NullsIter};
use crate::common::{BorrowedValue, Ty};

type View = BoolView;

#[derive(Debug, Clone)]
pub struct BoolView {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
}

impl IsColumnView for BoolView {
    fn ty(&self) -> Ty {
        Ty::Bool
    }

    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
        iter.map(|v| v.to_bool()).collect()
    }
}

impl std::ops::Add for BoolView {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        &self + &rhs
    }
}

impl std::ops::Add for &BoolView {
    type Output = BoolView;

    fn add(self, rhs: Self) -> Self::Output {
        let nulls = self
            .nulls
            .iter()
            .take(self.len())
            .chain(rhs.nulls.iter().take(rhs.len()))
            .collect();
        let data: Bytes = self
            .data
            .as_ref()
            .iter()
            .chain(rhs.data.as_ref().iter())
            .copied()
            .collect();

        BoolView { nulls, data }
    }
}

impl std::ops::Add<BoolView> for &BoolView {
    type Output = BoolView;

    fn add(self, rhs: BoolView) -> Self::Output {
        self + &rhs
    }
}

impl std::ops::Add<&BoolView> for BoolView {
    type Output = BoolView;

    fn add(self, rhs: &BoolView) -> Self::Output {
        &self + rhs
    }
}

impl BoolView {
    /// Rows
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Raw pointer of the slice.
    pub fn as_raw_ptr(&self) -> *const bool {
        self.data.as_ptr() as *const bool
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> NullsIter<'_> {
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

    #[inline(always)]
    unsafe fn get_raw_at(&self, index: usize) -> *const bool {
        self.data.as_ptr().add(index) as _
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<bool> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(*self.get_raw_at(row))
        }
    }

    pub unsafe fn get_ref_unchecked(&self, row: usize) -> Option<*const bool> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(self.get_raw_at(row))
        }
    }

    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue<'_> {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Bool), BorrowedValue::Bool)
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        if self.nulls.is_null_unchecked(row) {
            (Ty::Bool, std::mem::size_of::<bool>() as _, std::ptr::null())
        } else {
            (
                Ty::Bool,
                std::mem::size_of::<bool>() as _,
                self.get_raw_at(row) as _,
            )
        }
    }

    pub fn slice(&self, mut range: Range<usize>) -> Option<Self> {
        if range.start >= self.len() {
            return None;
        }
        if range.end >= self.len() {
            range.end = self.len();
        }
        if range.is_empty() {
            return None;
        }

        let nulls = unsafe { self.nulls.slice(range.clone()) };
        let data = self.data.slice(range);
        Some(Self { nulls, data })
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> BoolViewIter<'_> {
        BoolViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<bool>> {
        self.iter().collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        let nulls = self.nulls.0.as_ref();
        debug_assert_eq!(nulls.len(), self.len().div_ceil(8));
        wtr.write_all(nulls)?;
        wtr.write_all(&self.data)?;
        Ok(nulls.len() + self.data.len())
    }

    pub fn concat(&self, rhs: &View) -> View {
        let nulls = self
            .nulls
            .iter()
            .take(self.len())
            .chain(rhs.nulls.iter().take(rhs.len()))
            .collect();
        let data: Bytes = self
            .data
            .as_ref()
            .iter()
            .chain(rhs.data.as_ref().iter())
            .copied()
            .collect();

        View { nulls, data }
    }
}

pub struct BoolViewIter<'a> {
    view: &'a BoolView,
    row: usize,
}

impl Iterator for BoolViewIter<'_> {
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

impl ExactSizeIterator for BoolViewIter<'_> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_slice() {
        let data = [true, false, false, true];
        let view = BoolView::from_iter(data);
        dbg!(&view);
        let slice = view.slice(1..3);
        dbg!(&slice);

        let data = [None, Some(false), Some(true), None];
        let view = BoolView::from_iter(data);
        dbg!(&view);
        let slice = view.slice(1..4);
        dbg!(&slice);

        let chain = &view + slice.as_ref().unwrap();
        dbg!(&chain);
        assert!(chain.len() == 7);
        assert!(chain
            .slice(0..4)
            .unwrap()
            .iter()
            .zip(view.iter())
            .all(|(l, r)| dbg!(l) == r));
    }
}
