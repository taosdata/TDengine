use std::ffi::c_void;

use bytes::Bytes;

use super::{IsColumnView, NullBits, NullsIter};
use crate::common::{BorrowedValue, Ty};

type Item = i32;
type View = IntView;
const ITEM_SIZE: usize = std::mem::size_of::<Item>();

#[derive(Debug, Clone)]
pub struct IntView {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
}

impl IsColumnView for View {
    fn ty(&self) -> Ty {
        Ty::Int
    }

    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self {
        iter.map(|v| v.to_i32()).collect()
    }
}

impl std::ops::Add for View {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        &self + &rhs
    }
}
impl std::ops::Add for &View {
    type Output = View;

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

        View { nulls, data }
    }
}

impl std::ops::Add<View> for &View {
    type Output = View;

    fn add(self, rhs: View) -> Self::Output {
        self + &rhs
    }
}

impl std::ops::Add<&View> for View {
    type Output = View;

    fn add(self, rhs: &View) -> Self::Output {
        &self + rhs
    }
}

impl IntView {
    /// Rows
    pub fn len(&self) -> usize {
        self.data.len() / std::mem::size_of::<Item>()
    }

    /// Raw slice of target type.
    pub fn as_raw_slice(&self) -> &[Item] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const Item, self.len()) }
    }

    /// Raw pointer of the slice.
    pub fn as_raw_ptr(&self) -> *const Item {
        self.data.as_ptr() as *const Item
    }

    /// Build a nulls vector.
    pub fn to_nulls_vec(&self) -> Vec<bool> {
        self.is_null_iter().collect()
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> NullsIter<'_> {
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

    #[inline(always)]
    unsafe fn get_raw_at(&self, index: usize) -> *const Item {
        self.data.as_ptr().add(index * ITEM_SIZE) as _
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<Item> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(self.get_unchecked_inner(row).read_unaligned())
        }
    }

    unsafe fn get_unchecked_inner(&self, row: usize) -> *const Item {
        self.data
            .as_ptr()
            .offset(row as isize * std::mem::size_of::<Item>() as isize) as _
    }

    pub unsafe fn get_ref_unchecked(&self, row: usize) -> Option<*const Item> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(self.get_raw_at(row))
        }
    }

    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue<'_> {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Int), BorrowedValue::Int)
    }

    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        if self.nulls.is_null_unchecked(row) {
            (Ty::Int, std::mem::size_of::<Item>() as _, std::ptr::null())
        } else {
            (
                Ty::Int,
                std::mem::size_of::<Item>() as _,
                self.get_raw_at(row) as *const Item as _,
            )
        }
    }

    /// Create a slice of view.
    pub fn slice(&self, mut range: std::ops::Range<usize>) -> Option<Self> {
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
        let data = self
            .data
            .slice(range.start * ITEM_SIZE..range.end * ITEM_SIZE);
        Some(Self { nulls, data })
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> IntViewIter<'_> {
        IntViewIter { view: self, row: 0 }
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

pub struct IntViewIter<'a> {
    view: &'a IntView,
    row: usize,
}

impl Iterator for IntViewIter<'_> {
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

impl ExactSizeIterator for IntViewIter<'_> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{BigEndian, ByteOrder};

    use crate::views::int_view::{Item, View, ITEM_SIZE};

    #[test]
    fn test_slice() {
        let data = [0, 1, Item::MIN, Item::MAX];
        let view = View::from_iter(data);
        dbg!(&view);
        let slice = view.slice(1..3);
        dbg!(&slice);

        let data = [None, Some(Item::MIN), Some(Item::MAX), None];
        let view = View::from_iter(data);
        dbg!(&view);
        let range = 1..4;
        let slice = view.slice(range.clone()).unwrap();
        for (v, i) in slice.iter().zip(range) {
            assert_eq!(v, data[i]);
        }
    }

    #[cfg(target_endian = "little")]
    #[test]
    fn test_from_iterator() {
        let data = [0x12345678];
        let view = View::from_iter(data);
        let bytes = view.data;
        assert_eq!(bytes.to_vec(), vec![0x78, 0x56, 0x34, 0x12]);
    }

    #[cfg(target_endian = "little")]
    #[test]
    fn test_from_iterator_mock_big_endian() {
        let mut bytes = [0u8; 16];
        BigEndian::write_i32(&mut bytes, 0x12345678);
        BigEndian::write_i32(&mut bytes[4..], 0x78563412);
        BigEndian::write_i32(&mut bytes[8..], 0x12131415);
        BigEndian::write_i32(&mut bytes[12..], 0x51413121);

        for i in (0..bytes.len()).step_by(ITEM_SIZE) {
            let j = i + ITEM_SIZE;
            let val = Item::from_be_bytes(bytes[i..j].try_into().unwrap());
            bytes[i..j].copy_from_slice(&val.to_le_bytes());
        }

        let expect = vec![
            0x78, 0x56, 0x34, 0x12, 0x12, 0x34, 0x56, 0x78, 0x15, 0x14, 0x13, 0x12, 0x21, 0x31,
            0x41, 0x51,
        ];

        assert_eq!(bytes.to_vec(), expect);
    }
}
