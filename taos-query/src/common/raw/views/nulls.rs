use std::fmt::Debug;

use bytes::{Bytes, BytesMut};

/// A bitmap for nulls.
#[derive(Debug, Clone)]
pub struct NullBits(pub(crate) Bytes);

impl<T: Into<Bytes>> From<T> for NullBits {
    fn from(v: T) -> Self {
        NullBits(v.into())
    }
}
impl NullBits {
    pub const fn new() -> Self {
        Self(Bytes::new())
    }
    pub unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        const BIT_LOC_SHIFT: usize = 3;
        const BIT_POS_SHIFT: usize = 7;

        // Check bit at index: `$row >> 3` with bit position `$row % 8` from a u8 slice bitmap view.
        // It's a left-to-right bitmap, eg: 0b10000000, means row 0 is null.
        // Here we use right shift and then compare with 0b1.
        (self.0.as_ref().get_unchecked(row >> BIT_LOC_SHIFT)
            >> (BIT_POS_SHIFT - (row & BIT_POS_SHIFT)) as u8)
            & 0x1
            == 1
    }
}

pub struct NullsIter<'a> {
    pub(super) nulls: &'a NullBits,
    pub(super) row: usize,
    pub(super) len: usize,
}

impl<'a> Iterator for NullsIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.row;
        self.row += 1;
        if row < self.len {
            Some(unsafe { self.nulls.is_null_unchecked(row) })
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct NullsMut(BytesMut);

impl<T: Into<BytesMut>> From<T> for NullsMut {
    fn from(v: T) -> Self {
        Self(v.into())
    }
}

impl NullsMut {
    pub fn with_capacity(cap: usize) -> Self {
        let bytes_len = (cap + 7) / 8;
        let bytes = BytesMut::with_capacity(bytes_len);
        Self(bytes)
    }

    pub fn new(len: usize) -> Self {
        let bytes_len = (len + 7) / 8;
        let mut bytes = BytesMut::with_capacity(bytes_len);
        bytes.resize(bytes_len, 0);
        Self(bytes)
    }

    pub unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        const BIT_LOC_SHIFT: usize = 3;
        const BIT_POS_SHIFT: usize = 7;

        // Check bit at index: `$row >> 3` with bit position `$row % 8` from a u8 slice bitmap view.
        // It's a left-to-right bitmap, eg: 0b10000000, means row 0 is null.
        // Here we use right shift and then compare with 0b1.
        (self.0.as_ref().get_unchecked(row >> BIT_LOC_SHIFT)
            >> (BIT_POS_SHIFT - (row & BIT_POS_SHIFT)) as u8)
            & 0x1
            == 1
    }
    pub unsafe fn set_null_unchecked(&mut self, index: usize) {
        const BIT_LOC_SHIFT: usize = 3;
        const BIT_POS_SHIFT: usize = 7;
        let loc = self.0.get_unchecked_mut(index >> BIT_LOC_SHIFT);
        *loc |= 1 << (BIT_POS_SHIFT - (index & BIT_POS_SHIFT));
        println!("0x{:b}", loc);
        debug_assert!(self.is_null_unchecked(index));
    }

    pub fn into_nulls(self) -> NullBits {
        NullBits::from(self.0)
    }

    pub fn from_bools(iter: impl ExactSizeIterator<Item = bool>) -> Self {
        let mut nulls = Self::new(iter.len());
        iter.enumerate().for_each(|(i, is_null)| {
            if is_null {
                unsafe { nulls.set_null_unchecked(i) };
            }
        });
        nulls
    }
}

#[test]
fn test_nulls_mut() {
    let mut nulls = NullsMut::new(22);

    unsafe {
        for i in 0..22 {
            nulls.set_null_unchecked(i);
        }
    }
}
