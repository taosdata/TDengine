use std::alloc::Layout;
use std::fmt::Debug;
use std::ops::Range;

use bytes::Bytes;

const fn null_bits_len(len: usize) -> usize {
    len.div_ceil(8)
}

/// A bitmap for nulls.
///
/// ```text
///        +---+---+---+---+---+---+---+---+
/// byte0: | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
///        +---+---+---+---+---+---+---+---+
/// byte1: | 8 | 9 | 10| 11| 12| 13| 14| 15|
///        +---+---+---+---+---+---+---+---+
/// ```
///
/// Example, bytes `0b1000_0000` represents a boolean slice:
/// `[true, false * 7]`
#[derive(Debug, Clone)]
pub struct NullBits(pub(crate) Bytes);

impl<T: Into<Bytes>> From<T> for NullBits {
    fn from(v: T) -> Self {
        NullBits(v.into())
    }
}

impl FromIterator<bool> for NullBits {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let booleans = iter.into_iter().collect::<Vec<_>>();
        let len = null_bits_len(booleans.len());
        let inner = vec![0; len];
        let nulls = NullBits(inner.into());
        booleans.into_iter().enumerate().for_each(|(i, is_null)| {
            if is_null {
                unsafe { nulls.set_null_unchecked(i) };
            }
        });
        nulls
    }
}

impl NullBits {
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

    pub unsafe fn set_null_unchecked(&self, index: usize) {
        const BIT_LOC_SHIFT: usize = 3;
        const BIT_POS_SHIFT: usize = 7;
        let loc = self.0.as_ptr().add(index >> BIT_LOC_SHIFT) as *mut u8;
        *loc |= 1 << (BIT_POS_SHIFT - (index & BIT_POS_SHIFT));
        debug_assert!(self.is_null_unchecked(index));
    }

    pub unsafe fn slice(&self, range: Range<usize>) -> Self {
        let len = range.end - range.start;
        let bytes_len = null_bits_len(len);
        let inner = std::alloc::alloc(Layout::from_size_align_unchecked(bytes_len, 1));
        inner.write_bytes(0, bytes_len);
        let bytes: Box<[u8]> = Box::from_raw(std::slice::from_raw_parts_mut(inner, bytes_len));
        let nulls = NullBits(bytes.into());
        for i in 0..len {
            if self.is_null_unchecked(i + range.start) {
                unsafe { nulls.set_null_unchecked(i) };
            }
        }
        nulls
    }

    pub fn iter(&self) -> NullsIter<'_> {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a NullBits {
    type Item = bool;

    type IntoIter = NullsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        NullsIter {
            nulls: self,
            row: 0,
            len: self.0.len() * u8::BITS as usize,
        }
    }
}

pub struct NullsIter<'a> {
    pub(super) nulls: &'a NullBits,
    pub(super) row: usize,
    pub(super) len: usize,
}

impl Iterator for NullsIter<'_> {
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

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if self.row + n >= self.len {
            None
        } else {
            Some(unsafe { self.nulls.is_null_unchecked(self.row + n) })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.len > self.row {
            let hint = self.len - self.row;
            (hint, Some(hint))
        } else {
            (0, Some(0))
        }
    }
}

impl ExactSizeIterator for NullsIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_bits() {
        let bools = [true, false, false, true, false, false, true, true];
        let nulls = NullBits::from_iter(bools);
        println!("0b{:0b}", nulls.0[0]);

        // get
        for i in 0..6 {
            assert_eq!(bools[i], unsafe { nulls.is_null_unchecked(i) });
        }
        // iter
        let iter = nulls.iter();
        dbg!(iter.len());
        let iter = nulls.into_iter().take(bools.len());
        let len = iter.len();
        dbg!(len);
        // dbg!(iter.len());

        // assert_eq!(iter.len(), bools.len());
        let slice = unsafe { nulls.slice(1..5) };
        println!("0b{:0b}", slice.0[0]);
        for i in 0..4 {
            dbg!(unsafe { slice.is_null_unchecked(i) });
        }

        for i in 0..4 {
            assert_eq!(bools[i + 1], unsafe { slice.is_null_unchecked(i) });
        }
        let slice_iter = slice.iter().take(3);
        let bools: Vec<bool> = slice_iter.collect();
        assert_eq!(&bools, &[false, false, true]);
    }

    #[test]
    fn test_null_bits_slice() {
        const SIZE: usize = 1235;
        let bools: Vec<bool> = (0..SIZE).map(|_| rand::random()).collect();
        let nulls = NullBits::from_iter(bools.clone());

        for start in (0..SIZE).step_by(3) {
            for end in (start + 1..SIZE).step_by(7) {
                let range = start..end;
                let slice = unsafe { nulls.slice(range) };
                // let new = slice.is_null_unchecked(row)
                for i in start..end {
                    let correct = bools[i];
                    let data = unsafe { slice.is_null_unchecked(i - start) };
                    assert_eq!(
                        correct,
                        data,
                        "{} range {:?}. bytes: {:?}",
                        i,
                        start..end,
                        nulls.0
                    );
                }
            }
        }
    }
}
