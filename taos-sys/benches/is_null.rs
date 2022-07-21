#![feature(test)]

extern crate test;
use bitvec::prelude::*;

unsafe fn is_null_bit_slice(ptr: *const u8, row: usize) -> bool {
    let len = 1;
    let slice = std::slice::from_raw_parts(ptr, len);
    let is_nulls: &BitSlice<u8, Msb0> = &BitSlice::from_slice_unchecked(slice);
    is_nulls[row]
}

unsafe fn is_null(ptr: *const u8, row: usize) -> bool {
    macro_rules! is_null {
        ($bm:expr, $row:expr) => {
            *$bm.offset($row as isize >> 3) >> (7 - ($row & 7)) & 0x1 == 1
        };
    }
    is_null!(ptr, row)
}
#[cfg(test)]
mod tests {
    use super::*;
    use test::{black_box, Bencher};

    #[test]
    fn test() {
        let ptr = [0b11110000; 1].as_ptr();
        for i in 0..8 {
            assert_eq!(
                unsafe { is_null_bit_slice(ptr, i) },
                unsafe { is_null(ptr, i) },
                " in row: {}",
                i
            );
        }
    }

    #[bench]
    fn bench_bit_slice(b: &mut Bencher) {
        let slice = black_box([0b1010101; 6]);
        b.iter(|| {
            for i in 0..8 {
                let _ = unsafe { is_null_bit_slice(slice.as_ptr(), i) };
            }
        })
    }

    #[bench]
    fn bench_macro(b: &mut Bencher) {
        let slice = black_box([0b1010101; 6]);
        b.iter(|| {
            for i in 0..8 {
                let _ = unsafe { is_null(slice.as_ptr(), i) };
            }
        })
    }
}
