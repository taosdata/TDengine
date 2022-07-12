use std::ops::Deref;
use std::{fmt::Debug, ops::DerefMut};

use bytes::{Bytes, BytesMut};

mod bool_view;
pub use bool_view::*;

mod tinyint_view;
pub use tinyint_view::*;

mod smallint_view;
pub use smallint_view::*;

mod int_view;
pub use int_view::*;

mod big_int_view;
pub use big_int_view::*;

mod float_view;
pub use float_view::*;

mod double_view;
pub use double_view::*;

mod tinyint_unsigned_view;
pub use tinyint_unsigned_view::*;

mod small_int_unsigned_view;
pub use small_int_unsigned_view::*;

mod int_unsigned_view;
pub use int_unsigned_view::*;

mod big_int_unsigned_view;
pub use big_int_unsigned_view::*;

mod timestamp_view;
pub use timestamp_view::*;

mod var_char_view;
pub use var_char_view::*;

mod n_char_view;
pub use n_char_view::*;

mod json_view;

pub use json_view::*;

use super::ColSchema;

/// A bitmap for nulls.
#[derive(Debug)]
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
    nulls: &'a NullBits,
    row: usize,
    len: usize,
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

/// A [i32] slice offsets, which will represent the value is NULL (if offset is `-1`) or not.
pub struct Offsets(Bytes);

impl<T: Into<Bytes>> From<T> for Offsets {
    fn from(value: T) -> Self {
        Offsets(value.into())
    }
}

impl Debug for Offsets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Offsets {
    pub fn from_offsets(iter: impl ExactSizeIterator<Item = i32>) -> Self {
        OffsetsMut::from_offsets(iter).into_offsets()
    }
    /// As a i32 slice.
    pub fn as_slice(&self) -> &[i32] {
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const i32,
                self.0.len() / std::mem::size_of::<i32>(),
            )
        }
    }
}

impl Deref for Offsets {
    type Target = [i32];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

/// A [i32] slice offsets, which will represent the value is NULL (if offset is `-1`) or not.
pub struct OffsetsMut(BytesMut);

impl<T: Into<BytesMut>> From<T> for OffsetsMut {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Debug for OffsetsMut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl OffsetsMut {
    pub fn new(len: usize) -> Self {
        let bytes_len = len * std::mem::size_of::<i32>();
        let mut bytes = BytesMut::with_capacity(bytes_len);
        bytes.resize(bytes_len, 0);
        Self(bytes)
    }

    pub fn from_offsets(iter: impl ExactSizeIterator<Item = i32>) -> Self {
        let mut offsets = Self::new(iter.len());

        iter.enumerate().for_each(|(i, offset)| unsafe {
            *offsets.get_unchecked_mut(i) = offset;
        });

        offsets
    }

    /// As a i32 slice.
    pub fn as_slice(&self) -> &[i32] {
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const i32,
                self.0.len() / std::mem::size_of::<i32>(),
            )
        }
    }
    /// As a i32 slice.
    pub fn as_slice_mut(&self) -> &mut [i32] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.0.as_ptr() as *mut i32,
                self.0.len() / std::mem::size_of::<i32>(),
            )
        }
    }

    pub fn into_offsets(self) -> Offsets {
        Offsets::from(self.0)
    }
}

impl Deref for OffsetsMut {
    type Target = [i32];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for OffsetsMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice_mut()
    }
}

impl IntoIterator for OffsetsMut {
    type Item = i32;

    type IntoIter = std::vec::IntoIter<i32>;

    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().to_vec().into_iter()
    }
}

impl<'a> IntoIterator for &'a OffsetsMut {
    type Item = &'a i32;

    type IntoIter = std::slice::Iter<'a, i32>;

    fn into_iter(self) -> Self::IntoIter {
        self.deref().into_iter()
    }
}

impl<'a> IntoIterator for &'a mut OffsetsMut {
    type Item = &'a mut i32;

    type IntoIter = std::slice::IterMut<'a, i32>;

    fn into_iter(self) -> Self::IntoIter {
        self.deref_mut().into_iter()
    }
}

pub struct Schemas(pub(super) Bytes);

impl<T: Into<Bytes>> From<T> for Schemas {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Debug for Schemas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.deref(), f)
    }
}

impl Schemas {
    /// As a [ColSchema] slice.
    pub fn as_slice(&self) -> &[ColSchema] {
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const ColSchema,
                self.0.len() / std::mem::size_of::<ColSchema>(),
            )
        }
    }
}

impl Deref for Schemas {
    type Target = [ColSchema];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

pub struct Lengths(pub(super) Bytes);

impl<T: Into<Bytes>> From<T> for Lengths {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Debug for Lengths {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.deref(), f)
    }
}

impl Lengths {
    /// As a [ColSchema] slice.
    pub fn as_slice(&self) -> &[u32] {
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const u32,
                self.0.len() / std::mem::size_of::<u32>(),
            )
        }
    }
}

impl Deref for Lengths {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

pub(crate) struct LengthsMut(BytesMut);

impl<T: Into<BytesMut>> From<T> for LengthsMut {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Debug for LengthsMut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.deref(), f)
    }
}

impl LengthsMut {
    /// Creates a new `LengthsMut` with the specified length `len`.
    ///
    pub fn new(len: usize) -> Self {
        let mut bytes = BytesMut::with_capacity(len * std::mem::size_of::<u32>());
        bytes.resize(len * std::mem::size_of::<u32>(), 0);
        Self(bytes)
    }
    /// As a [i32] slice.
    fn as_slice(&self) -> &mut [u32] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.0.as_ptr() as *mut u32,
                self.0.len() / std::mem::size_of::<u32>(),
            )
        }
    }
    pub fn into_lengths(self) -> Lengths {
        Lengths::from(self.0)
    }
}

impl Deref for LengthsMut {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for LengthsMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice()
    }
}

/// Compatible version for var char.
pub enum Version {
    V2,
    V3,
}

#[test]
fn test_lengths_mut() {
    let mut lengths = LengthsMut::new(2);
    let lengths = lengths.deref_mut();
    lengths[0] = 1;
    lengths[1] = 2;
    dbg!(lengths);
}
