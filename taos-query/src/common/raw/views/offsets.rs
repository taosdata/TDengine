use std::ops::Deref;
use std::{fmt::Debug, ops::DerefMut};

use bytes::{Bytes, BytesMut};

/// A [i32] slice offsets, which will represent the value is NULL (if offset is `-1`) or not.
#[derive(Clone)]
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
