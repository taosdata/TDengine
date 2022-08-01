use std::ops::Deref;
use std::{fmt::Debug, ops::DerefMut};

use bytes::{Bytes, BytesMut};

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

    /// As a [u8] slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.as_ref()
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

#[test]
fn test_lengths_mut() {
    let mut lengths = LengthsMut::new(2);
    let lengths = lengths.deref_mut();
    lengths[0] = 1;
    lengths[1] = 2;
    dbg!(lengths);
}
