use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use bytes::{Bytes, BytesMut};

const ITEM_SIZE: usize = std::mem::size_of::<i32>();

/// A `[i32]` slice offsets, which will represent the value is NULL (if offset is `-1`) or not.
#[derive(Clone)]
pub struct Offsets(pub(super) Bytes);

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
    pub fn from_offsets<T: ExactSizeIterator<Item = i32>>(iter: T) -> Self {
        OffsetsMut::from_offsets(iter).into_offsets()
    }

    pub fn iter(&self) -> OffsetsIter<'_> {
        OffsetsIter {
            offsets: self,
            index: 0,
        }
    }

    /// As a [u8] slice.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn len(&self) -> usize {
        self.0.len() / std::mem::size_of::<i32>()
    }

    pub unsafe fn get_unchecked(&self, index: usize) -> i32 {
        unsafe {
            std::ptr::read_unaligned(self.0.as_ptr().add(index * std::mem::size_of::<i32>()) as _)
        }
    }

    pub unsafe fn slice_unchecked(
        &self,
        range: std::ops::Range<usize>,
    ) -> (Self, Option<(i32, Option<i32>)>) {
        use std::alloc::{alloc, Layout};

        let len = range.len();
        let ptr = alloc(Layout::from_size_align_unchecked(
            len * ITEM_SIZE,
            ITEM_SIZE,
        ));
        ptr.write_bytes(0, len * ITEM_SIZE);
        let slice = ptr as *mut i32;

        let mut offset0 = None;
        let start = range.start;
        for i in range.clone() {
            let offset = self.get_unchecked(i);
            let ptr = slice.offset(i as isize - start as isize);
            if offset == -1 {
                ptr.write(-1);
            } else if offset0.is_none() {
                ptr.write(0);
                offset0.replace(offset);
            } else if let Some(offset0) = offset0 {
                ptr.write(offset - offset0);
            }
        }

        let mut offset1 = None;
        if offset0.is_some() && range.end < self.len() {
            for i in range.end..self.len() {
                let offset = self.get_unchecked(i);
                if offset != -1 && offset1.is_none() {
                    offset1.replace(offset);
                    break;
                }
            }
        }

        if offset0 == offset1 {
            offset1.take();
        }

        let bytes: Box<[u8]> = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len * ITEM_SIZE));

        (
            Self(bytes.into()),
            offset0.map(|offset0| (offset0, offset1)),
        )
    }
}

pub struct OffsetsIter<'a> {
    offsets: &'a Offsets,
    index: usize,
}

impl Iterator for OffsetsIter<'_> {
    type Item = i32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.offsets.len() {
            let offset = unsafe { self.offsets.get_unchecked(self.index) };
            self.index += 1;
            Some(offset)
        } else {
            None
        }
    }
}

impl<'a> IntoIterator for &'a Offsets {
    type Item = i32;

    type IntoIter = OffsetsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        OffsetsIter {
            offsets: self,
            index: 0,
        }
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

    pub fn from_offsets<T: ExactSizeIterator<Item = i32>>(iter: T) -> Self {
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
    pub fn as_slice_mut(&mut self) -> &mut [i32] {
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

// impl IntoIterator for OffsetsMut {
//     type Item = i32;

//     type IntoIter = std::vec::IntoIter<i32>;

//     fn into_iter(self) -> Self::IntoIter {
//         self.as_slice().to_vec().into_iter()
//     }
// }

impl<'a> IntoIterator for &'a OffsetsMut {
    type Item = &'a i32;

    type IntoIter = std::slice::Iter<'a, i32>;

    fn into_iter(self) -> Self::IntoIter {
        self.deref().iter()
    }
}

impl<'a> IntoIterator for &'a mut OffsetsMut {
    type Item = &'a mut i32;

    type IntoIter = std::slice::IterMut<'a, i32>;

    fn into_iter(self) -> Self::IntoIter {
        self.deref_mut().iter_mut()
    }
}

#[test]
fn test_slice() {
    let offsets = &[-1i32, 0, 8, 12, -1, -1, 23];
    let bytes =
        unsafe { std::slice::from_raw_parts(offsets.as_ptr() as *const u8, offsets.len() * 4) };

    let data = Offsets(bytes.to_vec().into());
    dbg!(&data);
    let (slice, range) = unsafe { data.slice_unchecked(2..7) };
    assert_eq!(range, Some((8, None)));
    dbg!(&slice);
    for i in slice.iter() {
        dbg!(i);
    }
    let (_slice, range) = unsafe { data.slice_unchecked(2..6) };
    assert_eq!(range, Some((8, Some(23))));
    let (_slice, range) = unsafe { data.slice_unchecked(2..4) };
    assert_eq!(range, Some((8, Some(23))));

    let (_slice, range) = unsafe { data.slice_unchecked(0..1) };
    assert_eq!(range, None);

    let (_slice, range) = unsafe { data.slice_unchecked(0..2) };
    assert_eq!(range, Some((0, Some(8))));
    let (_slice, range) = unsafe { data.slice_unchecked(4..6) };
    assert_eq!(range, None);
}
