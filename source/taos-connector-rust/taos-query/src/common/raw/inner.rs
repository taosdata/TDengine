use std::ffi::c_void;
use std::fmt::Debug;
use std::mem::{size_of, transmute};
use std::slice;

use bitvec::macros::internal::funty::Numeric;
use itertools::Itertools;
use once_cell::unsync::OnceCell;

use crate::common::{BorrowedValue, Column, Field, Precision, Timestamp, Ty, Value};
use crate::util::{Inlinable, InlinableRead, InlinableWrite};
use crate::BlockExt;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
#[repr(packed(2))] // use packed(2) because it's int16_t in raw block.
pub struct ColSchema {
    pub(crate) ty: Ty,
    pub(crate) len: u32,
}

impl ColSchema {
    #[inline]
    pub(crate) const fn new(ty: Ty, len: u32) -> Self {
        Self { ty, len }
    }
    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe { std::mem::transmute::<&Self, &[u8; 6]>(self) }
    }

    #[inline]
    pub fn into_bytes(self) -> [u8; 6] {
        unsafe { std::mem::transmute::<Self, [u8; 6]>(self) }
    }
}

#[test]
fn col_schema() {
    let col = ColSchema {
        ty: Ty::BigInt,
        len: 1,
    };
    let bytes: [u8; 6] = unsafe { std::mem::transmute_copy(&col) };
    dbg!(&bytes);

    let bytes: [u8; 6] = [4, 0, 1, 0, 0, 0];
    let col2: ColSchema = unsafe { std::mem::transmute_copy(&bytes) };
    dbg!(col2);
}
#[test]
fn test_bin() {
    let v: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let ptr = v.as_ptr();

    let v_u16 = unsafe { *transmute::<*const u8, *const u16>(ptr) };
    println!("{v_u16:#x?}: {:?}", v_u16.to_le_bytes());
    #[derive(Debug, Clone, Copy)]
    #[repr(packed)]
    #[allow(dead_code)]
    struct A {
        a: u16,
        b: u32,
    }
    println!("A size: {}", std::mem::size_of::<A>());
    let a: &A = unsafe { transmute::<*const u8, *const A>(ptr).as_ref().unwrap() };
    println!("{a:#x?}");
}

/// Raw data block format (B for bytes):
///
/// ```text,ignore
/// +-----+----------+---------------+-----------+-----------------------+-----------------+
/// | len | group id | col_schema... | length... | (bitmap or offsets    | col data)   ... |
/// | 4B  | 8B       | (2+4)B * cols | 4B * cols | (row+7)/8 or 4 * rows | length[col] ... |
/// +-----+----------+---------------+-----------+-----------------------+-----------------+
/// ```
///
/// The length of bitmap is decided by number of rows of this data block, and the length of each column data is
/// recorded in the first segment, next to the struct header
#[derive(Clone)]
pub struct RawBlock {
    data: Vec<u8>,
    rows: usize,
    cols: usize,
    precision: Precision,
    offsets: OnceCell<Vec<(Ty, isize, isize)>>,
}

unsafe impl Send for RawBlock {}
unsafe impl Sync for RawBlock {}

impl Default for RawBlock {
    fn default() -> Self {
        Self {
            data: Default::default(),
            rows: Default::default(),
            cols: Default::default(),
            precision: Precision::Millisecond,
            offsets: Default::default(),
        }
    }
}

impl Debug for RawBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for row in 0..self.nrows() {
            let line = (0..self.ncols())
                .map(|col| unsafe { format!("{}", self.get_unchecked(row, col)) })
                .join(" | ");
            f.write_fmt(format_args!("{}\n", line))?;
        }

        Ok(())
    }
}
// impl Drop for RawBlock {
//     #[inline]
//     fn drop(&mut self) {
//         unsafe { Vec::from_raw_parts(self.data, self.len, self.cap) };
//     }
// }

impl Inlinable for RawBlock {
    /// **NOTE**: raw block bytes is not enough to parse data from.
    /// You must call [with_rows](#method.with_rows)
    fn read_inlined<R: std::io::Read>(mut reader: R) -> std::io::Result<Self> {
        let len = reader.read_u32()? as usize;
        let mut bytes = Vec::with_capacity(len - 4);
        bytes.resize(len - 4, 0);
        reader.read_exact(&mut bytes)?;
        Ok(Self::new(bytes))
    }

    fn write_inlined<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let bytes = self.as_bytes();
        wtr.write_u32(bytes.len() as u32)?;
        wtr.write(bytes)
    }
}
impl RawBlock {
    #[inline]
    pub fn new(mut vec: Vec<u8>) -> Self {
        // let data = vec.as_mut_ptr();
        // let len = vec.len();
        // let cap = vec.capacity();
        // assert_eq!(len, cap);
        // std::mem::forget(vec);
        Self {
            data: vec,
            rows: Default::default(),
            cols: Default::default(),
            precision: Precision::Millisecond,
            offsets: Default::default(),
        }
    }

    #[inline]
    pub fn from_v2(
        bytes: &[u8],
        fields: &[Field],
        lengths: &[u32],
        rows: usize,
        precision: Precision,
    ) -> Self {
        let mut data = Vec::new();
        // data len place holder
        // data.extend(0u32.to_le_bytes());
        // group id, always use 0
        data.extend(0u64.to_le_bytes());
        // column schema
        for field in fields {
            data.extend(field.to_column_schema().into_bytes())
        }
        // lengths placeholder for each columns, use u32.
        data.extend(std::iter::repeat(0).take(fields.len() * 4));

        let lengths_offset = 8 + fields.len() * 6;

        // let column_lengths =
        //     unsafe { data.as_mut_ptr().offset(8 + fields.len() as isize * 6) as *mut u32 };
        use bitvec::prelude::*;

        let mut offset = 0;
        for (i, (field, length)) in fields.into_iter().zip(lengths).enumerate() {
            match field.ty() {
                Ty::Null => unreachable!(),
                Ty::Bool => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let slice = &bytes[offset..(offset + rows)];
                    is_null.extend(slice.iter().map(|b| *b == 0x2));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::TinyInt => {
                    // dbg!(&data);
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const i8>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == i8::MIN));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::SmallInt => {
                    // dbg!(&data);
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);

                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const i16>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == i16::MIN));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::Int => {
                    // dbg!(&data);
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const i32>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == i32::MIN));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::BigInt => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const i64>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == i64::MIN));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::Float => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const f32>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == f32::NAN));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::Double => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const f64>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == f64::NAN));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::VarChar => unsafe {
                    let mut offsets: Vec<i32> = Vec::with_capacity(rows);
                    let mut slice: Vec<u8> = Vec::new();
                    let mut row_offset = 0;

                    for i in 0..rows {
                        let col = bytes
                            .as_ptr()
                            .offset(offset as _)
                            .offset(i as isize * *length as isize);
                        let len = *transmute::<*const u8, *const u16>(col);
                        // offset
                        if len == 1 && *col.offset(2) == 0xFF {
                            // is null
                            offsets.push(-1);
                        } else {
                            // not null
                            offsets.push(row_offset);
                            slice.extend(len.to_le_bytes());
                            slice.extend(slice::from_raw_parts(col.offset(2), len as usize));

                            row_offset += len as i32 + 2;
                        }
                    }

                    // Write slice.len() as column length.
                    // Do not include the offsets part length.
                    std::ptr::copy_nonoverlapping(
                        (slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                        data.as_mut_ptr()
                            .offset(lengths_offset as isize + i as isize * 4),
                        4,
                    );
                    // let len = *(data
                    //     .as_ptr()
                    //     .offset(lengths_offset as isize + i as isize * 4)
                    //     as *const u32);
                    data.extend(slice::from_raw_parts(
                        offsets.as_ptr() as *const u8,
                        offsets.len() * std::mem::size_of::<i32>(),
                    ));
                    data.extend(slice);
                    offset += rows * *length as usize;
                },
                Ty::Timestamp => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const i64>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == i64::MIN));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::NChar => unsafe {
                    let mut offsets: Vec<i32> = Vec::with_capacity(rows);
                    let mut slice: Vec<u8> = Vec::new();
                    let mut row_offset = 0;
                    for i in 0..rows {
                        let col = bytes
                            .as_ptr()
                            .offset(row_offset as _)
                            .offset(i as isize * *length as isize);
                        let len = *transmute::<*const u8, *const u16>(col);
                        // offset
                        if len == 4
                            && *col.offset(2) == 0xFF
                            && *col.offset(4) == 0xFF
                            && *col.offset(6) == 0xFF
                            && *col.offset(8) == 0xFF
                        {
                            // is null
                            offsets.push(-1);
                        } else {
                            // not null
                            offsets.push(row_offset);
                            slice.extend(len.to_le_bytes());
                            slice.extend(slice::from_raw_parts(col.offset(2), len as usize));

                            row_offset += len as i32 + 2;
                        }
                    }

                    std::ptr::copy_nonoverlapping(
                        (slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                        data.as_mut_ptr()
                            .offset(lengths_offset as isize + i as isize * 4),
                        4,
                    );

                    data.extend(slice::from_raw_parts(
                        offsets.as_ptr() as *const u8,
                        offsets.len() * 4,
                    ));
                    data.extend(slice);
                    offset += rows * *length as usize;
                },
                Ty::UTinyInt => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let slice = &bytes[offset..(offset + rows * *length as usize)];
                    is_null.extend(slice.iter().map(|b| *b == u8::MAX));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::USmallInt => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const u16>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == u16::MAX));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::UInt => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const u32>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == u32::MAX));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                Ty::UBigInt => {
                    debug_assert_eq!(field.bytes(), *length);
                    let mut is_null: BitVec<u8> = BitVec::with_capacity(rows);
                    let byte_slice = &bytes[offset..(offset + rows * *length as usize)];
                    let value_slice = unsafe {
                        slice::from_raw_parts(
                            transmute::<*const u8, *const u64>(byte_slice.as_ptr()),
                            rows,
                        )
                    };
                    is_null.extend(value_slice.iter().map(|b| *b == u64::MAX));
                    debug_assert_eq!(is_null.as_raw_slice().len(), (rows + 7) / 8);
                    data.extend(is_null.as_raw_slice());
                    data.extend(byte_slice);
                    offset += rows * *length as usize;

                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            (byte_slice.len() as u32).to_le_bytes().as_slice().as_ptr(),
                            data.as_mut_ptr()
                                .offset(lengths_offset as isize + i as isize * 4),
                            4,
                        );
                    }
                }
                _ => todo!(),
            }
        }

        debug_assert_eq!(fields.len(), lengths.len());
        let cols = lengths.len();
        Self {
            data,
            rows,
            cols,
            precision,
            offsets: OnceCell::new(),
        }
    }

    #[inline]
    pub fn with_rows(&mut self, rows: usize) -> &mut Self {
        self.rows = rows;
        self
    }

    #[inline]
    pub fn with_cols(&mut self, cols: usize) -> &mut Self {
        self.cols = cols;
        self
    }

    #[inline]
    pub fn with_precision(&mut self, precision: Precision) -> &mut Self {
        self.precision = precision;
        self
    }

    #[inline]
    /// From raw data block.
    pub unsafe fn from_ptr(
        data: *const u8,
        rows: usize,
        cols: usize,
        precision: Precision,
    ) -> Self {
        let len = *transmute::<*const u8, *const u32>(data) as usize;
        let data = std::slice::from_raw_parts(data.offset(4), len - 4).to_vec();
        Self::from_bytes(data, rows, cols, precision)
    }

    #[inline]
    /// Build inner block from bytes.
    pub fn from_bytes(mut vec: Vec<u8>, rows: usize, cols: usize, precision: Precision) -> Self {
        Self {
            data: vec,
            rows,
            cols,
            precision,
            offsets: OnceCell::new(),
        }
    }

    #[inline]
    /// The whole block slice length.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub const fn nrows(&self) -> usize {
        self.rows
    }

    #[inline]
    pub const fn ncols(&self) -> usize {
        self.cols
    }

    #[inline]
    /// The group id of the raw block.
    pub fn group_id(&self) -> u64 {
        unsafe { *std::mem::transmute::<*const u8, *const u64>(self.as_ptr()) }
    }

    /// Inner block as bytes slice.
    #[inline]
    // #[rustversion::attr(nightly, const)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    /// Raw data block bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_owned()
    }

    #[inline]
    /// Raw data into block bytes.
    pub fn into_vec(self) -> Vec<u8> {
        // unsafe { Vec::from_raw_parts(self.data, self.len, self.cap) }
        self.data
    }

    #[inline]
    /// Pointer to raw block data slice.
    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    #[inline]
    /// Offset to column schema start position.
    const fn schema_offset(&self) -> isize {
        // 4 = block data length.
        // 8 = group id.
        8
    }

    #[inline]
    /// Offset to lengths start position.
    const fn lengths_offset(&self) -> isize {
        // 6 == size_of::<ColumnSchema>()
        self.schema_offset() + self.cols as isize * 6
    }

    #[inline]
    /// Offset to column data start position.
    const fn data_offset(&self) -> isize {
        self.lengths_offset() + self.cols as isize * 4
    }

    #[inline]
    /// Pointer to specific offset.
    unsafe fn offset(&self, count: isize) -> *const u8 {
        self.as_ptr().offset(count)
    }

    #[inline]
    /// Length of each bitmap block.
    const fn bitmap_len(&self) -> usize {
        (self.rows + 7) / 8
    }

    /// A lazy-init-ed index to each column.
    ///
    /// For each column, the index is a 3-element tuple:
    ///
    /// 0. Column data type represented as [Ty]
    /// 1. Offset to column data start position relative to the block front.
    ///   - For var-type, it's a `rows` length `i32` vector contains the offsets to each row.
    ///   - For non-var-type, it's the is-null bitmap.
    /// 2. Offset to the start position of real column data.
    fn column_offsets(&self) -> &[(Ty, isize, isize)] {
        self.offsets.get_or_init(|| {
            let lengths = self.lengths();
            let mut data_offset = self.data_offset();
            // dbg!(data_offset, self.len());
            debug_assert!(data_offset < self.len() as isize);
            self.schemas()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    // debug_assert!(data_offset < self.len() as isize);
                    if col.ty.is_var_type() {
                        let o = (col.ty, data_offset, data_offset + 4 * self.rows as isize);
                        data_offset = o.2 + lengths[i] as isize;
                        o
                    } else {
                        let o = (
                            col.ty,
                            data_offset,
                            data_offset + self.bitmap_len() as isize,
                        );
                        data_offset = o.2 + lengths[i] as isize;

                        //assert!(data_offset < self.len() as isize);
                        o
                    }
                })
                .collect()
        })
    }

    /// Column schema extractor.
    #[inline]
    pub fn schemas(&self) -> &[ColSchema] {
        unsafe {
            let ptr = self.offset(self.schema_offset());
            slice::from_raw_parts(ptr as *mut ColSchema, self.cols)
        }
    }

    /// Get column data type.
    #[inline]
    // #[rustversion::attr(nightly, const)]
    pub fn get_type_of(&self, col: usize) -> Ty {
        self.get_schema_of(col).ty
    }

    /// Get column schema which includes data type and bytes length.
    #[inline]
    // #[rustversion::attr(nightly, const)]
    pub fn get_schema_of(&self, col: usize) -> &ColSchema {
        unsafe { self.schemas().get_unchecked(col) }
    }

    #[inline]
    /// Lengths for each column raw data.
    fn lengths(&self) -> &[i32] {
        unsafe {
            let ptr = self.offset(self.lengths_offset());
            slice::from_raw_parts(ptr as *mut i32, self.cols)
        }
    }

    #[inline]
    /// Get one value at `(row, col)` of the block.
    pub unsafe fn get_raw_value_unchecked(
        &self,
        row: usize,
        col: usize,
    ) -> (Ty, u32, *const c_void) {
        let (ty, o1, o2) = self.column_offsets().get_unchecked(col);

        const BIT_LOC_SHIFT: isize = 3;
        const BIT_POS_SHIFT: usize = 7;

        macro_rules! is_null {
            ($bm:expr, $row:expr) => {{
                // Check bit at index: `$row >> 3` with bit position `$row % 8` from a u8 slice bitmap view.
                // It's a left-to-right bitmap, eg: 0b10000000, means row 0 is null.
                // Here we use right shift and then compare with 0b1.
                (*$bm.offset($row as isize >> BIT_LOC_SHIFT) >> (BIT_POS_SHIFT - ($row & BIT_POS_SHIFT)) as u8) & 0x1 == 1
            }};
        }

        macro_rules! _primitive_value {
            ($ty:ident, $native:ty) => {{
                let ptr = self.offset(*o1);
                if is_null!(ptr, row) {
                    (*ty, size_of::<$native>() as u32, std::ptr::null())
                } else {
                    (
                        *ty,
                        size_of::<$native>() as u32,
                        self.offset(o2 + (row * size_of::<$native>()) as isize) as _,
                    )
                }
            }};
        }

        match ty {
            Ty::Null => (*ty, ty.fixed_length() as u32, std::ptr::null()),
            Ty::Bool => _primitive_value!(Bool, bool),
            Ty::TinyInt => _primitive_value!(TinyInt, i8),
            Ty::SmallInt => _primitive_value!(SmallInt, i16),
            Ty::Int => _primitive_value!(Int, i32),
            Ty::BigInt => _primitive_value!(BigInt, i64),
            Ty::Float => _primitive_value!(Float, f32),
            Ty::Double => _primitive_value!(Double, f64),
            Ty::VarChar => {
                //
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    (*ty, 0, std::ptr::null())
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);
                    (*ty, len as _, ptr.offset(2) as _)
                }
            }
            Ty::Timestamp => {
                let ptr = self.offset(*o1);
                if is_null!(ptr, row) {
                    (*ty, 8, std::ptr::null())
                } else {
                    (
                        *ty,
                        8,
                        (self.offset(o2 + (row * size_of::<i64>()) as isize) as *const i64) as _,
                    )
                }
            }
            Ty::NChar => {
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    (*ty, 0, std::ptr::null())
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);
                    (*ty, len as _, ptr.offset(2) as _)
                }
            }
            Ty::UTinyInt => _primitive_value!(UTinyInt, u8),
            Ty::USmallInt => _primitive_value!(USmallInt, u16),
            Ty::UInt => _primitive_value!(UInt, u32),
            Ty::UBigInt => _primitive_value!(UBigInt, u64),
            Ty::Json => {
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    (*ty, 8, std::ptr::null())
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);
                    (*ty, len as _, ptr.offset(2) as _)
                }
            }
            ty => unreachable!("unsupported type: {ty}"),
        }
    }

    #[inline]
    /// Get one value at `(row, col)` of the block.
    pub unsafe fn get_ref_unchecked(&self, row: usize, col: usize) -> BorrowedValue {
        use BorrowedValue::*;
        let (ty, o1, o2) = self.column_offsets().get_unchecked(col);

        macro_rules! is_null {
            ($bm:expr, $row:expr) => {{
                (*$bm.offset($row as isize >> 3) >> (7 - ($row & 7)) as u8) & 0x1 == 1
            }};
        }

        macro_rules! _primitive_value {
            ($ty:ident, $native:ty) => {{
                let ptr = self.offset(*o1);
                if is_null!(ptr, row) {
                    Null
                } else {
                    let v = *(self.offset(o2 + (row * size_of::<$native>()) as isize)
                        as *const $native);
                    $ty(v)
                }
            }};
        }

        match ty {
            Ty::Null => Null,
            Ty::Bool => _primitive_value!(Bool, bool),
            Ty::TinyInt => _primitive_value!(TinyInt, i8),
            Ty::SmallInt => _primitive_value!(SmallInt, i16),
            Ty::Int => _primitive_value!(Int, i32),
            Ty::BigInt => _primitive_value!(BigInt, i64),
            Ty::Float => _primitive_value!(Float, f32),
            Ty::Double => _primitive_value!(Double, f64),
            Ty::VarChar => {
                //
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    Null
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);
                    VarChar(std::str::from_utf8_unchecked(slice::from_raw_parts(
                        ptr.offset(2),
                        len as usize,
                    )))
                }
            }
            Ty::Timestamp => {
                let ptr = self.offset(*o1);
                if is_null!(ptr, row) {
                    Null
                } else {
                    let v = *(self.offset(o2 + (row * size_of::<i64>()) as isize) as *const i64);
                    Timestamp(crate::common::Timestamp::new(v, self.precision))
                }
            }
            Ty::NChar => {
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    Null
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);
                    if len < 0 {
                        return Null;
                    }
                    NChar(
                        slice::from_raw_parts(ptr.offset(2) as *mut char, len as usize / 4)
                            .into_iter()
                            .collect::<String>()
                            .into(),
                    )
                }
            }
            Ty::UTinyInt => _primitive_value!(UTinyInt, u8),
            Ty::USmallInt => _primitive_value!(USmallInt, u16),
            Ty::UInt => _primitive_value!(UInt, u32),
            Ty::UBigInt => _primitive_value!(UBigInt, u64),
            Ty::Json => {
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    Null
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);
                    debug_assert!(len >= 0);
                    let bytes = slice::from_raw_parts(ptr.offset(2), len as usize);
                    Json(bytes.into())
                }
            }
            ty => unreachable!("unsupported type: {ty}"),
        }
    }

    #[inline]
    /// Get one value at `(row, col)` of the block.
    pub unsafe fn get_unchecked(&self, row: usize, col: usize) -> Value {
        let (ty, o1, o2) = self.column_offsets().get_unchecked(col);

        macro_rules! is_null {
            ($bm:expr, $row:expr) => {{
                (*$bm.offset($row as isize >> 3) >> (7 - ($row & 7)) as u8) & 0x1 == 1
            }};
        }

        macro_rules! _primitive_value {
            ($ty:ident, $native:ty) => {{
                let ptr = self.offset(*o1);
                if is_null!(ptr, row) {
                    Value::Null
                } else {
                    let v = *(self.offset(o2 + (row * size_of::<$native>()) as isize)
                        as *const $native);
                    Value::$ty(v)
                }
            }};
        }

        match ty {
            Ty::Null => Value::Null,
            Ty::Bool => _primitive_value!(Bool, bool),
            Ty::TinyInt => _primitive_value!(TinyInt, i8),
            Ty::SmallInt => _primitive_value!(SmallInt, i16),
            Ty::Int => _primitive_value!(Int, i32),
            Ty::BigInt => _primitive_value!(BigInt, i64),
            Ty::Float => _primitive_value!(Float, f32),
            Ty::Double => _primitive_value!(Double, f64),
            Ty::VarChar => {
                //
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    Value::Null
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);
                    Value::VarChar(
                        std::str::from_utf8_unchecked(slice::from_raw_parts(
                            ptr.offset(2),
                            len as usize,
                        ))
                        .to_string(),
                    )
                }
            }
            Ty::Timestamp => {
                let ptr = self.offset(*o1);
                if is_null!(ptr, row) {
                    Value::Null
                } else {
                    let v = *(self.offset(o2 + (row * size_of::<i64>()) as isize) as *const i64);
                    Value::Timestamp(Timestamp::new(v, self.precision))
                }
            }
            Ty::NChar => {
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    Value::Null
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);

                    Value::NChar(
                        slice::from_raw_parts(ptr.offset(2) as *mut char, len as usize / 4)
                            .into_iter()
                            .collect(),
                    )
                }
            }
            Ty::UTinyInt => _primitive_value!(UTinyInt, u8),
            Ty::USmallInt => _primitive_value!(USmallInt, u16),
            Ty::UInt => _primitive_value!(UInt, u32),
            Ty::UBigInt => _primitive_value!(UBigInt, u64),
            Ty::Json => {
                let offset =
                    *transmute::<*const u8, *const i32>(self.offset(o1 + row as isize * 4));
                if offset < 0 {
                    Value::Null
                } else {
                    let ptr = self.offset(o2 + offset as isize);
                    let len: i16 = *(ptr as *mut i16);
                    debug_assert!(len >= 0);
                    let bytes = slice::from_raw_parts(ptr.offset(2), len as usize);
                    let json: serde_json::Value = serde_json::from_slice(bytes).unwrap();
                    Value::Json(json)
                }
            }
            ty => {
                dbg!(*ty as u8);
                unreachable!("unsupported type: {ty}")
            }
        }
    }
}

#[test]
fn inner_block() {
    use crate::common::Precision::Millisecond;
    use crate::common::Timestamp::Milliseconds;
    let bytes = b"5\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\t\x00\x08\x00\x00\x00\x01\x00\x01\x00\x00\x00\x02\x00\x01\x00\x00\x00\x03\x00\x02\x00\x00\x00\x04\x00\x04\x00\x00\x00\x05\x00\x08\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x0c\x00\x02\x00\x00\x00\r\x00\x04\x00\x00\x00\x0e\x00\x08\x00\x00\x00\x06\x00\x04\x00\x00\x00\x07\x00\x08\x00\x00\x00\x08\x00f\x00\x00\x00\n\x00\x92\x01\x00\x00\x10\x00\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x05\x00\x00\x00\x16\x00\x00\x00\x00\xf2=\xc3u\x81\x01\x00\x00\xdaA\xc3u\x81\x01\x00\x00@\x01\x00@\xff\x00@\xff\xff\x00\x00@\xff\xff\xff\xff\x00\x00\x00\x00@\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00@\x01\x00@\x01\x00\x00\x00@\x01\x00\x00\x00\x00\x00\x00\x00@\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x03\x00abc\x00\x00\x00\x00\xff\xff\xff\xff\x14\x00\x9bm\x00\x00\x1d`\x00\x00\x1e\xd1\x01\x00pe\x00\x00nc\x00\x00\x00\x00\x00\x00";
    let rows = 2;
    let cols = 14;
    let precision = Millisecond;

    let block = unsafe { RawBlock::from_ptr(bytes.as_ptr(), rows, cols, precision) };

    assert_eq!(block.len(), bytes.len() - 4);
    assert_eq!(block.as_bytes(), &bytes[4..]);
    assert_eq!(block.group_id(), 0);

    use Value::*;
    let values = vec![
        vec![
            Timestamp(Milliseconds(1655538138610)),
            Bool(true),
            Value::TinyInt(-1),
            SmallInt(-1),
            Int(-1),
            BigInt(-1),
            UTinyInt(1),
            USmallInt(1),
            UInt(1),
            UBigInt(1),
            Float(0.0),
            Double(0.0),
            VarChar("abc".to_string()),
            NChar("涛思𝄞数据".to_string()),
        ],
        {
            Some(Timestamp(Milliseconds(1655538139610)))
                .into_iter()
                .chain(std::iter::repeat(Value::Null).take(14))
                .collect::<Vec<_>>()
        },
    ];
    assert!(block.cols == 14);

    for row in 0..rows {
        for col in 0..block.cols {
            let v = unsafe { block.get_unchecked(row, col) };
            assert_eq!(v, values[row][col]);
        }
    }

    let inlined = block.inlined();
    assert!(inlined == bytes);
}

#[test]
fn inner_block_with_json() {
    use crate::common::Precision::Millisecond;
    use crate::common::Timestamp::Milliseconds;
    let precision = Millisecond;
    let rows = 3;
    let cols = 15;
    let bytes = b"\xcc\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\t\x00\x08\x00\x00\x00\x01\x00\x01\x00\x00\x00\x02\x00\x01\x00\x00\x00\x03\x00\x02\x00\x00\x00\x04\x00\x04\x00\x00\x00\x05\x00\x08\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x0c\x00\x02\x00\x00\x00\r\x00\x04\x00\x00\x00\x0e\x00\x08\x00\x00\x00\x06\x00\x04\x00\x00\x00\x07\x00\x08\x00\x00\x00\x08\x00f\x00\x00\x00\n\x00\x92\x01\x00\x00\x0f\x00\x00@\x00\x00\x18\x00\x00\x00\x03\x00\x00\x00\x03\x00\x00\x00\x06\x00\x00\x00\x0c\x00\x00\x00\x18\x00\x00\x00\x03\x00\x00\x00\x06\x00\x00\x00\x0c\x00\x00\x00\x18\x00\x00\x00\x0c\x00\x00\x00\x18\x00\x00\x00\x05\x00\x00\x00\x16\x00\x00\x004\x00\x00\x00\x00?\x8c\xfa\x84\x81\x01\x00\x00>\x8c\xfa\x84\x81\x01\x00\x00?\x8c\xfa\x84\x81\x01\x00\x00\xc0\x00\x00\x01\xc0\x00\x00\xff\xc0\x00\x00\x00\x00\xff\xff\xc0\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xc0\x00\x00\x01\xc0\x00\x00\x00\x00\x01\x00\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x03\x00abc\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x14\x00\x9bm\x00\x00\x1d`\x00\x00\x1e\xd1\x01\x00pe\x00\x00nc\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x1a\x00\x00\x00\x18\x00{\"a\":\"\xe6\xb6\x9b\xe6\x80\x9d\xf0\x9d\x84\x9e\xe6\x95\xb0\xe6\x8d\xae\"}\x18\x00{\"a\":\"\xe6\xb6\x9b\xe6\x80\x9d\xf0\x9d\x84\x9e\xe6\x95\xb0\xe6\x8d\xae\"}\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

    let block = unsafe { RawBlock::from_ptr(bytes.as_ptr(), rows, cols, precision) };

    assert_eq!(block.len(), bytes.len() - 4);
    assert_eq!(block.as_bytes(), &bytes[4..]);
    assert_eq!(block.group_id(), 0);

    use Value::*;
    let values = vec![
        {
            Some(Timestamp(Milliseconds(1655793421375)))
                .into_iter()
                .chain(std::iter::repeat(Value::Null).take(14))
                .collect::<Vec<_>>()
        },
        {
            Some(Timestamp(Milliseconds(1655793421374)))
                .into_iter()
                .chain(std::iter::repeat(Value::Null).take(13))
                .chain(Some(Json(serde_json::json!({
                    "a": "涛思𝄞数据"
                }))))
                .collect::<Vec<_>>()
        },
        vec![
            Timestamp(Milliseconds(1655793421375)),
            Bool(true),
            Value::TinyInt(-1),
            SmallInt(-1),
            Int(-1),
            BigInt(-1),
            UTinyInt(1),
            USmallInt(1),
            UInt(1),
            UBigInt(1),
            Float(0.0),
            Double(0.0),
            VarChar("abc".to_string()),
            NChar("涛思𝄞数据".to_string()),
            Json(serde_json::json!({
                "a": "涛思𝄞数据"
            })),
        ],
    ];
    assert!(block.ncols() == 15);

    for row in 0..rows {
        assert_eq!(values[row].len(), block.ncols());
        for col in 0..block.ncols() {
            let v = unsafe { block.get_unchecked(row, col) };
            assert_eq!(v, values[row][col], "Error at ({row}, {col})");
        }
    }

    let inlined = block.inlined();
    assert!(inlined == bytes);
}

#[test]
fn test_from_v2() {
    let raw = RawBlock::from_v2(
        &[1],
        &[Field::new("a", Ty::TinyInt, 1)],
        &[1],
        1,
        Precision::Millisecond,
    );
    dbg!(raw.as_bytes());
    let v = unsafe { raw.get_ref_unchecked(0, 0) };
    dbg!(v);

    let raw = RawBlock::from_v2(
        &[1, 0, 0, 0],
        &[Field::new("a", Ty::Int, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(raw.as_bytes());
    let v = unsafe { raw.get_ref_unchecked(0, 0) };
    dbg!(v);

    let raw = RawBlock::from_v2(
        &[2, 0, b'a', b'b'],
        &[Field::new("b", Ty::VarChar, 2)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(raw.as_bytes());
    let v = unsafe { raw.get_ref_unchecked(0, 0) };
    dbg!(v);

    let raw = RawBlock::from_v2(
        &[1, 1, 0],
        &[
            Field::new("a", Ty::TinyInt, 1),
            Field::new("b", Ty::SmallInt, 2),
        ],
        &[1, 2],
        1,
        Precision::Millisecond,
    );
    dbg!(raw.len(), raw.as_bytes());
    let v = unsafe { raw.get_ref_unchecked(0, 0) };
    dbg!(v);
    let v = unsafe { raw.get_ref_unchecked(0, 1) };
    dbg!(v);
    let raw = RawBlock::from_v2(
        &[1, 2, 0, b'a', b'b'],
        &[
            Field::new("a", Ty::TinyInt, 1),
            Field::new("b", Ty::VarChar, 2),
        ],
        &[1, 4],
        1,
        Precision::Millisecond,
    );
    dbg!(raw.as_bytes());
    let v = unsafe { raw.get_ref_unchecked(0, 0) };
    dbg!(v);
    let v = unsafe { raw.get_ref_unchecked(0, 1) };
    dbg!(v);
}

#[test]
fn test_from_v2_raw() {
    let bytes = b"\x10\x86\x1aA \xcc)AB\xc2\x14AZ],A\xa2\x8d$A\x87\xb9%A\xf5~\x0fA\x96\xf7,AY\xee\x17A1|\x15As\x00\x00\x00q\x00\x00\x00s\x00\x00\x00t\x00\x00\x00u\x00\x00\x00t\x00\x00\x00n\x00\x00\x00n\x00\x00\x00n\x00\x00\x00r\x00\x00\x00";

    let block = RawBlock::from_v2(
        bytes.as_slice(),
        &[Field::new("a", Ty::Float, 4), Field::new("b", Ty::Int, 4)],
        &[4, 4],
        10,
        Precision::Millisecond,
    );
    dbg!(block);
}
#[test]
fn test_from_v2_meters_limit_10() {
    let bytes = include_bytes!("../../../tests/test.txt");

    let block = RawBlock::from_v2(
        bytes.as_slice(),
        &[
            Field::new("ts", Ty::Timestamp, 8),
            Field::new("current", Ty::Float, 4),
            Field::new("voltage", Ty::Int, 4),
            Field::new("phase", Ty::Float, 4),
            Field::new("group_id", Ty::Int, 4),
            Field::new("location", Ty::VarChar, 16),
        ],
        &[8, 4, 4, 4, 4, 18],
        10,
        Precision::Millisecond,
    );
    dbg!(block);
}
#[test]
fn test_null() {
    let float = unsafe { transmute::<u32, f32>(0x7FF00000) };
    assert!(float.is_nan());
}
