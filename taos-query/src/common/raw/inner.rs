use crate::{
    common::{BorrowedValue, Column, Field, Precision, Timestamp, Ty, Value},
    util::{Inlinable, InlinableRead, InlinableWrite},
    BlockExt,
};
use once_cell::unsync::OnceCell;

use core::slice;
use std::{fmt::Debug, mem::size_of, mem::transmute};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
#[repr(packed(2))] // use packed(2) because it's int16_t in raw block.
pub struct ColSchema {
    ty: Ty,
    len: u32,
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
#[derive(Debug)]
pub struct RawBlock {
    data: *mut u8,
    len: usize,
    cap: usize, // usually same to len.
    rows: usize,
    cols: usize,
    precision: Precision,
    offsets: OnceCell<Vec<(Ty, isize, isize)>>,
}

impl Default for RawBlock {
    fn default() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: Default::default(),
            cap: Default::default(),
            rows: Default::default(),
            cols: Default::default(),
            precision: Precision::Millisecond,
            offsets: Default::default(),
        }
    }
}

impl Drop for RawBlock {
    #[inline]
    fn drop(&mut self) {
        unsafe { Vec::from_raw_parts(self.data, self.len, self.cap) };
    }
}

impl Inlinable for RawBlock {
    /// **NOTE**: raw block bytes is not enough to parse data from.
    /// You must call [with_rows](#method.with_rows)
    fn read_inlined<R: std::io::Read>(mut reader: R) -> std::io::Result<Self> {
        let bytes = reader.read_inlined_bytes::<4>()?;
        Ok(Self::new(bytes))
    }

    fn write_inlined<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        wtr.write_inlined_bytes::<4>(self.as_bytes())
    }
}
impl RawBlock {
    #[inline]
    pub fn new(mut vec: Vec<u8>) -> Self {
        let data = vec.as_mut_ptr();
        let len = vec.len();
        let cap = vec.capacity();
        assert_eq!(len, cap);
        std::mem::forget(vec);
        Self {
            data,
            len,
            cap,
            rows: Default::default(),
            cols: Default::default(),
            precision: Precision::Millisecond,
            offsets: Default::default(),
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
        let data = std::slice::from_raw_parts(data.offset(4), len).to_vec();
        Self::from_bytes(data, rows, cols, precision)
    }

    #[inline]
    /// Build inner block from bytes.
    pub fn from_bytes(mut vec: Vec<u8>, rows: usize, cols: usize, precision: Precision) -> Self {
        let data = vec.as_mut_ptr();
        let len = vec.len();
        let cap = vec.capacity();
        assert_eq!(len, cap);
        std::mem::forget(vec);
        Self {
            data,
            len,
            cap,
            rows,
            cols,
            precision,
            offsets: OnceCell::new(),
        }
    }

    #[inline]
    /// The whole block slice length.
    pub const fn len(&self) -> usize {
        self.len
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
    pub const fn group_id(&self) -> u64 {
        unsafe { *std::mem::transmute::<*const u8, *const u64>(self.as_ptr()) }
    }

    /// Inner block as bytes slice.
    #[inline]
    #[rustversion::attr(nightly, const)]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }

    #[inline]
    /// Raw data block bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_owned()
    }

    #[inline]
    /// Raw data into block bytes.
    pub fn into_vec(self) -> Vec<u8> {
        unsafe { Vec::from_raw_parts(self.data, self.len, self.cap) }
    }

    #[inline]
    /// Pointer to raw block data slice.
    const fn as_ptr(&self) -> *const u8 {
        self.data
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

    /// Pointer to specific offset.
    const unsafe fn offset(&self, count: isize) -> *const u8 {
        self.as_ptr().offset(count)
    }

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
            self.schemas()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    assert!(data_offset < self.len() as isize);
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

                        assert!(data_offset < self.len() as isize);
                        o
                    }
                })
                .collect()
        })
    }

    /// Column schema extractor.
    #[inline]
    pub const fn schemas(&self) -> &[ColSchema] {
        unsafe {
            let ptr = self.offset(self.schema_offset());
            slice::from_raw_parts(ptr as *mut ColSchema, self.cols)
        }
    }

    /// Get column data type.
    #[inline]
    #[rustversion::attr(nightly, const)]
    pub fn get_type_of(&self, col: usize) -> Ty {
        self.get_schema_of(col).ty
    }

    /// Get column schema which includes data type and bytes length.
    #[inline]
    #[rustversion::attr(nightly, const)]
    pub fn get_schema_of(&self, col: usize) -> &ColSchema {
        unsafe { self.schemas().get_unchecked(col) }
    }

    #[inline]
    /// Lengths for each column raw data.
    const fn lengths(&self) -> &[i32] {
        unsafe {
            let ptr = self.offset(self.lengths_offset());
            slice::from_raw_parts(ptr as *mut i32, self.cols)
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
            ty => unreachable!("unsupported type: {ty}"),
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
