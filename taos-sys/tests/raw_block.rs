// todo: some const functions are not available for stable Rust.
#![feature(const_slice_from_raw_parts)]
#![feature(const_slice_index)]

use once_cell::unsync::OnceCell;
use taos_query::{
    common::{Column, Field, Timestamp, Ty, Value},
    BlockExt,
};
use taos_sys::Precision;

use core::slice;
use std::{
    fmt::Debug,
    mem::transmute,
    mem::{size_of, transmute_copy},
    ops::Deref,
};

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
    let bytes: [u8; 6] = unsafe { transmute_copy(&col) };
    dbg!(&bytes);

    let bytes: [u8; 6] = [4, 0, 1, 0, 0, 0];
    let col2: ColSchema = unsafe { transmute_copy(&bytes) };
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

pub struct RawBlock {
    data: Vec<u8>,
    len: usize,
    rows: usize,
    cols: usize,
    precision: Precision,
    offsets: Vec<(Ty, isize, isize)>,
}

impl RawBlock {
    pub unsafe fn copy_from_ptr(
        data: *const u8,
        rows: usize,
        cols: usize,
        precision: Precision,
    ) -> Self {
        let len = *transmute::<*const u8, *const u32>(data) as usize + 4;
        Self {
            data: slice::from_raw_parts(data, len).into(),
            len,
            rows,
            cols,
            precision,
            offsets: Vec::new(),
        }
    }
}

/// Raw data block format:
///
/// ```text,ignore
/// +--------------+----------+---------------+-----------+-----------------------+-----------------+
/// | total length | group id | col_schema... | length... | (bitmap or offsets    | col data)   ... |
/// |  4 bytes     | 8 bytes  | (2 + 4) * cols| 4 * cols  | (row+7)/8 or 4 * rows | length[col] ... |
/// +--------------+----------+---------------+-----------+-----------------------+-----------------+
/// ```
///
/// The length of bitmap is decided by number of rows of this data block, and the length of each column data is
/// recorded in the first segment, next to the struct header
#[derive(Debug)]
pub struct InnerBlock {
    data: *const u8,
    len: usize,
    rows: usize,
    cols: usize,
    precision: Precision,
    offsets: OnceCell<Vec<(Ty, isize, isize)>>,
}

impl InnerBlock {
    /// From raw data block.
    pub unsafe fn from_ptr(
        data: *const u8,
        rows: usize,
        cols: usize,
        precision: Precision,
    ) -> Self {
        let len = *transmute::<*const u8, *const u32>(data) as usize + 4;
        Self {
            data,
            len,
            rows,
            cols,
            precision,
            offsets: OnceCell::new(),
        }
    }

    /// The whole block slice length.
    pub const fn len(&self) -> usize {
        unsafe { *transmute::<*const u8, *const u32>(self.as_ptr()) as usize + 4 }
    }

    /// The group id of the raw block.
    pub const fn group_id(&self) -> u64 {
        unsafe { *std::mem::transmute::<*const u8, *const u64>(self.as_ptr()) }
    }

    /// Inner block as bytes slice.
    pub const fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.len) }
    }

    /// Raw data block bytes.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_owned()
    }

    /// Pointer to raw block data slice.
    const fn as_ptr(&self) -> *const u8 {
        self.data
    }

    /// Offset to column schema start position.
    const fn schema_offset(&self) -> isize {
        // 4 = block data length.
        // 8 = group id.
        4 + 8
    }

    /// Offset to lengths start position.
    const fn lengths_offset(&self) -> isize {
        // 6 == size_of::<ColumnSchema>()
        self.schema_offset() + self.cols as isize * 6
    }

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
    pub const fn get_type_of(&self, col: usize) -> Ty {
        self.get_schema_of(col).ty
    }

    /// Get column schema which includes data type and bytes length.
    #[inline]
    pub const fn get_schema_of(&self, col: usize) -> &ColSchema {
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
                    debug_assert!(self.len() as isize >= o1 + row as isize * 4 + len as isize);
                    let chars_len = len / 4;
                    let chars_ptr = ptr.offset(2) as *mut char;
                    let chars = slice::from_raw_parts(chars_ptr, chars_len as usize);
                    let json: String = chars.into_iter().collect();

                    serde_json::from_str(&json)
                        .ok()
                        .map(Value::Json)
                        .unwrap_or(Value::Null)
                }
            }
            ty => unreachable!("unsupported type: {ty}"),
        }
    }
}

#[test]
fn raw_block() -> Result<(), taos_error::Error> {
    use taos_sys::*;
    let taos = RawTaos::connect(
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        0,
    )?;
    let rs = taos.query("show databases")?;
    let fields = rs.fields();
    let precision = rs.precision();
    let field_count = rs.field_count();
    let (ptr, rows) = rs.fetch_raw_block()?;

    let inner = unsafe { InnerBlock::from_ptr(ptr, rows as _, field_count as _, precision) };
    let gid = inner.group_id();
    println!("group id: {gid}");

    for i in 0..field_count {
        let col = inner.get_schema_of(i as _);
        let field = &fields[i as usize];
        println!("{field:?}, {col:#x?}");
    }

    let schemas = inner.schemas();
    dbg!(schemas);
    let lengths = inner.lengths();
    dbg!(lengths);
    let offsets = inner.column_offsets();
    dbg!(offsets);

    dbg!(unsafe { inner.get_unchecked(0, field_count as usize - 1) });
    for row in 0..dbg!(rows) as usize {
        for col in 0..field_count as usize {
            println!("({row}, {col}): ");
            let v = unsafe { inner.get_unchecked(row, col) };
            dbg!(v);
        }
    }
    Ok(())
}

// impl BlockExt for InnerBlock {
//     fn num_of_rows(&self) -> usize {
//         todo!()
//     }

//     fn fields(&self) -> &[Field] {
//         todo!()
//     }

//     fn precision(&self) -> Precision {
//         todo!()
//     }

//     fn is_null(&self, row: usize, col: usize) -> bool {
//         todo!()
//     }

//     unsafe fn cell_unchecked(
//         &self,
//         row: usize,
//         col: usize,
//     ) -> (&Field, taos_query::common::BorrowedValue) {
//         todo!()
//     }

//     unsafe fn get_col_unchecked(&self, col: usize) -> taos_query::common::BorrowedColumn {
//         todo!()
//     }
// }

#[test]
fn inner_block() {
    use taos_query::common::Timestamp::Milliseconds;
    use taos_sys::Precision::Millisecond;
    let bytes = b"5\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\t\x00\x08\x00\x00\x00\x01\x00\x01\x00\x00\x00\x02\x00\x01\x00\x00\x00\x03\x00\x02\x00\x00\x00\x04\x00\x04\x00\x00\x00\x05\x00\x08\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x0c\x00\x02\x00\x00\x00\r\x00\x04\x00\x00\x00\x0e\x00\x08\x00\x00\x00\x06\x00\x04\x00\x00\x00\x07\x00\x08\x00\x00\x00\x08\x00f\x00\x00\x00\n\x00\x92\x01\x00\x00\x10\x00\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x05\x00\x00\x00\x16\x00\x00\x00\x00\xf2=\xc3u\x81\x01\x00\x00\xdaA\xc3u\x81\x01\x00\x00@\x01\x00@\xff\x00@\xff\xff\x00\x00@\xff\xff\xff\xff\x00\x00\x00\x00@\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00@\x01\x00@\x01\x00\x00\x00@\x01\x00\x00\x00\x00\x00\x00\x00@\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x03\x00abc\x00\x00\x00\x00\xff\xff\xff\xff\x14\x00\x9bm\x00\x00\x1d`\x00\x00\x1e\xd1\x01\x00pe\x00\x00nc\x00\x00\x00\x00\x00\x00";
    let rows = 2;
    let cols = 14;
    let precision = Millisecond;

    let block = unsafe { InnerBlock::from_ptr(bytes.as_ptr(), rows, cols, precision) };

    assert_eq!(block.as_bytes(), bytes);
    assert_eq!(block.len(), bytes.len());

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
}

#[test]
fn raw_block_full_test() -> Result<(), taos_error::Error> {
    use taos_sys::*;
    let taos = RawTaos::connect(
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        0,
    )?;

    let _ = taos.query("drop database if exists _rs_ts_raw_block_full_")?;
    let _ = taos.query("create database if not exists _rs_ts_raw_block_full_")?;
    let _ = taos.query("use _rs_ts_raw_block_full_")?;
    let _ = taos.query("create stable stb1 (ts timestamp,vb bool,vi8 tinyint,vi16 smallint,\
        vi32 int,vi64 bigint, vu8 tinyint unsigned,vu16 smallint unsigned,vu32 int unsigned,vu64 bigint unsigned,\
        vf float,vd double,vv varchar(100), vn nchar(100)) tags(tj json)")?;

    let _ = taos.query(
        "insert into tb1 using stb1 tags(NULL) values\
        (now,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
    )?;
    let _ = taos.query(
        "insert into tb2 using stb1 tags('{\"a\":\"涛思𝄞数据\"}') values\
        (now,true,-1,-1,-1,-1, 1,1,1,1,0.0,0.0,'abc', '涛思𝄞数据')\
        (now+1s,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
    )?;
    let rs = taos.query("select * from tb2 order by tbname")?;
    let fields = rs.fields();
    let precision = rs.precision();
    let field_count = rs.field_count();
    let (ptr, rows) = rs.fetch_raw_block()?;

    let inner = unsafe { InnerBlock::from_ptr(ptr, rows as _, field_count as _, precision) };
    let gid = inner.group_id();
    println!("group id: {gid}");

    use std::ascii::escape_default;

    pub fn show_buf<B: AsRef<[u8]>>(buf: B) -> String {
        String::from_utf8(
            buf.as_ref()
                .iter()
                .map(|b| escape_default(*b))
                .flatten()
                .collect(),
        )
        .unwrap()
    }

    dbg!(inner.len());
    dbg!(inner.as_bytes());
    let bytes = inner.to_vec();
    println!("{}", show_buf(bytes));

    for i in 0..field_count {
        let col = inner.get_schema_of(i as _);
        let field = &fields[i as usize];
        println!("{field:?}, {col:#x?}");
    }

    let schemas = inner.schemas();
    dbg!(schemas);
    let lengths = inner.lengths();
    dbg!(lengths);
    let offsets = inner.column_offsets();
    dbg!(offsets);

    dbg!(unsafe { inner.get_unchecked(0, field_count as usize - 1) });
    for row in 0..dbg!(rows) as usize {
        for col in 0..field_count as usize {
            println!("({row}, {col}): ");
            let v = unsafe { inner.get_unchecked(row, col) };

            dbg!(v);
        }
    }
    Ok(())
}

#[test]
fn char_size() {
    assert_eq!(size_of::<char>(), 4);
}
