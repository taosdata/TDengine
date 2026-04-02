#![allow(clippy::size_of_in_element_count)]
#![allow(clippy::type_complexity)]

use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::ffi::c_void;
use std::fmt::{Debug, Display};
use std::io::Cursor;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::AtomicU32;

use bytes::Bytes;
use chrono_tz::Tz;
pub use data::*;
use derive_builder::Builder;
use itertools::Itertools;
use layout::Layout;
pub use meta::*;
use rayon::prelude::*;
pub use rows::*;
use serde::Deserialize;
use taos_error::Error;
pub use views::ColumnView;
use views::*;

use crate::common::{BorrowedValue, Field, Precision, Ty, Value};

pub mod layout;
pub mod meta;
#[allow(clippy::missing_safety_doc)]
#[allow(clippy::should_implement_trait)]
pub mod views;

mod data;
mod rows;

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
struct Header {
    version: u32,
    length: u32,
    nrows: u32,
    ncols: u32,
    flag: u32,
    group_id: u64,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            version: 1,
            length: Default::default(),
            nrows: Default::default(),
            ncols: Default::default(),
            flag: u32::MAX,
            group_id: Default::default(),
        }
    }
}

impl Header {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            let ptr = self as *const Self;
            let len = std::mem::size_of::<Self>();
            std::slice::from_raw_parts(ptr as *const u8, len)
        }
    }

    fn len(&self) -> usize {
        self.length as _
    }

    fn nrows(&self) -> usize {
        self.nrows as _
    }

    fn ncols(&self) -> usize {
        self.ncols as _
    }
}

/// Raw data block format (B for bytes):
///
/// ```text,ignore
/// +-----+----------+---------------+-----------+-----------------------+-----------------+
/// | header | col_schema... | length... | (bitmap or offsets    | col data)   ... |
/// | 28 B   | (1+4)B * cols | 4B * cols | (row+7)/8 or 4 * rows | length[col] ... |
/// +-----+----------+---------------+-----------+-----------------------+-----------------+
/// ```
///
/// The length of bitmap is decided by number of rows of this data block, and the length of each column data is
/// recorded in the first segment, next to the struct header
pub struct RawBlock {
    /// Layout is auto detected.
    layout: Rc<AtomicU32>,
    /// Raw bytes version, may be v2 or v3.
    version: Version,
    /// Data is required, which could be v2 websocket block or a v3 raw block.
    data: Cell<Bytes>,
    /// Number of rows in current data block.
    rows: usize,
    /// Number of columns (or fields) in current data block.
    cols: usize,
    /// Timestamp precision in current data block.
    precision: Precision,
    /// Database name, if is tmq message.
    database: Option<String>,
    /// Table name of current data block.
    table: Option<String>,
    /// Field names of current data block.
    fields: Vec<String>,
    /// Group id in current data block, it always be 0 in v2 block, and be meaningful in v3.
    group_id: u64,
    /// Column schemas of current data block, contains only data type and the length defined in `create table`.
    schemas: Schemas,
    /// Data lengths collection for all columns.
    lengths: Lengths,
    /// A vector of [ColumnView] that represent column of values efficiently.
    columns: Vec<ColumnView>,
    /// Time zone for the block, if applicable.
    tz: Option<Tz>,
}

unsafe impl Send for RawBlock {}
unsafe impl Sync for RawBlock {}

impl Debug for RawBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // todo: more helpful debug impl.
        f.debug_struct("Raw")
            .field("layout", &self.layout)
            .field("version", &self.version)
            .field("data", &"...")
            .field("rows", &self.rows)
            .field("cols", &self.cols)
            .field("precision", &self.precision)
            .field("table", &self.table)
            .field("fields", &self.fields)
            .field("group_id", &self.group_id)
            .field("schemas", &self.schemas)
            .field("lengths", &self.lengths)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl RawBlock {
    pub fn from_views(views: &[ColumnView], precision: Precision) -> Self {
        Self::parse_from_raw_block(views_to_raw_block(views), precision)
    }

    /// # Safety
    ///
    /// When using this with correct TDengine 3.x raw block pointer, it's safe.
    pub unsafe fn parse_from_ptr(ptr: *mut c_void, precision: Precision) -> Self {
        let header = &*(ptr as *const Header);
        let len = header.length as usize;
        let bytes = std::slice::from_raw_parts(ptr as *const u8, len);
        let bytes = Bytes::from(bytes.to_vec());
        Self::parse_from_raw_block(bytes, precision).with_layout(Layout::default())
    }

    /// # Safety
    ///
    /// When using this with correct TDengine 2.x block pointer, it's safe.
    #[allow(clippy::needless_range_loop)]
    pub unsafe fn parse_from_ptr_v2(
        ptr: *const *const c_void,
        fields: &[Field],
        lengths: &[u32],
        rows: usize,
        precision: Precision,
    ) -> Self {
        let mut bytes = Vec::new();
        for i in 0..fields.len() {
            let slice = ptr.add(i).read();
            bytes.extend_from_slice(std::slice::from_raw_parts(
                slice as *const u8,
                lengths[i] as usize * rows,
            ));
        }
        Self::parse_from_raw_block_v2(bytes, fields, lengths, rows, precision)
    }

    pub fn parse_from_raw_block_v2<T: Into<Bytes>>(
        bytes: T,
        fields: &[Field],
        lengths: &[u32],
        rows: usize,
        precision: Precision,
    ) -> Self {
        use bytes::BufMut;
        debug_assert_eq!(fields.len(), lengths.len());

        #[inline(always)]
        fn bool_is_null(v: *const bool) -> bool {
            unsafe { (v as *const u8).read_unaligned() == 0x02 }
        }
        #[inline(always)]
        fn tiny_int_is_null(v: *const i8) -> bool {
            unsafe { (v as *const u8).read_unaligned() == 0x80 }
        }
        #[inline(always)]
        fn small_int_is_null(v: *const i16) -> bool {
            unsafe { (v as *const u16).read_unaligned() == 0x8000 }
        }
        #[inline(always)]
        fn int_is_null(v: *const i32) -> bool {
            unsafe { (v as *const u32).read_unaligned() == 0x80000000 }
        }
        #[inline(always)]
        fn big_int_is_null(v: *const i64) -> bool {
            unsafe { (v as *const u64).read_unaligned() == 0x8000000000000000 }
        }
        #[inline(always)]
        fn u_tiny_int_is_null(v: *const u8) -> bool {
            unsafe { v.read_unaligned() == 0xFF }
        }
        #[inline(always)]
        fn u_small_int_is_null(v: *const u16) -> bool {
            unsafe { v.read_unaligned() == 0xFFFF }
        }
        #[inline(always)]
        fn u_int_is_null(v: *const u32) -> bool {
            unsafe { v.read_unaligned() == 0xFFFFFFFF }
        }
        #[inline(always)]
        fn u_big_int_is_null(v: *const u64) -> bool {
            unsafe { v.read_unaligned() == 0xFFFFFFFFFFFFFFFF }
        }
        #[inline(always)]
        fn float_is_null(v: *const f32) -> bool {
            unsafe { (v as *const u32).read_unaligned() == 0x7FF00000 }
        }
        #[inline(always)]
        fn double_is_null(v: *const f64) -> bool {
            unsafe { (v as *const u64).read_unaligned() == 0x7FFFFF0000000000 }
        }

        let layout = Rc::new(AtomicU32::new(
            Layout::INLINE_DEFAULT.with_schema_changed().as_inner(),
        ));

        let bytes = bytes.into();
        let cols = fields.len();

        let mut schemas_bytes =
            bytes::BytesMut::with_capacity(cols * std::mem::size_of::<DataType>());
        for field in fields.iter() {
            schemas_bytes.put(field.to_column_schema().as_bytes());
        }
        let schemas = Schemas::from(schemas_bytes);

        let mut data_lengths = LengthsMut::new(cols);

        let mut columns = Vec::new();

        let mut offset = 0;

        for (i, (field, length)) in fields.iter().zip(lengths).enumerate() {
            macro_rules! _primitive_view {
                ($ty:ident, $prim:ty) => {{
                    debug_assert_eq!(field.bytes(), *length);
                    // column start
                    let start = offset;
                    // column end
                    offset += rows * std::mem::size_of::<$prim>() as usize;
                    // byte slice from start to end: `[start, end)`.
                    let data = bytes.slice(start..offset);
                    let nulls = NullBits::from_iter((0..rows).map(|row| unsafe {
                        paste::paste! { [<$ty:snake _is_null>] (
                            data
                                .as_ptr()
                                .offset(row as isize * std::mem::size_of::<$prim>() as isize)
                                as *const $prim,
                        ) }
                    }));

                    // Set data lengths for v3-compatible block.
                    data_lengths[i] = data.len() as u32;

                    // build column view
                    let column = paste::paste! { ColumnView::$ty([<$ty View>] { nulls, data }) };
                    columns.push(column);
                }};
            }

            match field.ty() {
                Ty::Null => unreachable!(),
                Ty::Bool => _primitive_view!(Bool, bool),
                Ty::TinyInt => _primitive_view!(TinyInt, i8),
                Ty::SmallInt => _primitive_view!(SmallInt, i16),
                Ty::Int => _primitive_view!(Int, i32),
                Ty::BigInt => _primitive_view!(BigInt, i64),
                Ty::UTinyInt => _primitive_view!(UTinyInt, u8),
                Ty::USmallInt => _primitive_view!(USmallInt, u16),
                Ty::UInt => _primitive_view!(UInt, u32),
                Ty::UBigInt => _primitive_view!(UBigInt, u64),
                Ty::Float => _primitive_view!(Float, f32),
                Ty::Double => _primitive_view!(Double, f64),
                Ty::VarChar => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = (ptr as *const u16).read_unaligned();
                        if len == 1 && *ptr.offset(2) == 0xFF {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(ColumnView::VarChar(VarCharView { offsets, data }));

                    data_lengths[i] = *length * rows as u32;
                }
                Ty::Timestamp => {
                    // column start
                    let start = offset;
                    // column end
                    offset += rows * std::mem::size_of::<i64>();
                    // byte slice from start to end: `[start, end)`.
                    let data = bytes.slice(start..offset);
                    let nulls = (0..rows)
                        .map(|row| unsafe {
                            big_int_is_null(
                                &(data
                                    .as_ptr()
                                    .offset(row as isize * std::mem::size_of::<i64>() as isize)
                                    as *const i64)
                                    .read_unaligned() as _,
                            )
                        })
                        .collect();

                    // Set data lengths for v3-compatible block.
                    data_lengths[i] = data.len() as u32;

                    // build column view
                    let column = ColumnView::Timestamp(TimestampView {
                        nulls,
                        data,
                        precision,
                    });
                    columns.push(column);
                }
                Ty::NChar => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = (ptr as *const u16).read_unaligned();
                        if len == 4 && (ptr.offset(2) as *const u32).read_unaligned() == 0xFFFFFFFF
                        {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(ColumnView::NChar(NCharView {
                        offsets,
                        data,
                        is_chars: UnsafeCell::new(false),
                        version: Version::V2,
                        layout: layout.clone(),
                    }));

                    data_lengths[i] = *length * rows as u32;
                }
                Ty::Json => {
                    let start = offset;
                    offset += *length as usize * rows;
                    let data = bytes.slice(start..offset);
                    let data_ptr = data.as_ptr();

                    let offsets = Offsets::from_offsets((0..rows).map(|row| unsafe {
                        let offset = row as i32 * *length as i32;
                        let ptr = data_ptr.offset(offset as isize);
                        let len = (ptr as *const u16).read_unaligned();
                        if len == 4 && (ptr.offset(2) as *const u32).read_unaligned() == 0xFFFFFFFF
                        {
                            -1
                        } else {
                            offset
                        }
                    }));

                    columns.push(ColumnView::Json(JsonView { offsets, data }));

                    data_lengths[i] = *length * rows as u32;
                }
                _ => todo!(),
            }
        }

        Self {
            layout,
            version: Version::V2,
            data: Cell::new(bytes),
            rows,
            cols,
            schemas,
            lengths: data_lengths.into_lengths(),
            precision,
            database: None,
            table: None,
            fields: fields.iter().map(|s| s.name().to_string()).collect(),
            columns,
            group_id: 0,
            tz: None,
        }
    }

    pub fn parse_from_raw_block<T: Into<Bytes>>(bytes: T, precision: Precision) -> Self {
        let schema_start: usize = std::mem::size_of::<Header>();

        let layout = Rc::new(AtomicU32::new(Layout::INLINE_DEFAULT.as_inner()));

        let bytes = bytes.into();
        let ptr = bytes.as_ptr();

        let header = unsafe { &*(ptr as *const Header) };

        let rows = header.nrows();
        let cols = header.ncols();
        let len = header.len();
        debug_assert_eq!(bytes.len(), len);
        let group_id = header.group_id;

        let schema_end = schema_start + cols * std::mem::size_of::<DataType>();
        let schemas = Schemas::from(bytes.slice(schema_start..schema_end));

        let lengths_end = schema_end + std::mem::size_of::<u32>() * cols;
        let lengths = Lengths::from(bytes.slice(schema_end..lengths_end));

        let mut data_offset = lengths_end;
        let mut columns = Vec::with_capacity(cols);
        for col in 0..cols {
            let length = unsafe { lengths.get_unchecked(col) } as usize;
            let schema = unsafe { schemas.get_unchecked(col) };

            macro_rules! _primitive_value {
                ($ty:ident, $prim:ty) => {{
                    let o1 = data_offset;
                    let o2 = data_offset + ((rows + 7) >> 3); // bitmap len
                    data_offset = o2 + rows * std::mem::size_of::<$prim>();
                    let nulls = bytes.slice(o1..o2);
                    let data = bytes.slice(o2..data_offset);
                    ColumnView::$ty(paste::paste! {[<$ty View>] {
                        nulls: NullBits(nulls),
                        data,
                    }})
                }};
            }

            macro_rules! _variable_value {
                ($ty:ident) => {{
                    let o1 = data_offset;
                    let o2 = data_offset + std::mem::size_of::<i32>() * rows;
                    data_offset = o2 + length;
                    let offsets = Offsets::from(bytes.slice(o1..o2));
                    let data = bytes.slice(o2..data_offset);
                    ColumnView::$ty(paste::paste! {[<$ty View>] {
                        offsets,
                        data,
                    }})
                }};
            }

            macro_rules! _decimal_value {
                ($ty:expr, $prim:ty) => {{
                    let o1 = data_offset;
                    let o2 = data_offset + ((rows + 7) >> 3); // null bitmap len.
                    data_offset = o2 + rows * std::mem::size_of::<$prim>();
                    let nulls = bytes.slice(o1..o2);
                    let data = bytes.slice(o2..data_offset);
                    $ty(DecimalView::new(
                        NullBits(nulls),
                        data,
                        schema.as_prec_scale_unchecked(),
                    ))
                }};
            }

            let column = match schema.ty() {
                Ty::Null => unreachable!("raw block does not contains type NULL"),
                Ty::Bool => _primitive_value!(Bool, i8),
                Ty::TinyInt => _primitive_value!(TinyInt, i8),
                Ty::SmallInt => _primitive_value!(SmallInt, i16),
                Ty::Int => _primitive_value!(Int, i32),
                Ty::BigInt => _primitive_value!(BigInt, i64),
                Ty::UTinyInt => _primitive_value!(UTinyInt, u8),
                Ty::USmallInt => _primitive_value!(USmallInt, u16),
                Ty::UInt => _primitive_value!(UInt, u32),
                Ty::UBigInt => _primitive_value!(UBigInt, u64),
                Ty::Float => _primitive_value!(Float, f32),
                Ty::Double => _primitive_value!(Double, f64),
                Ty::VarChar => _variable_value!(VarChar),
                Ty::VarBinary => _variable_value!(VarBinary),
                Ty::Geometry => _variable_value!(Geometry),
                Ty::Json => _variable_value!(Json),
                Ty::Blob => _variable_value!(Blob),
                Ty::Decimal => _decimal_value!(ColumnView::Decimal, i128),
                Ty::Decimal64 => _decimal_value!(ColumnView::Decimal64, i64),
                Ty::Timestamp => {
                    let o1 = data_offset;
                    let o2 = data_offset + ((rows + 7) >> 3);
                    data_offset = o2 + rows * std::mem::size_of::<i64>();
                    let nulls = bytes.slice(o1..o2);
                    let data = bytes.slice(o2..data_offset);
                    ColumnView::Timestamp(TimestampView {
                        nulls: NullBits(nulls),
                        data,
                        precision,
                    })
                }
                Ty::NChar => {
                    let o1 = data_offset;
                    let o2 = data_offset + std::mem::size_of::<i32>() * rows;
                    data_offset = o2 + length;
                    let offsets = Offsets::from(bytes.slice(o1..o2));
                    let data = bytes.slice(o2..data_offset);
                    ColumnView::NChar(NCharView {
                        offsets,
                        data,
                        is_chars: UnsafeCell::new(true),
                        version: Version::V3,
                        layout: layout.clone(),
                    })
                }
                ty => unreachable!("unsupported type: {ty}"),
            };
            columns.push(column);
            debug_assert!(data_offset <= len);
        }

        RawBlock {
            layout,
            version: Version::V3,
            data: Cell::new(bytes),
            rows,
            cols,
            precision,
            group_id,
            schemas,
            lengths,
            database: None,
            table: None,
            fields: Vec::new(),
            columns,
            tz: None,
        }
    }

    pub fn parse_from_multi_raw_block<T: Into<Bytes>>(
        bytes: T,
    ) -> Result<VecDeque<Self>, std::io::Error> {
        use byteorder::ReadBytesExt;

        let mut cursor = MultiBlockCursor::new(Cursor::new(bytes.into()));
        let code = cursor.read_i32::<byteorder::LittleEndian>()?;
        let message = cursor.get_str()?;
        if code != 0 {
            return Err(std::io::Error::other(message));
        }

        cursor.read_u64::<byteorder::LittleEndian>()?; //skip msg id
        cursor.read_u16::<byteorder::LittleEndian>()?; //skip meta type
        cursor.read_u32::<byteorder::LittleEndian>()?; //skip raw block length

        // skip header
        cursor.skip_head()?;

        let block_num = cursor.read_i32::<byteorder::LittleEndian>()?;
        let with_table_name: bool = cursor.read_u8()? != 0; // withTableName
        cursor.read_u8()?; // skip withSchema, always true

        let mut raw_block_queue = VecDeque::with_capacity(block_num as usize);
        for _ in 0..block_num {
            let block_total_len = cursor.parse_variable_byte_integer()?;
            let cur_position = cursor.position() + 17;
            cursor.set_position(cur_position);
            let precision = cursor.read_u8()?;

            // parse block data
            let block_start_pos = cursor.position() as usize;
            let block_end_pos = cursor.position() + block_total_len as u64 - 18;

            let bytes = cursor
                .get_ref()
                .slice(block_start_pos..block_end_pos as usize);
            let mut raw_block = RawBlock::parse_from_raw_block(bytes, precision.into());

            cursor.set_position(block_end_pos);

            // parse block schema
            let cols = cursor.parse_zigzag_variable_byte_integer()?;
            cursor.parse_zigzag_variable_byte_integer()?; // skip version

            let mut fields = Vec::new();
            for _ in 0..cols {
                let field = cursor.parse_schema()?;
                fields.push(field);
            }
            if with_table_name {
                raw_block.with_table_name(cursor.parse_name()?);
            }
            raw_block.with_field_names(fields.iter());
            raw_block_queue.push_back(raw_block);
        }

        Ok(raw_block_queue)
    }

    /// Set table name of the block
    pub fn with_database_name<T: Into<String>>(&mut self, name: T) -> &mut Self {
        self.database = Some(name.into());
        self
    }

    /// Set table name of the block
    pub fn with_table_name<T: Into<String>>(&mut self, name: T) -> &mut Self {
        self.table = Some(name.into());
        unsafe { &mut *(self.layout.as_ptr() as *mut Layout) }.with_table_name();

        self
    }

    /// Set field names of the block
    pub fn with_field_names<S: Display, I: IntoIterator<Item = S>>(
        &mut self,
        names: I,
    ) -> &mut Self {
        self.fields = names.into_iter().map(|name| name.to_string()).collect();
        unsafe { &mut *(self.layout.as_ptr() as *mut Layout) }.with_field_names();
        self
    }

    pub fn with_timezone(mut self, tz: Option<Tz>) -> Self {
        self.tz = tz;
        self
    }

    fn with_layout(mut self, layout: Layout) -> Self {
        self.layout = Rc::new(AtomicU32::new(layout.as_inner()));
        self
    }

    #[inline]
    pub fn timezone(&self) -> Option<Tz> {
        self.tz
    }

    /// Number of columns
    #[inline]
    pub fn ncols(&self) -> usize {
        self.columns.len()
    }

    /// Number of rows
    #[inline]
    pub fn nrows(&self) -> usize {
        self.rows
    }

    /// Precision for current block.
    #[inline]
    pub const fn precision(&self) -> Precision {
        self.precision
    }

    #[inline]
    pub const fn group_id(&self) -> u64 {
        self.group_id
    }

    #[inline]
    pub fn table_name(&self) -> Option<&str> {
        self.table.as_deref()
    }

    #[inline]
    pub fn tmq_db_name(&self) -> Option<&str> {
        self.database.as_deref()
    }

    #[inline]
    pub fn schemas(&self) -> &[DataType] {
        &self.schemas
    }

    /// Get field names.
    pub fn field_names(&self) -> &[String] {
        &self.fields
    }

    /// Data view in columns.
    #[inline]
    pub fn columns(&self) -> std::slice::Iter<'_, ColumnView> {
        self.columns.iter()
    }

    pub fn column_views(&self) -> &[ColumnView] {
        &self.columns
    }

    /// Data view in rows.
    #[inline]
    pub fn rows<'a>(&self) -> RowsIter<'a> {
        RowsIter {
            raw: NonNull::new(self as *const Self as *mut Self).unwrap(),
            row: 0,
            tz: self.tz,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn into_rows<'a>(self) -> IntoRowsIter<'a>
    where
        Self: 'a,
    {
        let tz = self.tz;
        IntoRowsIter {
            raw: self,
            row: 0,
            tz,
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn deserialize<'de, 'a: 'de, T>(
        &'a self,
    ) -> std::iter::Map<rows::RowsIter<'a>, fn(RowView<'a>) -> Result<T, DeError>>
    where
        T: Deserialize<'de>,
    {
        self.rows().map(|mut row| T::deserialize(&mut row))
    }

    pub fn as_raw_bytes(&self) -> &[u8] {
        let mut layout = self.layout();
        if layout.schema_changed() {
            let bytes = views_to_raw_block(&self.columns);
            let bytes = bytes.into();
            self.data.replace(bytes);
            layout.set_schema_changed(false);
            self.set_layout(layout);
        }
        unsafe { &(*self.data.as_ptr()) }
    }

    pub fn is_null(&self, row: usize, col: usize) -> bool {
        if row >= self.nrows() || col >= self.ncols() {
            return true;
        }
        unsafe { self.columns.get_unchecked(col).is_null_unchecked(row) }
    }

    pub fn is_null_by_col_unchecked(&self, mut rows: usize, col: usize) -> Vec<bool> {
        if rows > self.nrows() {
            rows = self.nrows();
        }
        let mut is_nulls = Vec::with_capacity(rows);
        for row in 0..rows {
            is_nulls.push(self.is_null(row, col));
        }
        is_nulls
    }

    pub fn get_col_data_offset_unchecked(&self, col: usize) -> Vec<i32> {
        let view = unsafe { self.columns.get_unchecked(col) };
        match view {
            ColumnView::VarChar(view) => {
                let len = view.offsets.len();
                let mut offsets = Vec::with_capacity(len);
                for i in 0..len {
                    let offset = unsafe { view.offsets.get_unchecked(i) };
                    offsets.push(offset);
                }
                offsets
            }
            ColumnView::NChar(view) => {
                let len = view.offsets.len();
                let mut offsets = Vec::with_capacity(len);
                for i in 0..len {
                    let offset = unsafe { view.offsets.get_unchecked(i) };
                    offsets.push(offset);
                }
                offsets
            }
            ColumnView::Json(view) => {
                let len = view.offsets.len();
                let mut offsets = Vec::with_capacity(len);
                for i in 0..len {
                    let offset = unsafe { view.offsets.get_unchecked(i) };
                    offsets.push(offset);
                }
                offsets
            }
            ColumnView::VarBinary(view) => {
                let len = view.offsets.len();
                let mut offsets = Vec::with_capacity(len);
                for i in 0..len {
                    let offset = unsafe { view.offsets.get_unchecked(i) };
                    offsets.push(offset);
                }
                offsets
            }
            ColumnView::Geometry(view) => {
                let len = view.offsets.len();
                let mut offsets = Vec::with_capacity(len);
                for i in 0..len {
                    let offset = unsafe { view.offsets.get_unchecked(i) };
                    offsets.push(offset);
                }
                offsets
            }
            ColumnView::Blob(view) => {
                let len = view.offsets.len();
                let mut offsets = Vec::with_capacity(len);
                for i in 0..len {
                    let offset = unsafe { view.offsets.get_unchecked(i) };
                    offsets.push(offset);
                }
                offsets
            }
            _ => vec![],
        }
    }

    /// Get one value at `(row, col)` of the block.
    ///
    /// # Safety
    ///
    /// `(row, col)` should not exceed the block limit.
    #[inline]
    pub unsafe fn get_raw_value_unchecked(
        &self,
        row: usize,
        col: usize,
    ) -> (Ty, u32, *const c_void) {
        let view = self.columns.get_unchecked(col);
        view.get_raw_value_unchecked(row)
    }

    pub fn get_ref(&self, row: usize, col: usize) -> Option<BorrowedValue<'_>> {
        if row >= self.nrows() || col >= self.ncols() {
            return None;
        }
        Some(unsafe { self.get_ref_unchecked(row, col) })
    }

    /// Get one value at `(row, col)` of the block.
    ///
    /// # Safety
    ///
    /// Ensure that `row` and `col` not exceed the limit of the block.
    #[inline]
    pub unsafe fn get_ref_unchecked(&self, row: usize, col: usize) -> BorrowedValue<'_> {
        self.columns.get_unchecked(col).get_ref_unchecked(row)
    }

    pub fn to_values(&self) -> Vec<Vec<Value>> {
        self.rows().map(RowView::into_values).collect_vec()
    }

    pub fn write<W: std::io::Write>(&self, _wtr: W) -> std::io::Result<usize> {
        todo!()
    }

    fn layout(&self) -> Layout {
        unsafe { *(self.layout.as_ptr() as *mut Layout) }
    }

    fn set_layout(&self, layout: Layout) {
        unsafe { *(self.layout.as_ptr() as *mut Layout) = layout }
    }

    pub fn fields(&self) -> Vec<Field> {
        self.schemas()
            .iter()
            .zip(self.field_names())
            .map(|(schema, name)| Field::new(name, schema.ty(), schema.len()))
            .collect_vec()
    }

    pub fn pretty_format(&self) -> PrettyBlock<'_> {
        PrettyBlock::new(self)
    }

    pub fn to_create(&self) -> Option<MetaCreate> {
        self.table_name().map(|table_name| MetaCreate::Normal {
            table_name: table_name.to_string(),
            columns: self.fields().into_iter().map(Into::into).collect(),
        })
    }

    /// Concatenate two blocks into one by rows.
    ///
    /// # Panics
    ///
    /// If the two blocks have different schemas.
    pub fn concat(&self, rhs: &Self) -> Self {
        debug_assert_eq!(self.ncols(), rhs.ncols());
        #[cfg(debug_assertions)]
        {
            for (l, r) in self.fields().into_iter().zip(rhs.fields()) {
                debug_assert_eq!(l, r);
            }
        }
        let fields = self.field_names();
        let views = rhs
            .column_views()
            .iter()
            .zip(self.column_views())
            .collect_vec()
            .into_par_iter()
            .map(|(r, l)| r.concat_strictly(l))
            .collect::<Vec<_>>();
        let mut block = Self::from_views(&views, self.precision());
        block.with_field_names(fields);
        if let Some(table_name) = self.table_name().or(rhs.table_name()) {
            block.with_table_name(table_name);
        }
        block
    }

    /// Cast the block into a new block with new precision.
    pub fn cast_precision(&self, precision: Precision) -> Self {
        let views = self
            .column_views()
            .iter()
            .map(|view| view.cast_precision(precision))
            .collect::<Vec<ColumnView>>();
        let mut block = Self::from_views(&views, precision);
        block.with_field_names(self.field_names());
        if let Some(table_name) = self.table_name() {
            block.with_table_name(table_name);
        }
        block
    }
}

impl std::ops::Add for RawBlock {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        self.concat(&rhs)
    }
}

impl std::ops::Add for &RawBlock {
    type Output = RawBlock;

    fn add(self, rhs: Self) -> Self::Output {
        self.concat(rhs)
    }
}

pub struct PrettyBlock<'a> {
    raw: &'a RawBlock,
}

impl<'a> PrettyBlock<'a> {
    fn new(raw: &'a RawBlock) -> Self {
        Self { raw }
    }
}

impl Deref for PrettyBlock<'_> {
    type Target = RawBlock;
    fn deref(&self) -> &Self::Target {
        self.raw
    }
}

impl Display for PrettyBlock<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use prettytable::{Row, Table};
        let mut table = Table::new();
        if let Some(table) = self.table_name() {
            writeln!(
                f,
                "Table view with {} rows, {} columns, table name \"{}\"",
                self.nrows(),
                self.ncols(),
                table
            )?;
        } else {
            writeln!(
                f,
                "Table view with {} rows, {} columns",
                self.nrows(),
                self.ncols(),
            )?;
        }
        table.set_titles(Row::from_iter(self.field_names()));
        let types: Row = self
            .column_views()
            .iter()
            .map(|view| view.schema().to_string())
            .collect();
        table.add_row(types);
        let nrows = self.nrows();
        const MAX_DISPLAY_ROWS: usize = 10;
        let mut rows_iter = self.raw.rows();
        if f.alternate() {
            for row in rows_iter {
                table.add_row(row.map(|s| s.1.to_string().unwrap_or_default()).collect());
            }
        } else if nrows > 2 * MAX_DISPLAY_ROWS {
            for row in (&mut rows_iter).take(MAX_DISPLAY_ROWS) {
                table.add_row(row.map(|s| s.1.to_string().unwrap_or_default()).collect());
            }
            table.add_row(std::iter::repeat_n("...", self.ncols()).collect());
            for row in rows_iter.skip(nrows - 2 * MAX_DISPLAY_ROWS) {
                table.add_row(row.map(|s| s.1.to_string().unwrap_or_default()).collect());
            }
        } else {
            for row in rows_iter {
                table.add_row(row.map(|s| s.1.to_string().unwrap_or_default()).collect());
            }
        }

        f.write_fmt(format_args!("{table}"))?;
        Ok(())
    }
}

struct InlineBlock(Bytes);

impl From<InlineBlock> for Bytes {
    fn from(raw: InlineBlock) -> Self {
        raw.0
    }
}

impl crate::prelude::sync::Inlinable for InlineBlock {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        use crate::prelude::sync::InlinableRead;
        let version = reader.read_u32()?;
        let len = reader.read_u32()?;
        let mut bytes = vec![0; len as usize];
        unsafe {
            std::ptr::copy_nonoverlapping(
                version.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr(),
                std::mem::size_of::<u32>(),
            );
            std::ptr::copy_nonoverlapping(
                len.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr().offset(4),
                std::mem::size_of::<u32>(),
            );
        }
        let buf = &mut bytes[8..];
        reader.read_exact(buf)?;
        Ok(Self(bytes.into()))
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        wtr.write_all(self.0.as_ref())?;
        Ok(self.0.len())
    }
}

#[async_trait::async_trait]
impl crate::prelude::AsyncInlinable for InlineBlock {
    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        use tokio::io::*;

        let version = reader.read_u32_le().await?;
        let len = reader.read_u32_le().await?;
        let mut bytes = vec![0; len as usize];
        unsafe {
            std::ptr::copy_nonoverlapping(
                version.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr(),
                std::mem::size_of::<u32>(),
            );
            std::ptr::copy_nonoverlapping(
                len.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr().offset(4),
                std::mem::size_of::<u32>(),
            );
        }
        let buf = &mut bytes[8..];
        reader.read_exact(buf).await?;
        Ok(Self(bytes.into()))
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        use tokio::io::*;
        wtr.write_all(self.0.as_ref()).await?;
        Ok(self.0.len())
    }
}

impl crate::prelude::sync::Inlinable for RawBlock {
    fn read_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        use crate::prelude::sync::InlinableRead;
        let layout = reader.read_u32()?;
        let layout = Layout::from_bits(layout).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid layout bits in raw block",
            )
        })?;

        let precision = layout.precision();
        let raw: InlineBlock = reader.read_inlinable()?;
        let mut raw = Self::parse_from_raw_block(raw.0, precision);
        let cols = raw.ncols();

        if layout.expect_table_name() {
            let name = reader.read_inlined_str::<2>()?;
            raw.with_table_name(name);
        }
        if layout.expect_field_names() {
            let names: Vec<_> = (0..cols)
                .map(|_| reader.read_inlined_str::<1>())
                .try_collect()?;
            raw.with_field_names(names);
        }

        Ok(raw)
    }

    fn read_optional_inlined<R: std::io::Read>(reader: &mut R) -> std::io::Result<Option<Self>> {
        use crate::prelude::sync::InlinableRead;
        let layout = reader.read_u32()?;
        if layout == 0xFFFFFFFF {
            return Ok(None);
        }
        let layout = Layout::from_bits(layout).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid layout bits in raw block",
            )
        })?;

        let precision = layout.precision();
        let raw: InlineBlock = reader.read_inlinable()?;
        let mut raw = Self::parse_from_raw_block(raw.0, precision);

        if layout.expect_table_name() {
            let name = reader.read_inlined_str::<2>()?;
            raw.with_table_name(name);
        }
        if layout.expect_field_names() {
            let names: Vec<_> = (0..raw.ncols())
                .map(|_| reader.read_inlined_str::<1>())
                .try_collect()?;
            raw.with_field_names(names);
        }
        tracing::trace!(
            "table name: {}, cols: {}, rows: {}",
            &raw.table_name().unwrap_or("(?)"),
            raw.ncols(),
            raw.nrows()
        );

        Ok(Some(raw))
    }

    fn write_inlined<W: std::io::Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        use crate::prelude::sync::InlinableWrite;
        let layout = self.layout();
        let mut l = wtr.write_u32_le(layout.as_inner())?;

        let raw = self.as_raw_bytes();
        wtr.write_all(raw)?;
        l += raw.len();

        if layout.expect_table_name() {
            let name = self.table_name().expect("table name should be known");
            l += wtr.write_inlined_bytes::<2>(name.as_bytes())?;
        }
        if layout.expect_field_names() {
            debug_assert_eq!(self.field_names().len(), self.ncols());
            for field in self.field_names() {
                l += wtr.write_inlined_str::<1>(field)?;
            }
        }
        Ok(l)
    }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum SchemalessProtocol {
    Unknown = 0,
    #[default]
    Line,
    Telnet,
    Json,
}

impl From<i32> for SchemalessProtocol {
    fn from(value: i32) -> Self {
        match value {
            1 => SchemalessProtocol::Line,
            2 => SchemalessProtocol::Telnet,
            3 => SchemalessProtocol::Json,
            _ => SchemalessProtocol::Unknown,
        }
    }
}

#[repr(C)]
#[derive(Default, Debug, Copy, Clone)]
pub enum SchemalessPrecision {
    NonConfigured = 0,
    Hours,
    Minutes,
    Seconds,
    #[default]
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<SchemalessPrecision> for String {
    fn from(precision: SchemalessPrecision) -> Self {
        match precision {
            SchemalessPrecision::Hours => "h".to_string(),
            SchemalessPrecision::Minutes => "m".to_string(),
            SchemalessPrecision::Seconds => "s".to_string(),
            SchemalessPrecision::Millisecond => "ms".to_string(),
            SchemalessPrecision::Microsecond => "us".to_string(),
            SchemalessPrecision::Nanosecond => "ns".to_string(),
            SchemalessPrecision::NonConfigured => String::new(),
        }
    }
}

impl From<i32> for SchemalessPrecision {
    fn from(value: i32) -> Self {
        match value {
            0 => SchemalessPrecision::NonConfigured,
            1 => SchemalessPrecision::Hours,
            2 => SchemalessPrecision::Minutes,
            3 => SchemalessPrecision::Seconds,
            5 => SchemalessPrecision::Microsecond,
            6 => SchemalessPrecision::Nanosecond,
            _ => SchemalessPrecision::Millisecond,
        }
    }
}

#[derive(Default, Builder, Debug)]
#[builder(setter(into))]
pub struct SmlData {
    protocol: SchemalessProtocol,
    #[builder(setter(into, strip_option), default)]
    precision: SchemalessPrecision,
    data: Vec<String>,
    #[builder(setter(into, strip_option), default)]
    ttl: Option<i32>,
    #[builder(setter(into, strip_option), default)]
    req_id: Option<u64>,
    #[builder(setter(into, strip_option), default)]
    table_name_key: Option<String>,
}

impl SmlData {
    #[inline]
    pub fn protocol(&self) -> SchemalessProtocol {
        self.protocol
    }

    #[inline]
    pub fn precision(&self) -> SchemalessPrecision {
        self.precision
    }

    #[inline]
    pub fn data(&self) -> &Vec<String> {
        &self.data
    }

    #[inline]
    pub fn ttl(&self) -> Option<i32> {
        self.ttl
    }

    #[inline]
    pub fn req_id(&self) -> Option<u64> {
        self.req_id
    }

    #[inline]
    pub fn table_name_key(&self) -> Option<&String> {
        self.table_name_key.as_ref()
    }
}

impl From<SmlDataBuilderError> for Error {
    fn from(value: SmlDataBuilderError) -> Self {
        Error::from_any(value)
    }
}

#[async_trait::async_trait]
impl crate::prelude::AsyncInlinable for RawBlock {
    async fn read_optional_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Option<Self>> {
        use tokio::io::*;

        use crate::util::AsyncInlinableRead;
        let layout = reader.read_u32_le().await?;
        if layout == 0xFFFFFFFF {
            return Ok(None);
        }
        let layout = Layout::from_bits(layout).expect("invalid layout");

        let precision = layout.precision();

        let raw: InlineBlock =
            <InlineBlock as crate::prelude::AsyncInlinable>::read_inlined(reader).await?;
        let mut raw = Self::parse_from_raw_block(raw.0, precision);

        if layout.expect_table_name() {
            let name = reader.read_inlined_str::<2>().await?;
            raw.with_table_name(name);
        }
        if layout.expect_field_names() {
            let mut names = Vec::with_capacity(raw.ncols());
            for _ in 0..raw.ncols() {
                names.push(reader.read_inlined_str::<1>().await?);
            }
            raw.with_field_names(names);
        }

        Ok(Some(raw))
    }

    async fn read_inlined<R: tokio::io::AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        <Self as crate::prelude::AsyncInlinable>::read_optional_inlined(reader)
            .await?
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid raw data format",
            ))
    }

    async fn write_inlined<W: tokio::io::AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize> {
        use tokio::io::*;

        use crate::util::AsyncInlinableWrite;

        let layout = self.layout();
        wtr.write_u32_le(layout.as_inner()).await?;

        let raw = self.as_raw_bytes();
        wtr.write_all(raw).await?;

        let mut l = std::mem::size_of::<u32>() + raw.len();

        if layout.expect_table_name() {
            let name = self.table_name().expect("table name should be known");
            l += wtr.write_inlined_bytes::<2>(name.as_bytes()).await?;
        }
        if layout.expect_field_names() {
            debug_assert_eq!(self.field_names().len(), self.ncols());
            for field in self.field_names() {
                l += wtr.write_inlined_str::<1>(field).await?;
            }
        }

        Ok(l)
    }
}

pub struct MultiBlockCursor {
    cursor: Cursor<Bytes>,
}

impl Deref for MultiBlockCursor {
    type Target = Cursor<Bytes>;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl DerefMut for MultiBlockCursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cursor
    }
}

impl MultiBlockCursor {
    pub fn new(data: Cursor<Bytes>) -> Self {
        Self { cursor: data }
    }

    fn get_type_skip(t: u8) -> i32 {
        match t {
            1 => 8,
            2 | 3 => 16,
            _ => panic!("getTypeSkip error, type: {t}"),
        }
    }

    fn skip_head(&mut self) -> Result<(), std::io::Error> {
        use byteorder::ReadBytesExt;

        let version: u8 = self.cursor.read_u8()?;
        if version >= 100 {
            let skip: u32 = self.cursor.read_u32::<byteorder::LittleEndian>()?;
            self.cursor
                .set_position(self.cursor.position() + skip as u64);
        } else {
            let skip = MultiBlockCursor::get_type_skip(version);
            self.cursor
                .set_position(self.cursor.position() + skip as u64);

            let version = self.cursor.read_u8()?;
            let skip = MultiBlockCursor::get_type_skip(version);
            self.cursor
                .set_position(self.cursor.position() + skip as u64);
        }

        Ok(())
    }

    fn zigzag_decode(n: i32) -> i32 {
        (n >> 1) ^ -(n & 1)
    }

    fn parse_zigzag_variable_byte_integer(&mut self) -> Result<i32, std::io::Error> {
        let i = self.parse_variable_byte_integer()?;
        Ok(MultiBlockCursor::zigzag_decode(i))
    }

    fn parse_variable_byte_integer(&mut self) -> Result<i32, std::io::Error> {
        use byteorder::ReadBytesExt;

        let mut multiplier = 1;
        let mut value = 0;
        loop {
            let encoded_byte = self.cursor.read_u8()?;
            value += (encoded_byte & 127) as i32 * multiplier;
            if (encoded_byte & 128) == 0 {
                break;
            }
            multiplier *= 128;
        }
        Ok(value)
    }

    fn convert_error(err: std::string::FromUtf8Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, err)
    }

    fn parse_name(&mut self) -> Result<String, std::io::Error> {
        use std::io::Read;

        let name_len = self.parse_variable_byte_integer()? as usize;
        let mut name = vec![0; name_len - 1];

        self.cursor.read_exact(&mut name).unwrap();
        self.cursor.set_position(self.cursor.position() + 1);
        let result = String::from_utf8(name).map_err(MultiBlockCursor::convert_error)?;
        Ok(result)
    }

    fn get_str(&mut self) -> Result<String, std::io::Error> {
        use std::io::Read;

        use byteorder::ReadBytesExt;

        let len = self.cursor.read_u32::<byteorder::LittleEndian>()?;
        let mut str = vec![0; len as usize];

        self.cursor.read_exact(&mut str).unwrap();
        let result = String::from_utf8(str).map_err(MultiBlockCursor::convert_error)?;
        Ok(result)
    }

    fn parse_schema(&mut self) -> Result<String, std::io::Error> {
        use std::io::Read;

        use byteorder::ReadBytesExt;
        let _taos_type = self.cursor.read_u8()?;
        self.cursor.read_u8()?; // skip flag
        self.parse_zigzag_variable_byte_integer()?; // skip field len
        self.parse_zigzag_variable_byte_integer()?; // skip colid

        let name_len = self.parse_variable_byte_integer()? as usize;
        let mut name = vec![0; name_len - 1];
        self.cursor.read_exact(&mut name).unwrap();
        self.cursor.set_position(self.cursor.position() + 1);

        let field_name = String::from_utf8(name).map_err(MultiBlockCursor::convert_error)?;
        Ok(field_name)
    }
}

#[tokio::test]
async fn test_raw_from_v2() {
    use std::ops::Deref;

    use crate::prelude::AsyncInlinable;

    let bytes = b"\x10\x86\x1aA \xcc)AB\xc2\x14AZ],A\xa2\x8d$A\x87\xb9%A\xf5~\x0fA\x96\xf7,AY\xee\x17A1|\x15As\x00\x00\x00q\x00\x00\x00s\x00\x00\x00t\x00\x00\x00u\x00\x00\x00t\x00\x00\x00n\x00\x00\x00n\x00\x00\x00n\x00\x00\x00r\x00\x00\x00";

    let block = RawBlock::parse_from_raw_block_v2(
        bytes.as_slice(),
        &[Field::new("a", Ty::Float, 4), Field::new("b", Ty::Int, 4)],
        &[4, 4],
        10,
        Precision::Millisecond,
    );
    assert!(block.lengths.deref() == &[40, 40]);

    let bytes = include_bytes!("../../../tests/test.txt");

    let block = RawBlock::parse_from_raw_block_v2(
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

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        ts: String,
        current: f32,
        voltage: i32,
        phase: f32,
        group_id: i32,
        location: String,
    }
    let rows: Vec<Record> = block.deserialize().try_collect().unwrap();
    dbg!(rows);
    // dbg!(block);
    let bytes = views_to_raw_block(&block.columns);
    let raw2 = RawBlock::parse_from_raw_block(bytes, block.precision);
    dbg!(&raw2);
    let inlined = raw2.inlined().await;
    dbg!(&inlined);
    let mut raw3 = RawBlock::read_inlined(&mut inlined.as_slice())
        .await
        .unwrap();
    raw3.with_field_names(["ts", "current", "voltage", "phase", "group_id", "location"])
        .with_table_name("meters");
    dbg!(&raw3);

    let raw4 = RawBlock::read_inlined(&mut raw3.inlined().await.as_slice())
        .await
        .unwrap();
    dbg!(&raw4);
    assert_eq!(raw3.table_name(), raw4.table_name());

    let raw5 = RawBlock::read_optional_inlined(&mut raw3.inlined().await.as_slice())
        .await
        .unwrap();
    dbg!(raw5);
}

#[test]
fn test_v2_full() {
    let bytes = include_bytes!("../../../tests/v2.block.gz");

    use std::io::prelude::*;

    use flate2::read::GzDecoder;
    let mut buf = Vec::new();
    let len = GzDecoder::new(&bytes[..]).read_to_end(&mut buf).unwrap();
    assert_eq!(len, 66716);
    let block = RawBlock::parse_from_raw_block_v2(
        buf,
        &[
            Field::new("ts", Ty::Timestamp, 8),
            Field::new("b1", Ty::Bool, 1),
            Field::new("c8i1", Ty::TinyInt, 1),
            Field::new("c16i1", Ty::SmallInt, 2),
            Field::new("c32i1", Ty::Int, 4),
            Field::new("c64i1", Ty::BigInt, 8),
            Field::new("c8u1", Ty::UTinyInt, 1),
            Field::new("c16u1", Ty::USmallInt, 2),
            Field::new("c32u1", Ty::UInt, 4),
            Field::new("c64u1", Ty::UBigInt, 8),
            Field::new("cb1", Ty::VarChar, 100),
            Field::new("cn1", Ty::NChar, 10),
            Field::new("b2", Ty::Bool, 1),
            Field::new("c8i2", Ty::TinyInt, 1),
            Field::new("c16i2", Ty::SmallInt, 2),
            Field::new("c32i2", Ty::Int, 4),
            Field::new("c64i2", Ty::BigInt, 8),
            Field::new("c8u2", Ty::UTinyInt, 1),
            Field::new("c16u2", Ty::USmallInt, 2),
            Field::new("c32u2", Ty::UInt, 4),
            Field::new("c64u2", Ty::UBigInt, 8),
            Field::new("cb2", Ty::VarChar, 100),
            Field::new("cn2", Ty::NChar, 10),
            Field::new("jt", Ty::Json, 4096),
        ],
        &[
            8, 1, 1, 2, 4, 8, 1, 2, 4, 8, 102, 42, 1, 1, 2, 4, 8, 1, 2, 4, 8, 12, 66, 16387,
        ],
        4,
        Precision::Millisecond,
    );
    let bytes = views_to_raw_block(&block.columns);
    let raw2 = RawBlock::parse_from_raw_block(bytes, block.precision);

    let added = &raw2 + &raw2;
    dbg!(raw2, added);
}

#[test]
fn test_v2_null() {
    let raw = RawBlock::parse_from_raw_block_v2(
        [0x2, 0].as_slice(),
        &[Field::new("b", Ty::Bool, 1)],
        &[1],
        2,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let bytes = views_to_raw_block(&raw.columns);
    let raw2 = RawBlock::parse_from_raw_block(bytes, raw.precision);
    dbg!(raw2);
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    assert!(null.is_null());
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(1, 0) };
    assert!(!null.is_null());

    let raw = RawBlock::parse_from_raw_block_v2(
        [0x80].as_slice(),
        &[Field::new("b", Ty::TinyInt, 1)],
        &[1],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    assert!(null.is_null());

    let raw = RawBlock::parse_from_raw_block_v2(
        [0, 0, 0, 0x80].as_slice(),
        &[Field::new("a", Ty::Int, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    assert!(null.is_null());

    let raw = RawBlock::parse_from_raw_block_v2(
        [0, 0, 0xf0, 0x7f].as_slice(),
        &[Field::new("a", Ty::Float, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    let (_ty, _len, null) = unsafe { raw.get_raw_value_unchecked(0, 0) };
    dbg!(raw);
    assert!(null.is_null());
}

#[test]
fn test_from_v2() {
    let raw = RawBlock::parse_from_raw_block_v2(
        [1].as_slice(),
        &[Field::new("a", Ty::TinyInt, 1)],
        &[1],
        1,
        Precision::Millisecond,
    );
    let bytes = raw.as_raw_bytes();
    let bytes = Bytes::copy_from_slice(bytes);
    let raw2 = RawBlock::parse_from_raw_block(bytes, raw.precision());
    dbg!(&raw, raw2);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawBlock::parse_from_raw_block_v2(
        [1, 0, 0, 0].as_slice(),
        &[Field::new("a", Ty::Int, 4)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawBlock::parse_from_raw_block_v2(
        [2, 0, b'a', b'b'].as_slice(),
        &[Field::new("b", Ty::VarChar, 2)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    // dbg!(raw.as_bytes());
    // let v = unsafe { raw.get_ref_unchecked(0, 0) };
    // dbg!(v);

    let raw = RawBlock::parse_from_raw_block_v2(
        [2, 0, b'a', b'b'].as_slice(),
        &[Field::new("b", Ty::VarChar, 2)],
        &[4],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);
    let raw = RawBlock::parse_from_raw_block_v2(
        &[1, 1, 1][..],
        &[
            Field::new("a", Ty::TinyInt, 1),
            Field::new("b", Ty::SmallInt, 2),
        ],
        &[1, 2],
        1,
        Precision::Millisecond,
    );
    dbg!(&raw);

    println!("{}", raw.pretty_format());
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;
    use crate::common::decimal::Decimal;

    #[test]
    fn parse_decimal64_raw_block_test() -> anyhow::Result<()> {
        let mut bytes = BytesMut::new();

        // header
        bytes.extend_from_slice(
            Header {
                version: 3,
                length: 82,
                nrows: 1,
                ncols: 3,
                flag: 0,
                group_id: 0,
            }
            .as_bytes(),
        );
        // 28

        // schema (1+4) * 3
        bytes.extend_from_slice(
            DataType::new(Ty::Decimal64, 16 << 24 | 0 << 16 | 5 << 8 | 2).as_bytes(), // 123.45
        );
        bytes.extend_from_slice(
            DataType::new(Ty::Decimal64, 16 << 24 | 0 << 16 | 5 << 8 | 0).as_bytes(), // 12345
        );
        bytes.extend_from_slice(
            DataType::new(Ty::Decimal64, 16 << 24 | 0 << 16 | 5 << 8 | 5).as_bytes(), // 0.12345
        );
        // 43

        // length 4 * 3
        for _ in 0..3 {
            bytes.put_u32_ne(9);
        }

        // nums (1 + 8) * 3
        for _ in 0..3 {
            bytes.extend(NullBits::from_iter([false, false, false]).0);
            bytes.put_i64_ne(12345);
        }

        let block = RawBlock::parse_from_raw_block(bytes, Precision::Microsecond);
        let cols = block.column_views();
        let mut iter = cols.iter();

        let mut check_decimal = |precision: u8, scale: u8| {
            let col1 = iter.next();
            let Some(ColumnView::Decimal64(rows)) = col1 else {
                anyhow::bail!("not decimal type")
            };
            let Some(Some(decimal)) = rows.iter().next() else {
                anyhow::bail!("decimal row not found")
            };
            assert_eq!(
                decimal,
                Decimal {
                    data: 12345,
                    precision,
                    scale
                }
            );
            Ok(())
        };
        check_decimal(5, 2)?;
        check_decimal(5, 0)?;
        check_decimal(5, 5)?;

        Ok(())
    }

    #[test]
    fn parse_decimal_raw_block_test() -> anyhow::Result<()> {
        let mut bytes = BytesMut::new();

        // header
        bytes.extend_from_slice(
            Header {
                version: 3,
                length: 106,
                nrows: 1,
                ncols: 3,
                flag: 0,
                group_id: 0,
            }
            .as_bytes(),
        );
        // 28

        // schema (1+4) * 3
        bytes.extend_from_slice(
            DataType::new(Ty::Decimal, 16 << 24 | 0 << 16 | 5 << 8 | 2).as_bytes(), // 123.45
        );
        bytes.extend_from_slice(
            DataType::new(Ty::Decimal, 16 << 24 | 0 << 16 | 5 << 8 | 0).as_bytes(), // 12345
        );
        bytes.extend_from_slice(
            DataType::new(Ty::Decimal, 16 << 24 | 0 << 16 | 5 << 8 | 5).as_bytes(), // 0.12345
        );
        // 43

        // length 4 * 3
        for _ in 0..3 {
            bytes.put_u32_ne(9);
        }

        // nums (1 + 16) * 3
        for _ in 0..3 {
            bytes.extend(NullBits::from_iter([false, false, false]).0);
            bytes.put_i128_ne(12345);
        }

        let block = RawBlock::parse_from_raw_block(bytes, Precision::Microsecond);
        let cols = block.column_views();
        let mut iter = cols.iter();

        let mut check_decimal = |precision: u8, scale: u8| {
            let col1 = iter.next();
            let Some(ColumnView::Decimal(rows)) = col1 else {
                anyhow::bail!("not decimal type")
            };
            let Some(Some(decimal)) = rows.iter().next() else {
                anyhow::bail!("decimal row not found")
            };
            assert_eq!(
                decimal,
                Decimal {
                    data: 12345,
                    precision,
                    scale
                }
            );
            Ok(())
        };
        check_decimal(5, 2)?;
        check_decimal(5, 0)?;
        check_decimal(5, 5)?;

        Ok(())
    }

    #[test]
    fn parse_decimal_float_test() -> anyhow::Result<()> {
        let bytes = Bytes::from_static(&[
            1, 0, 0, 0, 79, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0,
            9, 8, 0, 0, 0, 21, 2, 5, 0, 8, 4, 4, 0, 0, 0, 8, 0, 0, 0, 8, 0, 0, 0, 4, 0, 0, 0, 0,
            171, 120, 92, 155, 153, 1, 0, 0, 0, 57, 48, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
        ]);
        let block = RawBlock::parse_from_raw_block(bytes, Precision::Microsecond);
        let cols = block.column_views();
        let mut iter = cols.iter();
        while let Some(ColumnView::Decimal64(rows)) = iter.next() {
            let Some(Some(decimal)) = rows.iter().next() else {
                anyhow::bail!("decimal row not found")
            };
            assert_eq!(
                decimal,
                Decimal {
                    data: 12345,
                    precision: 5,
                    scale: 2
                }
            );
        }

        Ok(())
    }

    #[test]
    fn test_parse_blob_raw_block() -> anyhow::Result<()> {
        let mut bytes = BytesMut::new();

        // header
        bytes.extend_from_slice(
            Header {
                version: 3,
                length: 82,
                nrows: 1,
                ncols: 3,
                flag: 0,
                group_id: 0,
            }
            .as_bytes(),
        );

        // schema
        bytes.extend_from_slice(DataType::new(Ty::Blob, 0).as_bytes());
        bytes.extend_from_slice(DataType::new(Ty::Blob, 0).as_bytes());
        bytes.extend_from_slice(DataType::new(Ty::Blob, 0).as_bytes());

        // length
        bytes.put_u32_ne(8);
        bytes.put_u32_ne(0);
        bytes.put_u32_ne(7);

        // blobs
        bytes.put_i32_ne(0);
        bytes.put_u32_ne(4);
        bytes.put_slice(&[1, 2, 3, 4]);

        bytes.put_i32_le(-1);

        bytes.put_i32_ne(0);
        bytes.put_u32_ne(3);
        bytes.put_slice(&[2, 3, 3]);

        let block = RawBlock::parse_from_raw_block(bytes, Precision::Millisecond);
        let views = block.column_views();
        assert_eq!(views.len(), 3);

        if let Some(ColumnView::Blob(view)) = views.get(0) {
            assert_eq!(view.len(), 1);
            assert_eq!(view.to_vec(), [Some(vec![1, 2, 3, 4])]);
        } else {
            panic!("expected blob column");
        }

        if let Some(ColumnView::Blob(view)) = views.get(1) {
            assert_eq!(view.len(), 1);
            assert_eq!(view.to_vec(), [None]);
        } else {
            panic!("expected blob column");
        }

        if let Some(ColumnView::Blob(view)) = views.get(2) {
            assert_eq!(view.len(), 1);
            assert_eq!(view.to_vec(), [Some(vec![2, 3, 3])]);
        } else {
            panic!("expected blob column");
        }

        let offsets = block.get_col_data_offset_unchecked(0);
        assert_eq!(offsets, &[0]);

        let offsets = block.get_col_data_offset_unchecked(1);
        assert_eq!(offsets, &[-1]);

        let offsets = block.get_col_data_offset_unchecked(2);
        assert_eq!(offsets, &[0]);

        Ok(())
    }
}
