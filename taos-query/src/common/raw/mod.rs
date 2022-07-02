mod inner;

use std::io::Write;

use super::{BorrowedValue, Column, Field, Precision};

use crate::{util::*, BlockExt};

pub mod inner_v2;

pub use inner::*;
pub use inner_v2::*;

use bitflags::bitflags;
use once_cell::unsync::OnceCell;

bitflags! {
    #[derive(Default)]
    #[repr(transparent)]
    struct Fx: u8 {
        /// Has database name in the block.
        const HAS_DB = 0b00000001;
        /// Has table name in the block
        const HAS_TB = 0b00000010;
        /// Has fields data in the block
        const HAS_FL = 0b00000100;
        /// Has precision set.
        const HAS_PC = 0b00001000;
        /// Precision set as 'ms'(milliseconds).
        const IS_MS  = 0b00001000;
        /// Precision set as 'us'(microseconds).
        const IS_US  = 0b00011000;
        /// Precision set as 'ns'(nanoseconds).
        const IS_NS  = 0b00101000;
    }
}

#[derive(Debug)]
pub struct Block {
    database: Option<String>,
    table: Option<String>,
    fields: Option<Vec<Field>>,
    raw: OnceCell<RawBlock>,
    precision: Precision,
    columns: OnceCell<Vec<Column>>,
}

impl Inlinable for Block {
    #[inline]
    fn read_inlined<R: std::io::Read>(mut reader: R) -> std::io::Result<Self> {
        let flags = Fx::from_bits(reader.read_u8()?).unwrap();
        let database = if flags.contains(Fx::HAS_DB) {
            Some(reader.read_inlined_str::<2>()?)
        } else {
            None
        };
        let table = if flags.contains(Fx::HAS_DB) {
            Some(reader.read_inlined_str::<2>()?)
        } else {
            None
        };
        use itertools::Itertools;
        let fields = if flags.contains(Fx::HAS_FL) {
            Some(
                (0..reader.read_u16()?)
                    .map(|_| reader.read_inlinable::<Field>())
                    .try_collect()?,
            )
        } else {
            None
        };

        let precision = if flags.contains(Fx::IS_MS) {
            Precision::Millisecond
        } else if flags.contains(Fx::IS_US) {
            Precision::Microsecond
        } else if flags.contains(Fx::IS_NS) {
            Precision::Nanosecond
        } else {
            Precision::Millisecond
        };

        let rows = reader.read_u32()?;
        let cols = reader.read_u32()?;

        let bytes = reader.read_inlined_bytes::<4>()?;
        let raw = OnceCell::new();
        raw.try_insert(RawBlock::from_bytes(
            bytes,
            rows as usize,
            cols as usize,
            precision,
        ))
        .unwrap();

        Ok(Self {
            database,
            table,
            fields,
            raw,
            precision,
            columns: OnceCell::new(),
        })
    }

    #[inline]
    fn write_inlined<W: Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let mut l = wtr.write_u8(self.flags().bits)?;

        if let Some(name) = self.database.as_ref() {
            l += wtr.write_inlined_bytes::<2>(name.as_bytes())?;
        }
        if let Some(name) = self.table.as_ref() {
            l += wtr.write_inlined_bytes::<2>(name.as_bytes())?;
        }
        if let Some(fields) = self.fields.as_ref() {
            l += wtr.write_len_with_width::<2>(fields.len())?;
            for field in fields {
                l += wtr.write_inlinable(field)?;
            }
        }
        let raw = self.as_raw_block();
        l += wtr.write_len_with_width::<4>(raw.nrows())?;
        l += wtr.write_len_with_width::<4>(raw.ncols())?;
        l += wtr.write_inlined_bytes::<4>(raw.as_bytes())?;
        Ok(l)
    }
}

impl Block {
    #[inline]
    pub fn fields(&self) -> &[Field] {
        const FIELDS: [Field; 0] = [];
        if let Some(f) = self.fields.as_ref() {
            &f
        } else {
            &FIELDS
        }
    }

    #[inline]
    pub const fn has_database_name(&self) -> bool {
        self.database.is_some()
    }
    #[inline]
    pub const fn has_table_name(&self) -> bool {
        self.table.is_some()
    }

    #[inline]
    pub fn with_database_name(&mut self, database: &str) -> &mut Self {
        self.database = Some(database.to_string());
        self
    }

    #[inline]
    pub fn with_table_name(&mut self, table: &str) -> &mut Self {
        self.table = Some(table.to_string());
        self
    }

    #[inline]
    pub fn with_fields(&mut self, fields: Vec<Field>) -> &mut Self {
        assert!(fields.len() > 0);
        self.fields = Some(fields);
        self
    }

    #[inline]
    pub fn from_raw_block(data: RawBlock) -> Self {
        let raw = OnceCell::new();
        raw.try_insert(data).unwrap();
        Self {
            raw,
            database: None,
            table: None,
            fields: None,
            precision: Precision::Millisecond,
            columns: OnceCell::new(),
        }
    }

    #[inline]
    pub fn from_raw_bytes(bytes: Vec<u8>, rows: usize, cols: usize, precision: Precision) -> Self {
        let data = RawBlock::from_bytes(bytes, rows, cols, precision);
        Self::from_raw_block(data)
    }

    #[inline]
    pub fn push_column(&mut self, column: impl Into<Column>) -> &mut Self {
        if let Some(mut v) = self.columns.take() {
            v.push(column.into());
            self.columns.try_insert(v).unwrap();
        } else {
            let v = vec![column.into()];
            self.columns.try_insert(v).unwrap();
        }
        self
    }

    #[inline]
    pub fn as_raw_block(&self) -> &RawBlock {
        self.raw.get_or_init(|| todo!())
    }

    #[inline]
    pub fn to_raw_vec(&self) -> std::io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.write_inlinable(self)?;
        Ok(bytes)
    }

    #[inline]
    pub fn nrows(&self) -> usize {
        self.as_raw_block().nrows()
    }

    #[inline]
    pub fn ncols(&self) -> usize {
        self.as_raw_block().ncols()
    }

    #[inline]
    pub const fn precision(&self) -> Precision {
        self.precision
    }

    #[inline]
    pub unsafe fn get_ref_unchecked(&self, row: usize, col: usize) -> BorrowedValue {
        self.as_raw_block().get_ref_unchecked(row, col)
    }

    fn flags(&self) -> Fx {
        let mut flags = Fx::default();
        if self.has_database_name() {
            flags |= Fx::HAS_DB;
        }
        if self.has_table_name() {
            flags |= Fx::HAS_TB;
        }
        if self.fields.is_some() {
            flags |= Fx::HAS_FL;
        }
        flags
    }
}

impl BlockExt for Block {
    fn num_of_rows(&self) -> usize {
        self.nrows()
    }

    fn fields(&self) -> &[Field] {
        self.fields()
    }

    fn precision(&self) -> Precision {
        self.precision()
    }

    fn is_null(&self, row: usize, col: usize) -> bool {
        todo!()
    }

    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, BorrowedValue) {
        (
            self.get_field_unchecked(col),
            self.get_ref_unchecked(row, col),
        )
    }

    unsafe fn get_col_unchecked(&self, col: usize) -> super::BorrowedColumn {
        todo!()
    }
}

#[test]
fn inner_block() -> anyhow::Result<()> {
    use crate::common::Precision::Millisecond;
    let bytes = b"5\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\t\x00\x08\x00\x00\x00\x01\x00\x01\x00\x00\x00\x02\x00\x01\x00\x00\x00\x03\x00\x02\x00\x00\x00\x04\x00\x04\x00\x00\x00\x05\x00\x08\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x0c\x00\x02\x00\x00\x00\r\x00\x04\x00\x00\x00\x0e\x00\x08\x00\x00\x00\x06\x00\x04\x00\x00\x00\x07\x00\x08\x00\x00\x00\x08\x00f\x00\x00\x00\n\x00\x92\x01\x00\x00\x10\x00\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x08\x00\x00\x00\x10\x00\x00\x00\x05\x00\x00\x00\x16\x00\x00\x00\x00\xf2=\xc3u\x81\x01\x00\x00\xdaA\xc3u\x81\x01\x00\x00@\x01\x00@\xff\x00@\xff\xff\x00\x00@\xff\xff\xff\xff\x00\x00\x00\x00@\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00@\x01\x00@\x01\x00\x00\x00@\x01\x00\x00\x00\x00\x00\x00\x00@\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x03\x00abc\x00\x00\x00\x00\xff\xff\xff\xff\x14\x00\x9bm\x00\x00\x1d`\x00\x00\x1e\xd1\x01\x00pe\x00\x00nc\x00\x00\x00\x00\x00\x00";
    let rows = 2;
    let cols = 14;
    let precision = Millisecond;

    let raw = unsafe { RawBlock::from_ptr(bytes.as_ptr(), rows, cols, precision) };

    let mut block = Block::from_raw_block(raw);
    block.with_database_name("abc").with_table_name("n1");

    let a = block.printable_inlined();

    let inlined = block.inlined();

    anyhow::ensure!(
        &inlined[0..18] == b"\x03\x03\x00abc\x02\x00n1\x02\x00\x00\x00\x0e\x00\x00\x00"
    );
    assert!(&inlined[18..] == bytes);

    let block = Block::read_inlined(inlined.as_slice())?;
    let b = block.printable_inlined();

    assert_eq!(a, b);
    Ok(())
}
