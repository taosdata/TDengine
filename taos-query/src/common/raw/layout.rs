use std::ops::{BitAnd, BitAndAssign, BitOrAssign};

use bitflags::bitflags;

use crate::common::Precision;

/// Inlined data format in backup file.
pub enum InlineFormat {
    /// Only located at the exactly header of a file, contains basic information describing the data included.
    ///
    /// 1. database name
    /// 2. database schema
    /// 3. single table or not.
    Header,
    /// Raw meta message from TMQ, which could be write back to a TDengine cluster seamlessly.
    RawMeta,
    /// Raw data message from TMQ, contains data block of a child table or common table.
    RawData,
    /// For stable data not use TMQ, contains STable schema and table name - tags entry map.
    ///
    /// This type of data will be built from legacy whole backup strategy.
    StbData,
}

bitflags! {
    /// Inline memory layout for raw block.
    #[repr(transparent)]
    pub struct Layout: u32 {
        // Lowest 4 bits for layout components.

        /// With table name in the block.
        const WITH_TABLE_NAME = 0b_0001;
        /// With field names.
        const WITH_FIELD_NAMES = 0b_0010;
        /// If raw data block contains group id.
        const WITH_GROUP_ID = 0b_0100;
        /// If raw data contains schema
        const WITH_FIELD_SCHEMA = 0b_1000;

        /// If NChar decoded or not in data block.
        const NCHAR_IS_DECODED = 0b_0001_0000;
        /// If Schema has been changed, eg. subset or added some columns.
        const SCHEMA_CHANGED = 0b_0010_0000;

        // Inline layout types.

        /// Inline with full raw layout, this is the default behavior.
        ///
        ///
        /// ```text
        /// +-----+----+----+-----+------+---+---+------+-------+------------+
        /// |flags|rows|cols|table|fields|len|gid|schema|lengths|    data    |
        /// +-----+----+----+-----+------+---+---+------+-------+------------+
        /// |2    |4   |4   | dyn | dyn  | 4 | 8 |cols*6|cols*4 |sum(lengths)|
        /// +-----+----+----+-----+------+---+---+------+-------+------------+
        /// ```
        const INLINE_DEFAULT = Self::WITH_FIELD_NAMES.bits | Self::WITH_GROUP_ID.bits | Self::WITH_FIELD_SCHEMA.bits;

        /// Inline as raw block only, without table names and field names.
        ///
        /// ```text
        /// +-----+----+----+---+---+------+-------+------------+
        /// |flags|rows|cols|len|gid|schema|lengths|    data    |
        /// +-----+----+----+---+---+------+-------+------------+
        /// |2    |4   |4   |4  | 8 |cols*6|cols*4 |sum(lengths)|
        /// +-----+----+----+---+---+------+-------+------------+
        /// ```
        const INLINE_AS_RAW_BLOCK = Self::WITH_GROUP_ID.bits | Self::WITH_FIELD_SCHEMA.bits;

        /// Inline as data only to save space.
        ///
        /// ```text
        /// +-----+----+----+-------+------------|
        /// |flags|rows|cols|lengths|    data    |
        /// +-----+----+----+-------+------------|
        /// |2    |4   |4   |cols*4 |sum(lengths)|
        /// +-----+----+----+-------+------------|
        /// ```
        const INLINE_AS_DATA_ONLY = 0;

        // Next 2 bits for precision

        /// Precision mask.
        const PRECISION_MASK = 0b11_0000_0000;
        const PRECISION_CLEAR_MASK = 0b1111_1100_1111_1111;
        const IS_MILLIS = 0b0_0000_0000;
        const IS_MICROS = 0b1_0000_0000;
        const IS_NANOS = 0b10_0000_0000;

        // Top 4 bits for data block type.
        //
        //  0: table raw data
        //  1: table raw meta
        //  2: stable schema and name-tags data.
        //  3: database schema
        const FORMAT_MASK = 0b_0011_0000_0000_0000;

        /// is raw data
        const IS_RAW_DATA = 0b_0000_0000_0000_0000;

        /// is raw meta
        const IS_RAW_META = 0b_0001_0000_0000_0000;

        /// Is STable data, contains sql schema and table name - tags map.
        const IS_STB_DATA = 0b_0010_0000_0000_0000;

        /// Is database schema, used for database rebuild.
        const IS_HEADER = 0b_0011_0000_0000_0000;

        // Non-exhausted.
    }
}

// explicit `Default` implementation
impl Default for Layout {
    fn default() -> Layout {
        Self::INLINE_DEFAULT
    }
}

impl Layout {
    pub fn precision(&self) -> Precision {
        let layout = *self & Self::PRECISION_MASK;
        match layout {
            l if l == Self::IS_MILLIS => Precision::Millisecond,
            l if l == Self::IS_MICROS => Precision::Microsecond,
            Self::IS_NANOS => Precision::Nanosecond,
            _ => unreachable!(),
        }
    }

    pub fn expect_table_name(&self) -> bool {
        self.contains(Self::WITH_TABLE_NAME)
    }

    pub fn with_table_name(&mut self) -> &mut Self {
        self.set(Self::WITH_TABLE_NAME, true);
        self
    }

    pub fn expect_field_names(&self) -> bool {
        self.contains(Self::WITH_FIELD_NAMES)
    }

    pub fn with_field_names(&mut self) -> &mut Self {
        self.set(Self::WITH_FIELD_NAMES, true);
        self
    }

    pub fn nchar_is_decoded(&self) -> bool {
        self.contains(Self::NCHAR_IS_DECODED)
    }

    pub fn with_nchar_decoded(&mut self) -> &mut Self {
        self.set(Self::NCHAR_IS_DECODED, true);
        self
    }

    pub fn with_precision(&mut self, precision: Precision) -> &mut Self {
        self.bitand_assign(Self::PRECISION_CLEAR_MASK);
        let precision = match precision {
            Precision::Millisecond => Self::IS_MILLIS,
            Precision::Microsecond => Self::IS_MICROS,
            Precision::Nanosecond => Self::IS_NANOS,
        };
        self.bitor_assign(precision);
        self
    }

    pub fn with_schema_changed(&mut self) -> &mut Self {
        self.set(Self::SCHEMA_CHANGED, true);
        self
    }

    pub fn schema_changed(&self) -> bool {
        self.contains(Self::SCHEMA_CHANGED)
    }

    pub fn inline_format(&self) -> InlineFormat {
        match self.bitand(Self::FORMAT_MASK) {
            Self::IS_RAW_DATA => InlineFormat::RawData,
            Self::IS_RAW_META => InlineFormat::RawMeta,
            Self::IS_STB_DATA => InlineFormat::StbData,
            Self::IS_HEADER => InlineFormat::Header,
            _ => unreachable!(),
        }
    }

    pub fn as_inner(&self) -> u32 {
        self.bits
    }
}

#[test]
fn test_layout() {
    let mut default = Layout::default();
    assert_eq!(default, Layout::INLINE_DEFAULT);

    assert!(default.expect_table_name());
    assert!(!default.expect_field_names());
    assert!(default.with_field_names().expect_field_names());

    assert_eq!(default.precision(), Precision::Millisecond);
    let precisions = [
        Precision::Nanosecond,
        Precision::Microsecond,
        Precision::Millisecond,
    ];
    for precision in precisions {
        assert_eq!(default.with_precision(precision).precision(), precision);
    }

    assert!(!default.nchar_is_decoded());
    assert!(default.with_nchar_decoded().nchar_is_decoded());

    let mut a = Layout::INLINE_AS_DATA_ONLY;
    assert!(a.with_table_name().expect_table_name());
    assert!(a.with_field_names().expect_field_names());

    assert!(!a.schema_changed());
    assert!(a.with_schema_changed().schema_changed());
}
