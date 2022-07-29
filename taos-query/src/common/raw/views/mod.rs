mod bool_view;

pub use bool_view::BoolView;

mod tinyint_view;

pub use tinyint_view::TinyIntView;

mod smallint_view;
pub use smallint_view::SmallIntView;

mod int_view;
pub use int_view::IntView;

mod big_int_view;
pub use big_int_view::BigIntView;

mod float_view;
pub use float_view::FloatView;

mod double_view;
pub use double_view::DoubleView;

mod tinyint_unsigned_view;
pub use tinyint_unsigned_view::UTinyIntView;

mod small_int_unsigned_view;
pub use small_int_unsigned_view::USmallIntView;

mod int_unsigned_view;
pub use int_unsigned_view::UIntView;

mod big_int_unsigned_view;
pub use big_int_unsigned_view::UBigIntView;

mod timestamp_view;
pub use timestamp_view::TimestampView;

mod var_char_view;
pub use var_char_view::VarCharView;

mod n_char_view;
pub use n_char_view::NCharView;

mod json_view;
pub use json_view::JsonView;

mod schema;
pub(crate) use schema::*;

mod nulls;
pub(crate) use nulls::*;

mod offsets;
pub(crate) use offsets::*;

mod lengths;
pub(crate) use lengths::*;

use crate::common::{BorrowedValue, Column, Ty};

use std::{ffi::c_void, fmt::Debug, iter::FusedIterator};

/// Compatible version for var char.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Version {
    V2,
    V3,
}

// #[derive(Debug)]
pub enum ColumnView {
    Bool(BoolView),           // 1
    TinyInt(TinyIntView),     // 2
    SmallInt(SmallIntView),   // 3
    Int(IntView),             // 4
    BigInt(BigIntView),       // 5
    Float(FloatView),         // 6
    Double(DoubleView),       // 7
    VarChar(VarCharView),     // 8
    Timestamp(TimestampView), // 9
    NChar(NCharView),         // 10
    UTinyInt(UTinyIntView),   // 11
    USmallInt(USmallIntView), // 12
    UInt(UIntView),           // 13
    UBigInt(UBigIntView),     // 14
    Json(JsonView),           // 15
}

impl Debug for ColumnView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool(view) => f.debug_tuple("Bool").field(&view.to_vec()).finish(),
            Self::TinyInt(view) => f.debug_tuple("TinyInt").field(&view.to_vec()).finish(),
            Self::SmallInt(view) => f.debug_tuple("SmallInt").field(&view.to_vec()).finish(),
            Self::Int(view) => f.debug_tuple("Int").field(&view.to_vec()).finish(),
            Self::BigInt(view) => f.debug_tuple("BigInt").field(&view.to_vec()).finish(),
            Self::Float(view) => f.debug_tuple("Float").field(&view.to_vec()).finish(),
            Self::Double(view) => f.debug_tuple("Double").field(&view.to_vec()).finish(),
            Self::VarChar(view) => f.debug_tuple("VarChar").field(&view.to_vec()).finish(),
            Self::Timestamp(view) => f.debug_tuple("Timestamp").field(&view.to_vec()).finish(),
            Self::NChar(view) => f.debug_tuple("NChar").field(&view.to_vec()).finish(),
            Self::UTinyInt(view) => f.debug_tuple("UTinyInt").field(&view.to_vec()).finish(),
            Self::USmallInt(view) => f.debug_tuple("USmallInt").field(&view.to_vec()).finish(),
            Self::UInt(view) => f.debug_tuple("UInt").field(&view.to_vec()).finish(),
            Self::UBigInt(view) => f.debug_tuple("UBigInt").field(&view.to_vec()).finish(),
            Self::Json(view) => f.debug_tuple("Json").field(&view.to_vec()).finish(),
        }
    }
}

pub struct ColumnViewIter<'c> {
    view: &'c ColumnView,
    row: usize,
}

impl<'c> Iterator for ColumnViewIter<'c> {
    type Item = BorrowedValue<'c>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.view.len() {
            return None;
        }
        let row = self.row;
        self.row += 1;
        Some(unsafe { self.view.get_ref_unchecked(row) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.row, Some(self.view.len() - self.row))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let row = self.row + n;
        if row >= self.view.len() {
            return None;
        }
        self.row += n;
        Some(unsafe { self.view.get_ref_unchecked(row) })
    }

    #[inline]
    fn count(self) -> usize {
        self.view.len()
    }
}

impl<'c> ExactSizeIterator for ColumnViewIter<'c> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

impl<'a> FusedIterator for ColumnViewIter<'a> {}

impl<'a> IntoIterator for &'a ColumnView {
    type Item = BorrowedValue<'a>;

    type IntoIter = ColumnViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl ColumnView {
    /// It's equal to the cols
    pub fn len(&self) -> usize {
        match self {
            ColumnView::Bool(view) => view.len(),
            ColumnView::TinyInt(view) => view.len(),
            ColumnView::SmallInt(view) => view.len(),
            ColumnView::Int(view) => view.len(),
            ColumnView::BigInt(view) => view.len(),
            ColumnView::Float(view) => view.len(),
            ColumnView::Double(view) => view.len(),
            ColumnView::VarChar(view) => view.len(),
            ColumnView::Timestamp(view) => view.len(),
            ColumnView::NChar(view) => view.len(),
            ColumnView::UTinyInt(view) => view.len(),
            ColumnView::USmallInt(view) => view.len(),
            ColumnView::UInt(view) => view.len(),
            ColumnView::UBigInt(view) => view.len(),
            ColumnView::Json(view) => view.len(),
        }
    }

    /// Check if a value at `row` is null
    #[inline]
    pub(super) unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        match self {
            ColumnView::Bool(view) => view.is_null_unchecked(row),
            ColumnView::TinyInt(view) => view.is_null_unchecked(row),
            ColumnView::SmallInt(view) => view.is_null_unchecked(row),
            ColumnView::Int(view) => view.is_null_unchecked(row),
            ColumnView::BigInt(view) => view.is_null_unchecked(row),
            ColumnView::Float(view) => view.is_null_unchecked(row),
            ColumnView::Double(view) => view.is_null_unchecked(row),
            ColumnView::VarChar(view) => view.is_null_unchecked(row),
            ColumnView::Timestamp(view) => view.is_null_unchecked(row),
            ColumnView::NChar(view) => view.is_null_unchecked(row),
            ColumnView::UTinyInt(view) => view.is_null_unchecked(row),
            ColumnView::USmallInt(view) => view.is_null_unchecked(row),
            ColumnView::UInt(view) => view.is_null_unchecked(row),
            ColumnView::UBigInt(view) => view.is_null_unchecked(row),
            ColumnView::Json(view) => view.is_null_unchecked(row),
        }
    }

    /// Get one value at `row` index of the column view.
    #[inline]
    pub(super) unsafe fn get_ref_unchecked(&self, row: usize) -> BorrowedValue {
        match self {
            ColumnView::Bool(view) => view.get_value_unchecked(row),
            ColumnView::TinyInt(view) => view.get_value_unchecked(row),
            ColumnView::SmallInt(view) => view.get_value_unchecked(row),
            ColumnView::Int(view) => view.get_value_unchecked(row),
            ColumnView::BigInt(view) => view.get_value_unchecked(row),
            ColumnView::Float(view) => view.get_value_unchecked(row),
            ColumnView::Double(view) => view.get_value_unchecked(row),
            ColumnView::VarChar(view) => view.get_value_unchecked(row),
            ColumnView::Timestamp(view) => view.get_value_unchecked(row),
            ColumnView::NChar(view) => view.get_value_unchecked(row),
            ColumnView::UTinyInt(view) => view.get_value_unchecked(row),
            ColumnView::USmallInt(view) => view.get_value_unchecked(row),
            ColumnView::UInt(view) => view.get_value_unchecked(row),
            ColumnView::UBigInt(view) => view.get_value_unchecked(row),
            ColumnView::Json(view) => view.get_value_unchecked(row),
        }
    }

    /// Get pointer to value.
    #[inline]
    pub(super) unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        match self {
            ColumnView::Bool(view) => view.get_raw_value_unchecked(row),
            ColumnView::TinyInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::SmallInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::Int(view) => view.get_raw_value_unchecked(row),
            ColumnView::BigInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::Float(view) => view.get_raw_value_unchecked(row),
            ColumnView::Double(view) => view.get_raw_value_unchecked(row),
            ColumnView::VarChar(view) => view.get_raw_value_unchecked(row),
            ColumnView::Timestamp(view) => view.get_raw_value_unchecked(row),
            ColumnView::NChar(view) => view.get_raw_value_unchecked(row),
            ColumnView::UTinyInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::USmallInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::UInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::UBigInt(view) => view.get_raw_value_unchecked(row),
            ColumnView::Json(view) => view.get_raw_value_unchecked(row),
        }
    }

    pub fn iter(&self) -> ColumnViewIter {
        ColumnViewIter { view: self, row: 0 }
    }
    pub fn into_column(&self) -> Column {
        match self {
            ColumnView::Bool(view) => Column::Bool(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::TinyInt(view) => Column::TinyInt(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::SmallInt(view) => Column::SmallInt(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::Int(view) => Column::Int(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::BigInt(view) => Column::BigInt(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::Float(view) => Column::Float(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::Double(view) => Column::Double(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::VarChar(view) => Column::VarBinary(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.iter()
                    .map(|v| v.map(|s| s.as_bytes().to_vec()).unwrap_or_default())
                    .collect(),
            ),
            ColumnView::Timestamp(_) => todo!(),
            ColumnView::NChar(view) => {
                Column::NChar(view.iter().map(|s| s.map(|s| s.to_string())).collect())
            }
            ColumnView::UTinyInt(view) => Column::UTinyInt(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::USmallInt(view) => Column::USmallInt(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::UInt(view) => Column::UInt(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::UBigInt(view) => Column::UBigInt(
                bitvec_simd::BitVec::from_bool_iterator(view.is_null_iter()),
                view.as_raw_slice().to_vec(),
            ),
            ColumnView::Json(_) => todo!(),
        }
    }
}
