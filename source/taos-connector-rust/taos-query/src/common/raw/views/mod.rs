use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use bitvec::macros::internal::funty::Fundamental;
use itertools::Itertools;
use nom::Slice;
use std::ffi::c_void;
use std::fmt::{Debug, Display};
use std::io::Write;
use std::iter::FusedIterator;
use std::str::FromStr;

use crate::common::{BorrowedValue, Ty, Value};
use crate::Precision;

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

mod varbinary_view;
pub use varbinary_view::VarBinaryView;

mod geometry_view;
pub use geometry_view::GeometryView;

mod decimal_view;
pub(crate) use decimal_view::DecimalView;

mod blob_view;
pub use blob_view::BlobView;

mod schema;
pub(crate) use schema::*;

mod nulls;
pub(crate) use nulls::*;

mod offsets;
pub(crate) use offsets::*;

mod lengths;
pub(crate) use lengths::*;

mod from;

pub(crate) trait IsColumnView: Sized {
    #[allow(dead_code)]
    /// View item data type.
    fn ty(&self) -> Ty;

    #[allow(dead_code)]
    fn from_value_iter<'a>(iter: impl Iterator<Item = &'a Value>) -> Self {
        Self::from_borrowed_value_iter::<'a>(iter.map(|v| v.to_borrowed_value()))
    }

    fn from_borrowed_value_iter<'b>(iter: impl Iterator<Item = BorrowedValue<'b>>) -> Self;
}

/// Compatible version for var char.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Version {
    V2,
    V3,
}

#[derive(Clone)]
pub enum ColumnView {
    Bool(BoolView),              // 1
    TinyInt(TinyIntView),        // 2
    SmallInt(SmallIntView),      // 3
    Int(IntView),                // 4
    BigInt(BigIntView),          // 5
    Float(FloatView),            // 6
    Double(DoubleView),          // 7
    VarChar(VarCharView),        // 8
    Timestamp(TimestampView),    // 9
    NChar(NCharView),            // 10
    UTinyInt(UTinyIntView),      // 11
    USmallInt(USmallIntView),    // 12
    UInt(UIntView),              // 13
    UBigInt(UBigIntView),        // 14
    Json(JsonView),              // 15
    VarBinary(VarBinaryView),    // 16
    Decimal(DecimalView<i128>),  // 17
    Blob(BlobView),              // 18
    Geometry(GeometryView),      // 20
    Decimal64(DecimalView<i64>), // 21
}

unsafe impl Send for ColumnView {}
unsafe impl Sync for ColumnView {}

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
            Self::VarBinary(view) => f.debug_tuple("VarBinary").field(&view.to_vec()).finish(),
            Self::Decimal(view) => f.debug_tuple("Decimal").field(&view.to_vec()).finish(),
            Self::Decimal64(view) => f.debug_tuple("Decimal64").field(&view.to_vec()).finish(),
            Self::Geometry(view) => f.debug_tuple("Geometry").field(&view.to_vec()).finish(),
            Self::Blob(view) => f.debug_tuple("Blob").field(&view.to_vec()).finish(),
        }
    }
}

impl std::ops::Add for ColumnView {
    type Output = ColumnView;

    fn add(self, rhs: Self) -> Self::Output {
        self.concat(&rhs)
    }
}

#[derive(Debug)]
pub struct CastError {
    from: Ty,
    to: Ty,
    message: &'static str,
}

impl Display for CastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Cast from {} to {} error: {}",
            self.from, self.to, self.message
        ))
    }
}

impl std::error::Error for CastError {}

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

impl ExactSizeIterator for ColumnViewIter<'_> {
    fn len(&self) -> usize {
        self.view.len() - self.row
    }
}

impl FusedIterator for ColumnViewIter<'_> {}

impl<'a> IntoIterator for &'a ColumnView {
    type Item = BorrowedValue<'a>;

    type IntoIter = ColumnViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl ColumnView {
    pub fn from_millis_timestamp<T: Into<Option<i64>>>(values: Vec<T>) -> Self {
        ColumnView::Timestamp(TimestampView::from_millis(values))
    }

    pub fn from_micros_timestamp<T: Into<Option<i64>>>(values: Vec<T>) -> Self {
        ColumnView::Timestamp(TimestampView::from_micros(values))
    }

    pub fn from_nanos_timestamp<T: Into<Option<i64>>>(values: Vec<T>) -> Self {
        ColumnView::Timestamp(TimestampView::from_nanos(values))
    }

    pub fn from_bools<T: Into<Option<bool>>>(values: Vec<T>) -> Self {
        ColumnView::Bool(BoolView::from_iter(values))
    }

    pub fn from_tiny_ints<T: Into<Option<i8>>>(values: Vec<T>) -> Self {
        ColumnView::TinyInt(TinyIntView::from_iter(values))
    }

    pub fn from_small_ints<T: Into<Option<i16>>>(values: Vec<T>) -> Self {
        ColumnView::SmallInt(SmallIntView::from_iter(values))
    }

    pub fn from_ints<T: Into<Option<i32>>>(values: Vec<T>) -> Self {
        ColumnView::Int(IntView::from_iter(values))
    }

    pub fn from_big_ints<T: Into<Option<i64>>>(values: Vec<T>) -> Self {
        ColumnView::BigInt(BigIntView::from_iter(values))
    }

    pub fn from_unsigned_tiny_ints<T: Into<Option<u8>>>(values: Vec<T>) -> Self {
        ColumnView::UTinyInt(UTinyIntView::from_iter(values))
    }

    pub fn from_unsigned_small_ints<T: Into<Option<u16>>>(values: Vec<T>) -> Self {
        ColumnView::USmallInt(USmallIntView::from_iter(values))
    }

    pub fn from_unsigned_ints<T: Into<Option<u32>>>(values: Vec<T>) -> Self {
        ColumnView::UInt(UIntView::from_iter(values))
    }

    pub fn from_unsigned_big_ints<T: Into<Option<u64>>>(values: Vec<T>) -> Self {
        ColumnView::UBigInt(UBigIntView::from_iter(values))
    }

    pub fn from_floats<T: Into<Option<f32>>>(values: Vec<T>) -> Self {
        ColumnView::Float(FloatView::from_iter(values))
    }

    pub fn from_doubles<T: Into<Option<f64>>>(values: Vec<T>) -> Self {
        ColumnView::Double(DoubleView::from_iter(values))
    }

    pub fn from_decimal<I, T>(values: I, precision: u8, scale: u8) -> Self
    where
        T: Into<Option<i128>>,
        I: IntoIterator<Item = T>,
    {
        ColumnView::Decimal(DecimalView::<i128>::from_values(values, precision, scale))
    }

    pub fn from_decimal64<I, T>(values: I, precision: u8, scale: u8) -> Self
    where
        T: Into<Option<i64>>,
        I: IntoIterator<Item = T>,
    {
        ColumnView::Decimal64(DecimalView::<i64>::from_values(values, precision, scale))
    }

    pub fn from_varchar<
        S: AsRef<str>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::VarChar(VarCharView::from_iter(iter))
    }

    pub fn from_nchar<
        S: AsRef<str>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::NChar(NCharView::from_iter(iter))
    }

    pub fn from_json<
        S: AsRef<str>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::Json(JsonView::from_iter(iter))
    }

    pub fn from_bytes<
        S: AsRef<[u8]>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::VarBinary(VarBinaryView::from_iter(iter))
    }

    pub fn from_geobytes<
        S: AsRef<[u8]>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::Geometry(GeometryView::from_iter(iter))
    }

    pub fn from_blob_bytes<
        S: AsRef<[u8]>,
        T: Into<Option<S>>,
        I: ExactSizeIterator<Item = T>,
        V: IntoIterator<Item = T, IntoIter = I>,
    >(
        iter: V,
    ) -> Self {
        ColumnView::Blob(BlobView::from_iter(iter))
    }

    #[inline]
    pub fn concat_iter<'b, 'a: 'b, T: Iterator<Item = BorrowedValue<'b>>>(
        &'a self,
        rhs: T,
        ty: Ty,
    ) -> ColumnView {
        match ty {
            Ty::Null => unreachable!(),
            Ty::Bool => ColumnView::Bool(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::TinyInt => ColumnView::TinyInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::SmallInt => ColumnView::SmallInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Int => ColumnView::Int(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::BigInt => ColumnView::BigInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::UTinyInt => ColumnView::UTinyInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::USmallInt => ColumnView::USmallInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::UInt => ColumnView::UInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::UBigInt => ColumnView::UBigInt(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Float => ColumnView::Float(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Double => ColumnView::Double(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Timestamp => ColumnView::Timestamp(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::VarChar => ColumnView::VarChar(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::NChar => ColumnView::NChar(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Json => ColumnView::Json(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::VarBinary => ColumnView::VarBinary(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::Decimal | Ty::Decimal64 => {
                unimplemented!("Unable to determine the values for precision and scale")
            }
            Ty::Blob => ColumnView::Blob(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
            Ty::MediumBlob => todo!(),
            Ty::Geometry => ColumnView::Geometry(IsColumnView::from_borrowed_value_iter(
                self.iter().chain(rhs),
            )),
        }
    }

    /// Concatenate another column view, output a new column view with exact type of self.
    #[inline]
    pub fn concat(&self, rhs: &ColumnView) -> ColumnView {
        self.concat_as(rhs, self.as_ty())
    }

    /// Concatenate another column view strictly, output a new column view with exact type of self.
    ///
    /// # Panics
    ///
    /// Panics if the two column views have different types.
    #[inline]
    pub fn concat_strictly(&self, rhs: &ColumnView) -> ColumnView {
        match (self, rhs) {
            (ColumnView::Timestamp(a), ColumnView::Timestamp(b)) => {
                ColumnView::Timestamp(a.concat(b))
            }
            (ColumnView::Bool(a), ColumnView::Bool(b)) => ColumnView::Bool(a.concat(b)),
            (ColumnView::TinyInt(a), ColumnView::TinyInt(b)) => ColumnView::TinyInt(a.concat(b)),
            (ColumnView::UTinyInt(a), ColumnView::UTinyInt(b)) => ColumnView::UTinyInt(a.concat(b)),
            (ColumnView::SmallInt(a), ColumnView::SmallInt(b)) => ColumnView::SmallInt(a.concat(b)),
            (ColumnView::USmallInt(a), ColumnView::USmallInt(b)) => {
                ColumnView::USmallInt(a.concat(b))
            }
            (ColumnView::Int(a), ColumnView::Int(b)) => ColumnView::Int(a.concat(b)),
            (ColumnView::UInt(a), ColumnView::UInt(b)) => ColumnView::UInt(a.concat(b)),
            (ColumnView::BigInt(a), ColumnView::BigInt(b)) => ColumnView::BigInt(a.concat(b)),
            (ColumnView::UBigInt(a), ColumnView::UBigInt(b)) => ColumnView::UBigInt(a.concat(b)),
            (ColumnView::Float(a), ColumnView::Float(b)) => ColumnView::Float(a.concat(b)),
            (ColumnView::Double(a), ColumnView::Double(b)) => ColumnView::Double(a.concat(b)),
            (ColumnView::VarChar(a), ColumnView::VarChar(b)) => ColumnView::VarChar(a.concat(b)),
            (ColumnView::NChar(a), ColumnView::NChar(b)) => ColumnView::NChar(a.concat(b)),
            (ColumnView::Json(a), ColumnView::Json(b)) => ColumnView::Json(a.concat(b)),
            (ColumnView::VarBinary(_a), ColumnView::VarBinary(_b)) => todo!(),
            (ColumnView::Geometry(_a), ColumnView::Geometry(_b)) => todo!(),
            _ => panic!("strict concat needs same schema: {self:?}, {rhs:?}"),
        }
    }

    /// Concatenate another column view, output a new column view with specified type `ty`.
    #[inline]
    pub fn concat_as(&self, rhs: &ColumnView, ty: Ty) -> ColumnView {
        self.concat_iter(rhs.iter(), ty)
    }

    /// Generate single element view for specified type `ty`.
    pub fn null(n: usize, ty: Ty) -> Self {
        match ty {
            Ty::Null => panic!("type should be known"),
            Ty::Bool => Self::from_bools(vec![None; n]),
            Ty::TinyInt => Self::from_tiny_ints(vec![None; n]),
            Ty::SmallInt => Self::from_small_ints(vec![None; n]),
            Ty::Int => Self::from_ints(vec![None; n]),
            Ty::BigInt => Self::from_big_ints(vec![None; n]),
            Ty::UTinyInt => Self::from_unsigned_tiny_ints(vec![None; n]),
            Ty::USmallInt => Self::from_unsigned_small_ints(vec![None; n]),
            Ty::UInt => Self::from_unsigned_ints(vec![None; n]),
            Ty::UBigInt => Self::from_unsigned_big_ints(vec![None; n]),
            Ty::Float => Self::from_floats(vec![None; n]),
            Ty::Double => Self::from_doubles(vec![None; n]),
            Ty::Timestamp => Self::from_millis_timestamp(vec![None; n]),
            Ty::VarChar => Self::from_varchar::<&'static str, _, _, _>(vec![None; n]),
            Ty::NChar => Self::from_nchar::<&'static str, _, _, _>(vec![None; n]),
            Ty::Decimal | Ty::Decimal64 => {
                unimplemented!("Unable to determine the values for precision and scale")
            }
            _ => todo!(),
        }
    }

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
            ColumnView::VarBinary(view) => view.len(),
            ColumnView::Geometry(view) => view.len(),
            ColumnView::Decimal(view) => view.len(),
            ColumnView::Decimal64(view) => view.len(),
            ColumnView::Blob(view) => view.len(),
        }
    }

    pub fn max_variable_length(&self) -> usize {
        match self {
            ColumnView::Bool(_) | ColumnView::TinyInt(_) | ColumnView::UTinyInt(_) => 1,
            ColumnView::SmallInt(_) | ColumnView::USmallInt(_) => 2,
            ColumnView::Int(_) | ColumnView::UInt(_) | ColumnView::Float(_) => 4,
            ColumnView::BigInt(_)
            | ColumnView::Double(_)
            | ColumnView::UBigInt(_)
            | ColumnView::Timestamp(_)
            | ColumnView::Decimal64(_) => 8,
            ColumnView::Decimal(_) => 16,
            ColumnView::VarChar(view) => view.max_length(),
            ColumnView::NChar(view) => view.max_length(),
            ColumnView::Json(view) => view.max_length(),
            ColumnView::VarBinary(view) => view.max_length(),
            ColumnView::Geometry(view) => view.max_length(),
            ColumnView::Blob(view) => view.max_length(),
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
            ColumnView::VarBinary(view) => view.is_null_unchecked(row),
            ColumnView::Geometry(view) => view.is_null_unchecked(row),
            ColumnView::Decimal(view) => view.is_null_unchecked(row),
            ColumnView::Decimal64(view) => view.is_null_unchecked(row),
            ColumnView::Blob(view) => view.is_null_unchecked(row),
        }
    }

    pub fn get(&self, row: usize) -> Option<BorrowedValue<'_>> {
        if row < self.len() {
            Some(unsafe { self.get_ref_unchecked(row) })
        } else {
            None
        }
    }

    /// Get one value at `row` index of the column view.
    #[inline]
    pub(super) unsafe fn get_ref_unchecked(&self, row: usize) -> BorrowedValue<'_> {
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
            ColumnView::VarBinary(view) => view.get_value_unchecked(row),
            ColumnView::Geometry(view) => view.get_value_unchecked(row),
            ColumnView::Decimal(view) => view.get_value_unchecked(row),
            ColumnView::Decimal64(view) => view.get_value_unchecked(row),
            ColumnView::Blob(view) => view.get_value_unchecked(row),
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
            ColumnView::VarBinary(view) => view.get_raw_value_unchecked(row),
            ColumnView::Geometry(view) => view.get_raw_value_unchecked(row),
            ColumnView::Decimal(view) => view.get_raw_value_unchecked(row),
            ColumnView::Decimal64(view) => view.get_raw_value_unchecked(row),
            ColumnView::Blob(view) => view.get_raw_value_unchecked(row),
        }
    }

    pub fn iter(&self) -> ColumnViewIter<'_> {
        ColumnViewIter { view: self, row: 0 }
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> Option<Self> {
        match self {
            ColumnView::Bool(view) => view.slice(range).map(ColumnView::Bool),
            ColumnView::TinyInt(view) => view.slice(range).map(ColumnView::TinyInt),
            ColumnView::SmallInt(view) => view.slice(range).map(ColumnView::SmallInt),
            ColumnView::Int(view) => view.slice(range).map(ColumnView::Int),
            ColumnView::BigInt(view) => view.slice(range).map(ColumnView::BigInt),
            ColumnView::Float(view) => view.slice(range).map(ColumnView::Float),
            ColumnView::Double(view) => view.slice(range).map(ColumnView::Double),
            ColumnView::VarChar(view) => view.slice(range).map(ColumnView::VarChar),
            ColumnView::Timestamp(view) => view.slice(range).map(ColumnView::Timestamp),
            ColumnView::NChar(view) => view.slice(range).map(ColumnView::NChar),
            ColumnView::UTinyInt(view) => view.slice(range).map(ColumnView::UTinyInt),
            ColumnView::USmallInt(view) => view.slice(range).map(ColumnView::USmallInt),
            ColumnView::UInt(view) => view.slice(range).map(ColumnView::UInt),
            ColumnView::UBigInt(view) => view.slice(range).map(ColumnView::UBigInt),
            ColumnView::Json(view) => view.slice(range).map(ColumnView::Json),
            ColumnView::Decimal(view) => view.slice(range).map(ColumnView::Decimal),
            ColumnView::Decimal64(view) => view.slice(range).map(ColumnView::Decimal64),
            _ => todo!(),
        }
    }

    pub(super) fn write_raw_into<W: Write>(&self, wtr: &mut W) -> std::io::Result<usize> {
        match self {
            ColumnView::Bool(view) => view.write_raw_into(wtr),
            ColumnView::TinyInt(view) => view.write_raw_into(wtr),
            ColumnView::SmallInt(view) => view.write_raw_into(wtr),
            ColumnView::Int(view) => view.write_raw_into(wtr),
            ColumnView::BigInt(view) => view.write_raw_into(wtr),
            ColumnView::Float(view) => view.write_raw_into(wtr),
            ColumnView::Double(view) => view.write_raw_into(wtr),
            ColumnView::VarChar(view) => view.write_raw_into(wtr),
            ColumnView::Timestamp(view) => view.write_raw_into(wtr),
            ColumnView::NChar(view) => view.write_raw_into(wtr),
            ColumnView::UTinyInt(view) => view.write_raw_into(wtr),
            ColumnView::USmallInt(view) => view.write_raw_into(wtr),
            ColumnView::UInt(view) => view.write_raw_into(wtr),
            ColumnView::UBigInt(view) => view.write_raw_into(wtr),
            ColumnView::Json(view) => view.write_raw_into(wtr),
            ColumnView::VarBinary(view) => view.write_raw_into(wtr),
            ColumnView::Geometry(view) => view.write_raw_into(wtr),
            ColumnView::Decimal(view) => view.write_raw_into(wtr),
            ColumnView::Decimal64(view) => view.write_raw_into(wtr),
            ColumnView::Blob(view) => view.write_raw_into(wtr),
        }
    }

    pub fn as_ty(&self) -> Ty {
        match self {
            ColumnView::Bool(_) => Ty::Bool,
            ColumnView::TinyInt(_) => Ty::TinyInt,
            ColumnView::SmallInt(_) => Ty::SmallInt,
            ColumnView::Int(_) => Ty::Int,
            ColumnView::BigInt(_) => Ty::BigInt,
            ColumnView::Float(_) => Ty::Float,
            ColumnView::Double(_) => Ty::Double,
            ColumnView::VarChar(_) => Ty::VarChar,
            ColumnView::Timestamp(_) => Ty::Timestamp,
            ColumnView::NChar(_) => Ty::NChar,
            ColumnView::UTinyInt(_) => Ty::UTinyInt,
            ColumnView::USmallInt(_) => Ty::USmallInt,
            ColumnView::UInt(_) => Ty::UInt,
            ColumnView::UBigInt(_) => Ty::UBigInt,
            ColumnView::Json(_) => Ty::Json,
            ColumnView::VarBinary(_) => Ty::VarBinary,
            ColumnView::Geometry(_) => Ty::Geometry,
            ColumnView::Decimal(_) => Ty::Decimal,
            ColumnView::Decimal64(_) => Ty::Decimal64,
            ColumnView::Blob(_) => Ty::Blob,
        }
    }

    pub fn as_raw_ptr(&self) -> *const c_void {
        match self {
            ColumnView::Bool(view) => view.as_raw_ptr() as _,
            ColumnView::TinyInt(view) => view.as_raw_ptr() as _,
            ColumnView::SmallInt(view) => view.as_raw_ptr() as _,
            ColumnView::Int(view) => view.as_raw_ptr() as _,
            ColumnView::BigInt(view) => view.as_raw_ptr() as _,
            ColumnView::UTinyInt(view) => view.as_raw_ptr() as _,
            ColumnView::USmallInt(view) => view.as_raw_ptr() as _,
            ColumnView::UBigInt(view) => view.as_raw_ptr() as _,
            ColumnView::UInt(view) => view.as_raw_ptr() as _,
            ColumnView::Float(view) => view.as_raw_ptr() as _,
            ColumnView::Double(view) => view.as_raw_ptr() as _,
            ColumnView::Timestamp(view) => view.as_raw_ptr() as _,
            ColumnView::VarChar(view) => view.as_raw_ptr() as _,
            ColumnView::NChar(view) => view.as_raw_ptr() as _,
            ColumnView::Json(view) => view.as_raw_ptr() as _,
            ColumnView::VarBinary(view) => view.as_raw_ptr() as _,
            ColumnView::Geometry(view) => view.as_raw_ptr() as _,
            ColumnView::Decimal(view) => view.as_raw_ptr() as _,
            ColumnView::Decimal64(view) => view.as_raw_ptr() as _,
            ColumnView::Blob(view) => view.as_raw_ptr() as _,
        }
    }

    /// Get the schema of the column view.
    ///
    /// The schema is used to describe the column's type and properties.
    /// It includes the type, maximum variable length, precision, and scale for decimal types.
    /// If the column view is a decimal type, it will return the precision and scale.
    /// If it is not a decimal type, it will return the type and maximum variable length.
    pub fn schema(&self) -> DataType {
        let ty = self.as_ty();
        if ty.is_decimal() {
            let (precision, scale) = match self {
                ColumnView::Decimal(view) => view.precision_and_scale(),
                ColumnView::Decimal64(view) => view.precision_and_scale(),
                _ => unreachable!(),
            };
            DataType::new_decimal(ty, precision, scale)
        } else {
            DataType::new(ty, self.max_variable_length() as _)
        }
    }

    /// Cast behaviors:
    ///
    /// - BOOL to VARCHAR/NCHAR: true => "true", false => "false"
    /// - numeric(integers/float/double) to string(varchar/nchar): like print or to_string.
    /// - string to primitive: can be parsed => primitive, others => null.
    /// - timestamp to string: RFC3339 with localized timezone.
    ///
    /// Not supported:
    /// - any to timestamp
    pub fn cast(&self, ty: Ty) -> Result<ColumnView, CastError> {
        let l_ty = self.as_ty();
        if l_ty == ty {
            return Ok(self.clone());
        }
        use Ty::*;
        match self {
            ColumnView::Bool(booleans) => {
                macro_rules! _cast_bool_to {
                    ($ty:ty) => {
                        booleans
                            .iter()
                            .map(|v| v.map(|b| if b { 1 as $ty } else { 0 as $ty }))
                            .collect_vec()
                    };
                }

                let view = match ty {
                    TinyInt => Self::from_tiny_ints(_cast_bool_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_bool_to!(i16)),
                    Int => Self::from_ints(_cast_bool_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_bool_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_bool_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_bool_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_bool_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_bool_to!(u64)),
                    Float => Self::from_floats(_cast_bool_to!(f32)),
                    Double => Self::from_doubles(_cast_bool_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::TinyInt(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(view.iter().map(|v| v.map(|b| b > 0)).collect_vec()),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "tinyint can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::SmallInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "smallint can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::Int(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(view.iter().map(|v| v.map(|b| b > 0)).collect_vec()),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "int can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::BigInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "bigint can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::UTinyInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "unsigned tinyint can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::USmallInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "unsigned smallint can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::UInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "unsigned int can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::UBigInt(booleans) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        booleans.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => {
                        Self::from_bools(booleans.iter().map(|v| v.map(|b| b > 0)).collect_vec())
                    }
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        booleans.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "unsigned big int can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::Float(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(view.iter().map(|v| v.map(|b| b > 0.0)).collect_vec()),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::Double(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter().map(|v| v.map(|b| b as $ty)).collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(view.iter().map(|v| v.map(|b| b > 0.0)).collect_vec()),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    Decimal64 => {
                        // 遍历 view，找到最大的有效位数和小数点后位数
                        let (max_precision, max_scale) =
                            view.iter().flatten().fold((0, 0), |(max_p, max_s), b| {
                                let s = b.to_string();
                                let parts: Vec<&str> = s.split('.').collect();
                                let p = parts[0].len() + parts.get(1).map_or(0, |s| s.len());
                                let s = parts.get(1).map_or(0, |s| s.len());
                                (max_p.max(p), max_s.max(s))
                            });
                        if max_precision > 18 {
                            return Err(CastError {
                                from: l_ty,
                                to: ty,
                                message: "decimal64 overflow",
                            });
                        }
                        // 将 f64 转换为 i64
                        Self::from_decimal64(
                            view.iter().map(|v| {
                                v.map(|b| {
                                    let s = b.to_string();
                                    let parts: Vec<&str> = s.split('.').collect();
                                    let scale = parts.get(1).map_or(0, |s| s.len());
                                    let mut s = s.replace(".", "");
                                    // 如果 s.len() < max_precision 将 s 后面补零
                                    if scale < max_scale {
                                        for _ in scale..max_scale {
                                            s.push('0');
                                        }
                                    }
                                    tracing::trace!(
                                        "decimal64, precision: {}, scale: {}, b: {:?}, s: {:?}",
                                        max_precision,
                                        max_scale,
                                        b,
                                        s
                                    );
                                    let b: i64 = s.parse().unwrap();
                                    b
                                })
                            }),
                            max_precision as u8,
                            max_scale as u8,
                        )
                    }
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "double cast error",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::VarChar(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter()
                            .map(|v| v.and_then(|b| b.as_str().parse::<$ty>().ok()))
                            .collect_vec()
                    };
                }
                let view = match ty {
                    Bool => Self::from_bools(_cast_to!(bool)),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "booleans can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::NChar(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter()
                            .map(|v| v.and_then(|b| b.parse::<$ty>().ok()))
                            .collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(_cast_to!(bool)),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter().map(|v| v.map(|b| b.to_string())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "can be casted to primitive types only",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::Timestamp(view) => {
                macro_rules! _cast_to {
                    ($ty:ty) => {
                        view.iter()
                            .map(|v| v.map(|b| b.as_raw_i64() as $ty))
                            .collect_vec()
                    };
                }

                let view = match ty {
                    Bool => Self::from_bools(
                        view.iter()
                            .map(|v| v.map(|b| b.as_raw_i64() > 0))
                            .collect_vec(),
                    ),
                    TinyInt => Self::from_tiny_ints(_cast_to!(i8)),
                    SmallInt => Self::from_small_ints(_cast_to!(i16)),
                    Int => Self::from_ints(_cast_to!(i32)),
                    BigInt => Self::from_big_ints(_cast_to!(i64)),
                    UTinyInt => Self::from_unsigned_tiny_ints(_cast_to!(u8)),
                    USmallInt => Self::from_unsigned_small_ints(_cast_to!(u16)),
                    UInt => Self::from_unsigned_ints(_cast_to!(u32)),
                    UBigInt => Self::from_unsigned_big_ints(_cast_to!(u64)),
                    Float => Self::from_floats(_cast_to!(f32)),
                    Double => Self::from_doubles(_cast_to!(f64)),
                    VarChar => Self::from_varchar::<String, _, _, _>(
                        view.iter()
                            .map(|v| v.map(|b| b.to_datetime_with_tz().to_rfc3339())),
                    ),
                    NChar => Self::from_nchar::<String, _, _, _>(
                        view.iter()
                            .map(|v| v.map(|b| b.to_datetime_with_tz().to_rfc3339())),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "Timestamp cast error",
                        })
                    }
                };
                Ok(view)
            }
            ColumnView::Decimal64(view) => {
                let view = match ty {
                    Double => Self::from_doubles(
                        view.iter()
                            .map(|v| v.and_then(|b| b.as_bigdecimal().to_f64()))
                            .collect_vec(),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "decimal64 can be casted to double only",
                        });
                    }
                };
                Ok(view)
            }
            ColumnView::Decimal(view) => {
                let view = match ty {
                    Double => Self::from_doubles(
                        view.iter()
                            .map(|v| v.and_then(|b| b.as_bigdecimal().to_f64()))
                            .collect_vec(),
                    ),
                    _ => {
                        return Err(CastError {
                            from: l_ty,
                            to: ty,
                            message: "decimal can be casted to double only",
                        });
                    }
                };
                Ok(view)
            }
            _ => todo!(),
        }
    }

    pub fn cast_origin_type(&self, ty: Ty, origin_ty: &str) -> Result<ColumnView, CastError> {
        let l_ty = self.as_ty();
        if l_ty == ty {
            return Ok(self.clone());
        }
        match (self, ty) {
            (ColumnView::Double(d_view), Ty::Decimal64) => {
                // origin_ty 的格式 DECIMAL(precision, scale)， 从中解析出 precision 和 scale
                let start = origin_ty.find('(').ok_or(CastError {
                    from: Ty::Double,
                    to: Ty::Decimal64,
                    message: "invalid decimal64 type",
                })?;
                let end = origin_ty.find(')').ok_or(CastError {
                    from: Ty::Double,
                    to: Ty::Decimal64,
                    message: "invalid decimal64 type",
                })?;
                let params = origin_ty.slice(start + 1..end);
                let mut parts = params.split(',').map(|x| x.trim());
                let precision = parts
                    .next()
                    .and_then(|p| p.parse::<u8>().ok())
                    .expect("precision must be specified in origin_ty");
                let scale = parts
                    .next()
                    .and_then(|s| s.parse::<u8>().ok())
                    .expect("scale must be specified in origin_ty");

                Ok(Self::from_decimal64(
                    d_view.iter().map(|v| {
                        v.map(|b| {
                            let s = b.to_string();
                            let parts: Vec<&str> = s.split('.').collect();
                            let scale_cur = parts.get(1).map_or(0, |s| s.len().as_u8());
                            let mut s = s.replace(".", "");
                            // 如果 scale_cur < scale 将 s 后面补零
                            if scale_cur < scale {
                                for _ in scale_cur..scale {
                                    s.push('0');
                                }
                            }
                            let b: i64 = s.parse().unwrap();
                            b
                        })
                    }),
                    precision,
                    scale,
                ))
            }
            (ColumnView::Double(d_view), Ty::Decimal) => {
                // origin_ty 的格式 DECIMAL(precision, scale)， 从中解析出 precision 和 scale
                let start = origin_ty.find('(').ok_or(CastError {
                    from: Ty::Double,
                    to: Ty::Decimal,
                    message: "invalid decimal type",
                })?;
                let end = origin_ty.find(')').ok_or(CastError {
                    from: Ty::Double,
                    to: Ty::Decimal,
                    message: "invalid decimal type",
                })?;
                let params = origin_ty.slice(start + 1..end);
                let mut parts = params.split(',').map(|x| x.trim());
                let precision = parts
                    .next()
                    .and_then(|p| p.parse::<u8>().ok())
                    .expect("precision must be specified in origin_ty");
                let scale = parts
                    .next()
                    .and_then(|s| s.parse::<u8>().ok())
                    .expect("scale must be specified in origin_ty");

                Ok(Self::from_decimal(
                    d_view.iter().map(|v| {
                        v.map(|b| {
                            let s = b.to_string();
                            let parts: Vec<&str> = s.split('.').collect();
                            let scale_cur = parts.get(1).map_or(0, |s| s.len().as_u8());
                            let mut s = s.replace(".", "");
                            // 如果 scale_cur < scale 将 s 后面补零
                            if scale_cur < scale {
                                for _ in scale_cur..scale {
                                    s.push('0');
                                }
                            }
                            let b: i128 = s.parse().unwrap();
                            tracing::info!("b: {b}, precision: {precision}, scale: {scale}");
                            b
                        })
                    }),
                    precision,
                    scale,
                ))
            }
            _ => todo!(),
        }
    }

    pub fn cast_with_schema(&self, schema: &DataType) -> Result<ColumnView, CastError> {
        let ty = schema.ty();
        if !ty.is_decimal() {
            self.cast(ty)
        } else {
            let prec_scale = schema.as_prec_scale_unchecked();
            macro_rules! to_decimal {
                ($iter:expr) => {{
                    let iter = { $iter };
                    match ty {
                        Ty::Decimal => Ok(ColumnView::Decimal(
                            DecimalView::<i128>::from_bigdecimal_with(iter, prec_scale),
                        )),
                        Ty::Decimal64 => Ok(ColumnView::Decimal64(
                            DecimalView::<i64>::from_bigdecimal_with(iter, prec_scale),
                        )),
                        _ => unreachable!(),
                    }
                }};
            }
            match self {
                ColumnView::Bool(view) => {
                    to_decimal!(view
                        .iter()
                        .map(|v| v.and_then(|v| BigDecimal::from_i8(v as i8))))
                }
                ColumnView::TinyInt(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_i8)))
                }
                ColumnView::SmallInt(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_i16)))
                }
                ColumnView::Int(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_i32)))
                }
                ColumnView::BigInt(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_i64)))
                }
                ColumnView::Float(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_f32)))
                }
                ColumnView::Double(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_f64)))
                }
                ColumnView::Timestamp(view) => {
                    to_decimal!(view.iter().map(|v| {
                        v.and_then(|v| {
                            let ts = v.as_raw_i64();
                            BigDecimal::from_i64(ts)
                        })
                    }))
                }
                ColumnView::UTinyInt(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_u8)))
                }
                ColumnView::USmallInt(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_u16)))
                }
                ColumnView::UInt(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_u32)))
                }
                ColumnView::UBigInt(view) => {
                    to_decimal!(view.iter().map(|v| v.and_then(BigDecimal::from_u64)))
                }
                ColumnView::VarChar(view) => {
                    to_decimal!(view
                        .iter()
                        .map(|v| { v.and_then(|s| { BigDecimal::from_str(s.as_str()).ok() }) }))
                }
                ColumnView::NChar(view) => {
                    to_decimal!(view
                        .iter()
                        .map(|v| { v.and_then(|s| { BigDecimal::from_str(s).ok() }) }))
                }
                ColumnView::Decimal(view) => {
                    to_decimal!(view.iter().map(|v| v.map(|d| { d.as_bigdecimal() })))
                }
                ColumnView::Decimal64(view) => {
                    to_decimal!(view.iter().map(|v| v.map(|d| { d.as_bigdecimal() })))
                }
                ColumnView::Json(_) => Err(CastError {
                    from: self.as_ty(),
                    to: ty,
                    message: "json can not be casted to decimal type",
                }),
                ColumnView::VarBinary(_) => Err(CastError {
                    from: self.as_ty(),
                    to: ty,
                    message: "varbinary can not be casted to decimal type",
                }),
                ColumnView::Geometry(_) => Err(CastError {
                    from: self.as_ty(),
                    to: ty,
                    message: "geometry can not be casted to decimal type",
                }),
                ColumnView::Blob(_) => Err(CastError {
                    from: self.as_ty(),
                    to: ty,
                    message: "blob can not be casted to decimal type",
                }),
            }
        }
    }

    pub fn cast_precision(&self, precision: Precision) -> ColumnView {
        match self {
            ColumnView::Timestamp(view) => ColumnView::Timestamp(view.cast_precision(precision)),
            _ => self.clone(),
        }
    }

    pub unsafe fn as_timestamp_view(&self) -> &TimestampView {
        match self {
            ColumnView::Timestamp(view) => view,
            _ => unreachable!(),
        }
    }
}

pub fn views_to_raw_block(views: &[ColumnView]) -> Vec<u8> {
    let header = super::Header {
        nrows: views.first().map_or(0, ColumnView::len) as _,
        ncols: views.len() as _,
        ..Default::default()
    };

    let ncols = views.len();

    let mut bytes = Vec::new();
    bytes.extend(header.as_bytes());

    let schemas = views.iter().map(|view| view.schema()).collect_vec();
    let schema_bytes = unsafe {
        std::slice::from_raw_parts(
            schemas.as_ptr() as *const u8,
            ncols * std::mem::size_of::<DataType>(),
        )
    };
    bytes.write_all(schema_bytes).unwrap();

    let length_offset = bytes.len();
    bytes.resize(bytes.len() + ncols * std::mem::size_of::<u32>(), 0);

    let mut lengths = vec![0; ncols];
    for (i, view) in views.iter().enumerate() {
        let cur = bytes.len();
        let n = view.write_raw_into(&mut bytes).unwrap();
        let len = bytes.len();
        debug_assert!(cur + n == len);
        if !view.as_ty().is_primitive() {
            lengths[i] = (n - header.nrows() * 4) as _;
        } else {
            lengths[i] = (header.nrows() * view.as_ty().fixed_length()) as _;
        }
    }
    unsafe {
        (*(bytes.as_mut_ptr() as *mut super::Header)).length = bytes.len() as _;
        std::ptr::copy_nonoverlapping(
            lengths.as_ptr() as *mut u8,
            bytes.as_mut_ptr().add(length_offset),
            lengths.len() * std::mem::size_of::<u32>(),
        );
    }
    bytes
}

impl From<Value> for ColumnView {
    fn from(value: Value) -> Self {
        match value {
            Value::Null(ty) => ColumnView::null(1, ty),
            Value::Bool(v) => vec![v].into(),
            Value::TinyInt(v) => vec![v].into(),
            Value::SmallInt(v) => vec![v].into(),
            Value::Int(v) => vec![v].into(),
            Value::BigInt(v) => vec![v].into(),
            Value::Float(v) => vec![v].into(),
            Value::Double(v) => vec![v].into(),
            Value::VarChar(v) => ColumnView::from_varchar::<String, _, _, _>(vec![v]),
            Value::Timestamp(v) => ColumnView::Timestamp(TimestampView::from_timestamp(vec![v])),
            Value::NChar(v) => ColumnView::from_nchar::<String, _, _, _>(vec![v]),
            Value::UTinyInt(v) => vec![v].into(),
            Value::USmallInt(v) => vec![v].into(),
            Value::UInt(v) => vec![v].into(),
            Value::UBigInt(v) => vec![v].into(),
            _ => todo!(),
        }
    }
}

macro_rules! _impl_from_iter {
    ($(($view:ident, $item:ty)),+ $(,)?) => {
        $(
            impl<A: Into<Option<$item>>> FromIterator<A> for $view {
                fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
                    let (nulls, mut values): (Vec<bool>, Vec<_>) = iter
                        .into_iter()
                        .map(|v| match v.into() {
                            Some(v) => (false, v),
                            None => (true, <$item>::default()),
                        })
                        .unzip();

                    Self {
                        nulls: NullBits::from_iter(nulls),
                        data: bytes::Bytes::from({
                            let (ptr, len, cap) = (values.as_mut_ptr(), values.len(), values.capacity());
                            std::mem::forget(values);

                            let item_size = std::mem::size_of::<$item>();

                            #[cfg(target_endian = "little")]
                            unsafe {
                                Vec::from_raw_parts(ptr as *mut u8, len * item_size, cap * item_size)
                            }

                            #[cfg(target_endian = "big")]
                            {
                                let mut bytes = unsafe {
                                    Vec::from_raw_parts(ptr as *mut u8, len * item_size, cap * item_size)
                                };
                                for i in (0..bytes.len()).step_by(item_size) {
                                    let j = i + item_size;
                                    let val = <$item>::from_ne_bytes(&bytes[i..j].try_into().expect("slice with incorrect length"));
                                    bytes[i..j].copy_from_slice(&val.to_le_bytes());
                                }
                                bytes
                            }
                        }),
                    }
                }
            }
        )+
    };
}

_impl_from_iter!(
    (BoolView, bool),
    (TinyIntView, i8),
    (SmallIntView, i16),
    (IntView, i32),
    (BigIntView, i64),
    (UTinyIntView, u8),
    (USmallIntView, u16),
    (UIntView, u32),
    (UBigIntView, u64),
    (FloatView, f32),
    (DoubleView, f64),
);

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::common::decimal::Decimal;

    #[test]
    fn test_column_view_null() {
        let null_value = Value::Null(Ty::Int);
        let null_column_view = ColumnView::from(null_value);
        assert_eq!(null_column_view.len(), 1);
        assert_eq!(null_column_view.as_ty(), Ty::Int);
    }

    #[test]
    fn test_column_view_bool() {
        let bool_value = Value::Bool(true);
        let bool_column_view = ColumnView::from(bool_value);
        assert_eq!(bool_column_view.len(), 1);
        assert_eq!(bool_column_view.as_ty(), Ty::Bool);
        assert_eq!(ColumnView::null(1, Ty::Bool).len(), 1);
        assert_eq!(bool_column_view.max_variable_length(), 1);
        println!("{:?}", bool_column_view.slice(0..1));
        println!("{:?}", bool_column_view.cast(Ty::TinyInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::SmallInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::Int).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::BigInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::UTinyInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::USmallInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::UInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::UBigInt).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::Float).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::Double).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::VarChar).unwrap());
        println!("{:?}", bool_column_view.cast(Ty::NChar).unwrap());
    }

    #[test]
    fn test_concat_iter() {
        let values = [
            BorrowedValue::Int(7),
            BorrowedValue::UInt(8),
            BorrowedValue::Int(9),
        ];

        let column_view_int = ColumnView::from(vec![1, 2, 3]);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Bool);
        assert_eq!(res.len(), 6);
        let column_view_int = ColumnView::from(vec![1, 2, 3]);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Bool);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::TinyInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::SmallInt);
        assert_eq!(res.len(), 6);
        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::TinyInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::SmallInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Int);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::BigInt);
        assert_eq!(res.len(), 6);
        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Int);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::BigInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::UTinyInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::USmallInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::UInt);
        assert_eq!(res.len(), 6);
        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::UTinyInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::USmallInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::UInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::UBigInt);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Float);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Double);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::VarChar);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::NChar);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Json);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::VarBinary);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Blob);
        assert_eq!(res.len(), 6);

        let res = column_view_int.concat_iter(values.iter().cloned(), Ty::Geometry);
        assert_eq!(res.len(), 6);
    }

    #[test]
    fn decimal_column_view_test() -> anyhow::Result<()> {
        let view = ColumnView::from_decimal([Some(12345), None, Some(333)], 10, 2);
        assert_eq!(format!("{view:?}"), "Decimal([Some(Decimal { data: 12345, precision: 10, scale: 2 }), None, Some(Decimal { data: 333, precision: 10, scale: 2 })])");
        assert_eq!(view.len(), 3);
        assert_eq!(view.max_variable_length(), 16);
        assert!(!unsafe { view.is_null_unchecked(0) });
        assert!(unsafe { view.is_null_unchecked(1) });
        assert!(!unsafe { view.is_null_unchecked(2) });
        assert_eq!(
            view.get(0),
            Some(BorrowedValue::Decimal(Decimal::new(12345, 10, 2)))
        );
        assert_eq!(view.get(1), Some(BorrowedValue::Null(Ty::Decimal)));
        assert_eq!(
            view.get(2),
            Some(BorrowedValue::Decimal(Decimal::new(333, 10, 2)))
        );
        let slice = view.slice(1..3).unwrap();
        assert_eq!(slice.len(), 2);
        assert_eq!(slice.get(0), Some(BorrowedValue::Null(Ty::Decimal)));
        assert_eq!(
            slice.get(1),
            Some(BorrowedValue::Decimal(Decimal::new(333, 10, 2)))
        );

        let view = ColumnView::from_decimal64([Some(12345), None, Some(333)], 10, 0);
        assert_eq!(format!("{view:?}"), "Decimal64([Some(Decimal { data: 12345, precision: 10, scale: 0 }), None, Some(Decimal { data: 333, precision: 10, scale: 0 })])");
        assert_eq!(view.len(), 3);
        assert_eq!(view.max_variable_length(), 8);
        assert!(!unsafe { view.is_null_unchecked(0) });
        assert!(unsafe { view.is_null_unchecked(1) });
        assert!(!unsafe { view.is_null_unchecked(2) });
        assert_eq!(
            view.get(0),
            Some(BorrowedValue::Decimal64(Decimal::new(12345, 10, 0)))
        );
        assert_eq!(view.get(1), Some(BorrowedValue::Null(Ty::Decimal64)));
        assert_eq!(
            view.get(2),
            Some(BorrowedValue::Decimal64(Decimal::new(333, 10, 0)))
        );
        let slice = view.slice(1..3).unwrap();
        assert_eq!(slice.len(), 2);
        assert_eq!(slice.get(0), Some(BorrowedValue::Null(Ty::Decimal64)));
        assert_eq!(
            slice.get(1),
            Some(BorrowedValue::Decimal64(Decimal::new(333, 10, 0)))
        );

        Ok(())
    }

    #[test]
    fn any_to_decimal_view() {
        let schema = DataType::from_str("DECIMAL(10,2)").unwrap();
        assert_eq!(schema.to_string(), "DECIMAL(10,2)");
        let view = ColumnView::from_bools(vec![true, false, true]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_tiny_ints(vec![1, 2, 3]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_small_ints(vec![1, 2, 3]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_ints(vec![1, 2, 3]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_big_ints(vec![1, 2, 3]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_unsigned_tiny_ints(vec![1, 2, 3]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_unsigned_small_ints(vec![1, 2, 3]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_unsigned_ints(vec![1, 2, 3]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_unsigned_big_ints(vec![1, 2, 3]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.00");

        let view = ColumnView::from_floats(vec![1.23f32, 2.0, 3.0]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.23");

        let view = ColumnView::from_doubles(vec![1.23f64, 2.0, 3.0]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(
            dbg!(decimal_view.get(0)).unwrap().to_string().unwrap(),
            "1.23"
        );

        let view = ColumnView::from_varchar(vec!["1.23", "2.0", "3.0"]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.23");

        let view = ColumnView::from_nchar(vec!["1.23", "2.0", "3.0"]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1.23");

        let view = ColumnView::from_millis_timestamp(vec![1234567890]);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(
            decimal_view.get(0).unwrap().to_string().unwrap(),
            "1234567890.00"
        );

        let view = ColumnView::from_decimal(vec![12345], 10, 1);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1234.50");

        let view = ColumnView::from_decimal64(vec![12345], 10, 1);
        let decimal_view = view.cast_with_schema(&schema).unwrap();
        assert_eq!(decimal_view.get(0).unwrap().to_string().unwrap(), "1234.50");

        let view = ColumnView::from_json(vec![serde_json::json!({"key": "value"}).to_string()]);
        let decimal_view = view.cast_with_schema(&schema);
        assert!(decimal_view.is_err());

        let view = ColumnView::from_bytes(vec![bytes::Bytes::from("test")]);
        let decimal_view = view.cast_with_schema(&schema);
        assert!(decimal_view.is_err());

        let view = ColumnView::from_geobytes(vec!["", ""]);
        let decimal_view = view.cast_with_schema(&schema);
        assert!(decimal_view.is_err());

        let view = ColumnView::from_blob_bytes(vec!["", ""]);
        let decimal_view = view.cast_with_schema(&schema);
        assert!(decimal_view.is_err());
    }

    #[test]
    fn test_blob_column_view() -> anyhow::Result<()> {
        let view = ColumnView::from_blob_bytes::<Vec<u8>, _, _, _>([
            Some(vec![1, 2, 3, 4]),
            None,
            Some(vec![2, 3, 3]),
        ]);

        assert_eq!(view.as_ty(), Ty::Blob);
        assert_eq!(view.len(), 3);
        assert_eq!(view.max_variable_length(), 4);
        assert_eq!(
            format!("{view:?}"),
            "Blob([Some([1, 2, 3, 4]), None, Some([2, 3, 3])])"
        );

        assert!(!view.as_raw_ptr().is_null());

        unsafe {
            assert!(!view.is_null_unchecked(0));
            assert!(view.is_null_unchecked(1));
            assert!(!view.is_null_unchecked(2));
        }

        assert_eq!(
            view.get(0),
            Some(BorrowedValue::Blob(Cow::from(vec![1, 2, 3, 4])))
        );
        assert_eq!(view.get(1), Some(BorrowedValue::Null(Ty::Blob)));
        assert_eq!(
            view.get(2),
            Some(BorrowedValue::Blob(Cow::from(vec![2, 3, 3])))
        );

        unsafe {
            assert_eq!(
                view.get_ref_unchecked(0),
                BorrowedValue::Blob(Cow::from(vec![1, 2, 3, 4]))
            );
            assert_eq!(view.get_ref_unchecked(1), BorrowedValue::Null(Ty::Blob));
            assert_eq!(
                view.get_ref_unchecked(2),
                BorrowedValue::Blob(Cow::from(vec![2, 3, 3]))
            );
        }

        unsafe {
            let (ty, size, ptr) = view.get_raw_value_unchecked(0);
            assert_eq!(ty, Ty::Blob);
            assert_eq!(size, 4);
            assert!(!ptr.is_null());

            let (ty, size, ptr) = view.get_raw_value_unchecked(1);
            assert_eq!(ty, Ty::Blob);
            assert_eq!(size, 0);
            assert!(ptr.is_null());

            let (ty, size, ptr) = view.get_raw_value_unchecked(2);
            assert_eq!(ty, Ty::Blob);
            assert_eq!(size, 3);
            assert!(!ptr.is_null());
        }

        Ok(())
    }
}
