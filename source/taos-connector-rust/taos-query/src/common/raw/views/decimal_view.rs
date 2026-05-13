use std::ffi::{c_char, c_void, CString};
use std::marker::PhantomData;
use std::ptr;
use std::sync::OnceLock;

use bigdecimal::BigDecimal;
use bytes::Bytes;

use super::{NullBits, NullsIter};
use crate::common::views::PrecScale;
use crate::common::{BorrowedValue, Ty};
use crate::decimal::{Decimal, DecimalAllowedTy};

type View<T> = DecimalView<T>;

#[derive(Debug, Clone)]
pub struct DecimalView<T: DecimalAllowedTy> {
    pub(crate) nulls: NullBits,
    pub(crate) data: Bytes,
    pub(crate) prec_scale: PrecScale,
    pub(crate) _p: PhantomData<T>,
    buf: OnceLock<Vec<*mut c_char>>,
}

impl<T: DecimalAllowedTy> DecimalView<T> {
    const ITEM_SIZE: usize = std::mem::size_of::<T>();
    const MAX_SCALE: u8 = T::MAX_SCALE;

    /// Create a new decimal view.
    pub(crate) fn new(nulls: NullBits, data: Bytes, prec_scale: PrecScale) -> Self {
        Self {
            nulls,
            data,
            prec_scale,
            _p: PhantomData,
            buf: OnceLock::new(),
        }
    }

    pub fn precision_and_scale(&self) -> (u8, u8) {
        (self.prec_scale.prec, self.prec_scale.scale)
    }

    pub fn precision(&self) -> u8 {
        self.prec_scale.prec
    }

    pub fn scale(&self) -> u8 {
        self.prec_scale.scale
    }

    /// Rows
    pub fn len(&self) -> usize {
        self.data.len() / std::mem::size_of::<T>()
    }

    pub fn as_raw_ptr(&self) -> *const u8 {
        self.data.as_ptr() as _
    }

    /// A iterator only decide if the value at some row index is NULL or not.
    pub fn is_null_iter(&self) -> NullsIter<'_> {
        NullsIter {
            nulls: &self.nulls,
            row: 0,
            len: self.len(),
        }
    }

    /// Check if the value at `row` index is NULL or not.
    pub fn is_null(&self, row: usize) -> bool {
        if row < self.len() {
            unsafe { self.is_null_unchecked(row) }
        } else {
            false
        }
    }

    /// Unsafe version for [methods.is_null]
    pub unsafe fn is_null_unchecked(&self, row: usize) -> bool {
        self.nulls.is_null_unchecked(row)
    }

    #[inline(always)]
    unsafe fn get_raw_data_at(&self, index: usize) -> *const T {
        self.data.as_ptr().add(index * Self::ITEM_SIZE) as _
    }

    /// Get nullable value at `row` index.
    pub unsafe fn get_unchecked(&self, row: usize) -> Option<Decimal<T>> {
        if self.nulls.is_null_unchecked(row) {
            None
        } else {
            Some(Decimal {
                data: self.get_raw_data_at(row).read_unaligned(),
                precision: self.precision(),
                scale: self.scale(),
            })
        }
    }

    /// Create a slice of view.
    pub fn slice(&self, mut range: std::ops::Range<usize>) -> Option<Self> {
        if range.start >= self.len() {
            return None;
        }
        if range.end >= self.len() {
            range.end = self.len();
        }
        if range.is_empty() {
            return None;
        }

        let item_size = std::mem::size_of::<T>();
        let nulls = unsafe { self.nulls.slice(range.clone()) };
        let data = self
            .data
            .slice(range.start * item_size..range.end * item_size);
        Some(Self::new(nulls, data, self.prec_scale))
    }

    /// A iterator to nullable values of current row.
    pub fn iter(&self) -> DecimalViewIter<'_, T> {
        DecimalViewIter { view: self, row: 0 }
    }

    /// Convert data to a vector of all nullable values.
    pub fn to_vec(&self) -> Vec<Option<Decimal<T>>> {
        self.iter().collect()
    }

    /// Write column data as raw bytes.
    pub(crate) fn write_raw_into<W: std::io::Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        let nulls = self.nulls.0.as_ref();
        debug_assert_eq!(nulls.len(), self.len().div_ceil(8));
        wtr.write_all(nulls)?;
        wtr.write_all(&self.data)?;
        Ok(nulls.len() + self.data.len())
    }

    pub fn concat(&self, rhs: &View<T>) -> View<T> {
        assert_eq!(
            self.prec_scale, rhs.prec_scale,
            "decimal strict concat needs same schema"
        );

        let nulls = self
            .nulls
            .iter()
            .take(self.len())
            .chain(rhs.nulls.iter().take(rhs.len()))
            .collect();
        let data: Bytes = self
            .data
            .as_ref()
            .iter()
            .chain(rhs.data.as_ref().iter())
            .copied()
            .collect();

        Self::new(nulls, data, self.prec_scale)
    }

    /// Get raw value at `row` index.
    pub unsafe fn get_raw_value_unchecked(&self, row: usize) -> (Ty, u32, *const c_void) {
        let buf = self.buf.get_or_init(|| {
            let mut buf = vec![ptr::null_mut(); self.len()];
            for i in 0..self.len() {
                if !self.nulls.is_null_unchecked(i) {
                    let data = self.get_raw_data_at(i).read_unaligned();
                    let dec = Decimal::new(data, self.precision(), self.scale());
                    let str = dec.as_bigdecimal().to_string();
                    let cstr = CString::new(str).unwrap();
                    buf[i] = cstr.into_raw();
                }
            }
            buf
        });

        (T::ty(), Self::ITEM_SIZE as _, buf[row] as _)
    }
}

impl<T: DecimalAllowedTy> Drop for DecimalView<T> {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.get() {
            for ptr in buf.iter() {
                if !ptr.is_null() {
                    let _ = unsafe { CString::from_raw(*ptr) };
                }
            }
        }
    }
}

macro_rules! impl_from_iter {
    ($ty: ty) => {
        /// Convert decimals to TDengine values.
        ///
        /// Returns the list if error.
        pub fn from_str_iter<I, T>(values: I) -> Result<Self, Vec<Option<BigDecimal>>>
        where
            T: AsRef<str>,
            I: IntoIterator<Item = T>,
        {
            Self::from_decimals(values.into_iter().map(|v| v.as_ref().parse().ok()))
        }

        /// Convert decimals to TDengine values.
        ///
        /// Returns the list if error.
        pub fn from_decimals<I, T>(values: I) -> Result<Self, Vec<Option<BigDecimal>>>
        where
            T: TryInto<Option<BigDecimal>>,
            I: IntoIterator<Item = T>,
        {
            let values: Vec<Option<BigDecimal>> = values
                .into_iter()
                .map(|v| v.try_into().ok().and_then(|v| v))
                .collect();
            let scale = values
                .iter()
                .flatten()
                .map(|v| v.as_bigint_and_exponent().1)
                .max()
                .unwrap_or(0) as u8;
            if scale > Self::MAX_SCALE {
                return Err(values);
            }
            let precision = scale; // Default precision for i64

            let values = values.into_iter().map(|v| {
                v.and_then(|v| {
                    <$ty>::try_from(
                        v.with_scale_round(scale as _, bigdecimal::RoundingMode::HalfUp)
                            .into_bigint_and_scale()
                            .0,
                    )
                    .ok()
                })
            });
            Ok(Self::from_values(values, precision, scale))
        }

        pub(super) fn from_bigdecimal_with<I, T>(values: I, prec_scale: PrecScale) -> Self
        where
            T: TryInto<Option<BigDecimal>>,
            I: IntoIterator<Item = T>,
        {
            let values = values.into_iter().map(|v| {
                v.try_into().ok().and_then(|v| v).and_then(|v| {
                    <$ty>::try_from(
                        v.with_scale_round(prec_scale.scale as _, bigdecimal::RoundingMode::HalfUp)
                            .into_bigint_and_scale()
                            .0,
                    )
                    .ok()
                })
            });
            Self::from_values(values, prec_scale.prec, prec_scale.scale)
        }

        pub fn from_values<I, T>(values: I, precision: u8, scale: u8) -> Self
        where
            T: Into<Option<$ty>>,
            I: IntoIterator<Item = T>,
        {
            let (nulls, mut values): (Vec<bool>, Vec<$ty>) = values
                .into_iter()
                .map(|v| match v.into() {
                    Some(v) => (false, v),
                    None => (true, 0),
                })
                .unzip();

            let nulls = NullBits::from_iter(nulls);
            let data = bytes::Bytes::from({
                let (ptr, len, cap) = (values.as_mut_ptr(), values.len(), values.capacity());
                std::mem::forget(values);

                let item_size = std::mem::size_of::<$ty>();

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
                        let val = <$ty>::from_ne_bytes(
                            &bytes[i..j].try_into().expect("slice with incorrect length"),
                        );
                        bytes[i..j].copy_from_slice(&val.to_le_bytes());
                    }
                    bytes
                }
            });

            let prec_scale = PrecScale::new(std::mem::size_of::<$ty>() as _, precision, scale);

            Self::new(nulls, data, prec_scale)
        }
    };
}

impl DecimalView<i128> {
    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue<'_> {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Decimal), BorrowedValue::Decimal)
    }
    impl_from_iter!(i128);
}

impl DecimalView<i64> {
    pub unsafe fn get_value_unchecked(&self, row: usize) -> BorrowedValue<'_> {
        self.get_unchecked(row)
            .map_or(BorrowedValue::Null(Ty::Decimal64), BorrowedValue::Decimal64)
    }
    impl_from_iter!(i64);
}

pub struct DecimalViewIter<'a, T: DecimalAllowedTy> {
    view: &'a DecimalView<T>,
    row: usize,
}

impl<T: DecimalAllowedTy> Iterator for DecimalViewIter<'_, T> {
    type Item = Option<Decimal<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row < self.view.len() {
            let row = self.row;
            self.row += 1;
            Some(unsafe { self.view.get_unchecked(row) })
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.row < self.view.len() {
            let len = self.view.len() - self.row;
            (len, Some(len))
        } else {
            (0, Some(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::common::decimal::Decimal;

    #[test]
    fn test_from_iter_64() -> anyhow::Result<()> {
        let view = DecimalView::<i64>::from_values([Some(12345), None, Some(22), Some(5)], 10, 2);

        let (precision, scale) = view.precision_and_scale();
        assert_eq!(precision, 10);
        assert_eq!(scale, 2);

        assert_eq!(view.len(), 4);

        assert_eq!(
            unsafe { view.get_value_unchecked(0) },
            BorrowedValue::Decimal64(Decimal::new(12345, precision, scale))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(1) },
            BorrowedValue::Null(Ty::Decimal64)
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(2) },
            BorrowedValue::Decimal64(Decimal::new(22, precision, scale))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(3) },
            BorrowedValue::Decimal64(Decimal::new(5, precision, scale))
        );

        assert!(!view.is_null(0));
        assert!(view.is_null(1));
        assert!(!view.is_null(2));
        assert!(!view.is_null(3));
        assert!(!view.is_null(4));

        let mut nulls = view.is_null_iter();
        assert!(!nulls.next().unwrap());
        assert!(nulls.next().unwrap());
        assert!(!nulls.next().unwrap());
        assert!(!nulls.next().unwrap());

        let mut iter = view.iter();
        assert_eq!(
            iter.next(),
            Some(Some(Decimal::new(12345, precision, scale)))
        );
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(Decimal::new(22, precision, scale))));
        assert_eq!(iter.next(), Some(Some(Decimal::new(5, precision, scale))));
        assert_eq!(iter.next(), None);

        let view = view.slice(1..3).unwrap();
        let mut iter = view.iter();
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(Decimal::new(22, precision, scale))));
        assert_eq!(iter.next(), None);
        Ok(())
    }

    #[test]
    fn test_from_iter_128() -> anyhow::Result<()> {
        let view = DecimalView::<i128>::from_values([Some(12345), None, Some(22), Some(5)], 10, 2);

        let (precision, scale) = view.precision_and_scale();
        assert_eq!(precision, 10);
        assert_eq!(scale, 2);

        assert_eq!(view.len(), 4);

        assert_eq!(
            unsafe { view.get_value_unchecked(0) },
            BorrowedValue::Decimal(Decimal::new(12345, precision, scale))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(1) },
            BorrowedValue::Null(Ty::Decimal)
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(2) },
            BorrowedValue::Decimal(Decimal::new(22, precision, scale))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(3) },
            BorrowedValue::Decimal(Decimal::new(5, precision, scale))
        );

        assert!(!view.is_null(0));
        assert!(view.is_null(1));
        assert!(!view.is_null(2));
        assert!(!view.is_null(3));
        assert!(!view.is_null(4));

        let mut nulls = view.is_null_iter();
        assert!(!nulls.next().unwrap());
        assert!(nulls.next().unwrap());
        assert!(!nulls.next().unwrap());
        assert!(!nulls.next().unwrap());

        let mut iter = view.iter();
        assert_eq!(
            iter.next(),
            Some(Some(Decimal::new(12345, precision, scale)))
        );
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(Decimal::new(22, precision, scale))));
        assert_eq!(iter.next(), Some(Some(Decimal::new(5, precision, scale))));
        assert_eq!(iter.next(), None);

        let view = view.slice(1..3).unwrap();
        let mut iter = view.iter();
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(Decimal::new(22, precision, scale))));
        assert_eq!(iter.next(), None);
        Ok(())
    }

    #[test]
    fn concat_test() {
        let view1 = DecimalView::<i64>::from_values([Some(12345), None, Some(22)], 10, 2);
        let view2 = DecimalView::<i64>::from_values([Some(5), None, Some(100)], 10, 2);
        let view = view1.concat(&view2);
        assert_eq!(view.len(), 6);
        assert_eq!(
            unsafe { view.get_value_unchecked(0) },
            BorrowedValue::Decimal64(Decimal::new(12345, 10, 2))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(1) },
            BorrowedValue::Null(Ty::Decimal64)
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(2) },
            BorrowedValue::Decimal64(Decimal::new(22, 10, 2))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(3) },
            BorrowedValue::Decimal64(Decimal::new(5, 10, 2))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(4) },
            BorrowedValue::Null(Ty::Decimal64)
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(5) },
            BorrowedValue::Decimal64(Decimal::new(100, 10, 2))
        );
        assert_eq!(
            unsafe { view.get_value_unchecked(0) }.to_string().unwrap(),
            "123.45"
        );
        assert!(!view.is_null(0));
        assert!(view.is_null(1));
        assert!(!view.is_null(2));
        assert!(!view.is_null(3));
        assert!(view.is_null(4));
        assert!(!view.is_null(5));
    }

    #[test]
    fn test_from_bigdecimal_with() -> anyhow::Result<()> {
        let d1 = bigdecimal::BigDecimal::from_str("12345678901234567.89")?;
        assert_eq!(d1.as_bigint_and_exponent().1, 2);
        let d2 = d1.with_scale(3);
        assert_eq!(d2.as_bigint_and_exponent().1, 3);

        let ps = PrecScale::new(16, 21, 4);
        let view = DecimalView::<i128>::from_bigdecimal_with([Some(d1), Some(d2)], ps);
        assert_eq!(view.len(), 2);
        assert_eq!(view.precision_and_scale(), (21, 4));

        let decimal = unsafe { view.get_unchecked(0) }.unwrap();
        assert_eq!(decimal.data, 123456789012345678900);
        assert_eq!(decimal.precision, 21);
        assert_eq!(decimal.scale, 4);

        let decimal = unsafe { view.get_unchecked(1) }.unwrap();
        assert_eq!(decimal.data, 123456789012345678900);
        assert_eq!(decimal.precision, 21);
        assert_eq!(decimal.scale, 4);

        Ok(())
    }

    #[test]
    fn test_from_decimals() {
        use bigdecimal::BigDecimal;

        let decimals = ["1.2", "1.23", "10.234"];
        let values = decimals
            .into_iter()
            .map(|v| BigDecimal::from_str(v).unwrap());
        let d1 = DecimalView::<i128>::from_decimals(values).unwrap();
        assert_eq!(d1.precision_and_scale(), (3, 3));

        assert_eq!(
            unsafe { d1.get_value_unchecked(0).to_string().unwrap() },
            "1.200"
        );
        assert_eq!(
            unsafe { d1.get_value_unchecked(1).to_string().unwrap() },
            "1.230"
        );
        assert_eq!(
            unsafe { d1.get_value_unchecked(2).to_string().unwrap() },
            "10.234"
        );

        let decimals = ["0.1234567890123456789"];

        let values = decimals
            .into_iter()
            .map(|v| BigDecimal::from_str(v).unwrap());
        let d1 = DecimalView::<i64>::from_decimals(values);
        assert!(d1.is_err());

        let decimals = ["0.1234567890123456789012345678901234567890"];

        let values = decimals
            .into_iter()
            .map(|v| BigDecimal::from_str(v).unwrap());
        let d1 = DecimalView::<i128>::from_decimals(values);
        assert!(d1.is_err());
    }

    #[test]
    fn test_get_raw_value() {
        let decimals = ["1.2", "1.23", "10.234"];
        let d1 = DecimalView::<i128>::from_str_iter(decimals).unwrap();

        let ptr = d1.as_raw_ptr();
        assert!(!ptr.is_null());

        let (t, l, p) = unsafe { d1.get_raw_value_unchecked(0) };
        assert_eq!(t, Ty::Decimal);
        assert_eq!(l, 16);
        let p = unsafe { std::ffi::CStr::from_ptr(p as _).to_str().unwrap() };
        assert_eq!(p, "1.200");

        let mut cursor = std::io::Cursor::new(Vec::new());
        d1.write_raw_into(&mut cursor).unwrap();
        let bytes = cursor.into_inner();
        assert_eq!(
            bytes.as_slice(),
            [
                0, 176, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 206, 4, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 250, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
    }
}
