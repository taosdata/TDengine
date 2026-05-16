use bigdecimal::num_bigint::BigInt;
use bigdecimal::{BigDecimal, ToPrimitive};

use super::Ty;

mod private {
    pub trait DecimalAllowedTy {}
    impl DecimalAllowedTy for i64 {}
    impl DecimalAllowedTy for i128 {}
}

pub trait DecimalAllowedTy: private::DecimalAllowedTy + Copy + Into<BigInt> {
    const MAX_SCALE: u8;
    fn ty() -> Ty;
}

impl DecimalAllowedTy for i64 {
    const MAX_SCALE: u8 = 18;
    #[inline]
    fn ty() -> Ty {
        Ty::Decimal64
    }
}

impl DecimalAllowedTy for i128 {
    const MAX_SCALE: u8 = 38;
    #[inline]
    fn ty() -> Ty {
        Ty::Decimal
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal<T: DecimalAllowedTy> {
    pub(crate) data: T,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
}

impl<T: DecimalAllowedTy> Decimal<T> {
    pub fn new(data: T, precision: u8, scale: u8) -> Self {
        Self {
            data,
            precision,
            scale,
        }
    }

    pub fn data(&self) -> T {
        self.data
    }

    pub fn precision_and_scale(&self) -> (u8, u8) {
        (self.precision, self.scale)
    }

    pub(crate) fn as_bigdecimal(&self) -> bigdecimal::BigDecimal {
        BigDecimal::from_bigint(self.data.into(), self.scale as _)
    }
}

macro_rules! from_bigdecimal {
    ($ty: ty) => {
        impl Decimal<$ty> {
            pub(crate) fn from_bigdecimal(decimal: &BigDecimal) -> Option<Self> {
                let (num, scale) = decimal.as_bigint_and_exponent();
                paste::paste! {
                    num.[<to_$ty>]().map(|data| Self {
                        data,
                        precision: decimal.digits() as _,
                        scale: scale as _,
                    })
                }
            }
        }
    };
}

from_bigdecimal!(i128);
from_bigdecimal!(i64);

impl<T: DecimalAllowedTy> std::fmt::Display for Decimal<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_bigdecimal().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_decimal() {
        let big_decimal = BigDecimal::from(12345678901234567890i128);
        let decimal = Decimal::<i128>::from_bigdecimal(&big_decimal).unwrap();
        assert_eq!(decimal.data(), 12345678901234567890);
        assert_eq!(decimal.precision_and_scale(), (20, 0));

        let big_decimal = BigDecimal::from_str("12345678901234567.890").unwrap();
        let decimal = Decimal::<i128>::from_bigdecimal(&big_decimal).unwrap();
        assert_eq!(decimal.data(), 12345678901234567890);
        assert_eq!(decimal.precision_and_scale(), (20, 3));

        let big_decimal = BigDecimal::from(1234567890i64);
        let decimal = Decimal::<i64>::from_bigdecimal(&big_decimal).unwrap();
        assert_eq!(decimal.data(), 1234567890);
        assert_eq!(decimal.precision_and_scale(), (10, 0));

        let big_decimal = BigDecimal::from_str("1234567.890").unwrap();
        let decimal = Decimal::<i64>::from_bigdecimal(&big_decimal).unwrap();
        assert_eq!(decimal.data(), 1234567890);
        assert_eq!(decimal.precision_and_scale(), (10, 3));

        let big_decimal = BigDecimal::from(12345678901234567890i128);
        let decimal = Decimal::<i64>::from_bigdecimal(&big_decimal);
        assert!(decimal.is_none());

        let big_decimal = BigDecimal::from(1234567890i64);
        let decimal = Decimal::<i128>::from_bigdecimal(&big_decimal).unwrap();
        assert_eq!(decimal.data(), 1234567890);
        assert_eq!(decimal.precision_and_scale(), (10, 0));
    }
}
