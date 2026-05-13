use std::fmt::{self, Display};
use std::num::ParseIntError;
use std::ops::Deref;
use std::{fmt::Debug, str::FromStr};

use bytes::Bytes;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::common::Ty;

#[allow(clippy::partial_pub_fields)]
#[derive(Clone, Copy, Eq, KnownLayout, IntoBytes, FromBytes, Unaligned, Immutable)]
#[repr(C)]
pub struct PrecScale {
    pub scale: u8,
    pub prec: u8,
    _empty: u8,
    pub len: u8,
}

#[allow(clippy::missing_fields_in_debug)]
impl Debug for PrecScale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrecScale")
            .field("len", &self.len)
            .field("prec", &self.prec)
            .field("scale", &self.scale)
            .finish()
    }
}

impl PrecScale {
    /// Create a new `PrecScale` with the given length, precision, and scale.
    #[inline]
    pub const fn new(len: u8, precision: u8, scale: u8) -> Self {
        Self {
            len,
            _empty: 0,
            prec: precision,
            scale,
        }
    }
}

impl PartialEq for PrecScale {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.prec == other.prec && self.scale == other.scale
    }
}

#[derive(Clone, Copy, Eq, KnownLayout, FromBytes, Immutable)]
#[repr(C)]
#[repr(packed(1))]
union LenOrDec {
    /// Length of the column schema for non-decimal types.
    ///
    /// - For fixed-length types, this is the length of the type in bytes.
    /// - For variable-length types, this is the length of the value in bytes.
    len: u32,
    /// Decimal precision and scale.
    ///
    /// The length is always 8 for `Decimal64` and 16 for `Decimal`.
    ///
    /// - The `len` field is used to store the length of the decimal value.
    /// - The `prec` field is used to store the precision of the decimal value.
    /// - The `scale` field is used to store the scale of the decimal value.
    dec: PrecScale,
}

impl Deref for LenOrDec {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::mem::transmute::<_, &[u8; 4]>(self) }.as_slice()
    }
}

unsafe impl zerocopy::ByteSlice for LenOrDec {}

impl Debug for LenOrDec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("LenOrDec")
            .field(&unsafe { self.len })
            .field(&unsafe { self.dec })
            .finish()
    }
}

impl PartialEq for LenOrDec {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.len == other.len }
    }
}

impl LenOrDec {
    #[inline]
    pub const fn new_len(len: u32) -> Self {
        Self { len }
    }
}

/// Represent column basics information: type, length.
#[allow(clippy::unsafe_derive_deserialize)]
#[derive(Clone, Copy, TryFromBytes, Immutable, DeserializeFromStr, SerializeDisplay, Unaligned)]
#[repr(C)]
#[repr(packed(1))]
pub struct DataType {
    ty: Ty,
    attr: LenOrDec,
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        self.ty == other.ty
            && self.len() == other.len()
            && self.precision() == other.precision()
            && self.scale() == other.scale()
    }
}

impl Eq for DataType {}

impl Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("ColSchema");
        debug.field("ty", &self.ty);
        if self.ty.is_decimal() {
            let dec = unsafe { self.attr.dec };
            debug
                .field("len", &dec.len)
                .field("prec", &dec.prec)
                .field("scale", &dec.scale);
        } else {
            let len = unsafe { self.attr.len };
            debug.field("len", &len);
        }
        debug.finish()
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ty.is_decimal() {
            let dec = unsafe { self.attr.dec };
            write!(f, "{}({},{})", self.ty, dec.prec, dec.scale)
        } else if self.ty.is_var_type() {
            write!(f, "{}({})", self.ty, unsafe { self.positive_var_len() })
        } else {
            write!(f, "{}", self.ty)
        }
    }
}

const MAX_DECIMAL_PRECISION: u8 = 38;
const MAX_DECIMAL64_PRECISION: u8 = 18;
const MIN_DECIMAL_SCALE: u8 = 0;

#[derive(thiserror::Error, Debug)]
pub enum ParseDataTypeError {
    #[error("Invalid data type number: {0}")]
    TypeNumberError(u8),
    #[error("Parse DECIMAL precision error: {0}")]
    ParseDecimalPrecisionError(ParseIntError),
    #[error("Parse DECIMAL scale error")]
    ParseDecimalScaleError(ParseIntError),
    #[error("Invalid DECIMAL precision: {0}")]
    InvalidDecimalPrecision(u8),
    #[error("Invalid DECIMAL scale: {0}")]
    InvalidDecimalScale(u8),
    #[error("Invalid VARCHAR length: {0}")]
    InvalidVarCharLength(String),
    #[error("Invalid VARBINARY length: {0}")]
    InvalidVarBinaryLength(String),
    #[error("Invalid NCHAR length: {0}")]
    InvalidNCharLength(String),
    #[error("Invalid GEOMETRY length: {0}")]
    InvalidGeometryLength(String),
    #[error("Invalid data type format: expect {0}, but got {1}")]
    FormatError(&'static str, String),
    #[error("Unknown or unsupported data type: {0}")]
    UnknownDataType(String),
}

impl FromStr for DataType {
    type Err = ParseDataTypeError;

    /// Zero-copy compatible parsing of a data type string.
    ///
    /// Requires allocation only when the input string is not a valid data type or in lower cases.
    ///
    /// This function supports parsing various data types, including fixed-length types like TINYINT, SMALLINT, INT, BIGINT,
    /// as well as variable-length types like VARCHAR, BINARY, NCHAR, VARBINARY, GEOMETRY, BLOB and JSON.
    /// It also supports decimal types with precision and scale, such as DECIMAL(p,s) or DECIMAL(p).
    /// It returns a `DataType` instance representing the parsed data type.
    ///
    /// # Errors
    ///
    /// Returns a `ParseDataTypeError` if the input string is not a valid data type or if the parameters are invalid.
    ///
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if let Ok(i) = u8::from_str(s) {
            // Handle fixed-length types: TINYINT, SMALLINT, INT, BIGINT, etc.
            return Ok(Self::from_ty(
                Ty::from_u8_option(i).ok_or(ParseDataTypeError::TypeNumberError(i))?,
            ));
        }

        match s {
            "BOOL" => Ok(Self::from_ty(Ty::Bool)),
            "TINYINT" => Ok(Self::from_ty(Ty::TinyInt)),
            "SMALLINT" => Ok(Self::from_ty(Ty::SmallInt)),
            "INT" => Ok(Self::from_ty(Ty::Int)),
            "BIGINT" => Ok(Self::from_ty(Ty::BigInt)),
            "TINYINT UNSIGNED" => Ok(Self::from_ty(Ty::UTinyInt)),
            "SMALLINT UNSIGNED" => Ok(Self::from_ty(Ty::USmallInt)),
            "INT UNSIGNED" => Ok(Self::from_ty(Ty::UInt)),
            "BIGINT UNSIGNED" => Ok(Self::from_ty(Ty::UBigInt)),
            "FLOAT" => Ok(Self::from_ty(Ty::Float)),
            "DOUBLE" => Ok(Self::from_ty(Ty::Double)),
            "TIMESTAMP" => Ok(Self::from_ty(Ty::Timestamp)),
            "BINARY" | "VARCHAR" => Ok(Self::from_ty(Ty::VarChar)),
            "NCHAR" => Ok(Self::from_ty(Ty::NChar)),
            "JSON" => Ok(Self::from_ty(Ty::Json)),
            "VARBINARY" => Ok(Self::from_ty(Ty::VarBinary)),
            "GEOMETRY" => Ok(Self::from_ty(Ty::Geometry)),
            "BLOB" => Ok(Self::from_ty(Ty::Blob)),
            "MEDIUMBLOB" => Ok(Self::from_ty(Ty::MediumBlob)),
            // Handle types with length, e.g. VARCHAR(100), NCHAR(50), VARBINARY(32), GEOMETRY(128)
            _ => {
                // Handle decimal types: DECIMAL(p,s) or DECIMAL(p)
                if let Some(rest) = s.strip_prefix("DECIMAL") {
                    let params = rest.trim_start_matches('(').trim_end_matches(')').trim();
                    if params.is_empty() {
                        // If no params, fallback to default
                        return Ok(Self::new_decimal(
                            Ty::Decimal64,
                            MAX_DECIMAL64_PRECISION,
                            MIN_DECIMAL_SCALE,
                        ));
                    }

                    if let Some((p, s)) = params.split_once(',') {
                        let p = p.trim();
                        let s = s.trim();
                        if p.is_empty() && s.is_empty() {
                            // If no params, fallback to default
                            return Ok(Self::new_decimal(
                                Ty::Decimal64,
                                MAX_DECIMAL64_PRECISION,
                                MIN_DECIMAL_SCALE,
                            ));
                        }

                        if p.is_empty() {
                            return Err(ParseDataTypeError::FormatError(
                                "DECIMAL(precision[,scale])",
                                s.to_string(),
                            ));
                        }

                        let precision: u8 = p
                            .parse()
                            .map_err(ParseDataTypeError::ParseDecimalPrecisionError)?;
                        let scale: u8 = if s.is_empty() {
                            0
                        } else {
                            s.parse()
                                .map_err(ParseDataTypeError::ParseDecimalScaleError)?
                        };
                        if scale > precision {
                            return Err(ParseDataTypeError::InvalidDecimalScale(scale));
                        }
                        match precision {
                            1..=MAX_DECIMAL64_PRECISION => {
                                return Ok(Self::new_decimal(Ty::Decimal64, precision, scale));
                            }
                            19..=MAX_DECIMAL_PRECISION => {
                                return Ok(Self::new_decimal(Ty::Decimal, precision, scale));
                            }
                            _ => {
                                return Err(ParseDataTypeError::InvalidDecimalPrecision(precision));
                            }
                        }
                    }

                    // If only precision is provided, scale defaults to 0
                    let precision: u8 = params
                        .parse()
                        .map_err(ParseDataTypeError::ParseDecimalPrecisionError)?;
                    match precision {
                        1..=MAX_DECIMAL64_PRECISION => {
                            return Ok(Self::new_decimal(
                                Ty::Decimal64,
                                precision,
                                MIN_DECIMAL_SCALE,
                            ));
                        }
                        19..=MAX_DECIMAL_PRECISION => {
                            return Ok(Self::new_decimal(
                                Ty::Decimal,
                                precision,
                                MIN_DECIMAL_SCALE,
                            ));
                        }
                        _ => {
                            return Err(ParseDataTypeError::InvalidDecimalPrecision(precision));
                        }
                    }
                }
                if let Some(rest) = s.strip_prefix("VARCHAR") {
                    let params = rest.trim_start_matches('(').trim_end_matches(')').trim();
                    if params.is_empty() {
                        // If no params, fallback to default
                        return Ok(Self::new(Ty::VarBinary, 0));
                    }
                    let mut parts = params.split(',').map(|x| x.trim());
                    match (parts.next(), parts.next()) {
                        (Some(len), None) => {
                            let len: u32 = len.parse().map_err(|_| {
                                ParseDataTypeError::InvalidVarCharLength(len.to_string())
                            })?;
                            return Ok(Self::new(Ty::VarChar, len));
                        }
                        _ => {
                            return Err(ParseDataTypeError::FormatError(
                                "VARCHAR(length)",
                                s.to_string(),
                            ));
                        }
                    }
                }
                if let Some(rest) = s.strip_prefix("BINARY") {
                    let params = rest.trim_start_matches('(').trim_end_matches(')').trim();
                    if params.is_empty() {
                        // If no params, fallback to default
                        return Ok(Self::new(Ty::VarBinary, 0));
                    }
                    let mut parts = params.split(',').map(|x| x.trim());
                    match (parts.next(), parts.next()) {
                        (Some(len), None) => {
                            let len: u32 = len.parse().map_err(|_| {
                                ParseDataTypeError::InvalidVarCharLength(len.to_string())
                            })?;
                            return Ok(Self::new(Ty::VarChar, len));
                        }
                        _ => {
                            return Err(ParseDataTypeError::FormatError(
                                "BINARY(length)",
                                s.to_string(),
                            ));
                        }
                    }
                }
                if let Some(rest) = s.strip_prefix("NCHAR(") {
                    let params = rest.trim_start_matches('(').trim_end_matches(')').trim();
                    if params.is_empty() {
                        // If no params, fallback to default
                        return Ok(Self::new(Ty::NChar, 0));
                    }
                    if let Some(len) = rest.strip_suffix(')') {
                        let len: u32 = len
                            .trim()
                            .parse()
                            .map_err(|_| ParseDataTypeError::InvalidNCharLength(len.to_string()))?;
                        return Ok(Self::new(Ty::NChar, len));
                    }
                    return Err(ParseDataTypeError::FormatError(
                        "NCHAR(length)",
                        s.to_string(),
                    ));
                }
                if let Some(rest) = s.strip_prefix("VARBINARY(") {
                    if let Some(len) = rest.strip_suffix(')') {
                        let len: u32 = len.trim().parse().map_err(|_| {
                            ParseDataTypeError::InvalidVarBinaryLength(len.to_string())
                        })?;
                        return Ok(Self::new(Ty::VarBinary, len));
                    }
                    return Err(ParseDataTypeError::FormatError(
                        "VARBINARY(length)",
                        s.to_string(),
                    ));
                }
                if let Some(rest) = s.strip_prefix("GEOMETRY(") {
                    if let Some(len) = rest.strip_suffix(')') {
                        let len: u32 = len.trim().parse().map_err(|_| {
                            ParseDataTypeError::InvalidGeometryLength(len.to_string())
                        })?;
                        return Ok(Self::new(Ty::Geometry, len));
                    }
                    return Err(ParseDataTypeError::FormatError(
                        "GEOMETRY(length)",
                        s.to_string(),
                    ));
                }
                let u = s.to_uppercase();
                if u == s {
                    return Err(ParseDataTypeError::UnknownDataType(s.to_string()));
                }
                Self::from_str(&u)
            }
        }
    }
}

impl From<Ty> for DataType {
    fn from(value: Ty) -> Self {
        Self::from_ty(value)
    }
}

impl DataType {
    #[inline]
    pub(crate) const fn new(ty: Ty, len: u32) -> Self {
        Self {
            ty,
            attr: LenOrDec { len },
        }
    }

    #[inline]
    pub(crate) const fn new_decimal(ty: Ty, precision: u8, scale: u8) -> Self {
        assert!(ty.is_decimal(), "Type must be decimal");
        Self {
            ty,
            attr: LenOrDec {
                dec: PrecScale {
                    len: ty.fixed_length() as u8,
                    _empty: 0,
                    prec: precision,
                    scale,
                },
            },
        }
    }

    #[inline]
    pub(crate) const fn from_ty(ty: Ty) -> Self {
        let attr = if ty.is_decimal() {
            LenOrDec {
                dec: PrecScale {
                    len: ty.fixed_length() as u8,
                    _empty: 0,
                    prec: 0,
                    scale: 0,
                },
            }
        } else {
            LenOrDec::new_len(ty.fixed_length() as _)
        };

        Self { ty, attr }
    }

    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        unsafe { std::mem::transmute::<&Self, &[u8; 5]>(self) }
    }

    #[inline]
    pub fn into_bytes(self) -> [u8; 5] {
        unsafe { std::mem::transmute::<Self, [u8; 5]>(self) }
    }

    /// Data type as a [Ty].
    pub fn ty(&self) -> Ty {
        self.ty
    }

    /// The raw length of the column schema.
    #[inline]
    pub fn len(&self) -> u32 {
        if self.ty.is_decimal() {
            unsafe { self.attr.dec.len as _ }
        } else {
            unsafe { self.attr.len }
        }
    }

    /// Schema length to use in SQL queries.
    ///
    /// If the length is 0, returns 64 when is variable-length type (VARBINARY, VARCHAR).
    #[inline]
    pub fn len_or_fixed(&self) -> u32 {
        let fixed = self.ty.fixed_length() as u32;
        if fixed != 0 {
            return fixed;
        }

        let len = unsafe { self.attr.len };
        if len == 0 {
            64 // Default variable length for other types
        } else {
            len
        }
    }

    /// Returns 64 when length is zero.
    unsafe fn positive_var_len(&self) -> u32 {
        let len = self.attr.len;
        if len == 0 {
            64 // Default variable length for other types
        } else {
            len
        }
    }

    /// Set the length of the column schema.
    ///
    /// If the type is decimal, it will change the type to `Decimal64` or `Decimal` based on the length.
    /// If the length is 8, it will set the type to `Decimal64`,
    /// if the length is 16, it will set the type to `Decimal`.
    ///
    /// # Panics
    ///
    /// Panics if the length is not 8 or 16 for decimal types.
    ///
    pub fn set_len(&mut self, len: u32) {
        if self.ty.is_decimal() {
            if len == 8 {
                self.ty = Ty::Decimal64;
            } else if len == 16 {
                self.ty = Ty::Decimal;
            } else {
                panic!("Invalid decimal length: {len}");
            }
            self.attr.dec.len = len as u8;
        } else {
            self.attr.len = len;
        }
    }

    #[inline]
    pub(crate) fn as_prec_scale_unchecked(&self) -> PrecScale {
        unsafe { self.attr.dec }
    }

    /// Precision of a decimal value
    #[inline]
    pub fn precision(&self) -> u8 {
        if self.ty.is_decimal() {
            unsafe { self.attr.dec.prec }
        } else {
            0
        }
    }

    /// Scale of a decimal value
    #[inline]
    pub fn scale(&self) -> u8 {
        if self.ty.is_decimal() {
            unsafe { self.attr.dec.scale }
        } else {
            0
        }
    }
}

pub struct Schemas(pub(crate) Bytes);

impl<T: Into<Bytes>> From<T> for Schemas {
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl Debug for Schemas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.deref(), f)
    }
}

impl Schemas {
    /// As a [DataType] slice.
    pub fn as_slice(&self) -> &[DataType] {
        unsafe {
            std::slice::from_raw_parts(
                self.0.as_ptr() as *const DataType,
                self.0.len() / std::mem::size_of::<DataType>(),
            )
        }
    }
}

impl Deref for Schemas {
    type Target = [DataType];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn col_schema() {
        let col = DataType {
            ty: Ty::BigInt,
            attr: LenOrDec::new_len(4),
        };
        let bytes: [u8; 5] = unsafe { std::mem::transmute_copy(&col) };
        dbg!(&bytes);

        let bytes: [u8; 5] = [4, 1, 0, 0, 0];
        let col2: DataType = unsafe { std::mem::transmute_copy(&bytes) };
        dbg!(col2);

        assert_eq!(std::mem::size_of_val(&col), 5);
        assert_eq!(std::mem::align_of_val(&col), 1);
    }

    #[test]
    fn test_bin() {
        let v: [u8; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let ptr = v.as_ptr();

        let v_u16 = unsafe { *std::mem::transmute::<*const u8, *const u16>(ptr) };
        println!("{v_u16:#x?}: {:?}", v_u16.to_le_bytes());
        #[derive(Debug, Clone, Copy)]
        #[repr(packed)]
        #[allow(dead_code)]
        struct A {
            a: u16,
            b: u32,
        }
        println!("A size: {}", std::mem::size_of::<A>());
        let a: &A = unsafe {
            std::mem::transmute::<*const u8, *const A>(ptr)
                .as_ref()
                .unwrap()
        };
        println!("{a:#x?}");
    }

    #[test]
    fn test_prec_scale() {
        let ps = PrecScale::new(8, 10, 2);
        assert_eq!(ps.scale, 2);
        assert_eq!(ps.prec, 10);
        assert_eq!(ps._empty, 0);
        assert_eq!(ps.len, 8);
        let expected: [u8; 4] = [2, 10, 0, 8];

        let bytes: [u8; 4] = unsafe { std::mem::transmute_copy(&ps) };
        assert_eq!(bytes, expected);

        assert_eq!(
            format!("{ps:?}"),
            "PrecScale { len: 8, prec: 10, scale: 2 }"
        );
        assert_eq!(ps.as_bytes(), expected);

        let ps: PrecScale = unsafe { std::mem::transmute_copy(&bytes) };
        assert_eq!(ps.len, 8);
        assert_eq!(ps.prec, 10);
        assert_eq!(ps.scale, 2);
        assert_eq!(ps._empty, 0);

        let ps2 = PrecScale::read_from_bytes(&bytes).unwrap();
        assert_eq!(ps.len, 8);
        assert_eq!(ps.prec, 10);
        assert_eq!(ps.scale, 2);
        assert_eq!(ps._empty, 0);

        assert_eq!(ps, ps2);

        let ps3 = PrecScale::ref_from_bytes(&bytes).unwrap();
        assert_eq!(ps, *ps3);
    }

    #[test]
    fn test_union_len_or_dec() {
        let len = LenOrDec::new_len(8);
        assert_eq!(unsafe { len.len }, 8);
        let dec = PrecScale::new(8, 10, 2);
        let dec = LenOrDec { dec };
        assert_eq!(unsafe { dec.dec.len }, 8);
        assert_eq!(unsafe { dec.dec.prec }, 10);
        assert_eq!(unsafe { dec.dec.scale }, 2);

        let bytes: [u8; 4] = unsafe { std::mem::transmute_copy(&len) };
        assert_eq!(bytes, [8, 0, 0, 0]);

        let bytes: [u8; 4] = unsafe { std::mem::transmute_copy(&dec) };
        assert_eq!(bytes, [2, 10, 0, 8]);

        let len2: LenOrDec = unsafe { std::mem::transmute_copy(&bytes) };

        let len3 = LenOrDec::read_from_bytes(&bytes).unwrap();
        assert_eq!(len2, len3);

        let len3 = LenOrDec::ref_from_bytes(&bytes).unwrap();
        assert_eq!(unsafe { len3.dec.len }, 8);
        assert_eq!(unsafe { len3.dec.prec }, 10);
        assert_eq!(unsafe { len3.dec.scale }, 2);
        assert_eq!(len2, *len3);

        assert_eq!(
            format!("{len:?}"),
            "LenOrDec(8, PrecScale { len: 0, prec: 0, scale: 8 })"
        );
        assert_eq!(
            format!("{len2:?}"),
            "LenOrDec(134220290, PrecScale { len: 8, prec: 10, scale: 2 })"
        );
    }

    #[test]
    fn test_dec() {
        let schema = DataType::new(Ty::BigInt, 8);
        assert_eq!(schema.len(), 8);
        let dec_schema = DataType::new_decimal(Ty::Decimal64, 10, 2);
        assert_eq!(dec_schema.len(), 8);
        assert_eq!(dec_schema.ty, Ty::Decimal64);
        assert_eq!(unsafe { dec_schema.attr.dec.prec }, 10);
        assert_eq!(unsafe { dec_schema.attr.dec.scale }, 2);

        let bytes = dec_schema.as_bytes();
        assert_eq!(bytes, [21, 2, 10, 0, 8]);
    }

    #[test]
    fn test_col_schema_from_ty() {
        let schema = DataType::from(Ty::BigInt);
        assert_eq!(schema.ty, Ty::BigInt);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "BIGINT");
    }

    #[test]
    fn test_col_schema_from_number() {
        let schema: DataType = "1".parse().unwrap();
        assert_eq!(schema.ty, Ty::Bool);
        for n in 1..=21 {
            assert!(DataType::from_str(&format!("{n}")).is_ok());
        }
        let schema = DataType::from_str("125");
        assert!(schema.is_err());
    }

    #[test]
    fn test_col_schema_from_str() {
        let schema: DataType = "VARCHAR(100)".parse().unwrap();
        assert_eq!(schema.ty, Ty::VarChar);
        assert_eq!(schema.len(), 100);
        assert_eq!(schema.to_string(), "BINARY(100)");
        let schema: DataType = "BINARY(100)".parse().unwrap();
        assert_eq!(schema.ty, Ty::VarChar);
        assert_eq!(schema.len(), 100);
        assert_eq!(schema.to_string(), "BINARY(100)");
        let schema: DataType = "NCHAR(50)".parse().unwrap();
        assert_eq!(schema.ty, Ty::NChar);
        assert_eq!(schema.len(), 50);
        assert_eq!(schema.to_string(), "NCHAR(50)");
        let schema: DataType = "VARBINARY(32)".parse().unwrap();
        assert_eq!(schema.ty, Ty::VarBinary);
        assert_eq!(schema.len(), 32);
        assert_eq!(schema.to_string(), "VARBINARY(32)");
        let schema: DataType = "GEOMETRY(128)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Geometry);
        assert_eq!(schema.len(), 128);
        assert_eq!(schema.to_string(), "GEOMETRY(128)");
        let schema: DataType = "JSON".parse().unwrap();
        assert_eq!(schema.ty, Ty::Json);
        assert_eq!(schema.len(), 0);
        assert_eq!(schema.to_string(), "JSON");
        let schema: DataType = "BLOB".parse().unwrap();
        assert_eq!(schema.ty, Ty::Blob);
        assert_eq!(schema.len(), 0);
        assert_eq!(schema.to_string(), "BLOB");
        let schema: DataType = "MEDIUMBLOB".parse().unwrap();
        assert_eq!(schema.ty, Ty::MediumBlob);
        assert_eq!(schema.len(), 0);
        assert_eq!(schema.to_string(), "MEDIUMBLOB");
        let schema: DataType = "BOOL".parse().unwrap();
        assert_eq!(schema.ty, Ty::Bool);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.to_string(), "BOOL");
        let schema: DataType = "TINYINT".parse().unwrap();
        assert_eq!(schema.ty, Ty::TinyInt);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.to_string(), "TINYINT");
        let schema: DataType = "SMALLINT".parse().unwrap();
        assert_eq!(schema.ty, Ty::SmallInt);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.to_string(), "SMALLINT");
        let schema: DataType = "INT".parse().unwrap();
        assert_eq!(schema.ty, Ty::Int);
        assert_eq!(schema.len(), 4);
        assert_eq!(schema.to_string(), "INT");
        let schema: DataType = "BIGINT".parse().unwrap();
        assert_eq!(schema.ty, Ty::BigInt);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "BIGINT");
        let schema: DataType = "TINYINT UNSIGNED".parse().unwrap();
        assert_eq!(schema.ty, Ty::UTinyInt);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema.to_string(), "TINYINT UNSIGNED");
        let schema: DataType = "SMALLINT UNSIGNED".parse().unwrap();
        assert_eq!(schema.ty, Ty::USmallInt);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.to_string(), "SMALLINT UNSIGNED");
        let schema: DataType = "INT UNSIGNED".parse().unwrap();
        assert_eq!(schema.ty, Ty::UInt);
        assert_eq!(schema.len(), 4);
        assert_eq!(schema.to_string(), "INT UNSIGNED");
        let schema: DataType = "BIGINT UNSIGNED".parse().unwrap();
        assert_eq!(schema.ty, Ty::UBigInt);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "BIGINT UNSIGNED");
        let schema: DataType = "FLOAT".parse().unwrap();
        assert_eq!(schema.ty, Ty::Float);
        assert_eq!(schema.len(), 4);
        assert_eq!(schema.to_string(), "FLOAT");
        let schema: DataType = "DOUBLE".parse().unwrap();
        assert_eq!(schema.ty, Ty::Double);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "DOUBLE");
        let schema: DataType = "TIMESTAMP".parse().unwrap();
        assert_eq!(schema.ty, Ty::Timestamp);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.to_string(), "TIMESTAMP");
        assert_eq!(format!("{schema:?}"), "ColSchema { ty: Timestamp, len: 8 }");
        let schema = "UNKNOWN".parse::<DataType>();
        assert!(schema.is_err());
        let schema = "DECIMAL(40,2)".parse::<DataType>();
        assert!(schema.is_err());
    }

    #[test]
    fn test_schema_decimal() {
        let schema: DataType = "DECIMAL(10,2)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal64);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.precision(), 10);
        assert_eq!(schema.scale(), 2);
        assert_eq!(schema.to_string(), "DECIMAL(10,2)");
        assert_eq!(
            format!("{schema:?}"),
            "ColSchema { ty: Decimal64, len: 8, prec: 10, scale: 2 }"
        );

        let schema: DataType = "DECIMAL(20,5)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal);
        assert_eq!(schema.len(), 16);
        assert_eq!(schema.precision(), 20);
        assert_eq!(schema.scale(), 5);
        assert_eq!(schema.to_string(), "DECIMAL(20,5)");

        let schema: DataType = "DECIMAL(38,0)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal);
        assert_eq!(schema.len(), 16);
        assert_eq!(schema.precision(), 38);
        assert_eq!(schema.scale(), 0);
        assert_eq!(schema.to_string(), "DECIMAL(38,0)");

        let schema: DataType = "DECIMAL(38)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal);
        assert_eq!(schema.len(), 16);
        assert_eq!(schema.precision(), 38);
        assert_eq!(schema.scale(), 0);
        assert_eq!(schema.to_string(), "DECIMAL(38,0)");

        let schema: DataType = "DECIMAL(18,4)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal64);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.precision(), 18);
        assert_eq!(schema.scale(), 4);
        assert_eq!(schema.to_string(), "DECIMAL(18,4)");

        let schema: DataType = "DECIMAL(18)".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal64);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.precision(), 18);
        assert_eq!(schema.scale(), 0);
        assert_eq!(schema.to_string(), "DECIMAL(18,0)");

        let schema: DataType = "DECIMAL".parse().unwrap();
        assert_eq!(schema.ty, Ty::Decimal64);
        assert_eq!(schema.len(), 8);
        assert_eq!(schema.precision(), 18);
        assert_eq!(schema.scale(), 0);
    }

    #[test]
    fn test_schema_decimal_invalid() {
        let schema = "DECIMAL(39,2)".parse::<DataType>();
        assert!(schema.is_err());
        let schema = "DECIMAL(0)".parse::<DataType>();
        assert!(schema.is_err());
        let schema = "DECIMAL(18,19)".parse::<DataType>();
        assert!(schema.is_err());
        let schema = "DECIMAL(18,20)".parse::<DataType>();
        assert!(schema.is_err());
        let schema = "DECIMAL(80)".parse::<DataType>();
        assert!(schema.is_err());
        let schema = "DECIMAL(18,2,3)".parse::<DataType>();
        assert!(schema.is_err());
    }
}
