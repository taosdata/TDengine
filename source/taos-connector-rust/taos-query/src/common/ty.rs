use std::fmt::{self, Display};
use std::os::raw::c_char;
use std::str::FromStr;

use serde::de::Visitor;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

/// TDengine data type enumeration.
///
/// | enum      | int | sql name          | rust type              |
/// | ----      |:---:| --- ----          |:---------:             |
/// | Null      | 0   | NULL              | None                   |
/// | Bool      | 1   | BOOL              | bool                   |
/// | TinyInt   | 2   | TINYINT           | i8                     |
/// | SmallInt  | 3   | SMALLINT          | i16                    |
/// | Int       | 4   | INT               | i32                    |
/// | BitInt    | 5   | BIGINT            | i64                    |
/// | Float     | 6   | FLOAT             | f32                    |
/// | Double    | 7   | DOUBLE            | f64                    |
/// | VarChar   | 8   | BINARY/VARCHAR    | str/String             |
/// | Timestamp | 9   | TIMESTAMP         | i64                    |
/// | NChar     | 10  | NCHAR             | str/String             |
/// | UTinyInt  | 11  | TINYINT UNSIGNED  | u8                     |
/// | USmallInt | 12  | SMALLINT UNSIGNED | u16                    |
/// | UInt      | 13  | INT UNSIGNED      | u32                    |
/// | UBigInt   | 14  | BIGINT UNSIGNED   | u64                    |
/// | Json      | 15  | JSON              | serde_json::Value      |
/// | VarBinary | 16  | VARBINARY         | Vec<u8>                |
/// | Decimal   | 17  | DECIMAL           | bigdecimal::BigDecimal |
/// | Blob      | 18  | BLOB              | Vec<u8>                |
/// | Geometry  | 20  | GEOMETRY          | Vec<u8>                |
/// | Decimal64 | 21  | DECIMAL           | bigdecimal::BigDecimal |
///
/// Notes:
/// - VarChar sql name is BINARY in TDengine 2.0, and VARCHAR in 3.0.
/// - Decimal and Blob types are not supported in TDengine 2.0.
/// - MediumBlob type is not supported in TDengine 2.0 and 3.0.
#[derive(
    Default,
    Debug,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    serde_repr::Serialize_repr,
    TryFromBytes,
    IntoBytes,
    Immutable,
    KnownLayout,
    Unaligned,
)]
#[repr(u8)]
#[non_exhaustive]
pub enum Ty {
    /// Null is only a value, not a *real* type, a nullable data type could be represented as [`Option<T>`] in Rust.
    ///
    /// A data type should never be Null.
    #[doc(hidden)]
    #[default]
    Null = 0,
    /// The `BOOL` type in sql, will be represented as [bool] in Rust.
    Bool = 1,
    /// The `TINYINT` type in sql, will be represented as [i8] in Rust.
    TinyInt = 2,
    /// The `SMALLINT` type in sql, will be represented as [i16] in Rust.
    SmallInt = 3,
    /// The `INT` type in sql, will be represented as [i32] in Rust.
    Int = 4,
    /// The `BIGINT` type in sql, will be represented as [i64] in Rust.
    BigInt = 5,
    /// The `FLOAT` type in sql, will be represented as [f32] in Rust.
    Float = 6,
    /// The `DOUBLE` type in sql, will be represented as [f64] in Rust.
    Double = 7,
    /// The `BINARY` type in sql for TDengine 2.x, `VARCHAR` for TDengine 3.x,
    /// will be represented as [&str] or [String] in Rust. This type of data be deserialized to [`Vec<u8>`].
    VarChar = 8,
    /// The `TIMESTAMP` type in sql, will be represented as [i64] in Rust.
    /// But can be deserialized to [chrono::naive::NaiveDateTime] or [String].
    Timestamp = 9,
    /// The `NCHAR` type in sql, will be represented as [&str] or [String] in Rust.
    /// The recommended way in TDengine to store utf-8 [String].
    NChar = 10,
    /// The `TINYINT UNSIGNED` type in sql, will be represented as [u8] in Rust.
    UTinyInt = 11,
    /// The `SMALLINT UNSIGNED` type in sql, will be represented as [u16] in Rust.
    USmallInt = 12,
    /// The `INT UNSIGNED` type in sql, will be represented as [u32] in Rust.
    UInt = 13,
    /// The `BIGINT UNSIGNED` type in sql, will be represented as [u64] in Rust.
    UBigInt = 14,
    /// The `JSON` type in sql, will be represented as [serde_json::value::Value] in Rust.
    Json = 15,
    /// The `VARBINARY` type in sql, will be represented as [`Vec<u8>`] in Rust.
    VarBinary = 16,
    /// The `DECIMAL` type in sql, will be represented as [bigdecimal::BigDecimal] in Rust.
    Decimal = 17,
    /// The `BLOB` type in sql, will be represented as [`Vec<u8>`] in Rust.
    Blob = 18,
    /// Not supported now.
    #[doc(hidden)]
    MediumBlob = 19,
    /// The `GEOMETRY` type in sql, will be represented as [`Vec<u8>`] in Rust.
    Geometry = 20,
    /// The `DECIMAL` type in sql, will be represented as [bigdecimal::BigDecimal] in Rust.
    Decimal64 = 21,
}

impl<'de> serde::Deserialize<'de> for Ty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TyVisitor;

        impl<'de> Visitor<'de> for TyVisitor {
            type Value = Ty;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("invalid TDengine type")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::from_u8(v as u8))
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::from_u8(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::from_u8(v as u8))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ty::from_str(v).map_err(<E as serde::de::Error>::custom)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_any(self)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Ty::Null)
            }
        }

        deserializer.deserialize_any(TyVisitor)
    }
}

impl FromStr for Ty {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        if let Some(s) = s.trim().strip_prefix("decimal") {
            let mut ps = s
                .split_terminator(['(', ',', ')'])
                .filter(|s| !s.trim().is_empty());
            let Some(precision) = ps.next() else {
                return Err("decimal type precision or scale not found".to_string());
            };
            let precision: u8 = precision
                .trim()
                .parse()
                .map_err(|e: std::num::ParseIntError| e.to_string())?;
            if precision <= 18 {
                return Ok(Self::Decimal64);
            }
            return Ok(Self::Decimal);
        }

        match s.as_str() {
            "timestamp" => Ok(Ty::Timestamp),
            "bool" => Ok(Ty::Bool),
            "tinyint" => Ok(Ty::TinyInt),
            "smallint" => Ok(Ty::SmallInt),
            "int" => Ok(Ty::Int),
            "bigint" => Ok(Ty::BigInt),
            "tinyint unsigned" => Ok(Ty::UTinyInt),
            "smallint unsigned" => Ok(Ty::USmallInt),
            "int unsigned" => Ok(Ty::UInt),
            "bigint unsigned" => Ok(Ty::UBigInt),
            "float" => Ok(Ty::Float),
            "double" => Ok(Ty::Double),
            "binary" | "varchar" => Ok(Ty::VarChar),
            "nchar" => Ok(Ty::NChar),
            "json" => Ok(Ty::Json),
            "varbinary" => Ok(Ty::VarBinary),
            "blob" => Ok(Ty::Blob),
            "mediumblob" => Ok(Ty::MediumBlob),
            "geometry" => Ok(Ty::Geometry),
            s => Err(format!("not a valid data type string: {s}")),
        }
    }
}

impl Ty {
    /// Check if the data type is null or not.
    pub const fn is_null(&self) -> bool {
        matches!(self, Ty::Null)
    }

    /// Var type is one of [Ty::VarChar], [Ty::VarBinary], [Ty::NChar], [Ty::Geometry].
    pub const fn is_var_type(&self) -> bool {
        use Ty::*;
        matches!(self, VarChar | VarBinary | NChar | Geometry)
    }

    pub const fn is_json(&self) -> bool {
        matches!(self, Ty::Json)
    }

    /// Is one of boolean/integers/float/double/decimal
    pub const fn is_primitive(&self) -> bool {
        use Ty::*;
        matches!(
            self,
            Bool | TinyInt
                | SmallInt
                | Int
                | BigInt
                | UTinyInt
                | USmallInt
                | UInt
                | UBigInt
                | Float
                | Double
                | Timestamp
                | Decimal
                | Decimal64
        )
    }

    /// Check if the data type is a numeric type.
    pub const fn is_numeric(&self) -> bool {
        use Ty::*;
        matches!(
            self,
            Bool | TinyInt
                | SmallInt
                | Int
                | BigInt
                | UTinyInt
                | USmallInt
                | UInt
                | UBigInt
                | Float
                | Double
                | Decimal
                | Decimal64
        )
    }

    /// Check if the data type is decimal.
    pub const fn is_decimal(&self) -> bool {
        use Ty::*;
        matches!(self, Decimal | Decimal64)
    }

    /// Check if the data type is blob.
    pub const fn is_blob(&self) -> bool {
        matches!(self, Ty::Blob)
    }

    /// Get fixed length if the type is primitive.
    pub const fn fixed_length(&self) -> usize {
        use Ty::*;
        match self {
            Bool | TinyInt | UTinyInt => 1,
            SmallInt | USmallInt => 2,
            Int | UInt | Float => 4,
            BigInt | Double | Timestamp | UBigInt | Decimal64 => 8,
            Decimal => 16,
            _ => 0,
        }
    }

    /// The sql name of type.
    pub const fn name(&self) -> &'static str {
        use Ty::*;
        match self {
            Null => "NULL",
            Bool => "BOOL",
            TinyInt => "TINYINT",
            SmallInt => "SMALLINT",
            Int => "INT",
            BigInt => "BIGINT",
            Float => "FLOAT",
            Double => "DOUBLE",
            VarChar => "BINARY",
            Timestamp => "TIMESTAMP",
            NChar => "NCHAR",
            UTinyInt => "TINYINT UNSIGNED",
            USmallInt => "SMALLINT UNSIGNED",
            UInt => "INT UNSIGNED",
            UBigInt => "BIGINT UNSIGNED",
            Json => "JSON",
            VarBinary => "VARBINARY",
            Decimal | Decimal64 => "DECIMAL",
            Blob => "BLOB",
            MediumBlob => "MEDIUMBLOB",
            Geometry => "GEOMETRY",
        }
    }

    pub const fn lowercase_name(&self) -> &'static str {
        use Ty::*;
        match self {
            Null => "null",
            Bool => "bool",
            TinyInt => "tinyint",
            SmallInt => "smallint",
            Int => "int",
            BigInt => "bigint",
            Float => "float",
            Double => "double",
            VarChar => "binary",
            Timestamp => "timestamp",
            NChar => "nchar",
            UTinyInt => "tinyint unsigned",
            USmallInt => "smallint unsigned",
            UInt => "int unsigned",
            UBigInt => "bigint unsigned",
            Json => "json",
            VarBinary => "varbinary",
            Decimal | Decimal64 => "decimal",
            Blob => "blob",
            MediumBlob => "mediumblob",
            Geometry => "geometry",
        }
    }

    pub const fn tsdb_name(&self) -> *const c_char {
        use Ty::*;
        match self {
            Null => "TSDB_DATA_TYPE_NULL\0".as_ptr() as *const c_char,
            Bool => "TSDB_DATA_TYPE_BOOL\0".as_ptr() as *const c_char,
            TinyInt => "TSDB_DATA_TYPE_TINYINT\0".as_ptr() as *const c_char,
            SmallInt => "TSDB_DATA_TYPE_SMALLINT\0".as_ptr() as *const c_char,
            Int => "TSDB_DATA_TYPE_INT\0".as_ptr() as *const c_char,
            BigInt => "TSDB_DATA_TYPE_BIGINT\0".as_ptr() as *const c_char,
            Float => "TSDB_DATA_TYPE_FLOAT\0".as_ptr() as *const c_char,
            Double => "TSDB_DATA_TYPE_DOUBLE\0".as_ptr() as *const c_char,
            VarChar => "TSDB_DATA_TYPE_VARCHAR\0".as_ptr() as *const c_char,
            Timestamp => "TSDB_DATA_TYPE_TIMESTAMP\0".as_ptr() as *const c_char,
            NChar => "TSDB_DATA_TYPE_NCHAR\0".as_ptr() as *const c_char,
            UTinyInt => "TSDB_DATA_TYPE_UTINYINT\0".as_ptr() as *const c_char,
            USmallInt => "TSDB_DATA_TYPE_USMALLINT\0".as_ptr() as *const c_char,
            UInt => "TSDB_DATA_TYPE_UINT\0".as_ptr() as *const c_char,
            UBigInt => "TSDB_DATA_TYPE_UBIGINT\0".as_ptr() as *const c_char,
            Json => "TSDB_DATA_TYPE_JSON\0".as_ptr() as *const c_char,
            VarBinary => "TSDB_DATA_TYPE_VARBINARY\0".as_ptr() as *const c_char,
            Decimal => "TSDB_DATA_TYPE_DECIMAL\0".as_ptr() as *const c_char,
            Decimal64 => "TSDB_DATA_TYPE_DECIMAL64\0".as_ptr() as *const c_char,
            Blob => "TSDB_DATA_TYPE_BLOB\0".as_ptr() as *const c_char,
            MediumBlob => "TSDB_DATA_TYPE_MEDIUMBLOB\0".as_ptr() as *const c_char,
            Geometry => "TSDB_DATA_TYPE_GEOMETRY\0".as_ptr() as *const c_char,
        }
    }

    #[inline]
    pub const fn from_u8_option(v: u8) -> Option<Self> {
        use Ty::*;
        match v {
            0 => Some(Null),
            1 => Some(Bool),
            2 => Some(TinyInt),
            3 => Some(SmallInt),
            4 => Some(Int),
            5 => Some(BigInt),
            6 => Some(Float),
            7 => Some(Double),
            8 => Some(VarChar),
            9 => Some(Timestamp),
            10 => Some(NChar),
            11 => Some(UTinyInt),
            12 => Some(USmallInt),
            13 => Some(UInt),
            14 => Some(UBigInt),
            15 => Some(Json),
            16 => Some(VarBinary),
            17 => Some(Decimal),
            18 => Some(Blob),
            19 => Some(MediumBlob),
            20 => Some(Geometry),
            21 => Some(Decimal64),
            _ => None,
        }
    }

    /// The enum constants directly to str.
    #[inline]
    pub(crate) const fn as_variant_str(&self) -> &'static str {
        use Ty::*;
        macro_rules! _var_str {
          ($($v:ident) *) => {
              match self {
                $($v => stringify!($v),) *
              }
          }
        }
        _var_str!(
            Null Bool TinyInt SmallInt Int BigInt UTinyInt USmallInt UInt UBigInt
            Float Double VarChar NChar Timestamp Json VarBinary Decimal Decimal64 Blob MediumBlob Geometry
        )
    }

    #[inline]
    const fn from_u8(v: u8) -> Self {
        use Ty::*;
        match v {
            0 => Null,
            1 => Bool,
            2 => TinyInt,
            3 => SmallInt,
            4 => Int,
            5 => BigInt,
            6 => Float,
            7 => Double,
            8 => VarChar,
            9 => Timestamp,
            10 => NChar,
            11 => UTinyInt,
            12 => USmallInt,
            13 => UInt,
            14 => UBigInt,
            15 => Json,
            16 => VarBinary,
            17 => Decimal,
            18 => Blob,
            19 => MediumBlob,
            20 => Geometry,
            21 => Decimal64,
            _ => panic!("unknown data type"),
        }
    }
}

impl From<u8> for Ty {
    #[inline]
    fn from(v: u8) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

impl Display for Ty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

macro_rules! _impl_from_primitive {
    ($($ty:ty) *) => {
        $(
            impl From<$ty> for Ty {
                #[inline]
                fn from(v: $ty) -> Self {
                    Self::from_u8(v as _)
                }
            }
        )*
    }
}

_impl_from_primitive!(i8 i16 i32 i64 u16 u32 u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal() -> anyhow::Result<()> {
        assert_eq!("DECIMAL(5,2)".parse::<Ty>().unwrap(), Ty::Decimal64);
        assert_eq!("DECIMAL(20,2)".parse::<Ty>().unwrap(), Ty::Decimal);

        assert_eq!(Ty::from_u8(17), Ty::Decimal);
        assert_eq!(Ty::from_u8(21), Ty::Decimal64);

        assert_eq!(Ty::Decimal.as_variant_str(), "Decimal");
        assert_eq!(Ty::Decimal64.as_variant_str(), "Decimal64");

        assert_eq!(Ty::Decimal.fixed_length(), 16);
        assert_eq!(Ty::Decimal64.fixed_length(), 8);

        assert!(!Ty::Decimal.is_json());
        assert!(!Ty::Decimal64.is_json());

        assert!(Ty::Decimal.is_primitive());
        assert!(Ty::Decimal64.is_primitive());

        assert_eq!(Ty::Decimal.name(), "DECIMAL");
        assert_eq!(Ty::Decimal64.name(), "DECIMAL");

        assert_eq!(Ty::Decimal.lowercase_name(), "decimal");
        assert_eq!(Ty::Decimal64.lowercase_name(), "decimal");

        assert_eq!(Ty::from_u8_option(17), Some(Ty::Decimal));
        assert_eq!(Ty::from_u8_option(21), Some(Ty::Decimal64));

        assert_eq!(Ty::from(17), Ty::Decimal);
        assert_eq!(Ty::from(21), Ty::Decimal64);

        assert_eq!(format!("{}", Ty::Decimal), "DECIMAL");
        assert_eq!(format!("{}", Ty::Decimal64), "DECIMAL");

        Ok(())
    }

    #[test]
    fn test_from_str() {
        assert_eq!("timestamp".parse::<Ty>().unwrap(), Ty::Timestamp);
        assert_eq!("bool".parse::<Ty>().unwrap(), Ty::Bool);
        assert_eq!("tinyint".parse::<Ty>().unwrap(), Ty::TinyInt);
        assert_eq!("smallint".parse::<Ty>().unwrap(), Ty::SmallInt);
        assert_eq!("int".parse::<Ty>().unwrap(), Ty::Int);
        assert_eq!("bigint".parse::<Ty>().unwrap(), Ty::BigInt);
        assert_eq!("tinyint unsigned".parse::<Ty>().unwrap(), Ty::UTinyInt);
        assert_eq!("smallint unsigned".parse::<Ty>().unwrap(), Ty::USmallInt);
        assert_eq!("int unsigned".parse::<Ty>().unwrap(), Ty::UInt);
        assert_eq!("bigint unsigned".parse::<Ty>().unwrap(), Ty::UBigInt);
        assert_eq!("float".parse::<Ty>().unwrap(), Ty::Float);
        assert_eq!("double".parse::<Ty>().unwrap(), Ty::Double);
        assert_eq!("binary".parse::<Ty>().unwrap(), Ty::VarChar);
        assert_eq!("varchar".parse::<Ty>().unwrap(), Ty::VarChar);
        assert_eq!("nchar".parse::<Ty>().unwrap(), Ty::NChar);
        assert_eq!("json".parse::<Ty>().unwrap(), Ty::Json);
        assert_eq!("varbinary".parse::<Ty>().unwrap(), Ty::VarBinary);
        assert_eq!("blob".parse::<Ty>().unwrap(), Ty::Blob);
        assert_eq!("mediumblob".parse::<Ty>().unwrap(), Ty::MediumBlob);
        assert_eq!("geometry".parse::<Ty>().unwrap(), Ty::Geometry);
        assert_eq!("decimal(5,2)".parse::<Ty>().unwrap(), Ty::Decimal64);
        assert_eq!("decimal(20,2)".parse::<Ty>().unwrap(), Ty::Decimal);
        assert_eq!(
            "invalid type".parse::<Ty>().unwrap_err(),
            "not a valid data type string: invalid type".to_string()
        );
    }

    #[test]
    fn test_is_null() {
        assert!(Ty::Null.is_null());
        assert!(!Ty::Bool.is_null());
        assert!(!Ty::Int.is_null());
        assert!(!Ty::VarChar.is_null());
    }

    #[test]
    fn test_name() {
        assert_eq!(Ty::Null.name(), "NULL");
        assert_eq!(Ty::Bool.name(), "BOOL");
        assert_eq!(Ty::TinyInt.name(), "TINYINT");
        assert_eq!(Ty::SmallInt.name(), "SMALLINT");
        assert_eq!(Ty::Int.name(), "INT");
        assert_eq!(Ty::BigInt.name(), "BIGINT");
        assert_eq!(Ty::Float.name(), "FLOAT");
        assert_eq!(Ty::Double.name(), "DOUBLE");
        assert_eq!(Ty::VarChar.name(), "BINARY");
        assert_eq!(Ty::Timestamp.name(), "TIMESTAMP");
        assert_eq!(Ty::NChar.name(), "NCHAR");
        assert_eq!(Ty::UTinyInt.name(), "TINYINT UNSIGNED");
        assert_eq!(Ty::USmallInt.name(), "SMALLINT UNSIGNED");
        assert_eq!(Ty::UInt.name(), "INT UNSIGNED");
        assert_eq!(Ty::UBigInt.name(), "BIGINT UNSIGNED");
        assert_eq!(Ty::Json.name(), "JSON");
        assert_eq!(Ty::VarBinary.name(), "VARBINARY");
        assert_eq!(Ty::Decimal.name(), "DECIMAL");
        assert_eq!(Ty::Decimal64.name(), "DECIMAL");
        assert_eq!(Ty::Blob.name(), "BLOB");
        assert_eq!(Ty::MediumBlob.name(), "MEDIUMBLOB");
        assert_eq!(Ty::Geometry.name(), "GEOMETRY");
    }

    #[test]
    fn test_lowercase_name() {
        assert_eq!(Ty::Null.lowercase_name(), "null");
        assert_eq!(Ty::Bool.lowercase_name(), "bool");
        assert_eq!(Ty::TinyInt.lowercase_name(), "tinyint");
        assert_eq!(Ty::SmallInt.lowercase_name(), "smallint");
        assert_eq!(Ty::Int.lowercase_name(), "int");
        assert_eq!(Ty::BigInt.lowercase_name(), "bigint");
        assert_eq!(Ty::Float.lowercase_name(), "float");
        assert_eq!(Ty::Double.lowercase_name(), "double");
        assert_eq!(Ty::VarChar.lowercase_name(), "binary");
        assert_eq!(Ty::Timestamp.lowercase_name(), "timestamp");
        assert_eq!(Ty::NChar.lowercase_name(), "nchar");
        assert_eq!(Ty::UTinyInt.lowercase_name(), "tinyint unsigned");
        assert_eq!(Ty::USmallInt.lowercase_name(), "smallint unsigned");
        assert_eq!(Ty::UInt.lowercase_name(), "int unsigned");
        assert_eq!(Ty::UBigInt.lowercase_name(), "bigint unsigned");
        assert_eq!(Ty::Json.lowercase_name(), "json");
        assert_eq!(Ty::VarBinary.lowercase_name(), "varbinary");
        assert_eq!(Ty::Decimal.lowercase_name(), "decimal");
        assert_eq!(Ty::Decimal64.lowercase_name(), "decimal");
        assert_eq!(Ty::Blob.lowercase_name(), "blob");
        assert_eq!(Ty::MediumBlob.lowercase_name(), "mediumblob");
        assert_eq!(Ty::Geometry.lowercase_name(), "geometry");
    }

    #[test]
    fn test_tsdb_name() {
        use std::ffi::CStr;

        unsafe {
            assert_eq!(CStr::from_ptr(Ty::Null.tsdb_name()), c"TSDB_DATA_TYPE_NULL");
            assert_eq!(CStr::from_ptr(Ty::Bool.tsdb_name()), c"TSDB_DATA_TYPE_BOOL");
            assert_eq!(
                CStr::from_ptr(Ty::TinyInt.tsdb_name()),
                c"TSDB_DATA_TYPE_TINYINT"
            );
            assert_eq!(
                CStr::from_ptr(Ty::SmallInt.tsdb_name()),
                c"TSDB_DATA_TYPE_SMALLINT"
            );
            assert_eq!(CStr::from_ptr(Ty::Int.tsdb_name()), c"TSDB_DATA_TYPE_INT");
            assert_eq!(
                CStr::from_ptr(Ty::BigInt.tsdb_name()),
                c"TSDB_DATA_TYPE_BIGINT"
            );
            assert_eq!(
                CStr::from_ptr(Ty::Float.tsdb_name()),
                c"TSDB_DATA_TYPE_FLOAT"
            );
            assert_eq!(
                CStr::from_ptr(Ty::Double.tsdb_name()),
                c"TSDB_DATA_TYPE_DOUBLE"
            );
            assert_eq!(
                CStr::from_ptr(Ty::VarChar.tsdb_name()),
                c"TSDB_DATA_TYPE_VARCHAR"
            );
            assert_eq!(
                CStr::from_ptr(Ty::Timestamp.tsdb_name()),
                c"TSDB_DATA_TYPE_TIMESTAMP"
            );
            assert_eq!(
                CStr::from_ptr(Ty::NChar.tsdb_name()),
                c"TSDB_DATA_TYPE_NCHAR"
            );
            assert_eq!(
                CStr::from_ptr(Ty::UTinyInt.tsdb_name()),
                c"TSDB_DATA_TYPE_UTINYINT"
            );
            assert_eq!(
                CStr::from_ptr(Ty::USmallInt.tsdb_name()),
                c"TSDB_DATA_TYPE_USMALLINT"
            );
            assert_eq!(CStr::from_ptr(Ty::UInt.tsdb_name()), c"TSDB_DATA_TYPE_UINT");
            assert_eq!(
                CStr::from_ptr(Ty::UBigInt.tsdb_name()),
                c"TSDB_DATA_TYPE_UBIGINT"
            );
            assert_eq!(CStr::from_ptr(Ty::Json.tsdb_name()), c"TSDB_DATA_TYPE_JSON");
            assert_eq!(
                CStr::from_ptr(Ty::VarBinary.tsdb_name()),
                c"TSDB_DATA_TYPE_VARBINARY"
            );
            assert_eq!(
                CStr::from_ptr(Ty::Decimal.tsdb_name()),
                c"TSDB_DATA_TYPE_DECIMAL"
            );
            assert_eq!(
                CStr::from_ptr(Ty::Decimal64.tsdb_name()),
                c"TSDB_DATA_TYPE_DECIMAL64"
            );
            assert_eq!(CStr::from_ptr(Ty::Blob.tsdb_name()), c"TSDB_DATA_TYPE_BLOB");
            assert_eq!(
                CStr::from_ptr(Ty::MediumBlob.tsdb_name()),
                c"TSDB_DATA_TYPE_MEDIUMBLOB"
            );
            assert_eq!(
                CStr::from_ptr(Ty::Geometry.tsdb_name()),
                c"TSDB_DATA_TYPE_GEOMETRY"
            );
        }
    }

    #[test]
    fn test_from_u8_option() {
        assert_eq!(Ty::from_u8_option(0), Some(Ty::Null));
        assert_eq!(Ty::from_u8_option(1), Some(Ty::Bool));
        assert_eq!(Ty::from_u8_option(2), Some(Ty::TinyInt));
        assert_eq!(Ty::from_u8_option(3), Some(Ty::SmallInt));
        assert_eq!(Ty::from_u8_option(4), Some(Ty::Int));
        assert_eq!(Ty::from_u8_option(5), Some(Ty::BigInt));
        assert_eq!(Ty::from_u8_option(6), Some(Ty::Float));
        assert_eq!(Ty::from_u8_option(7), Some(Ty::Double));
        assert_eq!(Ty::from_u8_option(8), Some(Ty::VarChar));
        assert_eq!(Ty::from_u8_option(9), Some(Ty::Timestamp));
        assert_eq!(Ty::from_u8_option(10), Some(Ty::NChar));
        assert_eq!(Ty::from_u8_option(11), Some(Ty::UTinyInt));
        assert_eq!(Ty::from_u8_option(12), Some(Ty::USmallInt));
        assert_eq!(Ty::from_u8_option(13), Some(Ty::UInt));
        assert_eq!(Ty::from_u8_option(14), Some(Ty::UBigInt));
        assert_eq!(Ty::from_u8_option(15), Some(Ty::Json));
        assert_eq!(Ty::from_u8_option(16), Some(Ty::VarBinary));
        assert_eq!(Ty::from_u8_option(17), Some(Ty::Decimal));
        assert_eq!(Ty::from_u8_option(18), Some(Ty::Blob));
        assert_eq!(Ty::from_u8_option(19), Some(Ty::MediumBlob));
        assert_eq!(Ty::from_u8_option(20), Some(Ty::Geometry));
        assert_eq!(Ty::from_u8_option(21), Some(Ty::Decimal64));
        assert_eq!(Ty::from_u8_option(22), None);
    }

    #[test]
    fn test_from_u8() {
        assert_eq!(Ty::from(0), Ty::Null);
        assert_eq!(Ty::from(1), Ty::Bool);
        assert_eq!(Ty::from(2), Ty::TinyInt);
        assert_eq!(Ty::from(3), Ty::SmallInt);
        assert_eq!(Ty::from(4), Ty::Int);
        assert_eq!(Ty::from(5), Ty::BigInt);
        assert_eq!(Ty::from(6), Ty::Float);
        assert_eq!(Ty::from(7), Ty::Double);
        assert_eq!(Ty::from(8), Ty::VarChar);
        assert_eq!(Ty::from(9), Ty::Timestamp);
        assert_eq!(Ty::from(10), Ty::NChar);
        assert_eq!(Ty::from(11), Ty::UTinyInt);
        assert_eq!(Ty::from(12), Ty::USmallInt);
        assert_eq!(Ty::from(13), Ty::UInt);
        assert_eq!(Ty::from(14), Ty::UBigInt);
        assert_eq!(Ty::from(15), Ty::Json);
        assert_eq!(Ty::from(16), Ty::VarBinary);
        assert_eq!(Ty::from(17), Ty::Decimal);
        assert_eq!(Ty::from(18), Ty::Blob);
        assert_eq!(Ty::from(19), Ty::MediumBlob);
        assert_eq!(Ty::from(20), Ty::Geometry);
        assert_eq!(Ty::from(21), Ty::Decimal64);
    }
}
