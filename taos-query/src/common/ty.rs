use std::{
    fmt::{self, Display},
    str::FromStr,
};

use serde::de::Visitor;

/// TDengine data type enumeration.
///
/// | enum       | int | sql name         | rust type |
/// | ----       |:---:| --------         |:---------:|
/// | Null       | 0   | NULL             | None      |
/// | Bool       | 1   | BOOL             | bool      |
/// | TinyInt    | 2   | TINYINT          | i8        |
/// | SmallInt   | 3   | SMALLINT         | i16       |
/// | Int        | 4   | INT              | i32       |
/// | BitInt     | 5   | BIGINT           | i64       |
/// | Float      | 6   | FLOAT            | f32       |
/// | Double     | 7   | DOUBLE           | f64       |
/// | VarChar    | 8   | BINARY/VARCHAR   | str/String        |
/// | Timestamp  | 9   | TIMESTAMP        | i64               |
/// | NChar      | 10  | NCHAR            | str/String        |
/// | UTinyInt   | 11  | TINYINT UNSIGNED | u8                |
/// | USmallInt  | 12  | SMALLINT UNSIGNED| u16               |
/// | UInt       | 13  | INT UNSIGNED     | u32               |
/// | UBigInt    | 14  | BIGINT UNSIGNED  | u64               |
/// | Json       | 15  | JSON             | serde_json::Value |
///
/// Note:
/// - VarChar sql name is BINARY in v2, and VARCHAR in v3.
/// - Decimal/Blob/MediumBlob is not supported in 2.0/3.0 .
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde_repr::Serialize_repr)]
#[repr(u8)]
#[non_exhaustive]
pub enum Ty {
    /// Null is only a value, not a *real* type, a nullable data type could be represented as [Option<T>] in Rust.
    ///
    /// A data type should never be Null.
    #[doc(hidden)]
    Null = 0,
    /// The `BOOL` type in sql, will be represented as [bool] in Rust.
    Bool = 1,
    /// `TINYINT` type in sql, will be represented in Rust as [i8].
    TinyInt = 2,
    /// `SMALLINT` type in sql, will be represented in Rust as [i16].
    SmallInt = 3,
    /// `INT` type in sql, will be represented in Rust as [i32].
    Int = 4,
    /// `BIGINT` type in sql, will be represented in Rust as [i64].
    BigInt = 5, // 5
    /// UTinyInt, `tinyint unsigned` in sql, [u8] in Rust.
    UTinyInt = 11, // 11
    /// 12: USmallInt, `smallint unsigned` in sql, [u16] in Rust.
    USmallInt = 12, // 12
    /// 13: UInt, `int unsigned` in sql, [u32] in Rust.
    UInt = 13, // 13
    /// 14: UBigInt, `bigint unsigned` in sql, [u64] in Rust.
    UBigInt = 14, // 14
    /// 6: Float, `float` type in sql, will be represented in Rust as [f32].
    Float = 6, // 6
    /// 7: Double, `tinyint` type in sql, will be represented in Rust as [f64].
    Double = 7, // 7
    /// 9: Timestamp, `timestamp` type in sql, will be represented as [i64] in Rust.
    /// But can be deserialized to [chrono::naive::NaiveDateTime] or [String].
    Timestamp = 9, // 9
    /// 8: VarChar, `binary` type in sql for TDengine 2.x, `varchar` for TDengine 3.x,
    ///  will be represented in Rust as [&str] or [String]. This type of data be deserialized to [Vec<u8>].
    VarChar = 8,
    /// 10: NChar, `nchar` type in sql, the recommended way in TDengine to store utf-8 [String].
    NChar = 10, // 10
    /// 15: Json, `json` tag in sql, will be represented as [serde_json::value::Value] in Rust.
    Json = 15, // 15

    /// 16, VarBinary, `varbinary` in sql, [Vec<u8>] in Rust, which is supported since TDengine 3.0.
    #[doc(hidden)]
    VarBinary, // 16
    /// 17, Not supported now.
    #[doc(hidden)]
    Decimal, // 17
    /// 18, Not supported now.
    #[doc(hidden)]
    Blob, // 18
    /// 19, Not supported now.
    #[doc(hidden)]
    MediumBlob, // 19
}

impl Default for Ty {
    fn default() -> Self {
        Ty::Null
    }
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
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
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
            "decimal" => Ok(Ty::Decimal),
            "blob" => Ok(Ty::Blob),
            "mediumblob" => Ok(Ty::MediumBlob),
            _ => Err("not a valid data type string"),
        }
    }
}

impl Ty {
    /// Check if the data type is null or not.
    pub const fn is_null(&self) -> bool {
        matches!(self, Ty::Null)
    }

    /// Var type is one of [Ty::VarChar], [Ty::VarBinary], [Ty::NChar].
    pub const fn is_var_type(&self) -> bool {
        use Ty::*;
        matches!(self, VarChar | VarBinary | NChar)
    }

    // /// Check if the data type need quotes, means one of [Ty::VarChar], [Ty::NChar], [Ty::Json].
    // pub const fn is_quote(&self) -> bool {
    //     use Ty::*;
    //     matches!(self, Json)
    // }

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
                | Decimal
        )
    }

    /// Get fixed length if the type is primitive.
    pub const fn fixed_length(&self) -> usize {
        use Ty::*;
        match self {
            Bool => 1,
            TinyInt => 1,
            SmallInt => 2,
            Int => 4,
            BigInt => 8,
            Float => 4,
            Double => 8,
            Timestamp => 8,
            UTinyInt => 1,
            USmallInt => 2,
            UInt => 4,
            UBigInt => 8,
            Decimal => 16,
            _ => usize::MAX,
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
            Decimal => "DECIMAL",
            Blob => "BLOB",
            MediumBlob => "MEDIUMBLOB",
            _ => panic!("not a type"),
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
            Decimal => "decimal",
            Blob => "blob",
            MediumBlob => "mediumblob",
            _ => panic!("not a type"),
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
        return _var_str!(
            Null Bool TinyInt SmallInt Int BigInt UTinyInt USmallInt UInt UBigInt
            Float Double VarChar NChar Timestamp Json VarBinary Decimal Blob MediumBlob
        );
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
