use std::str::FromStr;

use num_enum::FromPrimitive;

pub const TSDB_DATA_TYPE_NULL: TaosDataType = TaosDataType::Null; // 1 bytes
pub const TSDB_DATA_TYPE_BOOL: TaosDataType = TaosDataType::Bool; // 1 bytes
pub const TSDB_DATA_TYPE_TINYINT: TaosDataType = TaosDataType::TinyInt; // 1 byte
pub const TSDB_DATA_TYPE_SMALLINT: TaosDataType = TaosDataType::SmallInt; // 2 bytes
pub const TSDB_DATA_TYPE_INT: TaosDataType = TaosDataType::Int; // 4 bytes
pub const TSDB_DATA_TYPE_BIGINT: TaosDataType = TaosDataType::BigInt; // 8 bytes
pub const TSDB_DATA_TYPE_FLOAT: TaosDataType = TaosDataType::Float; // 4 bytes
pub const TSDB_DATA_TYPE_DOUBLE: TaosDataType = TaosDataType::Double; // 8 bytes
#[cfg(taos_v2)]
pub const TSDB_DATA_TYPE_BINARY: TaosDataType = TaosDataType::Binary; // string, alias for varchar
#[cfg(taos_v3)]
pub const TSDB_DATA_TYPE_BINARY: TaosDataType = TaosDataType::VarChar; // string, alias for varchar
pub const TSDB_DATA_TYPE_TIMESTAMP: TaosDataType = TaosDataType::Timestamp; // 8 bytes
pub const TSDB_DATA_TYPE_NCHAR: TaosDataType = TaosDataType::NChar; // unicode string
pub const TSDB_DATA_TYPE_UTINYINT: TaosDataType = TaosDataType::UTinyInt; // 1 byte
pub const TSDB_DATA_TYPE_USMALLINT: TaosDataType = TaosDataType::USmallInt; // 2 bytes
pub const TSDB_DATA_TYPE_UINT: TaosDataType = TaosDataType::UInt; // 4 bytes
pub const TSDB_DATA_TYPE_UBIGINT: TaosDataType = TaosDataType::UBigInt; // 8 bytes
pub const TSDB_DATA_TYPE_JSON: TaosDataType = TaosDataType::Json; // json
#[cfg(taos_v3)]
pub const TSDB_DATA_TYPE_VARCHAR: TaosDataType = TaosDataType::VarChar; // string
#[cfg(taos_v3)]
pub const TSDB_DATA_TYPE_VARBINARY: TaosDataType = TaosDataType::VarBinary; // binary
#[cfg(taos_v3)]
pub const TSDB_DATA_TYPE_DECIMAL: TaosDataType = TaosDataType::Decimal; // decimal
#[cfg(taos_v3)]
pub const TSDB_DATA_TYPE_BLOB: TaosDataType = TaosDataType::Blob; // binary
#[cfg(taos_v3)]
pub const TSDB_DATA_TYPE_MEDIUMBLOB: TaosDataType = TaosDataType::MediumBlob; // binary

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
#[cfg_attr(
    feature = "serde",
    derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)
)]
#[repr(u8)]
pub enum TaosDataType {
    Null = 0,
    Bool,     // 1
    TinyInt,  // 2
    SmallInt, // 3
    Int,      // 4
    BigInt,   // 5
    Float,    // 6
    Double,   // 7
    #[cfg(taos_v2)]
    Binary = 8, // 8
    #[cfg(taos_v3)]
    VarChar = 8, // 8, since 3.0 Binary is just an alias of VarChar
    Timestamp, // 9
    NChar,    // 10
    UTinyInt, // 11
    USmallInt, // 12
    UInt,     // 13
    UBigInt,  // 14
    Json,     // 15
    #[cfg(taos_v3)]
    VarBinary, // 16
    #[cfg(taos_v3)]
    Decimal, // 17
    #[cfg(taos_v3)]
    Blob, // 18
    #[cfg(taos_v3)]
    MediumBlob, // 18
    #[num_enum(default)]
    Unknown = 255,
}

impl FromStr for TaosDataType {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "timestamp" => Ok(TaosDataType::Timestamp),
            "bool" => Ok(TaosDataType::Bool),
            "tinyint" => Ok(TaosDataType::TinyInt),
            "smallint" => Ok(TaosDataType::SmallInt),
            "int" => Ok(TaosDataType::Int),
            "bigint" => Ok(TaosDataType::BigInt),
            "tinyint unsigned" => Ok(TaosDataType::UTinyInt),
            "smallint unsigned" => Ok(TaosDataType::USmallInt),
            "int unsigned" => Ok(TaosDataType::UInt),
            "bigint unsigned" => Ok(TaosDataType::UBigInt),
            "float" => Ok(TaosDataType::Float),
            "double" => Ok(TaosDataType::Double),
            "binary" | "varchar" => Ok(TSDB_DATA_TYPE_BINARY),
            "nchar" => Ok(TaosDataType::NChar),
            "json" => Ok(TaosDataType::Json),
            _ => Err("not a valid data type string"),
        }
    }
}

impl TaosDataType {
    pub const fn as_str(&self) -> &'static str {
        use TaosDataType::*;

        match self {
            Null => "NULL",
            Bool => "BOOL",
            TinyInt => "TINYINT",
            SmallInt => "SMALLINT",
            Int => "INT",
            BigInt => "BIGINT",
            Float => "FLOAT",
            Double => "DOUBLE",
            Binary => "BINARY",
            Timestamp => "TIMESTAMP",
            NChar => "NCHAR",
            UTinyInt => "TINYINT UNSIGNED",
            USmallInt => "SMALLINT UNSIGNED",
            UInt => "INT UNSIGNED",
            UBigInt => "BIGINT UNSIGNED",
            Json => "JSON",
            Unknown => "UNKNOWN",
        }
    }
    pub const fn as_variant_str(&self) -> &'static str {
        use TaosDataType::*;

        match self {
            Null => "Null",
            Bool => "Bool",
            TinyInt => "TinyInt",
            SmallInt => "SmallInt",
            Int => "Int",
            BigInt => "BigInt",
            Float => "Float",
            Double => "Double",
            Binary => "Binary",
            Timestamp => "Timestamp",
            NChar => "NChar",
            UTinyInt => "UTinyInt",
            USmallInt => "USmallInt",
            UInt => "UInt",
            UBigInt => "UBigInt",
            Json => "Json",
            Unknown => "Unknown",
        }
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::de::VariantAccess<'de> for TaosDataType {
    type Error = taos_error::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
        todo!()
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }
}
