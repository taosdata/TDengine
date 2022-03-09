use num_enum::FromPrimitive;

pub const TSDB_DATA_TYPE_NULL: TaosDataType = TaosDataType::Null; // 1 bytes
pub const TSDB_DATA_TYPE_BOOL: TaosDataType = TaosDataType::Bool; // 1 bytes
pub const TSDB_DATA_TYPE_TINYINT: TaosDataType = TaosDataType::TinyInt; // 1 byte
pub const TSDB_DATA_TYPE_SMALLINT: TaosDataType = TaosDataType::SmallInt; // 2 bytes
pub const TSDB_DATA_TYPE_INT: TaosDataType = TaosDataType::Int; // 4 bytes
pub const TSDB_DATA_TYPE_BIGINT: TaosDataType = TaosDataType::BigInt; // 8 bytes
pub const TSDB_DATA_TYPE_FLOAT: TaosDataType = TaosDataType::Float; // 4 bytes
pub const TSDB_DATA_TYPE_DOUBLE: TaosDataType = TaosDataType::Double; // 8 bytes
pub const TSDB_DATA_TYPE_BINARY: TaosDataType = TaosDataType::Binary; // string, alias for varchar
pub const TSDB_DATA_TYPE_TIMESTAMP: TaosDataType = TaosDataType::Timestamp; // 8 bytes
pub const TSDB_DATA_TYPE_NCHAR: TaosDataType = TaosDataType::NChar; // unicode string
pub const TSDB_DATA_TYPE_UTINYINT: TaosDataType = TaosDataType::UTinyInt; // 1 byte
pub const TSDB_DATA_TYPE_USMALLINT: TaosDataType = TaosDataType::USmallInt; // 2 bytes
pub const TSDB_DATA_TYPE_UINT: TaosDataType = TaosDataType::UInt; // 4 bytes
pub const TSDB_DATA_TYPE_UBIGINT: TaosDataType = TaosDataType::UBigInt; // 8 bytes
#[cfg(v2)]
pub const TSDB_DATA_TYPE_JSON: TaosDataType = TaosDataType::Json; // json
#[cfg(v3)]
pub const TSDB_DATA_TYPE_JSON: TaosDataType = TaosDataType::Json; // json
#[cfg(v3)]
pub const TSDB_DATA_TYPE_VARCHAR: TaosDataType = TaosDataType::VarChar; // string
#[cfg(v3)]
pub const TSDB_DATA_TYPE_VARBINARY: TaosDataType = TaosDataType::VarBinary; // binary
#[cfg(v3)]
pub const TSDB_DATA_TYPE_DECIMAL: TaosDataType = TaosDataType::Decimal; // decimal
#[cfg(v3)]
pub const TSDB_DATA_TYPE_BLOB: TaosDataType = TaosDataType::Blob; // binary

#[derive(Debug, FromPrimitive)]
#[repr(u8)]
pub enum TaosDataType {
    Null = 0,
    Bool,      // 1
    TinyInt,   // 2
    SmallInt,  // 3
    Int,       // 4
    BigInt,    // 5
    Float,     // 6
    Double,    // 7
    Binary,    // 8
    Timestamp, // 9
    NChar,     // 10
    UTinyInt,  // 11
    USmallInt, // 12
    UInt,      // 13
    UBigInt,   // 14
    #[cfg(v2)]
    Json, // 15
    #[cfg(v3)]
    VarChar, // 15
    #[cfg(v3)]
    VarBinary, // 16
    #[cfg(v3)]
    Json, // 17
    #[cfg(v3)]
    Decimal, // 18
    #[cfg(v3)]
    Blob, // 19
    #[num_enum(default)]
    Unknown = 255,
}

#[test]
fn type_json_id() {
    #[cfg(v2)]
    assert_eq!(TaosDataType::Json as u32, 15);
    #[cfg(v3)]
    {
        println!("test in v3");
        assert_eq!(TaosDataType::Json as u32, 17);
    }
}
