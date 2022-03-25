use crate::timestamp::TimestampValue;

#[derive(Debug)]
pub enum BorrowedValue<'block> {
    Null,        // 0
    Bool(bool),  // 1
    TinyInt(i8), // 2
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Binary(&'block [u8]),
    Timestamp(TimestampValue),
    NChar(&'block str),
    UTinyInt(u8),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64), // 14
    Json(&'block [u8]),
    VarChar(&'block [u8]),
    VarBinary(&'block [u8]),
    Decimal(f64),
    Blob(&'block [u8]),
}
