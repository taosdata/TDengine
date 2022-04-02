use crate::timestamp::TimestampValue;

use serde_json::Value as Json;

use super::{BorrowedValue, Value};

impl<'block, 'a> From<&'a BorrowedValue<'block>> for Value {
    fn from(v: &'a BorrowedValue<'block>) -> Self {
        v.to_value()
    }
}

impl<'block> From<BorrowedValue<'block>> for Value {
    fn from(v: BorrowedValue<'block>) -> Self {
        v.into_value()
    }
}

macro_rules! from_primitives {
    ($($ty:ident $to:ident),*) => {
        $(
            impl From<$ty> for Value {
                fn from(n: $ty) -> Self {
                    Value::$to(n)
                }
            }
        )*
    };
}

from_primitives!(bool Bool,
                i8 TinyInt,
                i16 SmallInt,
                i32 Int,
                i64 BigInt,
                u8 UTinyInt,
                u16 USmallInt,
                u32 UInt,
                u64 UBigInt,
                f32 Float,
                f64 Double,
                TimestampValue Timestamp,
                Json Json);
