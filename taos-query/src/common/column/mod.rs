use bitvec_simd::BitVec;
use itertools::Itertools;

// use crate::stmt::MultiBind;

use super::{value::BorrowedValue, Value};

#[derive(Debug, serde::Serialize)]
pub enum BorrowedColumn<'b> {
    Null(usize),
    Bool(BitVec, &'b [bool]),  // 1
    TinyInt(BitVec, &'b [i8]), // 2
    SmallInt(BitVec, &'b [i16]),
    Int(BitVec, &'b [i32]),
    BigInt(BitVec, &'b [i64]),
    Float(BitVec, &'b [f32]),
    Double(BitVec, &'b [f64]),
    Binary(Vec<Option<&'b [u8]>>),
    Timestamp(BitVec, &'b [i64]),
    NChar(Vec<Option<&'b str>>),
    UTinyInt(BitVec, &'b [u8]),
    USmallInt(BitVec, &'b [u16]),
    UInt(BitVec, &'b [u32]),
    UBigInt(BitVec, &'b [u64]), // 14
    Json(BitVec, &'b [u8]),
    VarChar(BitVec, Vec<&'b [u8]>),
    VarBinary(BitVec, Vec<&'b [u8]>),
    Decimal(BitVec, &'b [f64]),
    Blob(BitVec, Vec<&'b [u8]>),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Column {
    Null(usize),
    Bool(BitVec, Vec<bool>),  // 1
    TinyInt(BitVec, Vec<i8>), // 2
    SmallInt(BitVec, Vec<i16>),
    Int(BitVec, Vec<i32>),
    BigInt(BitVec, Vec<i64>),
    Float(BitVec, Vec<f32>),
    Double(BitVec, Vec<f64>),
    Binary(Vec<Option<Vec<u8>>>),
    Timestamp(BitVec, Vec<i64>),
    NChar(Vec<Option<String>>),
    UTinyInt(BitVec, Vec<u8>),
    USmallInt(BitVec, Vec<u16>),
    UInt(BitVec, Vec<u32>),
    UBigInt(BitVec, Vec<u64>), // 14
    Json(BitVec, Vec<u8>),
    VarChar(BitVec, Vec<Vec<u8>>),
    VarBinary(BitVec, Vec<Vec<u8>>),
    Decimal(BitVec, Vec<f64>),
    Blob(BitVec, Vec<Vec<u8>>),
}

impl<'block> BorrowedColumn<'block> {
    pub fn get(&self, index: usize) -> BorrowedValue<'block> {
        macro_rules! get_primitive {
            ($target:ident, $nulls:expr, $values:expr) => {
                paste::paste! {
                    if $nulls.get_unchecked(index) {
                        BorrowedValue::Null
                    } else {
                        BorrowedValue::$target(*unsafe { $values.get_unchecked(index) })
                    }
                }
            };
        }
        match self {
            Self::Null(_n) => BorrowedValue::Null,
            Self::Bool(nulls, values) => get_primitive!(Bool, nulls, values),
            Self::TinyInt(nulls, values) => get_primitive!(TinyInt, nulls, values),
            Self::SmallInt(nulls, values) => get_primitive!(SmallInt, nulls, values),
            Self::Int(nulls, values) => get_primitive!(Int, nulls, values),
            Self::BigInt(nulls, values) => get_primitive!(BigInt, nulls, values),
            Self::UTinyInt(nulls, values) => get_primitive!(UTinyInt, nulls, values),
            Self::USmallInt(nulls, values) => get_primitive!(USmallInt, nulls, values),
            Self::UInt(nulls, values) => get_primitive!(UInt, nulls, values),
            Self::UBigInt(nulls, values) => get_primitive!(UBigInt, nulls, values),
            Self::Float(nulls, values) => get_primitive!(Float, nulls, values),
            Self::Double(nulls, values) => get_primitive!(Double, nulls, values),
            Self::Timestamp(nulls, _values) => {
                if nulls.get_unchecked(index) {
                    BorrowedValue::Null
                } else {
                    // BorrowedValue::Timestamp(TimestampValue::new(*unsafe { values.get_unchecked(index) }, self.precision()))
                    todo!()
                }
            }
            Self::Binary(values) => match unsafe { values.get_unchecked(index) } {
                Some(bytes) => {
                    BorrowedValue::VarChar(unsafe { std::str::from_utf8_unchecked(bytes) })
                }
                None => BorrowedValue::Null,
            },
            Self::NChar(_values) => BorrowedValue::Null,
            _ => unreachable!(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Null(n) => *n,
            Self::Bool(_nulls, values) => values.len(),
            Self::TinyInt(_nulls, values) => values.len(),
            Self::SmallInt(_nulls, values) => values.len(),
            Self::Int(_nulls, values) => values.len(),
            Self::BigInt(_nulls, values) => values.len(),
            Self::UTinyInt(_nulls, values) => values.len(),
            Self::USmallInt(_nulls, values) => values.len(),
            Self::UInt(_nulls, values) => values.len(),
            Self::UBigInt(_nulls, values) => values.len(),
            Self::Float(_nulls, values) => values.len(),
            Self::Double(_nulls, values) => values.len(),
            Self::Timestamp(_nulls, values) => values.len(),
            Self::Binary(values) => values.len(),
            Self::NChar(values) => values.len(),
            _ => unreachable!(),
        }
    }

    pub fn to_json_array(&self) -> Vec<serde_json::Value> {
        (0..self.len())
            .map(|value| self.get(value).to_json_value())
            .collect()
    }
}

impl<'block> BorrowedColumn<'block> {
    pub fn into_owned(self) -> Column {
        match self {
            BorrowedColumn::Null(rows) => Column::Null(rows),
            BorrowedColumn::Bool(is_nulls, slice) => Column::Bool(is_nulls, slice.to_owned()),
            BorrowedColumn::TinyInt(is_nulls, slice) => Column::TinyInt(is_nulls, slice.to_owned()),
            BorrowedColumn::SmallInt(is_nulls, slice) => {
                Column::SmallInt(is_nulls, slice.to_owned())
            }
            BorrowedColumn::Int(is_nulls, slice) => Column::Int(is_nulls, slice.to_owned()),
            BorrowedColumn::BigInt(is_nulls, slice) => Column::BigInt(is_nulls, slice.to_owned()),
            BorrowedColumn::UTinyInt(is_nulls, slice) => {
                Column::UTinyInt(is_nulls, slice.to_owned())
            }
            BorrowedColumn::USmallInt(is_nulls, slice) => {
                Column::USmallInt(is_nulls, slice.to_owned())
            }
            BorrowedColumn::UInt(is_nulls, slice) => Column::UInt(is_nulls, slice.to_owned()),
            BorrowedColumn::UBigInt(is_nulls, slice) => Column::UBigInt(is_nulls, slice.to_owned()),

            BorrowedColumn::Float(is_nulls, slice) => Column::Float(is_nulls, slice.to_owned()),
            BorrowedColumn::Double(is_nulls, slice) => Column::Double(is_nulls, slice.to_owned()),
            BorrowedColumn::Binary(binary) => Column::Binary(
                binary
                    .into_iter()
                    .map(|val| val.map(ToOwned::to_owned))
                    .collect_vec(),
            ),
            BorrowedColumn::NChar(binary) => Column::NChar(
                binary
                    .into_iter()
                    .map(|val| val.map(|val| val.to_string()))
                    .collect_vec(),
            ),
            BorrowedColumn::Timestamp(is_nulls, slice) => {
                Column::Timestamp(is_nulls, slice.to_owned())
            }
            _ => unreachable!("unsupported data type"),
        }
    }
}

impl Column {
    pub fn get(&self, index: usize) -> Value {
        macro_rules! get_primitive {
            ($target:ident, $nulls:expr, $values:expr) => {
                paste::paste! {
                    if $nulls.get_unchecked(index) {
                        Value::Null
                    } else {
                        Value::$target(*unsafe { $values.get_unchecked(index) })
                    }
                }
            };
        }
        match self {
            Self::Null(_n) => Value::Null,
            Self::Bool(nulls, values) => get_primitive!(Bool, nulls, values),
            Self::TinyInt(nulls, values) => get_primitive!(TinyInt, nulls, values),
            Self::SmallInt(nulls, values) => get_primitive!(SmallInt, nulls, values),
            Self::Int(nulls, values) => get_primitive!(Int, nulls, values),
            Self::BigInt(nulls, values) => get_primitive!(BigInt, nulls, values),
            Self::UTinyInt(nulls, values) => get_primitive!(UTinyInt, nulls, values),
            Self::USmallInt(nulls, values) => get_primitive!(USmallInt, nulls, values),
            Self::UInt(nulls, values) => get_primitive!(UInt, nulls, values),
            Self::UBigInt(nulls, values) => get_primitive!(UBigInt, nulls, values),
            Self::Float(nulls, values) => get_primitive!(Float, nulls, values),
            Self::Double(nulls, values) => get_primitive!(Double, nulls, values),
            Self::Timestamp(nulls, _values) => {
                if nulls.get_unchecked(index) {
                    Value::Null
                } else {
                    // BorrowedValue::Timestamp(TimestampValue::new(*unsafe { values.get_unchecked(index) }, self.precision()))
                    todo!()
                }
            }
            Self::Binary(values) => match unsafe { values.get_unchecked(index) } {
                Some(bytes) => {
                    Value::VarChar(unsafe { std::str::from_utf8_unchecked(bytes).to_string() })
                }
                None => Value::Null,
            },
            Self::NChar(values) => match unsafe { values.get_unchecked(index).as_ref() } {
                Some(value) => Value::NChar(value.to_string()),
                None => Value::Null,
            },
            _ => unreachable!(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Null(n) => *n,
            Self::Bool(_nulls, values) => values.len(),
            Self::TinyInt(_nulls, values) => values.len(),
            Self::SmallInt(_nulls, values) => values.len(),
            Self::Int(_nulls, values) => values.len(),
            Self::BigInt(_nulls, values) => values.len(),
            Self::UTinyInt(_nulls, values) => values.len(),
            Self::USmallInt(_nulls, values) => values.len(),
            Self::UInt(_nulls, values) => values.len(),
            Self::UBigInt(_nulls, values) => values.len(),
            Self::Float(_nulls, values) => values.len(),
            Self::Double(_nulls, values) => values.len(),
            Self::Timestamp(_nulls, values) => values.len(),
            Self::Binary(values) => values.len(),
            Self::NChar(values) => values.len(),
            _ => unreachable!(),
        }
    }

    pub fn to_json_array(&self) -> Vec<serde_json::Value> {
        (0..self.len())
            .map(|value| self.get(value).to_json_value())
            .collect()
    }
}

#[test]
fn test_serde() {
    const N: usize = 100;
    let nulls = BitVec::zeros(N);
    let v: Vec<i32> = (0..N).map(|_| rand::random()).collect();
    let ints = BorrowedColumn::Int(nulls, &v);

    let json = serde_json::to_string(&ints).unwrap();

    let ints2: Column = serde_json::from_str(&json).unwrap();

    println!("{ints:?}");
    println!("{ints2:?}");
}
