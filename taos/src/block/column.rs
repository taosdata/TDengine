use bitvec_simd::BitVec;
use itertools::Itertools;

use crate::stmt::MultiBind;

use super::value::BorrowedValue;

#[derive(Debug, serde::Serialize)]
pub enum BorrowedColumn<'block> {
    Null(usize),
    Bool(BitVec, &'block [bool]),  // 1
    TinyInt(BitVec, &'block [i8]), // 2
    SmallInt(BitVec, &'block [i16]),
    Int(BitVec, &'block [i32]),
    BigInt(BitVec, &'block [i64]),
    Float(BitVec, &'block [f32]),
    Double(BitVec, &'block [f64]),
    Binary(Vec<Option<&'block [u8]>>),
    Timestamp(BitVec, &'block [i64]),
    NChar(Vec<Option<&'block str>>),
    UTinyInt(BitVec, &'block [u8]),
    USmallInt(BitVec, &'block [u16]),
    UInt(BitVec, &'block [u32]),
    UBigInt(BitVec, &'block [u64]), // 14
    Json(BitVec, &'block [u8]),
    VarChar(BitVec, Vec<&'block [u8]>),
    VarBinary(BitVec, Vec<&'block [u8]>),
    Decimal(BitVec, &'block [f64]),
    Blob(BitVec, Vec<&'block [u8]>),
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

impl Column {
    pub fn to_multi_bind(&self) -> MultiBind {
        match self {
            Self::Null(n) => MultiBind::nulls(*n),
            Self::Bool(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::TinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::SmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::Int(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::BigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::UTinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::USmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::UInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::UBigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::Float(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::Double(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::Timestamp(nulls, values) => MultiBind::from_raw_timestamps(nulls, values),
            Self::Binary(values) => MultiBind::from_binary_vec(values),
            Self::NChar(values) => MultiBind::from_string_vec(values),
            _ => unreachable!(),
        }
    }
}

impl<'block> BorrowedColumn<'block> {
    pub fn to_multi_bind(&self) -> MultiBind {
        match self {
            Self::Null(n) => MultiBind::nulls(*n),
            Self::Bool(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::TinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::SmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::Int(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::BigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::UTinyInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::USmallInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::UInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::UBigInt(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::Float(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::Double(nulls, values) => MultiBind::from_primitives(nulls, values),
            Self::Timestamp(nulls, values) => MultiBind::from_raw_timestamps(nulls, values),
            Self::Binary(values) => MultiBind::from_binary_vec(values),
            Self::NChar(values) => MultiBind::from_string_vec(values),
            _ => unreachable!(),
        }
    }

    pub fn get(&self, index: usize) -> BorrowedValue<'block> {
        macro_rules! get_primitive {
            ($target:ident, $nulls:expr, $values:expr) => {
                paste::paste! {
                    if unsafe { $nulls.get_unchecked(index) } {
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
            Self::Timestamp(nulls, values) => {
                if unsafe { nulls.get_unchecked(index) } {
                    BorrowedValue::Null
                } else {
                    // BorrowedValue::Timestamp(TimestampValue::new(*unsafe { values.get_unchecked(index) }, self.precision()))
                    todo!()
                }
            }
            Self::Binary(values) => match unsafe { values.get_unchecked(index) } {
                Some(bytes) => BorrowedValue::Binary(bytes),
                None => BorrowedValue::Null,
            },
            Self::NChar(values) => BorrowedValue::Null,
            _ => unreachable!(),
        }
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
