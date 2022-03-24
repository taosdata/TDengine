use std::{borrow::Borrow, ops::Mul};

use bitvec_simd::BitVec;
use taos_sys::{TaosDataType, TAOS_MULTI_BIND};

use crate::stmt::MultiBind;

#[derive(Debug, serde::Serialize)]
pub enum BorrowedBlock<'block> {
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
pub enum Block {
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

impl Block {
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

impl<'block> BorrowedBlock<'block> {
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
#[test]
fn test_serde() {
    const N: usize = 100;
    let nulls = BitVec::zeros(N);
    let v: Vec<i32> = (0..N).map(|_| rand::random()).collect();
    let ints = BorrowedBlock::Int(nulls, &v);

    let json = serde_json::to_string(&ints).unwrap();

    let ints2: Block = serde_json::from_str(&json).unwrap();

    println!("{ints:?}");
    println!("{ints2:?}");
}
