use bitvec_simd::BitVec;
use chrono_tz::Tz;
use clap::Args;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use taos::prelude::*;
use taos::{block::*, helpers::ColumnMeta};

use url::Url;

#[derive(Debug, Args)]
pub struct TaosUri {
    pub uri: Url,
}
#[derive(Debug, Args)]
pub struct TaosOpts {
    /// TDengine host
    #[clap(short, long, env = "TAOS_HOST", group = "taos-opts")]
    pub host: Option<String>,
    /// TDengine port
    #[clap(short, long, env = "TAOS_PORT", group = "taos-opts")]
    pub port: Option<u16>,
    /// TDengine username
    #[clap(short, long, env = "TAOS_USERNAME", group = "taos-opts")]
    pub username: Option<String>,
    /// TDengine password for the user
    #[clap(short = 'P', long, env = "TAOS_PASSWORD", group = "taos-opts")]
    pub password: Option<String>,
    /// Choose database for the connection
    #[clap(short, long, env = "TAOS_DATABASE", group = "taos-opts")]
    pub database: Option<String>,
    #[clap(short, long, env = "TZ")]
    /// Timezone, example: Asia/Shanghai
    pub timezone: Option<Tz>,
    #[clap(short, long, env = "TAOS_CFG_DIR")]
    /// TDengine config directory
    pub cfg_dir: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub replica: i16,
    pub quorum: i16,
    pub days: i16,
    pub keep: Option<String>,
    // pub cache: i32,
    pub blocks: i32,
    pub minrows: i32,
    pub maxrows: i32,
    pub wallevel: i8,
    pub fsync: i32,
    pub comp: i8,
    pub cachelast: i8,
    pub precision: String,
    pub update: i8,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TaosDescribe {
    pub name: String,
    pub describe: Vec<TaosColumnMeta>,
}

pub type TaosColumnMeta = ColumnMeta;
impl TaosDescribe {
    pub fn new(name: String, describe: Vec<ColumnMeta>) -> Self {
        Self { name, describe }
    }
}

// #[derive(Debug, Deserialize, Serialize, Clone)]
// pub enum TaosColumnMeta {
//     Column(TaosDescribed),
//     Tag(TaosDescribed),
// }

// #[derive(Debug, Deserialize, Serialize, Clone)]
// pub struct TaosDescribed {
//     pub field: String,
//     pub r#type: TaosDataType,
//     pub length: usize,
// }

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TaosTag {
    pub name: String,
    pub tags: Vec<Vec<Value>>,
}

impl TaosTag {
    pub fn new<'b, T: BlockExt>(name: String, iter: taos::query::RowsIter<'b, T>) -> Self {
        let mut values = vec![];
        for row in iter {
            let mut tmp = vec![];
            for bv in row {
                tmp.push(bv.1.into_value());
            }
            values.push(tmp);
        }
        Self { name, tags: values }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TaosBlock {
    pub name: String,
    pub data: Vec<TaosColumn>,
}

impl TaosBlock {
    pub fn new<'b>(name: String, iter: impl Iterator<Item = BorrowedColumn<'b>>) -> Self {
        let mut data = vec![];
        for col in iter {
            data.push(TaosColumn::new(col.into_owned()));
        }
        Self { name, data }
    }

    pub fn to_column_vec(&self) -> Vec<Column> {
        self.data.to_vec().iter().map(|v| v.to_column()).collect()
    }

    // pub fn to_multi_bind(&self) -> Vec<MultiBind> {
    //     self.data
    //         .to_vec()
    //         .iter()
    //         .map(|v| v.to_column().to_multi_bind())
    //         .collect()
    // }
}

#[derive(Debug, Deserialize, Serialize, Clone)]

pub enum TaosColumn {
    Null(usize),
    Bool(Vec<bool>, Vec<bool>),  // 1
    TinyInt(Vec<bool>, Vec<i8>), // 2
    SmallInt(Vec<bool>, Vec<i16>),
    Int(Vec<bool>, Vec<i32>),
    BigInt(Vec<bool>, Vec<i64>),
    Float(Vec<bool>, Vec<f32>),
    Double(Vec<bool>, Vec<f64>),
    Binary(Vec<Option<Vec<u8>>>),
    Timestamp(Vec<bool>, Vec<i64>),
    NChar(Vec<Option<String>>),
    UTinyInt(Vec<bool>, Vec<u8>),
    USmallInt(Vec<bool>, Vec<u16>),
    UInt(Vec<bool>, Vec<u32>),
    UBigInt(Vec<bool>, Vec<u64>), // 14
    Json(Vec<bool>, Vec<u8>),
    VarChar(Vec<bool>, Vec<Vec<u8>>),
    VarBinary(Vec<bool>, Vec<Vec<u8>>),
    Decimal(Vec<bool>, Vec<f64>),
    Blob(Vec<bool>, Vec<Vec<u8>>),
}

impl TaosColumn {
    pub fn new(column: Column) -> Self {
        match column {
            Column::Null(v) => Self::Null(v),
            Column::Bool(is_nulls, v) => Self::Bool(is_nulls.into_bools(), v),
            Column::TinyInt(is_nulls, v) => Self::TinyInt(is_nulls.into_bools(), v),
            Column::SmallInt(is_nulls, v) => Self::SmallInt(is_nulls.into_bools(), v),
            Column::Int(is_nulls, v) => Self::Int(is_nulls.into_bools(), v),
            Column::BigInt(is_nulls, v) => Self::BigInt(is_nulls.into_bools(), v),
            Column::Float(is_nulls, v) => Self::Float(is_nulls.into_bools(), v),
            Column::Double(is_nulls, v) => Self::Double(is_nulls.into_bools(), v),
            Column::Binary(v) => Self::Binary(v),
            Column::Timestamp(is_nulls, v) => Self::Timestamp(is_nulls.into_bools(), v),
            Column::NChar(v) => Self::NChar(v),
            Column::UTinyInt(is_nulls, v) => Self::UTinyInt(is_nulls.into_bools(), v),
            Column::USmallInt(is_nulls, v) => Self::USmallInt(is_nulls.into_bools(), v),
            Column::UInt(is_nulls, v) => Self::UInt(is_nulls.into_bools(), v),
            Column::UBigInt(is_nulls, v) => Self::UBigInt(is_nulls.into_bools(), v),
            Column::Json(is_nulls, v) => Self::Json(is_nulls.into_bools(), v),
            Column::VarChar(is_nulls, v) => Self::VarChar(is_nulls.into_bools(), v),
            Column::VarBinary(is_nulls, v) => Self::VarBinary(is_nulls.into_bools(), v),
            Column::Decimal(is_nulls, v) => Self::Decimal(is_nulls.into_bools(), v),
            Column::Blob(is_nulls, v) => Self::Blob(is_nulls.into_bools(), v),
        }
    }

    pub fn to_multi_bind(&self) -> Column {
        match self {
            TaosColumn::Null(v) => Column::Null(v.to_owned()),
            TaosColumn::Bool(is_nulls, v) => Column::Bool(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::TinyInt(is_nulls, v) => Column::TinyInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::SmallInt(is_nulls, v) => Column::SmallInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Int(is_nulls, v) => Column::Int(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::BigInt(is_nulls, v) => Column::BigInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Float(is_nulls, v) => Column::Float(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Double(is_nulls, v) => Column::Double(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Binary(v) => Column::Binary(v.to_owned()),
            TaosColumn::Timestamp(is_nulls, v) => Column::Timestamp(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::NChar(v) => Column::NChar(v.to_owned()),
            TaosColumn::UTinyInt(is_nulls, v) => Column::UTinyInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::USmallInt(is_nulls, v) => Column::USmallInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::UInt(is_nulls, v) => Column::UInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::UBigInt(is_nulls, v) => Column::UBigInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Json(is_nulls, v) => Column::Json(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::VarChar(is_nulls, v) => Column::VarChar(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::VarBinary(is_nulls, v) => Column::VarBinary(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Decimal(is_nulls, v) => Column::Decimal(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Blob(is_nulls, v) => Column::Blob(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
        }
    }

    pub fn to_column(&self) -> Column {
        match self {
            TaosColumn::Null(v) => Column::Null(v.to_owned()),
            TaosColumn::Bool(is_nulls, v) => Column::Bool(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::TinyInt(is_nulls, v) => Column::TinyInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::SmallInt(is_nulls, v) => Column::SmallInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Int(is_nulls, v) => Column::Int(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::BigInt(is_nulls, v) => Column::BigInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Float(is_nulls, v) => Column::Float(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Double(is_nulls, v) => Column::Double(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Binary(v) => Column::Binary(v.to_owned()),
            TaosColumn::Timestamp(is_nulls, v) => Column::Timestamp(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::NChar(v) => Column::NChar(v.to_owned()),
            TaosColumn::UTinyInt(is_nulls, v) => Column::UTinyInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::USmallInt(is_nulls, v) => Column::USmallInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::UInt(is_nulls, v) => Column::UInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::UBigInt(is_nulls, v) => Column::UBigInt(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Json(is_nulls, v) => Column::Json(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::VarChar(is_nulls, v) => Column::VarChar(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::VarBinary(is_nulls, v) => Column::VarBinary(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Decimal(is_nulls, v) => Column::Decimal(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
            TaosColumn::Blob(is_nulls, v) => Column::Blob(
                BitVec::from_bool_iterator(is_nulls.iter().copied()),
                v.to_owned(),
            ),
        }
    }
}
