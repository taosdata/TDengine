use std::{
    ffi::c_void,
    marker::PhantomData,
    slice,
    sync::{atomic::AtomicU64, Arc},
};

use bitvec_simd::BitVec;
use itertools::Itertools;
use taos_error::Code;
use thiserror::Error;

use taos_query::{common::*, Address, BlockExt, Dsn, DsnError, Fetchable, FromDsn, Queryable};
use taos_sys::{DroppableRawRes, RawRes};

use crate::{util::IntoCStr, Taos};

#[derive(Debug, Error)]
pub enum Error {
    #[error("parse dsn error: {0}")]
    DsnError(#[from] DsnError),
    #[error("{0}")]
    Driver(#[from] taos_error::Error),
    #[error("deserialization error {0}")]
    Deserialize(#[from] serde::de::value::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

// A result should not be clone-able.
// Result set live shorter than query lifetime.
#[derive(Debug)]
pub struct ResultSet {
    raw: DroppableRawRes,
    summary: Arc<(AtomicU64, AtomicU64)>,
}

impl ResultSet {
    pub(crate) fn from_ptr(ptr: *mut c_void) -> Result<Self, taos_error::Error> {
        let raw = RawRes::from_ptr(ptr).map(DroppableRawRes::new)?;
        Ok(ResultSet {
            raw,
            summary: Default::default(),
        })
    }
    pub(crate) fn from_ptr_with_code(
        ptr: *mut c_void,
        code: impl Into<Code>,
    ) -> Result<Self, taos_error::Error> {
        let raw = RawRes::from_ptr_with_code(ptr, code.into()).map(DroppableRawRes::new)?;
        Ok(ResultSet {
            raw,
            summary: Default::default(),
        })
    }

    pub(crate) fn new(raw: DroppableRawRes) -> Self {
        Self {
            raw,
            summary: Default::default(),
        }
    }

    pub(crate) fn append_num_of_rows(&self, num_of_rows: i32) {
        use std::sync::atomic::Ordering::SeqCst;
        self.summary.0.fetch_add(1, SeqCst);
        self.summary.1.fetch_add(num_of_rows as _, SeqCst);
    }
}

#[derive(Debug)]
pub struct SyncBlock {
    pub raw: Arc<RawRes>,
    pub precision: Precision,
    pub data: *mut *mut c_void,
    pub lengths: *const i32,
    pub num_of_rows: usize,
}

unsafe impl Send for SyncBlock {}
unsafe impl Sync for SyncBlock {}

impl SyncBlock {
    pub fn tmq_table_name(&self) -> Option<&str> {
        self.raw.tmq_table_name()
    }
}

impl<'b> BlockExt for SyncBlock {
    // type Value = BorrowedValue<'b>;

    fn num_of_rows(&self) -> usize {
        self.num_of_rows
    }

    fn fields(&self) -> &[Field] {
        &self.raw.fields()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    fn is_null(&self, row: usize, col: usize) -> bool {
        self.raw.is_null(row as _, col as _)
    }

    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, BorrowedValue) {
        let inner = self.data.add(col);
        // log::debug!("inner: {inner:?} at ({row}, {col})");

        let field = self.get_field_unchecked(col);
        let is_null = self.is_null(row, col);
        if is_null {
            return (field, BorrowedValue::Null);
        }

        macro_rules! parse_cell {
            ($f:ident, $t:ty) => {
                paste::paste! {
                    BorrowedValue::$f({
                        (*inner as *const $t).add(row).read()
                    })
                }
            };
        }

        let read_bytes_from_ptr = |inner: *mut *mut c_void, col: usize| {
            if crate::client_info().starts_with("3") {
                let offsets = self.raw.get_column_data_offset(col);
                let offset = offsets.add(row).read();
                if offset == -1 {
                    "".as_bytes()
                } else {
                    let ptr = (*inner as *const u8).add(offset as usize);
                    let len = ptr.cast::<i16>().read();
                    let start = ptr.offset(2);

                    slice::from_raw_parts(start, len as _)
                }
            } else {
                let length = *self.lengths.add(col) as usize;
                let ptr = (*inner as *const u8).add(row * length as usize);
                let len = ptr.cast::<i16>().read();
                let start = ptr.offset(2);

                slice::from_raw_parts(start, len as _)
            }
        };

        let value = match field.ty() {
            Ty::Null => BorrowedValue::Null,
            Ty::Bool => parse_cell!(Bool, bool),
            Ty::TinyInt => parse_cell!(TinyInt, i8),
            Ty::SmallInt => parse_cell!(SmallInt, i16),
            Ty::Int => parse_cell!(Int, i32),
            Ty::BigInt => parse_cell!(BigInt, i64),
            Ty::UTinyInt => parse_cell!(UTinyInt, u8),
            Ty::USmallInt => parse_cell!(USmallInt, u16),
            Ty::UInt => parse_cell!(UInt, u32),
            Ty::UBigInt => parse_cell!(UBigInt, u64),
            Ty::Float => parse_cell!(Float, f32),
            Ty::Double => parse_cell!(Double, f64),
            Ty::Timestamp => {
                let raw = (*inner as *const i64).add(row).read();
                let precision = self.precision();
                BorrowedValue::Timestamp(Timestamp::new(raw, precision))
            }
            Ty::VarChar => BorrowedValue::VarChar(std::str::from_utf8_unchecked(
                read_bytes_from_ptr(inner, col),
            )),
            Ty::NChar => BorrowedValue::NChar(std::str::from_utf8_unchecked(read_bytes_from_ptr(
                inner, col,
            ))),
            Ty::Json => BorrowedValue::Json(read_bytes_from_ptr(inner, col).into()),
            _ => BorrowedValue::Null,
        };
        (field as _, value)
    }

    unsafe fn get_col_unchecked(&self, col: usize) -> BorrowedColumn {
        let inner = self.data.add(col);
        let field = self.get_field_unchecked(col);
        let num_of_rows = self.num_of_rows() as usize;
        let is_nulls =
            BitVec::from_bool_iterator((0..num_of_rows as usize).map(|row| self.is_null(row, col)));

        macro_rules! column_transmute {
            ($f:ident, $t:ty) => {
                paste::paste! {
                    BorrowedColumn::$f(is_nulls, {
                        std::slice::from_raw_parts(*inner as *const $t, num_of_rows)
                    })
                }
            };
        }
        match field.ty() {
            Ty::Null => BorrowedColumn::Null(num_of_rows),
            Ty::Bool => column_transmute!(Bool, bool),
            Ty::TinyInt => column_transmute!(TinyInt, i8),
            Ty::SmallInt => column_transmute!(SmallInt, i16),
            Ty::Int => column_transmute!(Int, i32),
            Ty::BigInt => column_transmute!(BigInt, i64),
            Ty::UTinyInt => column_transmute!(UTinyInt, u8),
            Ty::USmallInt => column_transmute!(USmallInt, u16),
            Ty::UInt => column_transmute!(UInt, u32),
            Ty::UBigInt => column_transmute!(UBigInt, u64),
            Ty::Float => column_transmute!(Float, f32),
            Ty::Double => column_transmute!(Double, f64),
            Ty::Timestamp => {
                let raw = std::slice::from_raw_parts(*inner as *const i64, num_of_rows);
                BorrowedColumn::Timestamp(is_nulls, raw)
            }
            Ty::VarChar => {
                let length = self.lengths.add(col);
                let item = (0..num_of_rows)
                    .map(|n| {
                        let ptr = (*inner as *const u8).offset(n as isize * *length as isize);
                        let len = ptr.cast::<i16>().read();
                        let start = ptr.offset(2);
                        if is_nulls.get_unchecked(n) {
                            None
                        } else {
                            Some(slice::from_raw_parts(start, len as _))
                        }
                    })
                    .collect_vec();

                BorrowedColumn::Binary(item)
            }
            Ty::NChar => {
                let length = self.lengths.add(col);
                let item = (0..num_of_rows)
                    .map(|n| {
                        let ptr = (*inner as *const u8).offset(n as isize * *length as isize);
                        let len = ptr.cast::<i16>().read();
                        let start = ptr.offset(2);
                        if is_nulls.get_unchecked(n) {
                            None
                        } else {
                            Some(std::str::from_utf8_unchecked(slice::from_raw_parts(
                                start as _, len as _,
                            )))
                        }
                    })
                    .collect_vec();

                BorrowedColumn::NChar(item)
            }
            _ => unreachable!("unsupported borrowed column type"),
        }
    }
}

impl Fetchable for ResultSet {
    fn fields(&self) -> &[Field] {
        &self.raw.fields()
    }

    fn precision(&self) -> Precision {
        self.raw.precision()
    }

    fn summary(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::SeqCst;
        (
            self.summary.0.load(SeqCst) as _,
            self.summary.1.load(SeqCst) as _,
        )
    }

    fn affected_rows(&self) -> i32 {
        self.raw.affected_rows()
    }
}

impl Iterator for ResultSet {
    type Item = SyncBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(Some((data, num_of_rows, lengths))) = self.raw.fetch_block() {
            log::info!("fetch block: {num_of_rows}");
            self.append_num_of_rows(num_of_rows);

            Some(SyncBlock {
                raw: self.raw.raw(),
                precision: self.precision(),
                data,
                lengths,
                num_of_rows: num_of_rows as _,
            })
        } else {
            None
        }
    }
}

impl<'r, 'q> SyncBlock {
    #[inline]
    pub fn from_raw_with_ptr(
        raw: Arc<RawRes>,
        data: *mut *mut c_void,
        num_of_rows: i32,
    ) -> Option<Self> {
        let precision = raw.precision();
        if num_of_rows > 0 {
            let lengths = raw.fetch_lengths();
            Some(SyncBlock {
                raw,
                data,
                precision: precision,
                lengths,
                num_of_rows: num_of_rows as _,
            })
        } else {
            None
        }
    }

    #[inline]
    pub fn from_async_query(
        raw: Arc<RawRes>,
        data: *mut *mut c_void,
        num_of_rows: i32,
    ) -> Option<Self> {
        Self::from_raw_with_ptr(raw, data, num_of_rows)
    }
}

impl<'q> Queryable<'q> for Taos {
    type Error = Error;

    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> Result<ResultSet, Self::Error> {
        log::debug!("sql: {}", sql.as_ref());
        let raw = self.0.query(sql.as_ref().into_c_str().as_ptr())?;
        Ok(ResultSet::new(raw))
    }
}

impl FromDsn for Taos {
    type Err = Error;

    fn hygienize(dsn: Dsn) -> Result<(Dsn, Vec<Address>), DsnError> {
        if dsn.driver != "taos" {
            return Err(DsnError::InvalidDriver(dsn.driver));
        }
        Ok((dsn, vec![]))
    }

    fn from_dsn(dsn: &Dsn) -> Result<Self, Self::Err> {
        if dsn.addresses.len() == 0 {
            Ok(Taos::new(
                (),
                &dsn.username,
                &dsn.password,
                &dsn.database,
                0,
            )?)
        } else {
            let Address { host, port, .. } = &dsn.addresses[0];
            Ok(Taos::new(
                host,
                &dsn.username,
                &dsn.password,
                &dsn.database,
                port.unwrap_or_default(),
            )?)
        }
    }

    fn ping(_dsn: &Dsn) -> Result<(), Self::Err> {
        Ok(())
    }
}

#[taos_macros::test(log_level = "debug")]
fn show_databases(taos: &Taos) -> Result<(), Error> {
    let mut rs = <Taos as Queryable>::query(taos, "show databases")?;

    log::debug!("{rs:?}");

    let fields = rs.fields();

    log::debug!("fields[{}]: {fields:?}", fields.len());

    let precision = rs.precision();
    log::debug!("precision: {}", precision);

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        name: String,
        ntables: u64,
    }

    for record in rs.deserialize() {
        let record: Record = record?;
        println!("{record:?}");
    }

    Ok(())
}

#[taos_macros::test(crate, log_level = "trace")]
fn sync_query_on_non_queryable_sql(taos: &Taos, database: &str) -> Result<(), Error> {
    let mut rs = <Taos as Queryable>::query(taos, format!("use {database}"))?;

    assert!(rs.precision() == Precision::Millisecond); // `ms` is the default precision.
    assert!(rs.fields().len() == 0);
    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        ts: String,
        level: i8,
        content: String,
        dnode_id: i32,
        dnode_ep: String,
    }

    for record in rs.deserialize() {
        let _: Record = record?;
    }

    // Queried 0 rows.
    assert_eq!(rs.summary(), (0, 0));
    Ok(())
}

#[taos_macros::test(crate, log_level = "info")]
fn sync_query_de(taos: &Taos, _database: &str) -> Result<(), Error> {
    let affected_rows = <Taos as Queryable>::exec(taos, "use log")?;
    assert!(affected_rows == 0);
    let mut rs = <Taos as Queryable>::query(taos, "select * from log.logs limit 10000")?;

    assert!(rs.fields().len() == 5);
    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Record {
        ts: String,
        level: i8,
        content: String,
        dnode_id: i32,
        dnode_ep: String,
    }

    for record in rs.deserialize() {
        let _: Record = record?;
    }
    let (blocks, records) = rs.summary();
    println!("total blocks: {}, total rows: {}", blocks, records);
    Ok(())
}

#[taos_macros::test(crate)]
fn sync_query_block_de_ref(taos: &Taos, _database: &str) -> Result<(), Error> {
    use itertools::Itertools;
    let mut rs = <Taos as Queryable>::query(taos, "select * from log.logs limit 10000")?;

    for block in &mut rs {
        let des = block
            .deserialize::<(i64, i32, &str)>()
            .take(1)
            .collect_vec();
        log::info!("first row in block: {:?}", des);
    }

    let (blocks, records) = rs.summary();
    println!("total blocks: {}, total rows: {}", blocks, records);
    Ok(())
}

pub(crate) mod asyncs;
