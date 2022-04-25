use std::{
    borrow::{Borrow, Cow},
    ffi::c_void,
    marker::PhantomData,
    rc::{Rc, Weak},
    slice,
};
use thiserror::Error;

use taos_query::{common::*, *};
use taos_sys::{DroppableRawRes, RawRes};

use crate::{util::IntoCStr, Taos};

#[derive(Debug, Error)]
pub enum Error {
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
pub struct SyncResultSet<'q> {
    raw: DroppableRawRes<'q>,
    // fields: Rc<Cow<'q, [Field]>>,
    precision: Precision,
    records: Vec<i32>,
}

#[derive(Debug)]
pub struct SyncBlock<'r> {
    raw: Rc<RawRes>,
    precision: Precision,
    data: *mut *mut c_void,
    lengths: &'r [i32],
    num_of_rows: usize,
}

impl<'b, 'r> BlockExt<'b> for SyncBlock<'r> {
    type Value = BorrowedValue<'b>;

    fn num_of_rows(&self) -> usize {
        self.num_of_rows
    }

    fn fields(&self) -> &[Field] {
        &self.raw.fields()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (*const Field, Self::Value) {
        let inner = self.data.add(col);

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
                // use: self.res.precision()
                let precision = Precision::Microsecond;
                BorrowedValue::Timestamp(Timestamp::new(raw, precision))
            }
            Ty::VarChar | Ty::NChar => {
                let length = *self.lengths.get_unchecked(col) as usize;
                let ptr = (*inner as *const u8).add(row * length as usize);
                let len = ptr.cast::<i16>().read();
                let start = ptr.offset(2);

                BorrowedValue::VarChar(std::str::from_utf8_unchecked(slice::from_raw_parts(
                    start, len as _,
                )))
            }
            Ty::Json => {
                let length = *self.lengths.get_unchecked(col) as usize;
                let ptr = (*inner as *const u8).add(row * length as usize);
                let len = ptr.cast::<i16>().read();
                let start = ptr.offset(2);

                BorrowedValue::Json(slice::from_raw_parts(start, len as _).into())
            }
            _ => BorrowedValue::Null,
        };
        (field as _, value)
    }

    fn is_null(&self, row: usize, col: usize) -> bool {
        self.raw.is_null(row as _, col as _)
    }
}

impl<'q> ResultSet for SyncResultSet<'q> {
    fn fields(&self) -> &[Field] {
        &self.raw.fields()
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    fn summary(&self) -> (usize, usize) {
        (
            self.records.len(),
            self.records.iter().fold(0, |mut acc, v| {
                acc += *v as usize;
                acc
            }),
        )
    }
}

impl<'r, 'q> Iterator for &'r mut SyncResultSet<'q> {
    type Item = SyncBlock<'r>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(Some((data, num_of_rows, lengths))) = self.raw.fetch_block() {
            dbg!(num_of_rows);
            self.records.push(num_of_rows);
            // let lengths = self.raw.fetch_lengths();
            // let raw = self.raw.raw();
            // let fields = self.fields.clone();
            let num_of_fields = self.num_of_fields();
            let lengths = unsafe { std::slice::from_raw_parts(lengths, num_of_fields) };

            // let lengths = self.raw.fetch_lengths();
            Some(SyncBlock {
                raw: self.raw.raw(),
                precision: self.precision(),
                data,
                lengths,
                num_of_rows: num_of_rows as _,
                // _marker: PhantomData,
            })
        } else {
            None
        }
    }
}

impl<'r, 'q> SyncBlock<'r> {
    #[inline]
    pub fn from_raw_with_ptr(
        raw: Rc<RawRes>,
        precision: Precision,
        data: *mut *mut c_void,
        num_of_rows: i32,
    ) -> Option<Self> {
        if num_of_rows > 0 {
            let lengths = raw.fetch_lengths();
            Some(SyncBlock {
                raw,
                data,
                precision: precision,
                lengths,
                num_of_rows: num_of_rows as _,
                // _marker: PhantomData,
            })
        } else {
            None
        }
    }
}

impl<'q> Queryable<'q> for Taos {
    type Error = Error;

    type ResultSet = SyncResultSet<'q>;

    fn query<T: AsRef<str>>(
        &'q self,
        sql: T,
    ) -> Result<Result<SyncResultSet<'q>, usize>, Self::Error> {
        let raw = self.0.query(sql.as_ref().into_c_str().as_ptr())?;
        let n = raw.num_fields();
        let precision = raw.precision();
        if n == 0 {
            Ok(Err(raw.affected_rows() as _))
        } else {
            Ok(Ok(SyncResultSet {
                raw,
                precision,
                records: Default::default(),
            }))
        }
    }
}

#[taos_macros::test(crate, log_level = "info")]
async fn sync_query_de(taos: &Taos, _database: &str) -> Result<(), Error> {
    let mut rs = <Taos as Queryable>::query(taos, "select * from log.logs limit 10000")?.unwrap();

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

#[taos_macros::test(crate, log_level = "info")]
async fn sync_query_block_de_ref(taos: &Taos, _database: &str) -> Result<(), Error> {
    use itertools::Itertools;
    let mut rs = <Taos as Queryable>::query(taos, "select * from log.logs limit 10000")?.unwrap();

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
