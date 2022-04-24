use core::num;
use std::{
    borrow::{Borrow, Cow},
    cell::{Cell, RefCell},
    ffi::c_void,
    marker::PhantomData,
    ops::Deref,
    rc::Rc,
    slice,
};

use itertools::Itertools;
use once_cell::unsync::OnceCell;
use serde::{forward_to_deserialize_any, Deserialize};
use thiserror::Error;

use taos_query::{common::*, *};
use taos_sys::RawRes;

use crate::{util::IntoCStr, Taos};

type Fields<'f> = Cow<'f, [Field]>;

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
    raw: Rc<RawRes<'q>>,
    fields: Rc<Cow<'q, [Field]>>,
    precision: Precision,
    records: Vec<i32>,
}

/// 'q (query lifetime) must live longer than 'r (result set lifetime).
// #[derive(Debug, Clone, Copy)]
// struct SyncBlock<'r, 'q: 'r> {
//     res: &'r SyncResultSet<'q>,
//     data: *mut *mut c_void,
//     lengths: &'r [i32],
//     num_of_rows: usize,
// }
/// 'q (query lifetime) must live longer than 'r (result set lifetime).
#[derive(Debug)]
pub struct SyncBlock<'r, 'q: 'r> {
    raw: Rc<RawRes<'q>>,
    fields: Rc<Cow<'r, [Field]>>,
    precision: Precision,
    data: *mut *mut c_void,
    lengths: &'r [i32],
    num_of_rows: usize,
    _marker: PhantomData<&'q Field>,
}

// /// 'q (query lifetime) must live longer than 'r (result set lifetime).
// pub struct SyncResultSetIter<'r, 'q: 'r>(&'r SyncResultSet<'q>);

// pub struct SyncRsIterMute<'r, 'q: 'r>(&'r mut SyncResultSet<'q>);

// impl<'r, 'q: 'r, 'f> Iterator for SyncRsIterMute<'r, 'q> {
//     type Item = SyncBlock<'r, 'q>;

//     fn next(&mut self) -> Option<Self::Item> {
//         let raw = &self.0.raw;
//         let block = raw.fetch_block();
//         if let Ok(Some((data, num_of_rows))) = block {
//             dbg!(num_of_rows);
//             let fields = self.0.fields.clone();
//             // let fields = raw.fetch_fields().unwrap();
//             let lengths = raw.fetch_lengths();
//             Some(SyncBlock {
//                 raw: raw.clone(),
//                 precision: self.0.precision(),
//                 fields: fields,
//                 data,
//                 lengths,
//                 num_of_rows: num_of_rows as _,
//                 _marker: PhantomData,
//             })
//         } else {
//             None
//         }
//     }
// }

// impl<'r, 'q: 'r> IntoIterator for &'r mut SyncResultSet<'q> {
//     type Item = SyncBlock<'r, 'q>;

//     type IntoIter = SyncRsIterMute<'r, 'q>;

//     fn into_iter(self) -> Self::IntoIter {
//         SyncRsIterMute(self)
//     }
// }

// impl<'r, 'q> IntoIterator for &'r SyncResultSet<'q> {
//     type Item = SyncBlock<'r, 'q>;

//     type IntoIter = SyncResultSetIter<'r, 'q>;

//     fn into_iter(self) -> Self::IntoIter {
//         SyncResultSetIter(self)
//     }
// }

// impl<'q> SyncResultSet<'q>
// where
//     Self: 'q,
// {
//     fn _fields<'r>(&'r self) -> &'r [Field] {
//         self.fields.as_ref()
//     }
//     fn field_count(&self) -> usize {
//         self.fields.len()
//     }
//     // unsafe fn get_field_unchecked<'q>(&'q self, row: usize) -> &'q Field {
//     //     self.fields.get_unchecked(row)
//     // }
//     fn fetch_block<'r>(&'r self) -> Result<Option<SyncBlock<'r, 'q>>, Error> {
//         todo!()
//         // if let Some((data, num_of_rows)) = self.raw.fetch_block()? {
//         //     dbg!(num_of_rows);
//         //     let lengths = self.raw.fetch_lengths();
//         //     Ok(Some(SyncBlock {
//         //         res: self,
//         //         data,
//         //         lengths: unsafe { slice::from_raw_parts(lengths, self.field_count() as _) },
//         //         num_of_rows: num_of_rows as _,
//         //     }))
//         // } else {
//         //     Ok(None)
//         // }
//     }
//     fn fetch_block_mut<'r>(&'r mut self) -> Result<Option<SyncBlock<'r, 'q>>, Error> {
//         todo!()
//         // if let Some((data, num_of_rows)) = self.raw.fetch_block()? {
//         //     dbg!(num_of_rows);
//         //     let lengths = self.raw.fetch_lengths();
//         //     Ok(Some(SyncBlockMut {
//         //         fields: self.fields(),
//         //         data,
//         //         lengths: unsafe { slice::from_raw_parts(lengths, self.field_count() as _) },
//         //         num_of_rows: num_of_rows as _,
//         //     }))
//         // } else {
//         //     Ok(None)
//         // }
//     }

//     fn block_iter_mut<'r>(&'r mut self) -> SyncRsIterMute<'r, 'q> {
//         SyncRsIterMute(self)
//     }

//     // fn block_iter(&mut self) -> SyncResultSetIter<'_, 'q> {
//     //     SyncResultSetIter(self)
//     // }
// }

// impl<'r, 'q: 'r> Iterator for SyncResultSetIter<'r, 'q> {
//     type Item = SyncBlock<'r, 'q>;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.0.fetch_block().ok().unwrap_or_default()
//     }
// }

// impl Taos {
//     // define query lifetime: 'q.
//     fn blocking_query<'s, 'q>(
//         &'q self,
//         sql: impl IntoCStr<'s>,
//     ) -> Result<Result<SyncResultSet<'q>, usize>, Error> {
//         let raw = self.0.query(sql.into_c_str().as_ptr())?;
//         let fields = raw.fetch_fields();
//         let precision = raw.precision();
//         match fields {
//             Some(fields) => Ok(Ok(SyncResultSet {
//                 raw: Rc::new(raw),
//                 fields: Rc::new(fields),
//                 precision,
//                 records: Default::default(),
//             })),
//             None => Ok(Err(raw.affected_rows() as _)),
//         }
//     }
// }

// impl<'r, 'q> Deref for SyncBlock<'r, 'q> {
//     type Target = SyncResultSet<'r>;

//     fn deref(&self) -> &Self::Target {
//         self.raw
//     }
// }

// impl<'r, 'q> SyncBlock<'r, 'q> {
//     fn cell(&self, row: usize, col: usize) -> Option<(&'r Field, BorrowedValue<'r>)> {
//         if row < self.num_of_rows && col < self.fields.len() {
//             Some(unsafe { self.cell_unchecked(row, col) })
//         } else {
//             None
//         }
//     }

//     fn is_null(&self, row: usize, col: usize) -> bool {
//         self.raw.is_null(row as _, col as _)
//     }

//     unsafe fn _get_field_unchecked(&self, col: usize) -> &'r Field {
//         todo!()
//         // self.res.get_field_unchecked(col)
//     }
//     unsafe fn get_length_unchecked(&self, col: usize) -> usize {
//         *self.lengths.get_unchecked(col) as _
//     }
//     unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&'r Field, BorrowedValue<'r>) {
//         let inner = self.data.add(col);

//         let field = self._get_field_unchecked(col);
//         let is_null = self.is_null(row, col);
//         if is_null {
//             return (field, BorrowedValue::Null);
//         }

//         macro_rules! parse_cell {
//             ($f:ident, $t:ty) => {
//                 paste::paste! {
//                     BorrowedValue::$f({
//                         (*inner as *const $t).add(row).read()
//                     })
//                 }
//             };
//         }

//         let value = match field.ty() {
//             Ty::Null => BorrowedValue::Null,
//             Ty::Bool => parse_cell!(Bool, bool),
//             Ty::TinyInt => parse_cell!(TinyInt, i8),
//             Ty::SmallInt => parse_cell!(SmallInt, i16),
//             Ty::Int => parse_cell!(Int, i32),
//             Ty::BigInt => parse_cell!(BigInt, i64),
//             Ty::UTinyInt => parse_cell!(UTinyInt, u8),
//             Ty::USmallInt => parse_cell!(USmallInt, u16),
//             Ty::UInt => parse_cell!(UInt, u32),
//             Ty::UBigInt => parse_cell!(UBigInt, u64),
//             Ty::Float => parse_cell!(Float, f32),
//             Ty::Double => parse_cell!(Double, f64),
//             Ty::Timestamp => {
//                 let raw = (*inner as *const i64).add(row).read();
//                 // use: self.res.precision()
//                 let precision = Precision::Microsecond;
//                 BorrowedValue::Timestamp(Timestamp::new(raw, precision))
//             }
//             Ty::VarChar | Ty::NChar => {
//                 let length = self.get_length_unchecked(col);
//                 let ptr = (*inner as *const u8).add(row * length as usize);
//                 let len = ptr.cast::<i16>().read();
//                 let start = ptr.offset(2);

//                 BorrowedValue::VarChar(std::str::from_utf8_unchecked(slice::from_raw_parts(
//                     start, len as _,
//                 )))
//             }
//             Ty::Json => {
//                 let length = self.get_length_unchecked(col);
//                 let ptr = (*inner as *const u8).add(row * length as usize);
//                 let len = ptr.cast::<i16>().read();
//                 let start = ptr.offset(2);

//                 BorrowedValue::Json(slice::from_raw_parts(start, len as _))
//             }
//             _ => BorrowedValue::Null,
//         };
//         (field, value)
//     }

//     unsafe fn col_data(&self, col: usize) {}
// }

impl<'b, 'r, 'q> BlockExt2<'b> for SyncBlock<'r, 'q> {
    type Value = BorrowedValue<'b>;

    fn num_of_rows(&self) -> usize {
        self.num_of_rows
    }

    fn fields(&self) -> &[Field] {
        &self.fields
    }

    fn precision(&self) -> Precision {
        self.precision
    }

    unsafe fn cell_unchecked(&self, row: usize, col: usize) -> (&Field, Self::Value) {
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

                BorrowedValue::Json(slice::from_raw_parts(start, len as _))
            }
            _ => BorrowedValue::Null,
        };
        (field, value)
    }

    fn is_null(&self, row: usize, col: usize) -> bool {
        self.raw.is_null(row as _, col as _)
    }
}

// impl<'b, 'r, 'q: 'r> IntoIterator for &'b SyncBlock<'r, 'q> {
//     type Item = SyncRow<'b, 'r, 'q>;

//     type IntoIter = SyncBlockRowIter<'b, 'r, 'q>;

//     fn into_iter(self) -> Self::IntoIter {
//         SyncBlockRowIter {
//             block: &self,
//             row: 0,
//         }
//     }
// }

impl<'r, 'q: 'r> Rs2<'r> for SyncResultSet<'q> {
    fn fields(&self) -> &[Field] {
        &self.fields
    }

    fn precision(&self) -> Precision {
        Precision::Microsecond
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

    type Block = SyncBlock<'r, 'q>;

    fn fetch_block(&mut self) -> Option<Self::Block> {
        if let Ok(Some((data, num_of_rows))) = self.raw.fetch_block() {
            dbg!(num_of_rows);
            self.records.push(num_of_rows);
            let lengths = self.raw.fetch_lengths();
            Some(SyncBlock {
                raw: self.raw.clone(),
                fields: self.fields.clone(),
                precision: self.precision(),
                data,
                lengths,
                num_of_rows: num_of_rows as _,
                _marker: PhantomData,
            })
        } else {
            None
        }
    }
}

impl<'r, 'q: 'r> Queryable2<'r, 'q> for Taos {
    type Error = Error;

    type ResultSet = SyncResultSet<'q>;

    fn query<T: AsRef<str>>(
        &'q self,
        sql: T,
    ) -> Result<Result<Self::ResultSet, usize>, Self::Error> {
        let raw = self.0.query(sql.as_ref().into_c_str().as_ptr())?;
        let fields = raw.fetch_fields();
        let precision = raw.precision();
        match fields {
            Some(fields) => Ok(Ok(SyncResultSet {
                raw: Rc::new(raw),
                fields: Rc::new(fields),
                precision,
                records: Default::default(),
            })),
            None => Ok(Err(raw.affected_rows() as _)),
        }
    }
}

#[taos_macros::test(crate, log_level = "info")]
async fn sync_query_de(taos: &Taos, _database: &str) -> Result<(), Error> {
    let mut rs = <Taos as Queryable2>::query(taos, "select * from log.logs limit 10000")?.unwrap();
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct Record {
        ts: String,
        level: i8,
        content: String,
        dnode_id: i32,
        dnode_ep: String,
    }

    for record in rs.deserialize2() {
        let _: Record = record?;
    }
    let (blocks, records) = rs.summary();
    println!("total blocks: {}, total rows: {}", blocks, records);
    Ok(())
}

#[taos_macros::test(crate, log_level = "info")]
async fn sync_query_block_de_ref(taos: &Taos, _database: &str) -> Result<(), Error> {
    let mut rs = <Taos as Queryable2>::query(taos, "select * from log.logs limit 10000")?.unwrap();

    for block in rs.block_iter() {
        let des = block
            .deserialize::<(i64, i32, &str)>()
            .take(1)
            .collect_vec();
        println!("first row in block: {:?}", des);
    }

    let (blocks, records) = rs.summary();
    println!("total blocks: {}, total rows: {}", blocks, records);
    Ok(())
}
