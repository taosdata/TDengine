use std::{
    ffi::{c_void, CStr},
    marker::PhantomData,
    ops::Deref,
    os::raw::c_int,
    ptr,
    rc::Rc,
    slice,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

use futures::Stream;
use itertools::Itertools;

// use bitsvec::BitVec;
use bitvec_simd::BitVec;

use ::serde::{Deserialize, Serialize};
use taos_sys::*;

use crate::{timestamp::TimestampValue, Result, TaosError, TaosResult};

pub mod column;
pub use column::*;

pub mod row;
pub use row::*;

pub mod value;
use value::*;

struct WithFields<'a>(*mut TAOS_RES, &'a [TAOS_FIELD]);

pub struct BlockStream<'a> {
    result: &'a TaosResult<'a>,
    state: Arc<Mutex<BlockState>>,
}

impl<'a> BlockStream<'a> {
    pub fn new(result: &'a TaosResult<'a>) -> Self {
        let state = Arc::new(Mutex::new(BlockState {
            completed: false,
            result: std::ptr::null_mut(),
            num_of_rows: 0,
            waker: None,
        }));

        Self { result, state }
    }
}

unsafe impl<'a> Send for BlockStream<'a> {}

#[derive(Debug)]
pub struct Block<'a> {
    result: &'a TaosResult<'a>,
    inner: &'a [*mut c_void],
    lengths: &'a [i32],
    num_of_rows: i32,
}

struct BlockState {
    /// Whether or not the sleep time has elapsed
    completed: bool,
    result: *mut TAOS_RES,
    num_of_rows: i32,
    waker: Option<Waker>,
}

impl<'a> Deref for Block<'a> {
    type Target = TaosResult<'a>;

    fn deref(&self) -> &Self::Target {
        self.result
    }
}

impl<'a> Block<'a> {
    #[inline]
    fn new(result: &'a TaosResult<'a>, block: *mut TAOS_ROW, num_of_rows: i32) -> Self {
        let lengths = unsafe { taos_fetch_lengths(result.as_raw()) };
        let num_of_fields = result.num_fields();
        Self {
            result,
            inner: unsafe { std::slice::from_raw_parts(block.read(), num_of_fields) },
            lengths: unsafe { slice::from_raw_parts(lengths, num_of_fields) },
            num_of_rows,
        }
    }

    fn from_async_query(result: &'a TaosResult<'a>, num_of_rows: i32) -> Self {
        // let filed_count = unsafe { taos_num_fields(result) };
        let block = unsafe { taos_result_block(result.as_raw()) };
        Self::new(result, block, num_of_rows)
    }

    // for taos_fetch_block
    fn from_query(result: &'a TaosResult<'a>) -> Self {
        let block: *mut TAOS_ROW = Box::into_raw(Box::new(ptr::null_mut()));
        let num_of_rows = unsafe { taos_fetch_block(result.as_raw(), block) };
        Self::new(result, block, num_of_rows)
    }

    pub const fn num_of_fields(&self) -> usize {
        self.lengths.len()
    }

    pub const fn num_of_rows(&self) -> i32 {
        self.num_of_rows
    }

    pub fn rows_iter<'block>(&'block self) -> RowsIter<'block> {
        RowsIter {
            block: self,
            columns: self.columns_iter().collect(),
            precision: self.result.precision(),
            index: 0,
        }
    }

    pub fn columns_iter<'block>(&'block self) -> ColumnsIter<'block> {
        ColumnsIter {
            result: self.result.as_raw(),
            partial: self.inner,
            fields: unsafe { self.result.get_fields_unchecked() },
            lengths: self.lengths,
            precision: self.result.precision(),
            rows: self.num_of_rows as _,
            current: 0,
        }
    }

    pub fn into_iter_rows(self) -> impl Iterator<Item = Row<'a>> {
        let len = self.num_of_rows();
        std::iter::repeat(Rc::new(self))
            .enumerate()
            .map(|(index, block)| Row::new(block, index))
    }

    fn is_null(&self, row: usize, col: usize) -> bool {
        unsafe { taos_is_null(self.result.as_raw(), row as _, col as _) }
    }

    pub unsafe fn get_str(&'a self, row: usize, col: usize) -> Result<Option<&'a str>> {
        let field = self.result.get_field_unchecked(col);
        use TaosDataType::*;
        let ty = field.type_();
        if !matches!(field.type_(), NChar | Binary | Json) {
            return Err(TaosError::from_string(
                "unmatched data type pattern for string",
            ));
        }
        let slice = self.inner.get_unchecked(col);
        let is_null = self.is_null(row, col);
        if is_null {
            Ok(None)
        } else {
            match ty {
                TaosDataType::Binary | TaosDataType::NChar | TaosDataType::Json => {
                    let length = self.lengths.get_unchecked(col);

                    let ptr = (*slice as *const u8).offset(row as isize * *length as isize);
                    let len = ptr.cast::<i16>().read();
                    let start = ptr.offset(2);
                    let s = std::str::from_utf8(slice::from_raw_parts(start as _, len as _))
                        .map_err(|s| TaosError::from_string(s.to_string()))?;
                    Ok(Some(s))
                }
                _ => Ok(None),
            }
        }
    }

    fn inner(&self) -> &[*mut c_void] {
        self.inner
    }

    unsafe fn get_length_unchecked(&self, col: usize) -> i32 {
        *self.lengths.get_unchecked(col)
    }

    fn get_value<'block>(&self, row: usize, col: usize) -> Option<BorrowedValue<'block>> {
        if col < self.num_of_fields() {
            Some(unsafe { self.get_value_unchecked(row, col) })
        } else {
            None
        }
    }

    unsafe fn get_value_unchecked<'block>(&self, row: usize, col: usize) -> BorrowedValue<'block> {
        let inner = self.inner.get_unchecked(col);
        let field = unsafe { self.get_field_unchecked(col) };
        let is_null = unsafe { taos_is_null(self.as_raw(), row as _, col as _) };
        if is_null {
            return BorrowedValue::Null;
        }

        macro_rules! parse_cell {
            ($f:ident, $t:ty) => {
                paste::paste! {
                    BorrowedValue::$f(unsafe {
                        (*inner as *const $t).offset(row as _).read()
                    })
                }
            };
        }

        match field.type_() {
            TaosDataType::Null => BorrowedValue::Null,
            TaosDataType::Bool => parse_cell!(Bool, bool),
            TaosDataType::TinyInt => parse_cell!(TinyInt, i8),
            TaosDataType::SmallInt => parse_cell!(SmallInt, i16),
            TaosDataType::Int => parse_cell!(Int, i32),
            TaosDataType::BigInt => parse_cell!(BigInt, i64),
            TaosDataType::UTinyInt => parse_cell!(UTinyInt, u8),
            TaosDataType::USmallInt => parse_cell!(USmallInt, u16),
            TaosDataType::UInt => parse_cell!(UInt, u32),
            TaosDataType::UBigInt => parse_cell!(UBigInt, u64),
            TaosDataType::Float => parse_cell!(Float, f32),
            TaosDataType::Double => parse_cell!(Double, f64),
            TaosDataType::Timestamp => unsafe {
                let raw = (*inner as *const i64).offset(row as _).read();
                BorrowedValue::Timestamp(TimestampValue::new(raw, self.precision()))
            },
            TaosDataType::Binary => unsafe {
                let length = self.get_length_unchecked(col);
                let ptr = (*inner as *const u8).offset(row as isize * length as isize);
                let len = ptr.cast::<i16>().read();
                let start = ptr.offset(2);

                BorrowedValue::Binary(slice::from_raw_parts(start, len as _))
            },
            TaosDataType::NChar => unsafe {
                let length = self.get_length_unchecked(col);

                let ptr = (*inner as *const u8).offset(row as isize * length as isize);
                let len = ptr.cast::<i16>().read();
                let start = ptr.offset(2);

                BorrowedValue::NChar(std::str::from_utf8_unchecked(slice::from_raw_parts(
                    start as _, len as _,
                )))
            },
            _ => BorrowedValue::Null,
        }
    }
}

pub struct RowsIter<'block> {
    block: &'block Block<'block>,
    columns: Vec<BorrowedColumn<'block>>,
    precision: TimestampPrecision,
    index: usize,
}

impl<'a> Deref for RowsIter<'a> {
    type Target = Block<'a>;

    fn deref(&self) -> &Self::Target {
        self.block
    }
}

pub struct ColumnsIter<'block> {
    result: *mut TAOS_RES,
    partial: &'block [*mut c_void],
    fields: &'block [TAOS_FIELD],
    lengths: &'block [i32],
    rows: usize,
    precision: TimestampPrecision,
    current: usize,
}

impl<'block> RowsIter<'block> {
    const fn precision(&self) -> &TimestampPrecision {
        &self.precision
    }
}

impl<'block> ColumnsIter<'block> {
    fn num_of_fields(&self) -> usize {
        self.fields.len()
    }
    fn num_of_rows(&self) -> usize {
        self.rows
    }
    fn precision(&self) -> &TimestampPrecision {
        &self.precision
    }
}

impl<'block> Iterator for RowsIter<'block> {
    type Item = Vec<BorrowedValue<'block>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.num_of_rows() as _ {
            return None;
        }
        let num_of_fields = self.num_of_fields();

        {
            let mut row = Vec::with_capacity(num_of_fields);
            for column in &self.columns {
                todo!();
                // let slice = unsafe { inner.get_unchecked(col_idx) };
                // let field = unsafe { self.fields.get_unchecked(col_idx) };
                // let is_null = unsafe { taos_is_null(self.result, self.index as _, col_idx as _) };
                // if is_null {
                //     row.push(BorrowedValue::Null);
                //     continue;
                // }

                // macro_rules! parse_cell {
                //     ($f:ident, $t:ty) => {
                //         paste::paste! {
                //             BorrowedValue::$f(unsafe {
                //                 (*slice as *const $t).offset(self.row as _).read()
                //             })
                //         }
                //     };
                // }

                // let item = match field.type_() {
                //     TaosDataType::Null => BorrowedValue::Null,
                //     TaosDataType::Bool => parse_cell!(Bool, bool),
                //     TaosDataType::TinyInt => parse_cell!(TinyInt, i8),
                //     TaosDataType::SmallInt => parse_cell!(SmallInt, i16),
                //     TaosDataType::Int => parse_cell!(Int, i32),
                //     TaosDataType::BigInt => parse_cell!(BigInt, i64),
                //     TaosDataType::UTinyInt => parse_cell!(UTinyInt, u8),
                //     TaosDataType::USmallInt => parse_cell!(USmallInt, u16),
                //     TaosDataType::UInt => parse_cell!(UInt, u32),
                //     TaosDataType::UBigInt => parse_cell!(UBigInt, u64),
                //     TaosDataType::Float => parse_cell!(Float, f32),
                //     TaosDataType::Double => parse_cell!(Double, f64),
                //     TaosDataType::Timestamp => unsafe {
                //         let raw = (*slice as *const i64).offset(self.index as _).read();
                //         BorrowedValue::Timestamp(TimestampValue::new(raw, self.precision))
                //     },
                //     TaosDataType::Binary => unsafe {
                //         let length = self.lengths.get_unchecked(col_idx);

                //         let ptr =
                //             (*slice as *const u8).offset(self.index as isize * *length as isize);
                //         let len = ptr.cast::<i16>().read();
                //         let start = ptr.offset(2);

                //         BorrowedValue::Binary(slice::from_raw_parts(start, len as _))
                //     },
                //     TaosDataType::NChar => unsafe {
                //         let length = dbg!(self.lengths.get_unchecked(col_idx));

                //         let ptr =
                //             (*slice as *const u8).offset(self.index as isize * *length as isize);
                //         let len = ptr.cast::<i16>().read();
                //         let start = ptr.offset(2);

                //         BorrowedValue::NChar(std::str::from_utf8_unchecked(slice::from_raw_parts(
                //             start as _, len as _,
                //         )))
                //     },
                //     _ => BorrowedValue::Null,
                // };

                // row.push(item);
            }

            self.index += 1;
            Some(row)
        }
    }
}

impl<'block> ExactSizeIterator for RowsIter<'block> {
    fn len(&self) -> usize {
        self.num_of_rows() as _
    }
}

impl<'block> Iterator for ColumnsIter<'block> {
    type Item = BorrowedColumn<'block>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.num_of_fields() {
            return None;
        }
        let num_of_rows = self.num_of_rows();
        if let Some(slice) = self.partial.get(self.current) {
            let field = unsafe { self.fields.get_unchecked(self.current) };
            let mut is_nulls = BitVec::zeros(num_of_rows);
            (0..num_of_rows).for_each(|i| unsafe {
                if taos_is_null(self.result, i as _, self.current as _) {
                    is_nulls.set(i, true);
                }
            });

            macro_rules! column_transmute {
                ($f:ident, $t:ty) => {
                    paste::paste! {
                        BorrowedColumn::$f(is_nulls, unsafe {
                            std::slice::from_raw_parts(*slice as *const $t, num_of_rows)
                        })
                    }
                };
            }
            let item = match field.type_() {
                TaosDataType::Null => BorrowedColumn::Null(self.num_of_rows()),
                TaosDataType::Bool => column_transmute!(Bool, bool),
                TaosDataType::TinyInt => column_transmute!(TinyInt, i8),
                TaosDataType::SmallInt => column_transmute!(SmallInt, i16),
                TaosDataType::Int => column_transmute!(Int, i32),
                TaosDataType::BigInt => column_transmute!(BigInt, i64),
                TaosDataType::UTinyInt => column_transmute!(UTinyInt, u8),
                TaosDataType::USmallInt => column_transmute!(USmallInt, u16),
                TaosDataType::UInt => column_transmute!(UInt, u32),
                TaosDataType::UBigInt => column_transmute!(UBigInt, u64),
                TaosDataType::Float => column_transmute!(Float, f32),
                TaosDataType::Double => column_transmute!(Double, f64),
                TaosDataType::Timestamp => unsafe {
                    let raw = std::slice::from_raw_parts(*slice as *const i64, num_of_rows);
                    BorrowedColumn::Timestamp(is_nulls, raw)
                },
                TaosDataType::Binary => unsafe {
                    let length = self.lengths.get_unchecked(self.current);
                    let item = (0..num_of_rows)
                        .map(|n| {
                            let ptr = (*slice as *const u8).offset(n as isize * *length as isize);
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
                },
                TaosDataType::NChar => unsafe {
                    let length = self.lengths.get_unchecked(self.current);
                    let item = (0..num_of_rows)
                        .map(|n| {
                            let ptr = (*slice as *const u8).offset(n as isize * *length as isize);
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
                },
                _ => BorrowedColumn::Null(self.num_of_rows()),
            };
            self.current += 1;
            Some(item)
        } else {
            return None;
        }
    }
}

impl<'block> ExactSizeIterator for ColumnsIter<'block> {
    fn len(&self) -> usize {
        self.num_of_fields()
    }
}

impl<'a> Stream for BlockStream<'a> {
    // type Item = (*mut TAOS_RES, i32);
    type Item = Block<'a>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut s = self.state.lock().unwrap();
        unsafe extern "C" fn async_fetch_callback(
            param: *mut c_void,
            res: *mut TAOS_RES,
            num_of_rows: c_int,
        ) {
            let param = param as *const Arc<Mutex<BlockState>>;
            let state = param.read();
            let mut s = state.lock().unwrap();

            (*s).completed = true;
            (*s).result = res;
            (*s).num_of_rows = num_of_rows;
            if let Some(waker) = s.waker.take() {
                waker.wake()
            }
        }

        if s.completed && s.num_of_rows != 0 {
            let num_of_rows = s.num_of_rows;
            s.completed = false;
            s.num_of_rows = 0;
            Poll::Ready(Some(Self::Item::from_async_query(self.result, num_of_rows)))
        } else if s.completed && s.num_of_rows == 0 {
            // s.completed = false;
            Poll::Ready(None)
        } else {
            let res = if s.result.is_null() {
                self.result.as_raw()
            } else {
                s.result
            };
            s.waker = Some(cx.waker().clone());
            drop(s);
            unsafe {
                taos_fetch_rows_a(
                    res,
                    async_fetch_callback as _,
                    Box::into_raw(Box::new(self.state.clone())) as *mut _,
                );
            }
            Poll::Pending
        }
    }
}
