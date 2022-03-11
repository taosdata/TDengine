use std::{
    ffi::c_void,
    marker::PhantomData,
    mem::ManuallyDrop,
    os::raw::c_int,
    slice,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

use futures::Stream;
use itertools::Itertools;

use bitsvec::BitVec;

use taos_sys::*;

use crate::{timestamp::TimestampValue, TaosResult};

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
pub struct PartialRows<'a> {
    result: &'a TaosResult<'a>,
    block: *mut TAOS_ROW,
    lengths: &'a [i32],
    num_of_rows: i32,
    _marker: PhantomData<&'a BlockStream<'a>>,
}

struct BlockState {
    /// Whether or not the sleep time has elapsed
    completed: bool,
    result: *mut TAOS_RES,
    num_of_rows: i32,
    waker: Option<Waker>,
}

impl<'a> PartialRows<'a> {
    fn new(result: &'a TaosResult<'a>, num_of_rows: i32) -> Self {
        // let filed_count = unsafe { taos_num_fields(result) };
        let block = unsafe { taos_result_block(result.as_raw()) };
        let lengths = unsafe { taos_fetch_lengths(result.as_raw()) };
        Self {
            result,
            block,
            lengths: unsafe { slice::from_raw_parts(lengths, result.num_fields()) },
            num_of_rows,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn num_of_fields(&self) -> usize {
        self.lengths.len()
    }

    #[inline]
    pub fn num_of_rows(&self) -> i32 {
        self.num_of_rows
    }

    fn as_slice(&self) -> Vec<&[u8]> {
        let num_of_rows = self.num_of_rows();
        let num_of_fields = self.num_of_fields();
        let lengths = self.lengths;
        unsafe {
            let block = std::slice::from_raw_parts(self.block.read(), num_of_fields);
            block
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    std::slice::from_raw_parts(*col as *mut u8, (lengths[i] * num_of_rows) as usize)
                })
                .collect()
        }
    }

    pub fn to_owned(&self) -> Vec<Vec<u8>> {
        self.as_slice().into_iter().map(|v| v.to_owned()).collect()
    }

    pub fn rows_iter<'block>(&'block self) -> RowsIter<'block> {
        RowsIter {
            result: self.result.as_raw(),
            partial: unsafe { std::slice::from_raw_parts(self.block.read(), self.num_of_fields()) },
            fields: unsafe { self.result.get_fields_unchecked() },
            lengths: self.lengths,
            precision: self.result.precision(),
            rows: self.num_of_rows as _,
            current: 0,
        }
    }

    pub fn columns_iter<'block>(&'block self) -> ColumnsIter<'block> {
        ColumnsIter {
            result: self.result.as_raw(),
            partial: unsafe { std::slice::from_raw_parts(self.block.read(), self.num_of_fields()) },
            fields: unsafe { self.result.get_fields_unchecked() },
            lengths: self.lengths,
            precision: self.result.precision(),
            rows: self.num_of_rows as _,
            current: 0,
        }
    }
}

pub struct RowsIter<'block> {
    result: *mut TAOS_RES,
    partial: &'block [*mut c_void],
    fields: &'block [TAOS_FIELD],
    lengths: &'block [i32],
    rows: usize,
    precision: TimestampPrecision,
    current: usize,
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
    NChar(ManuallyDrop<String>),
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
impl<'block> Iterator for RowsIter<'block> {
    type Item = Vec<BorrowedValue<'block>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.num_of_rows() as _ {
            return None;
        }
        let num_of_fields = self.num_of_fields();
        {
            let mut row = Vec::with_capacity(num_of_fields);
            for col_idx in 0..num_of_fields {
                let slice = unsafe { self.partial.get_unchecked(col_idx) };
                let field = unsafe { self.fields.get_unchecked(col_idx) };
                dbg!(field.type_());
                let is_null = unsafe { taos_is_null(self.result, self.current as _, col_idx as _) };
                if is_null {
                    row.push(BorrowedValue::Null);
                    continue;
                }

                macro_rules! parse_cell {
                    ($f:ident, $t:ty) => {
                        paste::paste! {
                            BorrowedValue::$f(unsafe {
                                (*slice as *const $t).offset(self.current as _).read()
                            })
                        }
                    };
                }

                let item = match field.type_() {
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
                        let raw = (*slice as *const i64).offset(self.current as _).read();
                        BorrowedValue::Timestamp(TimestampValue::new(raw, self.precision))
                    },
                    TaosDataType::Binary => unsafe {
                        let length = self.lengths.get_unchecked(col_idx);

                        let ptr =
                            (*slice as *const u8).offset(self.current as isize * *length as isize);
                        let len = ptr.cast::<i16>().read();
                        let start = ptr.offset(2);

                        BorrowedValue::Binary(slice::from_raw_parts(start, len as _))
                    },
                    TaosDataType::NChar => unsafe {
                        let length = dbg!(self.lengths.get_unchecked(col_idx));

                        let ptr =
                            (*slice as *const u8).offset(self.current as isize * *length as isize);
                        let len = ptr.cast::<i16>().read();
                        let start = ptr.offset(2);

                        BorrowedValue::NChar(ManuallyDrop::new(String::from_raw_parts(
                            start as _, len as _, len as _,
                        )))
                    },
                    _ => BorrowedValue::Null,
                };

                row.push(item);
            }

            self.current += 1;
            Some(row)
        }
    }
}

impl<'block> ExactSizeIterator for RowsIter<'block> {
    fn len(&self) -> usize {
        self.num_of_rows()
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
                    BorrowedColumn::Timestamp(is_nulls, raw, self.precision)
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
                                Some(ManuallyDrop::new(String::from_raw_parts(
                                    start as _, len as _, len as _,
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

#[derive(Debug)]
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
    Timestamp(BitVec, &'block [i64], TimestampPrecision),
    NChar(Vec<Option<ManuallyDrop<String>>>),
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

impl<'block> BorrowedColumn<'block> {
    pub fn into_owned(self) -> PartialColumn {
        match self {
            BorrowedColumn::Null(rows) => PartialColumn::Null(rows),
            BorrowedColumn::Bool(is_nulls, slice) => PartialColumn::Bool(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::TinyInt(is_nulls, slice) => PartialColumn::TinyInt(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::SmallInt(is_nulls, slice) => PartialColumn::SmallInt(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::Int(is_nulls, slice) => PartialColumn::Int(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::BigInt(is_nulls, slice) => PartialColumn::BigInt(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::UTinyInt(is_nulls, slice) => PartialColumn::UTinyInt(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::USmallInt(is_nulls, slice) => PartialColumn::USmallInt(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::UInt(is_nulls, slice) => PartialColumn::UInt(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::UBigInt(is_nulls, slice) => PartialColumn::UBigInt(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),

            BorrowedColumn::Float(is_nulls, slice) => PartialColumn::Float(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::Double(is_nulls, slice) => PartialColumn::Double(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.into_iter())
                    .map(|(is_null, value)| if is_null { None } else { Some(*value) })
                    .collect_vec(),
            ),
            BorrowedColumn::Binary(binary) => PartialColumn::Binary(
                binary
                    .into_iter()
                    .map(|val| val.map(ToOwned::to_owned))
                    .collect_vec(),
            ),
            BorrowedColumn::NChar(binary) => PartialColumn::NChar(
                binary
                    .into_iter()
                    .map(|val| val.map(|val| val.to_string()))
                    .collect_vec(),
            ),
            BorrowedColumn::Timestamp(is_nulls, slice, precision) => PartialColumn::Timestamp(
                is_nulls
                    .into_bools()
                    .into_iter()
                    .zip(slice.iter())
                    .map(|(is_null, value)| {
                        if is_null {
                            None
                        } else {
                            Some(TimestampValue::new(*value, precision))
                        }
                    })
                    .collect_vec(),
            ),
            _ => unreachable!("unsupported data type"),
        }
    }
}

#[derive(Debug)]
pub enum PartialColumn {
    Null(usize),
    Bool(Vec<Option<bool>>),  // 1
    TinyInt(Vec<Option<i8>>), // 2
    SmallInt(Vec<Option<i16>>),
    Int(Vec<Option<i32>>),
    BigInt(Vec<Option<i64>>),
    Float(Vec<Option<f32>>),
    Double(Vec<Option<f64>>),
    Binary(Vec<Option<Vec<u8>>>),
    Timestamp(Vec<Option<TimestampValue>>),
    NChar(Vec<Option<String>>),
    UTinyInt(Vec<Option<u8>>),
    USmallInt(Vec<Option<u16>>),
    UInt(Vec<Option<u32>>),
    UBigInt(Vec<Option<u64>>), // 14
    Json(Vec<Option<String>>),
    VarChar(Vec<Option<String>>),
    VarBinary(Vec<Option<Vec<u8>>>),
    Decimal(Vec<Option<f64>>),
    Blob(Vec<Option<Vec<u8>>>),
}

impl<'a> Stream for BlockStream<'a> {
    // type Item = (*mut TAOS_RES, i32);
    type Item = PartialRows<'a>;

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
            (*s).num_of_rows = dbg!(num_of_rows);
            if let Some(waker) = s.waker.take() {
                waker.wake()
            }
        }

        if s.completed && s.num_of_rows != 0 {
            let num_of_rows = s.num_of_rows;
            if s.num_of_rows < 1000 {
                s.num_of_rows = 0;
            } else {
                s.completed = false;
            }
            Poll::Ready(Some(Self::Item::new(self.result, num_of_rows)))
        } else if s.completed && s.num_of_rows == 0 {
            s.completed = false;
            Poll::Ready(None)
        } else {
            let res = if s.result.is_null() {
                self.result.as_raw()
            } else {
                s.result
            };
            unsafe {
                taos_fetch_rows_a(
                    res,
                    async_fetch_callback as _,
                    Box::into_raw(Box::new(self.state.clone())) as *mut _,
                );
            }
            s.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
