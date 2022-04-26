use std::{
    ffi::c_void,
    marker::PhantomData,
    ops::Deref,
    os::raw::c_int,
    ptr,
    rc::Rc,
    slice,
    sync::{Arc, Mutex, RwLock},
    task::{Poll, Waker},
};

use bitvec_simd::BitVec;
use bstr::ByteSlice;
use futures::Stream;
use itertools::Itertools;

use taos_sys::ffi::*;
use taos_sys::*;

use taos_query::common::*;

use crate::{impls::SyncBlock, Error, Result, TaosResult};

mod column;
pub use column::*;

mod row;
pub use row::*;

mod value;
pub use value::*;

pub struct BlockStream<'a> {
    // result: &'a TaosResult<'a>,
    raw: Arc<RawRes>,
    records: Arc<RwLock<Vec<i32>>>,
    state: Arc<Mutex<BlockState>>,
    _marker: PhantomData<&'a u8>,
}

impl<'a> BlockStream<'a> {
    pub fn new(result: &'a TaosResult<'a>) -> Self {
        let state = Arc::new(Mutex::new(BlockState {
            completed: false,
            result: std::ptr::null_mut(),
            num_of_rows: 0,
            waker: None,
        }));
        let raw = result.as_raw().raw();

        Self {
            raw,
            state,
            records: Arc::new(RwLock::new(Vec::new())),
            _marker: PhantomData,
        }
    }

    pub fn from_raw(raw: Arc<RawRes>, records: Arc<RwLock<Vec<i32>>>) -> Self {
        let state = Arc::new(Mutex::new(BlockState {
            completed: false,
            result: std::ptr::null_mut(),
            num_of_rows: 0,
            waker: None,
        }));

        Self {
            raw,
            state,
            records,
            _marker: PhantomData,
        }
    }
}

unsafe impl<'a> Send for BlockStream<'a> {}
unsafe impl<'a> Sync for BlockStream<'a> {}

#[derive(Debug)]
pub struct Block<'a> {
    result: &'a TaosResult<'a>,
    inner: &'a [*mut c_void],
    lengths: &'a [i32],
    num_of_rows: i32,
}

unsafe impl<'a> Send for Block<'a> {}
unsafe impl<'a> Sync for Block<'a> {}

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
    fn new(result: &'a TaosResult<'a>, block: TAOS_ROW, num_of_rows: i32) -> Self {
        let lengths = result.as_raw().fetch_lengths();
        // let lengths = unsafe { taos_fetch_lengths(result.as_raw()) };
        let num_of_fields = result.num_of_fields();
        Self {
            result,
            inner: unsafe { std::slice::from_raw_parts(block, num_of_fields) },
            lengths,
            num_of_rows,
        }
    }

    #[inline]
    fn from_async_query(result: &'a TaosResult<'a>, num_of_rows: i32) -> Self {
        // let filed_count = unsafe { taos_num_fields(result) };
        let block = result.as_raw().block();
        Self::new(result, block, num_of_rows)
    }

    // for taos_fetch_block
    // fn from_query(result: &'a TaosResult<'a>) -> Result<Self> {
    //     // let block: *mut TAOS_ROW = Box::into_raw(Box::new(ptr::null_mut()));
    //     let (block , num_of_rows) = result.as_raw().fetch_block().expect("");
    //     Self::new(result, block, num_of_rows)
    // }

    pub const fn num_of_fields(&self) -> usize {
        self.lengths.len()
    }

    pub const fn num_of_rows(&self) -> i32 {
        self.num_of_rows
    }

    pub fn rows_iter(&self) -> RowsIter {
        RowsIter {
            block: self,
            // columns: self.columns_iter().collect(),
            index: 0,
        }
    }

    pub fn columns_iter(&self) -> ColumnsIter {
        ColumnsIter {
            block: self,
            current: 0,
        }
    }

    pub fn into_iter_rows(self) -> impl Iterator<Item = Row<'a>> {
        let num_of_rows = self.num_of_rows as _;
        std::iter::repeat(Rc::new(self))
            .enumerate()
            .take(num_of_rows)
            .map(|(index, block)| Row::new(block, index))
    }

    fn is_null(&self, row: usize, col: usize) -> bool {
        self.result.as_raw().is_null(row as _, col as _)
    }

    unsafe fn get_str(&'a self, row: usize, col: usize) -> Result<Option<&'a str>> {
        let field = self.result.get_field_unchecked(col);
        use Ty::*;
        let ty = field.ty();
        if !matches!(field.ty(), NChar | VarChar | Json) {
            return Err(Error::from_string("unmatched data type pattern for string"));
        }
        let slice = self.inner.get_unchecked(col);
        let is_null = self.is_null(row, col);
        if is_null {
            Ok(None)
        } else {
            match ty {
                VarChar | NChar | Json => {
                    let length = self.lengths.get_unchecked(col);

                    let ptr = (*slice as *const u8).offset(row as isize * *length as isize);
                    let len = ptr.cast::<i16>().read();
                    let start = ptr.offset(2);
                    let s = std::str::from_utf8(slice::from_raw_parts(start as _, len as _))
                        .map_err(|s| Error::from_string(s.to_string()))?;
                    Ok(Some(s))
                }
                _ => Ok(None),
            }
        }
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
        let field = self.get_field_unchecked(col);
        let is_null = self.is_null(row, col);
        if is_null {
            return BorrowedValue::Null;
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

        match field.ty() {
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
                BorrowedValue::Timestamp(Timestamp::new(raw, self.precision()))
            }
            Ty::VarChar => {
                let length = self.get_length_unchecked(col);
                let ptr = (*inner as *const u8).add(row * length as usize);
                let len = ptr.cast::<i16>().read();
                let start = ptr.offset(2);

                BorrowedValue::VarChar(std::str::from_utf8_unchecked(slice::from_raw_parts(
                    start, len as _,
                )))
            }
            Ty::NChar => {
                let length = self.get_length_unchecked(col);

                let ptr = (*inner as *const u8).add(row * length as usize);
                let len = ptr.cast::<i16>().read();
                let start = ptr.offset(2);

                BorrowedValue::NChar(std::str::from_utf8_unchecked(slice::from_raw_parts(
                    start as _, len as _,
                )))
            }
            Ty::Json => {
                log::debug!("field: {field}");
                let length = self.get_length_unchecked(col);
                let ptr = (*inner as *const u8).add(row * length as usize);
                let len = ptr.cast::<i16>().read();
                let start = ptr.offset(2);

                BorrowedValue::Json(slice::from_raw_parts(start, len as _).into())
            }
            _ => BorrowedValue::Null,
        }
    }

    unsafe fn get_col_unchecked(&self, col: usize) -> BorrowedColumn {
        let inner = self.inner.get_unchecked(col);
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
            TSDB_DATA_TYPE_BINARY => {
                let length = self.lengths.get_unchecked(col);
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
                let length = self.lengths.get_unchecked(col);
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

    fn into_rows_stream(self) -> () {
        todo!()
        // futures::stream::iter(self.into_iter_rows())
    }
}

pub struct RowsIter<'block> {
    block: &'block Block<'block>,
    // columns: Vec<BorrowedColumn<'block>>,
    index: usize,
}

impl<'a> Deref for RowsIter<'a> {
    type Target = Block<'a>;

    fn deref(&self) -> &Self::Target {
        self.block
    }
}

pub struct ColumnsIter<'block, 'a> {
    block: &'block Block<'a>,
    current: usize,
}

impl<'block> Iterator for RowsIter<'block> {
    type Item = Vec<BorrowedValue<'block>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.num_of_rows() as _ {
            return None;
        }
        let num_of_fields = self.num_of_fields();

        let mut row = Vec::with_capacity(num_of_fields);
        for col in 0..self.num_of_fields() {
            row.push(unsafe { self.get_value_unchecked(self.index, col) });
        }

        self.index += 1;
        Some(row)
    }
}

impl<'block> ExactSizeIterator for RowsIter<'block> {
    fn len(&self) -> usize {
        self.num_of_rows() as _
    }
}

impl<'block, 'a> Iterator for ColumnsIter<'block, 'a> {
    type Item = BorrowedColumn<'block>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.block.num_of_fields() {
            return None;
        }

        let v = unsafe { self.block.get_col_unchecked(self.current) };
        self.current += 1;
        Some(v)
    }
}

impl<'block, 'a> ExactSizeIterator for ColumnsIter<'block, 'a> {
    fn len(&self) -> usize {
        self.block.num_of_fields()
    }
}

impl<'a> Stream for BlockStream<'a> {
    // type Item = (*mut TAOS_RES, i32);
    type Item = SyncBlock<'a>;

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
            drop(s);
            
            self.records.write().unwrap().push(num_of_rows);

            // Wake up poll.
            Poll::Ready(Self::Item::from_async_query(
                self.raw.clone(),
                self.raw.block(),
                num_of_rows,
            ))
        } else if s.completed && s.num_of_rows == 0 {
            Poll::Ready(None)
        } else {
            let res = if s.result.is_null() {
                self.raw.as_ptr()
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
