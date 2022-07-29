use std::cell::UnsafeCell;
use std::ffi::CStr;
use std::os::raw::*;
use std::task::{Context, Poll, Waker};

use taos_error::{Code, Error};
use taos_query::{
    common::{Field, Precision},
    RawBlock,
};

use crate::types::from_raw_fields;
use crate::{ffi::*, tmq::ffi::tmq_res_t};

use super::blocks::SharedState;
use super::{blocks::Blocks, message::MessageStream};

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct RawRes(pub *mut TAOS_RES);

unsafe impl Send for RawRes {}
unsafe impl Sync for RawRes {}

impl RawRes {
    #[inline]
    pub fn as_ptr(&self) -> *mut TAOS_RES {
        self.0
    }

    #[inline]
    pub fn errno(&self) -> Code {
        unsafe { taos_errno(self.as_ptr()) & 0xffff }.into()
    }
    #[inline]
    pub fn errstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(taos_errstr(self.as_ptr())) }
    }
    #[inline]
    pub fn err_as_str(&self) -> &'static str {
        unsafe {
            std::str::from_utf8_unchecked(CStr::from_ptr(taos_errstr(self.as_ptr())).to_bytes())
        }
    }

    #[inline]
    pub fn from_ptr(ptr: *mut TAOS_RES) -> Result<Self, Error> {
        let raw = unsafe { Self::from_ptr_unchecked(ptr) };
        let code = raw.errno();
        raw.with_code(code)
    }

    #[inline]
    pub unsafe fn from_ptr_unchecked(ptr: *mut TAOS_RES) -> RawRes {
        Self(ptr)
    }

    #[inline]
    pub fn from_ptr_with_code(ptr: *mut TAOS_RES, code: Code) -> Result<RawRes, Error> {
        unsafe { RawRes::from_ptr_unchecked(ptr) }.with_code(code)
    }

    #[inline]
    pub fn with_code(self, code: Code) -> Result<Self, Error> {
        if code.success() {
            Ok(self)
        } else {
            Err(Error::new(code, self.err_as_str()))
        }
    }

    #[inline]
    pub fn field_count(&self) -> usize {
        unsafe { taos_field_count(self.as_ptr()) as _ }
    }

    pub fn fetch_fields(&self) -> Vec<Field> {
        let len = unsafe { taos_field_count(self.as_ptr()) };
        from_raw_fields(unsafe { taos_fetch_fields(self.as_ptr()) }, len as usize)
    }

    #[inline]
    pub fn fetch_lengths(&self) -> &[u32] {
        unsafe {
            std::slice::from_raw_parts(
                taos_fetch_lengths(self.as_ptr()) as *const u32,
                self.field_count(),
            )
        }
    }
    #[inline]
    unsafe fn fetch_lengths_raw(&self) -> *const i32 {
        taos_fetch_lengths(self.as_ptr())
    }

    #[inline]
    pub fn fetch_block(&self) -> Result<Option<(TAOS_ROW, i32, *const i32)>, Error> {
        let block = Box::into_raw(Box::new(std::ptr::null_mut()));
        // let mut num = 0;
        let num = unsafe { taos_fetch_block(self.as_ptr(), block) };
        // taos_fetch_block(res, rows)
        if num > 0 {
            Ok(Some(unsafe { (*block, num, self.fetch_lengths_raw()) }))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn get_column_data_offset(&self, col: usize) -> *const i32 {
        unsafe { taos_get_column_data_offset(self.as_ptr(), col as i32) }
    }

    #[inline]
    pub fn fetch_raw_block(&self, fields: &[Field]) -> Result<Option<RawBlock>, Error> {
        #[cfg(taos_v3)]
        return self.fetch_raw_block_v3(fields);
        #[cfg(not(taos_v3))]
        self.fetch_raw_block_v2(fields)
    }

    pub fn fetch_raw_block_async(
        &self,
        fields: &[Field],
        precision: Precision,
        state: &UnsafeCell<SharedState>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<RawBlock>, Error>> {
        let current = unsafe { &mut *state.get() };

        if current.done {
            // handle errors
            if current.code != 0 {
                let err = Error::new(current.code, self.err_as_str());

                return Poll::Ready(Err(err));
            }

            if current.num > 0 {
                // has next block.
                let mut raw = unsafe {
                    RawBlock::parse_from_ptr(
                        current.block as _,
                        current.num as usize,
                        fields.len(),
                        precision,
                    )
                };
                raw.with_field_names(fields.iter().map(|f| f.name()));
                if current.num < 100 {
                    // finish fetch loop fast.
                    current.done = true;
                    current.num = 0;
                } else {
                    current.done = false;
                }
                Poll::Ready(Ok(Some(raw)))
            } else {
                // no data todo, stop stream.
                Poll::Ready(Ok(None))
            }
        } else {
            let param = Box::new((&*state, cx.waker().clone()));
            unsafe extern "C" fn async_fetch_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                num_of_rows: c_int,
            ) {
                let param = param as *mut (&UnsafeCell<SharedState>, Waker);
                let param = Box::from_raw(param);
                let state = &mut *param.0.get();
                state.done = true;
                state.block = taos_get_raw_block(res);
                if num_of_rows < 0 {
                    state.code = num_of_rows;
                } else {
                    state.num = num_of_rows as _;
                }
                param.1.wake()
            }
            self.fetch_raw_block_a(
                async_fetch_callback as _,
                Box::into_raw(param) as *mut _ as _,
            );
            Poll::Pending
        }
    }

    /// Only for tmq.
    pub(crate) fn fetch_raw_message(&self, precision: Precision) -> Option<RawBlock> {
        let mut block: *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        unsafe { taos_fetch_raw_block(self.as_ptr(), &mut num as _, &mut block as _) };
        let fields = self.fetch_fields();

        if num == 0 || block.is_null() {
            return None;
        }
        let mut raw = unsafe {
            RawBlock::parse_from_ptr(block as _, num as usize, fields.len(), self.precision())
        };

        raw.with_field_names(fields.iter().map(Field::name));

        if let Some(name) = self.tmq_table_name() {
            raw.with_table_name(name);
        }

        Some(raw)
    }

    #[inline]
    pub fn fetch_raw_block_v3(&self, fields: &[Field]) -> Result<Option<RawBlock>, Error> {
        let mut block: *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        crate::err_or!(
            self,
            taos_fetch_raw_block(self.as_ptr(), &mut num as _, &mut block as _),
            if num > 0 {
                match self.tmq_message_type() {
                    tmq_res_t::TMQ_RES_INVALID => {
                        let mut raw = RawBlock::parse_from_ptr(
                            block as _,
                            num as usize,
                            self.field_count(),
                            self.precision(),
                        );
                        raw.with_field_names(self.fetch_fields().iter().map(Field::name));
                        Some(raw)
                    }
                    tmq_res_t::TMQ_RES_DATA => {
                        let fields = self.fetch_fields();

                        let mut raw = RawBlock::parse_from_ptr(
                            block as _,
                            num as usize,
                            fields.len(),
                            self.precision(),
                        );

                        raw.with_field_names(fields.iter().map(Field::name));

                        if let Some(name) = self.tmq_db_name() {
                            raw.with_database_name(name);
                        }

                        if let Some(name) = self.tmq_table_name() {
                            raw.with_table_name(name);
                        }

                        Some(raw)
                    }
                    tmq_res_t::TMQ_RES_TABLE_META => {
                        todo!()
                    }
                }
            } else {
                None
            }
        )
    }
    #[inline]
    pub fn fetch_raw_block_v2(&self, fields: &[Field]) -> Result<Option<RawBlock>, Error> {
        let mut block: *mut *mut c_void = std::ptr::null_mut();
        let mut num = 0;
        let lengths = self.fetch_lengths();
        let cols = self.field_count();
        let lengths = self.fetch_lengths();
        crate::err_or!(
            self,
            taos_fetch_block_s(self.as_ptr(), &mut num as _, &mut block as _),
            if num > 0 {
                let raw = RawBlock::parse_from_ptr_v2(
                    block as _,
                    &fields,
                    lengths,
                    num as usize,
                    self.precision(),
                );
                Some(raw)
            } else {
                None
            }
        )
    }

    pub fn to_blocks(&self) -> Blocks {
        Blocks::new(self.clone())
    }

    // // #[inline]
    // pub fn fetch_raw_block_async(&self) -> BlockStream {
    //     BlockStream::new(self.as_ptr(), fields, self.precision())
    // }

    // #[inline]
    pub fn fetch_raw_message_async(&self) -> MessageStream {
        MessageStream::new(self.clone())
    }

    #[inline]
    pub fn is_update_query(&self) -> bool {
        unsafe { taos_is_update_query(self.as_ptr()) }
    }

    #[inline]
    pub fn is_null(&self, row: i32, col: i32) -> bool {
        unsafe { taos_is_null(self.as_ptr(), row, col) }
    }

    #[inline]
    pub fn stop_query(&self) {
        unsafe { taos_stop_query(self.as_ptr()) }
    }

    #[inline]
    pub fn select_db(&self, db: *const i8) -> Result<(), Error> {
        crate::err_or!(self, taos_select_db(self.as_ptr(), db))
    }

    #[inline]
    pub fn affected_rows(&self) -> i32 {
        unsafe { taos_affected_rows(self.as_ptr()) }
    }

    #[inline]
    pub fn free_result(&mut self) {
        unsafe { taos_free_result(self.as_ptr()) }
    }

    #[inline]
    pub fn precision(&self) -> Precision {
        unsafe { taos_result_precision(self.as_ptr()) }.into()
    }

    // #[inline]
    // pub fn fetch_row(&self) -> TAOS_ROW {
    //     unsafe { taos_fetch_row(self.as_ptr()) }
    // }

    #[inline]
    pub fn fetch_rows_a(&self, fp: taos_async_fetch_cb, param: *mut c_void) {
        unsafe { taos_fetch_rows_a(self.as_ptr(), fp, param) }
    }

    #[inline]
    pub fn fetch_raw_block_a(&self, fp: taos_async_fetch_cb, param: *mut c_void) {
        unsafe { taos_fetch_raw_block_a(self.as_ptr(), fp, param) }
    }

    #[inline]
    pub fn block(&self) -> *mut *mut c_void {
        unsafe { taos_result_block(self.as_ptr()).read() }
    }

    pub(crate) fn drop(&mut self) {
        unsafe {
            taos_free_result(self.0);
        }
    }
}
