use std::{
    cell::UnsafeCell,
    ffi::{CStr, CString},
    os::raw::{c_int, c_void},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use taos_error::{Code, Error};
use taos_query::common::{Field, Precision, RawData};

use crate::{
    ffi::{
        taos_errstr, taos_fetch_fields, taos_fetch_raw_block, taos_fetch_raw_block_a,
        taos_field_count, taos_get_raw_block, taos_result_precision, TAOS_RES,
    },
    tmq_get_db_name, tmq_get_json_meta, tmq_get_res_type, tmq_get_table_name, tmq_res_t,
};

#[derive(Debug)]
pub struct BlockStream {
    msg_type: tmq_res_t,
    precision: Precision,
    fields: *const Field,
    cols: usize,
    res: *mut TAOS_RES,
    shared_state: UnsafeCell<SharedState>,
}

/// Shared state between the future and the waiting thread
struct SharedState {
    block: *mut c_void,
    done: bool,
    num: usize,
    code: i32,
}

impl BlockStream {
    #[inline]
    fn fields(&self) -> &[Field] {
        unsafe { std::slice::from_raw_parts(self.fields, self.cols) }
    }
}

impl BlockStream {
    /// TMQ message data block.
    #[inline]
    fn poll_next_tmq_data(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<RawData, Error>>> {
        let res = self.res;
        let state = unsafe { &mut *self.shared_state.get() };
        unsafe {
            let mut raw: *mut c_void = std::ptr::null_mut();
            let mut rows = 0;
            let code = taos_fetch_raw_block(res, &mut rows as _, &mut raw as _);

            if code != 0 {
                return Poll::Ready(Some(Err(Error::new(
                    code,
                    CStr::from_ptr(taos_errstr(res))
                        .to_str()
                        .unwrap()
                        .to_string(),
                ))));
            }

            if rows == 0 {
                return Poll::Ready(None);
            }
            let cols = taos_field_count(res) as usize;
            let precision: Precision = taos_result_precision(res).into();

            let mut raw = RawData::parse_from_ptr(raw as _, rows as usize, cols, precision);

            let field = taos_fetch_fields(res);
            // let fields: Vec<Field> = std::slice::from_raw_parts(field, cols)
            //     .iter()
            //     .map(|f| f.into())
            //     .collect();

            raw.with_field_names(
                std::slice::from_raw_parts(field, cols)
                    .iter()
                    .map(|f| f.name().to_str().unwrap()),
            );

            // let db_name = tmq_get_db_name(res);
            // if !db_name.is_null() {
            //     raw.with_database_name(CStr::from_ptr(db_name).to_str().unwrap());
            // }

            let tbname = tmq_get_table_name(res);
            if !tbname.is_null() {
                raw.with_table_name(CStr::from_ptr(tbname).to_str().unwrap());
            }
            return Poll::Ready(Some(Ok(raw)));
        }
    }
    /// Common query data block
    #[inline]
    fn poll_next_query_data(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<RawData, Error>>> {
        let res = self.res;
        let state = unsafe { &mut *self.shared_state.get() };
        if state.done {
            // handle errors
            if state.code != 0 {
                unsafe {
                    let err = taos_errstr(res);
                    let err = CStr::from_ptr(err).to_str().unwrap_or_default();
                    let err = Error::new(state.code, err);

                    // state.done = false;
                    state.num = 0;
                    state.code = 0; // stop at next poll

                    return Poll::Ready(Some(Err(err)));
                }
            }

            if state.num > 0 {
                // has next block.
                let mut raw = unsafe {
                    RawData::parse_from_ptr(
                        state.block as _,
                        state.num as usize,
                        self.fields().len(),
                        self.precision,
                    )
                };
                raw.with_fields(self.fields().to_vec());
                if state.num > 100 {
                    state.num = 0;
                    state.done = false;
                } else {
                    state.num = 0; // finish fast
                }
                Poll::Ready(Some(Ok(raw)))
            } else {
                // no data todo, stop stream.
                Poll::Ready(None)
            }
        } else {
            let param = Box::new((&self.shared_state, cx.waker().clone()));
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
            unsafe {
                taos_fetch_raw_block_a(
                    res,
                    async_fetch_callback as _,
                    Box::into_raw(param) as *mut SharedState as _,
                )
            };
            Poll::Pending
        }
    }

    /// TMQ meta data block.
    #[inline]
    fn poll_next_tmq_meta(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<RawData, Error>>> {
        let res = self.res;
        let state = unsafe { &mut *self.shared_state.get() };
        if state.done {
            return Poll::Ready(None);
        }
        unsafe {
            let meta = tmq_get_json_meta(res);
            let meta = CString::from_raw(meta);
            let meta: Result<serde_json::Value, _> = serde_json::from_slice(&meta.into_bytes())
                .map_err(|err| Error::new(Code::Failed, err.to_string()));
            state.done = true;
            dbg!(&meta);
            return Poll::Ready(None);
        }
    }
}

impl Stream for BlockStream {
    type Item = Result<RawData, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.msg_type {
            tmq_res_t::TMQ_RES_DATA => self.poll_next_tmq_data(cx),
            tmq_res_t::TMQ_RES_TABLE_META => self.poll_next_tmq_meta(cx),
            _ => self.poll_next_query_data(cx),
        }
    }
}

impl BlockStream {
    /// Create a new `TimerFuture` which will complete after the provided
    /// timeout.
    #[inline(always)]
    pub fn new(res: *mut TAOS_RES, fields: &[Field], precision: Precision) -> Self {
        let shared_state = UnsafeCell::new(SharedState {
            done: false,
            block: std::ptr::null_mut(),
            num: 0,
            code: 0,
        });

        let msg_type = unsafe { tmq_get_res_type(res) };
        // let msg_type = tmq_res_t::TMQ_RES_INVALID;
        BlockStream {
            res,
            msg_type,
            fields: fields.as_ptr(),
            cols: fields.len(),
            precision,
            shared_state,
        }
    }
}
