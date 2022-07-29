use std::{
    cell::UnsafeCell,
    os::raw::{c_int, c_void},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use taos_error::Error;
use taos_query::common::{Field, Precision, RawBlock};

use crate::ffi::{taos_get_raw_block, TAOS_RES};

use super::raw_res::RawRes;

#[derive(Debug)]
pub struct Blocks {
    precision: Precision,
    fields: Vec<Field>,
    res: RawRes,
    shared_state: UnsafeCell<SharedState>,
}

impl Iterator for Blocks {
    type Item = Result<RawBlock, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.res.fetch_raw_block(&self.fields).transpose()
    }
}

impl IntoIterator for RawRes {
    type Item = Result<RawBlock, Error>;

    type IntoIter = Blocks;

    fn into_iter(self) -> Self::IntoIter {
        Blocks::new(self)
    }
}

pub struct SharedState {
    pub block: *mut c_void,
    pub done: bool,
    pub num: usize,
    pub code: i32,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            block: std::ptr::null_mut(),
            done: Default::default(),
            num: Default::default(),
            code: Default::default(),
        }
    }
}

impl Stream for Blocks {
    type Item = Result<RawBlock, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let state = unsafe { &mut *self.shared_state.get() };
        if state.done {
            // handle errors
            if state.code != 0 {
                let err = Error::new(state.code, self.res.err_as_str());

                return Poll::Ready(Some(Err(err)));
            }

            if state.num > 0 {
                // has next block.
                let mut raw = unsafe {
                    RawBlock::parse_from_ptr(
                        state.block as _,
                        state.num as usize,
                        self.fields.len(),
                        self.precision,
                    )
                };
                raw.with_field_names(self.fields.iter().map(|f| f.name()));

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
            self.res.fetch_raw_block_a(
                async_fetch_callback as _,
                Box::into_raw(param) as *mut SharedState as _,
            );
            Poll::Pending
        }
    }
}

impl Blocks {
    /// Create a new `TimerFuture` which will complete after the provided
    /// timeout.
    #[inline(always)]
    pub(crate) fn new(res: RawRes) -> Self {
        let shared_state = UnsafeCell::new(SharedState {
            done: false,
            block: std::ptr::null_mut(),
            num: 0,
            code: 0,
        });

        Self {
            fields: res.fetch_fields(),
            precision: res.precision(),
            shared_state,
            res,
        }
    }
}
