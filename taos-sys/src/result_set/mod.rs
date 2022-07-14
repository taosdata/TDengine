use std::{
    cell::UnsafeCell,
    ffi::CStr,
    future::Future,
    os::raw::{c_int, c_void},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::Stream;
use taos_error::Error;
use taos_query::common::{Field, Precision, Raw};

use crate::ffi::{taos_errstr, taos_fetch_raw_block_a, taos_get_raw_block, TAOS_RES};

#[derive(Debug)]
pub struct BlockStream {
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
    fn fields(&self) -> &[Field] {
        unsafe { std::slice::from_raw_parts(self.fields, self.cols) }
    }
}

impl Stream for BlockStream {
    type Item = Result<Raw, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
                    Raw::parse_from_ptr(
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
}

impl BlockStream {
    /// Create a new `TimerFuture` which will complete after the provided
    /// timeout.
    pub fn new(res: *mut TAOS_RES, fields: &[Field], precision: Precision) -> Self {
        let shared_state = UnsafeCell::new(SharedState {
            done: false,
            block: std::ptr::null_mut(),
            num: 0,
            code: 0,
        });

        BlockStream {
            res,
            fields: fields.as_ptr(),
            cols: fields.len(),
            precision,
            shared_state,
        }
    }
}
