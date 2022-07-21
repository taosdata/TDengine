use std::borrow::Cow;
use std::cell::UnsafeCell;

use std::ffi::CStr;
use std::future::Future;
use std::os::raw::{c_int, c_void};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::ffi::TAOS_RES;
use crate::into_c_str::IntoCStr;
use crate::{RawRes, RawTaos};
use taos_query::prelude::RawError;

pub struct QueryFuture<'a> {
    raw: RawTaos,
    sql: Cow<'a, CStr>,
    state: UnsafeCell<State>,
}

/// Shared state between the future and the waiting thread
struct State {
    result: *mut TAOS_RES,
    code: i32,
}

unsafe impl Send for State {}
unsafe impl Sync for State {}

impl Unpin for State {}
impl<'a> Unpin for QueryFuture<'a> {}
impl<'a> Future for QueryFuture<'a> {
    type Output = Result<RawRes, RawError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &*self.state.get() };
        if !state.result.is_null() || state.code != 0 {
            Poll::Ready(RawRes::from_ptr_with_code(state.result, state.code.into()))
        } else {
            unsafe extern "C" fn async_query_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                code: c_int,
            ) {
                let param = param as *mut (&UnsafeCell<State>, Waker);
                let state = param.read();
                let mut s = { &mut *state.0.get() };

                (*s).result = res;
                (*s).code = code;
                state.1.wake();
            }

            let param = Box::new((&self.state, cx.waker().clone()));

            self.raw.query_a(
                self.sql.as_ref(),
                async_query_callback as _,
                Box::into_raw(param) as *mut _,
            );
            Poll::Pending
        }
    }
}
impl<'a> QueryFuture<'a> {
    /// Create a new `TimerFuture` which will complete after the provided
    /// timeout.
    pub fn new(taos: RawTaos, sql: impl IntoCStr<'a>) -> Self {
        let state = UnsafeCell::new(State {
            result: std::ptr::null_mut(),
            code: 0,
        });

        QueryFuture {
            raw: taos,
            sql: sql.into_c_str(),
            state,
        }
    }
}
