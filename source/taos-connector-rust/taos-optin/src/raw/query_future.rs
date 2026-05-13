use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::ffi::{c_char, CStr};
use std::future::Future;
use std::os::raw::{c_int, c_void};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use taos_query::prelude::RawError;

use super::ApiEntry;
use crate::into_c_str::IntoCStr;
use crate::types::TAOS_RES;
use crate::{RawRes, RawTaos};

pub struct QueryFuture<'a> {
    raw: RawTaos,
    sql: Cow<'a, CStr>,
    state: Rc<UnsafeCell<State>>,
}

unsafe impl Send for QueryFuture<'_> {}

/// Shared state between the future and the waiting thread
struct State {
    api: Arc<ApiEntry>,
    result: Option<Result<RawRes, RawError>>,
    waiting: bool,
    time: Instant,
    callback_cost: Option<Duration>,
}

impl State {
    pub fn new(api: Arc<ApiEntry>) -> Self {
        State {
            api,
            result: None,
            waiting: false,
            time: Instant::now(),
            callback_cost: None,
        }
    }
}

unsafe impl Send for State {}
unsafe impl Sync for State {}

struct AsyncQueryParam {
    state: Weak<UnsafeCell<State>>,
    sql: *mut c_char,
    waker: Waker,
}

impl Unpin for State {}
impl Unpin for QueryFuture<'_> {}

impl Future for QueryFuture<'_> {
    type Output = Result<RawRes, RawError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &mut *self.state.get() };

        if let Some(result) = state.result.take() {
            let d = state.time.elapsed();
            tracing::trace!(
                "Waken {:?} after callback received",
                d - state.callback_cost.unwrap()
            );
            Poll::Ready(result)
        } else {
            if state.waiting {
                tracing::trace!("It's waked but still waiting for taos_query_a callback.");
                return Poll::Pending;
            }

            state.waiting = true;

            #[no_mangle]
            #[inline(never)]
            unsafe extern "C" fn taos_optin_query_future_callback(
                param: *mut c_void,
                res: *mut TAOS_RES,
                code: c_int,
            ) {
                if param.is_null() {
                    tracing::error!("query callback param should not be null");
                    return;
                }
                let param = Box::from_raw(param as *mut AsyncQueryParam);
                if let Some(state) = param.state.upgrade() {
                    // let state = param.read();
                    let s = { &mut *state.get() };
                    let cost = s.time.elapsed();
                    tracing::trace!("Received query callback in {:?}", cost);
                    s.callback_cost.replace(cost);
                    if res.is_null() && code == 0 {
                        unreachable!("query callback should be ok or error");
                    }
                    if (code & 0xffff) == 0x032C {
                        tracing::warn!("Received 0x032C (Object is creating) error, retry");
                        s.waiting = false;
                        s.api.free_result(res);
                        param.waker.wake();
                        return;
                    }

                    let result = if code < 0 {
                        let str = s.api.err_str(res);
                        let err = RawError::new_with_context(
                            code,
                            str,
                            format!(
                                "Error while querying with sql \"{}\"",
                                CStr::from_ptr(param.sql).to_string_lossy()
                            ),
                        );
                        s.api.free_result(res);
                        Err(err)
                    } else {
                        debug_assert!(!res.is_null());
                        assert_ne!(res as usize, 1, "res should not be 1");
                        Ok(RawRes::from_ptr_unchecked(s.api.clone(), res))
                    };

                    s.result.replace(result);
                    s.waiting = false;
                    param.waker.wake();
                } else {
                    tracing::trace!("Query callback received but no listener");
                }
            }

            let param = Box::new(AsyncQueryParam {
                state: Rc::downgrade(&self.state),
                sql: self.sql.as_ptr() as _,
                waker: cx.waker().clone(),
            });
            self.raw.query_a(
                self.sql.as_ref(),
                taos_optin_query_future_callback as _,
                Box::into_raw(param) as *mut _,
            );
            Poll::Pending
        }
    }
}

impl<'a> QueryFuture<'a> {
    /// Create a new `TimerFuture` which will complete after the provided timeout.
    pub fn new<T: IntoCStr<'a>>(taos: RawTaos, sql: T) -> Self {
        let state = Rc::new(UnsafeCell::new(State::new(taos.c.clone())));
        let sql = sql.into_c_str();
        tracing::trace!("query with: {}", sql.to_str().unwrap_or("<...>"));

        QueryFuture {
            raw: taos,
            sql,
            state,
        }
    }
}
