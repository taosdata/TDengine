use std::borrow::Cow;
use std::ffi::{c_char, CStr};
use std::future::Future;
use std::os::raw::{c_int, c_void};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crossbeam::atomic::AtomicCell;
use pin_project_lite::pin_project;
use taos_query::prelude::RawError;

use super::ApiEntry;
use crate::into_c_str::IntoCStr;
use crate::types::TAOS_RES;
use crate::RawTaos;

pin_project! {
    /// A future that will resolve when the query is complete.
    ///
    /// This future will resolve to the number of affected rows or an error.
    /// The future will be resolved when the callback is called.
    pub struct ExecFuture<'f, 'a> {
        raw: &'f RawTaos,
        sql: Cow<'a, CStr>,
        state: Rc<State>,
    }
}

unsafe impl Send for ExecFuture<'_, '_> {}

/// Shared state between the future and the waiting thread
struct State {
    api: Arc<ApiEntry>,
    result: AtomicCell<Option<(Result<usize, RawError>, Duration)>>,
    waiting: AtomicBool,
    time: Instant,
}

impl State {
    pub fn new(api: Arc<ApiEntry>) -> Self {
        State {
            api,
            result: AtomicCell::new(None),
            waiting: AtomicBool::new(false),
            time: Instant::now(),
        }
    }
}

unsafe impl Send for State {}
unsafe impl Sync for State {}

struct AsyncQueryParam {
    state: Weak<State>,
    sql: *mut c_char,
    waker: Waker,
}

impl Unpin for State {}

impl Future for ExecFuture<'_, '_> {
    type Output = Result<usize, RawError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let state = this.state;

        if let Some((result, cost)) = state.result.take() {
            let d = state.time.elapsed();
            tracing::trace!("Waken {:?} after callback received", d - cost);
            Poll::Ready(result)
        } else {
            if state.waiting.load(Ordering::SeqCst) {
                tracing::trace!("It's waked but still waiting for taos_query_a callback.");
                return Poll::Pending;
            }

            state.waiting.store(true, Ordering::SeqCst);

            #[no_mangle]
            #[inline(never)]
            unsafe extern "C" fn taos_optin_exec_future_callback(
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
                    {
                        let s = state.as_ref();
                        let cost = s.time.elapsed();
                        tracing::trace!("Received query callback in {:?}", cost);
                        if res.is_null() && code == 0 {
                            unreachable!("query callback should be ok or error");
                        }

                        let result = if code != 0 {
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
                            let affected_rows = (s.api.taos_affected_rows)(res) as usize;
                            s.api.free_result(res);
                            Ok(affected_rows)
                        };

                        s.result.store(Some((result, cost)));
                        s.waiting.store(false, Ordering::SeqCst);
                    }
                    param.waker.wake();
                } else {
                    tracing::trace!("Query callback received but no listener");
                }
            }

            let param = Box::new(AsyncQueryParam {
                state: Rc::downgrade(state),
                sql: this.sql.as_ptr() as _,
                waker: cx.waker().clone(),
            });
            this.raw.query_a(
                this.sql.as_ref(),
                taos_optin_exec_future_callback as _,
                Box::into_raw(param) as *mut _,
            );
            Poll::Pending
        }
    }
}

impl<'f, 'a> ExecFuture<'f, 'a> {
    /// Create a new `TimerFuture` which will complete after the provided timeout.
    pub fn new<T: IntoCStr<'a>>(taos: &'f RawTaos, sql: T) -> Self {
        let state = Rc::new(State::new(taos.c.clone()));
        let sql = sql.into_c_str();
        tracing::trace!("query with: {}", sql.to_str().unwrap_or("<...>"));

        ExecFuture {
            raw: taos,
            sql,
            state,
        }
    }
}
