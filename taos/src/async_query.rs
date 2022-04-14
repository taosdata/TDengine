use std::{
    ffi::c_void,
    future::Future,
    marker::PhantomData,
    os::raw::c_int,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use std::sync::Mutex;
use taos_sys::{taos_query_a, TAOS_RES};

use crate::{util::IntoCStr, Result, Taos, TaosResult};

pub struct QueryFuture<'query> {
    shared_state: Arc<Mutex<SharedState>>,
    _marker: PhantomData<&'query Taos>,
}

/// Shared state between the future and the waiting thread
struct SharedState {
    completed: bool,
    result: *mut TAOS_RES,
    code: i32,
    waker: Option<Waker>,
}

unsafe impl Send for SharedState {}
unsafe impl Sync for SharedState {}

impl Unpin for SharedState {}
impl<'query> Unpin for QueryFuture<'query> {}
impl<'query> Future for QueryFuture<'query> {
    type Output = Result<TaosResult<'query>>;
    fn poll<'a>(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Look at the shared state to see if the timer has already completed.
        let mut shared_state = self.shared_state.lock().unwrap();
        if shared_state.completed {
            Poll::Ready(TaosResult::new(shared_state.result, shared_state.code))
        } else {
            // Set waker so that the thread can wake up the current task
            // when the timer has completed, ensuring that the future is polled
            // again and sees that `completed = true`.
            //
            // It's tempting to do this once rather than repeatedly cloning
            // the waker each time. However, the Future can move between
            // tasks on the executor, which could cause a stale waker pointing
            // to the wrong task, preventing from waking up
            // correctly.
            //
            // N.B. it's possible to check for this using the `Waker::will_wake`
            // function, but we omit that here to keep things simple.
            shared_state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
impl<'query> QueryFuture<'query> {
    /// Create a new `TimerFuture` which will complete after the provided
    /// timeout.
    pub fn new<'a>(taos: &Taos, sql: impl IntoCStr<'a>) -> Self {
        let shared_state = Arc::new(Mutex::new(SharedState {
            completed: false,
            result: std::ptr::null_mut(),
            code: 0,
            waker: None,
        }));

        unsafe extern "C" fn async_query_callback(
            param: *mut c_void,
            res: *mut TAOS_RES,
            code: c_int,
        ) {
            let param = param as *const Arc<Mutex<SharedState>>;
            let state = param.read();
            let mut s = state.lock().unwrap();

            (*s).completed = true;
            (*s).result = res;
            (*s).code = code;
            if let Some(waker) = s.waker.take() {
                waker.wake()
            }
        }

        unsafe {
            taos_query_a(
                taos.0,
                dbg!(sql.into_c_str()).as_ptr(),
                async_query_callback as _,
                Box::into_raw(Box::new(shared_state.clone())) as *mut _,
            );
        }

        QueryFuture {
            shared_state,
            _marker: PhantomData,
        }
    }
}
