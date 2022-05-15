use std::{
    ffi::c_void,
    marker::PhantomData,
    os::raw::c_int,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
    task::{Poll, Waker},
};

use futures::Stream;

use taos_sys::ffi::*;
use taos_sys::*;

pub use taos_query::common::*;

use crate::impls::SyncBlock;

pub struct BlockStream {
    raw: Arc<RawRes>,
    summary: Arc<(AtomicU64, AtomicU64)>,
    state: Arc<Mutex<BlockState>>,
}

impl BlockStream {
    pub(crate) fn from_raw(raw: Arc<RawRes>, summary: Arc<(AtomicU64, AtomicU64)>) -> Self {
        let state = Arc::new(Mutex::new(BlockState {
            completed: false,
            result: std::ptr::null_mut(),
            num_of_rows: 0,
            waker: None,
        }));

        Self {
            raw,
            state,
            summary,
        }
    }
    pub(crate) fn append_num_of_rows(&self, num_of_rows: i32) {
        use std::sync::atomic::Ordering::SeqCst;
        self.summary.0.fetch_add(1, SeqCst);
        self.summary.1.fetch_add(num_of_rows as _, SeqCst);
    }
}

unsafe impl Send for BlockStream {}
unsafe impl Sync for BlockStream {}

struct BlockState {
    /// Whether or not the sleep time has elapsed
    completed: bool,
    result: *mut TAOS_RES,
    num_of_rows: i32,
    waker: Option<Waker>,
}

impl Stream for BlockStream {
    // type Item = (*mut TAOS_RES, i32);
    type Item = SyncBlock;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // todo(3.0): remove these line to use taos_query_a in async/await impl.
        if crate::client_info().starts_with("3") {
            let block = if let Ok(Some((data, num_of_rows, lengths))) = self.raw.fetch_block() {
                log::trace!("fetch block: {num_of_rows}");

                self.append_num_of_rows(num_of_rows);

                Some(SyncBlock {
                    raw: self.raw.clone(),
                    precision: self.raw.precision(),
                    data,
                    lengths,
                    num_of_rows: num_of_rows as _,
                })
            } else {
                None
            };
            return Poll::Ready(block);
        }

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

            self.append_num_of_rows(num_of_rows);

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
