use std::{
    ffi::c_void,
    marker::PhantomData,
    os::raw::c_int,
    sync::{Arc, Mutex, RwLock},
    task::{Poll, Waker},
};

use futures::Stream;

use taos_sys::ffi::*;
use taos_sys::*;

pub use taos_query::common::*;

use crate::impls::SyncBlock;

pub struct BlockStream<'a> {
    raw: Arc<RawRes>,
    records: Arc<RwLock<Vec<i32>>>,
    state: Arc<Mutex<BlockState>>,
    _marker: PhantomData<&'a u8>,
}

impl<'a> BlockStream<'a> {
    pub(crate) fn from_raw(raw: Arc<RawRes>, records: Arc<RwLock<Vec<i32>>>) -> Self {
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

struct BlockState {
    /// Whether or not the sleep time has elapsed
    completed: bool,
    result: *mut TAOS_RES,
    num_of_rows: i32,
    waker: Option<Waker>,
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
