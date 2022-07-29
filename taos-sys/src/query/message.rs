use std::{
    cell::UnsafeCell,
    os::raw::c_void,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use taos_query::{common::Precision, RawBlock};

use crate::tmq::ffi::tmq_res_t;

use super::raw_res::RawRes;

#[derive(Debug)]
pub struct MessageStream {
    msg_type: tmq_res_t,
    precision: Precision,
    res: RawRes,
    shared_state: UnsafeCell<SharedState>,
}

/// Shared state between the future and the waiting thread
struct SharedState {
    block: *mut c_void,
    done: bool,
    num: usize,
    code: i32,
}

impl Stream for MessageStream {
    type Item = RawBlock;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.res.fetch_raw_message(self.precision))
    }
}

impl MessageStream {
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

        let msg_type = res.tmq_message_type();
        let precision = res.precision();
        MessageStream {
            res,
            msg_type,
            precision,
            shared_state,
        }
    }
}
