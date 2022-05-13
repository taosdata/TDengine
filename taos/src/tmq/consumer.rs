use std::task::Poll;

use crate::{prelude::ResultSet, Error, Result};
use futures::{Stream, TryStream};
use taos_sys::*;

use super::{Offset, Offsets, TmqList};

#[derive(Debug)]
pub struct ConsumerRef(*mut tmq_t);

impl ConsumerRef {
    pub(crate) fn from_ptr(ptr: *mut tmq_t) -> Self {
        ConsumerRef(ptr)
    }
    pub fn commit(&self, offsets: Offsets, is_async: i32) -> Result<()> {
        unsafe { tmq_commit(self.0, offsets.0, is_async) }.ok_or("commit failed")
    }
}

pub struct Consumer {
    ptr: *mut tmq_t,
    wait: i64,
}

impl Consumer {
    pub(crate) fn as_raw(&self) -> *mut tmq_t {
        self.ptr
    }
    pub(crate) fn new(ptr: *mut tmq_t, wait: i64) -> Self {
        Self { ptr, wait }
    }

    pub fn subscribe(&self, topic_list: &TmqList) -> Result<()> {
        let err = unsafe { tmq_subscribe(self.as_raw(), topic_list.0) };

        match err {
            tmq_resp_err_t::Success => Ok(()),
            tmq_resp_err_t::Fail => Err(Error::from_string("subscribe failed")),
        }
    }

    pub fn subscription(&self) -> Result<TmqList> {
        let tl = TmqList::new();

        let err = unsafe { tmq_subscription(self.as_raw(), &mut tl.as_ptr()) };
        match err {
            tmq_resp_err_t::Success => Ok(tl),
            tmq_resp_err_t::Fail => Err(Error::from_string("unsubscribe failed")),
        }
    }

    // todo: is_async better to rename to is_non_blocking
    pub fn commit(&self, offsets: Option<Offsets>, is_async: i32) -> Result<()> {
        let offsets = offsets.map(|o| o.0).unwrap_or(std::ptr::null_mut());
        let err = unsafe { tmq_commit(self.as_raw(), offsets, is_async) };
        match err {
            tmq_resp_err_t::Success => Ok(()),
            tmq_resp_err_t::Fail => Err(Error::from_string("commit failed")),
        }
    }

    pub fn poll(&self) -> Option<Result<ResultSet>> {
        let res = unsafe { tmq_consumer_poll(self.as_raw(), self.wait) };
        if res.is_null() {
            None
        } else {
            Some(ResultSet::from_ptr(res))
        }
    }

    pub fn poll_wait(&self, wait_time: i64) -> Option<Result<ResultSet>> {
        let res = unsafe { tmq_consumer_poll(self.as_raw(), wait_time) };
        if res.is_null() {
            None
        } else {
            Some(ResultSet::from_ptr(res))
        }
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        unsafe {
            log::trace!("close consumer");
            tmq_consumer_close(self.as_raw());
            log::trace!("consumer closed safely");
        }
    }
}

impl Stream for &Consumer {
    type Item = Result<ResultSet>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Poll::Ready(self.poll())
    }
}
