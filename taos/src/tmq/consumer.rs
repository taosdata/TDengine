use std::{intrinsics::transmute, task::Poll};

use crate::prelude::ResultSet;
use futures::{FutureExt, Stream};
use taos_error::*;
use taos_sys::*;

use super::{Offsets, TmqList};

#[derive(Debug)]
pub struct ConsumerRef(*mut tmq_t);

impl ConsumerRef {
    pub(crate) fn from_ptr(ptr: *mut tmq_t) -> Self {
        ConsumerRef(ptr)
    }
    pub fn commit(&self, offsets: Offsets) -> Result<()> {
        unsafe { tmq_commit_sync(self.0, offsets.0) }.ok_or("commit failed")
    }
}

#[derive(Debug)]
pub struct Consumer {
    ptr: *mut tmq_t,
    wait: i64,
}

impl Unpin for Consumer {}

unsafe impl Send for Consumer {}
unsafe impl Sync for Consumer {}

impl Consumer {
    pub(crate) fn as_raw(&self) -> *mut tmq_t {
        self.ptr
    }
    pub(crate) fn new(ptr: *mut tmq_t, wait: i64) -> Self {
        Self { ptr, wait }
    }

    pub(crate) fn subscribe(&mut self, topic_list: &TmqList) -> Result<()> {
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

    pub fn commit_sync(&mut self, offsets: impl Into<Offsets>) -> Result<()> {
        unsafe { tmq_commit_sync(self.as_raw(), offsets.into().as_ptr()) }.ok_or("commit failed")
    }

    pub fn commit_non_blocking(
        &mut self,
        offsets: impl Into<Offsets>,
        callback: fn(ConsumerRef, Result<Offsets>),
    ) {
        let offsets = offsets.into();
        unsafe {
            tmq_commit_async(
                self.as_raw(),
                offsets.as_ptr(),
                super::tmq_commit_callback,
                Box::into_raw(Box::new(callback)) as _,
            )
        }
    }

    pub async fn commit(&mut self, offsets: impl Into<Offsets>) -> Result<Offsets> {
        use tokio::sync::oneshot::{channel, Sender};
        let (sender, rx) = channel::<Result<Offsets>>();
        let offsets = offsets.into();
        unsafe extern "C" fn tmq_commit_async_cb(
            _tmq: *mut tmq_t,
            resp: tmq_resp_err_t,
            _topic: *mut tmq_topic_vgroup_list_t,
            param: *mut std::os::raw::c_void,
        ) {
            let offsets = resp.ok_or("commit failed").map(|_| Offsets(_topic));
            let sender = param as *mut Sender<_>;
            let sender = Box::from_raw(sender);
            sender.send(offsets).unwrap();
        }

        unsafe {
            tmq_commit_async(
                self.as_raw(),
                offsets.as_ptr(),
                tmq_commit_async_cb,
                Box::into_raw(Box::new(sender)) as *mut _,
            )
        }
        Ok(rx.await.unwrap()?)
    }

    pub fn poll(&mut self) -> Option<crate::Result<ResultSet>> {
        let res = unsafe { tmq_consumer_poll(self.as_raw(), self.wait) };
        if res.is_null() {
            None
        } else {
            Some(
                ResultSet::from_ptr(res)
                    .map(|rs| rs.independent())
                    .map_err(Into::into),
            )
        }
    }

    pub fn async_poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<crate::Result<ResultSet>>> {
        struct TmqRef(*mut tmq_t);
        unsafe impl Send for TmqRef {}
        unsafe impl Sync for TmqRef {}
        let wait = self.wait;
        let ptr = TmqRef(self.as_raw());
        tokio::task::spawn_blocking(move || {
            let ptr = unsafe { transmute(ptr) };
            let res = unsafe { tmq_consumer_poll(ptr, wait) };
            if res.is_null() {
                None
            } else {
                Some(
                    ResultSet::from_ptr(res)
                        .map(|rs| rs.independent())
                        .map_err(Into::into),
                )
            }
        })
        .poll_unpin(cx)
        .map(|res| res.unwrap())
    }

    pub async fn async_poll2(&mut self) -> Option<crate::Result<ResultSet>> {
        struct TmqRef(*mut tmq_t);
        unsafe impl Send for TmqRef {}
        unsafe impl Sync for TmqRef {}
        let wait = self.wait;
        let ptr = TmqRef(self.as_raw());
        tokio::task::spawn_blocking(move || {
            let ptr = unsafe { transmute(ptr) };
            let res = unsafe { tmq_consumer_poll(ptr, wait) };
            if res.is_null() {
                None
            } else {
                Some(
                    ResultSet::from_ptr(res)
                        .map(|rs| rs.independent())
                        .map_err(Into::into),
                )
            }
        })
        .await
        .unwrap_or(None)
    }

    pub fn poll_wait(&mut self, wait_time: i64) -> Option<crate::Result<ResultSet>> {
        let res = unsafe { tmq_consumer_poll(self.as_raw(), wait_time) };
        if res.is_null() {
            None
        } else {
            Some(ResultSet::from_ptr(res).map_err(Into::into))
        }
    }

    pub fn unsubscribe(&mut self) {
        unsafe {
            log::trace!("close consumer");
            tmq_consumer_close(self.as_raw());
            log::trace!("consumer closed safely");
        }
    }
}

impl Iterator for Consumer {
    type Item = Result<ResultSet>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
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

impl Stream for Consumer {
    type Item = crate::Result<ResultSet>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // self.async_poll2().boxed().poll_unpin(cx)
        Poll::Ready(self.poll())
    }
}
