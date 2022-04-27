use crate::{Error, Result};
use taos_sys::*;

use super::{Message, Offset, Offsets, TmqList};

pub struct Consumer(*mut tmq_t);

impl Consumer {
    pub(crate) fn as_raw(&self) -> *mut tmq_t {
        self.0
    }
    pub(crate) fn new(ptr: *mut tmq_t) -> Self {
        Self(ptr)
    }

    pub fn subscribe(&self, topic_list: &TmqList) -> Result<()> {
        let err = unsafe { tmq_subscribe(self.0, topic_list.0) };

        match err {
            tmq_resp_err_t::Success => Ok(()),
            tmq_resp_err_t::Fail => Err(Error::from_string("subscribe failed")),
        }
    }
    pub fn unsubscribe(&self) -> Result<()> {
        let err = unsafe { tmq_unsubscribe(self.0) };

        match err {
            tmq_resp_err_t::Success => Ok(()),
            tmq_resp_err_t::Fail => Err(Error::from_string("unsubscribe failed")),
        }
    }

    pub fn subscription(&self) -> Result<TmqList> {
        let ptr = Box::new(std::ptr::null_mut() as *mut tmq_list_t);
        let raw = Box::into_raw(ptr);

        let err = unsafe { tmq_subscription(self.0, raw) };
        match err {
            tmq_resp_err_t::Success => Ok(TmqList(unsafe { raw.read() })),
            tmq_resp_err_t::Fail => Err(Error::from_string("unsubscribe failed")),
        }
    }
    pub fn seek(&self, offset: Offset) -> Result<()> {
        let err = unsafe { tmq_seek(self.0, offset.0) };
        match err {
            tmq_resp_err_t::Success => Ok(()),
            tmq_resp_err_t::Fail => Err(Error::from_string("commit failed")),
        }
    }
    // todo: is_async better to rename to is_non_blocking
    pub fn commit(&self, offsets: Option<&Offsets>, is_async: i32) -> Result<()> {
        let offsets = offsets.map(|o| o.0).unwrap_or(std::ptr::null_mut());
        let err = unsafe { tmq_commit(self.0, offsets, is_async) };
        match err {
            tmq_resp_err_t::Success => Ok(()),
            tmq_resp_err_t::Fail => Err(Error::from_string("commit failed")),
        }
    }

    pub fn poll(&self, blocking_time: i64) -> Option<Message> {
        todo!()
        // let message = unsafe { tmq_consumer_poll(self.0, blocking_time) };
        // if message.is_null() {
        //     None
        // } else {
        //     Some(Message::new(self, message))
        // }
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        unsafe {
            dbg!("close consumer");
            tmq_consumer_close(self.0);
            dbg!("consumer closed safely");
        }
    }
}
