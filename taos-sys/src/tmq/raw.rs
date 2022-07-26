pub(super) use conf::Conf;
pub(super) use list::Topics;
pub(super) use tmq::RawTmq;

pub(super) mod tmq {
    use std::{os::raw::c_void, time::Duration};

    use itertools::Itertools;

    use crate::{RawError, RawRes};

    use super::{super::ffi::*, Topics};

    #[derive(Debug, Clone, Copy)]
    pub(crate) struct RawTmq(pub(crate) *mut tmq_t);

    unsafe impl Send for RawTmq {}
    unsafe impl Sync for RawTmq {}

    impl RawTmq {
        pub(crate) fn subscribe(&mut self, topics: &Topics) -> Result<(), RawError> {
            unsafe { tmq_subscribe(self.0, topics.0) }.ok_or(format!(
                "subscribe failed with topics: [{}]",
                topics.iter().join(",")
            ))
        }

        pub fn subscription(&self) -> Topics {
            let mut tl = Topics::new();

            unsafe { tmq_subscription(self.0, &mut tl.0) }
                .ok_or("get topic list failed")
                .expect("get topic shoudl always success");
            tl
        }

        pub fn commit_sync(&self, msg: RawRes) -> Result<(), RawError> {
            unsafe { tmq_commit_sync(self.0, msg.0 as _) }.ok_or("commit failed")
        }

        pub fn commit_async(&self, msg: RawRes, cb: tmq_commit_cb, param: *mut c_void) {
            unsafe { tmq_commit_async(self.0, msg.0, cb, param) }
        }

        pub fn commit_non_blocking(
            &mut self,
            msg: RawRes,
            callback: fn(RawTmq, Result<(), RawError>),
        ) {
            unsafe extern "C" fn tmq_commit_callback(
                _tmq: *mut tmq_t,
                resp: tmq_resp_err_t,
                param: *mut c_void,
            ) {
                log::info!("commit {resp:?}");
                let cons = RawTmq(_tmq);
                let cb = Box::from_raw(param as *mut Box<fn(RawTmq, Result<(), RawError>)>);
                cb(cons, resp.ok_or("commit failed"));
            }

            unsafe {
                tmq_commit_async(
                    self.0,
                    msg.0 as _,
                    tmq_commit_callback,
                    Box::into_raw(Box::new(callback)) as _,
                )
            }
        }

        pub async fn commit(&self, msg: RawRes) -> Result<(), RawError> {
            // use tokio::sync::oneshot::{channel, Sender};
            use std::sync::mpsc::{channel, Sender};
            let (sender, rx) = channel::<Result<(), RawError>>();
            unsafe extern "C" fn tmq_commit_async_cb(
                _tmq: *mut tmq_t,
                resp: tmq_resp_err_t,
                param: *mut std::os::raw::c_void,
            ) {
                let offsets = resp.ok_or("commit failed").map(|_| ());
                let sender = param as *mut Sender<_>;
                let sender = Box::from_raw(sender);
                sender.send(offsets).unwrap();
            }

            unsafe {
                tmq_commit_async(
                    self.0,
                    msg.0,
                    tmq_commit_async_cb,
                    Box::into_raw(Box::new(sender)) as *mut _,
                )
            }
            rx.recv().unwrap()
        }

        /// Wait a message forever
        pub fn next_or_forever(&self) -> RawRes {
            self.poll_timeout(-1)
                .expect("wait forever if there's no message")
        }

        pub fn poll_timeout(&self, timeout: i64) -> Option<RawRes> {
            let res = unsafe { tmq_consumer_poll(self.0, timeout) };
            if res.is_null() {
                None
            } else {
                Some(RawRes(res))
            }
        }

        pub fn unsubscribe(&mut self) {
            unsafe {
                log::trace!("close consumer");
                tmq_unsubscribe(self.0);
                log::trace!("consumer closed safely");
            }
        }

        pub fn close(&mut self) {
            unsafe {
                tmq_consumer_close(self.0);
            }
        }
    }
}

pub(super) mod conf {
    use crate::IntoCStr;
    use taos_error::*;

    use crate::*;
    use std::{ffi::c_void, iter::Iterator};
    use taos_query::Dsn;

    use super::RawTmq;

    /* tmq conf */
    pub struct Conf(*mut tmq_conf_t);

    impl Conf {
        pub(crate) fn as_ptr(&self) -> *mut tmq_conf_t {
            self.0
        }
        pub(crate) fn new() -> Self {
            Self(unsafe { tmq_conf_new() })
        }

        pub(crate) fn from_dsn(dsn: &Dsn) -> Result<Self> {
            let mut conf = Self::new();
            macro_rules! _set_opt {
                ($f:ident, $c:literal) => {
                    if let Some($f) = &dsn.$f {
                        conf.set(format!("td.connect.{}", $c), format!("{}", $f))?;
                    }
                };
                ($f:ident) => {
                    if let Some($f) = &dsn.$f {
                        conf.set(format!("td.connect.{}", stringify!($c)), format!("{}", $f))?;
                    }
                };
            }

            // todo: host port?
            _set_opt!(username, "user");
            _set_opt!(password, "pass");
            _set_opt!(database, "db");

            // todo: do explicitly param name filter.
            conf.with(dsn.params.iter().filter(|(k, _)| k.contains(".")))
        }

        pub(crate) fn with_group_id(mut self, id: &str) -> Self {
            self.set("group.id", id)
                .expect("set group.id should always be ok");
            self
        }
        pub(crate) fn with_client_id(mut self, id: &str) -> Self {
            self.set("client.id", id)
                .expect("set group.id should always be ok");
            self
        }

        pub(crate) fn enable_auto_commit(mut self) -> Self {
            self.set("enable.auto.commit", "true")
                .expect("set group.id should always be ok");
            self
        }
        pub fn disable_auto_commit(mut self) -> Self {
            self.set("enable.auto.commit", "false")
                .expect("set group.id should always be ok");
            self
        }

        pub(crate) fn with<K: AsRef<str>, V: AsRef<str>>(
            mut self,
            iter: impl Iterator<Item = (K, V)>,
        ) -> Result<Self> {
            for (k, v) in iter {
                self.set(k, v)?;
            }
            Ok(self)
        }

        fn set<K: AsRef<str>, V: AsRef<str>>(&mut self, key: K, value: V) -> Result<&mut Self> {
            let ret = unsafe {
                tmq_conf_set(
                    self.0,
                    key.as_ref().into_c_str().as_ptr(),
                    value.as_ref().into_c_str().as_ptr(),
                )
            };
            match ret {
                tmq_conf_res_t::Ok => Ok(self),
                tmq_conf_res_t::Invalid => {
                    Err(RawError::from_string("invalid key value set for tmq"))
                }
                tmq_conf_res_t::Unknown => Err(RawError::from_string("unknown key for tmq conf")),
            }
        }

        pub(crate) fn with_auto_commit_cb(&mut self, cb: tmq_commit_cb, param: *mut c_void) -> () {
            unsafe {
                tmq_conf_set_auto_commit_cb(self.0, cb, param);
            }
        }

        pub(crate) fn build(&self) -> Result<RawTmq> {
            unsafe {
                let mut err = [0; 256];
                let tmq = tmq_consumer_new(self.0, err.as_mut_ptr() as _, 255);
                if err[0] != 0 {
                    Err(RawError::from_string(
                        String::from_utf8_lossy(&err).to_string(),
                    ))
                } else {
                    Ok(RawTmq(tmq))
                }
            }
        }
    }

    impl Drop for Conf {
        fn drop(&mut self) {
            log::trace!("tmq config destroy");
            unsafe { tmq_conf_destroy(self.0) };
            log::trace!("tmq config destroyed safely");
        }
    }
}

pub(super) mod list {
    use std::ffi::CStr;

    use taos_query::prelude::{Code, RawError};

    use crate::into_c_str::IntoCStr;

    use super::super::ffi::*;

    type Result<T> = std::result::Result<T, RawError>;

    #[derive(Debug)]
    pub(crate) struct Topics(pub *mut tmq_list_t);

    impl Topics {
        pub(crate) fn new() -> Self {
            Self(unsafe { tmq_list_new() })
        }

        pub(crate) fn append<'a>(&mut self, c_str: impl IntoCStr<'a>) -> Result<()> {
            let ret = unsafe { tmq_list_append(self.0, c_str.into_c_str().as_ptr()) };
            if ret == 0 {
                Ok(())
            } else {
                Err(RawError::new(Code::Failed, "append tmq list error"))
            }
        }

        pub(crate) fn from_topics<'a, T: IntoCStr<'a>>(
            topics: impl IntoIterator<Item = T>,
        ) -> Result<Self> {
            let mut list = Self::new();
            for topic in topics {
                list.append(topic)?;
            }
            Ok(list)
        }

        pub fn iter(&self) -> Iter {
            let ptr = self.0;
            let len = unsafe { tmq_list_get_size(ptr) } as usize;
            let arr = unsafe { tmq_list_to_c_array(ptr) };
            let inner = unsafe { std::slice::from_raw_parts(arr, len as usize) };
            Iter {
                len,
                inner,
                index: 0,
            }
        }

        pub fn into_strings(&self) -> Vec<String> {
            self.iter().map(|s| s.to_string()).collect()
        }
    }

    impl<'a> IntoIterator for &'a Topics {
        type Item = &'a str;

        type IntoIter = Iter<'a>;

        fn into_iter(self) -> Self::IntoIter {
            self.iter()
        }
    }

    ///
    pub struct Iter<'a> {
        inner: &'a [*mut i8],
        len: usize,
        index: usize,
    }

    impl<'a> Iterator for Iter<'a> {
        type Item = &'a str;

        fn next(&mut self) -> Option<Self::Item> {
            self.inner
                .get(self.index)
                .map(|ptr| {
                    unsafe { CStr::from_ptr(*ptr) }
                        .to_str()
                        .expect("topic name must be valid utf8 str")
                })
                .map(|s| {
                    self.index += 1;
                    s
                })
        }
    }

    impl<'a> ExactSizeIterator for Iter<'a> {
        fn len(&self) -> usize {
            if self.len >= self.index {
                self.len - self.index
            } else {
                0
            }
        }
    }

    // todo: tmq_list_destroy cause double free error.
    impl Drop for Topics {
        fn drop(&mut self) {
            unsafe {
                log::trace!("list destroy");
                tmq_list_destroy(self.0);
                log::trace!("list destroy destroyed");
            }
        }
    }
}
