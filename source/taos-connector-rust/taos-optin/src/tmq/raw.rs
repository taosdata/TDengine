pub(super) use conf::Conf;
pub(super) use list::Topics;
pub(super) use tmq::RawTmq;

pub(super) mod tmq {
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use taos_query::tmq::{Assignment, VGroupId};
    use taos_query::RawError;
    use tokio::sync::oneshot;
    use tracing::trace;

    use super::Topics;
    use crate::into_c_str::IntoCStr;
    use crate::raw::{ApiEntry, TmqApi};
    use crate::types::{tmq_resp_err_t, tmq_t, SafeTmqT};
    use crate::{RawRes, RawResult};

    #[derive(Debug)]
    pub struct RawTmq {
        api: Arc<ApiEntry>,
        tmq_api: Arc<TmqApi>,
        tmq_ptr: *mut tmq_t,
        timeout: i64,
        sender: flume::Sender<oneshot::Sender<RawResult<Option<RawRes>>>>,
        receiver: Option<flume::Receiver<oneshot::Sender<RawResult<Option<RawRes>>>>>,
        thread_handle: Option<std::thread::JoinHandle<()>>,
        stop_signal: Arc<AtomicBool>,
        supports_poll_null_check: bool,
    }

    unsafe impl Send for RawTmq {}
    unsafe impl Sync for RawTmq {}

    impl RawTmq {
        pub(crate) fn new(
            api: Arc<ApiEntry>,
            tmq_api: Arc<TmqApi>,
            tmq_ptr: *mut tmq_t,
            timeout: i64,
        ) -> Self {
            let (sender, receiver) = flume::bounded(1);
            let supports_poll_null_check = api.ge_v3358();
            Self {
                api,
                tmq_api,
                tmq_ptr,
                timeout,
                sender,
                receiver: Some(receiver),
                thread_handle: None,
                stop_signal: Arc::new(AtomicBool::new(false)),
                supports_poll_null_check,
            }
        }

        fn as_ptr(&self) -> *mut tmq_t {
            self.tmq_ptr
        }

        pub(crate) fn subscribe(&mut self, topics: &Topics) -> RawResult<()> {
            let rsp = unsafe { (self.tmq_api.tmq_subscribe)(self.as_ptr(), topics.as_ptr()) };
            if rsp.is_err() {
                let str =
                    unsafe { CStr::from_ptr((self.tmq_api.tmq_err2str)(rsp)) }.to_string_lossy();
                return Err(taos_query::RawError::new(rsp.0, str));
            }
            Ok(())
        }

        pub fn err_as_str(&self, tmq_resp: tmq_resp_err_t) -> String {
            unsafe {
                CStr::from_ptr((self.tmq_api.tmq_err2str)(tmq_resp))
                    .to_string_lossy()
                    .to_string()
            }
        }

        pub fn subscription(&self) -> Topics {
            let tl = Topics::new(self.tmq_api.list_api);
            unsafe { (self.tmq_api.tmq_subscription)(self.as_ptr(), &mut tl.as_ptr()) }
                .ok_or("get topic list failed")
                .expect("get topic should always success");
            tl
        }

        pub fn commit_sync(&self, msg: RawRes) -> RawResult<()> {
            tracing::debug!("commit sync with {:?}", msg);
            unsafe { (self.tmq_api.tmq_commit_sync)(self.as_ptr(), msg.as_ptr() as _) }
                .ok_or("commit failed")
        }

        pub fn commit_offset_sync(
            &self,
            topic_name: &str,
            vgroup_id: VGroupId,
            offset: i64,
        ) -> RawResult<()> {
            if let Some(tmq_commit_offset_sync) = self.tmq_api.tmq_commit_offset_sync {
                unsafe {
                    tmq_commit_offset_sync(
                        self.as_ptr(),
                        topic_name.into_c_str().as_ptr(),
                        vgroup_id,
                        offset,
                    )
                    .ok_or("commit failed")
                }
            } else {
                unimplemented!("does not support tmq_commit_offset_sync");
            }
        }

        pub async fn commit(&self, msg: RawRes) -> RawResult<()> {
            use std::sync::mpsc::{channel, Sender};
            let (sender, rx) = channel::<RawResult<()>>();
            unsafe extern "C" fn tmq_commit_async_cb(
                _tmq: *mut tmq_t,
                resp: tmq_resp_err_t,
                param: *mut std::os::raw::c_void,
            ) {
                let offsets = resp.ok_or("commit failed").map(|_| ());
                let sender = param as *mut Sender<_>;
                let sender = Box::from_raw(sender);
                tracing::trace!("commit async callback");
                sender.send(offsets).unwrap();
            }

            unsafe {
                tracing::trace!("commit async with {:p}", msg.as_ptr());
                (self.tmq_api.tmq_commit_async)(
                    self.as_ptr(),
                    msg.as_ptr(),
                    tmq_commit_async_cb,
                    Box::into_raw(Box::new(sender)) as *mut _,
                );
            }
            rx.recv().unwrap()
        }

        pub async fn commit_offset_async(
            &self,
            topic_name: &str,
            vgroup_id: VGroupId,
            offset: i64,
        ) -> RawResult<()> {
            if let Some(tmq_commit_offset_async) = self.tmq_api.tmq_commit_offset_async {
                use std::sync::mpsc::{channel, Sender};
                let (sender, rx) = channel::<RawResult<()>>();
                unsafe extern "C" fn tmq_commit_offset_async_cb(
                    _tmq: *mut tmq_t,
                    resp: tmq_resp_err_t,
                    param: *mut std::os::raw::c_void,
                ) {
                    let offsets = resp.ok_or("commit offset failed").map(|_| ());
                    let sender = param as *mut Sender<_>;
                    let sender = Box::from_raw(sender);
                    tracing::trace!("commit offset async callback");
                    sender.send(offsets).unwrap();
                }

                unsafe {
                    tracing::trace!("commit offset async with {:p}", self.as_ptr());
                    (tmq_commit_offset_async)(
                        self.as_ptr(),
                        topic_name.into_c_str().as_ptr(),
                        vgroup_id,
                        offset,
                        tmq_commit_offset_async_cb,
                        Box::into_raw(Box::new(sender)) as *mut _,
                    );
                }
                rx.recv().unwrap()
            } else {
                unimplemented!("does not support tmq_commit_offset_async");
            }
        }

        pub fn poll_timeout(&self, timeout: i64) -> RawResult<Option<RawRes>> {
            tracing::trace!("poll next message with timeout {}", timeout);
            let res = unsafe { (self.tmq_api.tmq_consumer_poll)(self.as_ptr(), timeout) };
            if res.is_null() {
                if self.supports_poll_null_check {
                    self.api.check(res)?;
                }
                Ok(None)
            } else {
                Ok(Some(unsafe {
                    RawRes::from_ptr_unchecked(self.api.clone(), res)
                }))
            }
        }

        pub fn unsubscribe(&mut self) {
            unsafe {
                (self.tmq_api.tmq_unsubscribe)(self.as_ptr());
            }

            self.stop_signal.store(true, Ordering::Relaxed);

            if let Some(handle) = self.thread_handle.take() {
                let tmq_api = self.tmq_api.clone();
                let safe_tmq = SafeTmqT(self.as_ptr());

                std::thread::spawn(move || {
                    tracing::trace!("Waiting for worker thread to finish");
                    if let Err(err) = handle.join() {
                        tracing::error!("Failed to join worker thread: {err:?}");
                    }

                    let safe_tmq = safe_tmq;
                    unsafe {
                        (tmq_api.tmq_consumer_close)(safe_tmq.0);
                    }
                    tracing::trace!("tmq consumer closed successfully");
                });
            }
        }

        pub fn get_topic_assignment(&self, topic_name: &str) -> Vec<Assignment> {
            if let Some(tmq_get_topic_assignment) = self.tmq_api.tmq_get_topic_assignment {
                let mut num: i32 = 0;
                let mut pt: *mut Assignment = std::ptr::null_mut();

                let tmq_resp = unsafe {
                    tmq_get_topic_assignment(
                        self.as_ptr(),
                        topic_name.into_c_str().as_ptr(),
                        &mut pt,
                        &mut num,
                    )
                };

                if tmq_resp.is_err() || num == 0 || pt.is_null() {
                    return vec![];
                }

                let assignments = unsafe { std::slice::from_raw_parts(pt, num as usize).to_vec() };
                unsafe {
                    self.tmq_api
                        .tmq_free_assignment
                        .expect("tmq_free_assignment not found")(pt);
                };
                assignments
            } else {
                vec![]
            }
        }

        pub fn offset_seek(
            &mut self,
            topic_name: &str,
            vgroup_id: VGroupId,
            offset: i64,
        ) -> RawResult<()> {
            let tmq_resp;
            if let Some(tmq_offset_seek) = self.tmq_api.tmq_offset_seek {
                tmq_resp = unsafe {
                    tmq_offset_seek(
                        self.as_ptr(),
                        topic_name.into_c_str().as_ptr(),
                        vgroup_id,
                        offset,
                    )
                };
            } else {
                // unimplemented!("does not support tmq_offset_seek")
                return Ok(());
            }
            tracing::trace!(
                "offset_seek tmq_resp: {:?}, topic_name: {}, vgroup_id: {}, offset: {}",
                tmq_resp,
                topic_name,
                vgroup_id,
                offset
            );

            let err_str = self.err_as_str(tmq_resp);
            tracing::trace!("offset_seek tmq_resp as str: {}", err_str);

            tmq_resp.ok_or(format!("offset seek failed: {err_str}"))
        }

        pub fn committed(&self, topic_name: &str, vgroup_id: VGroupId) -> RawResult<i64> {
            let tmq_resp;
            if let Some(tmq_committed) = self.tmq_api.tmq_committed {
                tmq_resp = unsafe {
                    tmq_committed(self.as_ptr(), topic_name.into_c_str().as_ptr(), vgroup_id)
                };
            } else {
                unimplemented!("does not support tmq_committed");
            }
            tracing::trace!(
                "committed tmq_resp: {:?}, topic_name: {}, vgroup_id: {}",
                tmq_resp,
                topic_name,
                vgroup_id
            );

            if tmq_resp.0 > 0 {
                Ok(tmq_resp.0 as _)
            } else {
                let err_str = self.err_as_str(tmq_resp);
                tracing::trace!("committed tmq_resp err string: {}", err_str);

                Err(RawError::new(
                    tmq_resp.0,
                    format!("get committed failed: {err_str}"),
                ))
            }
        }

        pub fn position(&self, topic_name: &str, vgroup_id: VGroupId) -> RawResult<i64> {
            let tmq_resp;
            if let Some(tmq_position) = self.tmq_api.tmq_position {
                tmq_resp = unsafe {
                    tmq_position(self.as_ptr(), topic_name.into_c_str().as_ptr(), vgroup_id)
                };
            } else {
                unimplemented!("does not support tmq_position");
            }
            tracing::trace!(
                "position tmq_resp: {:?}, topic_name: {}, vgroup_id: {}",
                tmq_resp,
                topic_name,
                vgroup_id
            );

            if tmq_resp.0 > 0 {
                Ok(tmq_resp.0 as _)
            } else {
                let err_str = self.err_as_str(tmq_resp);
                tracing::trace!("position tmq_resp err string: {}", err_str);
                Err(RawError::new(
                    tmq_resp.0,
                    format!("get position failed: {err_str}"),
                ))
            }
        }

        pub(crate) fn spawn_thread(&mut self) {
            if self.receiver.is_none() {
                return;
            }

            trace!("Spawning worker thread to call C func `tmq_consumer_poll`");

            let receiver = self.receiver.take().unwrap();
            let safe_tmq = SafeTmqT(self.as_ptr());
            let tmq_api = self.tmq_api.clone();
            let api = self.api.clone();
            let timeout = self.timeout;
            let stop_signal = self.stop_signal.clone();
            let supports_poll_null_check = self.supports_poll_null_check;

            let handle = std::thread::spawn(move || {
                let safe_tmq = safe_tmq;
                let mut cache = None;

                while !stop_signal.load(Ordering::Relaxed) {
                    match receiver.recv() {
                        Ok(sender) => {
                            if let Some(res) = cache.take() {
                                if let Err(res) = sender.send(Ok(Some(res))) {
                                    trace!("Receiver has been closed, cached res: {res:?}");
                                    cache = res.unwrap();
                                }
                                trace!("Using cache, poll next message.");
                                continue;
                            }

                            let mut timeout = timeout;
                            let mut poll_res = std::ptr::null_mut();
                            let mut val = Ok(None);

                            while timeout > 0 {
                                let time = timeout.min(2000);
                                timeout -= time;

                                trace!(
                                    "Calling C func `tmq_consumer_poll` with ptr: {:?}, timeout: {}",
                                    safe_tmq.0,
                                    time
                                );
                                let now = std::time::Instant::now();
                                let res = unsafe { (tmq_api.tmq_consumer_poll)(safe_tmq.0, time) };
                                let elapsed = now.elapsed();
                                trace!(
                                    tmq.poll.elapsed.ms = elapsed.as_millis(),
                                    "C func `tmq_consumer_poll` returned a ptr: {res:?}"
                                );

                                if !res.is_null() {
                                    poll_res = res;
                                    break;
                                }

                                if supports_poll_null_check {
                                    if let Err(err) = api.check(res) {
                                        val = Err(err);
                                        break;
                                    }
                                }
                            }

                            if poll_res.is_null() {
                                if sender.send(val).is_err() {
                                    trace!("Receiver has been closed");
                                }
                                continue;
                            }

                            let res = unsafe { RawRes::from_ptr_unchecked(api.clone(), poll_res) };
                            if let Err(res) = sender.send(Ok(Some(res))) {
                                trace!("Receiver has been closed, cached res: {res:?}");
                                cache = res.unwrap();
                            }
                        }
                        Err(_) => {
                            trace!("Sender has been closed, exit thread.");
                            break;
                        }
                    };
                }

                trace!("Worker thread stopped.");
            });

            self.thread_handle = Some(handle);
        }

        pub(crate) fn tmq(&self) -> Arc<TmqApi> {
            self.tmq_api.clone()
        }

        pub(crate) fn sender(&self) -> &flume::Sender<oneshot::Sender<RawResult<Option<RawRes>>>> {
            &self.sender
        }
    }

    impl Drop for RawTmq {
        fn drop(&mut self) {
            self.unsubscribe();
        }
    }
}

pub(super) mod conf {
    use std::ffi::c_void;

    use taos_query::{value_is_true, Dsn};
    use types::tmq_resp_err_t;

    use crate::raw::TmqConfApi;
    use crate::types::{tmq_conf_t, tmq_t};
    use crate::*;

    #[derive(Debug)]
    struct Settings {
        experimental_snapshot_enable: bool,
        msg_enable_batchmeta: bool,
        with_table_name: bool,
        enable_auto_commit: bool,
        auto_commit_interval_ms: Option<i32>,
    }

    impl Default for Settings {
        fn default() -> Self {
            Self {
                experimental_snapshot_enable: false,
                msg_enable_batchmeta: true,
                with_table_name: true,
                enable_auto_commit: false,
                auto_commit_interval_ms: None,
            }
        }
    }

    const TMQ_CONF_AUTO_COMMIT_INTERVAL_MS: &str = "auto.commit.interval.ms";
    const TMQ_CONF_ENABLE_AUTO_COMMIT: &str = "enable.auto.commit";
    const TMQ_CONF_EXPERIMENTAL_SNAPSHOT_ENABLE: &str = "experimental.snapshot.enable";
    const TMQ_CONF_MSG_ENABLE_BATCHMETA: &str = "msg.enable.batchmeta";
    const TMQ_CONF_WITH_TABLE_NAME: &str = "msg.with.table.name";

    const TMQ_CONF_VALUE_TRUE: &str = "true";
    const TMQ_CONF_VALUE_FALSE: &str = "false";
    const TMQ_CONF_VALUE_ONE: &str = "1";
    const TMQ_CONF_VALUE_ZERO: &str = "0";
    const TMQ_CONF_MAX_AUTO_COMMIT_INTERVAL_MS: &str = "2147483647";
    const TMQ_CONF_DEFAULT_AUTO_COMMIT_INTERVAL_MS: &str = "5000";

    /* tmq conf */
    #[derive(Debug)]
    pub struct Conf {
        api: TmqConfApi,
        ptr: *mut tmq_conf_t,
        settings: Settings,
    }

    impl Conf {
        pub(crate) fn as_ptr(&self) -> *mut tmq_conf_t {
            self.ptr
        }

        pub(crate) fn new(api: TmqConfApi) -> Self {
            Self {
                api,
                ptr: unsafe { api.new_conf() },
                settings: Settings::default(),
            }
        }

        pub(crate) fn from_dsn(dsn: &Dsn, api: TmqConfApi) -> RawResult<Self> {
            let conf = Self::new(api);
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

            _set_opt!(username, "user");
            _set_opt!(password, "pass");
            _set_opt!(subject, "db");

            if let Some(addr) = dsn.addresses.first() {
                if let Some(host) = addr.host.as_ref() {
                    conf.set("td.connect.ip", host)?;
                }
                if let Some(port) = addr.port.as_ref() {
                    conf.set("td.connect.port", format!("{port}"))?;
                }
            }

            // todo: do explicitly param name filter.
            conf.with(dsn.params.iter().filter(|(k, _)| k.contains('.')))
        }

        pub(crate) fn with<K: AsRef<str>, V: AsRef<str>>(
            mut self,
            iter: impl Iterator<Item = (K, V)>,
        ) -> RawResult<Self> {
            for (k, v) in iter {
                match k.as_ref() {
                    "auto.commit.interval.ms" => {
                        self.settings.auto_commit_interval_ms =
                            v.as_ref().parse().ok().filter(|&x| x > 0);
                    }
                    "msg.enable.batchmeta" => {
                        self.settings.msg_enable_batchmeta = value_is_true(v.as_ref());
                    }
                    "enable.auto.commit" => {
                        self.settings.enable_auto_commit = value_is_true(v.as_ref());
                    }
                    "experimental.snapshot.enable" => {
                        self.settings.experimental_snapshot_enable = value_is_true(v.as_ref());
                    }
                    "msg.with.table.name" => {
                        self.settings.with_table_name = value_is_true(v.as_ref());
                    }
                    _ => {
                        self.set(k, v)?;
                    }
                };
            }
            Ok(self)
        }

        fn set<K: AsRef<str>, V: AsRef<str>>(&self, key: K, value: V) -> RawResult<&Self> {
            tracing::info!(
                "set {}={}",
                key.as_ref(),
                match key.as_ref() {
                    "td.connect.pass" | "td.connect.token" => "[REDACTED]",
                    _ => value.as_ref(),
                }
            );
            unsafe {
                self.api
                    .set_conf(self.as_ptr(), key.as_ref(), value.as_ref())
            }
            .map(|_| self)
        }

        pub(crate) fn build(&self) -> RawResult<*mut tmq_t> {
            if self.settings.enable_auto_commit {
                self.set(TMQ_CONF_ENABLE_AUTO_COMMIT, TMQ_CONF_VALUE_TRUE)?;
                if let Some(ms) = self.settings.auto_commit_interval_ms {
                    self.set(TMQ_CONF_AUTO_COMMIT_INTERVAL_MS, ms.to_string())?;
                } else {
                    self.set(
                        TMQ_CONF_AUTO_COMMIT_INTERVAL_MS,
                        TMQ_CONF_DEFAULT_AUTO_COMMIT_INTERVAL_MS,
                    )?;
                }
                // Safety: auto commit callback is called by C function.
                #[no_mangle]
                unsafe extern "C" fn auto_commit_callback_by_rust(
                    _tmq: *mut tmq_t,
                    resp: tmq_resp_err_t,
                    _param: *mut c_void,
                ) {
                    tracing::trace!(ok = resp.is_ok(), "auto commit callback is called");
                }
                unsafe {
                    self.api.auto_commit_cb(
                        self.as_ptr(),
                        auto_commit_callback_by_rust,
                        std::ptr::null_mut(),
                    );
                }
            } else {
                self.set(TMQ_CONF_ENABLE_AUTO_COMMIT, TMQ_CONF_VALUE_FALSE)?;
                self.set(
                    TMQ_CONF_AUTO_COMMIT_INTERVAL_MS,
                    TMQ_CONF_MAX_AUTO_COMMIT_INTERVAL_MS,
                )?;
            }
            if self.settings.experimental_snapshot_enable {
                self.set(TMQ_CONF_EXPERIMENTAL_SNAPSHOT_ENABLE, TMQ_CONF_VALUE_TRUE)?;
            } else {
                self.set(TMQ_CONF_EXPERIMENTAL_SNAPSHOT_ENABLE, TMQ_CONF_VALUE_FALSE)?;
            }
            if self.settings.msg_enable_batchmeta {
                self.set(TMQ_CONF_MSG_ENABLE_BATCHMETA, TMQ_CONF_VALUE_ONE)?;
            } else {
                self.set(TMQ_CONF_MSG_ENABLE_BATCHMETA, TMQ_CONF_VALUE_ZERO)?;
            }
            if self.settings.with_table_name {
                self.set(TMQ_CONF_WITH_TABLE_NAME, TMQ_CONF_VALUE_TRUE)?;
            } else {
                self.set(TMQ_CONF_WITH_TABLE_NAME, TMQ_CONF_VALUE_FALSE)?;
            }

            unsafe { self.api.new_consumer(self.as_ptr()) }
        }
    }

    impl Drop for Conf {
        fn drop(&mut self) {
            unsafe { self.api.destroy_conf(self.as_ptr()) };
        }
    }
}

pub(super) mod list {
    use std::ffi::CStr;
    use std::os::raw::c_char;

    use taos_query::prelude::RawResult;

    use crate::into_c_str::IntoCStr;
    use crate::raw::TmqListApi;
    use crate::types::tmq_list_t;

    #[derive(Debug)]
    pub struct Topics {
        api: TmqListApi,
        ptr: *mut tmq_list_t,
    }

    impl Topics {
        pub(super) fn as_ptr(&self) -> *mut tmq_list_t {
            self.ptr
        }

        pub(crate) fn new(api: TmqListApi) -> Self {
            Self {
                api,
                ptr: unsafe { api.new_list() },
            }
        }

        pub(crate) fn from_topics<'a, T: IntoCStr<'a>>(
            api: TmqListApi,
            topics: impl IntoIterator<Item = T>,
        ) -> RawResult<Self> {
            let ptr = unsafe { api.new_list_from_cstr(topics)? };
            Ok(Self { api, ptr })
        }

        pub fn iter(&self) -> Iter<'_> {
            let inner = self.api.as_c_str_slice(self.as_ptr());
            let len = inner.len();
            Iter {
                len,
                inner,
                index: 0,
            }
        }

        pub fn to_strings(&self) -> Vec<String> {
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

    pub struct Iter<'a> {
        inner: &'a [*mut c_char],
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
                .inspect(|_| {
                    self.index += 1;
                })
        }
    }

    impl ExactSizeIterator for Iter<'_> {
        fn len(&self) -> usize {
            self.len.saturating_sub(self.index)
        }
    }

    // todo: tmq_list_destroy cause double free error.
    impl Drop for Topics {
        fn drop(&mut self) {
            unsafe {
                self.api.destroy_list(self.as_ptr());
            }
        }
    }
}
