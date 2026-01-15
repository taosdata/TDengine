use std::sync::{
    OnceLock,
    atomic::{self, AtomicU64},
};

use bitfield::bitfield;
use http::HeaderMap;
use taoslog::{
    QidManager,
    utils::{QidMetadataSetter, Span},
};

pub(crate) static INSTANCE_ID: OnceLock<u8> = OnceLock::new();
pub(crate) const DEFAULT_INSTANCE_ID: u8 = 1;

static SESSION_ID: OnceLock<AtomicU64> = OnceLock::new();

bitfield! {
    struct QidInner(u64);

    u8, extension_id, set_extension_id: 7,0;
    u8, sequence_id, set_sequence_id: 15,8;
    u64, session_id, set_session_id: 55,16;
    u8, instance_id, set_instance_id: 63, 56;
}

pub struct Qid {
    inner: QidInner,
    first: bool,
}

impl Qid {
    pub(crate) fn add_sequence_id(&mut self) {
        if self.first {
            self.first = false;
            return;
        }
        self.inner.set_sequence_id(self.inner.sequence_id() + 1);
        Span.set_qid(self);
    }
}

impl Clone for Qid {
    fn clone(&self) -> Self {
        Self {
            inner: QidInner(self.inner.0),
            first: self.first,
        }
    }
}

impl QidManager for Qid {
    fn init() -> Self {
        let mut this = Self {
            inner: QidInner(0),
            first: true,
        };
        if let Some(id) = INSTANCE_ID.get() {
            this.inner.set_instance_id(*id);
        }
        this
    }

    fn init_on_request(_request: &actix_web::dev::ServiceRequest) -> Self {
        let mut qid = Self::init();

        let session_id = SESSION_ID
            .get_or_init(AtomicU64::default)
            .fetch_add(1, atomic::Ordering::Relaxed);
        qid.inner.set_session_id(session_id);

        qid
    }

    fn get(&self) -> u64 {
        self.inner.0
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self {
            inner: QidInner(value),
            first: true,
        }
    }
}

pub(crate) fn headers_with_qid(qid: &Qid) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.set_qid(qid);
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qid_test() {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
        let _guard = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .set_default();

        INSTANCE_ID.get_or_init(|| 1);

        let qid = Qid::from(0x0200000000000000);
        assert_eq!(qid.inner.instance_id(), 2);

        let mut qid = Qid::init();
        assert_eq!(qid.get(), 0x0100000000000000);

        qid.add_sequence_id();
        assert_eq!(qid.get(), 0x0100000000000000);

        qid.add_sequence_id();
        assert_eq!(qid.get(), 0x0100000000000100);

        qid.inner.set_extension_id(1);
        assert_eq!(qid.get(), 0x0100000000000101);

        qid.inner.set_session_id(1);
        assert_eq!(qid.get(), 0x0100000000010101)
    }

    #[test]
    fn test_on_request() {
        INSTANCE_ID.get_or_init(|| 1);

        let qid1 = Qid::init_on_request(&actix_web::test::TestRequest::default().to_srv_request());
        let qid2 = Qid::init_on_request(&actix_web::test::TestRequest::default().to_srv_request());

        assert_eq!(qid1.inner.instance_id(), 1);
        assert_eq!(qid2.inner.instance_id(), 1);
        assert_ne!(qid1.inner.session_id(), qid2.inner.session_id());
    }
}
