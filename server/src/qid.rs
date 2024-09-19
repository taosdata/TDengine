use std::sync::{
    atomic::{self, AtomicU32},
    OnceLock,
};

use bitfield::bitfield;
use http::HeaderMap;
use taoslog::{
    utils::{QidMetadataSetter, Span},
    QidManager,
};

pub(crate) static INSTANCE_ID: OnceLock<u8> = OnceLock::new();
pub(crate) const DEFAULT_INSTANCE_ID: u8 = 1;

static SESSION_ID: OnceLock<AtomicU32> = OnceLock::new();

bitfield! {
    pub struct Qid(u64);

    u8, extension_id, set_extension_id: 7,0;
    u8, sequence_id, set_sequence_id: 15,8;
    u32, session_id, set_session_id: 47,16;
    u8, downstream_id, set_downstream_id: 55, 48;
    u8, instance_id, set_instance_id: 63, 56;
}

impl Qid {
    pub(crate) fn set_taosx(&mut self) {
        self.set_downstream_id(1);
    }

    pub(crate) fn set_taos(&mut self) {
        self.set_downstream_id(2);
    }

    pub(crate) fn set_cloud(&mut self) {
        self.set_downstream_id(3);
    }

    pub(crate) fn add_sequence_id(&mut self) {
        self.set_sequence_id(self.sequence_id() + 1);
        Span.set_qid(self);
    }
}

impl Clone for Qid {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl QidManager for Qid {
    fn init() -> Self {
        let mut this = Self(0);
        this.set_instance_id(*INSTANCE_ID.get().unwrap());
        this
    }

    fn init_on_request(request: &actix_web::dev::ServiceRequest) -> Self {
        let mut qid = Self::init();

        let session_id = SESSION_ID
            .get_or_init(|| AtomicU32::new(0))
            .fetch_add(1, atomic::Ordering::Relaxed);
        qid.set_session_id(session_id);

        let path = request.path();
        if path.starts_with("/rest/")
            || path.starts_with("/api/-/password/")
            || path == "/api/-/license"
        {
            qid.set_taos();
            return qid;
        }
        if path.starts_with("/api/x/")
            || path == "/api/-/import"
            || path == "/api/-/profile"
            || path == "/api-doc/openapi.json"
        {
            qid.set_taosx();
            return qid;
        }

        if path == "/api/-/taosd-info" || path == "/api/-/verification-code" {
            qid.set_cloud();
        }

        qid
    }

    fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self(value)
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
        INSTANCE_ID.get_or_init(|| 1);

        let mut qid = Qid::init();
        assert_eq!(qid.get(), 0x0100000000000000);

        qid.set_taos();
        assert_eq!(qid.get(), 0x0102000000000000);

        qid.add_sequence_id();
        assert_eq!(qid.get(), 0x0102000000000100);

        qid.set_extension_id(1);
        assert_eq!(qid.get(), 0x0102000000000101);
    }
}
