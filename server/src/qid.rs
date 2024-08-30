use std::sync::{
    atomic::{self, AtomicU64},
    OnceLock,
};

use http::HeaderMap;
use taoslog::{utils::QidMetadataSetter, QidManager};

pub(crate) static INSTANCE_ID: OnceLock<u8> = OnceLock::new();
pub(crate) const DEFAULT_INSTANCE_ID: u8 = 1;

static SEQUENCE_ID: OnceLock<AtomicU64> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct Qid {
    instance_id: u8,
    downstream_id: u8,
    sequence_id: u64,
    extension_id: u8,
}

impl Qid {
    pub(crate) fn set_taosx(&mut self) {
        self.downstream_id = 1;
    }

    pub(crate) fn set_taos(&mut self) {
        self.downstream_id = 2;
    }

    pub(crate) fn set_cloud(&mut self) {
        self.downstream_id = 3;
    }

    pub(crate) fn add_sequence_id(&mut self) {
        let global = SEQUENCE_ID.get_or_init(|| AtomicU64::new(1));
        self.sequence_id = global.fetch_add(1, atomic::Ordering::SeqCst);
    }

    #[allow(unused)]
    pub(crate) fn set_extension_id(&mut self, extension_id: u8) {
        self.extension_id = extension_id;
    }
}

impl QidManager for Qid {
    fn init() -> Self {
        Self {
            instance_id: *INSTANCE_ID.get().unwrap(),
            downstream_id: 0,
            sequence_id: 0,
            extension_id: 0,
        }
    }

    fn get(&self) -> u64 {
        ((self.instance_id.to_le() as u64) << 56)
            | ((self.downstream_id.to_le() as u64) << 48)
            | (self.sequence_id.to_le() << 8)
            | (self.extension_id.to_le() as u64).to_le()
    }
}

impl From<u64> for Qid {
    fn from(value: u64) -> Self {
        Self {
            instance_id: ((value >> 56) & 0xFF) as u8,
            downstream_id: ((value >> 48) & 0xFF) as u8,
            sequence_id: ((value >> 8) & 0xFF),
            extension_id: ((value) & 0xFF) as u8,
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
