use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub fn next_req_id() -> u64 {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as u64;

    // [timestamp:42位][counter:22位]
    static LAST_STATE: AtomicU64 = AtomicU64::new(0);

    let new_base = timestamp << 22;
    LAST_STATE.fetch_max(new_base, Ordering::Relaxed);

    // 递增counter并返回
    LAST_STATE.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn req_id_test() {
        let mut set = HashSet::with_capacity(100000);
        let mut id = None;
        for _ in 0..100000 {
            let new_id = next_req_id();
            assert!(Some(new_id) > id);
            set.insert(new_id);
            id = Some(new_id);
        }
        assert_eq!(set.len(), 100000);
    }
}
