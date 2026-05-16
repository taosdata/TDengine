use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use once_cell::sync::Lazy;

static HASH_ID: Lazy<u64> = Lazy::new(|| {
    let uuid = uuid::Uuid::new_v4().into_bytes();
    let hash_id = murmur_hash3_32(&uuid) as u64;
    (hash_id & 0x0FFF) << 52
});

static PID: Lazy<u64> = Lazy::new(|| {
    let pid = std::process::id() as u64;
    (pid & 0x0FFF) << 40
});

static SERIAL_NO: AtomicU64 = AtomicU64::new(0);

/// The request id is an unsigned integer format of 64bit.
///
/// ```text
/// +-------+-------+-----------+---------------+
/// | uuid  | pid   | timestamp | serial number |
/// +-------+-------+-----------+---------------+
/// | 12bit | 12bit | 24bit     | 16bit         |
/// +-------+-------+-----------+---------------+
/// ```
#[inline]
pub fn generate_req_id() -> u64 {
    let now = SystemTime::now();
    let ts = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let val = SERIAL_NO.fetch_add(1, Ordering::SeqCst);
    *HASH_ID | *PID | ((ts & 0xFFFFFF) << 16) | (val & 0xFFFF)
}

fn murmur_hash3_32(key: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut h1: u32 = 0x12345678;
    let mut blocks = key.chunks_exact(4);
    for block in blocks.by_ref() {
        assert!(block.len() == 4);
        let mut k1 = u32::from_le_bytes([block[0], block[1], block[2], block[3]]);
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);

        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5);
        h1 = h1.wrapping_add(0xe6546b64);
    }

    let mut k1 = 0u32;
    let tail = blocks.remainder();
    for (i, &b) in tail.iter().enumerate() {
        k1 ^= (b as u32) << (i * 8);
    }

    if k1 != 0 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= key.len() as u32;
    fmix32(h1)
}

fn fmix32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

#[cfg(test)]
mod tests {
    use super::murmur_hash3_32;

    #[test]
    fn test_murmur_hash3_32() {
        let a = "a".repeat(1000);
        let test_cases = vec![
            ("02c9f9bb-5da4-412e-94f3-ad36439073ce", 0x7bfed446),
            ("hello", 0xc7e66d96),
            ("hell", 0xc9212d1a),
            ("hel", 0x28568542),
            ("he", 0xd2f4855c),
            ("h", 0x894ac50a),
            ("", 0xe37cd1bc),
            ("中文", 0xafb2cd2a),
            (&a, 0x176450c7),
        ];

        for (key, expected) in test_cases {
            assert_eq!(murmur_hash3_32(key.as_bytes()), expected);
        }
    }
}
