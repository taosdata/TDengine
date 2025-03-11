use bytes::Bytes;

pub fn payloads(count: usize) -> Vec<Bytes> {
    std::iter::repeat_n("hello, world!", count)
        .map(|v| Bytes::from_static(v.as_bytes()))
        .collect()
}
