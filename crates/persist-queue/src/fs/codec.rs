use std::{marker::PhantomData, path::PathBuf, slice::Iter};

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    BadChecksumSnafu, EmptyPayloadSnafu, Entry, InvalidPayloadLengthBytesSnafu, PayloadTooLongSnafu,
};

use super::EntryPosition;

type Result<T> = std::result::Result<T, crate::Error>;

pub(crate) struct WriteCodec<B> {
    pub(crate) path: PathBuf,
    pub(crate) position: EntryPosition,
    _p: PhantomData<B>,
}

impl<B> WriteCodec<B> {
    pub(crate) fn new(path: PathBuf, position: EntryPosition) -> Self {
        Self {
            path,
            position,
            _p: PhantomData,
        }
    }
}

impl<B> Encoder<B> for WriteCodec<B>
where
    B: AsRef<[u8]>,
{
    type Error = crate::Error;

    fn encode(&mut self, item: B, dst: &mut bytes::BytesMut) -> Result<()> {
        let item = item.as_ref();
        let len = item.len();

        let mut buf = BytesMut::with_capacity(4 + len_len(len));

        let crc = crc32fast::hash(item);
        buf.put_u32(crc);

        encode_payload_len(&mut buf, len)?;

        self.position.advance((buf.len() + item.len()) as u64);

        dst.extend(buf);
        dst.extend(item);

        Ok(())
    }
}

fn len_len(len: usize) -> usize {
    if len >= 2_097_152 {
        4
    } else if len >= 16_384 {
        3
    } else if len >= 128 {
        2
    } else {
        1
    }
}

fn encode_payload_len(buf: &mut BytesMut, len: usize) -> Result<()> {
    if len == 0 {
        return EmptyPayloadSnafu.fail();
    }
    if len > 268_435_455 {
        return PayloadTooLongSnafu { len }.fail();
    }
    let mut x = len;
    loop {
        let mut byte = (x % 128) as u8;
        x /= 128;
        if x > 0 {
            byte |= 128;
        }
        buf.put_u8(byte);
        if x == 0 {
            break;
        }
    }

    Ok(())
}

#[derive(Default)]
struct BufEntry {
    crc: Option<u32>,
    len: Option<(usize, usize)>,
}

pub(crate) struct ReadCodec {
    pub(crate) position: EntryPosition,
    buf_entry: BufEntry,
}

impl ReadCodec {
    pub(crate) fn new(position: EntryPosition) -> Self {
        Self {
            position,
            buf_entry: BufEntry::default(),
        }
    }
}

impl Decoder for ReadCodec {
    type Item = Entry<EntryPosition>;

    type Error = crate::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>> {
        let crc = match self.buf_entry.crc {
            Some(crc) => crc,
            None => {
                if src.remaining() < 4 {
                    return Ok(None);
                }
                let crc = src.get_u32();
                self.buf_entry.crc = Some(crc);
                crc
            }
        };

        let (len, len_len) = match self.buf_entry.len {
            Some(len) => len,
            None => match decode_payload_len(src.iter())? {
                Some((len, len_len)) => {
                    self.buf_entry.len = Some((len, len_len));
                    src.advance(len_len);
                    (len, len_len)
                }
                None => return Ok(None),
            },
        };

        if src.remaining() < len {
            src.reserve(len - src.remaining());
            return Ok(None);
        }

        let payload = src.split_to(len);
        self.buf_entry = BufEntry::default();

        snafu::ensure!(crc == crc32fast::hash(&payload), BadChecksumSnafu);

        self.position.advance((4 + len_len + len) as u64);

        Ok(Some(Entry {
            position: self.position,
            payload: payload.freeze(),
        }))
    }

    fn decode_eof(
        &mut self,
        buf: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        self.decode(buf)
    }
}

fn decode_payload_len(stream: Iter<u8>) -> Result<Option<(usize, usize)>> {
    let mut len: usize = 0;
    let mut len_len: usize = 0;
    let mut done = false;
    let mut shift = 0;
    for byte in stream {
        len_len += 1;
        let byte = *byte as usize;
        len += (byte & 0x7F) << shift;
        done = (byte & 0x80) == 0;
        if done {
            break;
        }

        shift += 7;

        snafu::ensure!(shift <= 21, InvalidPayloadLengthBytesSnafu);
    }

    if !done {
        return Ok(None);
    }

    Ok(Some((len, len_len)))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn payload_len_test() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        // 0 bytes len
        assert!(encode_payload_len(&mut buf, 0).is_err());
        // 1 bytes len
        encode_payload_len(&mut buf, 5)?;
        assert_eq!(buf.len(), len_len(5));
        assert_eq!(decode_payload_len(buf.iter())?, Some((5, len_len(5))));
        // 2 bytes len
        buf.clear();
        encode_payload_len(&mut buf, 130)?;
        assert_eq!(buf.len(), len_len(130));
        assert_eq!(decode_payload_len(buf.iter())?, Some((130, len_len(130))));
        // 3 bytes len
        buf.clear();
        encode_payload_len(&mut buf, 17_000)?;
        assert_eq!(buf.len(), len_len(17_000));
        assert_eq!(
            decode_payload_len(buf.iter())?,
            Some((17_000, len_len(17_000)))
        );
        // 4 bytes len
        buf.clear();
        encode_payload_len(&mut buf, 268_000_455)?;
        assert_eq!(buf.len(), len_len(268_000_455));
        assert_eq!(
            decode_payload_len(buf.iter())?,
            Some((268_000_455, len_len(268_000_455)))
        );
        // too long
        assert!(encode_payload_len(&mut buf, 269_435_455).is_err());
        Ok(())
    }

    #[test]
    fn codec_test() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();

        let mut encoder = WriteCodec::new(PathBuf::new(), EntryPosition::default());
        let mut decoder = ReadCodec::new(EntryPosition::default());

        encoder.encode("hello, world!".as_bytes(), &mut buf)?;
        assert_eq!(
            decoder.decode(&mut buf)?,
            Some(Entry {
                position: EntryPosition::new(0, 18),
                payload: Bytes::from_static("hello, world!".as_bytes())
            })
        );

        // crc test
        buf.clear();
        let payload = "hello, world!".repeat(50);
        encoder.encode(payload.as_bytes(), &mut buf)?;
        buf[40] = 98;
        assert!(decoder.decode(&mut buf).is_err());

        Ok(())
    }

    #[test]
    fn decode_part_test() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();

        let mut encoder = WriteCodec::new(PathBuf::new(), EntryPosition::default());
        let mut decoder = ReadCodec::new(EntryPosition::default());

        // 650 bytes payload
        let payload = {
            let payload = "hello, world!".repeat(50);
            Bytes::copy_from_slice(payload.as_bytes())
        };
        encoder.encode(payload.clone(), &mut buf)?;
        assert_eq!(buf.len(), 4 + 2 + 650);
        assert_eq!(
            decoder.decode(&mut BytesMut::from(&buf[..]))?,
            Some(Entry {
                position: EntryPosition::new(0, 656),
                payload: payload.clone()
            })
        );

        let mut decode_buf = BytesMut::new();
        let mut decoder = ReadCodec::new(EntryPosition::default());

        // part crc
        decode_buf.extend(buf.split_to(2));
        assert_eq!(decoder.decode(&mut decode_buf)?, None);
        assert!(decoder.buf_entry.crc.is_none());

        // crc + part len
        decode_buf.extend(buf.split_to(3));
        assert_eq!(decoder.decode(&mut decode_buf)?, None);
        assert!(decoder.buf_entry.crc.is_some());
        assert!(decoder.buf_entry.len.is_none());

        // crc + len + part payload
        decode_buf.extend(buf.split_to(4));
        let len = payload.len();
        assert_eq!(decoder.decode(&mut decode_buf)?, None);
        assert!(decoder.buf_entry.crc.is_some());
        assert_eq!(decoder.buf_entry.len, Some((len, len_len(len))));

        // crc + len + payload
        decode_buf.extend(buf);
        assert_eq!(
            decoder.decode(&mut decode_buf)?,
            Some(Entry {
                position: EntryPosition::new(0, 656),
                payload
            })
        );
        assert!(decoder.buf_entry.crc.is_none());
        assert!(decoder.buf_entry.len.is_none());

        Ok(())
    }
}
