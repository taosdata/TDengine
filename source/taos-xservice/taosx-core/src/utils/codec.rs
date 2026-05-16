use anyhow::Context;

const COMPRESS_NONE: &str = "none";
const COMPRESS_GZIP: &str = "gzip";
const COMPRESS_SNAPPY: &str = "snappy";
const COMPRESS_LZ4: &str = "lz4";
const COMPRESS_ZSTD: &str = "zstd";

const ENCODING_UTF8: &str = "UTF_8";
const ENCODING_GBK: &str = "GBK";
const ENCODING_GB18030: &str = "GB18030";
const ENCODING_BIG5: &str = "BIG5";

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Unsupported compression type: {s}"))]
    UnsupportedCompression { s: String },
    #[snafu(display("Unsupported string encoding type: {s}"))]
    UnsupportedEncoding { s: String },
}

pub trait Processor {
    fn process(&self, src: Vec<u8>) -> anyhow::Result<Vec<u8>>;
}

impl<L, R> Processor for (L, R)
where
    L: Processor,
    R: Processor,
{
    fn process(&self, src: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let left = self.0.process(src)?;
        self.1.process(left)
    }
}

impl<T> Processor for Option<T>
where
    T: Processor,
{
    fn process(&self, src: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        match self {
            Some(processor) => processor.process(src),
            None => Ok(src),
        }
    }
}

impl Processor for () {
    fn process(&self, src: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        Ok(src)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Decompressor {
    Noop,
    Gzip,
    Lz4,
    Snappy,
    Zstd,
}

impl std::str::FromStr for Decompressor {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            COMPRESS_NONE => Ok(Self::Noop),
            COMPRESS_GZIP => Ok(Self::Gzip),
            COMPRESS_LZ4 => Ok(Self::Lz4),
            COMPRESS_SNAPPY => Ok(Self::Snappy),
            COMPRESS_ZSTD => Ok(Self::Zstd),
            _ => UnsupportedCompressionSnafu { s }.fail(),
        }
    }
}

impl Processor for Decompressor {
    fn process(&self, src: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let decompressor: &mut dyn std::io::Read = match self {
            Decompressor::Noop => return Ok(src),
            Decompressor::Gzip => &mut flate2::read::GzDecoder::new(&src[..]),
            Decompressor::Lz4 => {
                &mut lz4::Decoder::new(&src[..]).context("init lz4 deocoder error")?
            }
            Decompressor::Snappy => &mut snap::read::FrameDecoder::new(&src[..]),
            Decompressor::Zstd => {
                &mut zstd::Decoder::new(&src[..]).context("init zstd decoder error")?
            }
        };

        let mut dest = Vec::new();
        decompressor
            .read_to_end(&mut dest)
            .context("data decompress error")?;
        Ok(dest)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StringDecoder {
    Utf8,
    GBK,
    GB18030,
    BIG5,
}

impl std::str::FromStr for StringDecoder {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ENCODING_UTF8 => Ok(Self::Utf8),
            ENCODING_GBK => Ok(Self::GBK),
            ENCODING_GB18030 => Ok(Self::GB18030),
            ENCODING_BIG5 => Ok(Self::BIG5),
            _ => UnsupportedEncodingSnafu { s }.fail(),
        }
    }
}

impl Processor for StringDecoder {
    fn process(&self, src: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let encoding = match self {
            StringDecoder::Utf8 => return Ok(src),
            StringDecoder::GBK => encoding_rs::GBK,
            StringDecoder::GB18030 => encoding_rs::GB18030,
            StringDecoder::BIG5 => encoding_rs::BIG5,
        };
        let (res, _, has_error) = encoding.decode(&src);
        if has_error {
            anyhow::bail!("raw message parse error")
        }
        Ok(res.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use base64::{Engine, engine::general_purpose};

    use super::*;

    #[test]
    fn decompress_test() -> anyhow::Result<()> {
        let decompressor: Decompressor = COMPRESS_NONE.parse()?;
        let res = decompressor.process(b"hello, world!".to_vec())?;
        assert_eq!(&res, b"hello, world!");

        // 动态霍夫曼
        decompress(
            COMPRESS_GZIP,
            "H4sIAAAAAAAAA8tIzcnJ11Eozy/KSVEEABONmFgNAAAA",
        )?;
        // 静态霍夫曼
        decompress(
            COMPRESS_GZIP,
            "H4sIAEyUNWcA/wWAoQkAAAgEV9HuWAqGgweL6z87oIrX0WkTjZhYDQAAAA==",
        )?;
        // 无压缩
        decompress(
            COMPRESS_GZIP,
            "H4sIAKeUNWcA/wENAPL/aGVsbG8sIHdvcmxkIRONmFgNAAAA",
        )?;

        // lz4
        decompress(COMPRESS_LZ4, "BCJNGEBw3w0AAIBoZWxsbywgd29ybGQhAAAAAA==")?;

        // snappy
        decompress(
            COMPRESS_SNAPPY,
            "/wYAAHNOYVBwWQERAAD2h0obaGVsbG8sIHdvcmxkIQ==",
        )?;

        // zstd
        decompress(COMPRESS_ZSTD, "KLUv/QRYaQAAaGVsbG8sIHdvcmxkIZ4lnmk=")?;
        Ok(())
    }

    fn decompress(name: &str, payload: &str) -> anyhow::Result<()> {
        let b = general_purpose::STANDARD.decode(payload)?;
        let decompressor: Decompressor = name.parse()?;
        let res = decompressor.process(b)?;
        assert_eq!(&res, b"hello, world!");
        Ok(())
    }

    #[test]
    fn decode_gbk_test() -> anyhow::Result<()> {
        decode(ENCODING_GBK, "xOO6w6OsysC956Oh")?;
        decode(ENCODING_BIG5, "p0GmbqFBpUCsyaFJ")?;
        decode(ENCODING_GB18030, "xOO6w6OsysC956Oh")?;
        Ok(())
    }

    #[test]
    fn unsupported_decompressor_fails() {
        assert!("bad-compress".parse::<Decompressor>().is_err());
    }

    #[test]
    fn unsupported_string_decoder_fails() {
        assert!("BAD_ENCODING".parse::<StringDecoder>().is_err());
    }

    #[test]
    fn processor_chain_runs_in_sequence() {
        let pipeline = (Some(Decompressor::Noop), Some(StringDecoder::Utf8));
        let res = pipeline.process(b"data".to_vec()).unwrap();
        assert_eq!(res, b"data");
    }

    fn decode(name: &str, payload: &str) -> anyhow::Result<()> {
        let b = general_purpose::STANDARD.decode(payload)?;
        let decoder: StringDecoder = name.parse()?;
        let res = decoder.process(b)?;
        assert_eq!(&res, "你好，世界！".as_bytes());
        Ok(())
    }

    #[test]
    fn parse_decompress_test() -> anyhow::Result<()> {
        assert!(StringDecoder::from_str("abc").is_err());
        assert!(Decompressor::from_str("abc").is_err());
        Ok(())
    }
}
