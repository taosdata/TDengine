use std::{io::Write, string::FromUtf8Error};

use snafu::{ResultExt, ensure};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Unsupported compression type: {s}"))]
    UnsupportedCompression { s: String },
    #[snafu(display("Unsupported encoding type: {s}"))]
    UnsupportedEncoding { s: String },
    #[snafu(display("Compress Write error: {source}"))]
    CompressWrite { source: std::io::Error },
    #[snafu(display("Build lz4 encoder error: {source}"))]
    BuildLz4Encoder { source: std::io::Error },
    #[snafu(display("Build zstd encoder error: {source}"))]
    BuildZstdEncoder { source: std::io::Error },
    #[snafu(display("Invalid utf8 string"))]
    InvalidUtf8 { source: FromUtf8Error },
    #[snafu(display("Encoding string error"))]
    EncodingString,
}

type Result<T> = std::result::Result<T, Error>;

pub trait Processor {
    fn process(&self, src: Vec<u8>) -> Result<Vec<u8>>;
}

impl<L, R> Processor for (L, R)
where
    L: Processor,
    R: Processor,
{
    fn process(&self, src: Vec<u8>) -> Result<Vec<u8>> {
        let left = self.0.process(src)?;
        self.1.process(left)
    }
}

impl<T> Processor for Option<T>
where
    T: Processor,
{
    fn process(&self, src: Vec<u8>) -> Result<Vec<u8>> {
        match self {
            Some(processor) => processor.process(src),
            None => Ok(src),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Compression {
    Gzip,
    Lz4,
    Snappy,
    Zstd,
}

impl std::str::FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "gzip" => Ok(Self::Gzip),
            "lz4" => Ok(Self::Lz4),
            "snappy" => Ok(Self::Snappy),
            "zstd" => Ok(Self::Zstd),
            s => UnsupportedCompressionSnafu { s }.fail(),
        }
    }
}

impl Processor for Compression {
    fn process(&self, src: Vec<u8>) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        {
            let writer: &mut dyn Write = match self {
                Compression::Gzip => {
                    &mut flate2::write::GzEncoder::new(&mut buf, flate2::Compression::best())
                }
                Compression::Lz4 => &mut lz4::EncoderBuilder::new()
                    .level(1)
                    .build(&mut buf)
                    .context(BuildLz4EncoderSnafu)?,
                Compression::Snappy => &mut snap::write::FrameEncoder::new(&mut buf),
                Compression::Zstd => &mut zstd::Encoder::new(&mut buf, 0)
                    .context(BuildZstdEncoderSnafu)?
                    .auto_finish(),
            };

            writer.write(&src).context(CompressWriteSnafu)?;
            writer.flush().context(CompressWriteSnafu)?;
        }
        Ok(buf)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Encoding {
    GBK,
    GB18030,
    BIG5,
}

impl std::str::FromStr for Encoding {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "gbk" => Ok(Self::GBK),
            "gb18030" => Ok(Self::GB18030),
            "big5" => Ok(Self::BIG5),
            s => UnsupportedEncodingSnafu { s }.fail(),
        }
    }
}

impl Processor for Encoding {
    fn process(&self, src: Vec<u8>) -> Result<Vec<u8>> {
        let encoding = match self {
            Encoding::GBK => encoding_rs::GBK,
            Encoding::GB18030 => encoding_rs::GB18030,
            Encoding::BIG5 => encoding_rs::BIG5,
        };

        let s = String::from_utf8(src).context(InvalidUtf8Snafu)?;
        let (res, _, has_err) = encoding.encode(&s);
        ensure!(!has_err, EncodingStringSnafu);
        Ok(res.to_vec())
    }
}
