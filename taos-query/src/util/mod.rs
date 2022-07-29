mod inline_bytes;
mod inline_json;
mod inline_nchar;
mod inline_str;

mod inline_read;
mod inline_write;

use std::{
    collections::BTreeMap,
    io::{Read, Write},
};

use tokio::io::{AsyncRead, AsyncWrite};

pub use inline_bytes::InlineBytes;
pub use inline_json::InlineJson;
pub use inline_nchar::InlineNChar;
pub use inline_str::InlineStr;

pub use inline_read::AsyncInlinableRead;
pub use inline_write::AsyncInlinableWrite;

pub trait InlinableWrite: Write {
    #[inline]
    /// Write `usize` length as little endian `N` bytes.
    fn write_len_with_width<const N: usize>(&mut self, len: usize) -> std::io::Result<usize> {
        self.write(&len.to_le_bytes()[0..N])?;
        Ok(N)
    }

    #[inline]
    /// Write a [u8] value to writer.
    fn write_u8_le(&mut self, value: u8) -> std::io::Result<usize> {
        self.write(&[value])
    }

    #[inline]
    /// Write a [u16] value to writer.
    fn write_u16_le(&mut self, value: u16) -> std::io::Result<usize> {
        self.write(&value.to_le_bytes())
    }

    #[inline]
    /// Write a [u32] value to writer.
    fn write_u32_le(&mut self, value: u32) -> std::io::Result<usize> {
        self.write(&value.to_le_bytes())
    }

    #[inline]
    /// Write a [i64] value to writer.
    fn write_i64_le(&mut self, value: i64) -> std::io::Result<usize> {
        self.write(&value.to_le_bytes())
    }

    #[inline]
    /// Write a [u64] value to writer.
    fn write_u64_le(&mut self, value: u64) -> std::io::Result<usize> {
        self.write(&value.to_le_bytes())
    }

    #[inline]
    /// Write a [u128] value to writer.
    fn write_u128_le(&mut self, value: u128) -> std::io::Result<usize> {
        self.write(&value.to_le_bytes())
    }

    #[inline]
    /// Write inlined bytes to writer with specific length width `N`.
    ///
    /// The inlined bytes are constructed as:
    ///
    /// ```text
    /// +--------------+-----------------+
    /// | len: N bytes | data: len bytes |
    /// +--------------+-----------------+
    /// ```
    ///
    ///  ## Safety
    ///
    ///  Write inlined bytes may not be safe if the input bytes length overflows to the width.
    ///  For example, write `256` bytes with length width `1` is not safe.
    fn write_inlined_bytes<const N: usize>(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        assert_eq!(bytes.len() >> N * 8, 0);
        let l = self.write(&bytes.len().to_le_bytes()[0..N])?;
        self.write_all(bytes)?;
        Ok(l + bytes.len())
    }

    #[inline]
    /// Write inlined string with specific length width `N`.
    fn write_inlined_str<const N: usize>(&mut self, s: &str) -> std::io::Result<usize> {
        self.write_inlined_bytes::<N>(s.as_bytes())
    }

    #[inline]
    /// Write an inlinable object.
    fn write_inlinable<T: Inlinable>(&mut self, value: &T) -> std::io::Result<usize>
    where
        Self: Sized,
    {
        value.write_inlined(self)
    }
}

impl<T> InlinableWrite for T where T: Write {}

macro_rules! _impl_read_exact {
    ($ty: ty, $N: literal) => {
        paste::paste! {
            fn [<read_ $ty>](&mut self) -> std::io::Result<$ty> {
                let mut bytes = [0; $N];
                self.read_exact(&mut bytes)?;
                Ok($ty::from_le_bytes(bytes))
            }
        }
    };
}
pub trait InlinableRead: Read {
    #[inline]
    /// Read `N` bytes as `usize`.
    ///
    /// Only 1/2/4/8 is valid as `N`.
    fn read_len_with_width<const N: usize>(&mut self) -> std::io::Result<usize> {
        let mut bytes: [u8; N] = [0; N];
        self.read_exact(&mut bytes)?;
        let len = match N {
            1 => bytes[0] as usize,
            2 => unsafe { *std::mem::transmute::<*const u8, *const u16>(bytes.as_ptr()) as usize },
            4 => unsafe { *std::mem::transmute::<*const u8, *const u32>(bytes.as_ptr()) as usize },
            8 => unsafe { *std::mem::transmute::<*const u8, *const u64>(bytes.as_ptr()) as usize },
            _ => unreachable!(),
        };
        Ok(len)
    }

    fn read_f32(&mut self) -> std::io::Result<f32> {
        let mut bytes = [0; 4];
        self.read_exact(&mut bytes)?;
        Ok(f32::from_le_bytes(bytes))
    }

    fn read_f64(&mut self) -> std::io::Result<f64> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(f64::from_le_bytes(bytes))
    }

    fn read_u8(&mut self) -> std::io::Result<u8> {
        let mut bytes = [0; 1];
        self.read_exact(&mut bytes)?;
        Ok(u8::from_le_bytes(bytes))
    }
    fn read_u16(&mut self) -> std::io::Result<u16> {
        let mut bytes = [0; 2];
        self.read_exact(&mut bytes)?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> std::io::Result<u32> {
        let mut bytes = [0; 4];
        self.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> std::io::Result<u64> {
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_u128(&mut self) -> std::io::Result<u128> {
        let mut bytes = [0; 16];
        self.read_exact(&mut bytes)?;
        Ok(u128::from_le_bytes(bytes))
    }

    #[inline]
    /// Read a bytes `len` with width `N` and the next `len - N` bytes into a `Vec`。
    ///
    /// The bytes contains:
    ///
    /// ```text
    /// +--------------+-----------------+
    /// | len: N bytes | data: len - N bytes |
    /// +--------------+-----------------+
    /// ```
    ///
    fn read_len_with_data<const N: usize>(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_len_with_width::<N>()?;
        let mut buf = Vec::with_capacity(len);
        buf.extend(&(len as u64).to_le_bytes()[0..N]);
        buf.resize(len, 0);
        self.read_exact(&mut buf[N..])?;
        Ok(buf)
    }
    #[inline]
    /// Read inlined bytes with specific length width `N`.
    ///
    /// The inlined bytes are constructed as:
    ///
    /// ```text
    /// +--------------+-----------------+
    /// | len: N bytes | data: len bytes |
    /// +--------------+-----------------+
    /// ```
    ///
    fn read_inlined_bytes<const N: usize>(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_len_with_width::<N>()?;
        let mut buf = Vec::with_capacity(len);
        unsafe { buf.set_len(len) };
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    #[inline]
    /// Read inlined string with specific length width `N`.
    ///
    /// The inlined string are constructed as:
    ///
    /// ```text
    /// +--------------+-----------------+
    /// | len: N bytes | data: len bytes |
    /// +--------------+-----------------+
    /// ```
    ///
    fn read_inlined_str<const N: usize>(&mut self) -> std::io::Result<String> {
        self.read_inlined_bytes::<N>().and_then(|vec| {
            String::from_utf8(vec)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
    }

    #[inline]
    /// Read some bytes into inlinable object.
    fn read_inlinable<T: Inlinable>(&mut self) -> std::io::Result<T>
    where
        Self: Sized,
    {
        T::read_inlined(self)
    }
}

impl<T> InlinableRead for T where T: Read {}

pub struct InlineOpts {
    pub opts: BTreeMap<String, String>,
}

/// If one struct could be serialized/flattened to bytes array, we call it **inlinable**.
pub trait Inlinable {
    /// Read inlined bytes into object.
    fn read_inlined<R: Read>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized;

    fn read_optional_inlined<R: Read>(reader: &mut R) -> std::io::Result<Option<Self>>
    where
        Self: Sized,
    {
        Self::read_inlined(reader).map(Some)
    }

    /// Write inlined bytes to a writer.
    fn write_inlined<W: Write>(&self, wtr: &mut W) -> std::io::Result<usize>;

    /// Write inlined bytes with specific options
    fn write_inlined_with<W: Write>(
        &self,
        wtr: &mut W,
        _opts: InlineOpts,
    ) -> std::io::Result<usize> {
        self.write_inlined(wtr)
    }

    #[inline]
    /// Get inlined bytes as vector.
    fn inlined(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_inlined(&mut buf)
            .expect("write to vec should always be success");
        buf
    }

    #[inline]
    /// Get inlined bytes as printable string, all the bytes will displayed with escaped ascii code.
    fn printable_inlined(&self) -> String {
        self.inlined().escape_ascii().to_string()
    }
}

/// If one struct could be serialized/flattened to bytes array, we call it **inlinable**.
#[async_trait::async_trait]
pub trait AsyncInlinable {
    /// Read inlined bytes into object.
    async fn read_inlined<R: AsyncRead + Send + Unpin>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized;

    async fn read_optional_inlined<R: AsyncRead + Send + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Option<Self>>
    where
        Self: Sized,
    {
        Self::read_inlined(reader).await.map(Some)
    }

    /// Write inlined bytes to a writer.
    async fn write_inlined<W: AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
    ) -> std::io::Result<usize>;

    /// Write inlined bytes with specific options
    async fn write_inlined_with<W: AsyncWrite + Send + Unpin>(
        &self,
        wtr: &mut W,
        _opts: InlineOpts,
    ) -> std::io::Result<usize> {
        self.write_inlined(wtr).await
    }

    #[inline]
    /// Get inlined bytes as vector.
    async fn inlined(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_inlined(&mut buf)
            .await
            .expect("write to vec should always be success");
        buf
    }

    #[inline]
    /// Get inlined bytes as printable string, all the bytes will displayed with escaped ascii code.
    async fn printable_inlined(&self) -> String {
        self.inlined().await.escape_ascii().to_string()
    }
}

#[test]
fn inlined_bytes() -> std::io::Result<()> {
    let s = "abcd";
    let mut vec: Vec<u8> = Vec::new();
    let bytes = InlinableWrite::write_inlined_bytes::<1>(&mut vec, s.as_bytes())?;
    assert_eq!(bytes, 5);
    assert_eq!(&vec, b"\x04abcd");

    let r = InlinableRead::read_inlined_str::<1>(&mut vec.as_slice())?;
    assert_eq!(r, "abcd");
    Ok(())
}
