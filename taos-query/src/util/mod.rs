mod inline_bytes;
mod inline_str;

use std::{
    io::{Read, Write},
    mem::size_of,
};

pub use inline_bytes::InlineBytes;
pub use inline_str::InlineStr;

pub trait InlinableWrite: Write {
    #[inline]
    /// Write `usize` length as little endian `N` bytes.
    fn write_len_with_width<const N: usize>(&mut self, len: usize) -> std::io::Result<usize> {
        self.write(&len.to_le_bytes()[0..N])?;
        Ok(N)
    }

    #[inline]
    /// Write a [u8] value to writer.
    fn write_u8(&mut self, value: u8) -> std::io::Result<usize> {
        self.write(&[value])?;
        Ok(1)
    }

    #[inline]
    /// Write a [u16] value to writer.
    fn write_u16(&mut self, value: u16) -> std::io::Result<usize> {
        self.write_all(&value.to_le_bytes())?;
        Ok(1)
    }

    #[inline]
    /// Write a [u32] value to writer.
    fn write_u32(&mut self, value: u32) -> std::io::Result<usize> {
        self.write_all(&value.to_le_bytes())?;
        Ok(1)
    }

    #[inline]
    /// Write a [u64] value to writer.
    fn write_u64(&mut self, value: u64) -> std::io::Result<usize> {
        self.write_all(&value.to_le_bytes())?;
        Ok(1)
    }

    #[inline]
    /// Write a [u128] value to writer.
    fn write_u128(&mut self, value: u128) -> std::io::Result<usize> {
        self.write_all(&value.to_le_bytes())?;
        Ok(1)
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
        let l = self.write(&bytes.len().to_le_bytes()[0..N])?;
        Ok(l + self.write(bytes)?)
    }

    #[inline]
    /// Write inlined string with specific length width `N`.
    fn write_inlined_str<const N: usize>(&mut self, s: &str) -> std::io::Result<usize> {
        self.write_inlined_bytes::<N>(s.as_bytes())
    }

    #[inline]
    /// Write an inlinable object.
    fn write_inlinable<T: Inlinable>(&mut self, value: &T) -> std::io::Result<usize> {
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
    fn read_inlinable<T: Inlinable>(&mut self) -> std::io::Result<T> {
        T::read_inlined(self)
    }
}

impl<T> InlinableRead for T where T: Read {}

/// If one struct could be serialized/flattened to bytes array, we call it **inlinable**.
pub trait Inlinable: Sized {
    /// Read inlined bytes into object.
    fn read_inlined<R: Read>(reader: R) -> std::io::Result<Self>;

    /// Write inlined bytes to a writer.
    fn write_inlined<W: Write>(&self, wtr: W) -> std::io::Result<usize>;

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

impl Inlinable for u8 {
    #[inline]
    fn write_inlined<W: Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        wtr.write_u8(*self)
    }

    #[inline]
    fn read_inlined<R: Read>(mut reader: R) -> std::io::Result<Self> {
        reader.read_u8()
    }
}

impl Inlinable for u16 {
    #[inline]
    fn write_inlined<W: Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        wtr.write_u16(*self)
    }

    #[inline]
    fn read_inlined<R: Read>(mut reader: R) -> std::io::Result<Self> {
        reader.read_u16()
    }
}

impl Inlinable for u32 {
    #[inline]
    fn write_inlined<W: Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        wtr.write_u32(*self)
    }

    #[inline]
    fn read_inlined<R: Read>(mut reader: R) -> std::io::Result<Self> {
        reader.read_u32()
    }
}

impl Inlinable for u64 {
    #[inline]
    fn write_inlined<W: Write>(&self, mut wtr: W) -> std::io::Result<usize> {
        wtr.write_u64(*self)
    }

    #[inline]
    fn read_inlined<R: Read>(mut reader: R) -> std::io::Result<Self> {
        reader.read_u64()
    }
}

#[test]
fn inlined_bytes() -> std::io::Result<()> {
    let s = "abcd";
    let mut vec: Vec<u8> = Vec::new();
    let bytes = vec.write_inlined_bytes::<1>(s.as_bytes())?;
    assert_eq!(bytes, 5);
    assert_eq!(&vec, b"\x04abcd");

    let r = vec.as_slice().read_inlined_str::<1>()?;
    assert_eq!(r, "abcd");
    Ok(())
}
