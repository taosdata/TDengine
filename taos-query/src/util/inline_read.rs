use tokio::io::*;

use super::AsyncInlinable;

#[async_trait::async_trait]
pub trait AsyncInlinableRead: AsyncRead + Unpin + Send {
    #[inline]
    /// Read `N` bytes as `usize`.
    ///
    /// Only 1/2/4/8 is valid as `N`.
    async fn read_len_with_width<const N: usize>(&mut self) -> std::io::Result<usize> {
        let mut bytes: [u8; N] = [0; N];
        self.read_exact(&mut bytes).await?;
        let len = match N {
            1 => bytes[0] as usize,
            2 => unsafe { *std::mem::transmute::<*const u8, *const u16>(bytes.as_ptr()) as usize },
            4 => unsafe { *std::mem::transmute::<*const u8, *const u32>(bytes.as_ptr()) as usize },
            8 => unsafe { *std::mem::transmute::<*const u8, *const u64>(bytes.as_ptr()) as usize },
            _ => unreachable!(),
        };
        Ok(len)
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
    async fn read_len_with_data<const N: usize>(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_len_with_width::<N>().await?;
        let mut buf = Vec::with_capacity(len);
        buf.extend(&(len as u64).to_le_bytes()[0..N]);
        buf.resize(len, 0);
        self.read_exact(&mut buf[N..]).await?;
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
    async fn read_inlined_bytes<const N: usize>(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_len_with_width::<N>().await?;
        let mut buf = Vec::with_capacity(len);
        unsafe { buf.set_len(len) };
        self.read_exact(&mut buf).await?;
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
    async fn read_inlined_str<const N: usize>(&mut self) -> std::io::Result<String> {
        self.read_inlined_bytes::<N>().await.and_then(|vec| {
            String::from_utf8(vec)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
    }

    #[inline]
    /// Read some bytes into inlinable object.
    async fn read_inlinable<T: AsyncInlinable>(&mut self) -> std::io::Result<T>
    where
        T: Sized,
    {
        self.read_inlinable().await
    }
}

impl<T> AsyncInlinableRead for T where T: AsyncRead + Send + Unpin {}
