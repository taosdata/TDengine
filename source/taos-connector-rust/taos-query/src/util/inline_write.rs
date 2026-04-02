use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::AsyncInlinable;

#[async_trait::async_trait]
pub trait AsyncInlinableWrite: AsyncWrite + Send + Unpin {
    #[inline]
    /// Write `usize` length as little endian `N` bytes.
    async fn write_len_with_width<const N: usize>(&mut self, len: usize) -> std::io::Result<usize> {
        self.write_all(&len.to_le_bytes()[0..N]).await?;
        Ok(N)
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
    async fn write_inlined_bytes<const N: usize>(
        &mut self,
        bytes: &[u8],
    ) -> std::io::Result<usize> {
        debug_assert_eq!(bytes.len() >> (N * 8), 0); // bytes.len() < (2 ^ (8 * N))
        let len = &bytes.len().to_le_bytes()[0..N];
        self.write_all(len).await?;
        self.write_all(bytes).await?;
        Ok(N + bytes.len())
    }

    #[inline]
    /// Write inlined string with specific length width `N`.
    async fn write_inlined_str<const N: usize>(&mut self, s: &str) -> std::io::Result<usize> {
        self.write_inlined_bytes::<N>(s.as_bytes()).await
    }

    #[inline]
    /// Write an inlinable object.
    async fn write_inlinable<T: AsyncInlinable + Sync>(
        &mut self,
        value: &T,
    ) -> std::io::Result<usize>
    where
        Self: Sized,
    {
        // self.write_inlinable(value).await
        T::write_inlined(value, self).await
    }
}

impl<T> AsyncInlinableWrite for T where T: AsyncWrite + Send + Unpin {}
