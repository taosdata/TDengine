use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use pin_project_lite::pin_project;

pin_project! {
    /// A future representing a value which may or may not be present.
    ///
    /// Created by the [`From`] implementation for [`Option`](std::option::Option).
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::future::OptionFuture;
    ///
    /// let mut a: OptionFuture<_> = Some(async { 123 }).into();
    /// assert_eq!(a.await, Some(123));
    ///
    /// a = None.into();
    /// assert_eq!(a.await, None);
    /// # });
    /// ```
    #[derive(Debug, Clone)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct OptionFuture<F> {
        #[pin]
        inner: Option<F>,
    }
}

impl<F: Future> Future for OptionFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.as_pin_mut() {
            Some(x) => x.poll(cx),
            None => Poll::Pending,
        }
    }
}

impl<T> From<Option<T>> for OptionFuture<T> {
    fn from(option: Option<T>) -> Self {
        Self { inner: option }
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    #[tokio::test]
    async fn poll_test() -> anyhow::Result<()> {
        assert_eq!(OptionFuture::from(Some(async { 1 })).await, 1);
        assert!(
            OptionFuture::from(None::<std::future::Ready<()>>)
                .now_or_never()
                .is_none()
        );
        Ok(())
    }
}
