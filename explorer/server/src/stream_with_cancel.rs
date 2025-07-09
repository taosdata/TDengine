use std::task::ready;

use actix_web::web::Bytes;
use pin_project_lite::pin_project;
use tokio_util::sync::{CancellationToken, DropGuard};

pin_project! {
    pub struct StreamWithCancel<S> {
        #[pin]
        inner: S,
        cancel: CancellationToken,
        _guard: DropGuard
    }
}

impl<S> StreamWithCancel<S> {
    pub fn new(inner: S, cancel: CancellationToken) -> Self {
        Self {
            inner,
            cancel: cancel.clone(),
            _guard: cancel.drop_guard(),
        }
    }
}

impl<S> futures::Stream for StreamWithCancel<S>
where
    S: futures::Stream<Item = std::io::Result<Bytes>>,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        match ready!(this.inner.as_mut().poll_next(cx)) {
            Some(item) => {
                if item.as_ref().is_ok_and(|s| s.is_empty()) {
                    std::task::Poll::Ready(None)
                } else {
                    std::task::Poll::Ready(Some(item))
                }
            }
            None => {
                this.cancel.cancel();
                std::task::Poll::Ready(None)
            }
        }
    }
}
