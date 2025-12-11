use core::fmt;
use core::pin::Pin;
use futures::stream::{FusedStream, Stream, TryStream};
use futures::task::{Context, Poll};
use pin_project_lite::pin_project;

pin_project! {
    /// Stream for the [`try_ready_chunks`](super::TryStreamExt::try_ready_chunks) method.
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct TryReadyChunks<St: TryStream> {
        #[pin]
        stream: St,
        cap: usize, // https://github.com/rust-lang/futures-rs/issues/1475
    }
}

impl<St: TryStream> TryReadyChunks<St> {
    pub fn new(stream: St, capacity: usize) -> Self {
        assert!(capacity > 0);

        Self {
            stream,
            cap: capacity,
        }
    }

    // delegate_access_inner!(stream, St, (. .));
}

type TryReadyChunksStreamError<St> =
    TryReadyChunksError<<St as TryStream>::Ok, <St as TryStream>::Error>;

impl<St: TryStream> Stream for TryReadyChunks<St> {
    type Item = Result<Vec<St::Ok>, TryReadyChunksStreamError<St>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        let mut items: Vec<St::Ok> = Vec::new();

        loop {
            match this.stream.as_mut().try_poll_next(cx) {
                // Flush all the collected data if the underlying stream doesn't
                // contain more ready values
                Poll::Pending => {
                    return if items.is_empty() {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(Ok(items)))
                    }
                }

                // Push the ready item into the buffer and check whether it is full.
                // If so, return the buffer.
                Poll::Ready(Some(Ok(item))) => {
                    if items.is_empty() {
                        items.reserve_exact(*this.cap);
                    }
                    items.push(item);
                    if items.len() >= *this.cap {
                        return Poll::Ready(Some(Ok(items)));
                    }
                }

                // Return the already collected items and the error.
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(TryReadyChunksError(items, e))));
                }

                // Since the underlying stream ran out of values, return what we
                // have buffered, if we have anything.
                Poll::Ready(None) => {
                    let last = if items.is_empty() {
                        None
                    } else {
                        Some(Ok(items))
                    };
                    return Poll::Ready(last);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.stream.size_hint();
        let lower = lower / self.cap;
        (lower, upper)
    }
}

impl<St: TryStream + FusedStream> FusedStream for TryReadyChunks<St> {
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }
}

/// Error indicating, that while chunk was collected inner stream produced an error.
///
/// Contains all items that were collected before an error occurred, and the stream error itself.
#[derive(PartialEq, Eq)]
pub struct TryReadyChunksError<T, E>(pub Vec<T>, pub E);

impl<T, E: fmt::Debug> fmt::Debug for TryReadyChunksError<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.fmt(f)
    }
}

impl<T, E: fmt::Display> fmt::Display for TryReadyChunksError<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures_test::task::noop_context;
    use tokio_test::assert_ready_eq;

    use super::*;

    #[test]
    fn empty_stream_return_none_test() {
        let mut context = noop_context();
        let (_tx, rx) = futures::channel::mpsc::unbounded::<Result<(), ()>>();
        let stream = TryReadyChunks::new(rx, 10);
        let mut stream = pin!(stream);
        assert_ready_eq!(stream.as_mut().poll_next(&mut context), None);
    }

    #[test]
    fn less_data_test() {
        let mut context = noop_context();
        let (tx, rx) = futures::channel::mpsc::unbounded::<Result<(), ()>>();
        let stream = TryReadyChunks::new(rx, 10);
        let mut stream = pin!(stream);
        for _ in 0..5 {
            let _ = tx.unbounded_send(Ok(()));
        }
        assert_ready_eq!(
            stream.as_mut().poll_next(&mut context),
            Some(Ok(std::iter::repeat_n((), 5).collect::<Vec<_>>()))
        );
    }

    #[test]
    fn more_data_test() {
        let mut context = noop_context();
        let (tx, rx) = futures::channel::mpsc::unbounded::<Result<(), ()>>();
        let stream = TryReadyChunks::new(rx, 10);
        let mut stream = pin!(stream);

        for _ in 0..20 {
            let _ = tx.unbounded_send(Ok(()));
        }

        assert_ready_eq!(
            stream.as_mut().poll_next(&mut context),
            Some(Ok(std::iter::repeat_n((), 10).collect::<Vec<_>>()))
        );

        assert_ready_eq!(
            stream.as_mut().poll_next(&mut context),
            Some(Ok(std::iter::repeat_n((), 10).collect::<Vec<_>>()))
        );
    }

    #[test]
    fn empty_drop_test() {
        let mut context = noop_context();
        let (tx, rx) = futures::channel::mpsc::unbounded::<Result<(), ()>>();
        let stream = TryReadyChunks::new(rx, 10);
        let mut stream = pin!(stream);
        drop(tx);
        assert_ready_eq!(stream.as_mut().poll_next(&mut context), None);
    }

    #[test]
    fn has_data_and_drop_test() {
        let mut context = noop_context();
        let (tx, rx) = futures::channel::mpsc::unbounded::<Result<(), ()>>();
        let stream = TryReadyChunks::new(rx, 10);
        let mut stream = pin!(stream);

        for _ in 0..20 {
            let _ = tx.unbounded_send(Ok(()));
        }

        drop(tx);

        assert_ready_eq!(
            stream.as_mut().poll_next(&mut context),
            Some(Ok(std::iter::repeat_n((), 10).collect::<Vec<_>>()))
        );

        assert_ready_eq!(
            stream.as_mut().poll_next(&mut context),
            Some(Ok(std::iter::repeat_n((), 10).collect::<Vec<_>>()))
        );
    }
}
