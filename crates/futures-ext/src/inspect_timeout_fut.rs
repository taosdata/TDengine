use std::{future::Future, task::ready, time::Duration};

use pin_project_lite::pin_project;
use tokio::time::{Instant, Interval};

pin_project! {
    pub struct InspectTimeoutFuture<Fut> {
        #[pin]
        interval: Interval,
        #[pin]
        fut: Fut,
        inspect: Box<dyn Fn(Duration) + 'static + Send>,
        start: Instant,
    }
}

impl<Fut> InspectTimeoutFuture<Fut> {
    pub fn new(
        period: Duration,
        fut: Fut,
        inspect: Box<dyn Fn(Duration) + 'static + Send>,
    ) -> Self {
        let interval = tokio::time::interval_at(Instant::now() + period, period);
        Self {
            interval,
            fut,
            inspect,
            start: Instant::now(),
        }
    }
}

impl<Fut> Future for InspectTimeoutFuture<Fut>
where
    Fut: Future,
{
    type Output = Fut::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.fut.as_mut().poll(cx) {
                std::task::Poll::Ready(res) => return std::task::Poll::Ready(res),
                std::task::Poll::Pending => {
                    ready!(this.interval.poll_tick(cx));
                    (this.inspect)(this.start.elapsed());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    };

    use super::*;

    #[tokio::test]
    async fn test() {
        let inspect_count = Arc::new(AtomicU8::default());

        let inspect_fut = InspectTimeoutFuture::new(
            Duration::from_millis(200),
            tokio::time::sleep(Duration::from_millis(500)),
            Box::new({
                let count = inspect_count.clone();
                move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            }),
        );

        inspect_fut.await;
        assert_eq!(inspect_count.load(Ordering::SeqCst), 2);
    }
}
