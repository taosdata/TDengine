use std::{
    task::{Poll, ready},
    time::{Duration, Instant},
};

use pin_project_lite::pin_project;
use tokio::sync::oneshot::Receiver;

use crate::PendingState;

pub enum PendingAckResult {
    State((Duration, Vec<PendingState>)),
    Closed,
}

pin_project! {
    pub struct PendingAckFuture {
        #[pin]
        rx: Receiver<Vec<PendingState>>,
        inst: Instant
    }
}

impl PendingAckFuture {
    pub fn new(rx: Receiver<Vec<PendingState>>) -> Self {
        Self {
            rx,
            inst: Instant::now(),
        }
    }
}

impl Future for PendingAckFuture {
    type Output = PendingAckResult;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match ready!(this.rx.poll(cx)) {
            Ok(state) => Poll::Ready(PendingAckResult::State((this.inst.elapsed(), state))),
            Err(_) => Poll::Ready(PendingAckResult::Closed),
        }
    }
}
