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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[test]
    fn test_pending_ack_future_new() {
        let (_tx, rx) = oneshot::channel::<Vec<PendingState>>();
        let future = PendingAckFuture::new(rx);

        // Test that the future was created successfully
        // We can't easily inspect internal fields due to pin_project,
        // but we can verify the type exists
        let _type_check: PendingAckFuture = future;
    }

    #[tokio::test]
    async fn test_pending_ack_result_state() {
        let (tx, rx) = oneshot::channel::<Vec<PendingState>>();
        let future = PendingAckFuture::new(rx);

        // Send some data
        let states = vec![];
        tx.send(states).unwrap();

        // Await the future
        let result = future.await;

        // Check that we got a State result
        match result {
            PendingAckResult::State((duration, states)) => {
                assert!(duration.as_nanos() > 0, "Duration should be positive");
                assert_eq!(states.len(), 0);
            }
            PendingAckResult::Closed => panic!("Expected State, got Closed"),
        }
    }

    #[tokio::test]
    async fn test_pending_ack_result_closed() {
        let (_tx, rx) = oneshot::channel::<Vec<PendingState>>();
        let future = PendingAckFuture::new(rx);

        // Drop the sender to close the channel
        drop(_tx);

        // Await the future
        let result = future.await;

        // Check that we got a Closed result
        match result {
            PendingAckResult::Closed => {
                // Expected
            }
            PendingAckResult::State(_) => panic!("Expected Closed, got State"),
        }
    }

    #[tokio::test]
    async fn test_pending_ack_duration_tracking() {
        let (tx, rx) = oneshot::channel::<Vec<PendingState>>();
        let future = PendingAckFuture::new(rx);

        // Add a small delay before sending
        tokio::time::sleep(Duration::from_millis(10)).await;

        let states = vec![];
        tx.send(states).unwrap();

        let result = future.await;

        match result {
            PendingAckResult::State((duration, _)) => {
                // Duration should be at least the sleep time
                assert!(
                    duration.as_millis() >= 10,
                    "Duration should be at least 10ms, got {:?}",
                    duration
                );
            }
            PendingAckResult::Closed => panic!("Expected State, got Closed"),
        }
    }

    #[tokio::test]
    async fn test_pending_ack_with_multiple_states() {
        let (tx, rx) = oneshot::channel::<Vec<PendingState>>();
        let future = PendingAckFuture::new(rx);

        // Create multiple pending states
        let states = vec![
            PendingState {
                topic: "test".to_string(),
                partition: 0,
                offset: 0,
            },
            PendingState {
                topic: "test".to_string(),
                partition: 1,
                offset: 100,
            },
            PendingState {
                topic: "test".to_string(),
                partition: 2,
                offset: 200,
            },
        ];

        tx.send(states).unwrap();

        let result = future.await;

        match result {
            PendingAckResult::State((_, returned_states)) => {
                assert_eq!(returned_states.len(), 3);
            }
            PendingAckResult::Closed => panic!("Expected State, got Closed"),
        }
    }

    #[test]
    fn test_pending_ack_result_state_variant() {
        let states = vec![];
        let duration = Duration::from_secs(1);
        let result = PendingAckResult::State((duration, states));

        match result {
            PendingAckResult::State((d, s)) => {
                assert_eq!(d, Duration::from_secs(1));
                assert_eq!(s.len(), 0);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_pending_ack_result_closed_variant() {
        let result = PendingAckResult::Closed;

        match result {
            PendingAckResult::Closed => {
                // Expected
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[tokio::test]
    async fn test_pending_ack_immediate_send() {
        let (tx, rx) = oneshot::channel::<Vec<PendingState>>();

        // Send before creating future
        let states = vec![PendingState {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
        }];
        tx.send(states).unwrap();

        let future = PendingAckFuture::new(rx);
        let result = future.await;

        match result {
            PendingAckResult::State((_, states)) => {
                assert_eq!(states.len(), 1);
            }
            PendingAckResult::Closed => panic!("Expected State, got Closed"),
        }
    }

    #[tokio::test]
    async fn test_pending_ack_empty_states() {
        let (tx, rx) = oneshot::channel::<Vec<PendingState>>();
        let future = PendingAckFuture::new(rx);

        // Send empty vector
        tx.send(vec![]).unwrap();

        let result = future.await;

        match result {
            PendingAckResult::State((_, states)) => {
                assert_eq!(states.len(), 0);
            }
            PendingAckResult::Closed => panic!("Expected State, got Closed"),
        }
    }
}
