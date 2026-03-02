use std::time::{Duration, Instant};

pub struct RetryBackoff {
    duration: BackoffDuration,

    retries: usize,
    start: Option<Instant>,
}

impl RetryBackoff {
    pub fn new(init: Duration, max: Duration) -> Self {
        RetryBackoff {
            duration: BackoffDuration::new(init, max),
            retries: 0,
            start: None,
        }
    }

    pub async fn wait(&mut self) {
        self.retries += 1;
        self.start.get_or_insert_with(Instant::now);
        let duration = self.duration.next();
        tokio::time::sleep(duration).await
    }

    pub fn reset(&mut self) {
        self.duration.reset();
        self.retries = 0;
        self.start = None;
    }

    pub fn retries(&self) -> usize {
        self.retries
    }

    pub fn elapsed(&self) -> Duration {
        self.start.as_ref().map(|v| v.elapsed()).unwrap_or_default()
    }
}

pub struct BackoffDuration {
    init: Duration,
    max: Duration,
    current: Duration,
}

impl BackoffDuration {
    pub fn new(init: Duration, max: Duration) -> Self {
        BackoffDuration {
            max,
            current: init,
            init,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Duration {
        let next = self.current;
        self.current = self.max.min(self.current * 2);
        next
    }

    pub fn reset(&mut self) {
        self.current = self.init;
    }
}
