//! 创建这个工具的最初目的是控制更新 tmq 消费进度的频率。因为获取最新的 offsets 是个有代价的操作，所以我们不想太频繁的去获取。
//! 通过这个工具可以设置获取 offsets 频率的上限，比如最多 5s 一次。
//! 当接收 tmq 消息的频率大于 5s 一次时，我们可以控制并不是每次消费完数据都去获取 offsets，这样比每次收到消息都更有性能优势。
//! 当接收 tmq 消息的频率小于 5s 一次时，又能保证只有在有新的消息时才去获取 offsets，这样可以避免不必要的操作，比定时去获取 offsets 也更有性能优势。

use std::{cell::Cell, time::Duration};

use tokio::time::Instant;

pub struct IntervalLimit {
    last_time: Cell<Instant>,
    interval_limit: Duration,
}

impl IntervalLimit {
    pub fn new(interval_limit: Duration) -> Self {
        Self {
            last_time: Cell::new(Instant::now()),
            interval_limit,
        }
    }

    /**
     * 检查经过的时间是否超过了预定的时间间隔
     */
    pub fn ticked(&self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_time.get()) >= self.interval_limit {
            self.last_time.set(now);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_interval_limit() {
        let interval_limit = IntervalLimit::new(Duration::from_secs(5));
        assert!(!interval_limit.ticked());
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!interval_limit.ticked());
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(interval_limit.ticked());
    }
}
