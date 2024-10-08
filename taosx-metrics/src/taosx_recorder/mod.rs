//! 能自动删除过期 metric 的 Recorder
//!
//! # 原理
//!
//! 1. 使用 GenerationalStorage，每个 metric 都有一个 generation，每次更新时都会自增。
//! 2. 使用 Recency 模块，它会记录每次获取 metrics 的时间和当时的版本。当下次获取时，如果版本没有变化，且超过了 idle_timeout，则会删除该 metric。
//!
//! # 使用示例：
//!
//! ```rust
//! use taosx_metrics::taosx_recorder::TaosXRecorder;
//! use std::time::Duration;
//! let recorder = TaosXRecorder::new(Some(Duration::from_secs(1)));
//! // 必须先获取 handle，再 install, 因为 install 会转移所有权
//! let handle = recorder.handle();
//! recorder.install();
//! // 从handdle 获取 snapshot, 快照的类型为 Vec<(Key, DebugValue)>
//! let snapshot = handle.snapshot();
//! metrics::counter!("test_counter").increment(10);
//! metrics::gauge!("test_gauge").set(10.0);
//! ```
//!
mod formatting;
mod registry;

use metrics::Key;
use metrics::Recorder;
use metrics_util::registry::GenerationalStorage;
use metrics_util::registry::{Recency, Registry};

use metrics_util::MetricKindMask;
use quanta::Clock;
use registry::AtomicStorage;
use registry::GenerationalAtomicStorage;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct Snapshot(Vec<(Key, DebugValue)>);

#[derive(Debug, PartialEq)]
pub enum DebugValue {
    /// Counter.
    Counter(u64),
    /// Gauge.
    Gauge(f64),
}

impl Snapshot {
    pub fn data(self) -> Vec<(Key, DebugValue)> {
        self.0
    }
}

pub(crate) struct Inner {
    registry: Registry<Key, GenerationalAtomicStorage>,
    recency: Recency<Key>,
}

impl Inner {
    pub fn new(idle_timeout: Option<Duration>) -> Self {
        let clock = Clock::new();
        let mask = MetricKindMask::ALL;
        Self {
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(clock, mask, idle_timeout),
        }
    }

    fn get_recent_metrics(&self) -> Snapshot {
        let mut snapshot = Vec::new();
        let counter_handles = self.registry.get_counter_handles();

        for (key, counter) in counter_handles {
            let gen = counter.get_generation();
            if !self.recency.should_store_counter(&key, gen, &self.registry) {
                continue;
            }
            let value = counter.get_inner().load(Ordering::Acquire);
            snapshot.push((key, DebugValue::Counter(value)));
        }

        let gauge_handles = self.registry.get_gauge_handles();

        for (key, gauge) in gauge_handles {
            let gen = gauge.get_generation();
            if !self.recency.should_store_gauge(&key, gen, &self.registry) {
                continue;
            }
            let value = f64::from_bits(gauge.get_inner().load(Ordering::Acquire));
            snapshot.push((key, DebugValue::Gauge(value)));
        }
        Snapshot(snapshot)
    }
}

pub struct TaosXRecorder {
    inner: Arc<Inner>,
}

impl TaosXRecorder {
    pub fn new(idle_timeout: Option<Duration>) -> Self {
        Self {
            inner: Arc::new(Inner::new(idle_timeout)),
        }
    }

    pub fn handle(&self) -> TaosXRecorderHandle {
        TaosXRecorderHandle {
            inner: self.inner.clone(),
        }
    }

    pub fn install(self) {
        metrics::set_global_recorder(self).expect("failed to install TaosXRecorder");
    }
}

impl Recorder for TaosXRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        unimplemented!()
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        unimplemented!()
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        unimplemented!()
    }

    fn register_counter(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        self.inner
            .registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        self.inner
            .registry
            .get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        self.inner
            .registry
            .get_or_create_histogram(key, |c| c.clone().into())
    }
}

#[derive(Clone)]
pub struct TaosXRecorderHandle {
    inner: Arc<Inner>,
}

impl TaosXRecorderHandle {
    pub fn snapshot(&self) -> Snapshot {
        self.inner.get_recent_metrics()
    }

    pub fn render(&self) -> String {
        let snapshot = self.snapshot();
        let mut output = String::new();
        for (key, value) in snapshot.0 {
            let (name, labels) = key.into_parts();
            let name = name.as_str();
            match value {
                DebugValue::Counter(v) => {
                    formatting::write_type_line(&mut output, name, "counter");
                    formatting::write_metric_line(&mut output, name, None, labels, v);
                }
                DebugValue::Gauge(v) => {
                    formatting::write_type_line(&mut output, name, "gauge");
                    formatting::write_metric_line(&mut output, name, None, labels, v);
                }
            }
            output.push('\n');
        }
        output
    }
}
#[cfg(test)]
mod test {
    use super::DebugValue;
    use super::TaosXRecorder;
    use metrics::counter;
    use metrics::Key;
    use std::time::Duration;
    #[test]
    fn test_taosx_recorder() {
        let recorder = TaosXRecorder::new(Some(Duration::from_secs(1)));
        let handle = recorder.handle();
        recorder.install();
        let key = Key::from_name("test_counter");
        let counter = counter!("test_counter");
        counter.increment(10);
        counter.increment(10);
        let snapshot = handle.snapshot();
        println!("{:?}", snapshot);
        assert_eq!(snapshot.0.len(), 1);
        assert_eq!(snapshot.0[0].0, key);
        assert_eq!(snapshot.0[0].1, DebugValue::Counter(20));
        std::thread::sleep(Duration::from_secs(2));
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.0.len(), 0);
    }
}
