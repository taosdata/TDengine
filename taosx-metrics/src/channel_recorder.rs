//! 将 metrics 直接写入 Channel 的 Recorder, Channel 的接收端可以自定义如何处理这些 metrics，比如写入数据库或者发送到远端。
//!
//! # Example
//!
use std::sync::Arc;

use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, Recorder};

use flume::Sender;

enum MetricOperation {
    IncrementCounter(u64),
    SetCounter(u64),
    IncrementGauge(f64),
    DecrementGauge(f64),
    SetGauge(f64),
    RecordHistogram(f64),
}

enum Event {
    Metric(Key, MetricOperation),
}
struct Handle {
    key: Key,
    tx: Arc<Sender<Event>>,
}

impl Handle {
    fn new(key: Key, tx: Arc<Sender<Event>>) -> Handle {
        Handle { key, tx }
    }
}

impl CounterFn for Handle {
    fn increment(&self, value: u64) {
        let _ = self.tx.try_send(Event::Metric(
            self.key.clone(),
            MetricOperation::IncrementCounter(value),
        ));
    }

    fn absolute(&self, value: u64) {
        let _ = self.tx.try_send(Event::Metric(
            self.key.clone(),
            MetricOperation::SetCounter(value),
        ));
    }
}

impl GaugeFn for Handle {
    fn increment(&self, value: f64) {
        let _ = self.tx.try_send(Event::Metric(
            self.key.clone(),
            MetricOperation::IncrementGauge(value),
        ));
    }

    fn decrement(&self, value: f64) {
        let _ = self.tx.try_send(Event::Metric(
            self.key.clone(),
            MetricOperation::DecrementGauge(value),
        ));
    }

    fn set(&self, value: f64) {
        let _ = self.tx.try_send(Event::Metric(
            self.key.clone(),
            MetricOperation::SetGauge(value),
        ));
    }
}

impl HistogramFn for Handle {
    fn record(&self, value: f64) {
        let _ = self.tx.try_send(Event::Metric(
            self.key.clone(),
            MetricOperation::RecordHistogram(value),
        ));
    }
}

struct ChannelRecorder {
    tx: Arc<Sender<Event>>,
}

impl ChannelRecorder {
    pub fn new(tx: Arc<Sender<Event>>) -> ChannelRecorder {
        ChannelRecorder { tx }
    }

    pub fn install(self) {
        metrics::set_global_recorder(self).expect("failed to install ChannelRecorder");
    }
}

impl Recorder for ChannelRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // 暂不支持 describe
        unimplemented!()
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // 暂不支持 describe
        unimplemented!()
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
        // 暂不支持 describe
        unimplemented!()
    }

    fn register_counter(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        Counter::from_arc(Arc::new(Handle::new(key.clone(), self.tx.clone())))
    }

    fn register_gauge(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        Gauge::from_arc(Arc::new(Handle::new(key.clone(), self.tx.clone())))
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        Histogram::from_arc(Arc::new(Handle::new(key.clone(), self.tx.clone())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flume::{Receiver, Sender};

    use super::ChannelRecorder;
    use metrics::Key;

    #[test]
    fn test_counter() {
        let (tx, rx): (Sender<super::Event>, Receiver<super::Event>) = flume::unbounded();
        let recorder = ChannelRecorder::new(Arc::new(tx));
        recorder.install();

        let key = Key::from_name("test");
        let counter = metrics::counter!("test");
        counter.increment(1);
        counter.increment(2);
        counter.increment(3);
        counter.increment(4);
        counter.increment(5);
        counter.increment(6);
        for i in 1..7 {
            let event = rx.recv().unwrap();
            match event {
                super::Event::Metric(k, super::MetricOperation::IncrementCounter(v)) => {
                    assert_eq!(k, key);
                    assert_eq!(v, i);
                }
                _ => panic!("unexpected event"),
            }
        }
    }

    #[test]
    fn test_gauge() {
        let (tx, rx): (Sender<super::Event>, Receiver<super::Event>) = flume::unbounded();
        let recorder = ChannelRecorder::new(Arc::new(tx));
        recorder.install();

        let gauge = metrics::gauge!("test", "pid" => "100");
        gauge.set(1.0);
        gauge.set(2.0);
        gauge.set(3.0);
        gauge.set(4.0);
        gauge.set(5.0);
        gauge.set(6.0);
        for i in 1..7 {
            let event = rx.recv().unwrap();
            match event {
                super::Event::Metric(k, super::MetricOperation::SetGauge(v)) => {
                    k.labels().for_each(|label| {
                        assert_eq!(label.key(), "pid");
                        assert_eq!(label.value(), "100");
                    });
                    assert_eq!(v, i as f64);
                }
                _ => panic!("unexpected event"),
            }
        }
    }
}
