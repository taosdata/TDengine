//! 将 metrics 直接写入 Channel 的 Recorder, Channel 的接收端可以自定义如何处理这些 metrics，比如写入数据库或者发送到远端。
//!
//! # Example
//!
use std::sync::Arc;

use bincode::{config, Decode, Encode};
use flume::Sender;
use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, Recorder};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Encode, Decode, Deserialize, Serialize)]
pub enum MetricOperation {
    IncrementCounter(u64),
    SetCounter(u64),
    IncrementGauge(f64),
    DecrementGauge(f64),
    SetGauge(f64),
    RecordHistogram(f64),
}

#[derive(Debug, PartialEq, Clone, Encode, Decode, Deserialize, Serialize)]
pub struct MetricEvent {
    pub key: String,
    pub labels: Vec<(String, String)>,
    pub operation: MetricOperation,
}

impl MetricEvent {
    pub fn new(key: Key, operation: MetricOperation) -> MetricEvent {
        MetricEvent {
            key: key.name().to_string(),
            labels: key
                .labels()
                .map(|label| (label.key().to_string(), label.value().to_string()))
                .collect(),
            operation,
        }
    }
}

/// 为了支持批量序列化和反序列化 metrics
#[derive(Debug, PartialEq, Clone, Encode, Decode, Deserialize, Serialize)]
pub struct MetricsEvents(Vec<MetricEvent>);
static BINCODE_CONFIG: config::Configuration = config::standard();

impl Default for MetricsEvents {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsEvents {
    pub fn new() -> MetricsEvents {
        let vec = Vec::new();
        MetricsEvents(vec)
    }

    pub fn events(&self) -> &Vec<MetricEvent> {
        &self.0
    }

    pub fn from_slice(src: &[u8]) -> Result<MetricsEvents, bincode::error::DecodeError> {
        match bincode::decode_from_slice(src, BINCODE_CONFIG) {
            Ok((events, _)) => Ok(events),
            Err(e) => Err(e),
        }
    }

    pub fn push(&mut self, event: MetricEvent) {
        self.0.push(event);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_vec_u8(&self) -> Vec<u8> {
        bincode::encode_to_vec(self, BINCODE_CONFIG).unwrap()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

struct Handle {
    key: Key,
    tx: Arc<Sender<MetricEvent>>,
}

impl Handle {
    fn new(key: Key, tx: Arc<Sender<MetricEvent>>) -> Handle {
        Handle { key, tx }
    }
}

impl CounterFn for Handle {
    fn increment(&self, value: u64) {
        let _ = self.tx.try_send(MetricEvent::new(
            self.key.clone(),
            MetricOperation::IncrementCounter(value),
        ));
    }

    fn absolute(&self, value: u64) {
        let _ = self.tx.try_send(MetricEvent::new(
            self.key.clone(),
            MetricOperation::SetCounter(value),
        ));
    }
}

impl GaugeFn for Handle {
    fn increment(&self, value: f64) {
        let _ = self.tx.try_send(MetricEvent::new(
            self.key.clone(),
            MetricOperation::IncrementGauge(value),
        ));
    }

    fn decrement(&self, value: f64) {
        let _ = self.tx.try_send(MetricEvent::new(
            self.key.clone(),
            MetricOperation::DecrementGauge(value),
        ));
    }

    fn set(&self, value: f64) {
        let _ = self.tx.try_send(MetricEvent::new(
            self.key.clone(),
            MetricOperation::SetGauge(value),
        ));
    }
}

impl HistogramFn for Handle {
    fn record(&self, value: f64) {
        let _ = self.tx.try_send(MetricEvent::new(
            self.key.clone(),
            MetricOperation::RecordHistogram(value),
        ));
    }
}

pub struct ChannelRecorder {
    tx: Arc<Sender<MetricEvent>>,
}

impl ChannelRecorder {
    pub fn new(tx: Arc<Sender<MetricEvent>>) -> ChannelRecorder {
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

    use crate::channel_recorder::MetricsEvents;

    use super::ChannelRecorder;

    #[test]
    fn test_counter() {
        let (tx, rx): (Sender<super::MetricEvent>, Receiver<super::MetricEvent>) =
            flume::unbounded();
        let recorder = ChannelRecorder::new(Arc::new(tx));
        recorder.install();

        let counter = metrics::counter!("test");
        counter.increment(1);
        counter.increment(2);
        counter.increment(3);
        counter.increment(4);
        counter.increment(5);
        counter.increment(6);
        for i in 1..7 {
            let event = rx.recv().unwrap();
            assert_eq!(event.key, "test");
            match event.operation {
                super::MetricOperation::IncrementCounter(v) => assert_eq!(v, i),
                _ => panic!("unexpected operation"),
            }
        }
    }

    #[test]
    fn test_gauge() {
        let (tx, rx): (Sender<super::MetricEvent>, Receiver<super::MetricEvent>) =
            flume::unbounded();
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
            assert_eq!(event.key, "test");
            for label in event.labels {
                assert_eq!(label.0, "pid");
                assert_eq!(label.1, "100");
            }
            match event.operation {
                super::MetricOperation::SetGauge(v) => assert_eq!(v, i as f64),
                _ => panic!("unexpected operation"),
            }
        }
    }

    #[test]
    fn test_receive_batch() {
        let (tx, rx): (Sender<super::MetricEvent>, Receiver<super::MetricEvent>) =
            flume::unbounded();
        let recorder = ChannelRecorder::new(Arc::new(tx));
        recorder.install();
        // start a new thread to send metrics every 5 seconds
        std::thread::spawn(move || {
            let counter = metrics::counter!("test-counter");
            let gauge = metrics::gauge!("test-gauge", "pid" => "100");
            for _ in 0..3 {
                for _ in 0..10 {
                    counter.increment(1);
                    gauge.set(1.0);
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });
        // start a new thread to receive metrics until channel become empty
        let h = std::thread::spawn(move || loop {
            let mut metrics_events = MetricsEvents::new();
            while let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(500)) {
                metrics_events.push(event)
            }
            if !metrics_events.is_empty() {
                assert_eq!(metrics_events.0.len(), 20); // 每批发 20 个，所以肯定收 20 个
                let _ = metrics_events.to_vec_u8();
                break;
            }
        });
        h.join().unwrap();
    }

    #[test]
    fn test_encode_and_decode() {
        let (tx, rx): (Sender<super::MetricEvent>, Receiver<super::MetricEvent>) =
            flume::unbounded();
        let recorder = ChannelRecorder::new(Arc::new(tx));
        recorder.install();
        // send 20 metrics events
        std::thread::spawn(move || {
            let counter = metrics::counter!("test-counter");
            let gauge = metrics::gauge!("test-gauge", "pid" => "100");
            for _ in 0..10 {
                counter.increment(1);
                gauge.set(1.0);
            }
        });
        // receive 20 metrics events
        let h = std::thread::spawn(move || {
            let mut metrics_events = MetricsEvents::new();
            while let Ok(event) = rx.recv_timeout(std::time::Duration::from_secs(1)) {
                metrics_events.push(event)
            }
            assert_eq!(metrics_events.0.len(), 20);
            let vec = metrics_events.to_vec_u8();
            print!("vec={:?}", &vec);
            // let s = std::str::from_utf8(&vec).unwrap();
            // let metrics_events2 = MetricsEvents::from_str(s);
            // println!(
            //     "len1={}, len2={}",
            //     metrics_events.0.len(),
            //     metrics_events2.0.len(),
            // );
            // assert_eq!(metrics_events, metrics_events2);
        });
        h.join().unwrap();
    }
}
