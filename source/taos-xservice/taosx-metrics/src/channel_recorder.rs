//! 将 metrics 直接写入 Channel 的 Recorder, Channel 的接收端可以自定义如何处理这些 metrics，比如写入数据库或者发送到远端。
//!
//! # Example
//!
use std::sync::Arc;

use bincode::{Decode, Encode, config};
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

    use flume::Receiver;
    use metrics::Recorder;

    use crate::channel_recorder::MetricsEvents;

    use super::{ChannelRecorder, MetricEvent, MetricOperation};

    fn channel_recorder() -> (ChannelRecorder, Receiver<MetricEvent>) {
        let (tx, rx) = flume::unbounded();
        (ChannelRecorder::new(Arc::new(tx)), rx)
    }

    fn test_metadata() -> metrics::Metadata<'static> {
        metrics::Metadata::new(module_path!(), metrics::Level::INFO, Some(module_path!()))
    }

    fn event_key_labels(event: &MetricEvent) -> (&str, Vec<(&str, &str)>) {
        (
            event.key.as_str(),
            event
                .labels
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect(),
        )
    }

    #[test]
    fn test_counter() {
        let (recorder, rx) = channel_recorder();
        let metadata = test_metadata();
        let counter = recorder.register_counter(&metrics::Key::from_name("test"), &metadata);
        for i in 1..7 {
            counter.increment(i);
        }

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
        let (recorder, rx) = channel_recorder();
        let metadata = test_metadata();
        let key = metrics::Key::from_parts("test", &[("pid", "100")]);
        let gauge = recorder.register_gauge(&key, &metadata);
        for i in 1..7 {
            gauge.set(i as f64);
        }

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
        let (recorder, rx) = channel_recorder();
        let metadata = test_metadata();
        let counter =
            recorder.register_counter(&metrics::Key::from_name("test-counter"), &metadata);
        let gauge = recorder.register_gauge(
            &metrics::Key::from_parts("test-gauge", &[("pid", "100")]),
            &metadata,
        );

        for _ in 0..10 {
            counter.increment(1);
            gauge.set(1.0);
        }

        let mut metrics_events = MetricsEvents::new();
        while let Ok(event) = rx.try_recv() {
            metrics_events.push(event);
        }
        assert_eq!(metrics_events.0.len(), 20);
        let _ = metrics_events.to_vec_u8();
    }

    #[test]
    fn test_encode_and_decode() {
        let (recorder, rx) = channel_recorder();
        let metadata = test_metadata();
        let counter =
            recorder.register_counter(&metrics::Key::from_name("test-counter"), &metadata);
        let gauge = recorder.register_gauge(
            &metrics::Key::from_parts("test-gauge", &[("pid", "100")]),
            &metadata,
        );

        for _ in 0..10 {
            counter.increment(1);
            gauge.set(1.0);
        }

        let mut metrics_events = MetricsEvents::new();
        while let Ok(event) = rx.try_recv() {
            metrics_events.push(event);
        }

        assert_eq!(metrics_events.0.len(), 20);
        let decoded = MetricsEvents::from_slice(&metrics_events.to_vec_u8()).unwrap();
        assert_eq!(decoded, metrics_events);
    }

    #[test]
    fn metric_event_new_preserves_key_labels_and_operation() {
        let key = metrics::Key::from_parts("http_requests", &[("method", "GET"), ("code", "200")]);
        let event = MetricEvent::new(key, MetricOperation::IncrementCounter(3));

        assert_eq!(event.key, "http_requests");
        assert_eq!(
            event.labels,
            vec![
                ("method".to_string(), "GET".to_string()),
                ("code".to_string(), "200".to_string())
            ]
        );
        assert_eq!(event.operation, MetricOperation::IncrementCounter(3));
    }

    #[test]
    fn metrics_events_round_trip_all_operation_variants() {
        let mut events = MetricsEvents::new();
        for operation in [
            MetricOperation::IncrementCounter(1),
            MetricOperation::SetCounter(2),
            MetricOperation::IncrementGauge(3.5),
            MetricOperation::DecrementGauge(1.25),
            MetricOperation::SetGauge(9.75),
            MetricOperation::RecordHistogram(42.0),
        ] {
            events.push(MetricEvent::new(
                metrics::Key::from_parts("metric", &[("source", "unit")]),
                operation,
            ));
        }

        let decoded = MetricsEvents::from_slice(&events.to_vec_u8()).unwrap();

        assert_eq!(decoded, events);
        assert_eq!(decoded.len(), 6);
        assert!(!decoded.is_empty());
    }

    #[test]
    fn metrics_events_reject_invalid_bincode_payload() {
        assert!(MetricsEvents::from_slice(b"not a metrics batch").is_err());
    }

    #[test]
    fn handle_emits_counter_gauge_and_histogram_operations() {
        let (tx, rx) = flume::unbounded();
        let handle = super::Handle::new(
            metrics::Key::from_parts("worker_metric", &[("worker", "a")]),
            Arc::new(tx),
        );

        metrics::CounterFn::absolute(&handle, 10);
        metrics::CounterFn::increment(&handle, 2);
        metrics::GaugeFn::increment(&handle, 1.5);
        metrics::GaugeFn::decrement(&handle, 0.5);
        metrics::GaugeFn::set(&handle, 7.0);
        metrics::HistogramFn::record(&handle, 11.0);

        let operations = (0..6)
            .map(|_| {
                let event = rx.recv().unwrap();
                assert_eq!(
                    event_key_labels(&event),
                    ("worker_metric", vec![("worker", "a")])
                );
                event.operation
            })
            .collect::<Vec<_>>();

        assert_eq!(
            operations,
            vec![
                MetricOperation::SetCounter(10),
                MetricOperation::IncrementCounter(2),
                MetricOperation::IncrementGauge(1.5),
                MetricOperation::DecrementGauge(0.5),
                MetricOperation::SetGauge(7.0),
                MetricOperation::RecordHistogram(11.0)
            ]
        );
    }

    #[test]
    fn handle_drops_events_when_bounded_channel_is_full() {
        let (tx, rx) = flume::bounded(1);
        let handle = super::Handle::new(metrics::Key::from_name("bounded"), Arc::new(tx));

        metrics::CounterFn::increment(&handle, 1);
        metrics::CounterFn::increment(&handle, 2);

        let event = rx.recv().unwrap();
        assert_eq!(event.operation, MetricOperation::IncrementCounter(1));
        assert!(rx.try_recv().is_err());
    }
}
