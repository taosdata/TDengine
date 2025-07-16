use std::{
    fmt::Debug,
    iter::Sum,
    ops::{Add, AddAssign},
    sync::Arc,
    time::Duration,
};

use crate::core_metrics::CoreMetrics;
use bon::Builder;
use chrono::{DateTime, Utc};
use ringbuf::{
    traits::{Consumer, Observer, RingBuffer},
    HeapRb,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIs, EnumString, VariantNames};
use tokio::{sync::broadcast, task::AbortHandle};
use tracing::{instrument, Instrument};

/// Health state machine.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Default,
    Display,
    Clone,
    Copy,
    EnumIs,
    EnumString,
    VariantNames,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
)]
#[repr(u8)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum State {
    #[default]
    Initial = 0, // 初始化
    Ready,       // 准备就绪
    Idle,        // 空闲
    Active,      // 活跃
    Pending,     // 源正常，写入端等待
    Busy,        // 繁忙
    Bounce,      // 偶发错误
    SourceError, // 数据源错误
    SinkError,   // 写入端错误
    Fatal,       // 致命错误
}

impl State {
    /// Tick the state machine.
    fn tick(self, state: State) -> State {
        if self == state {
            return self;
        }
        if self.is_initial() && matches!(state, State::Idle | State::Active | State::Pending) {
            return Self::Ready;
        }
        if state.is_initial() {
            return self;
        }
        state
    }
}

impl From<State> for u8 {
    fn from(state: State) -> u8 {
        state as u8
    }
}

impl From<u8> for State {
    fn from(value: u8) -> State {
        unsafe { std::mem::transmute(value) }
    }
}

const DEFAULT_HEALTH_CHECK_INTERVAL: u32 = 5;
const MAX_HEALTH_CHECK_INTERVAL: u32 = 60;
const DEFAULT_HEALTH_CHECK_WINDOW: u32 = 60;
const DEFAULT_BUSY_THRESHOLD: f64 = 1.0;
const DEFAULT_MAX_QUEUE_LENGTH: usize = 1000;
const DEFAULT_MAX_ERRORS_IN_WINDOW: usize = 10;
const DEFAULT_BROADCAST_CAPACITY: usize = 64;

#[derive(Debug, Clone, Copy, Builder, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct HealthOpts {
    /// The health check window in seconds.
    #[builder(default = DEFAULT_HEALTH_CHECK_WINDOW)]
    pub health_check_window_in_second: u32,
    /// The health check interval in seconds.
    #[builder(setters(vis = "", name = health_check_interval_in_second_internal), default = DEFAULT_HEALTH_CHECK_INTERVAL)]
    pub health_check_interval_in_second: u32,
    /// The busy threshold.
    #[builder(default = DEFAULT_BUSY_THRESHOLD)]
    pub busy_threshold: f64,
    /// The maximum queue length.
    #[builder(default = DEFAULT_MAX_QUEUE_LENGTH)]
    pub max_queue_length: usize,
    /// The maximum errors in the window, error number large than this will report as fatal.
    #[builder(default = DEFAULT_MAX_ERRORS_IN_WINDOW)]
    pub max_errors_in_window: usize,
    #[builder(default)]
    /// Repeat errors in event broadcast.
    pub repeat_errors: bool,
    #[builder(default = DEFAULT_BROADCAST_CAPACITY)]
    pub broadcast_capacity: usize,
}

impl<S: health_opts_builder::State> HealthOptsBuilder<S> {
    pub fn health_check_interval_in_second(
        self,
        value: u32,
    ) -> HealthOptsBuilder<health_opts_builder::SetHealthCheckIntervalInSecond<S>>
    where
        S::HealthCheckIntervalInSecond: health_opts_builder::IsUnset,
    {
        match value {
            0 => self.health_check_interval_in_second_internal(DEFAULT_HEALTH_CHECK_INTERVAL),
            1..=MAX_HEALTH_CHECK_INTERVAL => self.health_check_interval_in_second_internal(value),
            _ => self.health_check_interval_in_second_internal(MAX_HEALTH_CHECK_INTERVAL),
        }
    }
}

impl HealthOpts {
    pub const fn new() -> Self {
        Self {
            health_check_window_in_second: DEFAULT_HEALTH_CHECK_WINDOW,
            health_check_interval_in_second: DEFAULT_HEALTH_CHECK_INTERVAL,
            busy_threshold: DEFAULT_BUSY_THRESHOLD,
            max_queue_length: DEFAULT_MAX_QUEUE_LENGTH,
            max_errors_in_window: DEFAULT_MAX_ERRORS_IN_WINDOW,
            repeat_errors: false,
            broadcast_capacity: DEFAULT_BROADCAST_CAPACITY,
        }
    }
}

impl Default for HealthOpts {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct ErrorMetrics {
    source: u32,
    sink: u32,
    transform: u32,
    framework: u32,
}

impl AddAssign for ErrorMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.source += rhs.source;
        self.sink += rhs.sink;
        self.transform += rhs.transform;
        self.framework += rhs.framework;
    }
}

impl Add for ErrorMetrics {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            source: self.source + rhs.source,
            sink: self.sink + rhs.sink,
            transform: self.transform + rhs.transform,
            framework: self.framework + rhs.framework,
        }
    }
}
impl<'a> Sum<&'a Self> for ErrorMetrics {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + *x)
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct MessageMetrics {
    source_messages: u32,
    sink_messages: u32,
}

impl MessageMetrics {
    #[inline]
    fn in_queue(&self) -> u32 {
        self.source_messages.saturating_sub(self.sink_messages)
    }
}

impl Add for MessageMetrics {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            source_messages: self.source_messages + rhs.source_messages,
            sink_messages: self.sink_messages + rhs.sink_messages,
        }
    }
}

impl AddAssign for MessageMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.source_messages += rhs.source_messages;
        self.sink_messages += rhs.sink_messages;
    }
}

impl<'a> Sum<&'a Self> for MessageMetrics {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + *x)
    }
}

/// Health checker implementation.
///
/// The health checker is a tool to monitor the health of the system. It collects the health data
/// and updates the health state based on the data.
///
/// The health data includes the error count, the message progress metrics, and the current state.
///
/// The health state is a state machine that has the following states:
/// - Initial: The initial state.
/// - Ready: The system is ready.
/// - Idle: The system is idle.
/// - Busy: The system is busy.
/// - Bounce: The system is bouncing.
/// - SourceError: The system has a source error.
/// - SinkError: The system has a sink error.
/// - Fatal: The system has a fatal error.
///
struct HealthChecker {
    /// The health check options.
    options: HealthOpts,
    /// The capacity of the queues.
    cap: usize,
    /// The error count queue.
    errors_in_window: HeapRb<ErrorMetrics>,
    /// The messages progress metrics in window.
    messages_in_window: HeapRb<MessageMetrics>,
    /// Total errors.
    total_errors: ErrorMetrics,
    /// The total messages progress metrics.
    total_messages: MessageMetrics,
    /// The last timestamp in seconds we recorded the health data.
    started_at: i64,
    window_start: i64,
    /// The current health state.
    state: State,
    updated_at: i64,

    busy: bool,

    broadcast: broadcast::Sender<HealthNotify>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthNotify {
    pub at: DateTime<Utc>,
    pub state: State,
    pub error: ErrorMetrics,
    pub message: MessageMetrics,
}

pub type HealthSubscriber = broadcast::Receiver<HealthNotify>;

impl Debug for HealthChecker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthChecker")
            .field("options", &self.options)
            .field("cap", &self.cap)
            .field(
                "errors_in_window",
                &self.errors_in_window.iter().collect::<Vec<_>>(),
            )
            .field(
                "messages_in_window",
                &self.messages_in_window.iter().collect::<Vec<_>>(),
            )
            .field("total_errors", &self.total_errors)
            .field("total_messages", &self.total_messages)
            .field("started_at", &self.started_at)
            .field("window_start", &self.window_start)
            .field("state", &self.state)
            .field("updated_at", &self.updated_at)
            .field("busy", &self.busy)
            .finish()
    }
}

impl HealthChecker {
    fn new(options: HealthOpts) -> (Self, HealthSubscriber) {
        tracing::trace!("Creating health checker");
        let cap = (options.health_check_window_in_second / options.health_check_interval_in_second)
            .max(1) as _;

        let now = Utc::now().timestamp();
        let (broadcast, subscriber) = broadcast::channel(options.broadcast_capacity);
        (
            Self {
                options,
                cap,
                errors_in_window: HeapRb::new(cap),
                messages_in_window: HeapRb::new(cap),
                total_errors: ErrorMetrics::default(),
                total_messages: MessageMetrics::default(),
                started_at: now,
                window_start: now,
                state: State::Initial,
                updated_at: now,
                busy: false,
                broadcast,
            },
            subscriber,
        )
    }

    /// *Internal only* Update the health state.
    fn update_state(&mut self, state: State) {
        let last = self.state;

        if state == last {
            if self.options.repeat_errors && state > State::Idle {
                // Only repeat the state if the state priority is higher than idle.
                if let Err(err) = self.broadcast.send(HealthNotify {
                    at: Utc::now(),
                    state,
                    error: self.window_errors(),
                    message: self.window_messages(),
                }) {
                    tracing::warn!("Failed to send health notify: {:?}", err);
                }
            }
            return;
        }
        let timestamp = Utc::now().timestamp();
        tracing::info!(timestamp, %last, %state, "Health state updated");
        self.updated_at = timestamp;
        self.state = state;
        if let Err(err) = self.broadcast.send(HealthNotify {
            at: Utc::now(),
            state,
            error: self.window_errors(),
            message: self.window_messages(),
        }) {
            tracing::warn!(timestamp, "Failed to send health notify: {:?}", err);
        }
    }

    fn window_errors(&self) -> ErrorMetrics {
        self.errors_in_window.iter().sum()
    }
    fn window_messages(&self) -> MessageMetrics {
        self.messages_in_window.iter().sum()
    }

    /// *Internal only* Push the metrics to the queues.
    #[instrument(level = "trace")]
    fn push_metrics(&mut self, error: ErrorMetrics, message: MessageMetrics) {
        let now = Utc::now().timestamp();
        let window = self.options.health_check_interval_in_second * self.cap as u32;
        let window_start =
            self.started_at + (now - self.started_at) / window as i64 * window as i64;
        let period = now - self.window_start;
        tracing::trace!(%now, %window, %window_start, %period, ?error, ?message, messages_in_window = ?self.messages_in_window.iter().collect::<Vec<_>>(), "Pushing metrics");

        if self.window_start < window_start {
            (0..(window_start - self.window_start)
                / self.options.health_check_interval_in_second as i64)
                .for_each(|_| {
                    self.errors_in_window.try_pop();
                    self.messages_in_window.try_pop();
                });
        }

        let queue_index = period / self.options.health_check_interval_in_second as i64;
        if queue_index >= self.errors_in_window.occupied_len() as i64 {
            self.total_errors += error;
            self.total_messages += message;
            self.errors_in_window.push_overwrite(error);
            self.messages_in_window.push_overwrite(message);
        } else {
            self.total_errors += error;
            self.total_messages += message;
            *self.errors_in_window.iter_mut().last().unwrap() += error;
            *self.messages_in_window.iter_mut().last().unwrap() += message;
        }
        self.window_start = window_start;

        let state = self.get_state();
        self.update_state(state);
    }

    // fn tick(&mut self) {
    //     self.push_metrics(ErrorMetrics::default(), MessageMetrics::default());
    // }

    // fn push_notify(&mut self, notify: TaskNotify) {
    //     let error = match notify.source {
    //         EventSource::Source => ErrorMetrics {
    //             source: 1,
    //             ..ErrorMetrics::default()
    //         },
    //         EventSource::Sink => ErrorMetrics {
    //             sink: 1,
    //             ..ErrorMetrics::default()
    //         },
    //         EventSource::Transformer => ErrorMetrics {
    //             transform: 1,
    //             ..ErrorMetrics::default()
    //         },
    //         EventSource::Framework => ErrorMetrics {
    //             framework: 1,
    //             ..ErrorMetrics::default()
    //         },
    //     };
    //     self.push_metrics(error, MessageMetrics::default());
    // }

    /// Check if the system is busy.
    fn message_state(&self) -> State {
        if self.messages_in_window.is_empty() {
            return State::Idle;
        }
        let messages: MessageMetrics = self.messages_in_window.iter().sum();
        if messages.source_messages == 0 {
            return State::Idle;
        }
        let busy_ratio = messages.in_queue() as f64 / self.options.max_queue_length as f64;
        let busy = messages.in_queue() > 0 && busy_ratio >= self.options.busy_threshold;
        if busy {
            return State::Busy;
        }
        if messages.sink_messages == 0 {
            return State::Pending;
        }
        State::Active
    }
    fn error_state(&self) -> State {
        if self.errors_in_window.is_empty() {
            return State::Idle;
        }
        let errors: ErrorMetrics = self.errors_in_window.iter().sum();
        if errors.source > 0 {
            return State::SourceError;
        }
        if errors.sink > 0 {
            return State::SinkError;
        }
        if errors.transform > 0 || errors.framework > 0 {
            return State::Bounce;
        }
        State::Idle
    }
    fn get_state(&self) -> State {
        let last_state = self.state;
        let state = self.message_state();
        last_state.tick(state.max(self.error_state()))
    }
}

#[derive(Debug, EnumString, VariantNames, Display, Clone, Copy, PartialEq, Eq)]
#[strum(serialize_all = "snake_case")]
pub enum EventSource {
    /// 数据源错误
    Source = 0,
    /// 写入端错误
    Sink,
    /// 数据转换错误
    Transformer,
    /// Errors from the framework itself or system.
    Framework,
}

#[derive(Debug, EnumString, VariantNames, Display, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[strum(serialize_all = "snake_case")]
pub enum EventLevel {
    /// 信息
    Info = 0,
    /// 警告
    Warn,
    /// 错误
    Error,
    /// 致命错误
    Fatal,
}

#[derive(Debug, Clone)]
pub struct TaskNotify {
    pub source: EventSource,
    pub level: EventLevel,
    pub message: String,
}

impl TaskNotify {
    pub fn new(source: EventSource, level: EventLevel, message: impl Into<String>) -> Self {
        TaskNotify {
            source,
            level,
            message: message.into(),
        }
    }
    pub fn source_error(msg: impl Into<String>) -> Self {
        TaskNotify {
            source: EventSource::Source,
            level: EventLevel::Error,
            message: msg.into(),
        }
    }
    pub fn sink_error(msg: impl Into<String>) -> Self {
        TaskNotify {
            source: EventSource::Sink,
            level: EventLevel::Error,
            message: msg.into(),
        }
    }
    pub fn info(msg: impl Into<String>) -> Self {
        TaskNotify {
            source: EventSource::Framework,
            level: EventLevel::Info,
            message: msg.into(),
        }
    }
    pub fn warn(msg: impl Into<String>) -> Self {
        TaskNotify {
            source: EventSource::Framework,
            level: EventLevel::Warn,
            message: msg.into(),
        }
    }
    pub fn error(msg: impl Into<String>) -> Self {
        TaskNotify {
            source: EventSource::Framework,
            level: EventLevel::Error,
            message: msg.into(),
        }
    }
    // pub fn done() -> Self {
    //     Self::Done
    // }
}

pub type HealthNotifyReceiver = flume::Receiver<TaskNotify>;

pub fn health_checker(
    options: HealthOpts,
    rx: HealthNotifyReceiver,
    metrics: Arc<CoreMetrics>,
) -> (AbortHandle, HealthSubscriber) {
    let (checker, channel) = HealthChecker::new(options);

    let handle = tokio::spawn(
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(
                (checker.options.health_check_interval_in_second as u64 >> 1).max(1),
            ));
            tokio::pin!(rx);
            tokio::pin!(checker);

            let mut source_messages = 0;
            let mut sink_messages = 0;
            let mut metrics = || {
                let (source, sink) = (metrics.received_messages(), metrics.processed_messages());
                let metrics = MessageMetrics {
                    source_messages: (source - source_messages) as _,
                    sink_messages: (sink - sink_messages) as _,
                };
                source_messages = source;
                sink_messages = sink;
                metrics
            };
            let errors = |notify: TaskNotify| match notify.source {
                EventSource::Source => ErrorMetrics {
                    source: 1,
                    ..ErrorMetrics::default()
                },
                EventSource::Sink => ErrorMetrics {
                    sink: 1,
                    ..ErrorMetrics::default()
                },
                EventSource::Transformer => ErrorMetrics {
                    transform: 1,
                    ..ErrorMetrics::default()
                },
                EventSource::Framework => ErrorMetrics {
                    framework: 1,
                    ..ErrorMetrics::default()
                },
            };
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        tracing::trace!("Health checker ticked");
                        checker.push_metrics(Default::default(), metrics());
                    },
                    item = rx.recv_async() => {
                        match item {
                            Ok(notify) => {
                                tracing::debug!(%notify.level, %notify.source, "{}", notify.message);
                                if notify.level > EventLevel::Warn {
                                    checker.push_metrics(errors(notify), metrics());
                                }
                            }
                            Err(_) => {
                                tracing::trace!("Health checker will stopped");
                                break;
                            }
                        }
                    }
                };
            }
        }
        .instrument(tracing::info_span!("health_checker")),
    );

    (handle.abort_handle(), channel)
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::atomic::Ordering};

    use crate::sink::ipc_metric::IpcMetrics;

    use super::*;

    #[test]
    fn test_state() {
        for i in 0u8..State::VARIANTS.len() as u8 {
            let state = State::from(i);
            assert_eq!(i, u8::from(state));
            dbg!(state);
            let s = state.to_string();
            assert_eq!(s, s.to_lowercase());
        }
    }

    #[test]
    fn test_state_upgrade() {
        let state = State::Initial;
        assert_eq!(state.tick(State::Initial), State::Initial);
        assert_eq!(state.tick(State::Ready), State::Ready);
        assert_eq!(state.tick(State::Idle), State::Ready);
        assert_eq!(state.tick(State::Active), State::Ready);
        assert_eq!(state.tick(State::Pending), State::Ready);
        assert_eq!(state.tick(State::Busy), State::Busy);
        assert_eq!(state.tick(State::Bounce), State::Bounce);
        assert_eq!(state.tick(State::SourceError), State::SourceError);
        assert_eq!(state.tick(State::SinkError), State::SinkError);
        assert_eq!(state.tick(State::Fatal), State::Fatal);

        for state in State::VARIANTS.iter().skip(2) {
            let last = State::from_str(state).unwrap();

            for next in State::VARIANTS.iter() {
                let next = State::from_str(next).unwrap();
                if !next.is_initial() {
                    assert_eq!(last.tick(next), next);
                } else {
                    assert_eq!(last.tick(next), last);
                }
            }
        }
    }
    #[test]
    fn test_options_builder() {
        let opts = HealthOpts::builder()
            .health_check_interval_in_second(0)
            .busy_threshold(0.0)
            .build();
        assert_eq!(opts.busy_threshold, 0.0);
        assert_eq!(opts.health_check_window_in_second, 60);
        assert_eq!(opts.health_check_interval_in_second, 5);

        let opts = HealthOpts::builder().build();
        assert_eq!(dbg!(opts), HealthOpts::default());
        let opts = HealthOpts::builder()
            .health_check_interval_in_second(u32::MAX)
            .build();
        assert_eq!(
            opts.health_check_interval_in_second,
            MAX_HEALTH_CHECK_INTERVAL
        );
    }

    #[tokio::test]
    async fn test_health_checker() {
        // let _ = tracing_subscriber::fmt()
        //     .with_max_level(tracing::Level::TRACE)
        //     .try_init();
        let opts = HealthOpts::builder()
            .health_check_window_in_second(2)
            .health_check_interval_in_second(1)
            .max_queue_length(100)
            .max_errors_in_window(4)
            .repeat_errors(true)
            .busy_threshold(0.1)
            .broadcast_capacity(64)
            .build();

        let (tx, rx) = flume::unbounded();
        let metrics = Arc::new(CoreMetrics::IPC(IpcMetrics::default()));
        let (abort_handle, subscriber) = health_checker(opts, rx, metrics.clone());
        let mut rx = subscriber.resubscribe();
        tokio::spawn({
            let mut rx = subscriber.resubscribe();
            async move {
                while let Ok(notify) = rx.recv().await {
                    tracing::info!(sid = 0, "Received health notify: {:?}", notify);
                }
            }
            .instrument(tracing::info_span!("health_subscriber"))
        });

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        for _ in 0..2 {
            metrics.received_messages.fetch_add(100, Ordering::SeqCst);
            metrics.processed_messages.fetch_add(100, Ordering::SeqCst);
            interval.tick().await;
        }
        println!("1. Try get ready state...");
        let first = rx.try_recv().expect("Subscriber should receive notify");
        tokio::task::yield_now().await;
        assert_eq!(first.state, State::Ready);
        println!("1. OK.");
        interval.tick().await;
        interval.tick().await;
        println!("2. Try get active state...");
        let second = rx.try_recv().expect("Subscriber should receive notify");
        assert_eq!(second.state, State::Active);
        println!("2. OK.");

        for _ in 0..3 {
            metrics.received_messages.fetch_add(1, Ordering::SeqCst);
            interval.tick().await;
        }
        println!("3. Try get pending state...");
        loop {
            tokio::task::yield_now().await;
            let notify = rx.try_recv();
            if notify.is_err() {
                tracing::error!("Subscriber should receive notify");
                panic!("Subscriber should receive notify");
            }
            let notify = notify.unwrap();
            dbg!(&notify.state);
            if notify.state == State::Pending {
                println!("3. OK.");
                break;
            }
        }
        for _ in 0..4 {
            metrics.received_messages.fetch_add(100, Ordering::SeqCst);
            metrics.processed_messages.fetch_add(10, Ordering::SeqCst);
            interval.tick().await;
        }
        println!("4. Try get busy state...");
        loop {
            let notify = rx.try_recv();
            if notify.is_err() {
                tracing::error!("Subscriber should receive notify");
                panic!("Subscriber should receive notify");
            }
            let notify = notify.unwrap();
            dbg!(&notify.state);
            if notify.state == State::Busy {
                println!("3. OK.");
                break;
            }
        }
        // TD-33427: 任务不再处理消息后，状态仍是 busy
        println!("4. Try get idle state after busy...");
        loop {
            interval.tick().await;
            let notify = rx.try_recv();
            if notify.is_err() {
                tracing::error!("Subscriber should receive notify");
                panic!("Subscriber should receive notify");
            }
            let notify = notify.unwrap();
            dbg!(&notify);
            if notify.state == State::Idle {
                println!("4. OK.");
                break;
            }
        }

        println!("5. Try get source error state...");
        {
            tx.send_async(TaskNotify {
                source: EventSource::Source,
                level: EventLevel::Error,
                message: "Fake error".to_string(),
            })
            .await
            .expect("Failed to send notify");
            interval.tick().await;
        }
        loop {
            interval.tick().await;
            let notify = rx.try_recv();
            if notify.is_err() {
                tracing::error!("Subscriber should receive notify");
                panic!("Subscriber should receive notify");
            }
            let notify = notify.unwrap();
            if notify.state == State::SourceError {
                println!("5. OK...");
                break;
            }
        }

        println!("6. Try get sink error state...");
        {
            tx.send_async(TaskNotify {
                source: EventSource::Sink,
                level: EventLevel::Error,
                message: "Fake error".to_string(),
            })
            .await
            .expect("Failed to send notify");
            interval.tick().await;
        }
        loop {
            interval.tick().await;
            let notify = rx.try_recv();
            if notify.is_err() {
                tracing::error!("Subscriber should receive notify");
                panic!("Subscriber should receive notify");
            }
            let notify = notify.unwrap();
            if notify.state == State::SinkError {
                println!("6. OK...");
                break;
            }
        }
        {
            tx.send_async(TaskNotify {
                source: EventSource::Framework,
                level: EventLevel::Error,
                message: "Fake error".to_string(),
            })
            .await
            .expect("Failed to send notify");
            interval.tick().await;
        }
        {
            tx.send_async(TaskNotify {
                source: EventSource::Sink,
                level: EventLevel::Fatal,
                message: "Fake error".to_string(),
            })
            .await
            .expect("Failed to send notify");
            interval.tick().await;
        }
        // tokio::time::sleep(Duration::from_secs(5)).await;
        drop(tx);
        tracing::info!("Waiting for health checker to finish");
        tokio::time::sleep(Duration::from_secs(2)).await;
        tracing::info!("Aborting health checker");
        if !abort_handle.is_finished() {
            tracing::error!("Wait for next 5s to abort the health checker");
            tokio::time::sleep(Duration::from_secs(5)).await;
            abort_handle.abort();
        }

        subscriber
            .resubscribe()
            .try_recv()
            .expect_err("Subscriber should be closed");
    }
}
