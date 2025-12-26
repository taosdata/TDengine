use chrono::{DateTime, Utc};
use itertools::Itertools;
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use taosx_core::task_set::prelude::HealthOpts;
use taosx_core::utils;
use thiserror::Error;
use utoipa::*;

pub static DEFAULT_REPEAT_INTERVAL: OnceLock<Duration> = OnceLock::new();
const DEFAULT_REPEAT_INTERVAL_FALLBACK: Duration = Duration::from_secs(5);

pub fn init_repeat_interval(dur: Duration) {
    let _ = DEFAULT_REPEAT_INTERVAL.set(dur);
}

pub fn repeat_interval() -> Duration {
    DEFAULT_REPEAT_INTERVAL
        .get()
        .copied()
        .unwrap_or(DEFAULT_REPEAT_INTERVAL_FALLBACK)
}

/// How to resume a task.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResumeStrategy {
    Always,
    #[default]
    Never,
    Once,
    Retries(u16),
}

/// Task error handling strategy when error occurs.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default, ToSchema)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)] // keep this for future
pub enum HealthyStrategy {
    /// Raise error when task is not healthy.
    Bail,
    /// Ignore runtime errors.
    #[default]
    Ignore,
    /// Set unhealthy when task error after a timeout.
    Timeout(Duration),
    /// Set unhealthy when error rate is higher than the given value.
    ///
    /// The error rate is calculated by `threshold / duration`.
    ///
    /// For example, `error_rate(10/1m)` means the task is unhealthy when error
    /// count is more than 10 in 1 minute.
    Rate {
        threshold: u32,
        unit: Duration,
        timeout: Duration,
    },
}

impl Display for HealthyStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthyStrategy::Ignore => write!(f, "ignore"),
            HealthyStrategy::Bail => write!(f, "bail"),
            HealthyStrategy::Timeout(duration) => write!(f, "timeout({:?})", duration),
            HealthyStrategy::Rate {
                threshold,
                unit,
                timeout,
            } => {
                write!(f, "rate({threshold}/{unit:?}, {timeout:?})")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default, ToSchema)]
#[allow(dead_code)] // keep this for future
pub struct ErrorRate(u32, Duration);

impl Display for ErrorRate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{:?}", self.0, self.1)
    }
}

#[derive(Debug, Error)]
pub enum ParseErrorRateError {
    #[error("Invalid error rate: {0}. Use `count/duration` format.")]
    Format(String),
    #[error("Invalid count in error rate: {0} in `{1}`.")]
    Count(std::num::ParseIntError, String),
    #[error("Invalid duration in error rate: {0} in `{1}`.")]
    Duration(fundu::ParseError, String),
}

impl FromStr for ErrorRate {
    type Err = ParseErrorRateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.splitn(2, '/').collect_vec();
        if parts.len() < 2 {
            return Err(ParseErrorRateError::Format(s.to_string()));
        }
        let (count, duration) = (parts[0], parts[1]);
        let count = count
            .parse::<u32>()
            .map_err(|err| ParseErrorRateError::Count(err, s.to_string()))?;
        let duration = utils::parse_duration(duration)
            .map_err(|err| ParseErrorRateError::Duration(err, s.to_string()))?;
        Ok(ErrorRate(count, duration))
    }
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
#[serde(default)]
pub struct Strategy {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) schedule: Option<String>,
    pub(crate) resume: ResumeStrategy,
    pub(crate) health: HealthOpts,
    /// 任务的下次执行的日期时间
    pub(crate) upcoming: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "OptionHumanDuration")]
    pub(crate) interval: Option<(String, Duration)>,
}

serde_with::serde_conv!(
    OptionHumanDuration,
    Option<(String, Duration)>,
    |duration: &Option<(String, Duration)>| {
        match duration {
            None => None,
            Some((s, _d)) => Some(s.to_string()),
        }
    },
    |value: Option<String>| -> Result<_, fundu::ParseError> {
        match value {
            None => Ok(None),
            Some(s) => {
                let d = utils::parse_duration(&s)?;
                Ok(Some((s, d)))
            }
        }
        // let d = value.map(|value| utils::parse_duration(&value)).transpose();
    }
);

impl FromStr for Strategy {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(trigger) = serde_json::from_str::<Strategy>(s) {
            Ok(trigger)
        } else {
            debug_assert!(s.starts_with("schedule:"));
            Ok(Strategy {
                schedule: Some(s.trim_start_matches("schedule:").to_string()),
                ..Default::default()
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum Schedule {
    /// 任务按照 cron 表达式周期执行
    Cron(String),
    /// 任务只执行一次
    Oneshot,
    /// 任务出错后，以 duration 为间隔重试
    Repeated(Duration),
    /// 从 start_at 开始执行，每隔 interval 执行一次
    RepeatedWithStartAt(Duration, DateTime<Utc>),
    /// 任务出错后，以 duration 为间隔重试，最多重试次数为 limit
    RepeatedLimit(Duration, u16),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum StopCondition {
    /// Means never stop a job even in repeated or cron job.
    Never,
    /// Stop a job when it is done or fatal error.
    Done,
    /// Stop a job only when there is a fatal error.
    Fatal,
    /// Stop a job when it is unhealthy.
    Unhealthy,
    /// Stop a job when the remain repeated counter is zero.
    Repeated(Arc<AtomicU64>),
}

impl StopCondition {
    pub fn should_stop(&self) -> bool {
        match self {
            StopCondition::Never => false,
            StopCondition::Done => false,
            StopCondition::Fatal => false,
            StopCondition::Unhealthy => false,
            StopCondition::Repeated(atomic) => {
                atomic.load(std::sync::atomic::Ordering::Relaxed) == 0
            }
        }
    }

    pub fn should_stop_with_ok(&self) -> bool {
        match self {
            StopCondition::Never => false,
            StopCondition::Done => true,
            StopCondition::Fatal => true,
            StopCondition::Unhealthy => true,
            StopCondition::Repeated(atomic) => {
                atomic.load(std::sync::atomic::Ordering::Relaxed) > 0
            }
        }
    }

    pub fn should_stop_with_error(&self) -> bool {
        match self {
            StopCondition::Never | StopCondition::Done => false,
            StopCondition::Fatal | StopCondition::Unhealthy => true,
            StopCondition::Repeated(atomic) => {
                atomic.load(std::sync::atomic::Ordering::Relaxed) > 0
            }
        }
    }

    /// Tick the stop condition.
    pub fn tick(&self) {
        if let StopCondition::Repeated(atomic) = self {
            let _ = atomic.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                if v > 0 { Some(v - 1) } else { None }
            });
        }
    }

    pub(crate) fn should_stop_with(&self, result: &anyhow::Result<()>) -> bool {
        if result.is_err() {
            self.should_stop_with_error()
        } else {
            self.should_stop_with_ok()
        }
    }
}

impl Strategy {
    pub const fn const_new() -> Self {
        Self {
            schedule: None,
            resume: ResumeStrategy::Never,
            health: HealthOpts::new(),
            upcoming: None,
            interval: None,
        }
    }

    pub fn never_resume(mut self) -> Self {
        self.resume = ResumeStrategy::Never;
        self
    }

    #[allow(dead_code)]
    pub fn is_none(&self) -> bool {
        *self == Default::default()
    }

    pub fn schedule(&self) -> Schedule {
        if let Some(schedule) = self.schedule.as_deref() {
            return match schedule.to_lowercase().as_str() {
                "oneshot" => Schedule::Oneshot,
                _ => Schedule::Cron(schedule.to_string()),
            };
        }

        // repeatable job with start datetime, e.g. create a backup job which starts at 2021-01-01T02:00:00Z
        if let (Some((_raw, interval)), Some(upcoming)) =
            (self.interval.as_ref(), self.upcoming.as_ref())
        {
            return Schedule::RepeatedWithStartAt(*interval, *upcoming);
        }

        match self.resume {
            ResumeStrategy::Always => {
                let d = match &self.interval {
                    None => repeat_interval(),
                    Some((_raw, interval)) => *interval,
                };
                Schedule::Repeated(d)
            }
            ResumeStrategy::Never => Schedule::Oneshot,
            ResumeStrategy::Once => {
                let d = match &self.interval {
                    None => repeat_interval(),
                    Some((_raw, interval)) => *interval,
                };
                Schedule::RepeatedLimit(d, 1)
            }
            ResumeStrategy::Retries(num) => {
                let d = match &self.interval {
                    None => repeat_interval(),
                    Some((_raw, interval)) => *interval,
                };
                Schedule::RepeatedLimit(d, num)
            }
        }
    }

    pub fn stop_condition(&self) -> StopCondition {
        // Never stop for cron job.
        if let Some(schedule) = self.schedule.as_deref() {
            return match schedule.to_lowercase().as_str() {
                "oneshot" => StopCondition::Done,
                _ => StopCondition::Never,
            };
        }
        // Never stop for repeated job with start_at.
        if let (Some(_), Some(_)) = (self.interval.as_ref(), self.upcoming.as_ref()) {
            return StopCondition::Never;
        }

        match self.resume {
            ResumeStrategy::Always => StopCondition::Done,
            ResumeStrategy::Never => StopCondition::Fatal,
            ResumeStrategy::Once => StopCondition::Repeated(Arc::new(AtomicU64::new(1))),
            ResumeStrategy::Retries(num) => {
                StopCondition::Repeated(Arc::new(AtomicU64::new(num as _)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_serde_strategy() {
        let s = r#"{}"#;
        let s: Strategy = serde_json::from_str(s).unwrap();
        assert_eq!(s.schedule, None);
        assert_eq!(s.resume, ResumeStrategy::Never);
        assert_eq!(s.upcoming, None);
        assert_eq!(s.interval, None);

        let s = r#"{"interval": null}"#;
        let s: Strategy = serde_json::from_str(s).unwrap();
        assert_eq!(s.schedule, None);
        assert_eq!(s.resume, ResumeStrategy::Never);
        assert_eq!(s.upcoming, None);
        assert_eq!(s.interval, None);

        let s = r#"{"interval": "1s"}"#;
        let s: Strategy = serde_json::from_str(s).unwrap();
        assert_eq!(s.schedule, None);
        assert_eq!(s.resume, ResumeStrategy::Never);
        assert_eq!(s.upcoming, None);
        assert_eq!(s.interval, Some(("1s".to_string(), Duration::from_secs(1))));

        let s = r#"{"schedule": "oneshot", "resume": "never"}"#;
        let s: Strategy = serde_json::from_str(s).unwrap();
        assert_eq!(s.schedule, Some("oneshot".to_string()));
        assert_eq!(s.resume, ResumeStrategy::Never);
    }
}
