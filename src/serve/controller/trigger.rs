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
    #[default]
    Always,
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
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default, ToSchema)]
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

impl Schedule {
    pub(crate) fn is_repeatable_job(&self) -> bool {
        matches!(self, Schedule::Cron(_)) || matches!(self, Schedule::RepeatedWithStartAt(_, _))
    }
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

// trait FatalErrorExt {
//     fn is_fatal_error(&self) -> bool;
// }

// impl FatalErrorExt for anyhow::Error {
//     fn is_fatal_error(&self) -> bool {
//         let err = format!("{:#}", self);

//         if err.contains("0xE00")
//             || err.contains("0x000B")
//             || err.contains("WebSocket internal error")
//             || err.contains("WebSocket protocol error")
//         {
//             // Websocket error, connection error should not be fatal.
//             return false;
//         }

//         true
//     }
// }
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

    // Similar to `should_stop` but also check the result.
    // pub fn should_stop_with(&self, result: &Result<(), anyhow::Error>) -> bool {
    //     match self {
    //         StopCondition::Never => false,
    //         StopCondition::Done => match result {
    //             Ok(_) => true,
    //             Err(_) => false,
    //         },
    //         StopCondition::Fatal => match result {
    //             Ok(_) => false,
    //             Err(err) => {
    //                 return err.is_fatal_error();
    //             }
    //         },
    //         StopCondition::Unhealthy => result.is_err(),
    //         StopCondition::Repeated(atomic) => {
    //             atomic.load(std::sync::atomic::Ordering::Relaxed) > 0
    //         }
    //     }
    // }

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
            StopCondition::Never => false,
            StopCondition::Done => false,
            StopCondition::Fatal => true,
            StopCondition::Unhealthy => true,
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
            resume: ResumeStrategy::Always,
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

impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for Strategy
where
    &'r str: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::database::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let v: &'r str = sqlx::Decode::decode(value)?;
        Self::from_str(v).map_err(|err| Box::new(err) as _)
    }
}
impl<'q, DB: sqlx::Database> sqlx::encode::Encode<'q, DB> for Strategy
where
    String: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let val = serde_json::to_string(&self).unwrap();
        <String as sqlx::encode::Encode<'q, DB>>::encode(val, buf as _)
    }
}

impl<'t, DB: sqlx::Database> sqlx::Type<DB> for Strategy
where
    &'t str: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <&'t str as sqlx::Type<DB>>::type_info()
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
        assert_eq!(s.resume, ResumeStrategy::Always);
        assert_eq!(s.upcoming, None);
        assert_eq!(s.interval, None);

        let s = r#"{"interval": null}"#;
        let s: Strategy = serde_json::from_str(s).unwrap();
        assert_eq!(s.schedule, None);
        assert_eq!(s.resume, ResumeStrategy::Always);
        assert_eq!(s.upcoming, None);
        assert_eq!(s.interval, None);

        let s = r#"{"interval": "1s"}"#;
        let s: Strategy = serde_json::from_str(s).unwrap();
        assert_eq!(s.schedule, None);
        assert_eq!(s.resume, ResumeStrategy::Always);
        assert_eq!(s.upcoming, None);
        assert_eq!(s.interval, Some(("1s".to_string(), Duration::from_secs(1))));

        let s = r#"{"schedule": "oneshot", "resume": "never"}"#;
        let s: Strategy = serde_json::from_str(s).unwrap();
        assert_eq!(s.schedule, Some("oneshot".to_string()));
        assert_eq!(s.resume, ResumeStrategy::Never);
    }
}

#[cfg(test)]
mod additional_tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn strategy_is_none_detects_non_default_fields() {
        let default = Strategy::default();
        assert!(default.is_none());

        let with_schedule = Strategy {
            schedule: Some("cron:0 * * * *".to_string()),
            ..Strategy::default()
        };
        assert!(!with_schedule.is_none());
    }

    #[test]
    fn schedule_repeatable_detection_matches_variants() {
        let cron = Schedule::Cron("0 * * * *".to_string());
        assert!(cron.is_repeatable_job());

        let with_start = Schedule::RepeatedWithStartAt(Duration::from_secs(5), Utc::now());
        assert!(with_start.is_repeatable_job());

        assert!(!Schedule::Oneshot.is_repeatable_job());
        assert!(!Schedule::Repeated(Duration::from_secs(1)).is_repeatable_job());
    }

    #[test]
    fn stop_condition_repeated_and_tick_behaviour() {
        let counter = Arc::new(AtomicU64::new(2));
        let cond = StopCondition::Repeated(counter);
        assert!(!cond.should_stop());
        cond.tick();
        assert!(!cond.should_stop());
        cond.tick();
        assert!(cond.should_stop());
        cond.tick();
        assert!(cond.should_stop());
    }

    #[test]
    fn stop_condition_should_stop_with_respects_result_and_variant() {
        let ok: anyhow::Result<()> = Ok(());
        let err: anyhow::Result<()> = Err(anyhow!("boom"));

        assert!(!StopCondition::Never.should_stop_with(&ok));
        assert!(!StopCondition::Never.should_stop_with(&err));

        assert!(StopCondition::Done.should_stop_with(&ok));
        assert!(!StopCondition::Done.should_stop_with(&err));

        assert!(StopCondition::Fatal.should_stop_with(&ok));
        assert!(StopCondition::Fatal.should_stop_with(&err));

        assert!(StopCondition::Unhealthy.should_stop_with(&ok));
        assert!(StopCondition::Unhealthy.should_stop_with(&err));

        let repeated = StopCondition::Repeated(Arc::new(AtomicU64::new(1)));
        assert!(repeated.should_stop_with(&ok));

        let repeated_zero = StopCondition::Repeated(Arc::new(AtomicU64::new(0)));
        assert!(!repeated_zero.should_stop_with(&err));
    }

    #[test]
    fn strategy_stop_condition_matches_resume_and_schedule() {
        let cron = Strategy {
            schedule: Some("0 * * * *".into()),
            ..Default::default()
        };
        assert!(matches!(cron.stop_condition(), StopCondition::Never));

        let oneshot = Strategy {
            schedule: Some("oneshot".into()),
            resume: ResumeStrategy::Never,
            ..Default::default()
        };
        assert!(matches!(oneshot.stop_condition(), StopCondition::Done));

        let retries = Strategy {
            resume: ResumeStrategy::Retries(3),
            ..Default::default()
        };
        if let StopCondition::Repeated(counter) = retries.stop_condition() {
            assert_eq!(counter.load(Ordering::Relaxed), 3);
        } else {
            panic!("expected repeated stop condition");
        }
    }
}
