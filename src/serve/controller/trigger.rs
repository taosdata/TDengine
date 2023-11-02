use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::*;

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

/// Check if the task is healthy.
///
/// Start check point is the time when the task is **running** or resumed as running
/// from an **unhealthy** state.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthyCheck {
    #[default]
    /// Raise error as healthy check failed.
    ///
    /// Equivalent to `rate(1/0)`.
    Forward,
    /// Ignore runtime errors.
    Ignore,
    /// Set unhealthy when error rate is higher than the given value.
    Rate(ErrorRate),
}

/// Task error handling strategy when error occurs.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case", default)]
pub struct Healthy {
    /// Detect if the task is healthy.
    check: HealthyCheck,
    /// Unhealthy state will fadeout after the given duration if no error occurs.
    fadeout: Duration,
}

impl Healthy {
    pub const fn const_new() -> Self {
        Self {
            check: HealthyCheck::Forward,
            fadeout: Duration::from_secs(60),
        }
    }
}

impl Default for Healthy {
    fn default() -> Self {
        Self {
            check: Default::default(),
            fadeout: Duration::from_secs(60),
        }
    }
}
/// Task error handling strategy when error occurs.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default, ToSchema)]
#[serde(rename_all = "snake_case")]
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
pub struct ErrorRate(u32, Duration);

impl Display for ErrorRate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{:?}", self.0, self.1)
    }
}

#[derive(Debug, Error)]
pub enum ParseErrorRateError {
    #[error("Invalid error rate: {0}. Use `count/duration` format.")]
    InvalidFormat(String),
    #[error("Invalid count in error rate: {0} in `{1}`.")]
    InvalidCount(std::num::ParseIntError, String),
    #[error("Invalid duration in error rate: {0} in `{1}`.")]
    InvalidDuration(parse_duration::parse::Error, String),
}

impl FromStr for ErrorRate {
    type Err = ParseErrorRateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.splitn(2, '/').collect_vec();
        if parts.len() < 2 {
            return Err(ParseErrorRateError::InvalidFormat(s.to_string()));
        }
        let (count, duration) = (parts[0], parts[1]);
        let count = count
            .parse::<u32>()
            .map_err(|err| ParseErrorRateError::InvalidCount(err, s.to_string()))?;
        let duration = parse_duration::parse(duration)
            .map_err(|err| ParseErrorRateError::InvalidDuration(err, s.to_string()))?;
        Ok(ErrorRate(count, duration.into()))
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default, ToSchema)]
#[serde(default)]
pub struct Strategy {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) schedule: Option<String>,
    pub(crate) resume: ResumeStrategy,
    pub(crate) healthy: Healthy,
    pub(crate) interval: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum Schedule {
    Cron(String),
    Oneshot,
    Repeated(Duration),
    RepeatedLimit(Duration, u16),
}
impl Schedule {
    pub(crate) fn is_cron_job(&self) -> bool {
        match self {
            Schedule::Cron(_) => true,
            _ => false,
        }
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

trait FatalErrorExt {
    fn is_fatal_error(&self) -> bool;
}

impl FatalErrorExt for anyhow::Error {
    fn is_fatal_error(&self) -> bool {
        let err = format!("{:#}", self);

        if err.contains("0xE00")
            || err.contains("0x000B")
            || err.contains("WebSocket internal error")
            || err.contains("WebSocket protocol error")
        {
            // Websocket error, connection error should not be fatal.
            return false;
        }

        true
    }
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

    /// Similar to `should_stop` but also check the result.
    pub fn should_stop_with(&self, result: &Result<(), anyhow::Error>) -> bool {
        match self {
            StopCondition::Never => false,
            StopCondition::Done => match result {
                Ok(_) => true,
                Err(_) => false,
            },
            StopCondition::Fatal => match result {
                Ok(_) => false,
                Err(err) => {
                    return err.is_fatal_error();
                }
            },
            StopCondition::Unhealthy => result.is_err(),
            StopCondition::Repeated(atomic) => {
                atomic.load(std::sync::atomic::Ordering::Relaxed) > 0
            }
        }
    }

    /// Tick the stop condition.
    #[allow(dead_code)]
    pub fn tick(&self) {
        match self {
            StopCondition::Repeated(atomic) => {
                atomic.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
            _ => (),
        }
    }
}

impl Strategy {
    pub const DEFAULT: &'static Self = &Self::const_new();

    pub const fn const_new() -> Self {
        Self {
            schedule: None,
            resume: ResumeStrategy::Always,
            healthy: Healthy::const_new(),
            interval: None,
        }
    }

    #[allow(dead_code)]
    pub fn is_none(&self) -> bool {
        *self == Default::default()
    }

    pub fn schedule(&self) -> Schedule {
        if let Some(schedule) = self.schedule.as_deref() {
            Schedule::Cron(schedule.to_string())
        } else {
            match self.resume {
                ResumeStrategy::Always => {
                    Schedule::Repeated(self.interval.unwrap_or_else(|| Duration::from_secs(5)))
                }
                ResumeStrategy::Never => Schedule::Oneshot,
                ResumeStrategy::Once => Schedule::RepeatedLimit(
                    self.interval.unwrap_or_else(|| Duration::from_secs(5)),
                    1,
                ),
                ResumeStrategy::Retries(num) => Schedule::RepeatedLimit(
                    self.interval.unwrap_or_else(|| Duration::from_secs(5)),
                    num,
                ),
            }
        }
    }

    pub fn stop_condition(&self) -> StopCondition {
        // Never stop for cron job.
        if let Some(_) = self.schedule.as_deref() {
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

impl FromStr for Strategy {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(trigger) = serde_json::from_str::<Strategy>(s) {
            return Ok(trigger);
        } else {
            debug_assert!(s.starts_with("schedule:"));
            return Ok(Strategy {
                schedule: Some(s.trim_start_matches("schedule:").to_string()),
                ..Default::default()
            });
        }
    }
}

impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for Strategy
where
    &'r str: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef,
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
        buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
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
