use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;

use serde::Deserialize;
use serde_with::serde_as;

pub mod migrations;

pub use legacy::*;
pub use plugins::*;
pub use transform::Action;

pub mod core_metrics;
mod extensions;
mod fake;
mod legacy;
pub mod plugins;
pub mod s3;
pub mod taoz;
pub mod tmq;
pub mod transform;
pub mod utils;

pub mod global;
#[allow(dead_code)] // TODO: remove this
pub mod task_set;

// 全局定义的是否开启 agent 压缩的标志位
pub static AGENT_COMPRESSION: OnceLock<bool> = OnceLock::new();

shadow_rs::shadow!(build);

#[derive(clap::ValueEnum, Clone, Debug)]
enum Compression {
    None,
    Brotli,
    Bzip2,
    Deflate,
    Gzip,
    Lzma,
    Xz,
    Zlib,
    Zstd,
}

#[derive(Debug, Default)]
pub struct Transferred {
    pub stables: AtomicU32,
    pub tables: AtomicU32,
    pub records: AtomicU64,
    pub points: AtomicU64,
}

#[serde_as]
#[derive(Debug, Deserialize, Clone)]
pub struct ConnectorLicense {
    pub r#type: Option<String>,
    pub number: i64,
    pub speed: i64,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub expire: i64,
    pub expire_time: Option<String>,
}

impl ConnectorLicense {
    pub fn is_expired_day(&self) -> bool {
        let days = (chrono::Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days();

        days > self.expire && self.expire >= 0
    }

    pub fn expired_days(&self) -> Option<chrono::Duration> {
        let days = (chrono::Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days();

        if days > self.expire && self.expire >= 0 {
            Some(chrono::Duration::days((days - self.expire) as _))
        } else {
            None
        }
    }

    pub fn is_expired_second(&self) -> bool {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        seconds > self.expire as u64 && self.expire >= 0
    }

    pub fn expired_seconds(&self) -> Option<chrono::Duration> {
        let expire_time = chrono::DateTime::from_timestamp(self.expire as _, 0)?;
        let now = chrono::Utc::now();
        if expire_time > now || self.expire < 0 {
            None
        } else {
            Some(now - expire_time)
        }
    }
}

// Use public re-exports to avoid breaking changes
pub use task_set::prelude::TaskNotify;

pub type TaskNotifySender = flume::Sender<TaskNotify>;
pub type TaskNotifyReceiver = flume::Receiver<TaskNotify>;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, Utc};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_license(expire: i64) -> ConnectorLicense {
        ConnectorLicense {
            r#type: Some("test".to_string()),
            number: 1000,
            speed: 100,
            expire,
            expire_time: None,
        }
    }

    #[test]
    fn test_is_expired_day_with_expired_license() {
        // Create a license that expired 100 days ago
        let days_since_epoch =
            (Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        let license = create_license(days_since_epoch - 100);

        assert!(license.is_expired_day(), "License should be expired");
    }

    #[test]
    fn test_is_expired_day_with_valid_license() {
        // Create a license that expires 100 days in the future
        let days_since_epoch =
            (Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        let license = create_license(days_since_epoch + 100);

        assert!(!license.is_expired_day(), "License should not be expired");
    }

    #[test]
    fn test_is_expired_day_boundary_today() {
        // License expires exactly today
        let days_since_epoch =
            (Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        let license = create_license(days_since_epoch);

        assert!(
            !license.is_expired_day(),
            "License expiring today should not be expired"
        );
    }

    #[test]
    fn test_is_expired_day_with_negative_expiry() {
        // Negative expiry means never expires
        let license = create_license(-1);

        assert!(
            !license.is_expired_day(),
            "License with negative expiry should not be expired"
        );
    }

    #[test]
    fn test_expired_days_with_expired_license() {
        let days_since_epoch =
            (Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        let license = create_license(days_since_epoch - 50);

        let expired = license.expired_days();
        assert!(expired.is_some(), "Should return expired duration");
        assert_eq!(
            expired.unwrap().num_days(),
            50,
            "Should be expired for 50 days"
        );
    }

    #[test]
    fn test_expired_days_with_valid_license() {
        let days_since_epoch =
            (Utc::now().date_naive() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        let license = create_license(days_since_epoch + 50);

        assert!(
            license.expired_days().is_none(),
            "Valid license should return None"
        );
    }

    #[test]
    fn test_expired_days_with_negative_expiry() {
        let license = create_license(-1);

        assert!(
            license.expired_days().is_none(),
            "Negative expiry should return None"
        );
    }

    #[test]
    fn test_is_expired_second_with_expired_license() {
        // Create a license that expired 1 hour ago
        let seconds_since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let license = create_license((seconds_since_epoch - 3600) as i64);

        assert!(license.is_expired_second(), "License should be expired");
    }

    #[test]
    fn test_is_expired_second_with_valid_license() {
        // Create a license that expires 1 hour in the future
        let seconds_since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let license = create_license((seconds_since_epoch + 3600) as i64);

        assert!(
            !license.is_expired_second(),
            "License should not be expired"
        );
    }

    #[test]
    fn test_is_expired_second_boundary() {
        // License expires right now
        let seconds_since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let license = create_license(seconds_since_epoch as i64);

        assert!(
            !license.is_expired_second(),
            "License expiring now should not be expired"
        );
    }

    #[test]
    fn test_is_expired_second_with_negative_expiry() {
        let license = create_license(-1);

        assert!(
            !license.is_expired_second(),
            "License with negative expiry should not be expired"
        );
    }

    #[test]
    fn test_expired_seconds_with_expired_license() {
        // Create a license that expired 3600 seconds (1 hour) ago
        let seconds_since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let license = create_license((seconds_since_epoch - 3600) as i64);

        let expired = license.expired_seconds();
        assert!(expired.is_some(), "Should return expired duration");

        let duration = expired.unwrap();
        // Allow some tolerance for test execution time
        assert!(
            duration.num_seconds() >= 3599 && duration.num_seconds() <= 3601,
            "Should be expired for approximately 3600 seconds, got {}",
            duration.num_seconds()
        );
    }

    #[test]
    fn test_expired_seconds_with_valid_license() {
        let seconds_since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let license = create_license((seconds_since_epoch + 3600) as i64);

        assert!(
            license.expired_seconds().is_none(),
            "Valid license should return None"
        );
    }

    #[test]
    fn test_expired_seconds_with_negative_expiry() {
        let license = create_license(-1);

        assert!(
            license.expired_seconds().is_none(),
            "Negative expiry should return None"
        );
    }

    #[test]
    fn test_expired_seconds_with_zero_expiry() {
        // Expiry at Unix epoch (timestamp 0)
        let license = create_license(0);

        let expired = license.expired_seconds();
        assert!(
            expired.is_some(),
            "Should return expired duration for epoch"
        );
    }

    #[test]
    fn test_expired_seconds_with_far_future_timestamp() {
        // Test with a timestamp far in the future (year 2100)
        let license = create_license(4102444800); // Jan 1, 2100

        assert!(
            license.expired_seconds().is_none(),
            "Far future license should return None"
        );
    }
}
