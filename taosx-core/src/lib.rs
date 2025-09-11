use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::OnceLock;
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
pub mod types;
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
