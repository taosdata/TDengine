use chrono::{DateTime, TimeDelta, Utc};
use snafu::ResultExt;
use taos::Dsn;

use crate::controller::alloc_jobs::{InvalidTimestampSnafu, Result};

#[derive(Debug, Clone)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: Option<DateTime<Utc>>,
}

impl TimeRange {
    fn new(start: DateTime<Utc>, end: Option<DateTime<Utc>>) -> Self {
        Self { start, end }
    }
}

pub fn dsn_parse_timestamp(dsn: &Dsn, key: &str) -> Result<Option<DateTime<Utc>>> {
    let ts = dsn
        .get(key)
        .map(|ts| DateTime::parse_from_rfc3339(ts).context(InvalidTimestampSnafu { ts }))
        .transpose()?;
    Ok(ts.map(Into::into))
}

#[derive(Debug, Clone)]
pub struct NodeTimeRange {
    pub xnode_id: i32,
    pub time_range: TimeRange,
}

pub fn divide_timestamp_by_memory(
    start: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    nodes: Vec<(i32, u64)>,
) -> Vec<NodeTimeRange> {
    if nodes.is_empty() {
        return vec![];
    }

    if nodes.len() == 1 {
        return vec![NodeTimeRange {
            xnode_id: nodes[0].0,
            time_range: TimeRange::new(start, end),
        }];
    }

    // 计算总内存
    let total_memory: u64 = nodes.iter().map(|(_, mem)| mem).sum();

    let max_memory_xnode = nodes.iter().max_by(|l, r| l.1.cmp(&r.1)).copied();

    let ts_end = match end {
        Some(end) => end,
        None => Utc::now(),
    };

    // 计算总时长
    enum TimeUnit {
        Nano,
        Micro,
        Milli,
    }
    let total_duration = ts_end - start;
    let (total, unit) = if let Some(total) = total_duration.num_nanoseconds() {
        (total, TimeUnit::Nano)
    } else if let Some(total) = total_duration.num_microseconds() {
        (total, TimeUnit::Micro)
    } else {
        (total_duration.num_milliseconds(), TimeUnit::Milli)
    };

    let mut result = Vec::with_capacity(nodes.len());
    let mut current_time = start;
    let len = nodes.len();

    for (i, (xnode_id, memory)) in nodes.into_iter().enumerate() {
        let range_start = current_time;

        let range_end = if i == len - 1 {
            ts_end
        } else {
            let ratio = memory as f64 / total_memory as f64;
            let duration = (total as f64 * ratio) as i64;
            let duration = match unit {
                TimeUnit::Nano => TimeDelta::nanoseconds(duration),
                TimeUnit::Micro => TimeDelta::microseconds(duration),
                TimeUnit::Milli => TimeDelta::milliseconds(duration),
            };
            current_time + duration
        };

        result.push(NodeTimeRange {
            xnode_id,
            time_range: TimeRange::new(range_start, Some(range_end)),
        });

        current_time = range_end;
    }

    if end.is_none()
        && let Some((xnode_id, _)) = max_memory_xnode
    {
        result.push(NodeTimeRange {
            xnode_id,
            time_range: TimeRange::new(ts_end, None),
        });
    }

    result
}
