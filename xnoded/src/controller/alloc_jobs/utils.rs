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

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    use taos::Dsn;

    #[test]
    fn dsn_parse_timestamp_parses_valid_rfc3339() {
        let dsn = Dsn::from_str("taos://localhost:6030?ts=2025-01-01T00:00:00Z").unwrap();
        let ts = dsn_parse_timestamp(&dsn, "ts").unwrap().unwrap();
        assert_eq!(ts.to_rfc3339(), "2025-01-01T00:00:00+00:00");
    }

    #[test]
    fn dsn_parse_timestamp_returns_none_when_key_absent() {
        let dsn = Dsn::from_str("taos://localhost:6030").unwrap();
        let ts = dsn_parse_timestamp(&dsn, "ts").unwrap();
        assert!(ts.is_none());
    }

    #[test]
    fn dsn_parse_timestamp_returns_error_for_invalid_value() {
        let dsn = Dsn::from_str("taos://localhost:6030?ts=invalid").unwrap();
        let err = dsn_parse_timestamp(&dsn, "ts").unwrap_err();
        assert!(
            matches!(err, crate::controller::alloc_jobs::Error::InvalidTimestamp { ts, .. } if ts == "invalid")
        );
    }

    #[test]
    fn divide_timestamp_empty_nodes_returns_empty_vec() {
        let start = DateTime::from_timestamp(0, 0).unwrap();
        let res = divide_timestamp_by_memory(start, Some(start), Vec::new());
        assert!(res.is_empty());
    }

    #[test]
    fn divide_timestamp_single_node_keeps_full_range() {
        let start = DateTime::from_timestamp(0, 0).unwrap();
        let end = DateTime::from_timestamp(100, 0).unwrap();
        let res = divide_timestamp_by_memory(start, Some(end), vec![(1, 1024)]);
        assert_eq!(res.len(), 1);
        let r = &res[0];
        assert_eq!(r.xnode_id, 1);
        assert_eq!(r.time_range.start, start);
        assert_eq!(r.time_range.end, Some(end));
    }

    #[test]
    fn divide_timestamp_multiple_nodes_proportional() {
        let start = DateTime::from_timestamp(0, 0).unwrap();
        let end = DateTime::from_timestamp(100, 0).unwrap();
        let res = divide_timestamp_by_memory(start, Some(end), vec![(1, 1), (2, 3)]);
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].time_range.start, start);
        assert_eq!(res[1].time_range.end, Some(end));
        let dur1 = res[0].time_range.end.unwrap() - res[0].time_range.start;
        let dur2 = res[1].time_range.end.unwrap() - res[1].time_range.start;
        assert!(dur2 > dur1);
    }

    #[test]
    fn divide_timestamp_without_end_adds_open_range_for_max_memory_node() {
        let start = DateTime::from_timestamp(0, 0).unwrap();
        let res = divide_timestamp_by_memory(start, None, vec![(1, 1), (2, 2)]);
        assert_eq!(res.len(), 3);
        let last = &res[2];
        assert_eq!(last.xnode_id, 2);
        assert!(last.time_range.end.is_none());
    }
}
