use std::str::FromStr;

use ha_core::types::SplitJobResult;
use snafu::ResultExt;
use taos::Dsn;

use super::*;
use crate::controller::{
    alloc_jobs::utils::{NodeTimeRange, divide_timestamp_by_memory, dsn_parse_timestamp},
    xnodes::XNodes,
};

pub fn alloc_jobs(
    task: SplitJobResult,
    xnodes: &XNodes,
    via: Option<i64>,
) -> Result<AllocatedJobs> {
    let serde_json::Value::String(from) = task.from else {
        return FromDsnNotStringSnafu.fail();
    };

    let mut from = Dsn::from_str(&from).context(InvalidDsnSnafu { dsn: from })?;
    let start = dsn_parse_timestamp(&from, "start")?.context(StartTimestampNotFoundSnafu)?;
    let end = dsn_parse_timestamp(&from, "end")?;

    let availables = xnodes.available_xnodes_memory(via);
    let mut time_ranges = divide_timestamp_by_memory(start, end, availables);
    if time_ranges.len() == 1
        && let Some(NodeTimeRange {
            xnode_id,
            time_range,
        }) = time_ranges.pop()
    {
        from.set("start", time_range.start.to_rfc3339());
        if let Some(end) = time_range.end {
            from.set("end", end.to_rfc3339());
        }
        if let Some(cpus) = xnodes.cpu_cores(xnode_id) {
            from.set("read_concurrency", cpus.to_string());
        }
        return Ok(AllocatedJobs::Task(
            xnode_id,
            HaTask {
                from: from.to_string(),
                to: task.to,
                parser: task.parser,
                via,
            },
        ));
    }
    let mut jobs = Vec::with_capacity(time_ranges.len());
    for NodeTimeRange {
        xnode_id,
        time_range,
    } in time_ranges
    {
        from.set("start", time_range.start.to_rfc3339());
        if let Some(end) = time_range.end {
            from.set("end", end.to_rfc3339());
        }
        if let Some(cpus) = xnodes.cpu_cores(xnode_id) {
            from.set("read_concurrency", cpus.to_string());
        }
        jobs.push((
            xnode_id,
            HaTask {
                from: from.to_string(),
                to: task.to.clone(),
                parser: task.parser.clone(),
                via,
            },
        ));
    }
    Ok(AllocatedJobs::Jobs(jobs))
}
