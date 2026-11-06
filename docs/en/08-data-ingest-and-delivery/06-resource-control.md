---
sidebar_label: Resource Usage Control
title: Resource Usage Control
description: Use taosX and taosX-Agent [resource] settings to limit memory, CPU, and network usage for data ingestion tasks
toc_max_heading_level: 4
---

When a data ingestion task is catching up on historical data, processing many CSV files, or consuming from a high-throughput source, its memory, CPU, or network usage can spike and affect TDengine or other services on the same host. taosX and taosX-Agent provide process-level, configurable `[resource]` limits that regulate the data plane without changing the task DSN or SQL.

## When to Use

- A Kafka or MQTT consumer's memory grows continuously after a backlog builds up.
- Importing a large number of CSV or Parquet files causes network or memory peaks.
- Writing from InfluxDB, PI, or another source drives connector subprocess CPU too high.
- Multiple ingestion tasks run on one host and taosX needs reserved capacity.

## Configuration Location

| Process | Configuration File | Scope |
| --- | --- | --- |
| taosX serve mode | `/etc/taos/taosx.toml` (Linux) or `C:\TDengine\cfg\taosx.toml` (Windows) | taosX and its connector subprocesses |
| taosX command-line mode | `taosx run` `--resource.*` options or `TAOSX_RESOURCE_*` environment variables | The current `taosx run` process |
| taosX-Agent | `/etc/taos/agent.toml` (Linux) or `C:\TDengine\cfg\agent.toml` (Windows) | Agent and its connector subprocesses |

Limits apply independently to each process. Restart the process after changing the configuration; edits are not reloaded at runtime.

The resource governor is fully disabled when the `[resource]` section is absent. When the section exists, `maxMemoryMB=0` derives the budget from the current process's cgroup memory limit. To keep the section but disable the governor completely, set `enabled = false`.

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `enabled` | `true` (when `[resource]` exists) | Whether to enable the resource governor. The governor stays disabled when `[resource]` is absent. Set to `false` for a complete rollback |
| `maxWriteMBps` | `0` | Data-plane egress limit in MB/s (1 MB = 1024 * 1024 bytes); `0` disables it. A token bucket allows a 1-second burst |
| `maxMemoryMB` | `0` | Soft RSS budget in MiB for the process and registered children. `0` derives the budget from the cgroup memory limit; no cgroup limit disables the memory brake |
| `maxPauseSecs` | `10` | Maximum pause duration in seconds for any brake; taosX resumes with a warning when reached |
| `maxCpuPercent` | `0` | CPU high watermark as a percentage of all cores; crossing it pauses data-plane writes; `0` disables it |
| `cpuResumePercent` | `0` | CPU resume watermark; `0` uses `maxCpuPercent * 0.8` and must be lower than `maxCpuPercent` |
| `maxCpuCores` | `0` | Maximum runtime worker threads; `0` means unlimited. This sizes tokio pools only and is not a CPU usage cap |
| `cgroupPath` | — | Optional cgroup directory; by default resolved from `/proc/self/cgroup` |
| `cpuPriority` | `0` | Optional Linux nice value from `1` to `19`; ignored with a warning on other platforms |

## Recommended Settings

### Control Memory First

For Kafka backlog consumption and bulk file imports, start with a memory budget:

```toml
[resource]
# Reserve about 2 GiB for taosX and its connector children
maxMemoryMB = 2048
# Resume after at most 10 seconds to avoid stalling a task
maxPauseSecs = 10
```

Memory is a soft limit, not a hard cap. If the live working set stays above the budget, taosX pauses and resumes in `maxPauseSecs` cycles. Add a container or cgroup memory limit when a hard cap is required.

### Limit Egress Bandwidth

When the network is the bottleneck, set `maxWriteMBps`. For example, limit to 20 MB/s:

```toml
[resource]
maxWriteMBps = 20
```

Bytes are charged when they are handed to the sink. Kafka/MQTT protocol overhead and Parquet compression differences are not included. DDL, metadata, IPC ack/control, and heartbeats are not charged.

### Limit CPU

When CPU is the bottleneck, use `maxCpuPercent` and `cpuResumePercent`:

```toml
[resource]
# Pause writes above 50% of all cores; resume below 30%
maxCpuPercent = 50
cpuResumePercent = 30
```

CPU limiting uses duty-cycle feedback: instantaneous CPU can still fluctuate during a pause, but the time average is bounded. `maxCpuCores` only reduces the number of tokio workers and does not replace `maxCpuPercent`.

### Combined Example

```toml
[resource]
maxWriteMBps = 20
maxMemoryMB = 2048
maxPauseSecs = 10
maxCpuPercent = 50
cpuResumePercent = 30
```

Set `maxMemoryMB` or `maxWriteMBps` for the primary bottleneck first, then add CPU limits if needed. Very low budgets cause frequent pause/resume cycles and can reduce throughput.

## Behavior

- **Memory**: reaching the budget pauses data-plane reads; batches already in the pipeline continue to be written and drained. The task is not failed and data is not dropped. A pause lasts at most `maxPauseSecs`.
- **Bandwidth**: the token bucket starts with one second of burst. Batches larger than the bucket are delayed, not rejected; the long-term rate converges to `maxWriteMBps`.
- **CPU**: crossing the high watermark pauses data-plane writes; writes resume below the resume watermark. Pauses are also bounded by `maxPauseSecs`.
- **Control plane**: DDL, metadata, IPC ack/control, heartbeats, and xnode/agent management traffic are neither charged nor paused.
- **Task semantics**: resource limits do not change task success/failure semantics. Every pause is bounded so a task cannot stall forever on an unreachable budget.

## Monitoring and Troubleshooting

When `[monitor]` is configured for taosX serve mode, resource-governor metrics are exported to taosKeeper and written to the `log` database (by default), keyed by `taosx_id`. The TDinsight for taosX dashboard includes a Resource Governor row. Key metrics:

- `resource_rss_bytes`: total RSS of the process and children
- `resource_cpu_percent`: CPU usage of the process and children
- `resource_egress_bytes_total`: cumulative charged egress bytes
- `resource_bucket_wait_ms_total`: cumulative token-bucket wait time
- `resource_brake_paused`: whether a brake is currently pausing the data plane
- `resource_brake_max_pause_total` / `resource_cpu_brake_max_pause_total`: number of max-pause releases

A `memory brake max pause expired` or `cpu brake max pause expired` log means the working set or CPU baseline exceeds the configured budget. Increase the budget or combine it with a cgroup limit, and check for leaks or fragmentation.

The Agent does not export `resource_*` metrics and does not provide a local metrics endpoint; use the Agent log to observe `resource ... armed`, pause/resume, and max-pause events. Connector subprocess metrics use the real `taosx_id` of the endpoint they are connected to.

## Known Limitations

- The memory limit is a **soft governor**, not a hard RSS cap. Use cgroup or container limits when a hard cap is required.
- MQTT and SparkplugB are push-based sources and do not use the read-side memory brake (pausing `poll()` can affect keepalive/PUBACK). Bandwidth and CPU limits still apply.
- Internal reads and buffers of plugin/subprocess sources are outside the Rust read gate. Run a resource smoke test before enabling limits in production.
- cgroup v1 parsing is covered by unit tests but has no real v1 host end-to-end test. Windows Agent resource-limit end-to-end testing is not covered.

## References

- [taosX Configuration Reference](../12-operations-and-tooling/03-components/06-taosx.md#configuration)
- [taosX-Agent Configuration Reference](../12-operations-and-tooling/03-components/07-taosx-agent/01-configuration.md#resource-limits)
- [Operations Monitoring](../12-operations-and-tooling/02-operations/05-monitor.md#resource-governor-metrics)
