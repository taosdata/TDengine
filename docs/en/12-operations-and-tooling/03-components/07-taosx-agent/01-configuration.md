---
title: taosX-Agent Configuration Reference
sidebar_label: Configuration Reference
toc_max_heading_level: 4
---

The taosX-Agent configuration file is located at `/etc/taos/agent.toml` (Linux) or `C:\TDengine\cfg\agent.toml` (Windows) and uses the TOML format.

## Basic Configuration

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `endpoint` | Yes | — | gRPC service address of taosX, for example `http://localhost:6055`. Multiple addresses can be separated by commas. See [High Availability](./03-high-availability.md) |
| `token` | Yes | — | Token generated when creating the Agent in Explorer |
| `instanceId` | No | — | Agent instance ID. Must be unique when running multiple Agent instances on the same machine |
| `compression` | No | `false` | Whether to enable communication data compression between Agent and taosX |
| `in_memory_cache_capacity` | No | `64` | Maximum number of message batches that can be cached in memory. Must be greater than 0 |
| `keep_online` | No | `true` | Whether to keep the Agent running and attempt to reconnect when the taosX service is unavailable or the connection is lost |

## Resource Limits

Agent supports the same `[resource]` configuration as taosX to limit data-plane resource usage for the Agent process and its registered connector children. Limits apply independently to each Agent process and require an Agent restart after changes. The governor is disabled when `[resource]` is absent; set `resource.enabled = false` for a complete rollback.

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `resource.enabled` | No | `true` (when `[resource]` exists) | Whether to enable the resource governor; `false` disables it completely |
| `resource.maxWriteMBps` | No | `0` | Data-plane egress limit in MB/s; `0` disables the limit |
| `resource.maxMemoryMB` | No | `0` | Soft RSS budget for the process and children in MiB; `0` derives the limit from cgroup, and no cgroup limit disables it |
| `resource.maxPauseSecs` | No | `10` | Maximum pause duration for any brake in seconds; taosX resumes with a warning when reached |
| `resource.maxCpuPercent` | No | `0` | CPU high watermark as a percentage of all cores; crossing it pauses data-plane writes; `0` disables the limit |
| `resource.cpuResumePercent` | No | `0` | CPU resume watermark; `0` uses `maxCpuPercent * 0.8` and must be lower than `maxCpuPercent` |
| `resource.maxCpuCores` | No | `0` | Maximum Agent runtime worker threads; `0` means unlimited. This is not a CPU usage cap |
| `resource.cgroupPath` | No | — | Optional cgroup directory; by default resolved from `/proc/self/cgroup` |
| `resource.cpuPriority` | No | `0` | Optional Linux nice value from `1` to `19`; ignored with a warning on other platforms |

For full behavior and recommendations, see [Resource Usage Control](../../../08-data-ingest-and-delivery/06-resource-control.md). The Agent does not export `resource_*` metrics and does not provide a local metrics endpoint; use the Agent log to observe `resource ... armed`, pause/resume, and max-pause events. Connector subprocess metrics use the real `taosx_id` of the endpoint they are connected to.

## Client Port Range

When Agent creates a connection to taosX, the client randomly uses a port. You can limit the port range with the `[client_port_range]` configuration:

| Parameter | Required | Default | Range | Description |
| --- | --- | --- | --- | --- |
| `client_port_range.min` | No | `49152` | 49152–65535 | Minimum value of the client port range |
| `client_port_range.max` | No | `65535` | 49152–65535 | Maximum value of the client port range |

## Log Configuration

Configure logging behavior with the `[log]` section:

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `log.path` | No | `/var/log/taos` (Linux)<br/>`C:\TDengine\log` (Windows) | Directory where log files are stored |
| `log.level` | No | `info` | Log level. Options: `error`, `warn`, `info`, `debug`, `trace` |
| `log.compress` | No | `false` | Whether to compress log files after rolling |
| `log.rotationCount` | No | `30` | Maximum number of log files to retain in the directory. Older files are deleted when exceeded |
| `log.rotationSize` | No | `1GB` | File size that triggers log rolling. A new file is created when exceeded |
| `log.reservedDiskSize` | No | `1GB` | Stops writing logs when remaining disk space falls below this value |
| `log.keepDays` | No | `30` | Number of days to retain log files. Older logs are deleted |

:::note
`log_level` and `log_keep_days` are legacy parameters and have been deprecated. Use `log.level` and `log.keepDays` instead.
:::

## Full Configuration Example

```toml
# taosX gRPC service address (required). Separate multiple addresses with commas
endpoint = "http://192.168.1.100:6055"

# Token generated when creating the Agent in Explorer (required)
token = "your-agent-token-here"

# Agent instance ID (must be unique when running multiple instances on the same machine)
# instanceId = 48

# Enable communication data compression
# compression = true

# In-memory cache batch count
# in_memory_cache_capacity = 64

# Keep the agent running and reconnect when taosX is unavailable
keep_online = true

# Resource limits (enable as needed)
[resource]
# maxWriteMBps = 0
# maxMemoryMB = 0
# maxPauseSecs = 10
# maxCpuPercent = 0
# cpuResumePercent = 0
# maxCpuCores = 0
# cgroupPath = "/sys/fs/cgroup"
# cpuPriority = 0

# Client port range
[client_port_range]
# min = 49152
# max = 65535

# Log configuration
[log]
# path = "/var/log/taos"
# level = "info"
# compress = false
# rotationCount = 30
# rotationSize = "1GB"
# reservedDiskSize = "1GB"
# keepDays = 30
```
