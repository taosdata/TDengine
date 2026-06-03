---
title: Configuration Reference
sidebar_label: Configuration Reference
toc_max_heading_level: 4
---

The taosX-Agent configuration file is located at `/etc/taos/agent.toml` (Linux) or `C:\TDengine\cfg\agent.toml` (Windows) and uses the TOML format.

## Basic Configuration

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `endpoint` | Yes | — | gRPC service address of taosX, e.g., `http://localhost:6055` |
| `token` | Yes | — | Token generated when creating the Agent in Explorer |
| `instanceId` | No | — | Agent instance ID. Must be unique when running multiple Agent instances on the same machine |
| `compression` | No | `false` | Whether to enable communication data compression between Agent and taosX |
| `in_memory_cache_capacity` | No | `64` | Maximum number of message batches that can be cached in memory. Must be greater than 0 |
| `keep_online` | No | `true` | Whether to keep the Agent running and attempt to reconnect when the taosX service is unavailable or the connection is lost |

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
# taosX gRPC service address (required)
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
