---
title: taosX Agent Reference
sidebar_label: taosX Agent
slug: /tdengine-reference/components/taosx-agent
---

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section discusses how to deploy `Agent` (for `taosX`). Before using it, you need to install the TDengine Enterprise package. taosX-Agent is used in some data access scenarios, such as Pi, OPC UA, OPC DA, etc., where there are certain restrictions on accessing data sources or the network environment is special. In such cases, taosX-Agent can be deployed close to the data source or even on the same server as the data source, and it is responsible for reading data from the data source and sending it to taosX.

## Configuration

The default configuration file for `Agent` is located at `/etc/taos/agent.toml`, and includes the following configuration items:

- `endpoint`: Required, the GRPC service address of `taosX`.
- `token`: Required, the Token generated when creating `Agent` in `Explorer`.
- `instanceId`: The instance ID of the current taosx-agent service. If multiple taosx-agent instances are started on the same machine, it is necessary to ensure that the instance IDs of each instance are unique.
- `compression`: Optional, can be configured as `true` or `false`, default is `false`. If set to `true`, it enables data compression in communication between `Agent` and `taosX`.
- `in_memory_cache_capacity`: Optional, signifies the maximum number of message batches that can be cached in memory and can be configured as a positive integer greater than zero. The default value is set at 64.
- `log_level`: Optional, log level, default is `info`. Like `taosX`, it supports five levels: `error`, `warn`, `info`, `debug`, `trace`. Deprecated, please use `log.level` instead.
- `log_keep_days`: Optional, the number of days to keep logs, default is `30` days. Deprecated, please use `log.keepDays` instead.
- `log.path`: The directory where log files are stored.
- `log.level`: Log level, options are "error", "warn", "info", "debug", "trace".
- `log.compress`: Whether to compress the log files after rolling.
- `log.rotationCount`: The maximum number of log files to keep in the directory, older files are deleted when this number is exceeded.
- `log.rotationSize`: The file size that triggers log rolling (in bytes), a new file is created when the log file exceeds this size, and new logs are written to the new file.
- `log.reservedDiskSize`: The threshold of remaining disk space to stop writing logs (in bytes), logging stops when the disk space reaches this size.
- `log.keepDays`: The number of days to keep log files, older log files are deleted after this period.

As shown below:

```toml
# taosX service endpoint
#
#endpoint = "http://localhost:6055"

# !important!
# Uncomment it and copy-paste the token generated in Explorer.
#
#token = ""

# server instance id
# 
# The instanceId of each instance is unique on the host
# instanceId = 48

# enable communication data compression between Agent and taosX
#
#compression = true

# In-memory cache capacity
#
#in_memory_cache_capacity = 64

# log configuration
[log]
# All log files are stored in this directory
# 
#path = "/var/log/taos" # on linux/macOS
#path = "C:\\TDengine\\log" # on windows

# log filter level
#
#level = "info"

# Compress archived log files or not
# 
#compress = false

# The number of log files retained by the current explorer server instance in the `path` directory
# 
#rotationCount = 30

# Rotate when the log file reaches this size
# 
#rotationSize = "1GB"

# Log downgrade when the remaining disk space reaches this size, only logging `ERROR` level logs
# 
#reservedDiskSize = "1GB"

# The number of days log files are retained
#
#keepDays = 30
```

You don't need to be confused about how to set up the configuration file. Read and follow the prompts in `Explorer` to create an `Agent`, where you can view, modify, and check the configuration file.

## Start

On Linux systems, the `Agent` can be started with the Systemd command:

```shell
systemctl start taosx-agent
```

On Windows systems, find the taosx-agent service through the system management tool "Services", and then start it.

## Troubleshooting

You can view the log files or use the `journalctl` command to view the logs of the `Agent`.

The command to view logs with `journalctl` on Linux is as follows:

```shell
journalctl -u taosx-agent [-f]
```
