---
title: taosX Agent Reference
sidebar_label: taosX Agent
slug: /tdengine-reference/components/taosx-agent
---

This section explains how to deploy the `Agent` (for `taosX`). Before using it, you need to install the TDengine Enterprise installation package. The taosX-Agent is used in certain data access scenarios, such as Pi, OPC UA, and OPC DA, where access to the data source is restricted or the network environment is special. It can be deployed close to the data source or even on the same server as the data source, with the taosX-Agent responsible for reading data from the source and sending it to taosX.

## Configuration

The default configuration file for the `Agent` is located at `/etc/taos/agent.toml`, which contains the following configuration items:

- `endpoint`: Required, the GRPC service address of `taosX`.
- `token`: Required, the token generated when creating the `Agent` on `Explorer`.
- `instanceId`: The instance ID of the current taosx-agent service. If multiple taosx-agent instances are started on the same machine, the instance IDs must be unique.
- `compression`: Optional, can be set to `true` or `false`, defaulting to `false`. When set to `true`, it enables data compression for communication between the `Agent` and `taosX`.
- `log_level`: Optional, the log level, defaulting to `info`, supporting `error`, `warn`, `info`, `debug`, and `trace`. Deprecated; please use `log.level` instead.
- `log_keep_days`: Optional, the number of days to keep logs, defaulting to `30` days. Deprecated; please use `log.keepDays` instead.
- `log.path`: Directory where log files are stored.
- `log.level`: Log level, with optional values of "error", "warn", "info", "debug", and "trace".
- `log.compress`: Whether to compress the files after rolling the logs.
- `log.rotationCount`: The maximum number of files to retain in the log file directory; older files beyond this limit are deleted.
- `log.rotationSize`: The file size (in bytes) that triggers the log file to roll; a new file is created when the log exceeds this size, and new logs will be written to the new file.
- `log.reservedDiskSize`: The threshold (in bytes) for stopping log writing when the remaining disk space reaches this size.
- `log.keepDays`: The number of days log files are retained; old log files exceeding this number will be deleted.

As shown below:

```TOML
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

You don't need to be confused about how to set the configuration file; read and follow the prompts in `Explorer` when creating the `Agent`, and you can view, modify, and check the configuration file.

## Start

On Linux, the `Agent` can be started with the Systemd command:

```bash
systemctl start taosx-agent
```

On Windows, find the taosx-agent service in the system management tool "Services" and start it.

## Troubleshooting

You can view log files or use the `journalctl` command to check the logs of the `Agent`.

On Linux, the command to view logs with `journalctl` is as follows:

```bash
journalctl -u taosx-agent [-f]
```
