---
title: Set Up taosX
sidebar_label: taosX
---

## Introduction

This section describes how to deploy `taosX`. Once the `taosX` installation package is installed, `taosX` is available in the system. For details, please refer to [Installation and Configuration](../install).

## Configuration

`taosX` supports configuration through a configuration file. On Linux, the default configuration file path is `/etc/taos/taosx.toml`, and on Windows, it is `C:\\TDengine\\cfg\\taosx.toml`. It includes the following configuration items:

- `plugins_home`: The directory for `taosX` external data source SDK.
- `data_dir`: The directory for `taosX` data file storage.
- `logs_home`: The directory for `taosX` log file storage. The `taosX` service log file has the prefix `taosx.log`, and external data sources have their own log file name prefixes.
- `log_level`: Log level string, with optional levels including: `error`, `warn`, `info`, `debug`, `trace`. The default is `info`.
- `log_keep_days`: The maximum storage days for logs. `taosX` logs will be split into different files by day.
- `jobs`: The maximum number of threads for each runtime. In service mode, the total number of threads is `jobs * 2`, and the default number of threads is `current server cores * 2`.
- `serve.listen`: The `taosX` REST API listening address. The default is `0.0.0.0:6050`.
- `serve.database_url`: The address of the `taosX` database, in the form of `sqlite:<path>`.
- `monitor.fqdn`: FQDN of taosKeeper service, no default value. If blank, disable the monitor function.
- `monitor.port`: Port of taosKeeper service, default 6043
- `monitor.interval`: How often to send metrics to taosKeeper, default every 10 seconds. Only value from 1 to 10 is valid.

As shown below:

```toml
# plugins home
#plugins_home = "/usr/local/taos/plugins" # on linux/macOS
#plugins_home = "C:\\TDengine\\plugins" # on windows

# data dir
#data_dir = "/var/lib/taos/taosx" # on linux/macOS
#data_dir = "C:\\TDengine\\data\\taosx" # on windows

# logs home
#logs_home = "/var/log/taos" # on linux/macOS
#logs_home = "C:\\TDengine\\log" # on windows

# log level: off/error/warn/info/debug/trace
#log_level = "info"

# log keep days
#log_keep_days = 30

# number of jobs, default to 0, will use `jobs` number of works for TMQ
#jobs = 0

[serve]
# listen to ip:port address
#listen = "0.0.0.0:6050"

# database url
#database_url = "sqlite:taosx.db"

[monitor]
# FQDN of taosKeeper service, no default value
#fqdn = "localhost"
# port of taosKeeper service, default 6043
#port = 6043
# how often to send metrics to taosKeeper, default every 10 seconds. Only value from 1 to 10 is valid.
#interval = 10
```

## Start taosX

On Linux, use `systemd` to start the `taosX` service:

```shell
systemctl start taosx
```

On Windows, open the **Services** app and start the **taosX** service. Alternatively, in the Windows command line (cmd.exe or PowerShell), run command below:

```shell
sc.exe start taosx
```

## Troubleshooting

1. Modifying the `taosX` log level

The default log level for `taosX` is `info`. To specify a different level, please modify the configuration file, or use the following command-line parameters:
- `error`: `taosx serve -qq`
- `debug`: `taosx serve -q`
- `info`: `taosx serve -v`
- `debug`: `taosx serve -vv`
- `trace`: `taosx serve -vvv`

To specify command-line parameters when `taosX` is run as a service, see Configuration.

2. Viewing `taosX` logs

You can view the log file or use the `journalctl` command to view `taosX` log files.

The command to view logs using `journalctl` on Linux is as follows:

```bash
journalctl -u taosx [-f]
```