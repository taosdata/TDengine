---
title: 部署 taosX
sidebar_label: 部署 taosX
---

## 简介

本节讲述如何部署 taosX。在安装了 taosX 安装包后，系统中就具备了 taosX，细节请参考 [安装与配置](../install)。

## 配置

taosX 支持通过配置文件进行配置。在 Linux 上，默认配置文件路径是 `/etc/taos/taosx.toml`，在 Windows 上，默认配置文件路径为 `C:\\TDengine\\config\\taosx.toml`。一个完整的 taosX 配置文件示例如下：

```toml
# plugins home
#plugins_home = "/usr/local/taos/plugins"

# data dir
#data_dir = "/var/lib/taos/taosx"

# logs home
#logs_home = "/var/log/taos"

# log level: off/error/warn/info/debug/trace
#log_level = "info"

# log keep days
#log_keep_days = 30

# number of threads
#jobs = 0

# enable OpenTelemetry tracing and metrics exporter
#otel = false

#[serve]
# listen to ip:port address
#listen = "0.0.0.0:6050"

# GRPC listen address
#grpc = "0.0.0.0:6055"

# database url
#database_url = "sqlite:taosx.db"
```

Where:

- `plugins_home`: is the directory for taosX external data source SDK.
- `data_dir`: is the directory for taosX data file storage.
- `database_url`: is the address of the taosX database, in the form of `sqlite:<path>`.
- `logs_home`: is the directory for taosX log file storage. The taosX service log file has the prefix `taosx.log`, and external data sources have their own log file name prefixes.
- `log_level`: is a log level string, with optional levels including: `error`, `warn`, `info`, `debug`, `trace`. The default is `info`.
- `log_keep_days`: is the maximum storage days for logs. taosX logs will be split into different files by day.
- `jobs`: is the maximum number of threads for each runtime. In service mode, the total number of threads is `jobs * 2`, and the default number of threads is `current server cores * 2`.
- `serve.listen`: is the taosX REST API listening address. The default is `0.0.0.0:6050`.
- `serve.grpc`: is the taosX gRPC API listening address. The default is `0.0.0.0:6055`.

## Start

Start the taosX service using Systemd, and its Systemd configuration file is located at: `/etc/systemd/system/taosx.service`. The start command is as follows:

```shell
systemctl start taosx
```

On Windows systems, please find the "taosX" service in the "Services" system management tool, then click "Start this service". Alternatively, in the Windows command line (cmd.exe or PowerShell), run: `sc start taosx`.

## Troubleshooting

1. How to modify the log level of taosX?

    Modify the `log_level` parameter in the configuration file, which defaults to `info`. It can be increased (`debug`, `trace`) or decreased (`warn`, `error`). After modification, restart the service.

2. How to view the logs of taosX?

    On Linux, taosX service logs are stored by default in the `/var/log/taos/taosx.log` file. On Windows, they are stored by default in the `C:\\TDengine\\log\\taosx.log` file.
