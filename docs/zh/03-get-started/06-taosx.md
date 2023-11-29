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

其中：

- `plugins_home`: 为 taosX 外部数据源 SDK 存放目录。
- `data_dir`：为 taosX 数据文件存储目录。
- `database_url`：为 taosX 数据库地址，形式为 `sqlite:<path>`。
- `logs_home`：为 taosX 日志文件存放目录，taosX 服务日志文件名前缀为 `taosx.log`，外部数据源有各自的日志文件名前缀。
- `log_level`：为日志级别字符串，可选项包括：`error`，`warn`，`info`，`debug`，`trace` 五个级别，默认为 `info`。
- `log_keep_days`：日志最大存储天数，taosX 日志将按天拆分为不同的文件。
- `jobs`：为每个运行时最大线程数量，服务模式下总线程数为 `jobs * 2`，默认线程数为当前服务器 `核数 * 2`。
- `serve.listen`：taosX REST API 监听地址，默认为 `0.0.0.0:6050`。
- `serve.grpc`：taosX gRCP API 监听地址，默认为 `0.0.0.0:6055`。

## 启动

使用 Systemd 启动 taosX 服务，其 Systemd 的配置文件位于：`/etc/systemd/system/taosx.service`。启动命令如下：

```shell
systemctl start taosx
```

Windows 系统上，请在 "Services" 系统管理工具中找到 "taosX" 服务，然后点击 "启动这个服务"。或在 Windows 命令行（ `cwd.exe` 或 PowerShell）中运行：`sc start taosx`。

## 问题排查

1. 如何修改 taosX 的日志级别？

    修改配置文件中 `log_level` 参数，默认为 `info`，可提高（ `debug`，`trace`）或降低日志级别（`warn`，`error`）。修改后重启服务。

2. 如何查看 taosX 的日志？

    Linux 下 taosX 服务日志默认存储在 `/var/log/taos/taosx.log` 文件中，Windows 下默认存储在 `C:\\TDengine\\log\\taosx.log` 文件中。
