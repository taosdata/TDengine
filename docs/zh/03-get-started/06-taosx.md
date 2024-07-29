---
title: 部署 taosX
sidebar_label: 部署 taosX
---

## 简介

本节讲述如何部署 `taosX`。在安装了 `taosX` 安装包后，系统中就具备了 `taosX`，细节请参考 [安装与配置](../install)。

## 配置

`taosX` 支持通过配置文件进行配置。在 Linux 上，默认配置文件路径是 `/etc/taos/taosx.toml`，在 Windows 上，默认配置文件路径是 `C:\\TDengine\\cfg\\taosx.toml`，包含以下配置项：

- `plugins_home`：外部数据源连接器所在目录。
- `data_dir`：数据文件存放目录。
- `logs_home`：日志文件存放目录，`taosX` 日志文件的前缀为 `taosx.log`，外部数据源有自己的日志文件名前缀。
- `log_level`：日志等级，可选级别包括 `error`、`warn`、`info`、`debug`、`trace`，默认值为 `info`。
- `log_keep_days`：日志的最大存储天数，`taosX` 日志将按天划分为不同的文件。
- `jobs`：每个运行时的最大线程数。在服务模式下，线程总数为 `jobs*2`，默认线程数为`当前服务器内核*2`。
- `serve.listen`：是 `taosX` REST API 监听地址，默认值为 `0.0.0.0:6050`。
- `serve.database_url`：`taosX` 数据库的地址，格式为 `sqlite:<path>`。
- `monitor.fqdn`：`taosKeeper` 服务的 FQDN，没有默认值，置空则关闭监控功能。
- `monitor.port`：`taosKeeper` 服务的端口，默认`6043`。
- `monitor.interval`：向 `taosKeeper` 发送指标的频率，默认为每 10 秒一次，只有 1 到 10 之间的值才有效。

如下所示：

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

## 启动

Linux 系统上 `taosX` 可以通过 Systemd 命令启动：

```shell
systemctl start taosx
```

Windows 系统上通过系统管理工具 "Services" 找到 `taosX` 服务，然后启动它，或者在命令行工具（cmd.exe 或 PowerShell）中执行以下命令启动：

```shell
sc.exe start taosx
```

## 问题排查

1. 修改 `taosX` 日志级别

`taosX` 的默认日志级别为 `info`，要指定不同的级别，请修改配置文件，或使用以下命令行参数：
- `error`：`taosx serve -qq`
- `debug`：`taosx serve -q`
- `info`：`taosx serve -v`
- `debug`：`taosx serve -vv`
- `trace`：`taosx serve -vvv`

要在 `taosX` 作为服务运行时指定命令行参数，请参阅配置。

2. 查看 `taosX` 日志

您可以查看日志文件或使用 `journalctl` 命令来查看 `taosX` 的日志。

Linux 下 `journalctl` 查看日志的命令如下：

```bash
journalctl -u taosx [-f]
```