---
title: taosX-Agent 参考手册
sidebar_label: taosX-Agent
---

本节讲述如何部署 `Agent` (for `taosX`)。使用之前需要安装 TDengine Enterprise 安装包之后，taosX-Agent 用于在部分数据接入场景，如 Pi, OPC UA, OPC DA 等对访问数据源有一定限制或者网络环境特殊的场景下，可以将 taosX-Agent 部署在靠近数据源的环境中甚至与数据源在相同的服务器上，由 taosX-Agent 负责从数据源读取数据并发送给 taosX。

## 配置

`Agent` 默认的配置文件位于 `/etc/taos/agent.toml`, 包含以下配置项：

- `endpoint`: 必填，`taosX` 的 GRPC 服务地址。
- `token`: 必填，在 `Explorer` 上创建 `Agent` 时，产生的 Token。
- `instanceId`：当前 taosx-agent 服务的实例 ID，如果同一台机器上启动了多个 taosx-agent 实例，必须保证各个实例的实例 ID 互不相同。
- `compression`: 非必填，可配置为 `ture` 或 `false`, 默认为 `false`。配置为`true`, 则开启 `Agent` 和 `taosX` 通信数据压缩。
- `log_level`: 非必填，日志级别，默认为 `info`, 同 `taosX` 一样，支持 `error`，`warn`，`info`，`debug`，`trace` 五级。已弃用，请使用 `log.level` 代替。
- `log_keep_days`：非必填，日志保存天数，默认为 `30` 天。已弃用，请使用 `log.keepDays` 代替。
- `log.path`：日志文件存放的目录。
- `log.level`：日志级别，可选值为 "error", "warn", "info", "debug", "trace"。
- `log.compress`：日志文件滚动后的文件是否进行压缩。
- `log.rotationCount`：日志文件目录下最多保留的文件数，超出数量的旧文件被删除。
- `log.rotationSize`：触发日志文件滚动的文件大小（单位为字节），当日志文件超出此大小后会生成一个新文件，新的日志会写入新文件。
- `log.reservedDiskSize`：日志所在磁盘停止写入日志的阈值（单位为字节），当磁盘剩余空间达到此大小后停止写入日志。
- `log.keepDays`：日志文件保存的天数，超过此天数的旧日志文件会被删除。

如下所示：

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

您不必对配置文件如何设置感到疑惑，阅读并跟随 `Explorer` 中创建 `Agent` 的提示进行操作，您可以对配置文件进行查看、修改和检查。

## 启动

Linux 系统上 `Agent` 可以通过 Systemd 命令启动：

```bash
systemctl start taosx-agent
```

Windows 系统上通过系统管理工具 "Services" 找到 taosx-agent 服务，然后启动它。

## 问题排查

您可以查看日志文件或使用 `journalctl` 命令来查看 `Agent` 的日志。

Linux 下 `journalctl` 查看日志的命令如下：

```bash
journalctl -u taosx-agent [-f]
```
