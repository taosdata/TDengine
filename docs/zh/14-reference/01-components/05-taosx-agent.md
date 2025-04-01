---
title: taosX-Agent 参考手册
sidebar_label: taosX-Agent
---

taosX-Agent 是 TDengine Enterprise 的核心组件之一。本节将着重介绍如何对 `Agent` (for `taosX`) 进行配置。要使用 taosX-Agent，你只需安装 TDengine Enterprise 版本，安装完成后即可获取该组件。

在一些特定场景中，如数据源访问受限或者网络环境特殊的情况，数据接入会面临诸多挑战。taosX - Agent 正是为解决此类问题而设计，它能在诸如使用 Pi 设备、遵循 OPC UA 或 OPC DA 协议等场景中发挥重要作用。

为了更高效地实现数据采集与转发，建议将 taosX - Agent 部署在靠近数据源的环境中，甚至可以直接部署在数据源所在的服务器上。如此一来，taosX - Agent 就能顺利地从数据源采集数据，并将采集到的数据转发至 taosX 系统。

## 配置

`Agent` 默认的配置文件位于 `/etc/taos/agent.toml`，包含以下配置项：

- `endpoint`：必填，`taosX` 的 GRPC 服务地址。
- `token`：必填，在 `Explorer` 上创建 `Agent` 时，产生的 Token。
- `instanceId`：当前 taosx-agent 服务的实例 ID，如果同一台机器上启动了多个 taosx-agent 实例，必须保证各个实例的实例 ID 互不相同。
- `compression`：非必填，可配置为 `true` 或 `false`，默认为 `false`。配置为`true`，则开启 `Agent` 和 `taosX` 通信数据压缩。
- `in_memory_cache_capacity`：非必填，表示可在内存中缓存的最大消息批次数，可配置为大于 0 的整数。默认为 `64`。
- `client_port_range.min`：非必填，取值范围 `[49152-65535]`，默认为 `49152`，当 agent 向 taosx 创建 socket 连接时，socket 客户端会随机监听一个端口，此配置限制了端口范围的最小值。
- `client_port_range.max`：非必填，取值范围 `[49152-65535]`，默认为 `65535`，此配置限制了端口范围的最大值。
- `log_level`：非必填，日志级别，默认为 `info`，同 `taosX` 一样，支持 `error`、`warn`、`info`、`debug`、`trace` 五级。已弃用，请使用 `log.level` 代替。
- `log_keep_days`：非必填，日志保存天数，默认为 `30` 天。已弃用，请使用 `log.keepDays` 代替。
- `log.path`：日志文件存放的目录。
- `log.level`：日志级别，可选值为 "error"、"warn"、"info"、"debug"、"trace"。
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

# In-memory cache capacity
#
#in_memory_cache_capacity = 64

[client_port_range]
# Minimum boundary of listening port of agent, can not less than 49152
#
# min = 49152

# Maximum boundary of listening port of agent, can not greater than 65535
#
# max = 65535

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
