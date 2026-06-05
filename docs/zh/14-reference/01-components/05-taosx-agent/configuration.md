---
title: 配置参考
sidebar_label: 配置参考
toc_max_heading_level: 4
---

taosX-Agent 的配置文件默认路径为 `/etc/taos/agent.toml`（Linux）或 `C:\TDengine\cfg\agent.toml`（Windows），采用 TOML 格式。

## 基本配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `endpoint` | 是 | — | taosX 的 gRPC 服务地址，如 `http://localhost:6055` |
| `token` | 是 | — | 在 Explorer 中创建 Agent 时生成的 Token |
| `instanceId` | 否 | — | Agent 实例 ID。同一台机器部署多个 Agent 实例时，必须保证各实例 ID 唯一 |
| `compression` | 否 | `false` | 是否开启 Agent 与 taosX 之间的通信数据压缩 |
| `in_memory_cache_capacity` | 否 | `64` | 内存中可缓存的最大消息批次数，必须大于 0 |
| `keep_online` | 否 | `true` | taosX 服务不可用或连接断开时，是否保持 Agent 运行并尝试重连 |

## 客户端端口范围

当 Agent 向 taosX 创建连接时，客户端会随机使用一个端口。可通过 `[client_port_range]` 配置限制端口范围：

| 参数 | 必填 | 默认值 | 取值范围 | 说明 |
| --- | --- | --- | --- | --- |
| `client_port_range.min` | 否 | `49152` | 49152–65535 | 客户端端口范围最小值 |
| `client_port_range.max` | 否 | `65535` | 49152–65535 | 客户端端口范围最大值 |

## 日志配置

通过 `[log]` 配置日志行为：

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `log.path` | 否 | `/var/log/taos`（Linux）<br/>`C:\TDengine\log`（Windows） | 日志文件存放目录 |
| `log.level` | 否 | `info` | 日志级别，可选值：`error`、`warn`、`info`、`debug`、`trace` |
| `log.compress` | 否 | `false` | 日志文件滚动后是否压缩 |
| `log.rotationCount` | 否 | `30` | 日志目录下最多保留的文件数，超出则删除旧文件 |
| `log.rotationSize` | 否 | `1GB` | 触发日志滚动的文件大小，超出后创建新文件 |
| `log.reservedDiskSize` | 否 | `1GB` | 磁盘剩余空间低于此值时停止写入日志 |
| `log.keepDays` | 否 | `30` | 日志文件保留天数，超过则删除旧日志 |

:::note
`log_level` 和 `log_keep_days` 为旧版参数，已弃用。请使用 `log.level` 和 `log.keepDays` 替代。
:::

## 完整配置示例

```toml
# taosX 的 gRPC 服务地址（必填）
endpoint = "http://192.168.1.100:6055"

# 在 Explorer 中创建 Agent 时生成的 Token（必填）
token = "your-agent-token-here"

# Agent 实例 ID（同一台机器多实例时必须唯一）
# instanceId = 48

# 开启通信数据压缩
# compression = true

# 内存缓存批次数
# in_memory_cache_capacity = 64

# taosX 不可用时保持运行并重连
keep_online = true

# 客户端端口范围
[client_port_range]
# min = 49152
# max = 65535

# 日志配置
[log]
# path = "/var/log/taos"
# level = "info"
# compress = false
# rotationCount = 30
# rotationSize = "1GB"
# reservedDiskSize = "1GB"
# keepDays = 30
```
