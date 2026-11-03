---
title: taosX-Agent 配置参考
sidebar_label: 配置参考
toc_max_heading_level: 4
---

taosX-Agent 的配置文件默认路径为 `/etc/taos/agent.toml`（Linux）或 `C:\TDengine\cfg\agent.toml`（Windows），采用 TOML 格式。

## 基本配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `endpoint` | 是 | — | taosX 的 gRPC 服务地址，如 `http://localhost:6055`。可填写多个地址，以英文逗号分隔，详见 [高可用](./03-high-availability.md) |
| `token` | 是 | — | 在 taosExplorer 中创建 Agent 时生成的 Token |
| `instanceId` | 否 | — | Agent 实例 ID。同一台机器部署多个 Agent 实例时，必须保证各实例 ID 唯一 |
| `compression` | 否 | `false` | 是否开启 Agent 与 taosX 之间的通信数据压缩 |
| `in_memory_cache_capacity` | 否 | `64` | 内存中可缓存的最大消息批次数，必须大于 0 |
| `keep_online` | 否 | `true` | taosX 服务不可用或连接断开时，是否保持 Agent 运行并尝试重连 |

## 资源限制

Agent 可以使用与 taosX 相同的 `[resource]` 配置，限制本进程及其已注册连接器子进程的数据面资源占用。资源限制按 Agent 进程独立生效，修改后需重启 Agent。未配置 `[resource]` 时不启用治理器；设置 `resource.enabled = false` 可完整关闭。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `resource.enabled` | 否 | `true`（存在 `[resource]` 时） | 是否启用资源治理器；`false` 可完整回滚 |
| `resource.maxWriteMBps` | 否 | `0` | 数据面写入带宽上限，单位 MB/s；`0` 表示不限制 |
| `resource.maxMemoryMB` | 否 | `0` | 进程及子进程的 RSS 软预算，单位 MiB；`0` 表示自动读取 cgroup 限制，无 cgroup 限制时不启用 |
| `resource.maxPauseSecs` | 否 | `10` | 任意资源限制单次暂停的上限，单位秒；到点强制放行并告警 |
| `resource.maxCpuPercent` | 否 | `0` | CPU 高水位，占全部核数的百分比；超过后暂停数据面写入；`0` 表示不限制 |
| `resource.cpuResumePercent` | 否 | `0` | CPU 恢复水位；`0` 表示 `maxCpuPercent * 0.8`，必须小于 `maxCpuPercent` |
| `resource.maxCpuCores` | 否 | `0` | Agent 运行时 worker 线程数上限；`0` 表示不限制。该参数不限制实际 CPU 使用率 |
| `resource.cgroupPath` | 否 | — | 可选的 cgroup 目录；默认从 `/proc/self/cgroup` 解析 |
| `resource.cpuPriority` | 否 | `0` | Linux 可选 nice 值 `1` 到 `19`；其他平台告警并忽略 |

资源和数据平面行为的完整说明见 [资源占用控制](../../../08-data-ingest-and-delivery/06-resource-control.md)。Agent 不导出 `resource_*` 指标，也不提供本地 metrics 接口；可通过 Agent 日志观察 `resource ... armed`、暂停/恢复和 max-pause 事件。连接器子进程指标使用其连接 endpoint 的真实 `taosx_id`。

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
# taosX 的 gRPC 服务地址（必填），多个地址以英文逗号分隔
endpoint = "http://192.168.1.100:6055"

# 在 taosExplorer 中创建 Agent 时生成的 Token（必填）
token = "your-agent-token-here"

# Agent 实例 ID（同一台机器多实例时必须唯一）
# instanceId = 48

# 开启通信数据压缩
# compression = true

# 内存缓存批次数
# in_memory_cache_capacity = 64

# taosX 不可用时保持运行并重连
keep_online = true

# 资源限制（按需启用）
[resource]
# maxWriteMBps = 0
# maxMemoryMB = 0
# maxPauseSecs = 10
# maxCpuPercent = 0
# cpuResumePercent = 0
# maxCpuCores = 0
# cgroupPath = "/sys/fs/cgroup"
# cpuPriority = 0

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
