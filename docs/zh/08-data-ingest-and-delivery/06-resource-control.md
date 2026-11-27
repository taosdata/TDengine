---
sidebar_label: 资源占用控制
title: 资源占用控制
description: 使用 taosX 与 taosX-Agent 的 [resource] 配置限制数据接入任务的内存、CPU 和网络占用
toc_max_heading_level: 4
---

数据接入任务在追赶历史数据、处理大量 CSV 文件或从高吞吐数据源消费时，可能出现内存、CPU 或网络占用过高，影响同一台主机上的 TDengine 或其他服务。taosX 与 taosX-Agent 提供进程级、可配置的 `[resource]` 资源限制，在不修改任务 DSN/SQL 的前提下对数据面进行调节。

## 适用场景

- Kafka、MQTT 等消息源积压后，taosX 消费任务内存持续升高。
- 一次性导入大量 CSV/Parquet 文件时，网络带宽或内存出现峰值。
- InfluxDB、PI 等数据源写入时，连接器子进程 CPU 占用过高。
- 需要在同一台主机上同时运行多个数据接入任务，希望为 taosX 预留资源余量。

## 配置位置

| 进程 | 配置文件 | 说明 |
| --- | --- | --- |
| taosX 服务模式 | `/etc/taos/taosx.toml`（Linux）或 `C:\TDengine\cfg\taosx.toml`（Windows） | 限制 taosX 及其连接器子进程 |
| taosX 命令行模式 | `taosx run` 的 `--resource.*` 参数，或环境变量 `TAOSX_RESOURCE_*` | 限制本次 `taosx run` 进程 |
| taosX-Agent | `/etc/taos/agent.toml`（Linux）或 `C:\TDengine\cfg\agent.toml`（Windows） | 限制 Agent 及其连接器子进程 |

每个进程独立生效。配置修改后需要重启对应进程；运行中修改文件不会自动生效。

未配置 `[resource]` 时资源治理器完全关闭。存在 `[resource]` 配置后，`maxMemoryMB=0` 会自动读取当前进程的 cgroup 内存限制；如需保留 section 但完全关闭，设置 `enabled = false`。

## 参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `enabled` | `true`（存在 `[resource]` 时） | 是否启用资源治理器；未配置 `[resource]` 时不启用。设为 `false` 可完整回滚 |
| `maxWriteMBps` | `0` | 数据面写入带宽上限，单位 MB/s（1 MB = 1024 * 1024 字节）；`0` 表示不限制。使用令牌桶限速，允许 1 秒突发 |
| `maxMemoryMB` | `0` | 进程及已注册子进程的 RSS 软预算，单位 MiB。`0` 表示自动读取 cgroup 内存限制；没有 cgroup 限制时不启用内存限制 |
| `maxPauseSecs` | `10` | 任意资源限制单次暂停的上限，单位秒；到点强制放行并输出告警 |
| `maxCpuPercent` | `0` | CPU 高水位，占全部核数的百分比；超过后暂停数据面写入；`0` 表示不限制 |
| `cpuResumePercent` | `0` | CPU 恢复水位；`0` 表示 `maxCpuPercent * 0.8`，必须小于 `maxCpuPercent` |
| `maxCpuCores` | `0` | 运行时 worker 线程数上限；`0` 表示不限制。只调整 tokio 线程池，不是 CPU 使用率上限 |
| `cgroupPath` | — | 可选的 cgroup 目录；默认从 `/proc/self/cgroup` 解析当前进程的 cgroup |
| `cpuPriority` | `0` | Linux 可选 nice 值 `1` 到 `19`；其他平台告警并忽略 |

## 推荐配置

### 优先控制内存

Kafka 积压、批量文件导入等场景建议先设置内存预算：

```toml
[resource]
# 为 taosX 进程及连接器子进程预留约 2 GiB
maxMemoryMB = 2048
# 单次暂停最多 10 秒，避免任务停死
maxPauseSecs = 10
```

内存是软限制，不是硬上限。如果工作集长期高于预算，taosX 会按 `maxPauseSecs` 周期暂停/放行；需要硬上限时，请同时设置容器或 cgroup 内存限制。

### 限制写入带宽

网络是瓶颈时，可设置 `maxWriteMBps`。例如限制为 20 MB/s：

```toml
[resource]
maxWriteMBps = 20
```

带宽按交给 sink 的 payload/Arrow 字节计费，不包含 Kafka/MQTT 协议开销或 Parquet 压缩后的差异。DDL、元数据、IPC ack/control、心跳等控制面流量不计费。

### 限制 CPU

CPU 是瓶颈时，使用 `maxCpuPercent` 和 `cpuResumePercent`：

```toml
[resource]
# CPU 超过全部核数的 50% 时暂停写入，低于 30% 后恢复
maxCpuPercent = 50
cpuResumePercent = 30
```

CPU 限制采用占空比反馈控制：暂停期间瞬时 CPU 可能仍有波动，限制的是时间均值。`maxCpuCores` 只减少 tokio worker 数量，不能替代 `maxCpuPercent`。

### 组合配置

```toml
[resource]
maxWriteMBps = 20
maxMemoryMB = 2048
maxPauseSecs = 10
maxCpuPercent = 50
cpuResumePercent = 30
```

建议先用 `maxMemoryMB` 或 `maxWriteMBps` 控制主要瓶颈，再按需增加 CPU 限制。过低的预算会让任务频繁进入暂停/恢复周期，反而降低吞吐。

## 行为说明

- **内存**：达到预算后暂停数据面读取，已经进入管道的批次继续写入并排空；不会中断任务或丢弃数据。工作集高于预算时，暂停最长持续 `maxPauseSecs`。
- **带宽**：令牌桶允许 1 秒突发；超过桶容量的批次会被延迟，不会被拒绝。长期速率收敛到 `maxWriteMBps`。
- **CPU**：超过高水位后暂停数据面写入，低于恢复水位后继续；暂停同样受 `maxPauseSecs` 限制。
- **控制面**：DDL、元数据、IPC ack/control、心跳、xnode/agent 管理等流量不参与限速，也不会被资源限制暂停。
- **任务语义**：资源限制不改变任务的成功/失败语义；所有暂停都有上限，避免任务因预算不可达而永久停住。

## 监控与排查

在 taosX 服务模式配置 `[monitor]` 后，资源治理指标会通过 taosKeeper 写入 `log` 库（默认），并按 `taosx_id` 区分。TDinsight for taosX 面板包含 Resource Governor 行；相关指标包括：

- `resource_rss_bytes`：进程及子进程总 RSS
- `resource_cpu_percent`：进程及子进程 CPU 使用率
- `resource_egress_bytes_total`：累计计费写入字节
- `resource_bucket_wait_ms_total`：令牌桶累计等待时间
- `resource_brake_paused`：当前是否处于暂停状态
- `resource_brake_max_pause_total` / `resource_cpu_brake_max_pause_total`：达到暂停上限的次数

如果出现 `memory brake max pause expired` 或 `cpu brake max pause expired` 日志，说明工作集或 CPU 基线高于配置预算，应适当提高预算，或结合 cgroup 限制排查内存泄漏和碎片问题。

taosX-Agent 不导出 `resource_*` 指标，也不提供本地 metrics 接口；可通过 Agent 日志观察 `resource ... armed`、暂停/恢复和 max-pause 事件。Agent 连接器子进程指标按其连接 endpoint 的真实 `taosx_id` 上报。

## 已知限制

- 内存限制是**软调速器**，不承诺 RSS 硬上限；需要硬上限时请使用 cgroup/容器限制。
- MQTT 和 SparkplugB 是 push 数据源，不接入读侧内存限制（暂停 poll 可能影响 keepalive/PUBACK），带宽和 CPU 限制仍然生效。
- 插件/子进程数据源的内部读取和缓冲不在 Rust 读闸内；首次在生产环境启用前建议做一次资源冒烟测试。
- cgroup v1 已支持解析和单元测试，但尚无真实 v1 主机的端到端验证；Windows Agent 的资源限制端到端验证尚未覆盖。

## 参考

- [taosX 配置参考](../12-operations-and-tooling/03-components/06-taosx.md#配置)
- [taosX-Agent 配置参考](../12-operations-and-tooling/03-components/07-taosx-agent/01-configuration.md#资源限制)
- [运行监控](../12-operations-and-tooling/02-operations/05-monitor.md#资源治理指标)
