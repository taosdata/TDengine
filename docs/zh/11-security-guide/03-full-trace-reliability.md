---
sidebar_label: 全链路高可靠
title: 全链路高可靠
description: Checkpoint、WAL、备份恢复与落盘相关可靠性
toc_max_heading_level: 3
---

本文描述 TDengine TSDB 从用户触达到数据落盘的完整可靠性链路。按统一的六层分层架构展开——入口层 → 接入层 → 数据采集链路 → 集群内部 → 可观测性接入 → 存储与审计落盘，每层逐一介绍其可靠性保障机制。

> 术语约定
>
> - 接入层：指入口层之后、taosd 之前的协议/代理层，包括 taosAdapter 和 taosc 两条物理路径。
> - 断点续传：默认指数据源侧的 checkpoint，用于任务重启后恢复采集进度；它不等同于 taosX 向下游写入时使用的持久化队列。
> - 任务重启：指自动故障恢复手段全部用尽后，关闭并重新拉起 taosX 任务；它不同于“数据源连接重试”。

---

## 概述

```mermaid
graph TD
    classDef client fill:#f39c12,color:#000,stroke:#d68910
    classDef svc    fill:#1a6ea8,color:#fff,stroke:#0d4f7d
    classDef core   fill:#2c3e50,color:#fff,stroke:#1a252f
    classDef ops    fill:#16a085,color:#fff,stroke:#0e6655
    classDef disk   fill:#7f8c8d,color:#fff,stroke:#566573

    APP(["应用 / 连接器"])
    SRC(["外部数据源"])
    AGT(["taosX-Agent（可选）<br/>本地缓存"])
    TX(["taosX<br/>Checkpoint + 持久化队列"])
    GATE(["接入层<br/>taosAdapter / taosc"])
    CORE(["taosd 集群<br/>多副本 + Leader 切换"])
    OBS(["taosKeeper<br/>指标 / 审计（经典路径）"])
    STORE[("业务数据 / 审计日志<br/>WAL + 快照 + TDE + 备份")]

    SRC --> TX

    SRC .-> AGT

    AGT .-> TX

    APP --> GATE

    TX --> GATE
    GATE --> CORE
    CORE --> STORE
    CORE -. 指标与审计旁路 .-> OBS
    OBS -. 写回 log / 审计库 .-> STORE

    class SRC,APP client
    class AGT,TX,GATE svc
    class CORE core
    class OBS ops
    class STORE disk
```

### 可靠性四道防线

| 防线 | 机制 | 故障覆盖 | 所在层 |
|------|------|---------|--------|
| L1 数据接入 | 故障转移 + 断点续传 + 缓存队列 | 网络或服务中断 | 第 3 节 |
| L2 多实例 | 多实例负载均衡 / 故障转移 | 单一实例故障 | $2 接入层 |
| L3 WAL | 写入 ACK 前先落 WAL | 进程崩溃、突然断电 | 第 6 节 |
| L4 多副本 | Raft 3 副本跨节点 | 单节点磁盘/主机故障 | 第 4 节 |
| L5 TDE | 存储层透明加密 | 磁盘被盗、物理介质泄露 | 第 6 节 |
| L6 备份恢复 | taosdump / taosX | 误删除、数据中心级灾难 | 第 6 节 |

### 各层可靠性概览

| 层级 | 关键机制 |
|------|---------|
| ① 入口层 | 连接器内置重连 + 应用重试；Explorer 多实例前置 LB；CLI 续跑 |
| ② 接入层 | taosAdapter 连接池/限流/内存保护；taosc `firstEp`/`secondEp` 自动探测 |
| ③ 数据采集链路 | 数据源侧 checkpoint + Sink 侧持久化队列 + 任务重启；taosX-Agent 可选本地缓存 |
| ④ 集群内部 | Raft 多副本（VGroup/MNode）+ 自动 Leader 选举 |
| ⑤ 可观测性接入 | taosKeeper 指标缓冲/补推；默认路径亦承载审计上报；失效不影响业务读写 |
| ⑥ 存储与审计落盘 | WAL fsync + 快照 + TDE + 备份恢复 + 审计日志权限分离 |

---

## 1. 入口层可靠性

入口层是用户/应用触达 TDengine 的第一跳。本层通过**连接器内置重连 + 应用层重试 + CLI 续跑**三类手段保障：连接瞬断时自动恢复，长任务中途失败可从断点续跑，不要求用户手工干预。

### 1.1 程序化入口（应用 / 各语言连接器）

应用通过 WebSocket/REST 或原生 TCP 接入，连接器层面应配置**连接池 + 超时 + 重试**，并在应用层包装幂等重试逻辑。

**Java / JDBC + HikariCP：**

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:TAOS-RS://lb-vip:6041/db");
config.setUsername("tduser");
config.setPassword("SecurePass123!");
config.setConnectionTimeout(30_000);
config.setMaximumPoolSize(10);
config.setConnectionTestQuery("SELECT server_status()");
```

**Go：**

```go
db, _ := sql.Open("taosWS", "tduser:SecurePass123!@ws(lb-vip:6041)/db?readTimeout=30s")
db.SetMaxOpenConns(20)
db.SetConnMaxLifetime(10 * time.Minute)
```

**Python：**

```python
import taosws
conn = taosws.connect("taos+ws://tduser:SecurePass123!@lb-vip:6041/db")
# 推荐在应用层包装重试逻辑（指数退避 + 幂等写入）
```

> **应用层重试建议**：对幂等操作（INSERT 带显式时间戳、SELECT）使用指数退避（如初始 200 ms、倍增至 10 s、最多 5 次）；DDL/非幂等写入需配合业务层去重（`INSERT ... ON DUPLICATE KEY UPDATE` 或时间戳主键覆盖）。

### 1.2 Web UI 入口（taosExplorer）

taosExplorer 通过浏览器提供 Web 控制台，关注两类可靠性场景：

- **会话失效**：登录会话过期或后端重启后，Explorer 前端会自动跳转到登录页重新认证，用户无需手工刷新；未提交的编辑器内容保留在浏览器本地（页面不刷新即不丢失）。
- **长查询/长任务**：浏览器标签断开后，SQL 查询会在 taosd 侧被取消（由 taosAdapter 检测连接关闭）；面板数据看板用短连接轮询，网络抖动不影响已落盘数据。

生产建议：Explorer 以前置 Nginx/HAProxy 的**无状态多实例**方式部署；单实例故障后，浏览器重新建立连接即可切到其它实例，业务数据本身不受影响。

### 1.3 命令行工具入口（运维 CLI）

运维 CLI 在执行大批量数据搬运或长时压测时需要**可中断、可续跑**能力。

#### 1.3.1 `taos`（交互式 SQL shell）

`taos` 通过 taosc 直连 taosd :6030，taosc 内置自动重连：当前连接的 DNode 不可达时，taosc 会尝试通过 `firstEp` / `secondEp` 重新定位 MNode 并获取最新集群拓扑（详见 2.2 节）。用户在 shell 中感知为“短暂卡顿后自动恢复”。

#### 1.3.2 `taosX`（数据管道 CLI）

`taosX` 的核心可靠性能力是**断点续传**——任务中断（进程被 kill、网络断开、目标端不可用）后重新启动相同任务，会从上次的数据源侧 checkpoint 继续，不会重复消费或漏采（详见 3.2 节）。

```bash
# 任务异常退出后直接再次运行同一命令即续跑
taosX run \
  -f "taos+ws://tduser:SecurePass123!@src:6041/srcdb" \
  -t "taos+ws://tduser:SecurePass123!@dst:6041/dstdb"
```

#### 1.3.3 `taosBenchmark`（压测）

`taosBenchmark` 提供 `--retry` 相关参数，在写入过程中遇到临时错误可自动重试，避免短暂网络抖动或限流触发而整体失败。

```bash
# 写入失败自动重试（示例配置）
taosBenchmark -u tduser -p'SecurePass123!' \
  -R 3 \          # 每条记录最大重试次数
  -S 1000         # 重试间隔（毫秒）
```

#### 1.3.4 `taosdump`（备份/恢复）

`taosdump` 通过 `-S`（start time）/ `-E`（end time）控制时间范围，实现**增量备份**与**失败续跑**。若一次全量备份中途失败，可基于已完成时间段缩小范围重试。

```bash
# 全量备份
taosdump -h localhost -u tduser -p'SecurePass123!' -o /backup

# 增量备份（仅导出指定时间段）
taosdump -h localhost -u tduser -p'SecurePass123!' -D mydb \
  -S "2024-01-01 00:00:00" -E "2024-01-02 00:00:00" \
  -o /backup/incremental/2024-01-01
```

> **续跑策略**：按“天”切片进行增量备份，失败当天可独立重跑；全量备份建议在业务低峰单次完成，若中断则整体重跑到新目录。

---

## 2. 接入层可靠性

> **关于 taosc**：taosc 是 TDengine 的原生客户端库，作为**独立组件**向上提供 C 语言 API 和 DSN 连接接口，向下通过**私有协议**（TCP :6030）与 taosd 集群通信，并在协议层实现 `firstEp`/`secondEp` 双入口探测、集群拓扑刷新、Leader 切换透明重试等可靠性机制。路径 A（WebSocket/REST）由 taosAdapter 在服务端内部调用 taosc 完成到 taosd 的最后一跳，因此 Adapter 自身的限流/连接池保护叠加 taosc 的自动重连；路径 B 则由应用/CLI 通过嵌入 taosc 动态库直连 taosd，可靠性机制完全来自 taosc。两条路径最终都经 taosc 进入 taosd。

数据从入口层走到接入层，需要解决“服务端自我保护”和“客户端自动重连”两个问题。本层通过 **taosAdapter 限流/超时/内存保护**（路径 A）与 **taosc 节点探测/自动重连**（路径 B）保障：在服务端过载或节点故障时既不放任雪崩，也不让客户端永久挂起。

### 2.1 路径 A：WebSocket / REST（taosAdapter 自我保护）

taosAdapter 是 WebSocket/REST 流量的服务端网关，通过连接池、查询限流、内存水位多层机制保护 taosd 不被过载压垮。

**连接池配置**（`/etc/taos/taosadapter.toml`）：

```toml
[pool]
maxConnect  = 0       # 最大连接数（默认：CPU 核数×2）
maxIdle     = 0       # 最大空闲连接
idleTimeout = "0s"    # 空闲超时
waitTimeout = 60      # 等待连接超时（秒），超时返回 503
maxWait     = 0       # 最大等待队列（0=无限制）
```

**内存保护（反压 / backpressure）**：

```toml
[monitor]
disable                    = false
collectDuration            = "3s"
pauseQueryMemoryThreshold  = 70    # 查询暂停阈值（%）
pauseAllMemoryThreshold    = 80    # 全部暂停阈值（%）
```

健康检查 `/-/ping` 在阈值超限时返回 503，**上游负载均衡器据此自动摘除节点**，形成反压信号回流到客户端（连接器收到 503 触发重试）。

**查询限流**：

```toml
[request]
queryLimitEnable = true

[request.default]
queryLimit       = 0     # 默认并发查询数（0=无限制）
queryWaitTimeout = 900

[request.users.readonly_user]
queryLimit       = 10
queryWaitTimeout = 60
```

**SQL 拒绝正则**（防误操作放大故障半径）：

```toml
rejectQuerySqlRegex = [
  '(?i)^drop\\s+database\\s+.*',
  '(?i)^alter\\s+table\\s+.*'
]
```

### 2.2 路径 B：taosc → taosd 私有协议（自动重连）

原生路径下，taosc 作为嵌入应用进程的独立客户端库，通过 TDengine **私有协议**（TCP :6030）与 taosd 通信，并负责节点探测与故障切换：

- **`firstEp` / `secondEp` 双入口**：客户端配置两个初始接入点，连接 `firstEp` 失败自动切到 `secondEp`，避免单点失败。
- **拓扑刷新**：连接任一可用 DNode 后，taosc 从 MNode 获取最新集群拓扑（哪些 VGroup Leader 在哪个 DNode），后续写入直接定位到正确 Leader。
- **Leader 切换透明化**：当 VGroup Leader 发生切换（见 4.4 节），taosc 在下一次请求收到 `Not Leader` 错误时自动刷新路由并重试，对应用透明。

```ini
# 客户端侧 taos.cfg
firstEp  dnode1.example.com:6030
secondEp dnode2.example.com:6030
```

---

## 3. 数据采集链路可靠性

数据从外部数据源经“**外部数据源 → taosX-Agent（可选）→ taosX → taosAdapter → taosd**”进入集群。是否部署 taosX-Agent，不改变断点续传的核心语义：真正用于恢复采集进度的是**数据源侧 checkpoint**；而在数据已经被 taosX 读出、但尚未成功写入下游时，保护手段则是 **taosX 持久化队列**。

### 3.1 采集链路与缓存边界

taosX-Agent 只在边缘采集、弱网传输、或上游本身不提供可靠回放能力时需要部署；并非所有数据源都必须带本地缓存。

| 场景 | 是否建议 taosX-Agent 本地缓存 | 可靠性边界 |
|------|------------------------|-----------|
| Kafka / Pulsar / TMQ | 通常不需要 | 可依赖 Broker / 上游持久化缓存；taosX 仍需记录 checkpoint |
| MQTT QoS 0 / 串口 / 设备直采 | 建议需要 | 上游通常无可靠回放能力，应在 taosX-Agent 本地落盘 |
| OPC-UA / OPC-DA / 工业边缘网络 | 建议需要 | 边缘网络抖动常见，本地缓存用于跨断网续传 |
| MySQL / PostgreSQL / MSSQL 增量拉取 | 视场景 | 通常依赖源表保留窗口 + 增量列，无需额外本地缓存 |

> 若数据源可以直接连到 taosX，则链路可简化为“外部数据源 → taosX → taosAdapter → taosd”。这时是否带 taosX-Agent，只影响缓存位置，不影响 checkpoint 的定义。

### 3.2 数据源侧断点续传（Checkpoint）

每个 taosX 任务定期把**数据源消费进度**写入 checkpoint。任务重启后，恢复逻辑首先读取该 checkpoint，再决定从哪里继续采集：

- **位点型来源**（Kafka / Pulsar / TMQ）：记录 offset 或 cursor。
- **窗口型来源**（MySQL / PostgreSQL / TDengine 查询）：记录已完成的时间窗口或增量列。
- **边缘采集型来源**（经 taosX-Agent）：记录最近已确认的序号、时间戳或位点。

不要把 checkpoint 和持久化队列混为一体：

| 能力 | 记录对象 | 解决的问题 | 典型触发 |
|------|---------|-----------|---------|
| Checkpoint | 数据源侧 offset / 时间窗口 / 增量列 | 任务被关闭后从哪里继续“读” | 任务重启 |
| 持久化队列 | 已读出但尚未成功写入下游的数据 | 下游暂时不可用时如何“不丢写” | Sink 端抖动 / 目标不可达 |

### 3.3 taosX 持久化队列（Persist Queue）

taosX 使用持久化队列暂存**已经从数据源读取、但尚未成功写入 taosAdapter / taosd** 的数据。当 Sink 端短暂不可用时，数据会先落到本地磁盘，待下游恢复后再继续发送。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| 队列段大小 | 1 GB | 每个持久化文件最大体积 |
| 最大批量读取 | 1000 条 | 单次从队列读取的最大消息数 |
| 写入同步间隔 | 3 秒 | 持久化数据刷盘间隔 |
| 清理间隔 | 30 秒 | 已消费队列段自动清理周期 |

### 3.4 taosX 任务重启

在数据采集链路里，**先发生的是自动恢复**：数据源连接重试、taosAdapter 重连、持久化队列回放等。如果这些手段全部用尽，任务才会进入 `Failed`，随后由调度器或运维执行**任务重启**——即关闭并重新拉起整个 taosX 任务。

| 概念 | 触发时机 | 作用范围 | 是否关闭任务 |
|------|---------|---------|-------------|
| 数据源连接重试 | 单次连接中断 | 当前 Source 连接 | 否 |
| 任务重启 | 自动恢复手段全部失败后 | 整个 taosX 任务 | 是 |

任务重启后的恢复顺序通常是：

1. 读取数据源侧 checkpoint，确认应该从哪个位点继续采集。
2. 回放尚未清空的持久化队列，先补齐已经读出但未写完的数据。
3. 重新建立 Source / Sink 连接并恢复正常处理。

### 3.5 taosX-Agent 本地缓冲 + 断线重连

taosX-Agent 部署在边缘侧（靠近数据源），内置消息缓存队列，与中心 taosX 网络中断时在本地暂存数据、恢复后自动续传。配置与行为详见 [存储转发（Store and Forward）](../12-operations-and-tooling/03-components/07-taosx-agent/store-and-forward.md)。

```toml
# /etc/taos/agent.toml
in_memory_cache_capacity = 64     # 内存缓存队列容量
keep_online = true                # 断线后保持运行
data_dir = "/var/lib/taos/taosX-agent"  # 持久化目录
```

`keep_online = true`（默认）确保 Agent 在 taosX 断开时继续运行并缓存数据。适用于工厂内网、卫星链路等网络不稳定环境。

### 3.6 外部数据源连接重试

外部数据源到 taosX / Agent 的入口侧同样需要重连能力，由各 Source 连接器内置。超过重试阈值后，任务才会进入 `Failed`，再由第 3.4 节的任务重启机制接手。

| 数据源类型 | 最小间隔 | 最大间隔 | 最大次数 |
|-----------|---------|---------|---------|
| MQTT | 100 ms | 10 s | 10 |
| Kafka | — | 300 s | 3 |
| Pulsar | — | 300 s | 3 |
| 传统连接器 | — | — | 5 |

| 数据源 | 可靠性机制 |
|--------|-----------|
| MQTT | 客户端自动重连（断开后按指数退避重连 Broker）；QoS 1/2 保证至少一次投递 |
| Kafka | Consumer Group 再平衡；消费者重启后从已提交 offset 续传；单分区内消息有序 |
| Pulsar | 基于订阅游标（cursor）的断点续传；Broker 切换对消费者透明 |
| JDBC Source (MySQL/PG/MSSQL) | 查询失败按时间窗口重试；基于增量列（时间戳/自增 ID）幂等拉取 |
| OPC-UA / OPC-DA | 连接断开后由 Agent 自动重连；订阅模式下变化值缓存在服务端直到重订阅 |

> 配置要点：所有外部 Source 都应显式指定 **checkpoint 位点**（offset / 时间戳 / 增量列），避免依赖默认值导致重启后从头拉取或漏数据。

---

## 4. 集群内部可靠性

数据进入 taosd 后，本层通过 **Raft 多副本协议 + 自动 Leader 选举 + MNode 元数据高可靠** 三要素保障：单节点故障不中断写入、不丢数据；Leader 切换秒级完成；元数据（用户、库、表结构）与数据享同等级别的副本保护。

### 4.1 多副本 Raft 协议

TDengine 的 VNode（时序数据）和 MNode（元数据）均使用 Raft 协议进行多副本复制。Leader 负责接收写入，通过 AppendEntries RPC 同步到 Followers，多数派确认后提交。

### 4.2 副本架构（1 / 2 / 3 副本）

`REPLICA` 可为 `1`、`2` 或 `3`。不同数据库可按需选择；生产优先三副本，成本敏感场景可用双副本。

**三副本（Raft 多数派）**

```text
VGroup 1（replica=3）
  ├── VNode @ DNode-1  [Leader]   ← 接收写入
  ├── VNode @ DNode-2  [Follower] ← 实时同步
  └── VNode @ DNode-3  [Follower] ← 实时同步
```

```sql
CREATE DATABASE db REPLICA 3 VGROUPS 10;
ALTER DATABASE db REPLICA 3;
```

三副本按 Raft 多数派提交：允许 1 个副本故障仍可读写。不支持与双副本互转。

**双副本（企业版，Mnode 仲裁）**

双副本在保证一定可靠/可用性的前提下压缩存储成本：时序数据仅存 2 份，选主不由 VGroup 内 Raft 多数派决定，而由高可用 Mnode 充当 Arbitrator；某一 Vnode 故障且数据已同步时，可指定另一 Vnode 为 Assigned Leader 继续服务。集群至少 3 个节点（典型为 2 个数据节点 + 1 个仲裁节点，仲裁节点可将 `supportVnodes` 设为 `0`）。

```text
VGroup 1 (replica=2)
  ├── VNode @ DNode-1  [Leader / Assigned Leader]
  ├── VNode @ DNode-2  [Follower]
  └── Arbitrator @ Mnode（不存时序数据）
```

```sql
CREATE DATABASE db REPLICA 2 VGROUPS 10;
ALTER DATABASE db REPLICA 2;   -- 仅支持与单副本互转
```

容错边界：单服务故障且不出现连续故障时尚可恢复；两数据副本同时不可用，或未同步完成时再故障，则无法继续服务。强制指定 Leader 等运维见下。

部署约束、异常场景与 `ASSIGN LEADER FORCE` 等详见 [双副本方案](../12-operations-and-tooling/02-operations/11-ha/02-replica2.md)。

**单副本**

`REPLICA 1` 无冗余，节点故障即不可用，仅适合开发/测试。

### 4.3 写入一致性

Leader 收到写入 → 追加 WAL → 并行发送 AppendEntries → 多数派 ACK → Commit → 回复客户端。性能代价：写入吞吐下降 < 15%，延迟增加 < 5 ms。

### 4.4 故障切换

| 事件 | 自动处理 | 切换时间 |
|------|---------|---------|
| Leader 崩溃 | Follower 发起选举 | < 30 秒 |
| DNode 网络中断 | 剩余两副本继续服务 | 立即 |
| DNode 磁盘损坏 | `RESTORE DNODE` 从副本重建 | 按数据量 |

### 4.5 节点恢复

```sql
RESTORE DNODE <dnode_id>;
RESTORE MNODE ON DNODE <dnode_id>;
RESTORE VNODE ON DNODE <dnode_id>;
```

### 4.6 副本状态监控

```sql
SHOW VGROUPS;
-- status: leader / follower / offline / candidate
```

### 4.7 MNode 元数据高可靠

MNode 保存集群元数据（用户、库、表、DNode 列表等）。生产部署需 3 个 MNode 组成 Raft 组：

```sql
SHOW MNODES;
CREATE MNODE ON DNODE 2;
CREATE MNODE ON DNODE 3;
```

Leader 故障时自动选举新 Leader（< 30 秒），对客户端透明——期间正在执行的 DDL 会短暂失败，业务读写（定位到 VGroup Leader）不受影响。

---

## 5. 可观测性接入可靠性

taosKeeper 负责把 taosd / taosAdapter / taosX / taosExplorer 对外输出的监控指标写回 `log` 库。默认部署下（`auditSaveInSelf = 0`），企业版审计日志也经 Keeper 写入带 `IS_AUDIT` 的审计库；`auditSaveInSelf = 1`（`v3.4.1.0+`）时审计本集群直写、不经 Keeper。审计配置见 [审计与合规](./07-audit-and-compliance.md)。本节以指标链路可靠性为主，并说明默认审计路径对 Keeper 的依赖。

### 5.1 taosKeeper 指标采集可靠性

taosKeeper 接收各组件推送的指标，通过 WebSocket 写入 TDengine `log` 库；经典路径下亦接收 `taosd` 审计上报并写入审计库。可靠性要点：

- **采集端缓冲**：taosd / taosAdapter 等组件本地缓存最近一批指标，taosKeeper 或 taosAdapter 临时不可用时不会阻塞主路径（指标采集失败不影响业务读写）。
- **写入重试**：taosKeeper 到 taosAdapter 的 WebSocket 连接断开后自动重连；期间待写入指标暂存内存，重连后续写。
- **降级容忍**：监控链路短暂中断会导致少量指标丢点（监控图表出现空洞），但业务数据不受影响。默认审计路径下，Keeper 长时间不可用也会造成审计落库中断（业务读写仍不受影响）；需要审计与 Keeper 解耦时使用 `auditSaveInSelf`。

```toml
# /etc/taos/taoskeeper.toml
[tdengine]
host     = "localhost"
port     = 6041
username = "keeper_writer"
password = "KeeperPass123!"
usessl   = false
```

### 5.2 Keeper 失效影响

- taosKeeper 不在业务请求链路上，失效不影响业务读写。
- 失效期间新产生的指标会先停留在组件侧缓冲区；Keeper 恢复后继续补推。
- 若中断时间超过缓冲上限，会出现监控断层，影响可观测性，但不会回溯影响业务数据。
- 默认审计路径下，Keeper 失效期间审计无法经其落库；恢复后依赖 `taosd` 侧缓冲与重试窗口，超出窗口可能出现审计缺口。本集群直写（`auditSaveInSelf`）不受此影响。

生产建议：小规模集群单实例即可；如需连续观测（及经 Keeper 的审计连续性），可部署双实例并由前置 LB 分发。

---

## 6. 存储与审计落盘可靠性

数据最终在 taosd 侧落盘。本层通过 **WAL 预写日志 + 快照 + TDE 透明加密 + 备份恢复 + 审计日志权限分离** 五类机制保障：进程崩溃不丢数据、磁盘物理失窃不泄露、误删/灾难可恢复、审计日志不可篡改。

### 6.1 WAL（预写日志）

每个 VNode 维护独立 WAL，写入请求先追加到 WAL 并 fsync 落盘，再复制到 Follower，多数派确认后才 ACK 客户端。

```sql
-- WAL 模式 1：内存缓存（最高性能，崩溃可能丢最新数据）
CREATE DATABASE db WAL_LEVEL 1;

-- WAL 模式 2：fsync 落盘（推荐生产环境）
CREATE DATABASE db WAL_LEVEL 2 WAL_FSYNC_PERIOD 3000;
```

```ini
# taos.cfg
walRetentionPeriod 3600   # WAL 保留时长（秒）
walRetentionSize   0      # WAL 保留大小（字节）
```

### 6.2 快照（Snapshot / STT）

WAL 超过保留阈值后，TDengine 将已提交数据合并为 STT（Snapshot Tier）文件并清理旧 WAL。快照同时用于：

- **节点恢复加速**：新加入或长时间离线的 Follower 直接通过快照追平，不必回放全部 WAL。
- **Raft 日志压缩**：避免 WAL 无限增长。

快照由系统自动触发，一般无需人工干预；可通过 `walRetentionPeriod` / `walRetentionSize` 控制 WAL 保留窗口（窗口内的增量才支持 TMQ/taosX 订阅消费）。

### 6.3 透明数据加密（TDE）

操作细则（密钥层级、轮换、建加密库）见 [静态数据保护](./06-data-security.md)。

> TDE 保护**存储层**数据（落盘的数据文件、WAL、快照）。传输层加密参见 [全链路传输安全与压缩](./02-full-trace-transport.md)。

**密钥层次：**

```text
SVR_KEY → DB_KEY → CFG_KEY → META_KEY → DATA_KEY
```

**加密算法：**

| 算法 | 适用场景 |
|------|---------|
| SM4-CBC | 国密合规（常用） |
| AES-128-CBC | 国际标准 |

**启用示例（v3.4+，须先用 `taosk` 生成含 `DATA_KEY` 的密钥）：**

```sql
CREATE DATABASE db ENCRYPT_ALGORITHM 'SM4-CBC';
SHOW ENCRYPT_STATUS;
```

### 6.4 数据备份与恢复

#### 6.4.1 taosdump 逻辑备份（全量 + 增量）

```bash
# 全量备份
taosdump -h localhost -u tduser -P SecurePass123! -o /backup

# 增量备份
taosdump -h localhost -u tduser -P SecurePass123! -D mydb \
  -S "2024-01-01 00:00:00" -o /backup/incremental

# 恢复
taosdump -h localhost -u tduser -P SecurePass123! -i /backup/mydb
```

#### 6.4.2 taosX 数据迁移与复制（持续同步）

```bash
taosX run \
  --from "taos://source-host:6030/mydb" \
  --to   "taos://backup-host:6030/mydb_backup"
```

依托第 3 节的 checkpoint + 持久化队列 + 任务重启机制，taosX 复制链路本身也具备断点续传与自动恢复能力。

#### 6.4.3 备份策略建议

| 策略 | 方式 | 频率 | 覆盖场景 |
|------|------|------|---------|
| 多副本 | `REPLICA 3`（或企业版 `REPLICA 2`） | 实时，集群内 | 单节点故障（双副本容错边界见 4.2 节） |
| 实时复制 | taosX | 持续同步到异地 | 数据中心级灾难 |
| 增量备份 | taosdump + 时间范围 | 每天一次 | 误删除、逻辑损坏 |
| 全量备份 | taosdump | 每周一次 | 兜底冷备 |

### 6.5 审计日志不可篡改（权限分离）

审计库配置、操作列表与角色模型见 [审计与合规](./07-audit-and-compliance.md) 与 [权限管理 · 审计数据库](../05-tdengine-sql/07-user-and-privilege/02-grant.md#审计数据库)。

审计日志写入带 `IS_AUDIT` 的审计库（默认名常为 `audit`，不是监控用的 `log` 库）。`v3.4.0.0+` 创建审计库时服务端强制 `VGROUPS 1`、`WAL_LEVEL 2`、`PRECISION ns`、`ENCRYPT_ALGORITHM` 不得为 `none`、`KEEP ≥ 1825d` 等约束。通过 RBAC 降低被回溯篡改的风险：

- 仅 `SYSAUDIT_LOG` 可向审计库写入；仅 `SYSAUDIT` 可查看审计表数据。
- 不允许删除/修改审计表及其数据行；审计库默认 `ALLOW_DROP = 0`（删除前须改为 `1`，且仅 `SYSAUDIT` 可删改审计库）。
- 业务账号不应持有审计库写权限；查看侧使用审计员角色，与业务读写分离。
- `auditLevel ≥ 5` 时，对审计表的查询也可能产生新的审计事件。
- 经 Keeper 路径可将审计写到异地目标集群；亦可结合第 6.4 节的 taosX 复制，将审计库同步到独立安全集群，进一步降低单集群被篡改风险。

---

## 7. 常见故障排查

| 错误码 | 描述 | 处理 |
|--------|------|------|
| 0x80000903 | Sync timeout | 检查网络，等待选举完成后重试 |
| 0x8000090C | Sync leader unreachable | 检查 DNode 状态和网络 |
| 0x80000911 | Sync not ready | 等待节点恢复完成 |
| 0x80000914 | Sync leader restoring | 等待新 Leader 日志重演完成 |
| 0x80000916 | Sync buffer full | 降低写入并发 |
| 0x80000917 | Sync write stall | 检查磁盘 IO |

---

## 8. 部署清单

### 8.1 入口层

- [ ] 各语言连接器已配置连接池 + 超时
- [ ] 应用层对幂等操作包装了指数退避重试
- [ ] taosExplorer 前端部署 ≥ 2 实例 + 负载均衡
- [ ] 运维脚本使用 `taosX` 断点续传 / `taosdump` 时间切片续跑

### 8.2 接入层

- [ ] taosAdapter 查询限流（`queryLimitEnable = true`）
- [ ] taosAdapter 内存保护阈值（`pauseQueryMemoryThreshold` / `pauseAllMemoryThreshold`）
- [ ] 客户端配置 `firstEp` + `secondEp` 双入口
- [ ] 负载均衡器接入 taosAdapter `/-/ping` 健康检查

### 8.3 数据采集链路

- [ ] taosX 持久队列磁盘预留充足（建议 ≥ 50 GB）
- [ ] 数据源 checkpoint 位点已显式配置并完成断点续传验证
- [ ] 自动恢复阈值与任务重启路径已演练
- [ ] taosX-Agent `keep_online = true` 已启用（如使用 taosX-Agent）

### 8.4 集群内部

- [ ] 集群至少 3 个 DNode；生产库优先 `REPLICA 3`（成本敏感可用企业版 `REPLICA 2`，见 [双副本方案](../12-operations-and-tooling/02-operations/11-ha/02-replica2.md)）
- [ ] 3 个 MNode 分布在不同 DNode
- [ ] 已演练 `RESTORE DNODE` / `RESTORE VNODE` 恢复流程
- [ ] `SHOW VGROUPS` 定期巡检副本状态

### 8.5 可观测性接入

- [ ] taosKeeper 正常运行，`log` 库指标持续写入
- [ ] 组件侧监控缓冲容量与可接受断链时长匹配
- [ ] 关键环境已评估是否需要双 Keeper + 前置 LB
- [ ] 若审计经 Keeper：Keeper 高可用与审计连续性已评估；或已改用 `auditSaveInSelf`

### 8.6 存储与审计落盘

- [ ] 生产数据库 `WAL_LEVEL 2`，`WAL_FSYNC_PERIOD` 按 RPO 调优
- [ ] WAL 保留窗口满足 TMQ / taosX 订阅需求
- [ ] TDE 已开启（如有合规要求）
- [ ] taosdump 每日增量 + 每周全量定时任务已配置
- [ ] taosX 异地实时复制已运行
- [ ] 已创建 `IS_AUDIT` 审计库（`VGROUPS 1`、加密、`WAL_LEVEL 2`、`PRECISION ns`、`KEEP ≥ 1825d`）并启用审计
- [ ] 审计查看使用 `SYSAUDIT`；业务账号无审计库写权限
