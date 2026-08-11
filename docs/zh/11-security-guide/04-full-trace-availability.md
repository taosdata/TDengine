---
sidebar_label: 全链路高可用
title: 全链路高可用
description: 多实例、故障切换与双活等高可用能力
toc_max_heading_level: 3
---

本文描述 TDengine TSDB 按统一六层框架展开的高可用链路——从入口层、接入层、数据采集链路、集群内部、可观测性接入，到存储层。本文逐段讲解故障自动切换与负载分发；原“全链路负载均衡”的内容已并入对应章节。

> 术语约定
>
> - 接入层：指入口层之后、taosd 之前的协议/代理层，包括 taosAdapter 和 taosc 两条物理路径。
> - 任务重启：指自动恢复手段全部用尽后，关闭并重新拉起 taosX 任务；它不同于“数据源连接重试”。
> - Checkpoint：默认指数据源侧进度位点；是否使用 taosX-Agent，只影响本地缓存位置，不改变 checkpoint 的恢复语义。

---

## 概述

```mermaid
graph TD
    classDef client fill:#f39c12,color:#000,stroke:#d68910
    classDef svc    fill:#1a6ea8,color:#fff,stroke:#0d4f7d
    classDef core   fill:#2c3e50,color:#fff,stroke:#1a252f
    classDef ops    fill:#16a085,color:#fff,stroke:#0e6655
    classDef store  fill:#7f8c8d,color:#fff,stroke:#566573
    classDef ext    fill:#27ae60,color:#fff,stroke:#1e8449

    ENTRY(["① 入口层<br/>程序化入口 / Web UI / CLI"])
    ACCESS(["② 接入层<br/>Nginx / HAProxy + taosAdapter / taosc"])
    INGEST(["③ 数据采集链路<br/>外部数据源 → taosX-Agent（可选）→ taosX / XNode"])
    CORE(["④ 集群内部<br/>taosd 集群：多个 DNode + 多副本 + Leader 选举"])
    OBS(["⑤ 可观测性接入<br/>taosKeeper 多实例 + LB"])
    STORE(["⑥ 存储层<br/>VGroup / WAL / 快照 / 备份恢复"])
    DR(["扩展：双活跨集群灾备"])

    ENTRY --> ACCESS

    ACCESS --> INGEST

    INGEST --> CORE

    CORE --> OBS

    OBS --> STORE

    STORE -.-> DR

    class ENTRY client
    class ACCESS,INGEST svc
    class CORE core
    class OBS ops
    class STORE store
    class DR ext
```

> 上图按文档层次而非运行时主写入路径展开；其中 taosKeeper 属于旁路观测链路，双活灾备作为六层之外的扩展能力单列说明。

### 高可用层次总览

| 层次 | 组件 | HA 机制 | 说明 |
|------|------|---------|------|
| ① 入口层 | 连接器 / taosExplorer / CLI | 多节点 URL、多实例、客户端 failover | 连接建立阶段避免单点 |
| ② 接入层 | taosAdapter / taosc | Nginx/HAProxy + 健康检查；`firstEp`/`secondEp` + Leader 重定向 | 请求可路由到任一健康 taosd |
| ③ 数据采集 | taosX / XNode / taosX-Agent | XNode 多实例、任务迁移、任务重启、可选本地缓存 | 任务不中断、不丢采 |
| ④ 集群内部 | taosd 集群 | 多副本 + Leader 选举 | 单节点故障不中断写入 |
| ⑤ 可观测性接入 | taosKeeper | 多实例 + LB | 指标（及默认路径审计）连续；失效不阻塞业务 |
| ⑥ 存储层 | VGroup / WAL / 快照 | 多副本 + 追平 + 备份恢复 | 副本恢复与灾备兜底 |

---

## 1. 入口层高可用

入口层的高可用目标是：任一入口实例故障时，客户端仍能建立连接并把请求送到健康后端。关键手段是**多节点 / 多实例** + **客户端 failover**。

### 1.1 程序化入口（应用 / 各语言连接器）

#### 1.1.1 多节点 URL 配置

生产环境禁止硬编码单节点。应用侧连接串应指向 LB VIP 或多节点列表。

```java
// Java — 指向 LB VIP（WebSocket/REST 路径）
String url = "jdbc:TAOS-RS://lb-vip:6041/db";
```

```go
// Go（WebSocket）
db, _ := sql.Open("taosWS", "tduser:SecurePass123!@ws(lb-vip:6041)/db")
```

```python
# Python（WebSocket）
conn = taosws.connect("taos+ws://tduser:SecurePass123!@lb-vip:6041/db")
```

taosc 私有协议路径则以 `firstEp,secondEp` 多端点形式直连 taosd（详见 2.2 节）：

```java
// JDBC（嵌入 taosc，私有协议多 EP）
String url = "jdbc:TAOS://dnode1:6030,dnode2:6030/db";
```

```go
// Go（嵌入 taosc，私有协议多 EP）
cfg := taos.NewConfig()
cfg.Addr = "dnode1:6030,dnode2:6030"
```

#### 1.1.2 配置后如何自动 failover

**WebSocket / REST 路径（经 taosAdapter）**：

1. 连接器只连 `lb-vip:6041`，不感知单个 taosAdapter 实例。
2. 前置 Nginx / HAProxy 通过 `/-/ping` 把失效实例摘除。
3. 当前连接若已断开，连接器或应用层按重连策略重新建立连接。
4. 新连接会自动落到健康的 taosAdapter；该实例内部再由 taosc 路由到健康 taosd。

**原生 taosc 路径（直连 taosd）**：

1. 客户端配置 `firstEp` / `secondEp` 或多 EP DSN。
2. 首个接入点不可达时，taosc 自动切到下一个入口。
3. 拿到集群拓扑后，请求会直接路由到对应 Leader。
4. Leader 切换时，taosc 根据返回码刷新路由并透明重试。

#### 1.1.3 连接池与负载分发建议

连接池不仅用于并发与性能，也决定客户端如何把重连后的连接重新打散到多个 taosAdapter 实例。

| 语言 | 库 | 最大连接数 | 建议 |
|------|-----|----------|------|
| Java | HikariCP | `maximumPoolSize=10` | 配合 `connectionTestQuery`，故障后快速剔除坏连接 |
| Go | `database/sql` | `SetMaxOpenConns(20)` | 配合 `SetConnMaxLifetime()` 促使连接周期性重建 |
| Python | 应用层管理 | N/A | 捕获异常后重建连接并归还连接池 |
| Node.js | `taos.sqlConnect()` | N/A | 对长连接异常做透明重建 |

### 1.2 Web UI 入口（taosExplorer 多实例）

taosExplorer 本身是无状态 Web 前端，可在多台主机上部署多个实例，由前置 Nginx / HAProxy 反向代理实现高可用。

```text
explorer-1: taosExplorer:6060
explorer-2: taosExplorer:6060
```

```nginx
upstream tdengine_explorer {
    ip_hash;
    server explorer1.example.com:6060 max_fails=3 fail_timeout=10s;
    server explorer2.example.com:6060 max_fails=3 fail_timeout=10s;
}

server {
    listen 443 ssl;
    server_name explorer.example.com;
    ssl_certificate     /etc/taos/certs/explorer.crt;
    ssl_certificate_key /etc/taos/certs/explorer.key;

    location / {
        proxy_pass http://tdengine_explorer;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_next_upstream error timeout http_502 http_503 http_504;
    }
}
```

高可用如何实现：

1. 浏览器始终访问统一的 `explorer.example.com`。
2. Nginx 只把新请求转发到健康实例，实例失败后自动切到其它副本。
3. taosExplorer 会话无状态（需要在每个节点注册），单实例重启不会导致集群级单点。
4. taosExplorer 依赖长 WebSocket 会话且连接尽量固定，建议使用 `ip_hash` 策略。

### 1.3 命令行工具入口

运维 CLI 需要能在集群任一节点故障时仍可连接其它节点。

**`taos` shell（嵌入 taosc，私有协议）**：通过 `taos.cfg` 的 `firstEp` / `secondEp` 实现多端点故障转移：

```ini
# /etc/taos/taos.cfg
firstEp  dnode1:6030
secondEp dnode2:6030
```

`taos` 启动时先尝试 `firstEp`，失败后自动切换 `secondEp`；获得完整 DNode 列表后，后续请求再按 Leader 路由。

**`taosX` CLI（数据管道）**：源和目标 DSN 均可写成 LB VIP 或多节点：

```bash
taosX run \
  -f "mqtt://mqtt.example.com:1883" \
  -t "taos+ws://tduser:SecurePass123!@lb-vip:6041/db"
```

**`taosdump`（备份恢复）**：通过 `-h` 指定 LB VIP 或连接 `firstEp`；若单个节点不可达，可重试指向其它 DNode：

```bash
taosdump -h lb-vip -P 6030 -u tduser -p'SecurePass123!' -D db -o /backup
```

---

## 2. 接入层高可用

> **关于 taosc**：taosc 是 TDengine 的原生客户端库，作为**独立组件**向上提供 C 语言 API 和 DSN 连接接口，向下通过**私有协议**（TCP :6030）与 taosd 集群通信；其内置的 `firstEp`/`secondEp` 多 EP 探测与 Leader 重定向是接入层高可用的关键机制。路径 A（WebSocket/REST）由 taosAdapter 在服务端内部调用 taosc 完成到 taosd 的最后一跳（多实例 Adapter 前置 LB + 每个 Adapter 内嵌的 taosc 做节点 failover）；路径 B 则由应用/CLI 直接嵌入 taosc 动态库，完全由 taosc 完成节点切换。两条路径最终都经 taosc 进入 taosd。

接入层的高可用目标是：**入口层请求能路由到任一健康的 taosd 节点**。负载分发与故障切换在这一层合并实现。

### 2.1 路径 A：WebSocket / REST → taosAdapter

#### 2.1.1 taosAdapter 多实例部署

多个 taosAdapter 实例形成无状态集群：

```text
adapter-1: taosAdapter:6041
adapter-2: taosAdapter:6041
adapter-3: taosAdapter:6041
```

任意 taosAdapter 实例均可接收请求并转发到 taosd，故可在其前端放置 LB 做健康检查与流量分发。

#### 2.1.2 Nginx / HAProxy 配置

```nginx
upstream tdengine_adapter {
    least_conn;
    server dnode1:6041 max_fails=3 fail_timeout=10s;
    server dnode2:6041 max_fails=3 fail_timeout=10s;
    server dnode3:6041 max_fails=3 fail_timeout=10s;
}

server {
    listen 6041;

    location / {
        proxy_pass http://tdengine_adapter;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_connect_timeout 5s;
        proxy_read_timeout 300s;
        proxy_next_upstream error timeout http_502 http_503 http_504;
    }

    location /-/ping {
        proxy_pass http://tdengine_adapter/-/ping;
    }
}
```

```haproxy
defaults
    mode http
    timeout connect 5s
    timeout client  300s
    timeout server  300s

frontend tdengine_fe
    bind *:6041
    default_backend tdengine_be

backend tdengine_be
    balance leastconn
    option httpchk GET /-/ping
    server ta1 dnode1:6041 check inter 3s fall 3 rise 2
    server ta2 dnode2:6041 check inter 3s fall 3 rise 2
    server ta3 dnode3:6041 check inter 3s fall 3 rise 2
```

#### 2.1.3 高可用如何实现

1. LB 通过 `/-/ping` 只保留健康的 taosAdapter 实例。
2. 新请求按 `least_conn` / 轮询策略分发到健康实例，实现日常流量打散（区别于 taosExplorer）。
3. 实例返回 503、超时或断开时，LB 停止转发到该节点。
4. taosAdapter 内部调用 taosc 继续把请求路由到健康的 taosd Leader。

#### 2.1.4 策略选型

| 场景 | 建议策略 | 说明 |
|------|---------|------|
| 无状态 REST | 轮询 / `least_conn` | 新请求均匀打散，无需粘性 |
| WebSocket 长连接 | `ip_hash` / `balance source` | 尽量让同一客户端落在同一实例 |
| 混合负载 | `least_conn` | 避免长连接堆积在单节点 |

### 2.2 路径 B：taosc → taosd 私有协议（多 EP 探测）

原生连接（嵌入 taosc 作为独立客户端库）通过 TDengine **私有协议**（TCP :6030）直连 taosd，并借助 `firstEp` / `secondEp` 实现节点探测与自动 failover：

```ini
# taos.cfg
firstEp  dnode1:6030
secondEp dnode2:6030
```

- taosc 首次连接 `firstEp` 获取完整 MNode / DNode 列表。
- `firstEp` 不可达时自动切换 `secondEp`。
- 获得列表后，自动分发请求到对应 Leader。
- Leader 切换后，taosc 通过返回码识别并自动更新路由。

**连接器 DSN 支持多 EP**：

```java
// JDBC（嵌入 taosc，私有协议）
String url = "jdbc:TAOS://dnode1:6030,dnode2:6030/db";
```

```go
// Go（嵌入 taosc，私有协议）
cfg := taos.NewConfig()
cfg.Addr = "dnode1:6030,dnode2:6030"
```

> 生产建议：`firstEp` 与 `secondEp` 必须落在**不同主机 / 不同机架**，否则单机或单机架故障会同时失效。

---

## 3. 数据采集链路高可用

数据采集链路按“**外部数据源 → taosX-Agent（可选）→ taosX / XNode → taosAdapter → taosd**”工作。HA 目标是**任务不中断、不丢采、故障后可自动接管**。

### 3.1 XNode 多实例与任务迁移

taosX 管理多个 XNode 实例。任务失败时可自动调度到健康节点：

```text
taosX
  ├── XNode-1 @ Edge-A  [Running]
  ├── XNode-2 @ Edge-B  [Running]
  └── XNode-3 @ Edge-C  [Standby]
```

XNode 之间通过 taosX 的调度器统一管理，单个 XNode 宕机不影响全局任务运行。

### 3.2 自动恢复与任务重启

taosX 的自动恢复顺序通常是：

1. Source 连接重试；
2. Sink（taosAdapter）重连；
3. 回放本地持久化队列；
4. 若上述手段全部失败，则触发**任务重启**。

其中，checkpoint 记录的是**数据源侧位点**，用于任务重启后恢复采集进度；它和 Sink 侧的持久化队列不是同一机制。

| 概念 | 作用范围 | 触发者 | 是否关闭任务 |
|------|---------|-------|-------------|
| 数据源连接重试 | 当前 Source 连接 | Source 连接器 | 否 |
| Sink 重连 | 当前下游连接 | taosX / taosAdapter | 否 |
| 任务重启 | 整个任务 | 调度器 / 运维 | 是 |

### 3.3 taosX-Agent 是否必选

是否部署 taosX-Agent，只影响边缘缓存策略：

| 数据源场景 | 是否建议 Agent | 原因 |
|-----------|---------------|------|
| Kafka / Pulsar / TMQ | 通常不需要 | 可依赖上游持久化缓存与 offset/cursor |
| MQTT QoS 0 / 设备直采 | 建议需要 | 上游通常无法可靠回放 |
| OPC-UA / OPC-DA / 工业边缘网络 | 建议需要 | 弱网常见，需要本地缓存跨断网续传 |
| MySQL / PostgreSQL 增量拉取 | 视场景 | 常依赖源表保留窗口与增量列 |

### 3.4 taosX-Agent 断线重连

taosX-Agent 部署在远端边缘，长连接到 taosX。网络抖动或 taosX 实例切换时：

- Agent 自动重连并恢复会话；
- 采集缓冲（本地持久队列）保证重连期间数据不丢；
- 重连成功后，按已确认位点继续上送。

存储转发的配置细则见 [存储转发（Store and Forward）](../12-operations-and-tooling/03-components/07-taosx-agent/store-and-forward.md)。

---

## 4. 集群内部高可用

集群内部的高可用核心是：**多个 DNode + 多副本 + Leader 选举**。图中不展开 MNode / VGroup / Raft 细节，但实际故障切换依赖这些机制完成。

### 4.1 MNode 元数据高可用

MNode 管理集群元数据（库表定义、用户权限、VGroup 分布），采用 Raft 多副本（推荐 3 副本）：

```sql
SHOW MNODES;
CREATE MNODE ON DNODE 2;
CREATE MNODE ON DNODE 3;
```

| 节点 | 角色 | 状态 |
|------|------|------|
| DNode-1 | Leader | Ready |
| DNode-2 | Follower | Ready |
| DNode-3 | Follower | Ready |

### 4.2 VGroup 高可用概览

VGroup 采用多副本承载时序数据：

```sql
CREATE DATABASE db REPLICA 3 VGROUPS 10;
SHOW db.VGROUPS;
```

**写入路由**：taosc 自动将写入发送到 VNode Leader。Leader 切换后 taosc 自动更新路由。

**读取一致性**：

| 读取模式 | 参数 | 说明 |
|---------|------|------|
| 最终一致 | `queryPolicy=1` | 读 Follower，延迟低 |
| 强一致 | `queryPolicy=3` | 读 Leader，保证最新 |

### 4.3 Leader 选举机制

MNode 与 VGroup 均使用 Raft：

- Leader 定期发送心跳；Follower 超过选举超时未收到心跳则发起新一轮选举。
- 多数派（N/2+1）投票通过后新 Leader 生效。
- 选举期间（通常 < 30 秒）相应 Raft 组只读不写。
- 新 Leader 通过比较任期与 LogIndex，确保已提交日志不会丢失。

---

## 5. 可观测性接入高可用

taosKeeper 负责接收 taosd / taosAdapter / taosX / taosExplorer 输出的监控指标并写回 `log` 库。默认审计路径（`auditSaveInSelf = 0`）下，企业版审计也经 taosKeeper 写入审计库；`auditSaveInSelf = 1`（`v3.4.1.0+`）时审计不经 Keeper。详见 [审计与合规](./07-audit-and-compliance.md)。

### 5.1 taosKeeper 多实例部署

```text
keeper-1: taosKeeper:6043
keeper-2: taosKeeper:6043
```

```nginx
upstream tdengine_keeper {
    least_conn;
    server keeper1.example.com:6043 max_fails=3 fail_timeout=10s;
    server keeper2.example.com:6043 max_fails=3 fail_timeout=10s;
}

server {
    listen 6043;
    location / {
        proxy_pass http://tdengine_keeper;
        proxy_next_upstream error timeout http_502 http_503 http_504;
    }
}
```

各组件的 `monitorFqdn` / `keeperUrl`（及经 Keeper 的审计上报目标）指向 LB VIP 而非单实例。

### 5.2 失效影响

- taosKeeper 不在业务请求链路上，失效**不影响业务读写**。
- 失效期间新产生的指标先停留在组件侧缓冲区；Keeper 恢复后继续补推。
- 若中断时间超过缓冲上限，会出现监控断层，影响可观测性，但不会导致业务数据回滚或写入失败。
- 默认审计路径下，Keeper 失效还会中断审计落库；需要审计与 Keeper 解耦时使用 `auditSaveInSelf`。

---

## 6. 存储层高可用

存储层的 HA 核心是 **VGroup 多副本放置** + **新副本追平**。

### 6.1 VGroup 副本放置策略

每个 VGroup 包含 N 个 VNode（N = `REPLICA`），分布在不同 DNode：

- 同一 VGroup 的多个 VNode 必须分布在不同 DNode（系统强制约束）。
- 生产推荐：DNode 分布在不同主机 / 不同机架 / 不同机柜。
- `REPLICA 3` 允许 1 个 DNode 故障；更高副本数按业务要求规划。

```sql
-- 查看 VGroup 副本分布
SHOW db.VGROUPS;
```

### 6.2 快照 + WAL 追平新副本

当 Follower 副本失联超过窗口或新增副本时，Leader 通过 **快照 + 增量 WAL** 将其追平：

1. Leader 触发快照（SNAPSHOT）并将其传输给落后副本；
2. 落后副本加载快照至状态机；
3. Leader 继续发送快照点之后的 WAL 日志，逐条回放；
4. 追平后副本进入 Ready 状态，开始参与多数派投票。

该机制保证 DNode 宕机替换、扩容新增副本等场景下的数据自动恢复，详见 [全链路高可靠](./03-full-trace-reliability.md) 的 WAL / 快照章节。

---

## 7. 双活跨集群灾备（Active-Active）

> 3.3.6+ Enterprise

两个集群互为备份，各自接受写入，数据双向实时同步，用于跨机房 / 跨地域容灾。

```text
集群 A（北京） ↔ 集群 B（上海）
  taosd ①②③  ──taosX──→  taosd ④⑤⑥
  taosd ①②③  ←──taosX──  taosd ④⑤⑥
```

### 7.1 配置步骤

1. 两个集群独立部署（`REPLICA 3`）。
2. 部署 taosX 同步任务（A→B 和 B→A）。
3. 同步任务使用数据库订阅（TMQ）获取增量数据。
4. 配置冲突解决策略（默认：时间戳优先）。

### 7.2 切换策略

| 模式 | 切换时间 | 数据丢失 |
|------|---------|---------|
| 计划切换 | 0 | 0 |
| 故障切换 | < 1 分钟 | ≤ 同步延迟量 |

应用侧通过 DNS / GSLB / 前置 LB 切换流量到备用集群；连接串只需更换 VIP 即可。

---

## 8. 部署清单

### 8.1 入口层

- [ ] 应用连接器 URL 指向 LB VIP 或多 EP 列表，无硬编码单节点
- [ ] 连接池参数已配置，坏连接能被快速剔除
- [ ] taosExplorer 部署 ≥ 2 实例，前置 Nginx / HAProxy 分发
- [ ] `taos` / `taosX` / `taosdump` 等 CLI 配置多 EP 或指向 LB

### 8.2 接入层

- [ ] taosAdapter 部署 ≥ 2 实例
- [ ] Nginx / HAProxy 已配置 `/-/ping` 健康检查
- [ ] 已根据 REST / WebSocket 场景选择轮询、`least_conn` 或粘性策略
- [ ] `firstEp` / `secondEp` 不在同一主机

### 8.3 数据采集链路

- [ ] XNode ≥ 2 个用于关键数据接入任务
- [ ] 已验证 checkpoint 恢复、Sink 重连与任务重启路径
- [ ] taosX-Agent 本地持久队列容量与网络断连时长匹配（如使用 taosX-Agent）

### 8.4 集群内部

- [ ] DNode ≥ 3 个，分布不同主机 / 机架
- [ ] 3 个 MNode 分布在不同 DNode
- [ ] 已演练 Leader 故障自动选举

### 8.5 可观测性接入

- [ ] taosKeeper ≥ 2 实例（关键环境）
- [ ] 各组件 `monitorFqdn` / `keeperUrl` 指向 VIP
- [ ] 已评估 Keeper 失效时可接受的监控断层时长

### 8.6 存储层

- [ ] `REPLICA 3` 用于所有生产数据库
- [ ] VGroup 副本分布跨主机 / 跨机架
- [ ] 已演练 DNode 宕机后副本追平

### 8.7 灾备

- [ ] 双活跨集群（如有 RPO < 5 分钟要求）
- [ ] taosdump 定时备份（如有合规要求）
- [ ] 已演练故障切换流程
