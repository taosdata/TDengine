---
sidebar_label: 心跳与切主
title: 心跳与切主分析与调试
toc_max_heading_level: 4
---

TDengine 通过心跳维持集群成员之间的存活感知，并在心跳超时后由 Raft 协议（多副本）或 Arbitrator（双副本）完成重新选主。当集群出现频繁切主、选不出主、写入短暂不可用等现象时，通常需要围绕心跳与选举相关参数进行分析与调整。本文说明心跳与选举的机制、相关参数清单、调参建议，以及出现问题时需要收集的信息。

## 心跳与选举机制概述

TDengine 集群中存在三类与"心跳和切主"直接相关的交互：

1. **vnode（Raft 组）心跳**：每个 vgroup 内的 leader 周期性向 follower 发送 `sync-heartbeat`，follower 回复 `sync-heartbeat-reply`。follower 在选举超时时间内没有收到 leader 的心跳，就会发起新一轮选举。
2. **mnode（Raft 组）心跳**：mnode 之间同样是 Raft 组，心跳与选举参数与 vnode 相互独立，因此 mnode 的切主与 vnode 的切主需要分别分析。
3. **dnode 状态上报**：dnode 周期性向 mnode 上报状态（`status` 消息）。mnode 在 `statusTimeoutMs` 内没有收到某个 dnode 的状态上报，就将该 dnode 标记为 offline，进而可能触发该 dnode 上所有 vgroup 的切主。

对于双副本（Arbitrator）部署，vgroup 的选主不由 Raft 多数派决定，而是由 mnode 充当 Arbitrator 进行仲裁，vnode 与 Arbitrator 之间通过独立的仲裁心跳（`arb-hb`）交互，相关参数见下文"双副本（Arbitrator）参数"一节。

### 选举超时的实际取值

选举超时并非固定值：每次重置选举定时器时，系统会在 `[选举基线, 2 × 选举基线]` 区间内随机取值（日志中形如 `reset elect timer, min:%d, max:%d, ms:%d`）。例如在默认配置下，vnode 的实际选举超时在 4000 ms ~ 8000 ms 之间随机分布，这种随机化可以降低多个节点同时发起选举的概率。

因此，判断心跳与选举参数是否合理时，需要保证：**选举基线 > 心跳间隔 × 2**，否则心跳的正常抖动就可能触发无谓的选举。

## 心跳与选举参数清单

以下参数绝大多数为全局配置参数，必须使用 `ALTER ALL DNODES` 修改；标注为"局部"的参数可以使用 `ALTER DNODE <dnode_id>` 单独修改。参数生效方式均参见各参数说明。

### vnode / mnode（Raft 组）心跳与选举参数

| 参数名 | 默认值 | 单位 | 取值范围 | 作用域 | 是否支持动态修改 | 说明 |
| ------ | ------ | ---- | -------- | ------ | ---------------- | ---- |
| `syncVnodeElectIntervalMs` | 4000 | ms | 10 ~ 172800000 | 全局 | 支持，立即生效 | vnode Raft 组的选举超时基线，实际超时在 `[基线, 2 × 基线]` 内随机取值。自 `v3.3.6.23` 引入 |
| `syncVnodeHeartbeatIntervalMs` | 1000 | ms | 10 ~ 172800000 | 全局 | 支持，立即生效 | vnode Raft 组 leader 发送心跳的周期。自 `v3.3.6.23` 引入 |
| `syncMnodeElectIntervalMs` | 3000 | ms | 10 ~ 172800000 | 全局 | 支持，立即生效 | mnode Raft 组的选举超时基线。自 `v3.3.6.23` 引入 |
| `syncMnodeHeartbeatIntervalMs` | 500 | ms | 10 ~ 172800000 | 全局 | 支持，立即生效 | mnode Raft 组 leader 发送心跳的周期。自 `v3.3.6.23` 引入 |
| `syncElectInterval` | 4000 | ms | 10 ~ 172800000 | 全局 | 不支持 | 早期版本的心跳/选举参数，已被 `syncVnodeElectIntervalMs` 与 `syncMnodeElectIntervalMs` 取代，当前版本不再生效，不建议修改 |
| `syncHeartbeatInterval` | 1000 | ms | 10 ~ 172800000 | 全局 | 不支持 | 早期版本的心跳/选举参数，已被 `syncVnodeHeartbeatIntervalMs` 与 `syncMnodeHeartbeatIntervalMs` 取代，当前版本不再生效，不建议修改 |
| `syncHeartbeatTimeout` | 20000 | ms | 10 ~ 172800000 | 全局 | 不支持 | leader 判断副本心跳响应是否超时的阈值。当超过该时间仍未收到多数派（quorum）副本的心跳响应时，leader 会拒绝新的写入请求（propose 返回心跳超时错误） |
| `syncSnapReplMaxWaitN` | 128 | 个 | 16 ~ 256 | 全局 | 支持，立即生效 | 快照复制时允许等待的最大日志条数。副本落后过多需要走快照复制时，若长时间无法追上，会导致该副本长时间无法参与选举 |
| `snapshotRateLimit` | 0 | MB/s | 0 ~ 10240 | 全局 | 支持，立即生效 | 单个 dnode 上快照发送的总带宽上限，0 表示不限速。多个 vgroup 同时做快照复制时可能压满磁盘 IO，间接导致心跳超时。自 `v3.4.2.0` 引入 |

### dnode 状态上报参数

| 参数名 | 默认值 | 单位 | 取值范围 | 作用域 | 是否支持动态修改 | 说明 |
| ------ | ------ | ---- | -------- | ------ | ---------------- | ---- |
| `statusInterval` | 1 | s | 1 ~ 30 | 全局 | 支持，立即生效 | dnode 向 mnode 上报状态的周期。设置该参数时会自动换算为 `statusIntervalMs`。自 `v3.3.0.0` 引入 |
| `statusIntervalMs` | 1000 | ms | 50 ~ 30000 | 全局 | 支持，立即生效 | 同上，毫秒版本。自 `v3.3.6.23` 引入 |
| `statusTimeoutMs` | 5000 | ms | 50 ~ 30000 | 全局 | 支持，立即生效 | mnode 判定 dnode 离线的超时时间。dnode 超过该时间未上报状态即被标记为 offline，会触发其上的 vgroup 切主。自 `v3.3.6.23` 引入 |
| `statusSRTimeoutMs` | 5000 | ms | 50 ~ 30000 | 全局 | 支持，立即生效 | 状态类请求（status、key sync、config 等）的发送 - 接收超时时间。自 `v3.3.6.23` 引入 |

### 双副本（Arbitrator）参数

以下参数仅在双副本（Arbitrator）部署下生效。带 `Sec` 后缀的版本单位为秒，带 `Ms` 后缀的版本单位为毫秒，二者等价，设置 `Sec` 版本时会自动换算为 `Ms` 版本。

| 参数名 | 默认值 | 单位 | 取值范围 | 作用域 | 是否支持动态修改 | 说明 |
| ------ | ------ | ---- | -------- | ------ | ---------------- | ---- |
| `arbHeartBeatIntervalMs` | 2000 | ms | 100 ~ 172800000 | 全局 | 支持，立即生效 | dnode 向 Arbitrator（mnode）发送仲裁心跳的周期，以及 Arbitrator 检查心跳的周期。自 `v3.3.6.23` 引入 |
| `arbHeartBeatIntervalSec` | 2 | s | 1 ~ 172800 | 全局 | 支持，立即生效 | 同上，秒版本 |
| `arbCheckSyncIntervalMs` | 3000 | ms | 100 ~ 172800000 | 全局 | 支持，立即生效 | Arbitrator 检查 vgroup 内各副本数据同步状态的周期。自 `v3.3.6.23` 引入 |
| `arbCheckSyncIntervalSec` | 3 | s | 1 ~ 172800 | 全局 | 支持，立即生效 | 同上，秒版本 |
| `arbSetAssignedTimeoutMs` | 14000 | ms | 100 ~ 172800000 | 全局 | 支持，立即生效 | Arbitrator 判定成员心跳超时的阈值，超过该时间未收到某成员心跳则认为该成员不可用，可将其它成员指定为 assigned leader。自 `v3.3.6.23` 引入 |
| `arbSetAssignedTimeoutSec` | 14 | s | 1 ~ 172800 | 全局 | 支持，立即生效 | 同上，秒版本 |
| `syncAssignedCheckAppliedGap` | 20 | 条 | 0 ~ 10000 | 全局 | 支持，立即生效 | 双副本下 assigned leader 主动 step down 前，检查对端 applied index 与 commit index 差距的阈值，仅当差距不超过该值时才 step down，避免对端进入 restoring 状态。0 表示不检查（立即 step down）。自 `v3.4.1.0` 引入 |

### 统一超时参数 syncTimeout

`syncTimeout` 是一个便于统一调整心跳与选举超时的快捷参数，默认值为 0，表示不启用。修改 `syncTimeout` 后，系统会按下列公式自动派生并设置其它参数，**无需再逐个修改**：

| 派生参数 | 计算公式 |
| -------- | -------- |
| `arbSetAssignedTimeoutMs` | `syncTimeout` |
| `arbHeartBeatIntervalMs` | `syncTimeout / 4` |
| `arbCheckSyncIntervalMs` | `syncTimeout / 4` |
| `syncVnodeElectIntervalMs` | `(syncTimeout - syncTimeout / 4) / 2` |
| `syncMnodeElectIntervalMs` | `(syncTimeout - syncTimeout / 4) / 2` |
| `statusTimeoutMs` | `(syncTimeout - syncTimeout / 4) / 2` |
| `statusSRTimeoutMs` | `(syncTimeout - syncTimeout / 4) / 4` |
| `syncVnodeHeartbeatIntervalMs` | `(syncTimeout - syncTimeout / 4) / 8` |
| `syncMnodeHeartbeatIntervalMs` | `(syncTimeout - syncTimeout / 4) / 8` |
| `statusIntervalMs` | `(syncTimeout - syncTimeout / 4) / 8` |

例如设置 `syncTimeout` 为 20000（20 秒），则派生结果为：`arbSetAssignedTimeoutMs = 20000`、`arbHeartBeatIntervalMs = arbCheckSyncIntervalMs = 5000`、`syncVnodeElectIntervalMs = syncMnodeElectIntervalMs = statusTimeoutMs = 7500`、`statusSRTimeoutMs = 3750`、`syncVnodeHeartbeatIntervalMs = syncMnodeHeartbeatIntervalMs = statusIntervalMs = 1875`。

> 注意：`syncTimeout` 属于全局参数，必须使用 `ALTER ALL DNODES` 修改；修改后会自动下发并覆盖上表中各参数的当前值。如果之后又单独修改了上表中的某个参数，再次修改 `syncTimeout` 时该参数会被重新覆盖。

### 调试辅助参数

出现问题时，可临时开启以下参数获取更详细的日志。问题定位完成后请恢复默认值，避免产生大量日志影响性能。

| 参数名 | 默认值 | 取值范围 | 作用域 | 说明 |
| ------ | ------ | -------- | ------ | ---- |
| `sDebugFlag` | 131/135 | 131 / 135 / 143 | 全局 | sync（同步/选举）模块日志级别，131 输出 error 与 warning，135 增加 debug，143 增加 trace |
| `mDebugFlag` | 131/135 | 131 / 135 / 143 | 全局 | mnode 模块日志级别，分析 mnode 切主与 dnode 状态判定时使用 |
| `vDebugFlag` | 131/135 | 131 / 135 / 143 | 全局 | vnode 模块日志级别 |
| `dDebugFlag` | 131/135 | 131 / 135 / 143 | 全局 | dnode 模块日志级别，分析 dnode 状态上报时使用 |
| `debugFlag` | 131/135 | 131 / 135 / 143 | 全局 | 全局日志级别开关 |
| `syncLogHeartbeat` | false | true / false | 局部 | 是否将心跳相关日志从 trace 级别提升到 info 级别输出。开启后可以看到每一次心跳的收发，用于分析心跳延迟与丢包 |
| `syncRoutineReportInterval` | 300 | 5 ~ 600（秒） | 局部 | sync 节点例行状态日志（`timer routines`）的输出间隔，日志中包含 term、commit index、match index、选举次数等完整状态，是分析切主的关键信息 |

## 切主问题的参数调整

### 排查思路

1. **先确定切主的对象**：是 mnode 切主、某个/某些 vgroup 切主，还是 dnode 被判定为 offline 后引发的批量切主。
2. **再确定触发原因**：网络抖动或丢包、节点负载过高（CPU、内存、磁盘 IO）、磁盘或 WAL 写满、进程被 OOM 或重启、系统时间跳变等。
3. **最后再调整参数**：参数调整只能缓解"心跳/选举对抖动过于敏感"的问题，如果根因是网络或硬件故障，调整参数只能延长故障切换时间，无法消除问题。

### 调整建议

| 现象 | 优先调整的参数 | 调整方向 |
| ---- | -------------- | -------- |
| vgroup 频繁切主，日志中有大量心跳 `slow` 记录 | `syncVnodeElectIntervalMs`、`syncVnodeHeartbeatIntervalMs` | 同时放大，保持"选举基线 ≥ 心跳间隔 × 4"，例如心跳 2000 ms、选举基线 8000 ms |
| mnode 频繁切主 | `syncMnodeElectIntervalMs`、`syncMnodeHeartbeatIntervalMs` | 同时放大，例如心跳 1000 ms、选举基线 6000 ms |
| 网络质量较差、跨机房部署，需要整体放宽超时 | `syncTimeout` | 一次性派生所有心跳与选举参数，例如设置为 20000 ~ 30000 |
| dnode 被误判为 offline，进而引发 vgroup 批量切主 | `statusInterval`、`statusTimeoutMs`、`statusSRTimeoutMs` | 放大 `statusTimeoutMs`（例如 10000 ~ 15000），保证 `statusTimeoutMs` 显著大于 `statusIntervalMs` 的数倍 |
| 双副本下频繁发生 assigned leader 切换 | `arbHeartBeatIntervalMs`、`arbSetAssignedTimeoutMs`、`arbCheckSyncIntervalMs` | 放大心跳周期与超时阈值，保持 `arbSetAssignedTimeoutMs` 为 `arbHeartBeatIntervalMs` 的 5 倍以上 |
| 副本落后过多，快照复制期间心跳超时 | `snapshotRateLimit`、`syncSnapReplMaxWaitN` | 限制快照复制带宽，避免快照复制压满磁盘 IO 导致心跳超时 |
| 双副本下 assigned leader step down 后对端进入 restoring | `syncAssignedCheckAppliedGap` | 适当放大该阈值，等待对端 apply 追上后再 step down；设置为 0 表示不检查 |

### 调整步骤与示例

1. 修改前先记录当前值，便于对比与回退：

   ```sql
   SHOW CLUSTER VARIABLES LIKE '%elect%';
   SHOW CLUSTER VARIABLES LIKE '%heartbeat%';
   SHOW CLUSTER VARIABLES LIKE '%status%';
   ```

2. 使用 `ALTER ALL DNODES` 修改全局参数（心跳与选举参数均为全局参数，必须对所有 dnode 同时修改，否则各节点超时不一致会加剧切主）：

   ```sql
   -- 单独调整 vnode 的心跳与选举参数
   ALTER ALL DNODES 'syncVnodeHeartbeatIntervalMs' '2000';
   ALTER ALL DNODES 'syncVnodeElectIntervalMs' '8000';

   -- 单独调整 mnode 的心跳与选举参数
   ALTER ALL DNODES 'syncMnodeHeartbeatIntervalMs' '1000';
   ALTER ALL DNODES 'syncMnodeElectIntervalMs' '6000';

   -- 调整 dnode 状态上报超时
   ALTER ALL DNODES 'statusTimeoutMs' '10000';
   ```

3. 或者使用 `syncTimeout` 一次性派生全部相关参数：

   ```sql
   ALTER ALL DNODES 'syncTimeout' '20000';
   ```

4. 修改后确认各 dnode 上的取值一致：

   ```sql
   SHOW DNODE 1 VARIABLES LIKE '%elect%';
   SHOW DNODE 2 VARIABLES LIKE '%elect%';
   ```

5. 观察一段时间（至少一个完整业务周期）内是否仍有切主，若仍存在则需继续收集信息并排查网络、磁盘 IO 等根因。

### 注意事项

- 放大选举与心跳参数会**延长故障切换时间**。选举基线为 8000 ms 时，理论故障切换时间在 8 ~ 16 秒量级，需要业务侧能够接受。
- 不建议将心跳间隔或选举超时调得过小，否则轻微抖动即会触发选举，反而导致集群不稳定。
- 不建议同时修改过多参数，建议每次只调整一组（vnode、mnode、status 或 arb），观察效果后再决定是否继续调整。
- 全局参数必须使用 `ALTER ALL DNODES`；使用 `ALTER DNODE` 修改全局参数会被拒绝或造成各节点配置不一致。

## 需要收集的信息

出现切主问题且需要提交分析时，请按下面清单收集信息。日志与状态信息需要覆盖**切主发生前后各 5 ~ 10 分钟**。

### 1. 集群与配置状态

```sql
-- 集群与各节点基本信息
SHOW DNODES;
SHOW MNODES;
SHOW VGROUPS;

-- 心跳与选举相关参数的当前实际值
SHOW CLUSTER VARIABLES LIKE '%elect%';
SHOW CLUSTER VARIABLES LIKE '%heartbeat%';
SHOW CLUSTER VARIABLES LIKE '%status%';
SHOW CLUSTER VARIABLES LIKE '%arb%';
SHOW CLUSTER VARIABLES LIKE '%sync%';
```

### 2. 角色与选举时间

```sql
-- vnode 的角色与最近选举时间，重点关注 role_time 是否频繁变化
SELECT * FROM information_schema.ins_vnodes;

-- mnode 的角色与成为当前角色的时间
SELECT * FROM information_schema.ins_mnodes;

-- 双副本部署时收集仲裁组状态
SHOW ARBGROUPS;
SELECT * FROM information_schema.ins_arbgroups;
```

请记录发生切主的 `vgroup_id`、`dnode_id`、`role_time` 以及切换前后的 role。

### 3. taosd 日志

在所有 dnode 上收集 `/var/log/taos/` 目录下的 `taosdlog.*` 文件（默认路径，若修改过 `logDir` 请以实际路径为准）。建议先临时提升 sync 模块日志级别并开启心跳日志，复现问题后再恢复：

```sql
-- 开启 sync 模块 debug/trace 日志
ALTER ALL DNODES 'sDebugFlag' '143';

-- 将心跳日志提升为 info 级别输出（局部参数，可只针对出问题的 dnode 开启）
ALTER DNODE 1 'syncLogHeartbeat' 'true';
ALTER DNODE 2 'syncLogHeartbeat' 'true';

-- 缩小 sync 状态例行日志的输出间隔，便于定位切主时刻的完整状态
ALTER DNODE 1 'syncRoutineReportInterval' '30';

-- 问题复现后恢复默认值
ALTER ALL DNODES 'sDebugFlag' '131';
ALTER DNODE 1 'syncLogHeartbeat' 'false';
ALTER DNODE 1 'syncRoutineReportInterval' '300';
```

日志中需要重点关注的关键字：

| 关键字 | 含义 |
| ------ | ---- |
| `reset elect timer` | 选举定时器重置，可看到每次选举超时的 min/max 与实际取值 |
| `become leader` / `become follower` | 角色切换，切主的直接证据 |
| `sync-heartbeat` / `sync-heartbeat-reply` | 心跳收发，配合 `syncLogHeartbeat` 可以看到每一次心跳 |
| `slow(` | 心跳或心跳响应耗时超过 1500 ms 时会打印 slow 日志，是网络或负载问题的重要线索 |
| `timer routines` | sync 节点例行状态报告，包含 term、commit index、match index、选举次数等，是分析切主的核心信息 |
| `heartbeat timeout` | leader 判断心跳响应超时，该 leader 会拒绝写入 |
| `arb-hb` | 双副本下 Arbitrator 心跳的收发 |
| `offline` / `status msg timeout` | dnode 被 mnode 判定为离线 |

### 4. 操作系统与网络信息

在每个 dnode 所在服务器上收集：

- 节点间网络延迟与丢包：节点之间互相 `ping`，以及到 mnode 所在节点的延迟统计，建议持续采样 10 分钟以上
- 网络与磁盘负载：`sar -n DEV 1`、`iostat -x 1`、`top`/`vmstat 1` 的持续采样输出
- 系统日志：`dmesg` 或 `/var/log/messages`，重点检查是否存在 OOM、磁盘错误、网卡异常
- 系统时间同步状态：`chronyc sources` 或 `ntpq -p`，以及各节点之间的时间偏差。节点间时钟跳变会导致心跳超时与状态上报异常
- 磁盘剩余空间：检查数据目录与 WAL 目录所在磁盘是否已写满

### 5. 其它信息

- 版本信息：`SELECT server_version();` 或 `taosd -V`
- 部署拓扑：副本数（双副本 / 三副本）、dnode 数量、是否跨机房部署
- 问题发生的时间点（精确到秒，并注明时区）、发生的频率、影响范围（单个 vgroup 还是全部）
- 问题发生时业务侧的表现与错误码，例如 `Sync timeout`、`Sync leader is unreachable`、`Sync leader is restoring`
- 若进程发生过崩溃，同时收集 core 文件与崩溃时间点的 `taosdlog`
