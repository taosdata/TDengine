---
title: PI 连接器高可用与故障转移
sidebar_label: 高可用与故障转移
toc_max_heading_level: 4
---

本页说明在部署多套 taosX、并由引擎侧以多个 Xnode 管理时，PI 连接器任务的调度方式、故障转移行为，以及迁移窗口内的数据完整性表现，便于在生产环境评估冗余部署方案。

## taosX 与 Xnode

二者对应同一类数据接入执行能力，称呼随场景不同：

| 场景 | 名称 | 说明 |
| --- | --- | --- |
| 外部部署与运维 | taosX | 独立安装、以服务或进程运行的数据接入组件；监听 gRPC 等端口，供 Agent 与任务执行使用 |
| 引擎内集群管理 | Xnode | 在 TDengine 中登记与管理的数据接入执行节点；通过 SQL（如 `CREATE XNODE`、`SHOW XNODES`、`DRAIN XNODE`）纳入调度 |

创建 Xnode 时，`url` 指向对应 taosX 实例的 gRPC 地址（默认端口 `6055`）。守护进程 `xnoded` 负责该执行节点与 `taosd` 的连接与调度协同。概念与 SQL 详见 [数据接入（Xnode）](../../../05-tdengine-sql/08-cluster-management/02-xnode.md)；组件部署详见 [taosX 参考手册](../../../12-operations-and-tooling/03-components/06-taosx.md)。

下文在谈「部署几套服务、打补丁、配置 Agent 地址」时用 **taosX**；在谈「集群内登记、调度、排空、节点状态」时用 **Xnode**。

## 概览

在登记了多个 Xnode 的集群中，PI 连接器任务可以在某一 taosX 实例故障或计划维护后，迁移到其他 Xnode 对应的 taosX 上继续运行。调度模型是「同一时刻一个任务实例运行在一个 Xnode 上」，高可用主要靠故障后的重新调度完成。

迁移或宕机期间会产生短暂的采集中断。实时任务可通过 [重启补偿时间](./05-realtime-guide.md#重启补偿时间) 回填最近一段时间的数据；超出该窗口，或尚未刷盘的在途数据，需要结合 [历史数据回填](./04-backfill-guide.md) 等手段按业务要求处理。

| 能力 | 说明 |
| --- | --- |
| 单套 taosX / 单个 Xnode 不可用时，任务在其他节点上继续或恢复运行 | 支持，见下文调度与故障转移 |
| 迁移窗口内的 live 值、Snapshot 更新、乱序迟到值 | 依赖重启补偿窗口与后续回填流程 |
| 同一 PI 任务在两个节点上同时运行 | 调度与执行侧有防双跑机制 |
| 滚动升级单套 taosX | 可先 `DRAIN XNODE` 迁走任务再升级 |

:::note
下文行为基于 Xnode 通用调度机制与 PI 连接器现有实现说明。PI 数据接入任务的专项高可用测试仍在完善中。
:::

## 调度模型

PI 任务按单个 Job 调度到「当前最优」的一个 Xnode（对应 Xnode 在线、已挂接所需 Agent、空闲内存更充足等），同一时刻在一个 Xnode 上运行。

- 当前不是 active-standby 双实例并行采集，而是单实例调度。
- 节点故障后，由调度器在可用 Xnode 上重新拉起任务。
- 可在登记了 2 个或 3 个（及更多）Xnode 的集群中运行；节点数量不改变「单实例运行」语义。

## 故障转移行为

当运行中任务所在的 taosX 被停止、打补丁重启或宕机时：

1. Xnode 对 taosX 做心跳检测（约每 5 秒一次）。
2. 节点失败后被标记为 offline；经过约 6 次退避重试后，再平衡逻辑在可用 Xnode 中为相关 Job 选择最优节点并重新拉起。
3. 典型的「检测 + 迁移」延迟约在 10–30 秒量级（视网络与集群负载而定）。

计划性维护可主动迁移任务，避免等故障超时：

```sql
DRAIN XNODE <id>;
```

该命令将指定 Xnode 上的已有任务重新分配到其他 Xnode 执行。语法说明见 [排空节点](../../../05-tdengine-sql/08-cluster-management/02-xnode.md#排空节点)。

### 故障转移前提

请将 taosX-Agent 配置为可连接全部相关 taosX 的 endpoint（即各 Xnode 登记的 gRPC 地址）。若 Agent 只指向单一 taosX，该实例不可用时任务可能无处可迁移。Agent 配置见 [taosX-Agent 配置参考](../../../12-operations-and-tooling/03-components/07-taosx-agent/configuration.md)。

## 故障转移期间的订阅、检查点与状态

故障转移会中断 PI DataPipe 订阅：

- 原 taosX 上的订阅随进程退出而结束。
- 新 Xnode（对应另一套 taosX）上的任务实例会新建 DataPipe，并重新订阅相关点位。

当前实现下：

- PI 实时采集任务暂无随任务迁移的 checkpoint / 同步状态。
- 实时任务的 [重启补偿时间](./05-realtime-guide.md#重启补偿时间)（对应 MaxBackfillRange）会在重启后按配置窗口回填最近一段时间的历史数据，适合覆盖较短中断。
- 使用重启补偿时需了解其边界：
  - 崩溃时尚未刷盘的在途数据可能丢失；
  - 早于补偿窗口的迟到数据不会被该机制覆盖。

## 中断窗口内的数据如何处理

中断窗口内产生或更新的数据是否写入 TDengine，取决于中断时长与重启补偿配置：

- 落在重启补偿窗口内的历史数据，可在任务于新 Xnode 拉起后按配置回填。
- live 值、PI Snapshot 更新，以及乱序/迟到且超出补偿窗口的值，需要在确认中断时间范围后，用 [PI backfill 任务](./04-backfill-guide.md) 按时间段补录，并做数据量与抽检校验。

## 如何避免双节点同时跑同一任务

系统通过调度与执行侧约束降低同一 PI 任务双跑导致重复写入的风险：

- Xnode 运行在 mnode leader 上；worker 只确认一条带 connection-id 的调度连接，新调度器会对旧连接发送断开。
- 数据接入 worker 侧调度拒绝同一 `(task_id, job_id)` 的重复启动。

## 滚动升级与计划维护

可以逐台对 taosX 打补丁或升级，而不必主动停止整个采集业务，推荐流程：

1. 对该 taosX 对应的 Xnode 执行 `DRAIN XNODE <id>`，将任务迁走。
2. 对该 taosX 打补丁 / 升级 / 重启。
3. 对应 Xnode 重新 online 后，再按调度策略承接新任务。

补充说明：

- 计划性迁移同样会带来与故障转移类似的短暂停采窗口，数据完整性表现与上文一致。
- 若被升级节点同时承载 mnode leader，`xnoded` 切换期间可能出现集群范围内任务短暂停止，之后再自动恢复。

## 推荐生产架构

面向多地 PI 汇聚到中心 TDengine、并需要数据接入侧冗余的场景，建议：

| 组件 | 建议 |
| --- | --- |
| TDengine | 3 个 dnode，mnode 三副本 |
| taosX / Xnode | 部署 2–3 套 taosX，并在引擎中登记为对应 Xnode |
| 存储 | 共享存储（满足企业版 HA 部署要求） |
| taosX-Agent | 部署在可访问 PI 的 Windows 主机；配置全部相关 taosX endpoint |
| PI 依赖 | 已授权的 PI AF SDK（及相应 Windows 服务账户权限） |

网络与 Agent 代理拓扑选型仍参见 [部署架构](./02-deployment-architecture.md)。库侧副本高可用参见 [高可用](../../../12-operations-and-tooling/02-operations/11-ha/index.md)（与 Xnode 任务调度不是同一层能力）。

## 版本、组件与许可

使用上述能力时，通常需要：

- TDengine TSDB Enterprise v3.4.0.0 及以上
- 企业版许可
- 已部署并授权的 PI AF SDK（随 taosX / taosX-Agent 所在 Windows 环境）
- 满足部署要求的共享存储
- 已创建对应 Xnode，且 taosX-Agent 指向集群内全部相关 taosX endpoint

## 相关文档

- [部署架构](./02-deployment-architecture.md)
- [实时数据同步](./05-realtime-guide.md)（重启补偿时间）
- [历史数据回填](./04-backfill-guide.md)
- [数据接入（Xnode）](../../../05-tdengine-sql/08-cluster-management/02-xnode.md)
- [taosX 参考手册](../../../12-operations-and-tooling/03-components/06-taosx.md)
- [taosX-Agent 配置参考](../../../12-operations-and-tooling/03-components/07-taosx-agent/configuration.md)
