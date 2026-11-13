---
title: 双活方案
sidebar_label: 双活方案
toc_max_heading_level: 4
---

部分用户受部署环境限制，只能部署两台服务器，同时希望提高服务可用性和数据可靠性。TDengine 双活方案通过数据复制和客户端 Failover 满足这类需求，也可用于两套 TDengine 集群之间的灾备，集群规模不受两台服务器的限制。本文介绍双活系统的架构、配置和运维。

双活是 TDengine TSDB Enterprise 特有功能，首次发布于 v3.3.0.0。从 v3.4.3.11 起，双活任务改为通过 SQL 管理，v3.3.x 使用的 `taosx replica` 命令行也已删除。建议使用最新版本。

双活系统由两台服务器组成，每台分别部署一套服务。对业务层而言，两台服务器及其服务构成一个完整的系统，无需感知内部细节。两个节点通常称为 Master-Slave，即“主从”或“主备”，本文中可能混用这些称呼。

TDengine 双活系统的部署架构如下，主要依靠三个机制：

1. Client Driver 负责 Failover，在主节点宕机时切换到从节点
2. taosX 负责将数据从当前主节点复制到从节点，双活任务由 xnoded 统一调度
3. 数据订阅的写接口在写入复制的数据时，向 WAL 添加特殊标记；读接口自动过滤带有该标记的数据，避免循环复制

注：下图以单机版 TDengine 为例，实际部署时，每个 Host 均可替换为任意节点数量的 TDengine 集群。

![双活部署架构](../../../assets/dual-01.webp)

## 集群配置

部署双活需要满足以下前提条件：

- 版本：执行 `CREATE XNODE TASK` 的管理集群须升级到 v3.4.3.11 或更高版本；源端和目标端均不低于 v3.4.0.0，两端版本可以不同；taosX 与 taosd 使用相同版本。
- 数据库：源库和目标库均已预先创建且精度相同，源库的 `wal_retention_period` 大于 0。双活任务按库创建，不支持自动同步新增数据库。
- 授权：源端和目标端均具有 `active_active` 授权（云服务实例除外），目标端另须具有 `td3.0` 授权。
- 部署：管理集群已部署 taosX 并注册 xnode，且 xnode 能连通两端集群。注册方法见 [数据接入（Xnode）](../../../05-tdengine-sql/08-cluster-management/02-xnode.md)。建议由源集群兼任管理集群，并使用超级用户连接源端，以便管理订阅资源和 WAL 保留标记。

WAL 保留时长影响双活系统的容错能力。如果备节点的宕机时长超过主节点的 WAL 保留时长，备节点必定会缺失数据。即使未超过，也可能丢失数据，具体取决于宕机时长与 WAL 保留时长的接近程度，以及数据同步速度。详见 [异常情况](#异常情况)。

## 客户端配置

目前只有 Java 连接器在 WebSocket 连接模式下支持双活，配置示例如下：

```java
url = "jdbc:TAOS-WS://" + host + ":6041/?user=root&password=taosdata&varcharAsString=true";
Properties properties = new Properties();
properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, "192.168.1.11");
properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, "6041");
properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
connection = DriverManager.getConnection(url, properties);
```

各配置属性的含义如下：

| 属性名                              | 含义 |
| ---------------------------------- | --- |
| PROPERTY_KEY_SLAVE_CLUSTER_HOST    | 第二节点的主机名或者 ip，默认空 |
| PROPERTY_KEY_SLAVE_CLUSTER_PORT    | 第二节点的端口号，默认空  |
| PROPERTY_KEY_ENABLE_AUTO_RECONNECT | 是否启用自动重连。仅在使用 WebSocket 连接时生效。true: 启用，false: 不启用。默认为 false。双活场景下请设置为 true |
| PROPERTY_KEY_RECONNECT_INTERVAL_MS | 重连的时间间隔，单位毫秒：默认 2000 毫秒，也就是 2 秒；最小值为 0，表示立即重试；最大值不做限制 |
| PROPERTY_KEY_RECONNECT_RETRY_COUNT | 每节点最多重试次数：默认值为 3；最小值为 0，表示不进行重试；最大值不做限制 |

## 约束条件

1. 应用程序不能使用订阅接口；配置双活参数后，创建消费者会失败
2. 不建议应用程序通过参数绑定写入或查询数据；如需使用，应用必须自行处理连接切换后相关对象失效的问题
3. 双活场景下，不建议应用程序显式执行 `USE` 语句，应在连接参数中指定数据库
4. 使用同一客户端连接在两端切换时，该客户端使用的数据库命名、配置以及用户名、密码和权限需要在两端保持一致；双活任务本身可以将源库映射到不同名称的目标库
5. 上述客户端切换只支持 WebSocket 连接模式，双活任务支持 Native 和 WebSocket 两种连接方式
6. 一个双活任务只复制一个数据库，不支持自动同步新增的数据库；每个任务独占一个 topic，集群的 topic 数量受 `tmqMaxTopicNum` 限制

## 运维管理

### 任务管理

双活任务通过 SQL 创建、查看、启动、停止、修改和删除。完整语法、参数规则和删除行为见 [数据接入（Xnode）](../../../05-tdengine-sql/08-cluster-management/02-xnode.md#双活任务)。

使用 `CREATE XNODE TASK ... WITH TYPE 'replica'` 创建任务。一个任务将源集群的一个库单向复制到目标集群的一个库；如需双向复制，则在两端分别创建一个任务。以下示例在 td1 和 td2 之间双向复制数据库 `power`：

```sql
-- 在 td1 上执行：td1 -> td2
CREATE XNODE TASK 'aa_power_td1_td2'
  FROM 'tmq+ws://root:taosdata@td1:6041/power'
  TO 'taos+ws://root:taosdata@td2:6041/power'
  WITH TYPE 'replica';
START XNODE TASK 'aa_power_td1_td2';

-- 在 td2 上执行：td2 -> td1
CREATE XNODE TASK 'aa_power_td2_td1'
  FROM 'tmq+ws://root:taosdata@td2:6041/power'
  TO 'taos+ws://root:taosdata@td1:6041/power'
  WITH TYPE 'replica';
START XNODE TASK 'aa_power_td2_td1';
```

任务创建后处于停止状态，需执行 `START XNODE TASK` 启动。创建时，可在 `FROM` 中通过 `auto.offset.reset`、`experimental.snapshot.enable`、`msg.consume.start.ts` 等订阅起点参数控制复制的起始位置。这些参数创建后不能修改，如需调整，须删除任务后重建。重建的任务不继承旧任务进度。

与普通任务不同，双活任务采用异步删除。`DROP XNODE TASK` 返回成功只表示删除请求已受理，任务先进入 `deleting` 状态，待源端的 topic、消费组及位点清理完成后才从 `SHOW XNODE TASKS` 中消失。清理遇到权限或凭据等永久错误时，任务进入 `delete_failed` 状态，修复后需重新执行 `DROP XNODE TASK`。清理完成前不能重建相同映射的任务。

### 查看同步进度

先在管理集群查询集群 ID，再到源集群按系统生成的 topic 和消费组名查询各 vgroup 的消费位置：

```sql
-- 在管理集群执行（ins_cluster 为特权表，需要 SYSINFO 权限）
SELECT id FROM information_schema.ins_cluster;

-- 在源集群执行，将 <管理集群 ID> 和 <任务 ID> 替换为实际值
SELECT topic_name, vgroup_id, consumer_id, `offset`, `rows`
  FROM information_schema.ins_subscriptions
  WHERE topic_name = '__replica__<管理集群 ID>_<任务 ID>'
    AND consumer_group = '__replica__<管理集群 ID>_<任务 ID>';
```

- 快照复制阶段，`offset` 以 `tsdb:` 开头；进入 WAL 复制阶段后，显示为 `wal:<已消费版本>/<最新版本>`。两个版本之差是该 vgroup 的 WAL 版本差，可近似反映积压量，与 v3.3 版本 `taosx replica diff` 的含义相同。该位置由消费者心跳上报，可能超前于已提交位点。
- `rows` 是当前消费者进程累计消费的行数，任务重启后重新计数，不代表目标端成功写入的行数。
- `consumer_id` 为空表示该 vgroup 当前没有消费者，例如任务已停止。

## 异常情况

双活任务停止或目标端发生故障后，如果从停止同步到恢复并追平数据的总耗时超过源库的 WAL 保留时长（`wal_retention_period`），对应的 WAL 可能已被清理，导致目标端缺少尚未同步的数据。现有双活任务无法自动补齐这部分数据，需要人工核对两端数据，确定缺失的表和时间范围。消费位点追平并不代表两端数据一致。

如果源端仍保留相应的历史数据，可删除该复制方向的双活任务，等待清理完成后重新创建并启动。重建时，应启用历史数据快照（`auto.offset.reset=earliest`、`experimental.snapshot.enable=true`），并通过 `msg.consume.start.ts` 设置同步开始时间。该时间不能晚于最早缺失数据的时间戳，以便重新复制缺失数据。订阅起点参数只能在创建任务时设置，具体规则见 [数据接入（Xnode）](../../../05-tdengine-sql/08-cluster-management/02-xnode.md#双活任务)。

## 升级与迁移

### 版本升级与降级

- 滚动升级期间不要创建双活任务。所有 taosd 升级完成前，旧版本 mnode 可能忽略 `TYPE` 选项且不报错，将其创建为普通任务，导致任务无法防止循环复制。创建后，应通过 `SHOW XNODE TASKS` 确认 `type` 列为 `replica`。taosX 应与 taosd 同时升级。
- 降级到 v3.4.3.11 之前的版本前，必须删除所有双活任务：旧版本 mnode 不识别任务类型，可能将双活任务作为普通任务运行，并永久丢失类型标记。

### 从 v3.3 迁移

v3.3 的双活任务不会自动迁移，升级后必须重建，并通过 `msg.consume.start.ts` 指定新的同步开始时间。新任务不复用旧任务的 topic、消费组和已提交位点。从 v3.4.3.11 起，`taosx replica` 命令行已删除，因此必须在升级前停止旧任务。每个复制方向均需执行以下步骤：

1. 升级前，根据旧任务已确认的同步进度确定开始时间，记录为 Unix 毫秒时间戳。该时间不能晚于最早可能尚未同步的数据的时间戳；无法确认进度时，可以选择 `0`，重放所有仍可读取的数据。不要直接使用升级完成时间。
2. 调大两端源库的 `wal_retention_period`，使其覆盖停机、升级和重新追平的总时长。
3. 在旧版本上，对每个方向执行 `taosx replica stop <id>` 并等待任务停止。记录旧任务的库映射、连接地址、凭据和调优参数，然后停止旧 taosX 服务，避免其自动为新数据库创建任务。
4. 按发布说明完成升级，确认两端满足版本、数据库和授权要求，部署并注册 xnode。
5. 为每个映射创建并启动新任务，在 `FROM` 中通过 `msg.consume.start.ts` 指定第 1 步确定的开始时间。
6. 通过 `SHOW XNODE TASKS` 确认新任务的 `type` 为 `replica`，再按 [运维管理](#运维管理) 中的方法查询进度，确认订阅标识为 `__replica__<管理集群 ID>_<任务 ID>`。所有方向追平后，将 WAL 保留时长恢复原值。确认旧任务全部停止、旧 topic 不再被其他业务使用后，再按 v3.3 的运维方式另行清理旧资源。

第 5 步的创建语句示例如下：

```sql
CREATE XNODE TASK 'aa_power_td1_td2'
  FROM 'tmq+ws://root:taosdata@td1:6041/power?msg.consume.start.ts=1700000000000'
  TO 'taos+ws://root:taosdata@td2:6041/power'
  WITH TYPE 'replica';
START XNODE TASK 'aa_power_td1_td2';
```

开始时间之后，旧任务已复制的数据可能再次复制。重复写入相同时间戳的数据时会覆盖原有数据，元数据操作也会幂等重放，可避免产生重复行。v3.3 导出的双活任务不能通过 Explorer 的普通任务导入路径恢复，须按上述步骤使用 SQL 重建。
