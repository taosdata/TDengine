---
title: 双活方案
sidebar_label: 双活方案
toc_max_heading_level: 5
---

部分用户受部署环境限制，只能部署两台服务器，同时希望提高服务可用性和数据可靠性。TDengine 双活方案通过数据复制和客户端 Failover 满足这类需求，也可用于两套 TDengine 集群之间的灾备，集群规模不受两台服务器的限制。本文介绍双活系统的架构、配置和运维。

双活是 TDengine TSDB Enterprise 特有功能，首次发布于 v3.3.0.0。从 v3.4.3.12 起，双活任务改为通过 SQL 管理，v3.3.x 使用的 `taosx replica` 命令行也已删除。建议使用最新版本。

双活系统由两台服务器组成，每台分别部署一套服务。对业务层而言，两台服务器及其服务构成一个完整的系统，无需感知内部细节。两个节点通常称为 Master-Slave，即“主从”或“主备”，本文中可能混用这些称呼。

TDengine 双活系统的部署架构如下，主要依靠三个机制：

1. Client Driver 负责 Failover，在主节点宕机时切换到从节点
2. taosX 负责将数据从当前主节点复制到从节点，双活任务由 xnoded 统一调度
3. 数据订阅的写接口在写入复制的数据时，向 WAL 添加特殊标记；读接口自动过滤带有该标记的数据，避免循环复制

注：下图以单机版 TDengine 为例，实际部署时，每个 Host 均可替换为任意节点数量的 TDengine 集群。

![双活部署架构](../../../assets/dual-01.webp)

## 集群配置

部署双活需要满足以下前提条件：

- 版本：执行 `CREATE XNODE TASK` 的管理集群须升级到 v3.4.3.12 或更高版本；源端和目标端均不低于 v3.4.0.0，两端版本可以不同；taosX 与 taosd 使用相同版本。
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
SELECT id
FROM information_schema.ins_cluster;

-- 在源集群执行，将 <管理集群 ID> 和 <任务 ID> 替换为实际值
SELECT
  topic_name,
  vgroup_id,
  consumer_id,
  `offset`,
  `rows`
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
- 降级到 v3.4.3.12 之前的版本前，必须删除所有双活任务：旧版本 mnode 不识别任务类型，可能将双活任务作为普通任务运行，并永久丢失类型标记。

### 从 v3.3 迁移

v3.3 的双活任务不会随软件升级自动迁移，升级后需要用 SQL 重建。新任务不复用旧任务的 topic、消费组或已提交位点。

可以 [手动迁移](#手动迁移)，逐个记录旧配置并重建任务，也可以通过 [脚本迁移](#脚本迁移) 转换配置，按需停止、删除旧任务，并创建、启动新任务。两种方式都需先完成下述准备，并在迁移后按相同要求检查结果。v3.3 导出的双活任务不能通过 Explorer 的普通任务导入功能恢复。

#### 迁移前准备

1. 根据每个复制方向已确认的进度，确定同步开始时间，以 Unix 毫秒时间戳表示。该时间不能晚于最早可能尚未同步记录的时间戳；无法确认时，可选择 `0`，重放所有仍可读取的数据。不要直接使用升级完成时间。
2. 记录并调大两端源库的 `wal_retention_period`，使保留时长覆盖停机、升级和重新追平数据所需的总时间。仅从 WAL 消费时，确认开始时间对应的数据仍在保留范围内。
3. 核对每个旧 taosX 实例的任务范围、源库到目标库的映射、连接地址、凭据和调优参数。迁移文件及备份可能包含明文凭据，应限制访问。
4. 按发布说明规划升级，确认升级后满足 [集群配置](#集群配置) 中的版本、数据库、授权和部署要求。使用非零开始时间时，源端所有 vnode 都须达到 v3.4.3.6；如果同时使用 WebSocket 连接，所用的 taosAdapter 也须达到该版本。

从 v3.4.3.12 起，不再提供 `taosx replica` 命令。升级前，必须停止所有旧复制任务及关联的新库发现监控，再停止所有独立部署的旧 taosX 服务，以免旧任务或监控在升级后恢复运行。脚本在线导出和操作旧任务都依赖旧服务，应在停止服务前完成。

#### 手动迁移

先停止所有旧任务并完成软件升级，再按库映射逐个重建任务：

1. 在旧版本上记录所有复制方向的任务配置，分别执行 `taosx replica stop <id>`。等待全部任务停止后，停止旧 taosX 服务。
2. 按发布说明升级所有 taosd、taosX 和所需的 taosAdapter，准备数据库并注册 xnode，确认满足迁移前准备中的要求。
3. 为每个源库到目标库的映射编写 `CREATE XNODE TASK ... WITH TYPE 'replica'` 语句，并在 `FROM` 中通过 `msg.consume.start.ts` 指定已确定的同步开始时间。按 [数据接入（Xnode）](../../../05-tdengine-sql/08-cluster-management/02-xnode.md#双活任务) 中的 DSN 参数规则核对连接信息和调优参数，不要沿用旧 topic、消费组、客户端标识或其他系统保留参数。
4. 在选定的管理集群创建任务，通过 `SHOW XNODE TASKS WHERE type = 'replica';` 核对任务名称、映射和类型。确认配置正确后，执行 `START XNODE TASK` 启动对应任务。
5. 所有复制方向的任务重建完成后，按 [迁移后验证](#迁移后验证) 完成检查和收尾工作。

以下示例重建 td1 到 td2 的 `power` 数据库复制任务。将地址、凭据和开始时间替换为实际值；反向复制需另建任务：

```sql
CREATE XNODE TASK 'aa_power_td1_td2'
  FROM 'tmq+ws://root:taosdata@td1:6041/power?msg.consume.start.ts=1700000000000'
  TO 'taos+ws://root:taosdata@td2:6041/power'
  WITH TYPE 'replica';

SHOW XNODE TASKS WHERE type = 'replica';

START XNODE TASK 'aa_power_td1_td2';
```

#### 脚本迁移

迁移脚本可将 v3.3.6 的双活任务配置转换为新版本的 SQL。默认只生成迁移文件，也可以按需停止、删除旧任务，或在升级后创建、启动新任务。软件升级和迁移后的数据核对仍需手动完成。

展开下方代码，将其保存为 `migrate_active_active.py`，使用 Python 3.8 或更高版本运行，无需安装第三方依赖。执行新任务 SQL 时，脚本会调用本机的 `taos` 客户端。

<details>
<summary>迁移脚本</summary>

```python
{{#include docs/examples/taosx/migrate_active_active.py}}
```

</details>

##### 升级前操作

1. 在线导出旧任务并生成迁移文件。脚本直接读取旧 taosX REST API，默认端口为 `6050`，无需手动导出 JSON。将示例中的开始时间替换为迁移前准备时确定的值：

   ```bash
   python3 migrate_active_active.py \
     --old-taosx-url http://td-a:6050 \
     --start-time '2026-11-06T10:00:00+08:00' \
     --output-dir replica-migration-a
   ```

   如果各任务需要不同的开始时间，可以多次指定 `--task-start-time TASK_ID=TIME`，例如 `--task-start-time 7=1700000000000 --task-start-time 8=1700000001000`。开始时间的继承规则见 [脚本参数说明](#脚本参数说明)。

   也可以使用 `--sqlite /backup/taosx.db` 从 SQLite 备份离线生成文件，无需连接旧服务。应使用实际配置的数据库路径，通过 SQLite 备份工具或在停止服务后制作一致性备份。服务运行时，直接复制单个 `.db` 文件可能遗漏 WAL 中的内容。以下示例与在线导出二选一：

   ```bash
   python3 migrate_active_active.py \
     --sqlite /backup/taosx.db \
     --start-time '2026-11-06T10:00:00+08:00' \
     --output-dir replica-migration-a
   ```

2. 检查迁移目录中的 `tasks.json`、`create.sql` 和 `start.sql`，它们分别保存旧任务快照、创建语句和启动语句。核对任务范围、地址、库映射、凭据及开始时间。不同旧 taosX 实例的任务 ID 可能相同，因此每个实例应使用独立目录。脚本不会覆盖已有目录。

   脚本保留库映射、连接凭据和允许的调优参数，移除旧 DSN 中的系统保留参数，并生成新的任务名称。普通任务和已删除任务不参与迁移。不支持 `auto.offset.reset=latest`、`VIA`、自定义解析器或并行任务配置；遇到重复映射或不支持的参数时会报错，需人工核对旧配置。

   不要直接修改生成的迁移文件。脚本执行 SQL 前会检查 SQL 文件与任务快照是否一致，不一致时停止执行。

3. 在线导出后，使用同一迁移目录执行以下命令，停止该旧实例的全部双活任务及关联的新库发现监控，并等待停止完成：

   ```bash
   python3 migrate_active_active.py \
     --output-dir replica-migration-a --stop-old --stop-timeout 300
   ```

   删除旧任务是可选操作，可根据回滚窗口安排。确认旧任务已停止后，可使用同一目录执行 `--delete-old`，删除快照中的全部旧双活任务及关联的新库发现配置：

   ```bash
   python3 migrate_active_active.py \
     --output-dir replica-migration-a --delete-old
   ```

   脚本无法用离线生成的快照停止或删除旧任务，需按旧版本的运维方式手动完成这些操作。

4. 所有旧实例都完成上述操作后，停止所有独立部署的旧 taosX 服务。随后按发布说明升级 taosd、taosX 和所需的 taosAdapter，准备数据库并注册 xnode，再执行升级后操作。

##### 升级后操作

确认升级后的环境满足 [迁移前准备](#迁移前准备) 中的要求，再分别处理各旧实例生成的迁移目录。

可以使用脚本分步创建并启动新任务。`--dsn` 用于连接新管理集群，与任务 FROM、TO 中的源端、目标端连接配置相互独立：

```bash
python3 migrate_active_active.py \
  --output-dir replica-migration-a \
  --dsn 'ws://root:password@td-a:6041' --execute create

python3 migrate_active_active.py \
  --output-dir replica-migration-a \
  --dsn 'ws://root:password@td-a:6041' --execute start --start-timeout 300
```

`--execute create` 创建任务并核对配置；`--execute start` 先核对配置，再启动并等待任务进入 `running`。也可以使用 `--execute all`，在全部任务创建并核对成功后，再启动任务。脚本会核对任务名称、类型、连接配置和运行状态。

**安全提示**：迁移期间，`--dsn` 参数和脚本传给 `taos` 的 SQL 可能会将管理集群、源端和目标端的凭据暴露在进程参数或 Shell 历史记录中。请仅在受控的运维主机上运行脚本，并在迁移完成后按需轮换或撤销临时凭据。

管理集群开启 `xnodeDsnProtection` 时，脚本不能自动执行 SQL；如果无法确认配置状态，或存在待重启的配置变更，脚本也会拒绝执行。此时应由管理员按集群保护要求手动创建和核对任务，不要为运行脚本关闭保护。

也可以在选定的管理集群手动执行生成的 `create.sql` 和 `start.sql`。先检查 SQL，再执行创建文件，核对每个新任务的 `type='replica'` 和配置，最后执行启动文件，例如：

```bash
taos -h <管理集群主机> -f replica-migration-a/create.sql
```

```sql
SHOW XNODE TASKS WHERE type = 'replica';
```

```bash
taos -h <管理集群主机> -f replica-migration-a/start.sql
```

所有复制方向启动后，按 [迁移后验证](#迁移后验证) 检查同步结果。

##### 失败处理与重试

手动执行 SQL 失败时，先核对已创建或已启动任务的配置和状态，再重试未完成的步骤，避免重复提交整批语句。

旧任务的操作结果和新任务的执行结果分别保存在 `old-operations.json` 和 `new-operations.json` 中。失败后，保留原迁移目录，查看结果、排除原因，再用同一目录重试失败的阶段。

重试 CREATE 时，脚本只跳过同名且配置完全一致的任务；重试 START 时，跳过已运行的任务，并继续等待已排队的任务。`all` 的 CREATE 阶段失败时不会执行 START，已创建或已启动的新任务不会自动回滚。

如果只删除了部分旧任务，也应使用原目录重试。如果任务缺失且原因不明，或出现配置变化、文件校验失败，应保留原快照、SQL 和操作记录，核对任务实际状态后再处理；不要清空记录、替换快照或修改文件绕过检查。

##### 脚本参数说明

每次运行 `migrate_active_active.py` 处理一个旧 taosX 实例或一个新管理集群。不同旧实例应使用独立的输出目录。

| 参数 | 默认值或必填条件 | 适用阶段 | 说明 |
| --- | --- | --- | --- |
| `--output-dir DIR` | 除查看帮助外必填 | 全部 | 迁移文件目录。生成文件时须使用尚不存在的目录；后续分步操作和执行 SQL 时使用原目录 |
| `--old-taosx-url URL` | 生成时为 `http://127.0.0.1:6050`；分步操作继承快照地址 | 在线生成、旧任务操作 | 旧 taosX REST API 地址，支持 HTTP 和 HTTPS。分步操作须沿用快照中保存的地址。不能与 `--sqlite` 或 `--execute` 同用 |
| `--sqlite FILE` | 不指定则在线读取 | 离线生成 | 从一致性 SQLite 备份生成迁移文件，不连接旧服务。不能与 `--old-taosx-url`、旧任务操作或 `--execute` 同用 |
| `--start-time TIME` | 可选 | 生成 | 为所有迁移任务指定同一开始时间，覆盖 FROM DSN 的 `msg.consume.start.ts`。不能与 `--task-start-time` 同用 |
| `--task-start-time TASK_ID=TIME` | 可选，可重复 | 生成 | 按旧任务 ID 指定开始时间，只覆盖列出的任务。重复 ID 或不属于本次迁移范围的 ID 会报错 |
| `--stop-old` | 默认不停止 | 旧任务操作 | 停止快照中的全部旧任务及关联的新库发现监控，并等待停止。目录不存在时先在线生成迁移文件 |
| `--delete-old` | 默认不删除 | 旧任务操作 | 先删除关联的新库发现配置，再删除快照中的全部旧任务。任务必须已停止；此参数不会自动停止任务 |
| `--stop-delete-old` | 默认不停止或删除 | 旧任务操作 | 先停止快照中的全部旧任务，确认全部停止后再删除；停止失败或超时不会进入删除阶段 |
| `--stop-timeout SECONDS` | `300` 秒 | 旧任务操作的停止阶段 | 查询、停止监控和任务、等待停止的总超时时间，须大于 `0`。只能与旧任务操作参数一起提供；单独删除不使用此超时 |
| `--execute MODE` | 默认不执行 SQL | 升级后执行 | `create` 执行 CREATE 并核对任务；`start` 先核对任务配置，再执行 START 并检查运行状态；`all` 先完成全部 CREATE 并核对成功，再执行 START。读取已有迁移目录，必须同时提供 `--dsn` |
| `--start-timeout SECONDS` | `300` 秒 | 新任务启动阶段 | 启动任务并等待全部进入 `running` 的总超时时间，须大于 `0`。仅适用于 `--execute start` 或 `--execute all`，不包括预检和创建阶段 |
| `--dsn DSN` | 使用 `--execute` 时必填 | 升级后执行 | 新管理集群的连接 DSN，例如 `ws://root:password@td-a:6041`，与任务 FROM、TO 的业务连接分别配置 |
| `-h`、`--help` | 无需其他参数 | 帮助 | 显示命令行帮助并退出，不生成文件或操作任务 |

参数组合与时间格式要求如下：

- 不指定旧任务操作或 `--execute` 时，只生成文件。`--stop-old`、`--delete-old`、`--stop-delete-old` 三者互斥，也都不能与 `--execute` 同用。使用已有目录操作旧任务时，不能再指定开始时间参数。
- 两种开始时间参数均支持带 `Z` 或 `±HH:MM` 时区的 RFC 3339 时间，例如 `2026-11-06T10:00:00+08:00`，小数秒最多三位；整数按 Unix 毫秒解释，不能为负数。
- 开始时间按“显式指定 > 旧 FROM DSN 值 > 新任务默认值”的优先级处理。未显式指定开始时间的任务会沿用旧 FROM DSN 的 `msg.consume.start.ts`，旧值须为非负整数形式的 Unix 毫秒时间戳；旧值不存在时，使用新任务默认值 `0`，即不按时间过滤。
- 显式指定 `--start-time 0` 或 `--task-start-time TASK_ID=0` 会覆盖旧值并关闭时间过滤。最终生效的非零开始时间不能与非零 `msg.consume.rawdata` 共用。
- 继承时间过滤条件不代表继承旧 topic、消费组或消费位点，可读取范围仍受快照、数据保留和 WAL 保留情况限制。

#### 迁移后验证

无论选择手动迁移还是脚本迁移，都需要对每个复制方向完成以下检查：

1. 通过 `SHOW XNODE TASKS WHERE type = 'replica';` 核对任务数量、源库到目标库的映射、`type='replica'` 和运行状态，确认每个预期的复制方向都有对应的任务，且任务已经启动。
2. 按 [查看同步进度](#查看同步进度) 中的方法，使用新任务的 `__replica__<管理集群 ID>_<任务 ID>` 标识查询各 vgroup 的进度，确认任务持续复制并逐步追平。
3. 核对同步开始时间边界、表结构和实际数据，确认迁移前后没有遗漏需要复制的数据。生成 SQL 成功、任务显示 `running` 或消费位点追平，都不能单独证明没有丢数；数据已不在可读取的快照或 WAL 中时，需要另行补数。
4. 所有方向均追平且数据验证通过后，将 WAL 保留时长恢复为原值。确认旧任务全部停止、旧 topic 无其他业务使用后，再按 v3.3 的运维方式另行清理旧资源；删除新任务不会清理旧资源。

已同步且位于开始时间之后的数据可能被重放；同时间戳覆盖及元数据幂等重放避免重复行。新任务不自动恢复旧版本的新库发现规则，后续新增数据库时仍需按库创建双活任务。
