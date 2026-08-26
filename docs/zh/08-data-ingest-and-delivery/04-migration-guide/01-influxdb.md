---
title: 从 InfluxDB 迁移到 TDengine
sidebar_label: InfluxDB
description: 将 InfluxDB 1.x 的数据、写入链路和查询应用迁移到 TDengine
toc_max_heading_level: 4
---

本文介绍将 InfluxDB 1.x 的数据和应用迁移到 TDengine 的实施流程。迁移覆盖 TDengine Cloud、TDengine TSDB-OSS 和 TDengine TSDB-Enterprise 三种目标产品形态。

迁移不是单次数据导入。为避免历史数据、迁移期间新增数据和业务查询出现缺口，应依次完成源端盘点、目标端建模、增量保护、历史回灌、数据校验和读写切换。

> 本文中的 InfluxDB Line Protocol 映射适用于 TDengine 的无模式写入。关于协议完整语法、类型推断和限制，请参阅 [无模式写入](../../10-developer-guide/04-schemaless.md)。

## 迁移流程和目标产品形态

推荐采用“先保护增量、再回灌历史、最终追平后切读”的流程：

1. 盘点源端数据模型、写入链路和关键查询，确定迁移边界 `T0`；
2. 创建目标数据库，并用代表性数据验证数据模型；
3. 从 `T0` 开始启用应用双写或持续同步，保护新增数据；
4. 回灌 `T0` 前的历史数据；
5. 按时间窗口校验数据，停止旧写入后完成最终追平；
6. 灰度将读请求改为 TDengine SQL，并在观察期后下线旧链路。

三种目标产品形态的 SQL、数据模型和验收原则相同，历史迁移和实时写入的方式不同。

| 目标产品形态 | 历史数据迁移 | 实时写入 | 适用场景 |
| --- | --- | --- | --- |
| TDengine Cloud | 通过 Cloud InfluxDB 数据源和连接代理读取源端；可设置结束时间或持续同步 | 使用 Cloud InfluxDB Line Protocol 端点和 Cloud Token | 希望使用全托管服务，且连接代理能够访问源 InfluxDB |
| TDengine TSDB-OSS | 从源端导出 Line Protocol，再经自建 `taosAdapter` 导入 | 使用自建 `taosAdapter` 的 InfluxDB v1 写入端点 | 希望使用开源版本，并能够维护目标集群和导出链路 |
| TDengine TSDB-Enterprise | 通过 `taosX` 和 `taosExplorer` 创建 InfluxDB 数据源任务；可持续同步 | 使用自建 `taosAdapter` 的 InfluxDB v1 写入端点 | 需要可视化任务管理、进度恢复或私有化部署 |

## 迁移前评估和目标端准备

### 盘点源端

在创建迁移任务前，记录以下信息，并保存为迁移验收基线：

| 项目 | 需要确认的内容 |
| --- | --- |
| 数据范围 | 数据库、保留策略、最早和最新时间戳、历史数据量、每日增量和迁移截止点 `T0` |
| 数据模型 | measurement、tag、field、字段类型、空值比例和 tag 基数 |
| 时间语义 | 写入精度、时区处理方式、乱序数据范围和迟到数据的最大延迟 |
| 写入链路 | 应用、采集器、批量大小、重试策略、认证方式和是否能够双写 |
| 读取链路 | 关键 InfluxQL 查询、报表、告警、接口以及可接受的切换窗口 |
| 源端能力 | 是否可直接导出 Line Protocol；不能直接导出时，是否需要使用源服务商提供的备份或迁出方式 |

对每个 measurement 选择一个较小时间窗口作为试迁移样本。样本应包含多个 tag、数值和字符串 field、空值以及纳秒时间戳等具有代表性的数据。

### 创建目标数据库

InfluxDB 的时间精度可以混用秒、毫秒、微秒和纳秒。为避免精度丢失，迁移目标数据库应使用纳秒精度。例如：

```sql
CREATE DATABASE migration_db PRECISION 'ns';
```

生产迁移前还应完成以下准备：

- 为迁移程序和应用分别创建最小权限账号或凭据；
- 验证迁移主机、连接代理或 `taosX-Agent` 到源端和目标端的网络连通性；
- 确定双写开始时间 `T0`、历史数据截止时间和最终切换窗口；
- 为导出文件、失败批次、校验结果和迁移日志分配独立存储空间；
- 在非生产数据库完成一次完整的试迁移和查询回归。

## 数据模型和写入语义

通过 InfluxDB Line Protocol 写入时，TDengine 按下表映射数据模型。

| InfluxDB 概念 | TDengine 概念 |
| --- | --- |
| measurement | 超级表名称 |
| tag key/value | 标签列；tag 值自动转换为 `NCHAR` |
| field key/value | 普通列；按类型后缀或引号推断类型 |
| timestamp | 主键时间戳，默认列名为 `_ts` |
| measurement 和 tag set | 子表；默认按排序后的 tag 计算 MD5 生成表名 |

例如，下面的 Line Protocol：

```text
cpu,host=server01,region=cn-beijing usage=42.5,load=2i,status="ok" 1704067200000000000
```

会创建或使用超级表 `cpu`，将 `host`、`region` 写为标签列，将 `usage`、`load`、`status` 写为普通列，并将末尾纳秒时间戳写入 `_ts`。

迁移前应重点确认以下语义：

- 同一 field 在不同数据行中出现冲突类型会导致写入失败，应先统一源端类型或拆分字段；
- 未出现的 field 或 tag 会以 `NULL` 写入；无模式写入可增加列，但不会自动删除既有列；
- 自动生成的子表名不可读。若业务依赖可读表名，应在试迁移中验证子表命名配置；
- 单行最大长度为 64 KB，全部 tag 值总长度最大为 16 KB；超限数据需在源端拆分或转换；
- 超级表和子表名称区分大小写。迁移程序、应用代码和 SQL 中应保持一致的名称；
- 不要在首次无模式写入前手工创建同名但结构不同的超级表。

## 保护增量数据

历史导出或备份只能覆盖其开始前已经存在的数据，不能自动包含迁移期间的新增写入。因此，开始历史回灌前必须保护增量数据。

可采用以下一种方式：

- **应用双写**：从 `T0` 开始，应用同时写入源 InfluxDB 和 TDengine。应用应保留失败重试或持久化队列，以便补发未确认的数据；
- **持续同步**：TDengine Cloud 或 TDengine TSDB-Enterprise 的 InfluxDB 数据源任务不设置结束时间时，可持续读取新增数据。仍应监控任务延迟和错误，并在切换窗口完成最终校验；
- **采集器双输出**：支持多个输出目标的采集器可同时向源端和 TDengine 写入。必须分别监控两个输出的失败率。

无论使用哪种方式，均应记录 `T0`。历史迁移只处理 `T0` 前的数据，增量链路处理 `T0` 及之后的数据，从而使两个范围可独立校验。

## 迁移历史数据

所有路径均应先迁移单个 measurement 的小时间窗口，确认模型、记录数和时间边界后，再扩大范围。

### TDengine Cloud

TDengine Cloud 可以在数据写入页面创建 InfluxDB 数据源，并通过连接代理访问源 InfluxDB。连接代理必须部署在能够访问源端的网络中。

创建任务时：

1. 在 Cloud 实例中创建纳秒精度的目标数据库；
2. 配置连接代理和源端 InfluxDB 1.x 的只读账号；
3. 执行连通性检查，确认代理可以获取源端数据；
4. 选择需要迁移的 measurement，设置起始时间；设置结束时间时执行历史迁移，不设置结束时间时持续同步；
5. 根据源端负载和数据密度设置单次读取的时间范围。范围过大可能增加源端内存压力，范围过小会降低迁移效率；
6. 设置延迟以覆盖可能的乱序或迟到数据，并观察任务延迟和错误。

Cloud 数据源和连接代理的配置请参阅 [InfluxDB 数据源](https://docs.taosdata.com/cloud/data-in/ds/influx/)。

### TDengine TSDB-Enterprise

TDengine TSDB-Enterprise 通过 `taosExplorer` 创建 InfluxDB 数据源任务，由 `taosX` 从源端读取数据并写入目标数据库。

创建任务时：

1. 选择纳秒精度的目标数据库；
2. 配置源 InfluxDB 1.x 的地址、用户和密码，并执行连通性检查；
3. 选择 measurement、起始时间和可选的结束时间；
4. 根据源端性能设置每次读取的时间范围和延迟；
5. 提交任务后观察进度、延迟和错误；暂停、重启或异常恢复后，任务会从已保存的进度继续执行。

界面字段和参数说明请参阅 [InfluxDB 数据源](../01-no-code-ingestion/09-influxdb.md)。源端无法被目标端直接访问时，可部署 `taosX-Agent` 作为连接代理。

### TDengine TSDB-OSS

`taosAdapter` 负责接收写入请求，不会主动读取源 InfluxDB。因此，社区版需要先从源端导出 InfluxDB Line Protocol，再分批写入 TDengine。

源端具有本地 InfluxDB 数据目录时，可使用与源版本匹配的导出工具生成 Line Protocol。导出命令和选项随 InfluxDB 版本而变化，应先在一个小时间范围内验证导出结果。

源端是托管服务且不提供本地数据目录时，应使用服务商提供的迁出方法。例如，[阿里云 TSDB for InfluxDB 迁出方案](https://help.aliyun.com/zh/document_detail/2972630.html)要求先按其文档备份并恢复到自建 InfluxDB 中继，再使用 `influx_inspect export -lponly` 导出 Line Protocol。此类服务商专有前提、端口和资源要求可能变化，应以服务商最新文档为准。

导入时遵循以下原则：

1. 以 measurement 和时间窗口拆分导出文件，文件名中保留时间范围；
2. 先导入小批次，并使用 SQL 验证表结构、时间精度和字段类型；
3. 对成功批次记录 measurement、时间范围、行数和校验结果；
4. 对失败批次保留原始文件，修复数据后仅重试失败批次；
5. 历史回灌期间持续运行增量保护链路。

无模式写入的导入参数和限制请参阅 [taosAdapter](../../12-operations-and-tooling/03-components/03-taosadapter.md) 与 [无模式写入](../../10-developer-guide/04-schemaless.md)。

## 切换实时写入

### TDengine Cloud

TDengine Cloud 的 InfluxDB Line Protocol 写入端点格式如下：

```text
POST <TDENGINE_CLOUD_URL>/influxdb/v1/write?db=<TDENGINE_DATABASE>&token=<TDENGINE_CLOUD_TOKEN>&precision=ns
```

将 Cloud Token 通过密钥管理服务、环境变量或运行时注入提供，不要写入源码、脚本仓库或日志。端点和 Token 的获取方式请参阅 [InfluxDB Line Protocol](https://docs.taosdata.com/cloud/data-in/dca/schemaless-influxdb/)。

以下示例使用环境变量写入一条纳秒精度数据：

```shell
curl --request POST \
  "$TDENGINE_CLOUD_URL/influxdb/v1/write?db=$TDENGINE_DATABASE&token=$TDENGINE_CLOUD_TOKEN&precision=ns" \
  --data-binary 'cpu,host=server01,region=cn-beijing usage=42.5,load=2i,status="ok" 1704067200000000000'
```

### TDengine TSDB-OSS 和 TDengine TSDB-Enterprise

自建 `taosAdapter` 的 InfluxDB v1 写入端点格式如下：

```text
POST http://<TAOS_ADAPTER_HOST>:6041/influxdb/v1/write?db=<TDENGINE_DATABASE>&precision=ns
```

该接口支持 HTTP Basic Auth 和 URL 参数 `u`、`p`。TDengine TSDB-Enterprise 还支持使用 `Authorization: Bearer <token>` 传入由 `CREATE TOKEN` 生成的 TDengine Bearer Token；该 Token 不是 InfluxDB Token。

以下示例向目标数据库写入一条纳秒精度数据：

```shell
curl --request POST \
  "http://<TAOS_ADAPTER_HOST>:6041/influxdb/v1/write?db=<TDENGINE_DATABASE>&precision=ns" \
  --user "<TDENGINE_USER>:<TDENGINE_PASSWORD>" \
  --data-binary 'cpu,host=server01,region=cn-beijing usage=42.5,load=2i,status="ok" 1704067200000000000'
```

切换写入时，先在非生产数据库验证上述数据能够创建正确的超级表、标签列和普通列。然后启用双写，持续观察写入失败率、端到端延迟和字段类型冲突。双写稳定后逐步增加 TDengine 的主写入流量，并在观察期内保留旧写入链路。

## 校验、切换和回滚

在历史迁移、持续同步或双写完成后，按 measurement 和时间窗口执行校验。不要只比对全库总行数，因为总量相同不能证明时间范围和关键字段一致。

| 校验项 | 建议方法 |
| --- | --- |
| 时间边界 | 比对源端和目标端每个窗口的最小、最大时间戳 |
| 数据量 | 比对每个 measurement、tag 范围或业务维度的记录数 |
| 字段内容 | 抽样比对指定时间戳的字段值、tag 值和空值 |
| 聚合结果 | 比对 `COUNT`、`MIN`、`MAX`、`AVG` 等代表性聚合 |
| 写入质量 | 比对双写期间的失败率、重试次数和端到端延迟 |
| 查询行为 | 回归关键报表、告警和 API，确认窗口、填充和最新值语义 |

建议使用左闭右开的时间范围划分校验窗口，例如 `[_start, _end)`，避免相邻窗口重复或遗漏数据。

切换窗口内按以下顺序操作：

1. 暂停旧写入，或冻结新写入进入旧库；
2. 等待双写队列或持续同步任务处理完最后一个增量窗口；
3. 对最后一个窗口执行数据校验；
4. 灰度将读请求切换为 TDengine SQL；
5. 观察关键指标、告警和业务结果；
6. 观察期结束且无未解决差异后，下线旧读写链路。

出现字段类型冲突、时间精度错误、数据延迟超出阈值或关键查询结果不一致时，应停止扩大切换范围，将应用读写切回旧链路。根据应用持久化队列、写入日志或失败批次补发未确认数据，重新校验后再开始下一轮切换。

## 改造 InfluxQL 查询

迁移后，应用读请求需要从 InfluxQL 改写为 TDengine SQL。以下是常见对应关系。

| InfluxQL 查询意图 | TDengine SQL 方向 |
| --- | --- |
| 时间范围过滤 | `WHERE _ts >= ... AND _ts < ...` |
| tag 条件 | `WHERE tag_name = 'value'` |
| `MEAN`、`MAX`、`COUNT` | `AVG`、`MAX`、`COUNT` |
| `GROUP BY time(1m)` | `INTERVAL(1m)` |
| 按 tag 分组 | `PARTITION BY tag_name` 或 `GROUP BY tag_name`，按查询语义选择 |
| `LAST(field)` | `LAST(field)` 或 `LAST_ROW(*)` |
| `fill(null/previous/linear)` | `FILL(NULL/PREV/LINEAR)` |

### 时间范围、标签筛选和窗口聚合

下面的 InfluxQL 查询按主机统计一分钟窗口内的平均 CPU 使用率：

```sql
SELECT MEAN("usage")
FROM "cpu"
WHERE time >= '2024-01-01T00:00:00Z'
  AND time < '2024-01-01T00:02:00Z'
  AND "host" = 'server01'
GROUP BY time(1m), "host"
```

可改写为：

```sql
SELECT _wstart AS window_start, host, AVG(usage) AS avg_usage
FROM migration_db.cpu
WHERE _ts >= '2024-01-01 00:00:00.000000000'
  AND _ts < '2024-01-01 00:02:00.000000000'
  AND host = 'server01'
PARTITION BY host
INTERVAL(1m);
```

时间条件建议使用左闭右开区间，以便相邻迁移和校验窗口之间不产生重复或遗漏。

### 按标签分别聚合

下面的 InfluxQL 查询按 `region` 统计指定时间范围内的 CPU 最大值和样本数：

```sql
SELECT MAX("usage"), COUNT("usage")
FROM "cpu"
WHERE time >= '2024-01-01T00:00:00Z'
  AND time < '2024-01-02T00:00:00Z'
GROUP BY "region"
```

使用 `PARTITION BY` 可以让每个 `region` 独立计算：

```sql
SELECT region, MAX(usage) AS max_usage, COUNT(usage) AS sample_count
FROM migration_db.cpu
WHERE _ts >= '2024-01-01 00:00:00.000000000'
  AND _ts < '2024-01-02 00:00:00.000000000'
PARTITION BY region;
```

当需要逐个子表计算时，使用 `PARTITION BY tbname`。例如，以下查询返回每个子表在一小时内的平均值：

```sql
SELECT tbname, _wstart AS window_start, AVG(usage) AS avg_usage
FROM migration_db.cpu
WHERE _ts >= '2024-01-01 00:00:00.000000000'
  AND _ts < '2024-01-01 01:00:00.000000000'
PARTITION BY tbname
INTERVAL(1h);
```

### 最新值和完整最新行

若需要返回同一条最新记录中的全部字段，可使用 `LAST_ROW(*)`：

```sql
SELECT LAST_ROW(*)
FROM migration_db.cpu
WHERE host = 'server01';
```

若只需要字段的最后一个非空值，可使用 `LAST(expr)` 或 `LAST(*)`。窗口查询中的 `FILL` 选项、`LAST` 与 `LAST_ROW` 的完整语义请参阅 [数据查询](../../05-tdengine-sql/04-data-query/01-query.md) 和 [函数](../../05-tdengine-sql/04-data-query/03-function.md)。

### 最近 N 条原始数据

下面的 InfluxQL 查询获取 `server01` 最近的 10 条 CPU 数据：

```sql
SELECT "usage", "load"
FROM "cpu"
WHERE "host" = 'server01'
ORDER BY time DESC
LIMIT 10
```

可改写为：

```sql
SELECT _ts, usage, load
FROM migration_db.cpu
WHERE host = 'server01'
ORDER BY _ts DESC
LIMIT 10;
```

在 TDengine SQL 中，`LIMIT` 在 `ORDER BY` 之后生效；使用 `PARTITION BY` 时，`LIMIT` 会限制每个分区的输出数量。

### 窗口缺失值填充

当报表需要连续的时间窗口时，可以通过 `FILL` 指定缺失窗口的填充方式。下面的 InfluxQL 查询使用前一个有效值填充缺失窗口：

```sql
SELECT MEAN("usage")
FROM "cpu"
WHERE time >= '2024-01-01T00:00:00Z'
  AND time < '2024-01-01T01:00:00Z'
  AND "host" = 'server01'
GROUP BY time(1m)
fill(previous)
```

可改写为：

```sql
SELECT _wstart AS window_start, AVG(usage) AS avg_usage
FROM migration_db.cpu
WHERE _ts >= '2024-01-01 00:00:00.000000000'
  AND _ts < '2024-01-01 01:00:00.000000000'
  AND host = 'server01'
INTERVAL(1m)
FILL(PREV);
```

在上线前，应按实际业务分别验证 `FILL(NULL)`、`FILL(PREV)` 和 `FILL(LINEAR)` 的结果。它们会改变缺失窗口的展示和下游计算语义，不能仅根据语法名称推断等价性。

## 迁移后的能力扩展

完成基础数据校验和查询回归后，可以逐步引入 TDengine 的流式计算、数据订阅和虚拟表；不建议与基础切换同时上线。

### 流式计算

[流式计算](../../07-stream-processing/index.md) 可持续生成分钟级或小时级聚合表，也可将阈值和事件计算结果提供给告警服务。下面的示例按一分钟窗口计算 CPU 平均使用率，并将结果写入 `cpu_usage_1m`：

```sql
CREATE STREAM cpu_usage_1m_stream
  INTERVAL(1m) SLIDING(1m)
  FROM migration_db.cpu
  INTO cpu_usage_1m
  AS
    SELECT _twstart AS ts,
           AVG(usage) AS avg_usage
    FROM %%trows;
```

### 数据订阅

[数据订阅](../../06-data-subscription/index.md) 可将新写入数据或查询结果以主题形式分发给下游消费者，减少定时轮询。下面的查询主题将 CPU 数据提供给告警或实时分析服务：

```sql
CREATE TOPIC cpu_realtime_topic AS
SELECT tbname, _ts, usage, status
FROM migration_db.cpu;
```

查询主题不支持聚合和时间窗口。需要订阅聚合结果时，应先通过流式计算将结果写入表，再订阅该结果表。

### 虚拟表

[虚拟表](../../05-tdengine-sql/02-ddl/04-virtualtable.md) 可按时间戳组合多个物理表的列，适合为应用提供统一的只读查询入口。下面的示例将同一设备的 CPU 和环境指标组合为一张虚拟表：

```sql
CREATE VTABLE device_overview (
    ts TIMESTAMP,
    cpu_usage DOUBLE FROM migration_db.cpu_server01.usage,
    temperature FLOAT FROM migration_db.env_server01.temperature
);
```

虚拟表不直接存储数据。查询时，来源表中相同时间戳的列会组合为同一行；仅一个来源存在的时间戳会保留，缺失列为 `NULL`。示例中的来源表必须替换为迁移后实际存在的物理表或子表。

这些能力应分别建立功能测试、性能基线和回滚措施，再逐步投入生产使用。
