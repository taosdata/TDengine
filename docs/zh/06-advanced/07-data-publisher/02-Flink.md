---
sidebar_label: Flink
title: Flink
toc_max_heading_level: 4
---

import FlinkCommonInfo from '../../10-third-party/01-collection/flink/_flink-common-info.mdx'

Apache Flink 是一款由 Apache 软件基金会支持的开源分布式流批一体化处理框架，可用于流处理、批处理、复杂事件处理、实时数据仓库构建及为机器学习提供实时数据支持等诸多大数据处理场景。与此同时，Flink 拥有丰富的连接器与各类工具，可对接众多不同类型的数据源实现数据的读取与写入。在数据处理的过程中，Flink 还提供了一系列可靠的容错机制，有力保障任务即便遭遇意外状况，依然能稳定、持续运行。

借助 TDengine 的 Flink 连接器，Apache Flink 得以与 TDengine 数据库无缝对接，可以从 TDengine 数据库中快速、稳定地读取海量数据，并在此基础上进行全面、深入的分析处理，充分挖掘数据的潜在价值，为企业的决策制定提供有力的数据支持和科学依据，极大地提升数据处理的效率和质量，增强企业在数字化时代的竞争力和创新能力。

> **注意：本功能仅适用于 TDengine 企业版。**

## 安装和配置 Flink 服务器

安装 Apache Flink v1.19.0 或以上版本。详细请参考 [官方文档](https://flink.apache.org/)。

## 确认企业版服务正常

- 确认 taosd 服务正常；
- 确认 taosAdapter 服务正常；

<FlinkCommonInfo />

### 连接参数

建立连接的参数有 URL 和 Properties。  
URL 规范格式为：`jdbc:TAOS-WS://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&timezone={timezone}]`  

参数说明：

- user：登录 TDengine 用户名，默认值 'root'
- password：用户登录密码，默认值 'taosdata'
- database_name: 数据库名称。
- timezone: 时区设置。
- httpConnectTimeout: 连接超时时间，单位 ms，默认值为 60000。
- messageWaitTimeout: 消息超时时间，单位 ms，默认值为 60000。
- useSSL: 连接中是否使用 SSL。

### 数据准备

通过命令行工具 `taos` 或管理界面 Explorer 执行 SQL 语句，创建数据库，超级表，主题，并写入数据，供下一步订阅使用。以下为简单示例：

```sql
create database db vgroups 1;
create table db.meters (ts timestamp, f1 int) tags(t1 int);
create topic topic_meters as select ts, tbname, f1, t1 from db.meters;
insert into db.tb using db.meters tags(1) values(now, 1);
```

### Source

Source 拉取 TDengine 数据库中的数据，并将获取到的数据转换为 Flink 内部可处理的格式和类型，并以并行的方式进行读取和分发，为后续的数据处理提供高效的输入。
通过设置数据源的并行度，实现多个线程并行地从数据源中读取数据，提高数据读取的效率和吞吐量，充分利用集群资源进行大规模数据处理能力。

Properties 中配置参数如下：

- TDengineConfigParams.PROPERTY_KEY_USER：登录 TDengine 用户名，默认值 'root'
- TDengineConfigParams.PROPERTY_KEY_PASSWORD：用户登录密码，默认值 'taosdata'
- TDengineConfigParams.VALUE_DESERIALIZER：下游算子接收结果集反序列化方法，如果接收结果集类型是 `Flink` 的 `RowData`，仅需要设置为 `RowData`即可。也可继承 `TDengineRecordDeserialization` 并实现 `convert` 和 `getProducedType` 方法，根据 `SQL` 的 `ResultSet` 自定义反序列化方式
- TDengineConfigParams.TD_BATCH_MODE：此参数用于批量将数据推送给下游算子，如果设置为 True，创建 `TDengineSource` 对象时需要指定数据类型为 `SourceRecords` 类型的泛型形式
- TDengineConfigParams.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: 消息超时时间，单位 ms，默认值为 60000。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: 传输过程是否启用压缩。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: 是否启用自动重连。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: 自动重连重试间隔，单位毫秒，默认值 2000。仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: 自动重连重试次数，默认值 3，仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION: 关闭 SSL 证书验证。true: 启用，false: 不启用。默认为 false。

#### 按时间分片

用户可以对查询的 SQL 按照时间拆分为多个子任务，输入：开始时间，结束时间，拆分间隔，时间字段名称，系统会按照设置的间隔（时间左闭右开）进行拆分并行获取数据

```java
{{#include docs/examples/flink/source/Main.java:time_interval}}
```

#### 按超级表 TAG 分片

用户可以按照超级表的 TAG 字段将查询的 SQL 拆分为多个查询条件，系统会以一个查询条件对应一个子任务的方式对其进行拆分，进而并行获取数据。

```java
{{#include docs/examples/flink/source/Main.java:tag_split}}
```

#### 按表名分片

支持输入多个相同表结构的超级表或普通表进行分片，系统会按照一个表一个任务的方式进行拆分，进而并行获取数据。

```java
{{#include docs/examples/flink/source/Main.java:table_split}}
```

#### 使用 Source 连接器

查询结果为 RowData 数据类型示例：

<details>
<summary>RowData Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:source_test}}
```
</details>

批量查询结果示例：

<details>
<summary>Batch Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:source_batch_test}}
```
</details>

查询结果为自定义数据类型示例：

<details>
<summary>Custom Type Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:source_custom_type_test}}
```
</details>

- ResultBean 自定义的一个内部类，用于定义 Source 查询结果的数据类型。
- ResultSourceDeserialization 是自定义的一个内部类，通过继承 `TDengineRecordDeserialization` 并实现 `convert` 和 `getProducedType` 方法。

### CDC 数据订阅

Flink CDC 主要用于提供数据订阅功能，能实时监控 `TDengine` 数据库的数据变化，并将这些变更以数据流形式传输到 `Flink` 中进行处理，同时确保数据的一致性和完整性。

Properties 中配置参数如下：

- TDengineCdcParams.BOOTSTRAP_SERVERS：TDengine 服务端所在的`ip:port`，如果使用 `WebSocket` 连接，则为 taosAdapter 所在的`ip:port`
- TDengineCdcParams.CONNECT_USER：登录 TDengine 用户名，默认值 'root'
- TDengineCdcParams.CONNECT_PASS：用户登录密码，默认值 'taosdata'
- TDengineCdcParams.POLL_INTERVAL_MS：拉取数据间隔，默认 500ms
- TDengineCdcParams.VALUE_DESERIALIZER：结果集反序列化方法，如果接收结果集类型是 `Flink` 的 `RowData`，仅需要设置为 `RowData`即可。可以继承 `com.taosdata.jdbc.tmq.ReferenceDeserializer`，并指定结果集 bean，实现反序列化
- TDengineCdcParams.TMQ_BATCH_MODE：此参数用于批量将数据推送给下游算子，如果设置为 True，创建 `TDengineCdcSource` 对象时需要指定数据类型为 `ConsumerRecords` 类型的泛型形式
- TDengineCdcParams.GROUP_ID：消费组 ID，同一消费组共享消费进度。最大长度：192
- TDengineCdcParams.AUTO_OFFSET_RESET：消费组订阅的初始位置（ `earliest` 从头开始订阅，`latest` 仅从最新数据开始订阅，默认 `latest`）
- TDengineCdcParams.ENABLE_AUTO_COMMIT：是否启用消费位点自动提交，true: 自动提交；false：依赖 `checkpoint` 时间来提交，默认 false

> **注意**：自动提交模式 reader 获取完成数据后自动提交，不管下游算子是否正确的处理了数据，存在数据丢失的风险，主要用于为了追求高效的无状态算子场景或是数据一致性要求不高的场景

- TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS：消费记录自动提交消费位点时间间隔，单位为毫秒。默认值为 5000, 此参数在 `ENABLE_AUTO_COMMIT` 为 true 生效
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION：传输过程是否启用压缩。true: 启用，false: 不启用。默认为 false
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT：是否启用自动重连。true: 启用，false: 不启用。默认为 false
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS：自动重连重试间隔，单位毫秒，默认值 2000。仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT：自动重连重试次数，默认值 3，仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效
- TDengineCdcParams.TMQ_SESSION_TIMEOUT_MS：`consumer` 心跳丢失后超时时间，超时后会触发 `rebalance` 逻辑，成功后该 `consumer` 会被删除（从 3.3.3.0 版本开始支持），默认值为 12000，取值范围 [6000， 1800000]
- TDengineCdcParams.TMQ_MAX_POLL_INTERVAL_MS：`consumer poll` 拉取数据间隔的最长时间，超过该时间，会认为该 `consumer` 离线，触发 `rebalance` 逻辑，成功后该 `consumer` 会被删除。默认值为 300000，[1000，INT32_MAX]

#### 使用 CDC 连接器

CDC 连接器会根据用户设置的并行度进行创建 consumer，因此用户根据资源情况合理设置并行度。

订阅结果为 RowData 数据类型示例：

<details>
<summary>CDC Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:cdc_source}}
```
</details>

将订阅结果批量下发到算子的示例：

<details>
<summary>CDC Batch Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:cdc_batch_source}}
```
</details>

订阅结果为自定义数据类型示例：

<details>
<summary>CDC Custom Type</summary>
```java
{{#include docs/examples/flink/source/Main.java:cdc_custom_type_test}}
```
</details>

- ResultBean 是自定义的一个内部类，其字段名和数据类型与列的名称和数据类型一一对应，这样根据 `TDengineCdcParams.VALUE_DESERIALIZER` 属性对应的反序列化类可以反序列化出 ResultBean 类型的对象。

### Table SQL

使用 Table SQL 的方式从多个不同的数据源数据库（如 TDengine、MySQL、Oracle 等）中提取数据后，再进行自定义的算子操作（如数据清洗、格式转换、关联不同表的数据等），然后将处理后的结果加载到目标数据源（如 TDengine、Mysql 等）中。

#### Table Source 连接器

参数配置说明：

| 参数名称 | 类型 | 参数说明 |
| ----------------------- | :-----: | ------------ |
| connector | string | 连接器标识，设置 `tdengine-connector` |
| td.jdbc.url | string | 连接的 url |
| td.jdbc.mode | string | 连接器类型，设置 `source`、`sink` |
| table.name | string | 原表或目标表名称 |
| scan.query | string | 获取数据的 SQL 语句 |
| sink.db.name | string | 目标数据库名称 |
| sink.supertable.name | string | 写入的超级表名称 |
| sink.batch.size | integer | 写入的批大小 |
| sink.table.name | string | 写入的普通表或子表名称 |

使用示例：

将 power 库的 meters 表的子表数据，写入 power_sink 库的 sink_meters 超级表对应的子表中。

<details>
<summary>Table Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:source_table}}
```
</details>

#### Table CDC 连接器

参数配置说明：

| 参数名称 | 类型 | 参数说明 |
| ----------------------- | :-----: | ------------ |
| connector | string | 连接器标识，设置 `tdengine-connector` |
| user | string | 用户名，默认 root |
| password | string | 密码，默认 taosdata |
| bootstrap.servers | string | 服务器地址 |
| topic | string | 订阅主题 |
| td.jdbc.mode | string | 连接器类型，cdc、sink |
| group.id | string | 消费组 ID，同一消费组共享消费进度 |
| auto.offset.reset | string | 消费组订阅的初始位置<br/>`earliest`: 从头开始订阅<br/>`latest`: 仅从最新数据开始订阅<br/>默认 `latest` |
| poll.interval_ms | integer | 拉取数据间隔，默认 500ms |
| sink.db.name | string | 目标数据库名称 |
| sink.supertable.name | string | 写入的超级表名称 |
| sink.batch.size | integer | 写入的批大小 |
| sink.table.name | string | 写入的普通表或子表名称 |

使用示例：

订阅 power 库的 meters 超级表的子表数据，写入 power_sink 库的 sink_meters 超级表对应的子表中。

<details>
<summary>Table CDC</summary>
```java
{{#include docs/examples/flink/source/Main.java:cdc_table}}
```
</details>
