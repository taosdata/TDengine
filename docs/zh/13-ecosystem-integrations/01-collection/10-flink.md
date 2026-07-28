---
sidebar_label: Flink
title: TDengine Flink Connector
description: 使用 Flink 连接器向 TDengine 写入数据（Sink）；企业版另支持 Source/CDC 读数
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import FlinkCommonInfo from './flink/_flink-common-info.mdx'

Apache Flink 是由 Apache 软件基金会支持的开源分布式流批一体处理框架，可用于流处理、批处理、复杂事件处理、实时数仓构建等场景，并通过连接器对接多种数据源进行读写。

TDengine 通过 **Flink 连接器**与 Flink 集成：连接器既可将 Flink 作业中的处理结果写入 TDengine（Sink），也可在企业版中从 TDengine 读数（Source/CDC）。两侧通过 JDBC/连接器直接访问数据库，而不是把 TDengine 当作向 Flink 投递消息的消息队列。

本文说明社区版可用的 Sink / Table Sink 用法。企业版 Source、CDC 与 Table Source 详见 [数据发布](../../08-data-ingest-and-delivery/02-no-code-delivery/index.md) 中的 [Flink](../../08-data-ingest-and-delivery/02-no-code-delivery/02-Flink.md)。

## 前置条件

准备以下环境：

- TDengine 服务已部署并正常运行（企业及社区版均可）
- taosAdapter 能够正常运行。详细参考 [taosAdapter 使用手册](../../12-operations-and-tooling/03-components/03-taosadapter.md)
- Apache Flink 1.19.0 或以上版本已安装。安装 Apache Flink 请参考 [官方文档](https://flink.apache.org/)

## 支持的平台

Flink Connector 支持所有能运行 Flink 1.19 及以上版本的平台。

<FlinkCommonInfo />

### 连接参数

建立连接的参数有 URL 和 Properties。  
URL 规范格式为：
`jdbc:TAOS-WS://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&timezone={timezone}]`  

参数说明：

- user：登录 TDengine 用户名，默认值 'root'。
- password：用户登录密码，默认值 'taosdata'。
- database_name: 数据库名称。
- timezone: 时区设置。
- httpConnectTimeout: 连接超时时间，单位 ms，默认值为 60000。
- messageWaitTimeout: 消息超时时间，单位 ms，默认值为 60000。
- useSSL: 连接中是否使用 SSL。

### Sink

Sink 将 Flink 作业中经过处理的数据写入 TDengine。写入前须已创建目标数据库以及超级表或普通表。

:::note

- 写入的数据库必须已经创建。
- 写入的超级表/普通表必须已经创建。

:::

Properties 中配置参数如下：

- TDengineConfigParams.PROPERTY_KEY_USER：登录 TDengine 用户名，默认值 'root'。
- TDengineConfigParams.PROPERTY_KEY_PASSWORD：用户登录密码，默认值 'taosdata'。
- TDengineConfigParams.PROPERTY_KEY_DBNAME：写入的数据库名称。
- TDengineConfigParams.TD_SUPERTABLE_NAME：写入的超级表名称。写入的数据必须有 tbname 字段，确定写入哪张子表。
- TDengineConfigParams.TD_TABLE_NAME：写入子表或普通表的表名，此参数和 TD_SUPERTABLE_NAME 仅需要设置一个即可。
- TDengineConfigParams.VALUE_DESERIALIZER：接收结果集反序列化方法，如果接收结果集类型是 Flink 的 `RowData`，仅需要设置为 `RowData`即可。也可继承 [TDengineSinkRecordSerializer](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/main/java/com/taosdata/flink/sink/serializer/TDengineSinkRecordSerializer.java) 并实现 `serialize` 方法，根据 接收的数据类型自定义反序列化方式。
- TDengineConfigParams.TD_BATCH_SIZE：设置一次写入 TDengine 数据库的批大小 | 当到达批的数量后进行写入，或是一个 checkpoint 的时间也会触发写入数据库。
- TDengineConfigParams.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: 消息超时时间，单位 ms，默认值为 60000。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: 传输过程是否启用压缩。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: 是否启用自动重连。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: 自动重连重试间隔，单位毫秒，默认值 2000。仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: 自动重连重试次数，默认值 3，仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION: 关闭 SSL 证书验证。true: 启用，false: 不启用。默认为 false。

使用示例：

将 `RowData` 类型的数据写入 `power_sink` 库的 `sink_meters` 超级表对应的子表中。

<details>
<summary>RowData Into Super Table</summary>
```java
{{#include docs/examples/flink/sink/Main.java:RowDataToSuperTable}}
```
</details>

使用示例

将 `RowData` 类型的数据写入 `power_sink` 库的 `sink_normal` 普通表中。

<details>
<summary>RowData Into Normal Table</summary>
```java
{{#include docs/examples/flink/sink/Main.java:RowDataToNormalTable}}
```
</details>

使用示例：

将自定义类型的数据写入 `power_sink` 库的 `sink_meters` 超级表对应的子表中。

<details>
<summary>CustomType Into Super Table</summary>
```java
{{#include docs/examples/flink/sink/Main.java:CustomTypeToNormalTable}}
```
</details>

:::note

- [ResultBean](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/test/java/com/taosdata/flink/entity/ResultBean.java) 自定义的一个内部类，用于定义写入字段的数据类型。
- [ResultBeanSinkSerializer](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/test/java/com/taosdata/flink/entity/ResultBeanSinkSerializer.java) 是自定义的一个内部类，通过继承 TDengineRecordDeserialization 并实现 serialize 方法完成序列化。

:::

### Table Sink

使用 Flink Table 的方式从多个不同的数据源（如 MySQL、Oracle、Kafka 等）中提取数据后，再进行自定义的算子操作（如数据清洗、格式转换、关联不同表的数据等），然后将处理后的结果写入 TDengine。

参数配置说明：

|         参数名称          |  类型   | 参数说明      |
| ----------------------- | :-----: | ------------ |
| connector  | string | 连接器标识，设置 `tdengine-connector` 。|
| td.jdbc.url| string | 连接的 url。|
| td.jdbc.mode | string | 连接器类型，设置 `sink`。|
| sink.db.name|string| 目标数据库名称。|
| sink.batch.size | integer | 写入的批大小。|
| sink.supertable.name|string |写入的超级表名称。|
| sink.table.name|string|写入的普通表或子表名称。|

使用示例：

通过 SQL 语句写入 `power_sink` 库的 `sink_meters` 超级表对应的子表中。

<details>
<summary>Table SQL Into Super Table </summary>
```java
{{#include docs/examples/flink/sink/Main.java:TableSqlToSink}}
```
</details>

使用示例：

通过 SQL 语句写入 `power_sink` 库的 `sink_normal` 普通表中。

<details>
<summary>Table SQL Into Normal Table </summary>
```java
{{#include docs/examples/flink/sink/Main.java:NormalTableSqlToSink}}
```
</details>

使用示例：

将 `Row` 类型数据写入 `power_sink` 库的 `sink_meters` 超级表对应的子表中。

<details>
<summary>Table Row To Sink </summary>
```java
{{#include docs/examples/flink/sink/Main.java:TableRowToSink}}
```
</details>
