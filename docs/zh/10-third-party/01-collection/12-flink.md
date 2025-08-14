---
sidebar_label: Flink
title: TDengine TSDB Flink Connector
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Apache Flink 是一款由 Apache 软件基金会支持的开源分布式流批一体化处理框架，可用于流处理、批处理、复杂事件处理、实时数据仓库构建及为机器学习提供实时数据支持等诸多大数据处理场景。与此同时，Flink 拥有丰富的连接器与各类工具，可对接众多不同类型的数据源实现数据的读取与写入。在数据处理的过程中，Flink 还提供了一系列可靠的容错机制，有力保障任务即便遭遇意外状况，依然能稳定、持续运行。

借助 TDengine TSDB 的 Flink 连接器，Apache Flink 得以与 TDengine TSDB 数据库无缝对接，能够将来自不同数据源的数据经过复杂运算和深度分析后所得到的结果精准存入 TDengine TSDB 数据库，实现数据的高效存储与管理；为后续进行全面、深入的分析处理，充分挖掘数据的潜在价值，为企业的决策制定提供有力的数据支持和科学依据，极大地提升数据处理的效率和质量，增强企业在数字化时代的竞争力和创新能力。

## 前置条件

准备以下环境：

- TDengine TSDB 服务已部署并正常运行（企业及社区版均可）
- taosAdapter 能够正常运行。详细参考 [taosAdapter 使用手册](../../../reference/components/taosadapter)
- Apache Flink v1.19.0 或以上版本已安装。安装 Apache Flink 请参考 [官方文档](https://flink.apache.org/)

# 支持的平台

Flink Connector 支持所有能运行 Flink 1.19 及以上版本的平台。

## 版本历史

| Flink Connector 版本 |                   主要变化         |   TDengine TSDB 版本   |
| ------------------| ------------------------------------ | ---------------- |
|        2.1.3      | 增加数据转换异常信息输出 | - |
|        2.1.2      | 写入字段增加反引号过滤 | - |
|        2.1.1      | 修复 Stmt 相同表的数据绑定失败问题 | - |
|        2.1.0      | 修复不同数据源 varchar 类型写入问题 | - |
|        2.0.2      | Table Sink 支持 RowKind.UPDATE_BEFORE、RowKind.UPDATE_AFTER 和 RowKind.DELETE 类型 | - |
|        2.0.1      | Sink 支持对所有继承自 RowData 并已实现的类型进行数据写入 | - |
|        2.0.0      | 1. Sink 支持自定义数据结构序列化，写入 TDengine TSDB <br/> 2. 支持 Table SQL 方式写入 TDengine TSDB 数据库 | 3.3.5.1 及以上版本 |
|        1.0.0      | 支持 Sink 功能，将来着其他数据源的数据写入到 TDengine TSDB| 3.3.2.0 及以上版本|

## 异常和错误码

在任务执行失败后，查看 Flink 任务执行日志确认失败原因

具体的错误码请参考：

| Error Code       | Description                                              | Suggested Actions    |
| ---------------- |-------------------------------------------------------   | -------------------- |
| 0xa000     |connection param error                                          |连接器参数错误。
| 0xa010     |database name configuration error                               |数据库名配置错误。|
| 0xa011     |table name configuration error                                  |表名配置错误。|
| 0xa013     |value.deserializer parameter not set                            |未设置序列化方式。|
| 0xa014     |list of column names for target table not set                   |未设置目标表的列名列表。|
| 0x2301     |connection already closed                                       |连接已经关闭，检查连接情况，或重新创建连接去执行相关指令。|
| 0x2302     |this operation is NOT supported currently!                      |当前使用接口不支持，可以更换其他连接方式。|
| 0x2303     |invalid variables                                               |参数不合法，请检查相应接口规范，调整参数类型及大小。|
| 0x2304     |statement is closed                                             |statement 已经关闭，请检查 statement 是否关闭后再次使用，或是连接是否正常。|
| 0x2305     |resultSet is closed                                             |resultSet 结果集已经释放，请检查 resultSet 是否释放后再次使用。|
| 0x230d     |parameter index out of range                                    |参数越界，请检查参数的合理范围。|
| 0x230e     |connection already closed                                       |连接已经关闭，请检查 Connection 是否关闭后再次使用，或是连接是否正常。|
| 0x230f     |unknown sql type in TDengine                                    |请检查 TDengine TSDB 支持的 Data Type 类型。|
| 0x2315     |unknown taos type in TDengine                                   |在 TDengine TSDB 数据类型与 JDBC 数据类型转换时，是否指定了正确的 TDengine TSDB 数据类型。|
| 0x2319     |user is required                                                |创建连接时缺少用户名信息。|
| 0x231a     |password is required                                            |创建连接时缺少密码信息。|
| 0x231d     |can't create connection with server within                      |通过增加参数 httpConnectTimeout 增加连接耗时，或是请检查与 taosAdapter 之间的连接情况。|
| 0x231e     |failed to complete the task within the specified time           |通过增加参数 messageWaitTimeout 增加执行耗时，或是请检查与 taosAdapter 之间的连接情况。|
| 0x2352     |Unsupported encoding                                            |本地连接下指定了不支持的字符编码集。|
| 0x2353     |internal error of database, please see taoslog for more details |本地连接执行 prepareStatement 时出现错误，请检查 taos log 进行问题定位。|
| 0x2354     |connection is NULL                                              |本地连接执行命令时，Connection 已经关闭。请检查与 TDengine TSDB 的连接情况。|
| 0x2355     |result set is NULL                                              |本地连接获取结果集，结果集异常，请检查连接情况，并重试。|
| 0x2356     |invalid num of fields                                           |本地连接获取结果集的 meta 信息不匹配。|

## 数据类型映射

TDengine TSDB 目前支持时间戳、数字、字符、布尔类型，与 Flink RowData Type 对应类型转换如下：

| TDengine TSDB DataType | Flink RowDataType |
| ----------------- | ------------------ |
| TIMESTAMP         | TimestampData |
| INT               | Integer       |
| BIGINT            | Long          |
| FLOAT             | Float         |
| DOUBLE            | Double        |
| SMALLINT          | Short         |
| TINYINT           | Byte          |
| BOOL              | Boolean       |
| VARCHAR           | StringData    |
| BINARY            | StringData    |
| NCHAR             | StringData    |
| JSON              | StringData    |
| VARBINARY         | byte[]        |
| GEOMETRY          | byte[]        |

## 使用说明

### Flink 语义选择说明

采用 At-Least-Once（至少一次）语义原因：

- TDengine TSDB 目前不支持事务，不能进行频繁的检查点操作和复杂的事务协调。
- 由于 TDengine TSDB 采用时间戳作为主键，重复数据下游算子可以进行过滤操作，避免重复计算。
- 采用 At-Least-Once（至少一次）确保达到较高的数据处理的性能和较低的数据延时，设置方式如下：

使用方式：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
```

如果使用 Maven 管理项目，只需在 pom.xml 中加入以下依赖。

```xml
<dependency>
    <groupId>com.taosdata.flink</groupId>
    <artifactId>flink-connector-tdengine</artifactId>
    <version>2.1.3</version>
</dependency>
```

### 连接参数

建立连接的参数有 URL 和 Properties。  
URL 规范格式为：
`jdbc:TAOS-WS://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&timezone={timezone}]`  

参数说明：

- user：登录 TDengine TSDB 用户名，默认值 'root'。
- password：用户登录密码，默认值 'taosdata'。
- database_name: 数据库名称。
- timezone: 时区设置。
- httpConnectTimeout: 连接超时时间，单位 ms，默认值为 60000。
- messageWaitTimeout: 消息超时时间，单位 ms，默认值为 60000。
- useSSL: 连接中是否使用 SSL。

### Sink

Sink 的核心功能在于高效且精准地将经过 Flink 处理的、源自不同数据源或算子的数据写入 TDengine TSDB。在这一过程中 TDengine TSDB 所具备的高效写入机制发挥了至关重要的作用，有力保障了数据的快速和稳定存储。

:::note

- 写入的数据库必须已经创建。
- 写入的超级表/普通表必须已经创建。

:::

Properties 中配置参数如下：

- TDengineConfigParams.PROPERTY_KEY_USER：登录 TDengine TSDB 用户名，默认值 'root'。
- TDengineConfigParams.PROPERTY_KEY_PASSWORD：用户登录密码，默认值 'taosdata'。
- TDengineConfigParams.PROPERTY_KEY_DBNAME：写入的数据库名称。
- TDengineConfigParams.TD_SUPERTABLE_NAME：写入的超级表名称。写入的数据必须有 tbname 字段，确定写入那张子表。
- TDengineConfigParams.TD_TABLE_NAME：写入子表或普通表的表名，此参数和 TD_SUPERTABLE_NAME 仅需要设置一个即可。
- TDengineConfigParams.VALUE_DESERIALIZER：接收结果集反序列化方法，如果接收结果集类型是 Flink 的 `RowData`，仅需要设置为 `RowData`即可。也可继承 [TDengineSinkRecordSerializer](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/main/java/com/taosdata/flink/sink/serializer/TDengineSinkRecordSerializer.java) 并实现 `serialize` 方法，根据 接收的数据类型自定义反序列化方式。
- TDengineConfigParams.TD_BATCH_SIZE：设置一次写入 TDengine TSDB 数据库的批大小 | 当到达批的数量后进行写入，或是一个 checkpoint 的时间也会触发写入数据库。
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
{{#include docs/examples/flink/Main.java:RowDataToSuperTable}}
```
</details>

使用示例

将 `RowData` 类型的数据写入 `power_sink` 库的 `sink_normal` 普通表表中。

<details>
<summary>RowData Into Normal Table</summary>
```java
{{#include docs/examples/flink/Main.java:RowDataToNormalTable}}
```
</details>

使用示例：

将自定义类型的数据写入 `power_sink` 库的 `sink_meters` 超级表对应的子表中。

<details>
<summary>CustomType Into Super Table</summary>
```java
{{#include docs/examples/flink/Main.java:CustomTypeToNormalTable}}
```
</details>

:::note

- [ResultBean](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/test/java/com/taosdata/flink/entity/ResultBean.java) 自定义的一个内部类，用于定义写入字段的数据类型。
- [ResultBeanSinkSerializer](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/test/java/com/taosdata/flink/entity/ResultBeanSinkSerializer.java) 是自定义的一个内部类，通过继承 TDengineRecordDeserialization 并实现 serialize 方法完成序列化。

:::

### Table Sink

使用 Flink Table 的方式从多个不同的数据源数据库（如 MySQL、Oracle, Kafka 等）中提取数据后，再进行自定义的算子操作（如数据清洗、格式转换、关联不同表的数据等），然后将处理后的结果写入到 TDengine TSDB 中。

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
{{#include docs/examples/flink/Main.java:TableSqlToSink}}
```
</details>

使用示例：

通过 SQL 语句写入 `power_sink` 库的 `sink_normal` 普通表中。

<details>
<summary>Table SQL Into Normal Table </summary>
```java
{{#include docs/examples/flink/Main.java:NormalTableSqlToSink}}
```
</details>

使用示例：

将 `Row` 类型数据写入 `power_sink` 库的 `sink_meters` 超级表对应的子表中。

<details>
<summary>Table Row To Sink </summary>
```java
{{#include docs/examples/flink/Main.java:TableRowToSink}}
```
</details>
