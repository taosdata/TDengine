---
sidebar_label: Flink
title: TDengine Flink Connector
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Apache Flink 是一款由 Apache 软件基金会支持的开源分布式流批一体化处理框架，可用于流处理、批处理、复杂事件处理、实时数据仓库构建及为机器学习提供实时数据支持等诸多大数据处理场景。与此同时，Flink 拥有丰富的连接器与各类工具，可对接众多不同类型的数据源实现数据的读取与写入。在数据处理的过程中，Flink 还提供了一系列可靠的容错机制，有力保障任务即便遭遇意外状况，依然能稳定、持续运行。

借助 TDengine 的 Flink 连接器，Apache Flink 得以与 TDengine 数据库无缝对接，一方面能够将经过复杂运算和深度分析后所得到的结果精准存入 TDengine 数据库，实现数据的高效存储与管理；另一方面，也可以从 TDengine 数据库中快速、稳定地读取海量数据，并在此基础上进行全面、深入的分析处理，充分挖掘数据的潜在价值，为企业的决策制定提供有力的数据支持和科学依据，极大地提升数据处理的效率和质量，增强企业在数字化时代的竞争力和创新能力。

## 前置条件 

准备以下环境：
- TDengine 服务已部署并正常运行（企业及社区版均可）
- taosAdapter 能够正常运行。详细参考 [taosAdapter 使用手册](../../../reference/components/taosadapter)
- Apache Flink v1.19.0 或以上版本已安装。安装 Apache Flink 请参考 [官方文档](https://flink.apache.org/)

# 支持的平台

Flink Connector 支持所有能运行 Flink 1.19 及以上版本的平台。

## 版本历史
| Flink Connector 版本 |                   主要变化         |   TDengine 版本   |
| ------------------| ------------------------------------ | ---------------- |
|        2.0.2      | Table Sink 支持 RowKind.UPDATE_BEFORE、RowKind.UPDATE_AFTER 和 RowKind.DELETE 类型| - |
|        2.0.1      | Sink 支持对所有继承自 RowData 并已实现的类型进行数据写入| - |
|        2.0.0      | 1. 支持 SQL 查询 TDengine 数据库中的数据<br/> 2. 支持 CDC 订阅 TDengine 数据库中的数据<br/> 3. 支持 Table SQL 方式读取和写入 TDengine 数据库| 3.3.5.1 及以上版本 |
|        1.0.0      | 支持 Sink 功能，将来着其他数据源的数据写入到 TDengine| 3.3.2.0 及以上版本|

## 异常和错误码

在任务执行失败后，查看 Flink 任务执行日志确认失败原因

具体的错误码请参考：

| Error Code       | Description                                              | Suggested Actions    |
| ---------------- |-------------------------------------------------------   | -------------------- |
| 0xa000     |connection param error                                          |连接器参数错误。                                                                   
| 0xa001     |the groupid parameter of CDC is incorrect                       |CDC 的 groupid 参数错误。|
| 0xa002     |wrong topic parameter for CDC                                   |CDC 的 topic 参数错误。|
| 0xa010     |database name configuration error                               |数据库名配置错误。|
| 0xa011     |table name configuration error                                  |表名配置错误。|
| 0xa012     |no data was obtained from the data source                       |从数据源中获取数据失败。|
| 0xa013     |value.deserializer parameter not set                            |未设置序列化方式。|
| 0xa014     |list of column names for target table not set                   |未设置目标表的列名列表。|
| 0x2301     |connection already closed                                       |连接已经关闭，检查连接情况，或重新创建连接去执行相关指令。|
| 0x2302     |this operation is NOT supported currently!                      |当前使用接口不支持，可以更换其他连接方式。|
| 0x2303     |invalid variables                                               |参数不合法，请检查相应接口规范，调整参数类型及大小。|
| 0x2304     |statement is closed                                             |statement 已经关闭，请检查 statement 是否关闭后再次使用，或是连接是否正常。|
| 0x2305     |resultSet is closed                                             |resultSet 结果集已经释放，请检查 resultSet 是否释放后再次使用。|
| 0x230d     |parameter index out of range                                    |参数越界，请检查参数的合理范围。|
| 0x230e     |connection already closed                                       |连接已经关闭，请检查 Connection 是否关闭后再次使用，或是连接是否正常。|
| 0x230f     |unknown sql type in TDengine                                    |请检查 TDengine 支持的 Data Type 类型。|
| 0x2315     |unknown taos type in TDengine                                   |在 TDengine 数据类型与 JDBC 数据类型转换时，是否指定了正确的 TDengine 数据类型。|
| 0x2319     |user is required                                                |创建连接时缺少用户名信息。|
| 0x231a     |password is required                                            |创建连接时缺少密码信息。|
| 0x231d     |can't create connection with server within                      |通过增加参数 httpConnectTimeout 增加连接耗时，或是请检查与 taosAdapter 之间的连接情况。|
| 0x231e     |failed to complete the task within the specified time           |通过增加参数 messageWaitTimeout 增加执行耗时，或是请检查与 taosAdapter 之间的连接情况。|
| 0x2352     |Unsupported encoding                                            |本地连接下指定了不支持的字符编码集。|
| 0x2353     |internal error of database, please see taoslog for more details |本地连接执行 prepareStatement 时出现错误，请检查 taos log 进行问题定位。|
| 0x2354     |connection is NULL                                              |本地连接执行命令时，Connection 已经关闭。请检查与 TDengine 的连接情况。|
| 0x2355     |result set is NULL                                              |本地连接获取结果集，结果集异常，请检查连接情况，并重试。|
| 0x2356     |invalid num of fields                                           |本地连接获取结果集的 meta 信息不匹配。|
| 0x2357     |empty sql string                                                |填写正确的 SQL 进行执行。|
| 0x2371     |consumer properties must not be null!                           |创建订阅时参数为空，请填写正确的参数。|
| 0x2375     |topic reference has been destroyed                              |创建数据订阅过程中，topic 引用被释放。请检查与 TDengine 的连接情况。|
| 0x2376     |failed to set consumer topic, topic name is empty               |创建数据订阅过程中，订阅 topic 名称为空。请检查指定的 topic 名称是否填写正确。|
| 0x2377     |consumer reference has been destroyed                           |订阅数据传输通道已经关闭，请检查与 TDengine 的连接情况。|
| 0x2378     |consumer create error                                           |创建数据订阅失败，请根据错误信息检查 taos log 进行问题定位。|
| 0x237a     |vGroup not found in result set VGroup                           |没有分配给当前 consumer，由于 Rebalance 机制导致 Consumer 与 VGroup 不是绑定的关系。|

## 数据类型映射
TDengine 目前支持时间戳、数字、字符、布尔类型，与 Flink RowData Type 对应类型转换如下：

| TDengine DataType | Flink RowDataType |
| ----------------- | ------------------ |
| TIMESTAMP         | TimestampData |
| INT               | Integer       |
| BIGINT            | Long          |
| FLOAT             | Float         |
| DOUBLE            | Double        |
| SMALLINT          | Short         |
| TINYINT           | Byte          |
| BOOL              | Boolean       |
| BINARY            | byte[]        |
| NCHAR             | StringData    |
| JSON              | StringData    |
| VARBINARY         | byte[]        |
| GEOMETRY          | byte[]        |

## 使用说明

### Flink 语义选择说明

采用 At-Least-Once（至少一次）语义原因：
  - TDengine 目前不支持事务，不能进行频繁的检查点操作和复杂的事务协调。
  - 由于 TDengine 采用时间戳作为主键，重复数据下游算子可以进行过滤操作，避免重复计算。
  - 采用 At-Least-Once（至少一次）确保达到较高的数据处理的性能和较低的数据延时，设置方式如下：

使用方式:

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
    <version>2.0.2</version>
</dependency>
```

### 连接参数

建立连接的参数有 URL 和 Properties。  
URL 规范格式为：
`jdbc:TAOS-WS://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&timezone={timezone}]`  

参数说明：

- user：登录 TDengine 用户名，默认值 'root'。
- password：用户登录密码，默认值 'taosdata'。
- database_name: 数据库名称。
- timezone: 时区设置。
- httpConnectTimeout: 连接超时时间，单位 ms， 默认值为 60000。
- messageWaitTimeout: 消息超时时间，单位 ms， 默认值为 60000。 
- useSSL: 连接中是否使用 SSL。

### Source

Source 拉取 TDengine 数据库中的数据，并将获取到的数据转换为 Flink 内部可处理的格式和类型，并以并行的方式进行读取和分发，为后续的数据处理提供高效的输入。
通过设置数据源的并行度，实现多个线程并行地从数据源中读取数据，提高数据读取的效率和吞吐量，充分利用集群资源进行大规模数据处理能力。

Properties 中配置参数如下：

- TDengineConfigParams.PROPERTY_KEY_USER：登录 TDengine 用户名，默认值 'root'。
- TDengineConfigParams.PROPERTY_KEY_PASSWORD：用户登录密码，默认值 'taosdata'。
- TDengineConfigParams.VALUE_DESERIALIZER：下游算子接收结果集反序列化方法, 如果接收结果集类型是 `Flink` 的 `RowData`，仅需要设置为 `RowData`即可。也可继承 `TDengineRecordDeserialization` 并实现 `convert` 和 `getProducedType` 方法，根据 `SQL` 的 `ResultSet` 自定义反序列化方式。
- TDengineConfigParams.TD_BATCH_MODE：此参数用于批量将数据推送给下游算子，如果设置为 True，创建 `TDengineSource` 对象时需要指定数据类型为 `SourceRecords` 类型的泛型形式。
- TDengineConfigParams.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: 消息超时时间, 单位 ms， 默认值为 60000。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: 传输过程是否启用压缩。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: 是否启用自动重连。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: 自动重连重试间隔，单位毫秒，默认值 2000。仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: 自动重连重试次数，默认值 3，仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION: 关闭 SSL 证书验证 。true: 启用，false: 不启用。默认为 false。

#### 按时间分片

用户可以对查询的 SQL 按照时间拆分为多个子任务，输入：开始时间，结束时间，拆分间隔，时间字段名称，系统会按照设置的间隔（时间左闭右开）进行拆分并行获取数据。

```java
{{#include docs/examples/flink/Main.java:time_interval}}
```

#### 按超级表 TAG 分片

用户可以按照超级表的 TAG 字段将查询的 SQL 拆分为多个查询条件，系统会以一个查询条件对应一个子任务的方式对其进行拆分，进而并行获取数据。

```java
{{#include docs/examples/flink/Main.java:tag_split}}
```

#### 按表名分片

支持输入多个相同表结构的超级表或普通表进行分片，系统会按照一个表一个任务的方式进行拆分，进而并行获取数据。

```java
{{#include docs/examples/flink/Main.java:table_split}}
```

#### 使用 Source 连接器

查询结果为 RowData 数据类型示例：

<details>
<summary>RowData Source</summary>
```java
{{#include docs/examples/flink/Main.java:source_test}}
```
</details>

批量查询结果示例：

<details>
<summary>Batch Source</summary>
```java
{{#include docs/examples/flink/Main.java:source_batch_test}}
```
</details> 

查询结果为自定义数据类型示例：

<details>
<summary>Custom Type Source</summary>
```java
{{#include docs/examples/flink/Main.java:source_custom_type_test}}
```
</details> 

- ResultBean 自定义的一个内部类，用于定义 Source 查询结果的数据类型。
- ResultSoureDeserialization 是自定义的一个内部类，通过继承 `TDengineRecordDeserialization` 并实现 `convert` 和 `getProducedType` 方法。

### CDC 数据订阅

Flink CDC 主要用于提供数据订阅功能，能实时监控 `TDengine` 数据库的数据变化，并将这些变更以数据流形式传输到 `Flink` 中进行处理，同时确保数据的一致性和完整性。

Properties 中配置参数如下：

- TDengineCdcParams.BOOTSTRAP_SERVERS：TDengine 服务端所在的`ip:port`，如果使用 `WebSocket` 连接，则为 taosAdapter 所在的`ip:port`。
- TDengineCdcParams.CONNECT_USER：登录 TDengine 用户名，默认值 'root'。
- TDengineCdcParams.CONNECT_PASS：用户登录密码，默认值 'taosdata'。
- TDengineCdcParams.POLL_INTERVAL_MS：拉取数据间隔, 默认 500ms。
- TDengineCdcParams.VALUE_DESERIALIZER：结果集反序列化方法，如果接收结果集类型是 `Flink` 的 `RowData`，仅需要设置为 `RowData`即可。可以继承 `com.taosdata.jdbc.tmq.ReferenceDeserializer`，并指定结果集 bean，实现反序列化。
- TDengineCdcParams.TMQ_BATCH_MODE：此参数用于批量将数据推送给下游算子，如果设置为 True，创建 `TDengineCdcSource` 对象时需要指定数据类型为 `ConsumerRecords` 类型的泛型形式。
- TDengineCdcParams.GROUP_ID：消费组 ID，同一消费组共享消费进度。最大长度：192。
- TDengineCdcParams.AUTO_OFFSET_RESET： 消费组订阅的初始位置 （ `earliest` 从头开始订阅, `latest` 仅从最新数据开始订阅, 默认 `latest`）。
- TDengineCdcParams.ENABLE_AUTO_COMMIT：是否启用消费位点自动提交，true: 自动提交；false：依赖 `checkpoint` 时间来提交， 默认 false。
> **注意**：自动提交模式reader获取完成数据后自动提交，不管下游算子是否正确的处理了数据，存在数据丢失的风险，主要用于为了追求高效的无状态算子场景或是数据一致性要求不高的场景。

- TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS：消费记录自动提交消费位点时间间隔，单位为毫秒。默认值为 5000, 此参数在 `ENABLE_AUTO_COMMIT` 为 true 生效。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION：传输过程是否启用压缩。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT：是否启用自动重连。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS：自动重连重试间隔，单位毫秒，默认值 2000。仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT：自动重连重试次数，默认值 3，仅在 `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` 为 true 时生效。
- TDengineCdcParams.TMQ_SESSION_TIMEOUT_MS：`consumer` 心跳丢失后超时时间，超时后会触发 `rebalance` 逻辑，成功后该 `consumer` 会被删除（从3.3.3.0版本开始支持）， 默认值为 12000，取值范围 [6000， 1800000]。
- TDengineCdcParams.TMQ_MAX_POLL_INTERVAL_MS：`consumer poll` 拉取数据间隔的最长时间，超过该时间，会认为该 `consumer` 离线，触发 `rebalance` 逻辑，成功后该 `consumer` 会被删除。 默认值为 300000，[1000，INT32_MAX]。

#### 使用 CDC 连接器

CDC 连接器会根据用户设置的并行度进行创建 consumer，因此用户根据资源情况合理设置并行度。

订阅结果为 RowData 数据类型示例：

<details>
<summary>CDC Source</summary>
```java
{{#include docs/examples/flink/Main.java:cdc_source}}
```
</details> 

将订阅结果批量下发到算子的示例：

<details>
<summary>CDC Batch Source</summary>
```java
{{#include docs/examples/flink/Main.java:cdc_batch_source}}
```
</details> 

订阅结果为自定义数据类型示例：

<details>
<summary>CDC Custom Type</summary>
```java
{{#include docs/examples/flink/Main.java:cdc_custom_type_test}}
```
</details> 

- ResultBean 是自定义的一个内部类，其字段名和数据类型与列的名称和数据类型一一对应，这样根据 `TDengineCdcParams.VALUE_DESERIALIZER` 属性对应的反序列化类可以反序列化出 ResultBean 类型的对象。

### Sink 

Sink 的核心功能在于高效且精准地将经过 `Flink` 处理的、源自不同数据源或算子的数据写入 `TDengine`。在这一过程中，`TDengine` 所具备的高效写入机制发挥了至关重要的作用，有力保障了数据的快速和稳定存储。

Properties 中配置参数如下：

- TDengineConfigParams.PROPERTY_KEY_USER：登录 `TDengine` 用户名，默认值 'root'。
- TDengineConfigParams.PROPERTY_KEY_PASSWORD：用户登录密码，默认值 'taosdata'。
- TDengineConfigParams.PROPERTY_KEY_DBNAME：写入的数据库名称。
- TDengineConfigParams.TD_SUPERTABLE_NAME：写入的超级表名称。接收的数据必须有 tbname 字段，确定写入那张子表。
- TDengineConfigParams.TD_TABLE_NAME：写入子表或普通表的表名，此参数和TD_SUPERTABLE_NAME 仅需要设置一个即可。
- TDengineConfigParams.VALUE_DESERIALIZER：接收结果集反序列化方法, 如果接收结果集类型是 `Flink` 的 `RowData`，仅需要设置为 `RowData`即可。也可继承 `TDengineSinkRecordSerializer` 并实现 `serialize` 方法，根据 接收的数据类型自定义反序列化方式。
- TDengineConfigParams.TD_BATCH_SIZE：设置一次写入 `TDengine` 数据库的批大小 | 当到达批的数量后进行写入，或是一个checkpoint的时间也会触发写入数据库。 
- TDengineConfigParams.TD_BATCH_MODE：接收批量数据当设置为 True 时，如果数据来源是 `TDengine Source`，则使用 `SourceRecords` 泛型类型来创建 `TDengineSink` 对象；若来源是 `TDengine CDC`，则使用 `ConsumerRecords` 泛型来创建 `TDengineSink` 对象。
- TDengineConfigParams.TD_SOURCE_TYPE：设置数据来源。 当数据来源是 `TDengine Source` 是设置为 'tdengine_source', 当来源是 `TDengine CDC` 设置为 'tdengine_cdc'。当配置 `TD_BATCH_MODE` 为 True 生效。
- TDengineConfigParams.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: 消息超时时间, 单位 ms， 默认值为 60000。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: 传输过程是否启用压缩。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: 是否启用自动重连。true: 启用，false: 不启用。默认为 false。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: 自动重连重试间隔，单位毫秒，默认值 2000。仅在 PROPERTY_KEY_ENABLE_AUTO_RECONNECT 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: 自动重连重试次数，默认值 3，仅在 PROPERTY_KEY_ENABLE_AUTO_RECONNECT 为 true 时生效。
- TDengineConfigParams.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION: 关闭 SSL 证书验证 。true: 启用，false: 不启用。默认为 false。

使用示例：

将 power 库的 meters 表的子表数据，写入 power_sink 库的 sink_meters 超级表对应的子表中。

<details>
<summary>Sink RowData</summary>
```java
{{#include docs/examples/flink/Main.java:RowDataToSink}}
```
</details> 

使用示例：

订阅 power 库的 meters 超级表的子表数据，写入 power_sink 库的 sink_meters 超级表对应的子表中。

<details>
<summary>Cdc Sink</summary>
```java
{{#include docs/examples/flink/Main.java:CdcRowDataToSink}}
```
</details>

### Table SQL

使用 Table SQL 的方式从多个不同的数据源数据库（如 TDengine、MySQL、Oracle 等）中提取数据后， 再进行自定义的算子操作（如数据清洗、格式转换、关联不同表的数据等），然后将处理后的结果加载到目标数据源（如 TDengine、Mysql 等）中。

#### Table Source 连接器

参数配置说明：

|         参数名称          |  类型   | 参数说明      | 
| ----------------------- | :-----: | ------------ |
| connector  | string | 连接器标识，设置 `tdengine-connector` 。|
| td.jdbc.url| string | 连接的 url 。| 
| td.jdbc.mode | strng | 连接器类型, 设置 `source`, `sink`。|
| table.name| string| 原表或目标表名称。|
| scan.query| string| 获取数据的 SQL 语句。|
| sink.db.name|string| 目标数据库名称。|
| sink.supertable.name|string |写入的超级表名称。|
| sink.batch.size | integer | 写入的批大小。|
| sink.table.name|string|写入的普通表或子表名称。|


使用示例：

将 power 库的 meters 表的子表数据，写入 power_sink 库的 sink_meters 超级表对应的子表中。

<details>
<summary>Table Source</summary>
```java
{{#include docs/examples/flink/Main.java:source_table}}
```
</details>

#### Table CDC 连接器 

参数配置说明：

|         参数名称          |  类型   | 参数说明      |
| ----------------------- | :-----: | ------------ |
| connector  | string | 连接器标识，设置 `tdengine-connector`。|
| user| string | 用户名， 默认 root。|
| password | string | 密码， 默认taosdata。| 
| bootstrap.servers| string | 服务器地址。| 
| topic | string | 订阅主题。||
| td.jdbc.mode | strng | 连接器类型, cdc, sink。| 
| group.id| string| 消费组 ID，同一消费组共享消费进度。 | 
| auto.offset.reset| string| 消费组订阅的初始位置。<br/>`earliest`: 从头开始订阅 <br/> `latest`: 仅从最新数据开始订阅。<br/> 默认 `latest`。|
| poll.interval_ms| integer| 拉取数据间隔, 默认 500ms。|
| sink.db.name|string| 目标数据库名称。|
| sink.supertable.name|string |写入的超级表名称。|
| sink.batch.size | integer | 写入的批大小。|
| sink.table.name|string|写入的普通表或子表名称。|

使用示例：

订阅 power 库的 meters 超级表的子表数据，写入 power_sink 库的 sink_meters 超级表对应的子表中。

<details>
<summary>Table CDC</summary>
```java
{{#include docs/examples/flink/Main.java:cdc_table}}
```
</details>
