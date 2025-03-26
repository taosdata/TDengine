---
sidebar_label: Flink
title: TDengine Flink Connector
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Apache Flink is an open-source distributed stream batch integrated processing framework supported by the Apache Software Foundation, which can be used for many big data processing scenarios such as stream processing, batch processing, complex event processing, real-time data warehouse construction, and providing real-time data support for machine learning. At the same time, Flink has a wealth of connectors and various tools that can interface with numerous different types of data sources to achieve data reading and writing. In the process of data processing, Flink also provides a series of reliable fault-tolerant mechanisms, effectively ensuring that tasks can run stably and continuously even in the event of unexpected situations.

With the help of TDengine's Flink connector, Apache Flink can seamlessly integrate with the TDengine database. On the one hand, it can accurately store the results obtained after complex calculations and deep analysis into the TDengine database, achieving efficient storage and management of data; On the other hand, it is also possible to quickly and stably read massive amounts of data from the TDengine database, and conduct comprehensive and in-depth analysis and processing on this basis, fully tapping into the potential value of the data, providing strong data support and scientific basis for enterprise decision-making, greatly improving the efficiency and quality of data processing, and enhancing the competitiveness and innovation ability of enterprises in the digital age.

## Prerequisites

Prepare the following environment:

- TDengine cluster has been deployed and is running normally (both enterprise and community versions are available)
- TaosAdapter can run normally. 
- Apache Flink v1.19.0 or above is installed. Please refer to the installation of Apache Flink [Official documents](https://flink.apache.org/)

## Supported platforms

Flink Connector supports all platforms that can run Flink 1.19 and above versions.

## Version History

| Flink Connector Version | Major Changes | TDengine Version|
|-------------------------| ------------------------------------ | ---------------- |
| 2.1.0                   | Fix the issue of writing varchar types from different data sources.| - |
| 2.0.2                   | The Table Sink supports types such as RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, and RowKind.DELETE.| - |
| 2.0.1                   | Sink supports writing types from Rowdata implementations.| - |
| 2.0.0                   | 1.Support SQL queries on data in TDengine database. <br/> 2. Support CDC subscription to data in TDengine database.<br/> 3. Supports reading and writing to TDengine database using Table SQL. | 3.3.5.1  and higher|
| 1.0.0                   | Support Sink function to write data from other sources to TDengine in the future.| 3.3.2.0 and higher|

## Exception and error codes

After the task execution fails, check the Flink task execution log to confirm the reason for the failure
Please refer to:

| Error Code       | Description                                              | Suggested Actions    |
| ---------------- |-------------------------------------------------------   | -------------------- |
|0xa000 | connection param error | Connector parameter error.
|0xa001 | the groupid parameter of CDC is incorrect | The groupid parameter of CDC is incorrect.|
|0xa002 | wrong topic parameter for CDC | The topic parameter for CDC is incorrect.|
|0xa010 | database name configuration error | database name configuration error.|
|0xa011 | table name configuration error | Table name configuration error.|
|0xa012 | no data was obtained from the data source | Failed to retrieve data from the data source.|
|0xa013 | value.deserializer parameter not set | No serialization method set.|
|0xa014 | list of column names set incorrectly | List of column names for target table not set. |
|0x2301 | connection already closed | The connection has been closed. Check the connection status or create a new connection to execute the relevant instructions.|
|0x2302 | this operation is NOT supported currently  | The current interface is not supported, you can switch to other connection methods.|
|0x2303 | invalid variables | The parameter is invalid. Please check the corresponding interface specification and adjust the parameter type and size.|
|0x2304 | statement is closed | Statement has already been closed. Please check if the statement is closed and reused, or if the connection is working properly.|
|0x2305 | resultSet is closed | The ResultSet has been released. Please check if the ResultSet has been released and used again.|
|0x230d | parameter index out of range | parameter out of range, please check the reasonable range of the parameter.|
|0x230e | connection already closed | The connection has been closed. Please check if the connection is closed and used again, or if the connection is working properly.|
|0x230f | unknown SQL type in TDengine | Please check the Data Type types supported by TDengine.|
|0x2315 | unknown tao type in TDengine | Did the correct TDengine data type be specified when converting TDengine data type to JDBC data type.|
|0x2319 | user is required | Username information is missing when creating a connection.|
|0x231a | password is required | Password information is missing when creating a connection.|
|0x231d | can't create connection with server within | Increase connection time by adding the parameter httpConnectTimeout, or check the connection status with taosAdapter.|
|0x231e | failed to complete the task within the specified time | Increase execution time by adding the parameter messageWaitTimeout, or check the connection with taosAdapter.|
|0x2352 | unsupported encoding | An unsupported character encoding set was specified under the local connection.|
|0x2353 | internal error of database,  Please see taoslog for more details | An error occurred while executing prepareStatement on the local connection. Please check the taoslog for problem localization.|
|0x2354 | connection is NULL | Connection has already been closed while executing the command on the local connection. Please check the connection with TDengine.|
|0x2355 | result set is NULL | Local connection to obtain result set, result set exception, please check connection status and retry.|
|0x2356 | invalid num of fields | The meta information obtained from the local connection result set does not match.|
|0x2357 | empty SQL string | Fill in the correct SQL for execution.|
|0x2371 | consumer properties must not be null | When creating a subscription, the parameter is empty. Please fill in the correct parameter.|
|0x2375 | topic reference has been destroyed | During the process of creating a data subscription, the topic reference was released. Please check the connection with TDengine.|
|0x2376 | failed to set consumer topic,  Topic name is empty | During the process of creating a data subscription, the subscription topic name is empty. Please check if the specified topic name is filled in correctly.|
|0x2377 | consumer reference has been destroyed | The subscription data transmission channel has been closed, please check the connection with TDengine.|
|0x2378 | consumer create error | Failed to create data subscription. Please check the taos log based on the error message to locate the problem.|
|0x237a | vGroup not found in result set VGroup | Not assigned to the current consumer, due to the Rebalance mechanism, the relationship between Consumer and VGroup is not bound.|

## Data type mapping

TDengine currently supports timestamp, number, character, and boolean types, and the corresponding type conversions with Flink RowData Type are as follows:

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
| VARCHAR           | StringData    |
| BINARY            | StringData    |
| NCHAR             | StringData    |
| JSON              | StringData    |
| VARBINARY         | byte[]        |
| GEOMETRY          | byte[]        |

## Instructions for use

### Flink Semantic Selection Instructions

The semantic reason for using At Least One (at least once) is:

- TDengine currently does not support transactions and cannot perform frequent checkpoint operations and complex transaction coordination.
- Due to TDengine's use of timestamps as primary keys, downstream operators of duplicate data can perform filtering operations to avoid duplicate calculations.
- Using At Least One (at least once) to ensure high data processing performance and low data latency, the setting method is as follows:

Instructions:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
```

If using Maven to manage a project, simply add the following dependencies in pom.xml.

```xml
<dependency>
    <groupId>com.taosdata.flink</groupId>
    <artifactId>flink-connector-tdengine</artifactId>
    <version>2.1.0</version>
</dependency>
```

The parameters for establishing a connection include URL and Properties.
The URL specification format is:

`jdbc: TAOS-WS://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&timezone={timezone}]`  

Parameter description:

- User: Login TDengine username, default value is' root '.
- Password: User login password, default value 'taosdata'.
- database_name: database name。
- timezone: time zone。
- HttpConnectTimeout: The connection timeout time, measured in milliseconds, with a default value of 60000.
- MessageWaitTimeout: The timeout period for a message, measured in milliseconds, with a default value of 60000.
- UseSSL: Whether SSL is used in the connection.

### Source

Source retrieves data from the TDengine database, converts it into a format and type that Flink can handle internally, and reads and distributes it in parallel, providing efficient input for subsequent data processing.
By setting the parallelism of the data source, multiple threads can read data from the data source in parallel, improving the efficiency and throughput of data reading, and fully utilizing cluster resources for large-scale data processing capabilities.

#### Source Properties

The configuration parameters in Properties are as follows:

- TDengineConfigParams.PROPERTY_KEY_USER: Login to TDengine username, default value is 'root '.
- TDengineConfigParams.PROPERTY_KEY_PASSWORD: User login password, default value 'taosdata'.
- TDengineConfigParams.VALUE_DESERIALIZER: The downstream operator receives the result set deserialization method. If the received result set type is `RowData` of `Flink`, it only needs to be set to `RowData`. It is also possible to inherit `TDengineRecordDeserialization` and implement `convert` and `getProducedType` methods, customizing the deserialization method based on `ResultSet` of `SQL`.
- TDengineConfigParams.TD_BATCH_MODE: This parameter is used to batch push data to downstream operators. If set to True, when creating the `TDengine Source` object, it is necessary to specify the data type as a `Template` form of the `SourceRecords` type.
- TDengineConfigParams.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: Message timeout time, in milliseconds, default value is 60000.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: Is compression enabled during the transmission process. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: Whether to enable automatic reconnection. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: Automatic reconnection retry interval, in milliseconds, default value 2000. It only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: The default value for automatic reconnection retry is 3, which only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineConfigParams.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION: Turn off SSL certificate verification. true:  Enable, false:  Not enabled. The default is false.


#### Split by time

Users can split the SQL query into multiple subtasks based on time, entering: start time, end time, split interval, time field name. The system will split and obtain data in parallel according to the set interval (time left closed and right open).

```java
{{#include docs/examples/flink/Main.java:time_interval}}
```

Splitting by Super Table TAG

Users can split the query SQL into multiple query conditions based on the TAG field of the super table, and the system will split them into subtasks corresponding to each query condition, thereby obtaining data in parallel.

```java
{{#include docs/examples/flink/Main.java:tag_split}}
```

Classify by table

Support sharding by inputting multiple super tables or regular tables with the same table structure. The system will split them according to the method of one table, one task, and then obtain data in parallel.

```java
{{#include docs/examples/flink/Main.java:table_split}}
```

Use Source connector

The query result is RowData data type example:

<details>
<summary>RowData Source</summary>
```java
{{#include docs/examples/flink/Main.java:source_test}}
```
</details>

Example of batch query results:

<details>
<summary>Batch Source</summary>
```java
{{#include docs/examples/flink/Main.java:source_batch_test}}
```
</details> 

Example of custom data type query result:

<details>
<summary>Custom Type Source</summary>
```java
{{#include docs/examples/flink/Main.java:source_custom_type_test}}
```
</details> 

- ResultBean is a custom inner class used to define the data type of the Source query results.
- ResultSoureDeserialization is a custom inner class that inherits `TDengine` RecordDesrialization and implements convert and getProducedType methods.

### CDC Data Subscription

Flink CDC is mainly used to provide data subscription functionality, which can monitor real-time changes in TDengine database data and transmit these changes in the form of data streams to Flink for processing, while ensuring data consistency and integrity.

Parameter Description

- TDengineCdcParams.BOOTSTRAP_SERVERS: `ip:port` of the TDengine server, if using WebSocket connection, then it is the `ip:port` where taosAdapter is located.
- TDengineCdcParams.CONNECT_USER: Login to TDengine username, default value is 'root '.
- TDengineCdcParams.CONNECT_PASS: User login password, default value 'taosdata'.
- TDengineCdcParams.POLL_INTERVAL_MS: Pull data interval, default 500ms.
- TDengineCdcParams. VALUE_DESERIALIZER: Result set deserialization method, If the received result set type is `RowData` of `Flink`, simply set it to 'RowData'. You can inherit `com.taosdata.jdbc.tmq.ReferenceDeserializer`, specify the result set bean, and implement deserialization. You can also inherit `com.taosdata.jdbc.tmq.Deserializer` and customize the deserialization method based on the SQL resultSet.
- TDengineCdcParams.TMQ_BATCH_MODE: This parameter is used to batch push data to downstream operators. If set to True, when creating the `TDengineCdcSource` object, it is necessary to specify the data type as a template form of the `ConsumerRecords` type.
- TDengineCdcParams.GROUP_ID: Consumer group ID, the same consumer group shares consumption progress。Maximum length: 192.
- TDengineCdcParams.AUTO_OFFSET_RESET: Initial position of the consumer group subscription （ `earliest` subscribe from the beginning, `latest` subscribe from the latest data, default `latest`）。
- TDengineCdcParams.ENABLE_AUTO_COMMIT: Whether to enable automatic consumption point submission，true: automatic submission；false：submit based on the `checkpoint` time, default to false.
> **Note**：The automatic submission mode of the reader automatically submits data after obtaining it, regardless of whether the downstream operator has processed the data correctly. There is a risk of data loss, and it is mainly used for efficient stateless operator scenarios or scenarios with low data consistency requirements.

- TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS: Time interval for automatically submitting consumption records, in milliseconds, default 5000. This parameter takes effect when `ENABLE_AUTO_COMMIT` is set to true.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: Is compression enabled during the transmission process. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: Whether to enable automatic reconnection. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: Automatic reconnection retry interval, in milliseconds, default value 2000. It only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: The default value for automatic reconnection retry is 3, which only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineCdcParams.TMQ_SESSION_TIMEOUT_MS: Timeout after consumer heartbeat is lost, after which rebalance logic is triggered, and upon success, that consumer will be removed (supported from version 3.3.3.0)，Default is 12000, range [6000, 1800000].
- TDengineCdcParams.TMQ_MAX_POLL_INTERVAL_MS: The longest time interval for consumer poll data fetching, exceeding this time will be considered as the consumer being offline, triggering rebalance logic, and upon success, that consumer will be removed (supported from version 3.3.3.0) Default is 300000, range [1000, INT32_MAX].

#### Use CDC connector

The CDC connector will create consumers based on the parallelism set by the user, so the user should set the parallelism reasonably according to the resource situation.
The subscription result is RowData data type example:

<details>
<summary>CDC Source</summary>
```java
{{#include docs/examples/flink/Main.java:cdc_source}}
```
</details> 

Example of batch query results:

<details>
<summary>CDC Batch Source</summary>
```java
{{#include docs/examples/flink/Main.java:cdc_batch_source}}
```
</details> 

Example of custom data type query result:

<details>
<summary>CDC Custom Type</summary>
```java
{{#include docs/examples/flink/Main.java:cdc_custom_type_test}}
```
</details> 

- ResultBean is a custom inner class whose field names and data types correspond one-to-one with column names and data types. This allows the deserialization class corresponding to the value.ddeserializer property to deserialize objects of ResultBean type.

### Sink

The core function of Sink is to efficiently and accurately write Flink processed data from different data sources or operators into TDengine. In this process, the efficient write mechanism possessed by TDengine played a crucial role, effectively ensuring the fast and stable storage of data.

Sink Properties

- TDengineConfigParams.PROPERTY_KEY_USER: Login to TDengine username, default value is 'root '.
- TDengineConfigParams.PROPERTY_KEY_PASSWORD: User login password, default value 'taosdata'.
- TDengineConfigParams.PROPERTY_KEY_DBNAME: The database name.
- TDengineConfigParams.TD_SUPERTABLE_NAME:The name of the super table. The received data must have a tbname field to determine which sub table to write to.
- TDengineConfigParams.TD_TABLE_NAME: The table name of a sub table or a normal table. This parameter only needs to be set together with `TD_SUPERTABLE_NAME`.
- TDengineConfigParams.VALUE_DESERIALIZER: The deserialization method for receiving result sets. If the type of the received result set is RowData of Flink, it only needs to be set to RowData. It is also possible to inherit 'TDengine SinkRecordSequencer' and implement the 'serialize' method, customizing the deserialization method based on the received data type.
- TDengineConfigParams.TD_BATCH_SIZE: Set the batch size for writing to the `TDengine` database once | Writing will be triggered when the number of batches is reached, or when a checkpoint is set.
- TDengineConfigParams.TD_BATCH_MODE: When set to True for receiving batch data, if the data source is `TDengine Source` , use the `SourceRecords Template` type to create a `TDengineSink` object; If the source is `TDengine CDC`, use the `ConsumerRecords Template` to create a `TDengineSink` object.
- TDengineConfigParams.TD_SOURCE_TYPE: Set the data source. When the data source is `TDengine Source`, it is set to 'tdengine_stource', and when the source is `TDengine CDC`, it is set to 'tdengine_cdc'. When the configuration of `TD_BATCH_MODE` is set to True, it takes effect.
- TDengineConfigParams.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: Message timeout time, in milliseconds, default value is 60000.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: Is compression enabled during the transmission process. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: Whether to enable automatic reconnection. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: Automatic reconnection retry interval, in milliseconds, default value 2000. It only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: The default value for automatic reconnection retry is 3, which only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineConfigParams.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION: Turn off SSL certificate verification. true:  Enable, false:  Not enabled. The default is false.

Usage example:

Write the sub table data of the meters table in the power database into the corresponding sub table of the sink_meters super table in the power_stink database.

<details>
<summary>Sink RowData</summary>
```java
{{#include docs/examples/flink/Main.java:RowDataToSink}}
```
</details> 

Usage example:

Subscribe to the sub table data of the meters super table in the power database and write it to the corresponding sub table of the sink_meters super table in the power_stink database.

<details>
<summary>Cdc Sink</summary>
```java
{{#include docs/examples/flink/Main.java:CdcRowDataToSink}}
```
</details>

### Table SQL

Extract data from multiple different data source databases (such as TDengine, MySQL, Oracle, etc.) using Table SQL, perform custom operator operations (such as data cleaning, format conversion, associating data from different tables, etc.), and then load the processed results into the target data source (such as TDengine, MySQL, etc.).

#### Source connector

Parameter configuration instructions:

| Parameter Name        | Type | Parameter Description |
|-----------------------| :-----: | ------------ |
| connector             | string | connector identifier, set `tdengine-connector`|
| td.jdbc.url           | string | url of the connection |
| td.jdbc.mode          | strng  | connector type: `source`, `sink`|
| table.name            | string | original or target table name |
| scan.query            | string | SQL statement to retrieve data|
| sink.db.name          | string | target database name|
| sink.supertable.name  | string | name of the supertable|
| sink.batch.size       | integer| batch size written|
| sink.table.name       | string | the table name of a sub table or a normal table |

Usage example:

Write the sub table data of the meters table in the power database into the corresponding sub table of the sink_meters super table in the power_stink database.

<details>
<summary>Table Source</summary>
```java
{{#include docs/examples/flink/Main.java:source_table}}
```
</details>

#### Table CDC connector

Parameter configuration instructions:

| Parameter Name    | Type | Parameter Description                                                                |
|-------------------| :-----: |--------------------------------------------------------------------------------------|
| connector         | string | connector identifier, set `tdengine-connector`                                       |
| user              | string | username, default root                                                               |
| password          | string | password, default taosdata                                                           |
| bootstrap. servers| string | server address                                                                       |
| topic             | string | subscribe to topic                                                                   |
| td.jdbc.mode      | strng  | connector type: `cdc`, `sink`                                                        |
| group.id          | string | consumption group ID, sharing consumption progress within the same consumption group |
| auto.offset.reset | string | initial position for consumer group subscription. <br/> `earliest`: subscribe from the beginning <br/> `latest` subscribe from the latest data <br/>default `latest`|
| poll.interval_mas | integer | pull data interval, default 500ms                                                   |
| sink.db.name      | string | target database name                                                                 |
| sink.supertable.name | string | name of the supertable                                                |
| sink.batch.size   | integer | batch size written                                                                  |
| sink.table.name   | string | the table name of a sub table or a normal table                                       |

Usage example:

Subscribe to the sub table data of the meters super table in the power database and write it to the corresponding sub table of the sink_meters super table in the power_stink database.

<details>
<summary>Table CDC</summary>
```java
{{#include docs/examples/flink/Main.java:cdc_table}}
```
</details>

