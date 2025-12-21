---
sidebar_label: Flink 
title: TDengine Flink Connector
toc_max_heading_level: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import FlinkCommonInfo from '../../assets/resources/_flink-common-info.mdx'

Apache Flink is an open-source distributed stream batch integrated processing framework supported by the Apache Software Foundation, which can be used for many big data processing scenarios such as stream processing, batch processing, complex event processing, real-time data warehouse construction, and providing real-time data support for machine learning. At the same time, Flink has a wealth of connectors and various tools that can interface with numerous different types of data sources to achieve data reading and writing. In the process of data processing, Flink also provides a series of reliable fault-tolerant mechanisms, effectively ensuring that tasks can run stably and continuously even in the event of unexpected situations.

With the help of TDengine's Flink connector, Apache Flink can seamlessly integrate with TDengine Database. It enables efficient and stable reading of massive volumes of data from TDengine Database, based on which comprehensive and in-depth data analysis and processing can be conducted. This fully taps into the potential value of data, providing robust data support and scientific basis for enterprise decision-making, significantly improving the efficiency and quality of data processing, and enhancing enterprises' competitiveness and innovation capabilities in the digital era.

###### Note

This feature is only available in TDengine Enterprise Edition.

## Prerequisites

Prepare the following environment:

- TDengine cluster has been deployed and is running normally (both enterprise and community versions are available)
- TaosAdapter can run normally.
- Apache Flink v1.19.0 or above is installed. Please refer to the installation of Apache Flink [Official documents](https://flink.apache.org/)

## Supported platforms

Flink Connector supports all platforms that can run Flink 1.19 and above versions.

<FlinkCommonInfo />

### Connection Parameters

The parameters for establishing a connection include URL and Properties.
The URL specification format is:

`jdbc: TAOS-WS://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&timezone={timezone}]`  

Parameter description:

- User: Login TDengine username, default value is' root '.
- Password: User login password, default value 'taosdata'.
- database_name: database name.
- timezone: time zone.
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
{{#include docs/examples/flink/source/Main.java:time_interval}}
```

Splitting by Super Table TAG

Users can split the query SQL into multiple query conditions based on the TAG field of the super table, and the system will split them into subtasks corresponding to each query condition, thereby obtaining data in parallel.

```java
{{#include docs/examples/flink/source/Main.java:tag_split}}
```

Classify by table

Support sharding by inputting multiple super tables or regular tables with the same table structure. The system will split them according to the method of one table, one task, and then obtain data in parallel.

```java
{{#include docs/examples/flink/source/Main.java:table_split}}
```

Use Source connector

The query result is RowData data type example:

<details>
<summary>RowData Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:source_test}}
```
</details>

Example of batch query results:

<details>
<summary>Batch Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:source_batch_test}}
```
</details>

Example of custom data type query result:

<details>
<summary>Custom Type Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:source_custom_type_test}}
```
</details>

- ResultBean is a custom inner class used to define the data type of the Source query results.
- ResultSourceDeserialization is a custom inner class that inherits `TDengine` RecordDesrialization and implements convert and getProducedType methods.

### CDC Data Subscription

Flink CDC is mainly used to provide data subscription functionality, which can monitor real-time changes in TDengine database data and transmit these changes in the form of data streams to Flink for processing, while ensuring data consistency and integrity.

Parameter Description

- TDengineCdcParams.BOOTSTRAP_SERVERS: `ip:port` of the TDengine server, if using WebSocket connection, then it is the `ip:port` where taosAdapter is located.
- TDengineCdcParams.CONNECT_USER: Login to TDengine username, default value is 'root '.
- TDengineCdcParams.CONNECT_PASS: User login password, default value 'taosdata'.
- TDengineCdcParams.POLL_INTERVAL_MS: Pull data interval, default 500ms.
- TDengineCdcParams. VALUE_DESERIALIZER: Result set deserialization method, If the received result set type is `RowData` of `Flink`, simply set it to 'RowData'. You can inherit `com.taosdata.jdbc.tmq.ReferenceDeserializer`, specify the result set bean, and implement deserialization. You can also inherit `com.taosdata.jdbc.tmq.Deserializer` and customize the deserialization method based on the SQL resultSet.
- TDengineCdcParams.TMQ_BATCH_MODE: This parameter is used to batch push data to downstream operators. If set to True, when creating the `TDengineCdcSource` object, it is necessary to specify the data type as a template form of the `ConsumerRecords` type.
- TDengineCdcParams.GROUP_ID: Consumer group ID, the same consumer group shares consumption progress. Maximum length: 192.
- TDengineCdcParams.AUTO_OFFSET_RESET: Initial position of the consumer group subscription ( `earliest` subscribe from the beginning, `latest` subscribe from the latest data, default `latest`）。
- TDengineCdcParams.ENABLE_AUTO_COMMIT: Whether to enable automatic consumption point submission, true: automatic submission. false: submit based on the `checkpoint` time, default to false.

> **Note**：The automatic submission mode of the reader automatically submits data after obtaining it, regardless of whether the downstream operator has processed the data correctly. There is a risk of data loss, and it is mainly used for efficient stateless operator scenarios or scenarios with low data consistency requirements.

- TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS: Time interval for automatically submitting consumption records, in milliseconds, default 5000. This parameter takes effect when `ENABLE_AUTO_COMMIT` is set to true.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: Is compression enabled during the transmission process. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: Whether to enable automatic reconnection. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: Automatic reconnection retry interval, in milliseconds, default value 2000. It only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: The default value for automatic reconnection retry is 3, which only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineCdcParams.TMQ_SESSION_TIMEOUT_MS: Timeout after consumer heartbeat is lost, after which rebalance logic is triggered, and upon success, that consumer will be removed (supported from version 3.3.3.0), Default is 12000, range [6000, 1800000].
- TDengineCdcParams.TMQ_MAX_POLL_INTERVAL_MS: The longest time interval for consumer poll data fetching, exceeding this time will be considered as the consumer being offline, triggering rebalance logic, and upon success, that consumer will be removed (supported from version 3.3.3.0) Default is 300000, range [1000, INT32_MAX].

#### Use CDC connector

The CDC connector will create consumers based on the parallelism set by the user, so the user should set the parallelism reasonably according to the resource situation.
The subscription result is RowData data type example:

<details>
<summary>CDC Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:cdc_source}}
```
</details>

Example of batch query results:

<details>
<summary>CDC Batch Source</summary>
```java
{{#include docs/examples/flink/source/Main.java:cdc_batch_source}}
```
</details>

Example of custom data type query result:

<details>
<summary>CDC Custom Type</summary>
```java
{{#include docs/examples/flink/source/Main.java:cdc_custom_type_test}}
```
</details>

- ResultBean is a custom inner class whose field names and data types correspond one-to-one with column names and data types. This allows the deserialization class corresponding to the value.ddeserializer property to deserialize objects of ResultBean type.

### Table SQL

Extract data from multiple different data source databases (such as TDengine, MySQL, Oracle, etc.) using Table SQL, perform custom operator operations (such as data cleaning, format conversion, associating data from different tables, etc.), and then load the processed results into the target data source (such as TDengine, MySQL, etc.).

#### Source connector

Parameter configuration instructions:

| Parameter Name        | Type | Parameter Description |
|-----------------------| :-----: | ------------ |
| connector             | string | connector identifier, set `tdengine-connector`|
| td.jdbc.url           | string | url of the connection |
| td.jdbc.mode          | string  | connector type: `source`, `sink`|
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
{{#include docs/examples/flink/source/Main.java:source_table}}
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
| td.jdbc.mode      | string  | connector type: `cdc`, `sink`                                                        |
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
{{#include docs/examples/flink/source/Main.java:cdc_table}}
```
</details>
