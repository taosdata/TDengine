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

| Flink Connector Version | Major Changes                                                | TDengine Version    |
| ----------------------- | ------------------------------------------------------------ | ------------------- |
| 2.1.3                   | Add exception information output for data conversion. | - |
| 2.1.2                   | Add backtick filtering for written fields. | - |
| 2.1.1                   | Fix the issue of data binding failure for the same table in Stmt. | - |
| 2.1.0                   | Fix the issue of writing varchar types from different data sources. | - |
| 2.0.2                   | The Table Sink supports types such as RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, and RowKind.DELETE. | -  |
| 2.0.1                   | Sink supports writing types from Rowdata implementations.    | - |
| 2.0.0                   | 1.Sink supports custom data structure serialization and writing to TDengine.<br/> 2. Supports writing to TDengine database using Table SQL. | 3.3.5.1  and higher |
| 1.0.0                   | Support Sink function to write data from other sources to TDengine in the future. | 3.3.2.0 and higher  |

## Exception and error codes

After the task execution fails, check the Flink task execution log to confirm the reason for the failure
Please refer to:

| Error Code | Description                                                  | Suggested Actions                                            |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0xa000     | connection param error                                       | Connector parameter error.                                   |
| 0xa010     | database name configuration error                            | database name configuration error.                           |
| 0xa011     | table name configuration error                               | Table name configuration error.                              |
| 0xa013     | value.deserializer parameter not set                         | No serialization method set.                                 |
| 0xa014     | list of column names set incorrectly                         | List of column names for target table not set.               |
| 0x2301     | connection already closed                                    | The connection has been closed. Check the connection status or create a new connection to execute the relevant instructions. |
| 0x2302     | this operation is NOT supported currently                    | The current interface is not supported, you can switch to other connection methods. |
| 0x2303     | invalid variables                                            | The parameter is invalid. Please check the corresponding interface specification and adjust the parameter type and size. |
| 0x2304     | statement is closed                                          | Statement has already been closed. Please check if the statement is closed and reused, or if the connection is working properly. |
| 0x2305     | resultSet is closed                                          | The ResultSet has been released. Please check if the ResultSet has been released and used again. |
| 0x230d     | parameter index out of range                                 | parameter out of range, please check the reasonable range of the parameter. |
| 0x230e     | connection already closed                                    | The connection has been closed. Please check if the connection is closed and used again, or if the connection is working properly. |
| 0x230f     | unknown SQL type in TDengine                                 | Please check the Data Type types supported by TDengine.      |
| 0x2315     | unknown tao type in TDengine                                 | Did the correct TDengine data type be specified when converting TDengine data type to JDBC data type. |
| 0x2319     | user is required                                             | Username information is missing when creating a connection.  |
| 0x231a     | password is required                                         | Password information is missing when creating a connection.  |
| 0x231d     | can't create connection with server within                   | Increase connection time by adding the parameter httpConnectTimeout, or check the connection status with taosAdapter. |
| 0x231e     | failed to complete the task within the specified time        | Increase execution time by adding the parameter messageWaitTimeout, or check the connection with taosAdapter. |
| 0x2352     | unsupported encoding                                         | An unsupported character encoding set was specified under the local connection. |
| 0x2353     | internal error of database,  Please see taoslog for more details | An error occurred while executing prepareStatement on the local connection. Please check the taoslog for problem localization. |
| 0x2354     | connection is NULL                                           | Connection has already been closed while executing the command on the local connection. Please check the connection with TDengine. |
| 0x2355     | result set is NULL                                           | Local connection to obtain result set, result set exception, please check connection status and retry. |
| 0x2356     | invalid num of fields                                        | The meta information obtained from the local connection result set does not match. |

## Data type mapping

TDengine currently supports timestamp, number, character, and boolean types, and the corresponding type conversions with Flink RowData Type are as follows:

| TDengine DataType | Flink RowDataType |
| ----------------- | ----------------- |
| TIMESTAMP         | TimestampData     |
| INT               | Integer           |
| BIGINT            | Long              |
| FLOAT             | Float             |
| DOUBLE            | Double            |
| SMALLINT          | Short             |
| TINYINT           | Byte              |
| BOOL              | Boolean           |
| VARCHAR           | StringData        |
| BINARY            | StringData        |
| NCHAR             | StringData        |
| JSON              | StringData        |
| VARBINARY         | byte[]            |
| GEOMETRY          | byte[]            |

## Instructions for use

### Flink Semantic Selection Instructions

The semantic reason for using `At-Least-Once` is:

- TDengine currently does not support transactions and cannot perform frequent checkpoint operations and complex transaction coordination.
- Due to TDengine's use of timestamps as primary keys, downstream operators of duplicate data can perform filtering operations to avoid duplicate calculations.
- Using `At-Least-Once` to ensure high data processing performance and low data latency, the setting method is as follows:

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
    <version>2.1.3</version>
</dependency>
```

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

### Sink

The core function of Sink is to efficiently and accurately write Flink processed data from different data sources or operators into TDengine. In this process, the efficient write mechanism possessed by TDengine played a crucial role, effectively ensuring the fast and stable storage of data.

:::note

- The database to be written into must have been created already.
- The super table/ordinary table to be written into must have been created already.

:::

Sink Properties

- TDengineConfigParams.PROPERTY_KEY_USER: Login to TDengine username, default value is 'root '.
- TDengineConfigParams.PROPERTY_KEY_PASSWORD: User login password, default value 'taosdata'.
- TDengineConfigParams.PROPERTY_KEY_DBNAME: The database name.
- TDengineConfigParams.TD_SUPERTABLE_NAME:The name of the super table. The received data must have a tbname field to determine which sub table to write to.
- TDengineConfigParams.TD_TABLE_NAME: The table name of a sub table or a normal table. This parameter only needs to be set together with `TD_SUPERTABLE_NAME`.
- TDengineConfigParams.VALUE_DESERIALIZER: The deserialization method for receiving result sets. If the type of the received result set is `RowData` of Flink, it only needs to be set to `RowData`. It is also possible to inherit [TDengineSinkRecordSerializer](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/main/java/com/taosdata/flink/sink/serializer/TDengineSinkRecordSerializer.java) and implement the `serialize` method, customizing the deserialization method based on the received data type.
- TDengineConfigParams.TD_BATCH_SIZE: Set the batch size for writing to the TDengine database once | Writing will be triggered when the number of batches is reached, or when a checkpoint is set.
- TDengineConfigParams.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: Message timeout time, in milliseconds, default value is 60000.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_COMPRESSION: Is compression enabled during the transmission process. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: Whether to enable automatic reconnection. true:  Enable, false:  Not enabled. The default is false.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_INTERVAL_MS: Automatic reconnection retry interval, in milliseconds, default value 2000. It only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineConfigParams.PROPERTY_KEY_RECONNECT_RETRY_COUNT: The default value for automatic reconnection retry is 3, which only takes effect when `PROPERTY_KEY_ENABLE_AUTO_RECONNECT` is true.
- TDengineConfigParams.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION: Turn off SSL certificate verification. true:  Enable, false:  Not enabled. The default is false.

Usage example:

Write the data of the RowData type into the sub-table corresponding to the super table `sink_meters` in the `power_sink` database.

<details>
<summary>RowData Into Super Table</summary>
```java
{{#include docs/examples/flink/Main.java:RowDataToSuperTable}}
```
</details>

Usage example:

Write the data of the `RowData` type into the `sink_normal` table in the `power_sink` database.

<details>
<summary>RowData Into Normal Table</summary>
```java
{{#include docs/examples/flink/Main.java:RowDataToNormalTable}}
```
</details>

Usage example:

Write the data of the custom type into the sub-tables corresponding to the super table `sink_meters` in the `power_sink` database.

<details>
<summary>CustomType Into Super Table</summary>
```java
{{#include docs/examples/flink/Main.java:CustomTypeToNormalTable}}
```
</details>

:::note

- [ResultBean](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/test/java/com/taosdata/flink/entity/ResultBean.java)  is a custom inner class used to define the data type of the Source query results.
- [ResultBeanSinkSerializer](https://github.com/taosdata/flink-connector-tdengine/blob/main/src/test/java/com/taosdata/flink/entity/ResultBeanSinkSerializer.java) is a custom inner class that inherits TDengine RecordDesrialization and implements convert and getProducedType methods.

:::

### Table Sink

Extract data from multiple different data source databases (such as MySQL, Oracle, Kafka etc.) using Flink Table, perform custom operator operations (such as data cleaning, format conversion, associating data from different tables, etc.), and then write the processed results into the TDengine.

#### Source connector

Parameter configuration instructions:

| Parameter Name       |  Type   | Parameter Description                           |
| -------------------- | :-----: | ----------------------------------------------- |
| connector            | string  | connector identifier, set `tdengine-connector`  |
| td.jdbc.url          | string  | url of the connection                           |
| td.jdbc.mode         | string  | connector type `sink`                           |
| sink.db.name         | string  | target database name                            |
| sink.batch.size      | integer | batch size written                              |
| sink.supertable.name | string  | name of the supertable                          |
| sink.table.name      | string  | the table name of a sub table or a normal table |

Usage example:

Write data into the sub-tables corresponding to the super table `sink_meters` in the `power_sink` database via SQL statements.

<details>
<summary>Table SQL Into Super Table </summary>
```java
{{#include docs/examples/flink/Main.java:TableSqlToSink}}
```
</details>

Usage example:

Write data into the `sink_normal` table in the `power_sink` database via SQL statements.

<details>
<summary>Table SQL Into Normal Table </summary>
```java
{{#include docs/examples/flink/Main.java:NormalTableSqlToSink}}
```
</details>

Usage example:

Write data of the `Row` type into the sub-tables corresponding to the super table `sink_meters` in the `power_sink` database.

<details>
<summary>Table Row To Sink </summary>
```java
{{#include docs/examples/flink/Main.java:TableRowToSink}}
```
</details>
