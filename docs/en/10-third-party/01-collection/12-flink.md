---
sidebar_label: Flink
title: TDengine Flink Connector
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import FlinkCommonInfo from '../../assets/resources/_flink-common-info.mdx'

Apache Flink is an open-source distributed stream batch integrated processing framework supported by the Apache Software Foundation, which can be used for many big data processing scenarios such as stream processing, batch processing, complex event processing, real-time data warehouse construction, and providing real-time data support for machine learning. At the same time, Flink has a wealth of connectors and various tools that can interface with numerous different types of data sources to achieve data reading and writing. In the process of data processing, Flink also provides a series of reliable fault-tolerant mechanisms, effectively ensuring that tasks can run stably and continuously even in the event of unexpected situations.

With the help of TDengine's Flink connector, Apache Flink can seamlessly integrate with the TDengine database. On the one hand, it can accurately store the results obtained after complex calculations and deep analysis into the TDengine database, achieving efficient storage and management of data; On the other hand, it is also possible to quickly and stably read massive amounts of data from the TDengine database, and conduct comprehensive and in-depth analysis and processing on this basis, fully tapping into the potential value of the data, providing strong data support and scientific basis for enterprise decision-making, greatly improving the efficiency and quality of data processing, and enhancing the competitiveness and innovation ability of enterprises in the digital age.

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
{{#include docs/examples/flink/sink/Main.java:RowDataToSuperTable}}
```
</details>

Usage example:

Write the data of the `RowData` type into the `sink_normal` table in the `power_sink` database.

<details>
<summary>RowData Into Normal Table</summary>
```java
{{#include docs/examples/flink/sink/Main.java:RowDataToNormalTable}}
```
</details>

Usage example:

Write the data of the custom type into the sub-tables corresponding to the super table `sink_meters` in the `power_sink` database.

<details>
<summary>CustomType Into Super Table</summary>
```java
{{#include docs/examples/flink/sink/Main.java:CustomTypeToNormalTable}}
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
{{#include docs/examples/flink/sink/Main.java:TableSqlToSink}}
```
</details>

Usage example:

Write data into the `sink_normal` table in the `power_sink` database via SQL statements.

<details>
<summary>Table SQL Into Normal Table </summary>
```java
{{#include docs/examples/flink/sink/Main.java:NormalTableSqlToSink}}
```
</details>

Usage example:

Write data of the `Row` type into the sub-tables corresponding to the super table `sink_meters` in the `power_sink` database.

<details>
<summary>Table Row To Sink </summary>
```java
{{#include docs/examples/flink/sink/Main.java:TableRowToSink}}
```
</details>
