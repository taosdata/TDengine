---
title: OPC DA
slug: /advanced-features/data-connectors/opc-da
---

import Image from '@theme/IdealImage';
import imgStep1 from '../../assets/opc-da-01.png';
import imgStep2 from '../../assets/opc-da-02.png';
import imgStep3 from '../../assets/opc-da-03.png';
import imgStep4 from '../../assets/opc-da-04.png';
import imgStep5 from '../../assets/opc-da-05.png';
import imgStep6 from '../../assets/opc-da-06.png';
import imgStep7 from '../../assets/opc-da-07.png';
import imgStep8 from '../../assets/opc-da-08.png';

This section explains how to create a data migration task through the Explorer interface, syncing data from an OPC-DA server to the current TDengine cluster.

## Overview

OPC is one of the interoperability standards for securely and reliably exchanging data in industrial automation and other industries.

OPC DA (Data Access) is a classic COM-based specification that is only applicable to Windows. Although OPC DA is not the most modern or efficient data communication standard, it is widely used because some older devices only support OPC DA.

TDengine can efficiently read data from the OPC-DA server and write it to TDengine to achieve real-time data ingestion.

## Creating a Task

### 1. Add Data Source

On the Data Ingestion page, click the **+Add Data Source** button to go to the Add Data Source page.

<figure>
<Image img={imgStep1} alt=""/>
</figure>

### 2. Configure Basic Information

Enter a task name in the **Name** field, such as for a task monitoring environmental temperature and humidity, name it **environment-monitoring**.

Select **OPC-DA** from the **Type** dropdown list.

If the taosX service is running on the same server as the OPC-DA server, the agent is not required. Otherwise, you need to configure an agent: select a designated agent from the dropdown or click the **+Create New Agent** button to create a new agent, and follow the prompts to configure the agent.

Select a target database from the **Target Database** dropdown list, or click the **+Create Database** button on the right.

<figure>
<Image img={imgStep2} alt=""/>
</figure>

### 3. Configure Connection Information

In the **Connection Configuration** section, fill in the **OPC-DA Server Address**, such as `127.0.0.1/Matrikon.OPC.Simulation.1`, and configure the authentication method.

Click the **Connectivity Check** button to check if the data source is available.

<figure>
<Image img={imgStep3} alt=""/>
</figure>

### 4. Configure Data Points Set

You can choose to use a CSV file template or **Select All Data Points** for the **Data Points Set**.

#### 4.1. Upload CSV Configuration File

You can download an empty CSV template and configure the data point information based on the template, then upload the CSV configuration file to configure data points. Alternatively, download the data points based on the configured filtering conditions in the format specified by the CSV template.

The CSV file must follow these rules:

1. File Encoding

The uploaded CSV file must be encoded in one of the following formats:

(1) UTF-8 with BOM

(2) UTF-8 (i.e., UTF-8 without BOM)

2. Header Configuration Rules

The header is the first row of the CSV file. The rules are as follows:

(1) The following columns can be configured in the CSV header:

| No.  | Column Name             | Description                                                  | Required | Default Behavior                                             |
| ---- | ----------------------- | ------------------------------------------------------------ | -------- | ------------------------------------------------------------ |
| 1    | tag_name                | The id of the data point on the OPC DA server                | Yes      | None                                                         |
| 2    | stable                  | The supertable in TDengine corresponding to the data point   | Yes      | None                                                         |
| 3    | tbname                  | The subtable in TDengine corresponding to the data point     | Yes      | None                                                         |
| 4    | enable                  | Whether to collect data for this point                       | No       | Uses a default value of `1` as the enable value              |
| 5    | value_col               | The column name in TDengine where the collected value of the data point is stored | No       | Uses a default value of `val` as the value_col value         |
| 6    | value_transform         | The transformation function executed on the collected value in taosX | No       | No transformation will be applied                            |
| 7    | type                    | The data type of the collected value                         | No       | The original type of the collected value will be used as the data type in TDengine |
| 8    | quality_col             | The column name in TDengine where the quality of the collected value is stored | No       | No quality column will be added in TDengine                  |
| 9    | ts_col                  | The timestamp column in TDengine where the original timestamp of the data point is stored | No       | If both ts_col and received_ts_col are non-empty, the former is used as the timestamp column. If one of the two is empty, the non-empty column is used. If both are empty, the original timestamp of the data point is used as the timestamp. |
| 10   | received_ts_col         | The timestamp column in TDengine where the received timestamp of the data point is stored | No       | Same as above                                                |
| 11   | ts_transform            | The transformation function applied to the data point's timestamp in taosX | No       | No transformation will be applied to the original timestamp of the data point |
| 12   | received_ts_transform   | The transformation function applied to the received timestamp of the data point in taosX | No       | Same as above                                                |
| 13   | tag::VARCHAR(200)::name | The Tag column in TDengine corresponding to the data point. `tag` is a reserved keyword that indicates the column is a tag column. `VARCHAR(200)` indicates the tag's type. `name` is the actual name of the tag. | No       | If 1 or more tag columns are configured, the specified tag columns are used. If no tag columns are configured and the supertable exists in TDengine, the supertable's tags are used. If not, default tags are added: `point_id` and `point_name`. |

(2) The CSV header must not have duplicate columns.

(3) Columns like `tag::VARCHAR(200)::name` can be configured multiple times, corresponding to multiple tags in TDengine. However, tag names must not be duplicated.

(4) The order of columns in the CSV header does not affect the CSV file validation rules.

(5) Columns not listed in the table can also be configured, such as serial numbers. These columns will be automatically ignored.

3. Row Configuration Rules

Each row in the CSV file configures an OPC data point. The row rules are as follows:

(1) The columns in the row must correspond to the columns in the header:

| No.  | Header Column           | Value Type | Value Range                                                  | Required | Default Value                            |
| ---- | ----------------------- | ---------- | ------------------------------------------------------------ | -------- | ---------------------------------------- |
| 1    | tag_name                | String     | A string like `root.parent.temperature`, which must conform to the OPC DA ID specification | Yes      |                                          |
| 2    | enable                  | int        | 0: Do not collect data for this point. The subtable corresponding to the data point will be deleted from TDengine before the OPC DataIn task starts. 1: Collect data for this point. The subtable will not be deleted. | No       | 1                                        |
| 3    | stable                  | String     | Any string that conforms to TDengine supertable naming conventions. Special characters such as `.` will be replaced with underscores. If `{type}` is present: - If `type` is non-empty, it will be replaced by the value of `type`. - If `type` is empty, the original type of the collected value will be used. | Yes      |                                          |
| 4    | tbname                  | String     | Any string that conforms to TDengine subtable naming conventions. Special characters such as `.` will be replaced with underscores. If `{tag_name}` is present, it will be replaced with the tag_name. | Yes      |                                          |
| 5    | value_col               | String     | A column name that conforms to TDengine naming conventions   | No       | val                                      |
| 6    | value_transform         | String     | A calculation expression that conforms to the Rhai engine, such as `(val + 10) / 1000 * 2.0` or `log(val) + 10` | No       | None                                     |
| 7    | type                    | String     | Supported types include: `b/bool/i8/tinyint/i16/small/inti32/int/i64/bigint/u8/tinyint unsigned/u16/smallint unsigned/u32/int unsigned/u64/bigint unsigned/f32/float/f64/double/timestamp/timestamp(ms)/timestamp(us)/timestamp(ns)/json` | No       | The original type of the collected value |
| 8    | quality_col             | String     | A column name that conforms to TDengine naming conventions   | No       | None                                     |
| 9    | ts_col                  | String     | A column name that conforms to TDengine naming conventions   | No       | ts                                       |
| 10   | received_ts_col         | String     | A column name that conforms to TDengine naming conventions   | No       | rts                                      |
| 11   | ts_transform            | String     | Supports +, -, *, /, % operators, e.g., `ts / 1000 * 1000` to set the last 3 digits of a timestamp in ms to 0; `ts + 8 * 3600 * 1000` to add 8 hours to a timestamp in ms precision; `ts - 8 * 3600 * 1000` to subtract 8 hours from a timestamp in ms precision. | No       | None                                     |
| 12   | received_ts_transform   | String     | None                                                         | No       | None                                     |
| 13   | tag::VARCHAR(200)::name | String     | The value in the tag. When the tag type is VARCHAR, it can be in Chinese | No       | NULL                                     |

(2) tag_name must be unique across the entire DataIn task. In an OPC DataIn task, a data point can only be written to one subtable in TDengine. If you need to write a data point to multiple subtables, you must create multiple OPC DataIn tasks.

(3) If tag_name is different but tbname is the same, value_col must be different. This configuration allows data from multiple data points of different data types to be written to different columns in the same subtable. This approach corresponds to the use case of "wide tables for OPC data ingestion into TDengine."

4. Other Rules

(1) If the number of columns in the header and the row is inconsistent, validation fails, and the user is prompted with the row number that does not meet the requirements.

(2) The header must be in the first row and cannot be empty.

(3) At least one data point is required.

#### 4.2. Select Data Points

You can filter data points by configuring **Root Node ID** and **Regular Expression Match** as filtering conditions.

Specify the supertable and subtable where the data will be written by configuring **Supertable Name** and **Table Name**.

Configure the **Primary Key Column**: select origin_ts to use the original timestamp of the OPC data point as the primary key in TDengine, or select received_ts to use the received timestamp as the primary key in TDengine. You can also configure the **Primary Key Alias** to specify the name of the timestamp column in TDengine.

<figure>
<Image img={imgStep4} alt=""/>
</figure>

### 5. Collection Configuration

In the collection configuration, configure the collection interval, connection timeout, and collection timeout options for the current task.

<figure>
<Image img={imgStep5} alt=""/>
</figure>

As shown in the image above:

- **Connection Timeout**: Configure the timeout for connecting to the OPC server. The default is 10 seconds.
- **Collection Timeout**: If no data is returned from the OPC server within the specified time when reading data points, the read operation will fail. The default is 10 seconds.
- **Collection Interval**: The default is 10 seconds. The collection interval is the duration between the end of the previous data collection and the start of the next data collection, during which the latest value of the data point is polled and written to TDengine.

When **Data Points Set** is configured using the **Select Data Points** method, you can configure **Data Point Update Mode** and **Data Point Update Interval** in the collection configuration to enable dynamic data point updates. **Dynamic Data Point Update** means that during the task's execution, if the OPC Server adds or deletes data points, data points that meet the criteria will be automatically added to the current task without restarting the OPC task.

- Data Point Update Mode: You can choose `None`, `Append`, or `Update`.
  - None: Dynamic data point updates are not enabled.
  - Append: Dynamic data point updates are enabled, but only new data points are added.
  - Update: Dynamic data point updates are enabled, and data points can be added or removed.
- Data Point Update Interval: This is effective when the **Data Point Update Mode** is set to `Append` or `Update`. The unit is seconds, the default is 600, the minimum value is 60, and the maximum value is 2147483647.

### 6. Advanced Options

<figure>
<Image img={imgStep6} alt=""/>
</figure>

As shown in the image above, advanced options can be configured to further optimize performance, logging, and more.

The **Log Level** is set to `info` by default. The available options are `error`, `warn`, `info`, `debug`, and `trace`.

The **Max Write Concurrency** option sets the maximum concurrency limit for writing to taosX. The default value is 0, which means auto, and concurrency is automatically configured.

The **Batch Size** option sets the batch size for each write operation, i.e., the maximum number of messages sent at once.

The **Batch Delay** option sets the maximum delay for a single send operation (in seconds). When the timeout expires, if there is data, even if the **Batch Size** is not met, the data will be sent immediately.

In the **Save Raw Data** option, choose whether to save the raw data. The default is No.

When saving raw data, the following two parameters become effective.

The **Max Retention Days** option sets the maximum retention days for raw data.

The **Raw Data Storage Directory** option sets the path for storing raw data. If an agent is used, the storage path refers to the path on the agent's server; otherwise, it is the path on the taosX server. The path can use the `$DATA_DIR` placeholder and `:id` as part of the path.

- On Linux platforms, `$DATA_DIR` is `/var/lib/taos/taosx`. By default, the storage path is `/var/lib/taos/taosx/tasks/<task_id>/rawdata`.
- On Windows platforms, `$DATA_DIR` is `C:\TDengine\data\taosx`. By default, the storage path is `C:\TDengine\data\taosx\tasks\<task_id>\rawdata`.

### 7. Task Completion

Click the **Submit** button to complete the OPC DA to TDengine data synchronization task. Return to the **Data Sources List** page to view the task's execution status.

## Adding Data Points

While the task is running, click **Edit**, then click the **Add Data Points** button to append data points to the CSV file.

In the pop-up form, fill in the data point information.

Click the **Confirm** button to complete the addition of data points.
