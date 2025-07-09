---
title: OPC UA
slug: /advanced-features/data-connectors/opc-ua
---

import Image from '@theme/IdealImage';
import imgStep1 from '../../assets/opc-ua-01.png';
import imgStep2 from '../../assets/opc-ua-02.png';
import imgStep3 from '../../assets/opc-ua-03.png';
import imgStep4 from '../../assets/opc-ua-04.png';
import imgStep5 from '../../assets/opc-ua-05.png';
import imgStep6 from '../../assets/opc-ua-06.png';
import imgStep7 from '../../assets/opc-ua-07.png';
import imgStep8 from '../../assets/opc-ua-08.png';
import imgStep9 from '../../assets/opc-ua-09.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create data migration tasks through the Explorer interface to synchronize data from an OPC-UA server to the current TDengine cluster.

## Overview

OPC is one of the interoperability standards for securely and reliably exchanging data in the field of industrial automation and other industries.

OPC-UA is the next-generation standard of the classic OPC specifications, a platform-independent, service-oriented architecture specification that integrates all the functionalities of the existing OPC Classic specifications, providing a path to a more secure and scalable solution.

TDengine can efficiently read data from OPC-UA servers and write it to TDengine, enabling real-time data ingestion.

## Creating a Task

### 1. Add a Data Source

On the data writing page, click the **+ Add Data Source** button to enter the add data source page.

<figure>
<Image img={imgStep1} alt=""/>
</figure>

### 2. Configure Basic Information

Enter the task name in **Name**, for example, for environmental temperature and humidity monitoring, name it **environment-monitoring**.

Select **OPC-UA** from the **Type** dropdown list.

**Proxy** is optional, you can select a specific proxy from the dropdown list, or click the **+ Create New Proxy** button on the right.

Select a target database from the **Target Database** dropdown list, or click the **+ Create Database** button on the right.

<figure>
<Image img={imgStep2} alt=""/>
</figure>

### 3. Configure Connection Information

In the **Connection Configuration** area, fill in the **OPC-UA Service Address**, for example: `127.0.0.1:5000`, and configure the data transmission security mode, with three security modes available:

1. None: Communication data is transmitted in plaintext.
1. Sign: Communication data is verified using a digital signature to protect data integrity.
1. SignAndEncrypt: Communication data is verified using a digital signature and encrypted using encryption algorithms to ensure data integrity, authenticity, and confidentiality.

If you choose Sign or SignAndEncrypt as the security mode, you must select a valid security policy. Security policies define how to implement the encryption and verification mechanisms in the security mode, including the encryption algorithms used, key lengths, digital certificates, etc. Available security policies include:

1. None: Only selectable when the security mode is None.
1. Basic128Rsa15: Uses RSA algorithm and 128-bit key length to sign or encrypt communication data.
1. Basic256: Uses AES algorithm and 256-bit key length to sign or encrypt communication data.
1. Basic256Sha256: Uses AES algorithm and 256-bit key length, and encrypts digital signatures using the SHA-256 algorithm.
1. Aes128Sha256RsaOaep: Uses AES-128 algorithm for encrypting and decrypting communication data, encrypts digital signatures using the SHA-256 algorithm, and uses RSA algorithm and OAEP mode for encrypting and decrypting symmetric communication keys.
1. Aes256Sha256RsaPss: Uses AES-256 algorithm for encrypting and decrypting communication data, encrypts digital signatures using the SHA-256 algorithm, and uses RSA algorithm and PSS mode for encrypting and decrypting symmetric communication keys.

<figure>
<Image img={imgStep3} alt=""/>
</figure>

### 4. Choose Authentication Method

As shown below, switch tabs to choose different authentication methods, with the following options available:

1. Anonymous
1. Username
1. Certificate Access: Can be the same as the security communication certificate, or a different certificate.

<figure>
<Image img={imgStep4} alt=""/>
</figure>

After configuring the connection properties and authentication method, click the **Connectivity Check** button to check if the data source is available. If using a security communication certificate or authentication certificate, the certificate must be trusted by the OPC UA server, otherwise, it will still fail.

### 5. Configure Points Set

**Points Set** can choose to use a CSV file template or **Select All Points**.

#### 5.1. Upload CSV Configuration File

You can download the CSV blank template and configure the point information according to the template, then upload the CSV configuration file to configure points; or download data points according to the configured filter conditions, and download in the format specified by the CSV template.

CSV files have the following rules:

1. File Encoding

The encoding format of the CSV file uploaded by the user must be one of the following:

(1) UTF-8 with BOM

(2) UTF-8 (i.e., UTF-8 without BOM)

1. Header Configuration Rules

The header is the first line of the CSV file, with the following rules:

(1) The header of the CSV can configure the following columns:

| Number | Column Name             | Description                                                                                                                                                                                                        | Required | Default Behavior                                                                                                                                                                                                                                                                                                                                                      |
|--------|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| -------- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1      | point_id                | The id of the data point on the OPC UA server                                                                                                                                                                      | Yes      | None                                                                                                                                                                                                                                                                                                                                                                  |
| 2      | stable                  | The corresponding supertable for the data point in TDengine                                                                                                                                                        | Yes      | None                                                                                                                                                                                                                                                                                                                                                                  |
| 3      | tbname                  | The corresponding subtable for the data point in TDengine                                                                                                                                                          | Yes      | None                                                                                                                                                                                                                                                                                                                                                                  |
| 4      | enable                  | Whether to collect data from this point                                                                                                                                                                            | No       | Use the unified default value `1` for enable                                                                                                                                                                                                                                                                                                                          |
| 5      | value_col               | The column name in TDengine corresponding to the collected value of the data point                                                                                                                                 | No       | Use the unified default value `val` as the value_col                                                                                                                                                                                                                                                                                                                  |
| 6      | value_transform         | The transformation function executed in taosX for the collected value of the data point                                                                                                                            | No       | Do not transform the collected value uniformly                                                                                                                                                                                                                                                                                                                        |
| 7      | type                    | The data type of the collected value of the data point                                                                                                                                                             | No       | Use the original type of the collected value as the data type in TDengine                                                                                                                                                                                                                                                                                             |
| 8      | quality_col             | The column name in TDengine corresponding to the quality of the collected value                                                                                                                                    | No       | Do not add a quality column in TDengine uniformly                                                                                                                                                                                                                                                                                                                     |
| 9      | ts_col                  | The original timestamp column of the data point in TDengine                                                                                                                                                        | No       | ts_col, request_ts, received_ts these 3 columns, when there are more than 2 columns, the leftmost column is used as the primary key in TDengine.                                                                                                                                                                                                                      |
| 10     | request_ts_col          | The timestamp column in TDengine when the data point value is request                                                                                                                                              | No       | Same as above                                                                                                                                                                                                                                                                                                                                                         |
| 11     | received_ts_col         | The timestamp column in TDengine when the data point value is received                                                                                                                                             | No       | Same as above                                                                                                                                                                                                                                                                                                                                                         |
| 12     | ts_transform            | The transformation function executed in taosX for the original timestamp of the data point                                                                                                                         | No       | Do not transform the original timestamp of the data point uniformly                                                                                                                                                                                                                                                                                                   |
| 13     | request_ts_transform    | The transformation function executed in taosX for the request timestamp of the data point                                                                                                                          | No       | Do not transform the original timestamp of the data point uniformly                                                                                                                                                                                                                                                                                                   |
| 14     | received_ts_transform   | The transformation function executed in taosX for the received timestamp of the data point                                                                                                                         | No       | Do not transform the received timestamp of the data point uniformly                                                                                                                                                                                                                                                                                                   |
| 15     | tag::VARCHAR(200)::name | The Tag column corresponding to the data point in TDengine. Here `tag` is a reserved keyword indicating that this column is a tag; `VARCHAR(200)` indicates the type of tag; `name` is the actual name of the tag. | No       | If 1 or more tag columns are configured, use the configured tag columns; if no tag columns are configured and stable exists in TDengine, use the tags of the stable in TDengine; if no tag columns are configured and stable does not exist in TDengine, automatically add the following 2 tag columns: tag::VARCHAR(256)::point_id and tag::VARCHAR(256)::point_name |

(2) In the CSV Header, there cannot be duplicate columns;

(3) In the CSV Header, columns like `tag::VARCHAR(200)::name` can be configured multiple times, corresponding to multiple Tags in TDengine, but the names of the Tags cannot be repeated.

(4) In the CSV Header, the order of the columns does not affect the CSV file validation rules;

(5) In the CSV Header, columns that are not listed in the table above can be configured, such as: sequence number, these columns will be automatically ignored.

1. Row Configuration Rules

Each Row in the CSV file configures an OPC data point. The rules for Rows are as follows:

(1) Correspondence with columns in the Header

| Number | Column in Header        | Type of Value | Value Range                                                                                                                                                                                                                                                                            | Mandatory | Default Value                         |
|--------|-------------------------| ------------- |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| --------- |---------------------------------------|
| 1      | point_id                | String        | Strings like `ns=3;i=1005`, must meet the OPC UA ID specification, i.e., include ns and id parts                                                                                                                                                                                       | Yes       |                                       |
| 2      | enable                  | int           | 0: Do not collect this point, and delete the corresponding subtable in TDengine before the OPC DataIn task starts; 1: Collect this point, do not delete the subtable before the OPC DataIn task starts.                                                                                | No        | 1                                     |
| 3      | stable                  | String        | Any string that meets the TDengine supertable naming convention; if special character `.` exists, replace with underscore if `{type}` exists: if type in CSV file is not empty, replace with the value of type if type is empty, replace with the original type of the collected value | Yes       |                                       |
| 4      | tbname                  | String        | Any string that meets the TDengine subtable naming convention; for OPC UA: if `{ns}` exists, replace with ns from point_id if `{id}` exists, replace with id from point_id for OPC DA: if `{tag_name}` exists, replace with tag_name                                                   | Yes       |                                       |
| 5      | value_col               | String        | Column name that meets TDengine naming convention                                                                                                                                                                                                                                      | No        | val                                   |
| 6      | value_transform         | String        | Expressions that meet the Rhai engine, for example: `(val + 10) / 1000 * 2.0`, `log(val) + 10`, etc.;                                                                                                                                                                                  | No        | None                                  |
| 7      | type                    | String        | Supported types include: b/bool/i8/tinyint/i16/small/inti32/int/i64/bigint/u8/tinyint unsigned/u16/smallint unsigned/u32/int unsigned/u64/bigint unsigned/f32/float/f64/double/timestamp/timestamp(ms)/timestamp(us)/timestamp(ns)/json                                                | No        | Original type of the data point value |
| 8      | quality_col             | String        | Column name that meets TDengine naming convention                                                                                                                                                                                                                                      | No        | None                                  |
| 9      | ts_col                  | String        | Column name that meets TDengine naming convention                                                                                                                                                                                                                                      | No        | ts                                    |
| 10     | request_ts_col          | String        | Column name that meets TDengine naming convention                                                                                                                                                                                                                                      | No        | qts                                   |
| 11     | received_ts_col         | String        | Column name that meets TDengine naming convention                                                                                                                                                                                                                                      | No        | rts                                   |
| 12     | ts_transform            | String        | Supports +, -, *, /, % operators, for example: ts / 1000* 1000, sets the last 3 digits of a timestamp in ms to 0; ts + 8 *3600* 1000, adds 8 hours to a timestamp in ms; ts - 8 *3600* 1000, subtracts 8 hours from a timestamp in ms;                                                 | No        | None                                  |
| 13     | request_ts_transform    | String        | Supports +, -, *, /, % operators, for example: qts / 1000* 1000, sets the last 3 digits of a timestamp in ms to 0; qts + 8 *3600* 1000, adds 8 hours to a timestamp in ms; qts - 8 *3600* 1000, subtracts 8 hours from a timestamp in ms;                                              | No        | None                                  |
| 14     | received_ts_transform   | String        | Supports +, -, *, /, % operators, for example: qts / 1000* 1000, sets the last 3 digits of a timestamp in ms to 0; qts + 8 *3600* 1000, adds 8 hours to a timestamp in ms; qts - 8 *3600* 1000, subtracts 8 hours from a timestamp in ms;                                              | None      | None                                  |
| 15     | tag::VARCHAR(200)::name | String        | The value inside a tag, when the tag type is VARCHAR, can be in Chinese                                                                                                                                                                                                                | No        | NULL                                  |

(2) `point_id` is unique throughout the DataIn task, meaning: in an OPC DataIn task, a data point can only be written to one subtable in TDengine. If you need to write a data point to multiple subtables, you need to create multiple OPC DataIn tasks;

(3) When `point_id` is different but `tbname` is the same, `value_col` must be different. This configuration allows data from multiple data points of different types to be written to different columns in the same subtable. This method corresponds to the "OPC data into TDengine wide table" usage scenario.

1. Other Rules

(1) If the number of columns in Header and Row are inconsistent, the validation fails, and the user is prompted with the line number that does not meet the requirements;

(2) Header is on the first line and cannot be empty;

(3) There must be at least one data point;

#### 5.2. Selecting Data Points

Data points can be filtered by configuring **Root Node ID**, **Namespace**, **Regular Matching**, etc.

Configure **Supertable Name**, **Table Name** to specify the supertable and subtable where the data will be written.

Configure **Primary Key Column**, choose `origin_ts` to use the original timestamp of the OPC data point as the primary key in TDengine; choose `request_ts` to use the data's request timestamp as the primary key in TDengine; choose `received_ts` to use the data's reception timestamp as the primary key in TDengine. Configure **Primary Key Alias** to specify the name of the TDengine timestamp column.

<figure>
<Image img={imgStep5} alt=""/>
</figure>

### 6. Collection Configuration

In the collection configuration, configure the current task's collection mode, collection interval, collection timeout, etc.

<figure>
<Image img={imgStep6} alt=""/>
</figure>

As shown in the image above:

- **Collection Mode**: Can use `subscribe` or `observe` mode.
  - `subscribe`: Subscription mode, reports data changes and writes to TDengine.
  - `observe`: According to the `collection interval`, polls the latest value of the data point and writes to TDengine.
- **Collection Interval**: Default is 10 seconds, the interval for collecting data points, starting from the end of the last data collection, polls the latest value of the data point and writes to TDengine. Only configurable in `observe` **Collection Mode**.
- **Collection Timeout**: If the data from the OPC server is not returned within the set time when reading data points, the read fails, default is 10 seconds. Only configurable in `observe` **Collection Mode**.

When using **Selecting Data Points** in the **Data Point Set**, the collection configuration can configure **Data Point Update Mode** and **Data Point Update Interval** to enable dynamic data point updates. **Dynamic Data Point Update** refers to, during the task operation, after OPC Server adds or deletes data points, the data points that meet the conditions will automatically be added to the current task without needing to restart the OPC task.

- Data Point Update Mode: Can choose `None`, `Append`, `Update`.
  - None: Do not enable dynamic data point updates;
  - Append: Enable dynamic data point updates, but only append;
  - Update: Enable dynamic data point updates, append or delete;
- Data Point Update Interval: Effective when "Data Point Update Mode" is `Append` and `Update`. Unit: seconds, default value is 600, minimum value: 60, maximum value: 2147483647.

### 7. Advanced Options

<figure>
<Image img={imgStep7} alt=""/>
</figure>

As shown in the image above, configure advanced options for more detailed optimization of performance, logs, etc.

**Log Level** defaults to `info`, with options `error`, `warn`, `info`, `debug`, `trace`.

In **Maximum Write Concurrency**, set the maximum concurrency limit for writing to taosX. Default value: 0, meaning auto, automatically configures concurrency.

In **Batch Size**, set the batch size for each write, i.e., the maximum number of messages sent at one time.

In **Batch Delay**, set the maximum delay for a single send (in seconds), when the timeout ends, as long as there is data, it is sent immediately even if it does not meet the **Batch Size**.

In **Save Raw Data**, choose whether to save raw data. Default value: No.

When saving raw data, the following 2 parameters are effective.

In **Maximum Retention Days**, set the maximum retention days for raw data.

In **Raw Data Storage Directory**, set the path for saving raw data. If using Agent, the storage path refers to the path on the server where the Agent is located, otherwise it is on the taosX server. The path can use placeholders `$DATA_DIR` and `:id` as part of the path.

- On Linux platform, `$DATA_DIR` is /var/lib/taos/taosx, by default the storage path is `/var/lib/taos/taosx/tasks/<task_id>/rawdata`.
- On Windows platform, `$DATA_DIR` is C:\TDengine\data\taosx, by default the storage path is `C:\TDengine\data\taosx\tasks\<task_id>\rawdata`.

### 8. Completion

Click the **Submit** button to complete the creation of the OPC UA to TDengine data synchronization task. Return to the **Data Source List** page to view the status of the task execution.

## Add Data Points

During the task execution, click **Edit**, then click the **Add Data Points** button to append data points to the CSV file.

<figure>
<Image img={imgStep8} alt=""/>
</figure>

In the pop-up form, fill in the information for the data points.

<figure>
<Image img={imgStep9} alt=""/>
</figure>

Click the **Confirm** button to complete the addition of the data points.
