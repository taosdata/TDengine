---
title: Microsoft SQL Server
sidebar_label: SQL Server
slug: /advanced-features/data-connectors/sql-server
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/sql-server-01.png';
import imgStep02 from '../../assets/sql-server-02.png';
import imgStep03 from '../../assets/sql-server-03.png';
import imgStep04 from '../../assets/sql-server-04.png';
import imgStep05 from '../../assets/sql-server-05.png';
import imgStep06 from '../../assets/sql-server-06.png';
import imgStep07 from '../../assets/sql-server-07.png';
import imgStep08 from '../../assets/sql-server-08.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create data migration tasks through the Explorer interface, migrating data from Microsoft SQL Server to the current TDengine cluster.

## Feature Overview

Microsoft SQL Server is one of the most popular relational databases. Many systems have used or are using Microsoft SQL Server to store data reported by IoT and industrial internet devices. However, as the number of devices in the access systems grows and the demand for real-time data feedback from users increases, Microsoft SQL Server can no longer meet business needs. Starting from TDengine Enterprise Edition 3.3.2.0, TDengine can efficiently read data from Microsoft SQL Server and write it into TDengine, achieving historical data migration or real-time data synchronization, and solving technical pain points faced by businesses.

## Creating a Task

### 1. Add a Data Source

Click the **+ Add Data Source** button in the upper left corner of the data writing page to enter the Add Data Source page, as shown below:

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

Enter the task name in the **Name** field, for example *`test_mssql_01`*.

Select *`Microsoft SQL Server`* from the **Type** dropdown menu, as shown below (the fields on the page will change after selection).

**Agent** is optional. If needed, you can select a specific agent from the dropdown menu, or click the **+ Create New Agent** button on the right to create a new agent.

**Target Database** is required. You can click the **+ Create Database** button on the right to create a new database.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

Fill in the *`connection information for the source Microsoft SQL Server database`* in the **Connection Configuration** area, as shown below:

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure Authentication Information

**User** Enter the user of the source Microsoft SQL Server database, who must have read permissions in the organization.

**Password** Enter the login password for the user mentioned above in the source Microsoft SQL Server database.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

### 5. Configure Connection Options

**Instance Name** Set the Microsoft SQL Server instance name (defined in SQL Browser, only available on Windows platform, if specified, the port will be replaced with the value returned from SQL Browser).

**Application Name** Set the application name to identify the connecting application.

**Encryption** Set whether to use an encrypted connection. The default value is Off. Options include Off, On, NotSupported, Required.

**Trust Certificate** Set whether to trust the server certificate. If enabled, the server certificate will not be verified and will be accepted as is (if trust is enabled, the `Trust Certificate CA` field below will be hidden).

**Trust Certificate CA** Set whether to trust the server's certificate CA. If a CA file is uploaded, the server certificate will be verified based on the provided CA certificate in addition to the system trust store.

<figure>
<Image img={imgStep05} alt=""/>
</figure>
  
Then click the **Check Connectivity** button. Users can click this button to check if the information filled in above can normally retrieve data from the source Microsoft SQL Server database.

### 6. Configure SQL Query

**Subtable Field** is used to split subtables, it is a select distinct SQL statement that queries non-repeated items of specified field combinations, usually corresponding to the tag in transform:
> This configuration is mainly to solve the data migration disorder problem, and needs to be used in conjunction with **SQL Template**, otherwise it cannot achieve the expected effect, usage examples are as follows:
>
> 1. Fill in the subtable field statement `select distinct col_name1, col_name2 from table`, which means using the fields col_name1 and col_name2 in the source table to split the subtables of the target supertable
> 2. Add subtable field placeholders in the **SQL Template**, for example, the `${col_name1} and ${col_name2}` part in `select * from table where ts >= ${start} and ts < ${end} and ${col_name1} and ${col_name2}`
> 3. Configure the mappings of `col_name1` and `col_name2` as two tags in **transform**

**SQL Template** is used for the SQL statement template for querying, which must include a time range condition, and the start time and end time must appear in pairs. The time range defined in the SQL statement template consists of a column representing time in the source database and the placeholders defined below.
> SQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:
>
> 1. `${start}`, `${end}`: Represents RFC3339 format timestamp, e.g.: 2024-03-14T08:00:00+0800
> 2. `${start_no_tz}`, `${end_no_tz}`: Represents an RFC3339 string without timezone: 2024-03-14T08:00:00
> 3. `${start_date}`, `${end_date}`: Represents date only, e.g.: 2024-03-14
>
> Note: Only `datetime2` and `datetimeoffset` support querying using start/end, `datetime` and `smalldatetime` can only use start_no_tz/end_no_tz for querying, and `timestamp` cannot be used as a query condition.
>
> To solve the problem of data migration disorder, it is advisable to add a sorting condition in the query statement, such as `order by ts asc`.

**Start Time** The start time of the data migration, this field is mandatory.

**End Time** The end time of the data migration, which can be left blank. If set, the migration task will stop automatically after reaching the end time; if left blank, it will continuously synchronize real-time data, and the task will not stop automatically.

**Query Interval** The time interval for segmenting data queries, default is 1 day. To avoid querying too much data at once, a data synchronization subtask will use the query interval to segment the data.

**Delay Duration** In real-time data synchronization scenarios, to avoid losing data due to delayed writing, each synchronization task will read data from before the delay duration.

<figure>
<Image img={imgStep06} alt=""/>
</figure>

### 7. Configure Data Mapping

Fill in the related configuration parameters in the **Data Mapping** area.

Click the **Retrieve from Server** button to get sample data from the Microsoft SQL Server.

In **Extract or Split from Column**, fill in the fields to extract or split from the message body, for example: split the `vValue` field into `vValue_0` and `vValue_1`, select the split extractor, fill in the separator as `,`, and number as 2.

In **Filter**, fill in the filter conditions, for example: write `Value > 0`, then only data where Value is greater than 0 will be written to TDengine.

In **Mapping**, select the supertable in TDengine to which you want to map, and the columns to map to the supertable.

Click **Preview** to view the results of the mapping.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

### 8. Configure Advanced Options

The **Advanced Options** area is collapsed by default, click the `>` on the right to expand it, as shown below:

**Maximum Read Concurrency** Limit on the number of data source connections or reading threads, modify this parameter when default parameters do not meet the needs or when adjusting resource usage.

**Batch Size** The maximum number of messages or rows sent at once. The default is 10000.

<figure>
<Image img={imgStep08} alt=""/>
</figure>

### 9. Completion

Click the **Submit** button to complete the creation of the data synchronization task from Microsoft SQL Server to TDengine, and return to the **Data Source List** page to view the status of the task execution.
