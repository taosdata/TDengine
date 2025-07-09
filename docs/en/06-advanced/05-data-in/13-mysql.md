---
title: MySQL
slug: /advanced-features/data-connectors/mysql
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/mysql-01.png';
import imgStep02 from '../../assets/mysql-02.png';
import imgStep03 from '../../assets/mysql-03.png';
import imgStep04 from '../../assets/mysql-04.png';
import imgStep05 from '../../assets/mysql-05.png';
import imgStep06 from '../../assets/mysql-06.png';
import imgStep07 from '../../assets/mysql-07.png';
import imgStep08 from '../../assets/mysql-08.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create data migration tasks through the Explorer interface, migrating data from MySQL to the current TDengine cluster.

## Overview

MySQL is one of the most popular relational databases. Many systems have used or are using MySQL databases to store data reported by IoT and industrial internet devices. However, as the number of devices in the access systems grows and the demand for real-time data feedback from users increases, MySQL can no longer meet business needs. Starting from TDengine Enterprise Edition 3.3.0.0, TDengine can efficiently read data from MySQL and write it into TDengine, achieving historical data migration or real-time data synchronization, and solving the technical pain points faced by businesses.

## Creating a Task

### 1. Add a Data Source

Click the **+ Add Data Source** button in the top left corner of the data writing page to enter the Add Data Source page, as shown below:

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

Enter the task name in the **Name** field, for example *`test_mysql_01`*.

Select *`MySQL`* from the **Type** dropdown menu, as shown below (the fields on the page will change after selection).

**Proxy** is optional. If needed, you can select a specific proxy from the dropdown menu, or click the **+ Create New Proxy** button on the right to create a new proxy.

**Target Database** is required. You can click the **+ Create Database** button on the right to create a new database.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

Fill in the *`connection information for the source MySQL database`* in the **Connection Configuration** area, as shown below:

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure Authentication Information

**User** Enter the user of the source MySQL database, who must have read permissions in the organization.

**Password** Enter the login password for the user mentioned above in the source MySQL database.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

### 5. Configure Connection Options

**Character Set** Set the character set for the connection. The default character set is utf8mb4. MySQL 5.5.3 supports this feature. If connecting to an older version, it is recommended to change to utf8.
Options include utf8, utf8mb4, utf16, utf32, gbk, big5, latin1, ascii.

**SSL Mode** Set whether to negotiate a secure SSL TCP/IP connection with the server or the priority of negotiation. The default value is PREFERRED. Options include DISABLED, PREFERRED, REQUIRED.

<figure>
<Image img={imgStep05} alt=""/>
</figure>

Then click the **Check Connectivity** button, where users can click this button to check if the information filled in above can normally fetch data from the source MySQL database.

### 6. Configure SQL Query

**Subtable Field** is used to split subtables, it is a select distinct SQL statement that queries non-repeated items of specified field combinations, usually corresponding to the tag in transform:
> This configuration is mainly to solve the problem of data migration disorder, and it needs to be used together with **SQL Template**, otherwise it cannot achieve the expected effect, usage examples are as follows:
>
> 1. Fill in the subtable field statement `select distinct col_name1, col_name2 from table`, which means using the fields col_name1 and col_name2 in the source table to split the subtables of the target supertable
> 2. Add subtable field placeholders in the **SQL Template**, for example, the `${col_name1} and ${col_name2}` part in `select * from table where ts >= ${start} and ts < ${end} and ${col_name1} and ${col_name2}`
> 3. Configure `col_name1` and `col_name2` two tag mappings in **transform**

**SQL Template** is the SQL statement template used for querying, the SQL statement must include time range conditions, and the start and end times must appear in pairs. The time range defined in the SQL statement template consists of a column representing time in the source database and the placeholders defined below.
> SQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:
>
> 1. `${start}`, `${end}`: Represents RFC3339 format timestamps, e.g.: 2024-03-14T08:00:00+0800
> 2. `${start_no_tz}`, `${end_no_tz}`: Represents RFC3339 strings without timezone: 2024-03-14T08:00:00
> 3. `${start_date}`, `${end_date}`: Represents date only, e.g.: 2024-03-14
>
> To solve the problem of data migration disorder, it is advisable to add sorting conditions in the query statement, such as `order by ts asc`.

**Start Time** The start time for migrating data, this field is required.

**End Time** The end time for migrating data, which can be left blank. If set, the migration task will stop automatically after reaching the end time; if left blank, it will continuously synchronize real-time data and the task will not stop automatically.

**Query Interval** The time interval for querying data in segments, default is 1 day. To avoid querying a large amount of data at once, a data synchronization sub-task will use the query interval to segment the data retrieval.

**Delay Duration** In real-time data synchronization scenarios, to avoid losing data due to delayed writes, each synchronization task will read data from before the delay duration.

<figure>
<Image img={imgStep06} alt=""/>
</figure>

### 7. Configure Data Mapping

In the **Data Mapping** area, fill in the configuration parameters related to data mapping.

Click the **Retrieve from Server** button to fetch sample data from the MySQL server.

In **Extract or Split from Column**, fill in the fields to extract or split from the message body, for example: split the `vValue` field into `vValue_0` and `vValue_1`, select the split extractor, fill in the separator `,`, and number `2`.

In **Filter**, fill in the filtering conditions, for example: write `Value > 0`, then only data where Value is greater than 0 will be written to TDengine.

In **Mapping**, select the supertable in TDengine to map to, and the columns to map to the supertable.

Click **Preview** to view the results of the mapping.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

### 8. Configure Advanced Options

The **Advanced Options** area is collapsed by default, click the `>` on the right to expand it, as shown below:

**Maximum Read Concurrency** The limit on the number of data source connections or reading threads, modify this parameter when the default parameters do not meet the needs or when adjusting resource usage.

**Batch Size** The maximum number of messages or rows sent at once. The default is 10000.

<figure>
<Image img={imgStep08} alt=""/>
</figure>

### 9. Completion

Click the **Submit** button to complete the creation of the data synchronization task from MySQL to TDengine, and return to the **Data Source List** page to view the task execution status.
