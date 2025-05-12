---
title: Oracle Database
slug: /advanced-features/data-connectors/oracle-database
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/oracle-database-01.png';
import imgStep02 from '../../assets/oracle-database-02.png';
import imgStep03 from '../../assets/oracle-database-03.png';
import imgStep04 from '../../assets/oracle-database-04.png';
import imgStep05 from '../../assets/oracle-database-05.png';
import imgStep06 from '../../assets/oracle-database-06.png';
import imgStep07 from '../../assets/oracle-database-07.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create data migration tasks through the Explorer interface, migrating data from Oracle to the current TDengine cluster.

## Feature Overview

The Oracle database system is a popular relational database management system worldwide, known for its good portability, ease of use, and strong functionality, suitable for various large, medium, and small computer environments. It is an efficient, reliable, and high-throughput database solution.

TDengine can efficiently read data from Oracle and write it to TDengine, enabling historical data migration or real-time data synchronization.

## Creating a Task

### 1. Add a Data Source

Click the **+ Add Data Source** button in the upper left corner of the data writing page to enter the Add Data Source page, as shown below:

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

Enter the task name in the **Name** field, for example, *`test_oracle_01`*.

Select *`Oracle`* from the **Type** dropdown menu, as shown below (the fields on the page will change after selection).

**Agent** is optional. If needed, you can select a specific agent from the dropdown menu or click the **+ Create New Agent** button on the right to create a new agent.

**Target Database** is required. You can click the **+ Create Database** button on the right to create a new database.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

Fill in the *`connection information for the source Oracle database`* in the **Connection Configuration** area, as shown below:

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure Authentication Information

**User** Enter the user of the source Oracle database, who must have read permissions in the organization.

**Password** Enter the login password for the user mentioned above in the source Oracle database.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

Then click the **Check Connectivity** button, where users can click this button to check if the information filled in above can normally access data from the source Oracle database.

### 5. Configure SQL Query

**Subtable Field** is used to split the subtable field, which is a select distinct SQL statement querying non-repeated items of specified field combinations, usually corresponding to the tag in transform:
> This configuration is mainly to solve the problem of data migration disorder. It needs to be used in conjunction with **SQL Template**, otherwise, it cannot achieve the expected effect. Usage examples are as follows:
>
> 1. Fill in the subtable field statement `select distinct col_name1, col_name2 from table`, which means using the fields col_name1 and col_name2 in the source table to split the subtable of the target supertable
> 2. Add subtable field placeholders in the **SQL Template**, for example, the `${col_name1} and ${col_name2}` part in `select * from table where ts >= ${start} and ts < ${end} and ${col_name1} and ${col_name2}`
> 3. Configure the mappings of `col_name1` and `col_name2` as two tags in **transform**

**SQL Template** is the SQL statement template used for querying, which must include time range conditions, and the start and end times must appear in pairs. The time range defined in the SQL statement template consists of a column representing time in the source database and the placeholders defined below.
> Different placeholders represent different time format requirements in SQL, specifically including the following placeholder formats:
>
> 1. `${start}`, `${end}`: Represents RFC3339 format timestamps, e.g., 2024-03-14T08:00:00+0800
> 2. `${start_no_tz}`, `${end_no_tz}`: Represents RFC3339 strings without timezone: 2024-03-14T08:00:00
> 3. `${start_date}`, `${end_date}`: Represents date only, but since there is no pure date type in Oracle, it will include zero hour, zero minute, and zero second, e.g., 2024-03-14 00:00:00, so be careful when using `date <= ${end_date}` as it does not include data of the day 2024-03-14
>
> To solve the problem of data migration disorder, a sorting condition should be added to the query statement, such as `order by ts asc`.

**Start Time** The start time for migrating data, this field is required.

**End Time** The end time for migrating data, which can be left blank. If set, the migration task will stop automatically after reaching the end time; if left blank, it will continuously synchronize real-time data and the task will not stop automatically.

**Query Interval** The time interval for querying data in segments, default is 1 day. To avoid querying too much data at once, a data synchronization sub-task will use the query interval to segment the data retrieval.

**Delay Duration** In real-time data synchronization scenarios, to avoid losing data due to delayed writes, each synchronization task will read data from before the delay duration.

<figure>
<Image img={imgStep05} alt=""/>
</figure>

### 6. Configure Data Mapping

In the **Data Mapping** area, fill in the configuration parameters related to data mapping.

Click the **Retrieve from Server** button to get sample data from the Oracle server.

In **Extract or Split from Column**, fill in the fields to extract or split from the message body, for example: split the `vValue` field into `vValue_0` and `vValue_1`, choose the split extractor, fill in the separator `,`, and number `2`.

In **Filter**, fill in the filtering conditions, for example: write `Value > 0`, then only data where Value is greater than 0 will be written to TDengine.

In **Mapping**, select the supertable in TDengine to map to, and the columns to map to the supertable.

Click **Preview** to view the results of the mapping.

<figure>
<Image img={imgStep06} alt=""/>
</figure>

### 7. Configure Advanced Options

The **Advanced Options** area is collapsed by default, click the `>` on the right to expand it, as shown below:

**Maximum Read Concurrency** Limit on the number of data source connections or reading threads, modify this parameter when the default parameters do not meet the needs or when adjusting resource usage.

**Batch Size** The maximum number of messages or rows sent at once. The default is 10000.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

### 8. Completion

Click the **Submit** button to complete the creation of the data synchronization task from Oracle to TDengine, return to the **Data Source List** page to view the status of the task execution.
