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

This section explains how to create data migration tasks through the Explorer interface to migrate data from Oracle to the current TDengine cluster.

## Function Overview

The Oracle database system is one of the most popular relational database management systems in the world, known for its good portability, ease of use, and powerful features, suitable for various large, medium, and small computing environments. It is an efficient, reliable database solution capable of handling high throughput.

TDengine can efficiently read data from Oracle and write it to TDengine for historical data migration or real-time data synchronization.

## Creating a Task

### 1. Add a Data Source

Click the **+Add Data Source** button in the upper left corner of the data writing page to enter the Add Data Source page, as shown below:

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

In the **Name** field, enter a task name, such as *`test_oracle_01`*.

Select *`Oracle`* from the **Type** dropdown list, as shown below (the fields on the page will change after selection).

The **Agent** field is optional; if needed, you can select a specified agent from the dropdown or click the **+Create New Agent** button on the right to create a new agent.

The **Target Database** field is required; you can first click the **+Create Database** button on the right to create a new database.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

In the **Connection Configuration** area, fill in the *`source Oracle database connection information`*, as shown below:

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure Authentication Information

In the **User** field, enter the user for the source Oracle database; this user must have read permissions in the organization.

In the **Password** field, enter the login password for the user in the source Oracle database.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

Then click the **Check Connectivity** button; users can click this button to check if the information filled in above can successfully retrieve data from the source Oracle database.

### 5. Configure SQL Query

The **Subtable Fields** are used to split the subtable fields. It is a `select distinct` SQL statement that queries unique combinations of specified fields, typically corresponding to tags in the transform section:

This configuration is primarily aimed at solving the problem of data migration disorder and needs to be used in conjunction with the **SQL Template**; otherwise, the expected effect cannot be achieved. Here are usage examples:

1. Fill in the subtable fields with the statement `select distinct col_name1, col_name2 from table`, indicating that the fields col_name1 and col_name2 from the source table will be used to split the subtables of the target supertable.
2. In the **SQL Template**, add placeholders for the subtable fields, such as `${col_name1} and ${col_name2}` in `select * from table where ts >= ${start} and ts < ${end} and ${col_name1} and ${col_name2}`.
3. In the **transform** section, configure the tag mappings for `col_name1` and `col_name2`.

The **SQL Template** is an SQL statement template used for querying. The SQL statement must include time range conditions, and the start and end times must appear in pairs. The time range defined in the SQL statement template consists of a column representing time from the source database and the placeholders defined below.

SQL uses different placeholders to represent different time format requirements, specifically the following placeholder formats:

1. `${start}` and `${end}`: Represent RFC3339 formatted timestamps, e.g., 2024-03-14T08:00:00+0800
2. `${start_no_tz}` and `${end_no_tz}`: Represent RFC3339 strings without timezone: 2024-03-14T08:00:00
3. `${start_date}` and `${end_date}`: Represent only the date; however, Oracle does not have a pure date type, so it will include zero hours, minutes, and seconds, e.g., 2024-03-14 00:00:00. Therefore, care must be taken when using `date <= ${end_date}`; it should not include data from the day of 2024-03-14.

To solve the problem of migration data disorder, sorting conditions should be added to the query statement, such as `order by ts asc`.

**Start Time** is the starting time for migrating data; this is a required field.

**End Time** is the end time for migrating data and can be left blank. If set, the migration task will complete automatically when it reaches the end time; if left blank, it will continuously synchronize real-time data, and the task will not automatically stop.

**Query Interval** is the time interval for segmenting queries. The default is 1 day. To avoid querying an excessive amount of data, a sub-task for data synchronization will query data by time segments according to the query interval.

**Delay Duration** is an integer range from 1 to 30; to avoid the loss of delayed written data in real-time synchronization scenarios, each synchronization task will read data before the specified delay duration.

<figure>
<Image img={imgStep05} alt=""/>
</figure>

### 6. Configure Data Mapping

In the **Data Mapping** area, fill in the parameters related to data mapping.

Click the **Retrieve from Server** button to get sample data from the Oracle server.

In the **Extract or Split from Columns** section, fill in the fields to extract or split from the message body. For example, split the `vValue` field into `vValue_0` and `vValue_1` using the split extractor, specifying `,` as the separator and `2` for the number.

In the **Filtering** section, enter filtering conditions; for example, entering `Value > 0` means that only data where Value is greater than 0 will be written to TDengine.

In the **Mapping** section, select the supertable to map to TDengine and specify the columns to map to the supertable.

Click **Preview** to view the mapping results.

<figure>
<Image img={imgStep06} alt=""/>
</figure>

### 7. Configure Advanced Options

The **Advanced Options** area is folded by default; click the `>` button on the right to expand, as shown below:

**Maximum Read Concurrency** limits the number of connections to the data source or the number of reading threads. Modify this parameter when the default parameters do not meet your needs or when you need to adjust resource usage.

**Batch Size** is the maximum number of messages or rows sent at one time. The default is 10,000.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

### 8. Completion

Click the **Submit** button to complete the creation of the data synchronization task from Oracle to TDengine. Return to the **Data Source List** page to view the task execution status.
