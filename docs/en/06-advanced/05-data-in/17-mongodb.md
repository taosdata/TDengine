---
title: MongoDB
slug: /advanced-features/data-connectors/mongodb
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/mongodb-01.png';
import imgStep02 from '../../assets/mongodb-02.png';
import imgStep03 from '../../assets/mongodb-03.png';
import imgStep04 from '../../assets/mongodb-04.png';
import imgStep05 from '../../assets/mongodb-05.png';
import imgStep06 from '../../assets/mongodb-06.png';
import imgStep07 from '../../assets/mongodb-07.png';
import imgStep08 from '../../assets/mongodb-08.png';

This section explains how to create data migration tasks through the Explorer interface to migrate data from MongoDB to the current TDengine cluster.

## Function Overview

MongoDB is a product that sits between relational and non-relational databases and is widely used in various fields such as content management systems, mobile applications, and the Internet of Things. Starting from TDengine Enterprise Edition 3.3.3.0, TDengine can efficiently read data from MongoDB and write it to TDengine for historical data migration or real-time data synchronization, addressing the technical challenges faced by businesses.

## Creating a Task

### 1. Add a Data Source

Click the **+Add Data Source** button in the upper right corner of the data writing page to enter the Add Data Source page, as shown below:

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

In the **Name** field, enter a task name, such as `test_mongodb_01`.

Select `MongoDB` from the **Type** dropdown list, as shown below (the fields on the page will change after selection).

The **Agent** field is optional; if needed, you can select a specified agent from the dropdown or click the **+Create New Agent** button on the right to create a new agent.

The **Target Database** field is required; you can select a specified database from the dropdown or click the **+Create Database** button on the right to create a new database.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

In the **Connection Configuration** area, fill in the *`source MongoDB database connection information`*, as shown below:

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure Authentication Information

In the **User** field, enter the user for the source MongoDB database; this user must have read permissions in the MongoDB system.

In the **Password** field, enter the login password for the user in the source MongoDB database.

In the **Authentication Database** field, enter the database in MongoDB that stores user information, which defaults to admin.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

### 5. Configure Connection Options

In the **Application Name** field, set the application name used to identify the connecting application.

In the **SSL Certificate** field, set whether to use an encrypted connection, which is off by default. If enabled, you need to upload the following two files:

1. **CA File**: Upload the SSL encrypted certificate authorization file.
2. **Certificate File**: Upload the SSL encrypted certificate file.

<figure>
<Image img={imgStep05} alt=""/>
</figure>

Then click the **Check Connectivity** button; users can click this button to check if the information filled in above can successfully retrieve data from the source MongoDB database.

### 6. Configure Data Query

In the **Database** field, specify the source database in MongoDB, and you can use placeholders for dynamic configuration, such as `database_${Y}`. See the table below for the available placeholders.

In the **Collection** field, specify the collection in MongoDB, and you can also use placeholders for dynamic configuration, such as `collection_${md}`. See the table below for the available placeholders.

| Placeholder |                         Description                          | Example Data |
| :---------: | :----------------------------------------------------------: | :----------: |
|      Y      | Complete year in Gregorian calendar, zero-padded 4-digit integer |     2024     |
|      y      | Year in Gregorian calendar divided by 100, zero-padded 2-digit integer |      24      |
|      M      |                    Integer month (1 - 12)                    |      1       |
|      m      |                   Integer month (01 - 12)                    |      01      |
|      B      |              Full name of the month in English               |   January    |
|      b      |       Abbreviation of the month in English (3 letters)       |     Jan      |
|      D      |         Numeric representation of the date (1 - 31)          |      1       |
|      d      |         Numeric representation of the date (01 - 31)         |      01      |
|      J      |                  Day of the year (1 - 366)                   |      1       |
|      j      |                 Day of the year (001 - 366)                  |     001      |
|      F      |                Equivalent to `${Y}-${m}-${d}`                |  2024-01-01  |

The **Subtable Fields** are used to split the subtable fields, which typically correspond to tags in the transform section. Multiple fields are separated by commas, e.g., `col_name1,col_name2`.
This configuration is primarily aimed at solving the problem of data migration disorder and needs to be used in conjunction with the **Query Template**; otherwise, the expected effect cannot be achieved. Usage examples:

1. Configure two subtable fields `col_name1,col_name2`.
2. In the **Query Template**, add placeholders for the subtable fields, for example, `{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}}, ${col_name1}, ${col_name2}}` where `${col_name1}` and `${col_name2}` are the placeholders.
3. In the **transform** section, configure the tag mappings for `col_name1` and `col_name2`.

The **Query Template** is used for querying data. It must be in JSON format and must include time range conditions, with start and end times appearing in pairs. The defined time range in the template is composed of a column representing time from the source database and the placeholders defined below.
Using different placeholders represents different time format requirements, specifically the following placeholder formats:

1. `${start_datetime}` and `${end_datetime}`: Correspond to filtering based on backend datetime type fields, e.g., `{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}}}` will be converted to `{"ddate":{"$gte":{"$date":"2024-06-01T00:00:00+00:00"},"$lt":{"$date":"2024-07-01T00:00:00"}}}`
2. `${start_timestamp}` and `${end_timestamp}`: Correspond to filtering based on backend timestamp type fields, e.g., `{"ttime":{"$gte":${start_timestamp},"$lt":${end_timestamp}}}` will be converted to `{"ttime":{"$gte":{"$timestamp":{"t":123,"i":456}},"$lt":{"$timestamp":{"t":123,"i":456}}}}`

In the **Query Sorting** field, specify sorting conditions for executing the query in JSON format. It must comply with MongoDB's sorting condition format. Example usages:

1. `{"createtime":1}`: Returns MongoDB query results sorted by `createtime` in ascending order.
2. `{"createdate":1, "createtime":1}`: Returns MongoDB query results sorted by `createdate` in ascending order, followed by `createtime` in ascending order.

**Start Time** is the starting time for migrating data; this is a required field.

**End Time** is the end time for migrating data and can be left blank. If set, the migration task will complete automatically when it reaches the end time; if left blank, it will continuously synchronize real-time data, and the task will not automatically stop.

**Query Interval** is the time interval for segmenting queries. The default is 1 day. To avoid querying an excessive amount of data, a sub-task for data synchronization will query data by time segments according to the query interval.

**Delay Duration** is an integer range from 1 to 30; to avoid the loss of delayed written data in real-time synchronization scenarios, each synchronization task will read data before the specified delay duration.

<figure>
<Image img={imgStep06} alt=""/>
</figure>

### 7. Configure Data Mapping

In the **Payload Transformation** area, fill in the parameters related to data mapping.

Click the **Retrieve from Server** button to get sample data from the MongoDB server.

In the **Parsing** section, choose from JSON/Regex/UDT parsing rules for the raw message body; after configuration, click the **Preview** button on the right to view the parsing results.

In the **Extract or Split from Columns** section, fill in the fields to extract or split from the message body. For example, split the `vValue` field into `vValue_0` and `vValue_1` using the split extractor, specifying `,` as the separator and `2` for the number. After configuration, click the **Preview** button on the right to view the transformation results.

In the **Filtering** section, enter filtering conditions; for example, entering `Value > 0` means that only data where Value is greater than 0 will be written to TDengine. After configuration, click the **Preview** button on the right to view the filtering results.

In the **Mapping** section, select the supertable to map to TDengine and specify the columns to map to the supertable. After configuration, click the **Preview** button on the right to view the mapping results.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

### 8. Configure Advanced Options

The **Advanced Options** area is folded by default; click the `>` button on the right to expand, as shown below:

**Maximum Read Concurrency** limits the number of connections to the data source or the number of reading threads. Modify this parameter when the default parameters do not meet your needs or when you need to adjust resource usage.

**Batch Size** is the maximum number of messages or rows sent at one time. The default is 10,000.

<figure>
<Image img={imgStep08} alt=""/>
</figure>

### 9. Completion

Click the **Submit** button to complete the creation of the data synchronization task from MongoDB to TDengine. Return to the **Data Source List** page to view the task execution status.
