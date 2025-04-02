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

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create data migration tasks through the Explorer interface, migrating data from MongoDB to the current TDengine cluster.

## Feature Overview

MongoDB is a product that lies between relational and non-relational databases, widely used in content management systems, mobile applications, and the Internet of Things, among other fields. Starting from TDengine Enterprise Edition 3.3.3.0, TDengine can efficiently read data from MongoDB and write it into TDengine, achieving historical data migration or real-time data synchronization, and addressing technical pain points faced by businesses.

## Creating a Task

### 1. Add a Data Source

Click the **+ Add Data Source** button in the top right corner of the data writing page to enter the Add Data Source page, as shown below:

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

Enter the task name in the **Name** field, for example `test_mongodb_01`.

Select `MongoDB` from the **Type** dropdown menu, as shown below (the fields on the page will change after selection).

**Proxy** is optional. If needed, you can select a specific proxy from the dropdown menu, or click the **+ Create New Proxy** button on the right to create a new proxy.

**Target Database** is mandatory. You can select a specific database from the dropdown menu, or click the **+ Create Database** button on the right to create a new database.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

Fill in the *connection information for the source MongoDB database* in the **Connection Configuration** area, as shown below:

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure Authentication Information

**User** Enter the user of the source MongoDB database, who must have read permissions in the MongoDB system.

**Password** Enter the login password for the user mentioned above in the source MongoDB database.

**Authentication Database** The database in MongoDB where user information is stored, default is admin.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

### 5. Configure Connection Options

**Application Name** Set the application name to identify the connected application.

**SSL Certificate** Set whether to use an encrypted connection, which is off by default. If enabled, you need to upload the following two files:

&emsp; 1. **CA File** Upload the SSL encryption certificate authority file.

&emsp; 2. **Certificate File** Upload the SSL encryption certificate file.

<figure>
<Image img={imgStep05} alt=""/>
</figure>

Then click the **Check Connectivity** button, where users can click this button to check if the information filled in above can normally retrieve data from the source MongoDB database.

### 6. Configure Data Query

**Database** The source database in MongoDB, which can be dynamically configured using placeholders, such as `database_${Y}`. See the table below for a list of available placeholders.

**Collection** The collection in MongoDB, which can be dynamically configured using placeholders, such as `collection_${md}`. See the table below for a list of available placeholders.

|Placeholder|Description|Example Data|
| :-----: | :------------: |:--------:|
|Y|Complete Gregorian year, zero-padded 4-digit integer|2024|
|y|Gregorian year divided by 100, zero-padded 2-digit integer|24|
|M|Integer month (1 - 12)|1|
|m|Integer month (01 - 12)|01|
|B|Full English spelling of the month|January|
|b|Abbreviation of the month in English (3 letters)|Jan|
|D|Numeric representation of the date (1 - 31)|1|
|d|Numeric representation of the date (01 - 31)|01|
|J|Day of the year (1 - 366)|1|
|j|Day of the year (001 - 366)|001|
|F|Equivalent to `${Y}-${m}-${d}`|2024-01-01|

**Subtable Fields** Fields used to split subtables, usually corresponding to tags in transform, separated by commas, such as col_name1,col_name2.
This configuration is mainly to solve the problem of data migration disorder, and needs to be used in conjunction with **Query Template**, otherwise it cannot achieve the expected effect. Usage examples are as follows:

1. Configure two subtable fields `col_name1,col_name2`
2. Add subtable field placeholders in the **Query Template**, such as the `${col_name1}, ${col_name2}` part in `{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}}, ${col_name1}, ${col_name2}}`
3. Configure `col_name1` and `col_name2` two tag mappings in **transform**

**Query Template** is used for querying data with a JSON format query statement, which must include a time range condition, and the start and end times must appear in pairs. The time range defined in the template consists of a time-representing column from the source database and the placeholders defined below.
Different placeholders represent different time format requirements, specifically the following placeholder formats:

1. `${start_datetime}`, `${end_datetime}`: Corresponds to filtering by the backend datetime type field, e.g., `{"ddate":{"$gte":${start_datetime},"$lt":${end_datetime}}}` will be converted to `{"ddate":{"$gte":{"$date":"2024-06-01T00:00:00+00:00"},"$lt":{"$date":"2024-07-01T00:00:00+00:00"}}}`
2. `${start_timestamp}`, `${end_timestamp}`: Corresponds to filtering by the backend timestamp type field, e.g., `{"ttime":{"$gte":${start_timestamp},"$lt":${end_timestamp}}}` will be converted to `{"ttime":{"$gte":{"$timestamp":{"t":123,"i":456}},"$lt":{"$timestamp":{"t":123,"i":456}}}}`

**Query Sorting** Sorting conditions during query execution, in JSON format, must comply with MongoDB's sorting condition format specifications, with usage examples as follows:

1. `{"createtime":1}`: MongoDB query results are returned in ascending order by createtime.
2. `{"createdate":1, "createtime":1}`: MongoDB query results are returned in ascending order by createdate and createtime.

**Start Time** The start time for migrating data, this field is mandatory.

**End Time** The end time for migrating data, can be left blank. If set, the migration task will stop automatically after reaching the end time; if left blank, it will continuously synchronize real-time data, and the task will not stop automatically.

**Query Interval** The time interval for segmenting data queries, default is 1 day. To avoid querying too much data at once, a data synchronization subtask will use the query interval to segment the data.

**Delay Duration** In real-time data synchronization scenarios, to avoid losing data due to delayed writes, each synchronization task will read data from before the delay duration.

<figure>
<Image img={imgStep06} alt=""/>
</figure>

### 7. Configure Data Mapping

Fill in the data mapping related configuration parameters in the **Payload Transformation** area.

Click the **Retrieve from Server** button to fetch sample data from the MongoDB server.

In **Parsing**, choose from JSON/Regex/UDT to parse the original message body, and click the **Preview** button on the right to view the parsing results after configuration.

In **Extract or Split from Column**, fill in the fields to extract or split from the message body, for example: split the `vValue` field into `vValue_0` and `vValue_1`, select the split extractor, fill in the separator as `,`, number as 2, and click the **Preview** button on the right to view the transformation results after configuration.

In **Filter**, fill in the filtering conditions, for example: write `Value > 0`, then only data where Value is greater than 0 will be written to TDengine, and click the **Preview** button on the right to view the filtering results after configuration.

In **Mapping**, select the supertable in TDengine to which the data will be mapped, as well as the columns to map to the supertable, and click the **Preview** button on the right to view the mapping results after configuration.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

### 8. Configure Advanced Options

The **Advanced Options** area is collapsed by default, click the `>` on the right to expand it, as shown below:

**Maximum Read Concurrency** Limit on the number of data source connections or reading threads, modify this parameter when default parameters do not meet the needs or when adjusting resource usage.

**Batch Size** The maximum number of messages or rows sent at once. Default is 10000.

<figure>
<Image img={imgStep08} alt=""/>
</figure>

### 9. Completion

Click the **Submit** button to complete the creation of the data synchronization task from MongoDB to TDengine, and return to the **Data Source List** page to view the task execution status.
