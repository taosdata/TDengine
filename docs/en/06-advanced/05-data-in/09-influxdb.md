---
title: InfluxDB
slug: /advanced-features/data-connectors/influxdb
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/influxdb-01.png';
import imgStep02 from '../../assets/influxdb-02.png';
import imgStep03 from '../../assets/influxdb-03.png';
import imgStep04 from '../../assets/influxdb-04.png';
import imgStep05 from '../../assets/influxdb-05.png';
import imgStep06 from '../../assets/influxdb-06.png';
import imgStep07 from '../../assets/influxdb-07.png';
import imgStep08 from '../../assets/influxdb-08.png';
import imgStep09 from '../../assets/influxdb-09.png';
import imgStep10 from '../../assets/influxdb-10.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create a data migration task through the Explorer interface to migrate data from InfluxDB to the current TDengine cluster.

## Feature Overview

InfluxDB is a popular open-source time-series database optimized for handling large volumes of time-series data. TDengine can efficiently read data from InfluxDB through the InfluxDB connector and write it into TDengine, enabling historical data migration or real-time data synchronization.

The task saves progress information to the disk during operation, so if the task is paused and restarted, or if it automatically recovers from an anomaly, it will not start over. For more options, it is recommended to read the explanations of each form field on the task creation page in detail.

## Creating a Task

### 1. Add a Data Source

Click the **+ Add Data Source** button in the upper left corner of the data writing page to enter the add data source page, as shown below:

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

Enter the task name in the **Name** field, for example *`test_influxdb_01`*.

Select *`InfluxDB`* from the **Type** dropdown menu, as shown below (the fields on the page will change after selection).

**Proxy** is optional. If needed, you can select a specific proxy from the dropdown menu, or click the **+ Create New Proxy** button on the right.

**Target Database** is required. Since InfluxDB can store data with time precision of seconds, milliseconds, microseconds, and nanoseconds, you need to select a *`nanosecond precision database`* here, or click the **+ Create Database** button on the right.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

Fill in the *`connection information for the source InfluxDB database`* in the **Connection Configuration** area, as shown below:

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure Authentication Information

In the **Authentication** area, there are two tabs, *`1.x version`* and *`2.x version`*, due to different authentication parameters and significant API differences between different versions of InfluxDB databases. Please choose according to the actual situation:  
  *`1.x version`*  
  **Version** Select the version of the source InfluxDB database from the dropdown menu.  
  **User** Enter the user of the source InfluxDB database, who must have read permissions in that organization.  
  **Password** Enter the login password for the above user in the source InfluxDB database.

  <figure>
  <Image img={imgStep04} alt=""/>
  </figure>

  *`2.x version`*  
  **Version** Select the version of the source InfluxDB database from the dropdown menu.  
  **Organization ID** Enter the organization ID of the source InfluxDB database, which is a string of hexadecimal characters, not the organization name, and can be obtained from the InfluxDB console's Organization->About page.  
  **Token** Enter the access token for the source InfluxDB database, which must have read permissions in that organization.  
  **Add Database Retention Policy** This is a *`Yes/No`* toggle. InfluxQL requires a combination of database and retention policy (DBRP) to query data. The cloud version of InfluxDB and some 2.x versions require manually adding this mapping. Turn on this switch, and the connector can automatically add it when executing tasks.  

  <figure>
  <Image img={imgStep05} alt=""/>
  </figure>
Below the **Authentication** area, there is a **Connectivity Check** button. Users can click this button to check if the information filled in above can normally access the data of the source InfluxDB database. The check results are shown below:  

  <figure>
  <Image img={imgStep06} alt=""/>
  <figcaption>Failed</figcaption>
  </figure>

  <figure>
  <Image img={imgStep07} alt=""/>
  <figcaption>Successful</figcaption>
  </figure>

### 5. Configure Task Information

**Bucket** is a named space in the InfluxDB database for storing data. Each task needs to specify a bucket. Users need to first click the **Get Schema** button on the right to obtain the data structure information of the current source InfluxDB database, and then select from the dropdown menu as shown below:

<figure>
<Image img={imgStep08} alt=""/>
</figure>

**Measurements** are optional. Users can select one or more Measurements to synchronize from the dropdown menu. If none are specified, all will be synchronized.

**Start Time** refers to the start time of the data in the source InfluxDB database. The timezone for the start time uses the timezone selected in explorer, and this field is required.

**End Time** refers to the end time of the data in the source InfluxDB database. If no end time is specified, synchronization of the latest data will continue; if an end time is specified, synchronization will only continue up to this end time. The timezone for the end time uses the timezone selected in explorer, and this field is optional.

**Time Range per Read (minutes)** is the maximum time range for the connector to read data from the source InfluxDB database in a single read. This is a very important parameter, and users need to decide based on server performance and data storage density. If the range is too small, the execution speed of the synchronization task will be very slow; if the range is too large, it may cause the InfluxDB database system to fail due to high memory usage.

**Delay (seconds)** is an integer between 1 and 30. To eliminate the impact of out-of-order data, TDengine always waits for the duration specified here before reading data.

### 6. Configure Advanced Options

The **Advanced Options** area is collapsed by default. Click the `>` on the right to expand it, as shown below:

<figure>
<Image img={imgStep09} alt=""/>
</figure>

<figure>
<Image img={imgStep10} alt=""/>
</figure>

### 7. Completion of Creation

Click the **Submit** button to complete the creation of the data synchronization task from InfluxDB to TDengine. Return to the **Data Source List** page to view the status of the task execution.
