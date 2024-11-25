---
title: AVEVA Historian
slug: /advanced-features/data-connectors/aveva-historian
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/aveva-historian-01.png';
import imgStep02 from '../../assets/aveva-historian-02.png';
import imgStep03 from '../../assets/aveva-historian-03.png';
import imgStep04 from '../../assets/aveva-historian-04.png';
import imgStep05 from '../../assets/aveva-historian-05.png';
import imgStep06 from '../../assets/aveva-historian-06.png';
import imgStep07 from '../../assets/aveva-historian-07.png';
import imgStep08 from '../../assets/aveva-historian-08.png';

This section explains how to create data migration/data synchronization tasks through the Explorer interface to migrate/synchronize data from AVEVA Historian to the current TDengine cluster.

## Function Overview

AVEVA Historian is an industrial big data analytics software, formerly known as Wonderware. It captures and stores high-fidelity industrial big data, unlocking constrained potential to improve operations.

TDengine can efficiently read data from AVEVA Historian and write it to TDengine for historical data migration or real-time data synchronization.

## Creating a Task

### 1. Add a Data Source

Click the **+Add Data Source** button on the data writing page to enter the Add Data Source page.

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

In the **Name** field, enter a task name, such as: "test_avevaHistorian";

Select **AVEVA Historian** from the **Type** dropdown list.

The **Agent** field is optional; if needed, you can select a specified agent from the dropdown, or click the **+Create New Agent** button on the right.

In the **Target Database** dropdown list, select a target database, or click the **+Create Database** button on the right.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

In the **Connection Configuration** area, fill in the **Server Address** and **Server Port**.

In the **Authentication** area, fill in the **Username** and **Password**.

Click the **Connectivity Check** button to check if the data source is available.

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure Data Collection Information

In the **Collection Configuration** area, fill in the parameters related to the collection task.

#### 4.1. Migrate Data

To perform data migration, configure the following parameters:

Select **migrate** from the **Collection Mode** dropdown list.

In the **Tags** field, enter the list of tags to migrate, separated by commas (,).

In the **Tag Group Size** field, specify the size of the tag group.

In the **Task Start Time** field, enter the start time for the data migration task.

In the **Task End Time** field, enter the end time for the data migration task.

In the **Query Time Window** field, specify a time interval; the data migration task will segment the time window according to this interval.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

#### 4.2. Synchronize Data from the History Table

To synchronize data from the **Runtime.dbo.History** table to TDengine, configure the following parameters:

Select **synchronize** from the **Collection Mode** dropdown list.

In the **Table** field, select **Runtime.dbo.History**.

In the **Tags** field, enter the list of tags to migrate, separated by commas (,).

In the **Tag Group Size** field, specify the size of the tag group.

In the **Task Start Time** field, enter the start time for the data migration task.

In the **Query Time Window** field, specify a time interval; the historical data part will segment according to this time interval.

In the **Real-Time Synchronization Interval** field, specify a time interval for polling real-time data.

In the **Out-of-Order Time Limit** field, specify a time interval; data that arrives later than this interval may be lost during real-time synchronization.

<figure>
<Image img={imgStep05} alt=""/>
</figure>

#### 4.3. Synchronize Data from the Live Table

To synchronize data from the **Runtime.dbo.Live** table to TDengine, configure the following parameters:

Select **synchronize** from the **Collection Mode** dropdown list.

In the **Table** field, select **Runtime.dbo.Live**.

In the **Tags** field, enter the list of tags to migrate, separated by commas (,).

In the **Real-Time Synchronization Interval** field, specify a time interval for polling real-time data.

<figure>
<Image img={imgStep06} alt=""/>
</figure>

### 5. Configure Data Mapping

In the **Data Mapping** area, fill in the parameters related to data mapping.

Click the **Retrieve from Server** button to get sample data from the AVEVA Historian server.

In the **Extract or Split from Columns** section, fill in the fields to extract or split from the message body. For example, split the `vValue` field into `vValue_0` and `vValue_1` using the split extractor, specifying `,` as the separator and `2` for the number.

In the **Filtering** section, enter filtering conditions; for example, entering `Value > 0` means that only data where Value is greater than 0 will be written to TDengine.

In the **Mapping** section, select the supertable to map to TDengine, and specify the columns to map to the supertable.

Click **Preview** to view the mapping results.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

### 6. Configure Advanced Options

In the **Advanced Options** area, fill in the parameters related to advanced options.

In the **Maximum Read Concurrency** field, set the maximum read concurrency. The default value is 0, which means auto, automatically configuring the concurrency.

In the **Batch Size** field, set the batch size for each write, that is, the maximum number of messages sent at one time.

In the **Save Raw Data** section, choose whether to save the raw data. The default is no.

When saving raw data, the following two parameters take effect.

In the **Maximum Retention Days** field, set the maximum retention days for the raw data.

In the **Raw Data Storage Directory** field, set the path to save the raw data.

<figure>
<Image img={imgStep08} alt=""/>
</figure>

### 7. Completion

Click the **Submit** button to complete the task creation. After submitting the task, return to the **Data Writing** page to check the task status.
