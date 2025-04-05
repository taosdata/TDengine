---
title: TDengine 3.x
slug: /advanced-features/data-connectors/tdengine-3
---

import Image from '@theme/IdealImage';
import imgStep1 from '../../assets/tdengine-3-01.png';
import imgStep2 from '../../assets/tdengine-3-02.png';
import imgStep3 from '../../assets/tdengine-3-03.png';
import imgStep4 from '../../assets/tdengine-3-04.png';
import imgStep5 from '../../assets/tdengine-3-05.png';
import imgStep6 from '../../assets/tdengine-3-06.png';
import imgStep7 from '../../assets/tdengine-3-07.png';
import imgStep8 from '../../assets/tdengine-3-08.png';
import imgStep9 from '../../assets/tdengine-3-09.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This document describes how to use Explorer to subscribe to data from another cluster to this cluster.

## Preparation

Create the required Topic in the source cluster, which can subscribe to the entire database, supertable, or subtable. In this example, we demonstrate subscribing to a database named test.

### Step One: Enter the "Data Subscription" page

Open the Explorer interface of the source cluster, click the "Data Subscription" menu on the left, then click "Add New Topic".

<figure>
<Image img={imgStep1} alt=""/>
</figure>

### Step Two: Add a New Topic

Enter the topic name, select the database to subscribe to.

<figure>
<Image img={imgStep2} alt=""/>
</figure>

### Step Three: Copy the Topic's DSN

Click the "Create" button, return to the topic list and copy the **DSN** of the topic for later use.

<figure>
<Image img={imgStep3} alt=""/>
</figure>

## Create Subscription Task

### Step One: Enter the "Add Data Source" page

1. Click the "Data Writing" menu on the left
2. Click "Add Data Source"

<figure>
<Image img={imgStep4} alt=""/>
</figure>

### Step Two: Enter Data Source Information

1. Enter the task name
2. Select the task type "TDengine3"
3. Select the target database
4. Paste the DSN copied in the preparation step into the **Topic DSN** field. For example: tmq+ws://root:taosdata@localhost:6041/topic
5. After completing the above steps, click the "Connectivity Check" button to test connectivity with the source

<figure>
<Image img={imgStep5} alt=""/>
</figure>

### Step Three: Fill in Subscription Settings and Submit Task

1. Choose the subscription start position. Configurable to start from the earliest or latest data, default is earliest
2. Set the timeout period. Supports units ms (milliseconds), s (seconds), m (minutes), h (hours), d (days), M (months), y (years)
3. Set the subscription group ID. The subscription group ID is an arbitrary string used to identify a subscription group, with a maximum length of 192. If not specified, a randomly generated group ID will be used.
4. Set the client ID. The client ID is an arbitrary string used to identify the client, with a maximum length of 192.
5. Synchronize data that has been written to disk. If enabled, it can synchronize data that has been written to the TSDB time-series data storage file (i.e., not in WAL). If disabled, only data that has not yet been written to disk (i.e., saved in WAL) will be synchronized.
6. Synchronize table deletion operations. If enabled, table deletion operations will be synchronized to the target database.
7. Synchronize data deletion operations. If enabled, data deletion operations will be synchronized to the target database.
8. Compression. Enable WebSocket compression support to reduce network bandwidth usage.
9. Click the "Submit" button to submit the task

<figure>
<Image img={imgStep6} alt=""/>
</figure>

## Monitor Task Execution

After submitting the task, return to the data source page to view the task status. The task will first be added to the execution queue and will start running shortly.

<figure>
<Image img={imgStep7} alt=""/>
</figure>

Click the "View" button to monitor the dynamic statistical information of the task.

<figure>
<Image img={imgStep8} alt=""/>
</figure>

You can also click the left collapse button to expand the task's activity information. If the task runs abnormally, detailed explanations can be seen here.

<figure>
<Image img={imgStep9} alt=""/>
</figure>

## Advanced Usage

1. FROM DSN supports multiple Topics, with multiple Topic names separated by commas. For example: `tmq+ws://root:taosdata@localhost:6041/topic1,topic2,topic3`
2. In the FROM DSN, you can also use the database name, supertable name, or subtable name instead of the Topic name. For example: `tmq+ws://root:taosdata@localhost:6041/db1,db2,db3`, in this case, there is no need to create a Topic in advance, taosX will automatically recognize that a database name is used and automatically create a subscription Topic in the source cluster.
3. FROM DSN supports the group.id parameter, to explicitly specify the group ID used for subscription. If not specified, a randomly generated group ID will be used.
