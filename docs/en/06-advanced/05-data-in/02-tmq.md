---
title: TDengine 3.x
slug: /advanced-features/data-connectors/tdengine-3
---

This document explains how to use the Explorer interface to subscribe to data from another cluster into the current one.

## Preparation

Create the necessary subscription topic on the source cluster. You can subscribe to the entire database, a super table, or a sub-table. In this example, we will demonstrate subscribing to a database named `test`.

### Step 1: Access the Data Subscription page

Open the Explorer interface for the source cluster, click on the "Data Subscription" menu on the left, and then click on "Add New Topic."

![Preparation Step 1](../../assets/tdengine-3-01.png)

### Step 2: Add a New Topic

Enter the topic name and select the database you want to subscribe to.

![Preparation Step 2](../../assets/tdengine-3-02.png)

### Step 3: Copy the Topic's DSN

Click the "Create" button, go back to the topic list, and copy the topic's **DSN** for later use.

![Preparation Step 3](../../assets/tdengine-3-03.png)

## Create a Subscription Task

### Step 1: Go to the "Add Data Source" page

1. Click the "Data Ingestion" menu on the left.
2. Click "Add Data Source."

![Step 1](../../assets/tdengine-3-04.png)

### Step 2: Enter Data Source Information

1. Enter the task name.
2. Select the task type "TDengine3."
3. Choose the target database.
4. Paste the DSN copied from the preparation step into the **Topic DSN** field. For example: `tmq+ws://root:taosdata@localhost:6041/topic`
5. After completing the above steps, click the "Connectivity Check" button to test connectivity with the source.

![Step 2](../../assets/tdengine-3-05.png)

### Step 3: Configure Subscription Settings and Submit the Task

1. Choose the subscription starting point. You can configure it to start from the earliest or latest data, with the default being the earliest.
2. Set the timeout. Supported units include ms (milliseconds), s (seconds), m (minutes), h (hours), d (days), M (months), y (years).
3. Set the subscription group ID. The subscription group ID is an arbitrary string used to identify a subscription group, with a maximum length of 192 characters. Subscribers within the same group share consumption progress. If not specified, a randomly generated group ID will be used.
4. Set the client ID. The client ID is an arbitrary string used to identify the client, with a maximum length of 192 characters.
5. Synchronize data that has already been written to disk. If enabled, it will synchronize data that has already been flushed to the TSDB storage file (i.e., not in the WAL). If disabled, it will only synchronize data that has not yet been flushed (i.e., still in the WAL).
6. Synchronize table deletion operations. If enabled, it will synchronize table deletion operations to the target database.
7. Synchronize data deletion operations. If enabled, it will synchronize data deletion operations to the target database.
8. Compression. Enable WebSocket compression to reduce network bandwidth usage.
9. Click the "Submit" button to submit the task.

![Step 3](../../assets/tdengine-3-06.png)

## Monitoring Task Progress

After submitting the task, return to the data source page to view the task status. The task will first be added to the execution queue and will start running shortly after.
![Step 4](../../assets/tdengine-3-07.png)

Click the "View" button to monitor dynamic statistical information about the task.
![Step 5](../../assets/tdengine-3-08.png)

You can also click the collapse button on the left to expand the task's activity information. If the task encounters any issues, detailed explanations will be provided here.
![Step 6](../../assets/tdengine-3-09.png)

## Advanced Usage

1. The FROM DSN supports multiple Topics, separated by commas. For example: `tmq+ws://root:taosdata@localhost:6041/topic1,topic2,topic3`
2. In the FROM DSN, you can also use database names, super table names, or sub-table names in place of the Topic names. For example: `tmq+ws://root:taosdata@localhost:6041/db1,db2,db3`. In this case, it is not necessary to create Topics in advance; `taosX` will automatically recognize the use of database names and create the database subscription Topics in the source cluster.
3. The FROM DSN supports the `group.id` parameter to explicitly specify the group ID for the subscription. If not specified, a randomly generated group ID will be used.
