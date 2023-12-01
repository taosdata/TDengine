---
title: "TDEngine Subscription"
sidebar_label: "TDengine subscription"
---

This article describes how to use Explorer to subscribe data from another cluster to this cluster.

## Preparation work

Create the Topic required for subscription in the source cluster, which can subscribe to the entire library, super table, or sub-table. In this example, we demonstrate subscribing to a database named test.

### Step 1: Go to the "Data Subscription" page
Open the Explorer interface of the open source cluster, click the "Data Subscription" menu on the left, and then click "Add New Topic".

![Prepare one](./tmq-pre1.png)

### Step 2: Add a new topic
Enter the topic name and select the database to subscribe to.

![Preparation 2](./tmq-pre2.png)

### Step 3: Copy the DSN of the topic.

Click the "Create" button to return to the topic list and copy the ** DSN ** of the topic as a backup.

![Prepare three](./tmq-pre3.png)

## Create a subscription task

### Step 1: Go to the "New Data Source" page
1. Click the "Data Write" menu on the left
2. Click "Add Data Source"
![Step 1](./tmq-step1.png)

### Step 2: Enter data source information
1. Enter a task name
2. Select task type "TDengine subscription"
3. Select target database
4. Paste the DSN copied by the preparation steps into the field of **Topic DSN**. For example: `tmq+ws://root:taosdata@localhost:6041/topic`
5. Complete the above steps and click the "Connectivity Check" button to test the connectivity with the source end.
![Step 2](./tmq-step2.png)

### Step 3: Fill in subscription settings and submit tasks

1. Select the initial subscription location. You can configure to subscribe from the earliest or latest data, with the default being earliest.
2. Set timeout time. Support units ms (milliseconds), s (seconds), m (minutes), h (hours), d (days), M (months), y (years).
3. Click the "Add button" to submit a task
![Step 3](./tmq-step3.png)

## Monitor task running status

After submitting the task, you can return to the data source page to view the task status. The task will be added to the execution queue first and will start running later.
![Step 4](./tmq-step4.png)

Click the "View" button to monitor the dynamic statistics of the task.
![Step 5](./tmq-step5.png)

You can also click the Collapse button on the left to expand the activity information of the task. If the task runs abnormally, you can see the detailed description here.
![Step 6](./tmq-step6.png)

## Advanced Usage

1. Topic DSN supports multiple Topics, and the names of multiple Topics are separated by commas. For example: 'tmq + ws://root: taosdata@localhost: 6041/topic1, topic2, topic3'
2. Topic DSN, you can  also use database name, super table name or sub-table name instead of Topic name. For example: `tmq + ws://root:taosdata@localhost:6041/db1,db2,db3`. At this time, it is not necessary to create Topic in advance. TaosX will automatically recognize that the database name is used and automatically create a Topic that subscribes to the database in the source cluster.
3. FROM DSN supports group.id parameters to explicitly specify the group ID for the subscription. If not specified, the randomly generated group ID will be used.
