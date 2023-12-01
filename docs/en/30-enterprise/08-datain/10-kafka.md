---
title: "Kafka"
sidebar_label: "Kafka"
---
This section explains how to create a data migration task through the Explorer interface to migrate data from Kafka to the current TDengine cluster.

## Functional Overview
Apache Kafka is an open-source distributed streaming system used for stream processing, real-time data pipelines, and data integration at scale.

TDengine can efficiently read the data from Kafka and write to TDengine to achieve historical data migration or real-time data streaming.

## Create Task
### 1. Add Source
In the **Data In** page，click **+Add Source** button to enter the data source page.

![kafka-01.png](./kafka-01.png)

### 2. Configure Basic information
Enter the task name in the ** Name ** field, such as "test_kafka";

In the ** Type ** drop-down list, select **Kafka**.

** Agent ** is not required, if necessary, you can select the specified agent from the drop-down box, you can also click
the right **+ Create New Agent ** button [Create New Agent](#Create New Agent).

Select a target database from the ** Target DB ** drop-down list, or click the **+Create Database** button on the right
[Create Database](#Create Database).

![kafka-02.png](./kafka-02.png)

### 3. Configure Connection information

In the **Connection Configuration** area, fill in the **bootstrap-servers**, for example: `192.168.1.92:9092`.

Click the **Check Connection** button to check whether the data source is available.

![kafka-03.png](./kafka-03.png)

### 4. Configure Collection
In the **Collect** area, fill in the configuration parameters related to the collection task.

In the **Timeout** field, fill in the timeout time. When no data is consumed from Kafka, the data collection task will 
exit after timeout. The unit is milliseconds, and the default value is 500 ms. When the timeout is set to never, it will
wait until data is available or an error occurs.

In the **Topic** field, fill in the Topic name to be consumed. Multiple Topics can be configured, and Topics are 
separated by commas. For example: `tp1,tp2`.

In the **Offset** drop-down list, select which Offset to start consuming data from. There are three options: `Earliest`,
`Latest`, `ByTime(ms)`. The default value is Earliest.
* Earliest: Request the earliest offset.
* Latest: Request the latest offset.
* ByTime: Request all messages after a specific time (in milliseconds); the timestamp is in milliseconds.

In the **Maximum time to wait for data** field, fill in the maximum time in milliseconds to wait for insufficient data 
to become available when fetching messages. (e.g. fetch_max_wait_time=500ms) default is 100ms.

![kafka-04.png](./kafka-04.png)

### 5. Configure Payload Parsing
In the **Kafka Message Parser** area, fill in the configuration parameters related to the payload parsing.

The Kafka data source will upload the following fields:
* ts: the collect timestamp.
* topic: the topic name to subscribe.
* partition: the topic partition.
* offset: the message offset in the topic.
* key: the message offset in the topic.
* value: the data payload of the message.

taosX can extract JSON-formatted data from value and then split it into new columns.

In the **Message Body** field, fill in the sample data in the Kafka message body, for example: 
`{"id": 1, "message": "hello""}`. This sample data will be used to configure the extraction and filtering conditions later.

![kafka-05.png](./kafka-05.png)

In the **Extract or Split From A column** field, fill in the fields to be extracted or split from the message body, for
example: split the value field into `id` and `message` fields, select the json extractor, and configure the expression as
`id::int;message::binary`. 

Click the **Check** button to view the split result. 

Click the **Delete** button to delete the current extraction rule. 

Click the **Add** button to add more extraction rules.

![kafka-06.png](./kafka-06.png)

In the **Filter** field, fill in the filter conditions, for example: fill in `id != 0`, then only the data with id not
equal to 0 will be written to TDengine.

Click the **Check** button to view the filter result.

Click the **Delete** button to delete the current filter rule.

Click the **Add** button to add more filter rules.

![kafka-07.png](./kafka-07.png)

In the **Target Super Table** drop-down list, select a target super table, or click the **+Create STable** button on the
right to [Create Super Table](#Create STable).

In the **Mapping** area, fill in the sub-table name in the target super table, for example: `t_{id}`.

Click the **Calculate** button to view the mapping result.

![kafka-08.png](./kafka-08.png)

### 6. Finish
After completing the above information, click the **Add** button to initiate data synchronization from Kafka to TDengine.

## View Task Status
After submitting the task, you can go back to the data source page to view the task status. The task is first added to 
the execution queue and will start running later.

![kafka-09](./kafka-09.png)

After the task starts running, you can click the **View** button to monitor the dynamic statistics of the task.

![kafka-10](./kafka-10.png)

You can also click the **View** button on the right to view the task details.

![kafka-11](./kafka-11.png)
