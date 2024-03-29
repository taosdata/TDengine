---
sidebar_label: Kafka
title: Kafka Data Source
description: This document describes how to extract data from Kafka into a TDengine Cloud instance.
---

MQTT data source is the process of writing data from the Kafka server to the currently selected TDengine Cloud instance via a connection agent.

## Prerequisites

- Create an empty database to store your Kafka data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your OPC Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **Kafka**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. In the **bootstrap-servers** field, configure Kafka's bootstrap servers, for example, 192.168.1.92:9092, this field is required.
4. You can click the **Connectivity Check** button to check whether the connectivity between the Cloud instance and the Kafka service is available.
5. In the **Enable SSL** field, if SSL authentication is enabled, please upload the corresponding client certificate and client private key files.
6. In the **Consumer Configuration** field, you need to configure the timeout time of the consumer, the ID of the consumer group, and other parameters,
   - In the **Timeout** field, fill in the timeout time. When no data is consumed from Kafka, the data collection task will exit after timeout. The unit is milliseconds, and the default value is 500 ms. When the timeout is set to never, it will wait until data is available or an error occurs.
   - In the **Topic** field, fill in the Topic name to be consumed. Multiple Topics can be configured, and Topics are separated by commas. For example: `tp1,tp2`.
   - In the **Offset** drop-down list, select which Offset to start consuming data from. There are three options: `Earliest`, `Latest`, `ByTime(ms)`. The default value is Earliest. Earliest: Request the earliest offset, Latest: Request the latest offset, ByTime: Request all messages after a specific time (in milliseconds); the timestamp is in milliseconds.
   - In the **Maximum time to wait for data** field, fill in the maximum time in milliseconds to wait for insufficient data to become available when fetching messages. (e.g. fetch_max_wait_time=500ms) default is 100ms.
7. You can click the **Connectivity Check** button to check whether the connectivity between the Cloud instance and the Kafka service is available.
8. If the consumed Kafka data is in JSON format, you can configure the **Kafka Message Parser** card to parse and convert the data.
   - In the **Message Body** field, fill in the sample data in the Kafka message body, for example: `{"id": 1, "message": "hello-word"}{"id": 2, "message": "hello-word"}`. This sample data will be used to configure the extraction and filtering conditions later. Click the **Preview** button to preview parse results.
   - In the **Extract or Split From A column** field, fill in the fields to be extracted or split from the message body, for example: split the `message` field into `message_0` and `message_1` fields, with `split` Extractor, separator filled as `-`, number filled as `2`. Click the **Delete** button to delete the current extraction rule. Click the **Add** button to add more extraction rules. Click the **Preview** button to view the Extract or Split From A column result.
   - In the **Filter** field, fill in the filter conditions, for example: fill in `id != 1`, then only the data with id not equal to 1 will be written to TDengine. Click the **Check** button to view the filter result. Click the **Delete** button to delete the current filter rule. Click the **Preview** button to view the filter result.
   - In the **Target Super Table** drop-down list, select a target super table, or click the **+Create STable** button on the right to [Create Super Table](#Create STable). In the **Mapping** area, fill in the sub-table name in the target super table, for example: `t_{id}`. Click the **Preview** button to view the mapping result.
9. In the **Advanced Options** card, you can configure the following information:
   - Configure the maximum number of concurrent reads in **Read Concurrency** when the connector reads data from the source Kafka database.
10. After filling in the above information, click the **Submit** button to start the data synchronization from Kafka to TDengine.
