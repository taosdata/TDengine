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
3. In the **bootstrap-servers** field, configure Kafaka's bootstrap servers, for example, 192.168.1.92:9092, this field is required.
4. You can click the **Connectivity Check** button to check whether the connectivity between the Cloud instance and the Kafka service is available.
5. In the **Enable SSL** field, if SSL authentication is enabled, please upload the corresponding client certificate and client private key files.
6. In the **Consumer** field, you need to configure the timeout time of the consumer, the ID of the consumer group, and other parameters, and fill in at least one of the two parameters, topic and topic_partitions, and the other parameters have default values.
7. If the consumed Kafka data is in JSON format, you can configure the **Kafka Message Parser** part to parse and convert the data.
8. After filling in the above information, click the Submit button to start the data synchronization from Kafka to TDengine.
