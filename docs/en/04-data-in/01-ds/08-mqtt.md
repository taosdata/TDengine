---
sidebar_label: MQTT
title: MQTT Data Source
description: This document describes how to extract data from MQTT into a TDengine Cloud instance.
---

MQTT data source is the process of writing data from the MQTT server to the currently selected TDengine Cloud instance via a connection agent.

## Prerequisites

- Create an empty database to store your MQTT data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your OPC Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **MQTT**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. In the **MQTT Server Endpoint** part, enter the MQTT address, mandatory fields, including IP and port number, for example: 192.168.1.10:1883.
4. You can click the **Connectivity Check** button to check whether the communication between the Cloud instance and the MQTT server is available.
5. in the **Enable SSL** part, you can choose whether to turn on the SSL/TLS switch, if you turn on this switch, the communication between MQTT Connector and MQTT Server will be encrypted by SSL/TLS; after you turn on this switch, there will appear the three required configuration items of CA, Client Certificate and Client Private Key, where you can enter the content of the certificate and private key files.
6. In the **Connection** part, you can configure the following information:
   - MQTT Protocol Version: Supports 3.1/3.1.1/5.0 versions.
   - Client ID: The client ID used by the MQTT connector to connect to the MQTT server, used to identify the client.
   - Keep Alive: Used to configure the Keep Alive time between the MQTT Connector and the MQTT Server, the default value is 60 seconds.
   - Clean Session: Used to configure whether the MQTT Connector connects to the MQTT server as a Clean Session, the default value is True.
   - Topic QoS Config: here is used to configure the MQTT topic for listening and the maximum QoS supported by the topic, the configuration of topic and QoS is separated by ::, and the configuration of multiple topics is separated by ,, and the configuration of topic can support MQTT protocol wildparts # and +.
7. You can click the **Connectivity Check** button to check whether the communication between the Cloud instance and the MQTT server is available.
8. In the **MQTT Payload Parser** area, fill in the configuration parameters related to the payload parsing.
   - In the **Message Body** field, fill in the sample data in the MQTT message body, for example: `{"id": 1, "message": "hello-word"}{"id": 2, "message": "hello-word"}`. This sample data will be used to configure the extraction and filtering conditions later. Click the **Preview** button to preview parse results.
   - In the **Extract or Split From A column** field, fill in the fields to be extracted or split from the message body, for example: split the `message` field into `message_0` and `message_1` fields, with `split` Extractor, separator filled as `-`, number filled as `2`. Click the **Delete** button to delete the current extraction rule. Click the **Add** button to add more extraction rules. Click the **Preview** button to view the Extract or Split From A column result.
   - In the **Filter** field, enter the filter conditions. For example, input `id != 1`, then only the data with an id not equal to 1 will be written to TDengine. Click the **Delete** button to delete the current filter rule. Click the **Preview** button to view the filter result.
   - In the **Target Super Table** dropdown list, select a target super table, or click the **+Create STable** button on the right to [Create Super Table](#Create STable).
   - In the **Mapping** area, fill in the sub-table name in the target super table, for example: `t_{id}`. Click the **Preview** button to view the mapping result.
9. In the **Advanced Options** area, you can configure the following information:
   - **Log Level**: Configure the log level of the connector, support error, warn, info, debug, trace 5 levels, the default value is info.
   - **Keep Raw Data**: When the **Keep Raw Data** is enabled, the following two parameters take effect.
   - **Max Keep Days**: sets the maximum retention days of the raw data.
   - **Raw Data Directory**: sets the path to save the raw data.
10. After completing the above information, click the **Add** button to start the data synchronization from MQTT to TDengine directly.
