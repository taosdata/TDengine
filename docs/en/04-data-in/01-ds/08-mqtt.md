---
sidebar_label: MQTT
title: MQTT Data Source
description: This document describes how to extract data from MQTT into a TDengine Cloud instance.
---

MQTT data source is the process of writing data from the MQTT server to the currently selected TDengine Cloud instance via a connection agent.

## Prerequisites

- Create an empty database to store your MQTT data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your OPC  Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **MQTT**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. In the **MQTT Server Endpoint** part, enter the MQTT address, mandatory fields, including IP and port number, for example: 192.168.1.10:1883.
4. in the **Enable SSL** part, you can choose whether to turn on the SSL/TLS switch, if you turn on this switch, the communication between MQTT Connector and MQTT Server will be encrypted by SSL/TLS; after you turn on this switch, there will appear the three required configuration items of CA, Client Certificate and Client Private Key, where you can enter the content of the certificate and private key files.
5. In the **Connection** part, you can configure the following information:
    - MQTT Protocol Version: Supports 3.1/3.1.1/5.0 versions.
    - Client ID: The client ID used by the MQTT connector to connect to the MQTT server, used to identify the client.
    - Keep Alive: Used to configure the Keep Alive time between the MQTT Connector and the MQTT Server, the default value is 60 seconds.
    - Clean Session: Used to configure whether the MQTT Connector connects to the MQTT server as a Clean Session, the default value is True.
    - Topic QoS Config: here is used to configure the MQTT topic for listening and the maximum QoS supported by the topic, the configuration of topic and QoS is separated by ::, and the configuration of multiple topics is separated by ,, and the configuration of topic can support MQTT protocol wildparts # and +.
6. In the **Other** part, you can configure the logging level of the MQTT connector, support error, warn, info, debug, trace 5 levels, the default value is info.
7. on the **MQTT Payload Parser** part, you can configure how to parse MQTT messages:
    - The first line of the configuration table is the ts field, which is of type TIMESTAMP and its value is the time the MQTT message was received by the MQTT connector;
    - The second line of the configuration table is the topic field, which is the subject name of the message and can optionally be synchronized to the TDengine as a column or label; the third line of the configuration table is the qos field.
    - The third line of the configuration table is the qos field, which is the QoS attribute of the message, and can optionally be synchronized to TDengine as a column or a label.
    - The remaining configuration items are custom fields, each of which needs to be configured: field (source), column (target), column type (target). Field (source) is the name of the field in the MQTT message, currently only supports MQTT message synchronization of JSON type, you can use JSON Path syntax to extract the field from the MQTT message, e.g. $.data.id; Column (target) is the name of the field after synchronization to TDengine; Column Type (target) is the type of the field after synchronization to TDengine. field type (target) is the field type after synchronization to TDengine, which can be selected from the drop-down list; the next field can be added when and only when the above three configurations are filled in;
    - If the timestamp is included in the MQTT message, you can choose to add a new custom field and use it as the primary key when synchronizing to TDengine; it should be noted that the timestamp in the MQTT message only supports the Unix Timestamp format, and the selection of the column type (target) of this field needs to be the same as that configured during the creation of the TDengine database;
    - STable Name: used to configure the name of the sub-table, using the format of "prefix + {column type (target)}", for example, d{id}.
    - Super Table Name: used to configure the super table name when synchronizing to TDengine;
8. In the **Authentication** part, enter the user name and password of the MQTT connector when accessing the MQTT server. These two fields are optional, if you do not enter them, it means that anonymous authentication is used.
9. After completing the above information, click the **Add** button to start the data synchronization from MQTT to TDengine directly.
