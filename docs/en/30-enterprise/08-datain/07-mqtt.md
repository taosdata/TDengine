---
title: "Mqtt"
sidebar_label: "Mqtt"
---

This section explains how to create a data migration task through the Explorer interface to migrate data from MQTT to
the current TDengine cluster.

## Functional Overview

MQTT stands for Message Queuing Telemetry Transport. It is a lightweight messaging protocol that is easy to implement
and use.

TDengine can efficiently read the data from MQTT and write to TDengine to achieve historical data migration or real-time
data streaming.

## Create Task

### 1. Add Source

In the **Data In** page，click **+Add Source** button to enter the data source page.

![mqtt-01.png](./mqtt-01.png)

### 2. Configure Basic information

In the **Name** field, enter the task name, such as "test_mqtt";

In the **Type** drop-down list, select **MQTT**.

**Agent** is not required, if necessary, you can select the specified agent from the drop-down box, you can also click
the right **+ Create New Agent** button [Create New Agent](#Create New Agent).

Select a target database from the **Target DB** drop-down list, or click the **+Create Database** button on the right
[Create Database](#Create Database).

![mqtt-02.png](./mqtt-02.png)

### 3. Configure Connection and Authentication Information

In the **MQTT Address** field, fill in the address of the MQTT broker, for example: `192.168.1.42:1833`.

In the **Username** field, fill in the username of the MQTT broker.

In the **Password** field, fill in the password of the MQTT broker.

Click the **Check Connection** button to check whether the data source is available.

![mqtt-03.png](./mqtt-03.png)

### 4. Configure SSL Certificate

If the MQTT broker uses an SSL certificate, you need to upload the certificate file in **SSL Certificate**.

![mqtt-04.png](./mqtt-04.png)

### 5. Configure Collection

In the **Collect** area, fill in the configuration parameters related to the collection task.

In the **MQTT Protocol** drop-down list, select the MQTT protocol version. There are three
options: `3.1`, `3.1.1`, `5.0`.
The default value is 3.1.

In the **Client ID** field, fill in the client ID.

In the **Keep Alive** field, enter the keep alive interval. If the proxy does not receive any messages from the client
within the keep alive interval, it will assume that the client has disconnected and close the connection.
The keep alive interval is the time interval negotiated between the client and the proxy to detect whether the client is
active. If the client does not send a message to the proxy within the keep alive interval, the proxy will disconnect.

In the **Clean Session** field, select whether to clear the session. The default value is true.

In the **Subscribe Topic and QoS Configuration** field, fill in the Topic name to be consumed. Use the following format:
`topic1::0,topic2::1`.

![mqtt-05.png](./mqtt-05.png)

### 6. Configure MQTT Payload

In the **MQTT Payload Parser** area, fill in the configuration parameters related to the payload parsing.
MQTT Souce will upload the following fields:

* ts: timestamp of the collection.
* topic: the topic name to subscribe.
* qos: the topic qos.
* payload: the data payload of the message.

taosX can extract JSON-formatted data from value and then split it into new columns.

In the **Message Body** field, fill in the sample data in the Kafka message body, for example:
`{"id": 1, "message": "hello""}`. This sample data will be used to configure the extraction and filtering conditions
later.

![mqtt-06.png](./mqtt-06.png)

In the **Extract or Split From A column** field, fill in the fields to be extracted or split from the message body, for
example: split the value field into `id` and `message` fields, select the json extractor, and configure the expression
as
`id::int;message::binary`.

Click the **Check** button to view the split result.

Click the **Delete** button to delete the current extraction rule.

Click the **Add** button to add more extraction rules.

![mqtt-07.png](./mqtt-07.png)

In the **Filter** field, fill in the filter conditions, for example: fill in `id != 0`, then only the data with id not
equal to 0 will be written to TDengine.

Click the **Check** button to view the filter result.

Click the **Delete** button to delete the current filter rule.

Click the **Add** button to add more filter rules.

![mqtt-08.png](./mqtt-08.png)

In the **Target Super Table** drop-down list, select a target super table, or click the **+Create STable** button on the
right to [Create Super Table](#Create STable).

In the **Mapping** area, fill in the sub-table name in the target super table, for example: `t_{id}`.

Click the **Calculate** button to view the mapping result.

![mqtt-09.png](./mqtt-09.png)

### 7. Advanced Settings

In the **Log Level** drop-down list, select the log level. There are five
options: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`.
The default value is INFO.

![mqtt-10](./mqtt-10.png)

### 8. Finish

Click the **Add** button to complete the creation of the MQTT to TDengine data synchronization task.

## View Task Status

After submitting the task, you can go back to the data source page to view the task status. The task is first added to
the execution queue and will start running later.

![mqtt-11](./mqtt-11.png)

After the task starts running, you can click the **View** button to monitor the dynamic statistics of the task.
You can also click the **View** button on the right to view the task details.

![mqtt-12](./mqtt-12.png)
