---
title: MQTT
slug: /advanced-features/data-connectors/mqtt
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/mqtt-01.png';
import imgStep02 from '../../assets/mqtt-02.png';
import imgStep03 from '../../assets/mqtt-03.png';
import imgStep04 from '../../assets/mqtt-04.png';
import imgStep05 from '../../assets/mqtt-05.png';
import imgStep06 from '../../assets/mqtt-06.png';
import imgStep07 from '../../assets/mqtt-07.png';
import imgStep08 from '../../assets/mqtt-08.png';
import imgStep09 from '../../assets/mqtt-09.png';
import imgStep10 from '../../assets/mqtt-10.png';
import imgStep11 from '../../assets/mqtt-11.png';
import imgStep12 from '../../assets/mqtt-12.png';
import imgStep13 from '../../assets/mqtt-13.png';
import imgStep14 from '../../assets/mqtt-14.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create data migration tasks through the Explorer interface, migrating data from MQTT to the current TDengine cluster.

## Overview

MQTT stands for Message Queuing Telemetry Transport. It is a lightweight messaging protocol that is easy to implement and use.

TDengine can subscribe to data from an MQTT broker via an MQTT connector and write it into TDengine, enabling real-time data streaming.

## Creating a Task

### 1. Add a Data Source

On the data writing page, click the **+Add Data Source** button to enter the add data source page.

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

Enter the task name in **Name**, such as: "test_mqtt";

Select **MQTT** from the **Type** dropdown list.

**Broker** is optional, you can select a specific broker from the dropdown list or click the **+Create New Broker** button on the right.

Select a target database from the **Target Database** dropdown list, or click the **+Create Database** button on the right.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection and Authentication Information

Enter the MQTT broker's address in **MQTT Address**, for example: `192.168.1.42`

Enter the MQTT broker's port in **MQTT Port**, for example: `1883`

Enter the MQTT broker's username in **User**.

Enter the MQTT broker's password in **Password**.

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure SSL Certificate

If the MQTT broker uses an SSL certificate, upload the certificate file in **SSL Certificate**.

<figure>
<Image img={imgStep04} alt=""/>
</figure>

### 5. Configure Collection Information

Fill in the collection task related configuration parameters in the **Collection Configuration** area.

Select the MQTT protocol version from the **MQTT Protocol** dropdown list. There are three options: `3.1`, `3.1.1`, `5.0`. The default value is 3.1.

Enter the client identifier in **Client ID**, after which a client id with the prefix `taosx` will be generated (for example, if the identifier entered is `foo`, the generated client id will be `taosxfoo`). If the switch at the end is turned on, the current task's task id will be concatenated after `taosx` and before the entered identifier (the generated client id will look like `taosx100foo`). All client ids connecting to the same MQTT address must be unique.

Enter the keep alive interval in **Keep Alive**. If the broker does not receive any message from the client within the keep alive interval, it will assume the client has disconnected and will close the connection.
The keep alive interval is the time interval negotiated between the client and the broker to check if the client is active. If the client does not send a message to the broker within the keep alive interval, the broker will disconnect.

In **Clean Session**, choose whether to clear the session. The default value is true.

In the **Topics Qos Config**, fill in the topic name and QoS to subscribe. Use the following format: `{topic_name}::{qos}` (e.g., `my_topic::0`). MQTT protocol 5.0 supports shared subscriptions, allowing multiple clients to subscribe to the same topic for load balancing. Use the following format: `$share/{group_name}/{topic_name}::{qos}`, where `$share` is a fixed prefix indicating the enablement of shared subscription, and `group_name` is the client group name, similar to Kafka's consumer group.

In the **Topic Analysis**, fill in the MQTT topic parsing rules. The format is the same as the MQTT Topic, parsing each level of the MQTT Topic into corresponding variable names, with `_` indicating that the current level is ignored during parsing. For example: if the MQTT Topic `a/+/c` corresponds to the parsing rule `v1/v2/_`, it means assigning the first level `a` to variable `v1`, the value of the second level (where the wildcard `+` represents any value) to variable `v2`, and ignoring the value of the third level `c`, which will not be assigned to any variable. In the `payload parsing` below, the variables obtained from Topic parsing can also participate in various transformations and calculations.

In the **Compression**, configure the message body compression algorithm. After receiving the message, taosX uses the corresponding compression algorithm to decompress the message body and obtain the original data. Options include none (no compression), gzip, snappy, lz4, and zstd, with the default being none.

In the **Char Encoding**, configure the message body encoding format. After receiving the message, taosX uses the corresponding encoding format to decode the message body and obtain the original data. Options include UTF_8, GBK, GB18030, and BIG5, with the default being UTF_8.

Click the **Check Connection** button to check if the data source is available.

<figure>
<Image img={imgStep05} alt=""/>
</figure>

### 6. Configure MQTT Payload Parsing

Fill in the Payload parsing related configuration parameters in the **MQTT Payload Parsing** area.

taosX can use a JSON extractor to parse data and allows users to specify the data model in the database, including specifying table names and supertable names, setting ordinary columns and tag columns, etc.

#### 6.1 Parsing

There are three methods to obtain sample data:

Click the **Retrieve from Server** button to get sample data from MQTT.

Click the **File Upload** button to upload a CSV file and obtain sample data.

Fill in the example data from the MQTT message body in **Message Body**.

JSON data supports JSONObject or JSONArray, and the json parser can parse the following data:

```json
{"id": 1, "message": "hello-word"}
{"id": 2, "message": "hello-word"}
```

or

```json
[{"id": 1, "message": "hello-word"},{"id": 2, "message": "hello-word"}]
```

The analysis results are as follows:

<figure>
<Image img={imgStep06} alt=""/>
</figure>

Click the **magnifying glass icon** to view the preview of the analysis results.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

#### 6.2 Field Splitting

In **Extract or Split from Column**, fill in the fields to extract or split from the message body, for example: split the `message` field into `message_0` and `message_1`, select the split extractor, fill in the separator as -, and number as 2.

<figure>
<Image img={imgStep08} alt=""/>
</figure>

Click **Delete** to remove the current extraction rule.

Click **Add** to add more extraction rules.

Click the **magnifying glass icon** to view the preview of the extraction/split results.

<figure>
<Image img={imgStep09} alt=""/>
</figure>

#### 6.3 Data Filtering

In **Filter**, fill in the filtering conditions, for example: write `id != 1`, then only data with id not equal to 1 will be written to TDengine.

<figure>
<Image img={imgStep10} alt=""/>
</figure>

Click **Delete** to remove the current filtering rule.

Click the **magnifying glass icon** to view the preview of the filtering results.

<figure>
<Image img={imgStep11} alt=""/>
</figure>

#### 6.4 Table Mapping

In the **Target Supertable** dropdown, select a target supertable, or click the **Create Supertable** button on the right.

In **Mapping**, fill in the subtable name in the target supertable, for example: `t_{id}`. Fill in the mapping rules according to the requirements, where mapping supports setting default values.

<figure>
<Image img={imgStep12} alt=""/>
</figure>

Click **Preview** to view the mapping results.

<figure>
<Image img={imgStep13} alt=""/>
</figure>

### 7. Advanced Options

In the **Log Level** dropdown, select a log level. There are five options: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`. The default is INFO.

When **saving raw data**, the following two parameters are effective.

Set the maximum retention days for raw data in **Maximum Retention Days**.

Set the storage path for raw data in **Raw Data Storage Directory**.

<figure>
<Image img={imgStep14} alt=""/>
</figure>

### 8. Completion

Click the **Submit** button to complete the creation of the MQTT to TDengine data synchronization task, return to the **Data Source List** page to view the status of the task execution.
