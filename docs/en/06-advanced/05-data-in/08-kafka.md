---
title: Apache Kafka
sidebar_label: Kafka
slug: /advanced-features/data-connectors/kafka
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/kafka-01.png';
import imgStep02 from '../../assets/kafka-02.png';
import imgStep03 from '../../assets/kafka-03.png';
import imgStep04 from '../../assets/kafka-04.png';
import imgStep05 from '../../assets/kafka-05.png';
import imgStep06 from '../../assets/kafka-06.png';
import imgStep07 from '../../assets/kafka-07.png';
import imgStep08 from '../../assets/kafka-08.png';
import imgStep09 from '../../assets/kafka-09.png';
import imgStep10 from '../../assets/kafka-10.png';
import imgStep11 from '../../assets/kafka-11.png';
import imgStep12 from '../../assets/kafka-12.png';
import imgStep13 from '../../assets/kafka-13.png';
import imgStep14 from '../../assets/kafka-14.png';
import imgStep15 from '../../assets/kafka-15.png';
import imgStep16 from '../../assets/kafka-16.png';
import imgStep17 from '../../assets/kafka-17.png';
import imgStep18 from '../../assets/kafka-18.png';

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

This section describes how to create data migration tasks through the Explorer interface, migrating data from Kafka to the current TDengine cluster.

## Feature Overview

Apache Kafka is an open-source distributed streaming system used for stream processing, real-time data pipelines, and large-scale data integration.

TDengine can efficiently read data from Kafka and write it into TDengine, enabling historical data migration or real-time data streaming.

## Creating a Task

### 1. Add a Data Source

On the data writing page, click the **+Add Data Source** button to enter the add data source page.

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### 2. Configure Basic Information

Enter the task name in **Name**, such as: "test_kafka";

Select **Kafka** from the **Type** dropdown list.

**Proxy** is optional; if needed, you can select a specific proxy from the dropdown, or click **+Create New Proxy** on the right.

Select a target database from the **Target Database** dropdown list, or click the **+Create Database** button on the right.

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### 3. Configure Connection Information

**bootstrap-server**, for example: `192.168.1.92`.

**Service Port**, for example: `9092`.

When there are multiple broker addresses, add a **+Add Broker** button at the bottom right of the connection settings to add bootstrap-server and service port pairs.

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### 4. Configure SASL Authentication Mechanism

If the server has enabled SASL authentication, you need to enable SASL here and configure the relevant content. Currently, three authentication mechanisms are supported: PLAIN/SCRAM-SHA-256/GSSAPI. Please choose according to the actual situation.

#### 4.1. PLAIN Authentication

Select the `PLAIN` authentication mechanism and enter the username and password:

<figure>
<Image img={imgStep04} alt=""/>
</figure>

#### 4.2. SCRAM (SCRAM-SHA-256) Authentication

Select the `SCRAM-SHA-256` authentication mechanism and enter the username and password:

<figure>
<Image img={imgStep05} alt=""/>
</figure>

#### 4.3. GSSAPI Authentication

Select `GSSAPI`, which will use the [RDkafka client](https://github.com/confluentinc/librdkafka) to invoke the GSSAPI applying Kerberos authentication mechanism:

<figure>
<Image img={imgStep06} alt=""/>
</figure>

The required information includes:

- Kerberos service name, usually `kafka`;
- Kerberos authentication principal, i.e., the authentication username, such as `kafkaclient`;
- Kerberos initialization command (optional, generally not required);
- Kerberos keytab, you need to provide and upload the file;

All the above information must be provided by the Kafka service manager.

In addition, the [Kerberos](https://web.mit.edu/kerberos/) authentication service needs to be configured on the server. Use `apt install krb5-user` on Ubuntu; on CentOS, use `yum install krb5-workstation`.

After configuration, you can use the [kcat](https://github.com/edenhill/kcat) tool to verify Kafka topic consumption:

```shell
kcat <topic> \
  -b <kafka-server:port> \
  -G kcat \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanism=GSSAPI \
  -X sasl.kerberos.keytab=</path/to/kafkaclient.keytab> \
  -X sasl.kerberos.principal=<kafkaclient> \
  -X sasl.kerberos.service.name=kafka
```

If an error occurs: "Server xxxx not found in kerberos database", you need to configure the domain name corresponding to the Kafka node and configure reverse DNS resolution `rdns = true` in the Kerberos client configuration file `/etc/krb5.conf`.

### 5. Configure SSL Certificate

If the server has enabled SSL encryption authentication, SSL needs to be enabled here and related content configured.

<figure>
<Image img={imgStep07} alt=""/>
</figure>

### 6. Configure Collection Information

Fill in the configuration parameters related to the collection task in the **Collection Configuration** area.

Enter the timeout duration in **Timeout**. If no data is consumed from Kafka, and the timeout is exceeded, the data collection task will exit. The default value is 0 ms. When the timeout is set to 0, it will wait indefinitely until data becomes available or an error occurs.

Enter the Topic name to be consumed in **Topic**. Multiple Topics can be configured, separated by commas. For example: `tp1,tp2`.

Enter the client identifier in **Client ID**. After entering, a client ID with the prefix `taosx` will be generated (for example, if the identifier entered is `foo`, the generated client ID will be `taosxfoo`). If the switch at the end is turned on, the current task's task ID will be concatenated after `taosx` and before the entered identifier (the generated client ID will look like `taosx100foo`). Note that when using multiple taosX subscriptions for the same Topic to achieve load balancing, a consistent client ID must be entered to achieve the balancing effect.

Enter the consumer group identifier in **Consumer Group ID**. After entering, a consumer group ID with the prefix `taosx` will be generated (for example, if the identifier entered is `foo`, the generated consumer group ID will be `taosxfoo`). If the switch at the end is turned on, the current task's task ID will be concatenated after `taosx` and before the entered identifier (the generated consumer group ID will look like `taosx100foo`).

In the **Offset** dropdown, select from which Offset to start consuming data. There are three options: `Earliest`, `Latest`, `ByTime(ms)`. The default is Earliest.

- Earliest: Requests the earliest offset.
- Latest: Requests the latest offset.

Set the maximum duration to wait for insufficient data when fetching messages in **Maximum Duration to Fetch Data** (in milliseconds), the default value is 100ms.

Click the **Connectivity Check** button to check if the data source is available.

<figure>
<Image img={imgStep08} alt=""/>
</figure>

### 7. Configure Payload Parsing

Fill in the configuration parameters related to Payload parsing in the **Payload Parsing** area.

#### 7.1 Parsing

There are three methods to obtain sample data:

Click the **Retrieve from Server** button to get sample data from Kafka.

Click the **File Upload** button to upload a CSV file and obtain sample data.

Enter sample data from the Kafka message body in **Message Body**.

JSON data supports JSONObject or JSONArray, and the following data can be parsed using a JSON parser:

```json
{"id": 1, "message": "hello-word"}
{"id": 2, "message": "hello-word"}
```

or

```json
[{"id": 1, "message": "hello-word"},{"id": 2, "message": "hello-word"}]
```

The parsing results are shown as follows:

<figure>
<Image img={imgStep09} alt=""/>
</figure>

Click the **magnifying glass icon** to view the preview parsing results.

<figure>
<Image img={imgStep10} alt=""/>
</figure>

#### 7.2 Field Splitting

In **Extract or Split from Columns**, fill in the fields to extract or split from the message body, for example: split the message field into `message_0` and `message_1`, select the split extractor, fill in the separator as -, and number as 2.

Click **Add** to add more extraction rules.

Click **Delete** to delete the current extraction rule.

<figure>
<Image img={imgStep11} alt=""/>
</figure>

Click the **magnifying glass icon** to view the preview extraction/splitting results.

<figure>
<Image img={imgStep12} alt=""/>
</figure>

#### 7.3 Data Filtering

In **Filter**, fill in the filtering conditions, for example: enter `id != 1`, then only data with id not equal to 1 will be written to TDengine.

Click **Add** to add more filtering rules.

Click **Delete** to delete the current filtering rule.

<figure>
<Image img={imgStep13} alt=""/>
</figure>

Click the **magnifying glass icon** to view the preview filtering results.

<figure>
<Image img={imgStep14} alt=""/>
</figure>

#### 7.4 Table Mapping

In the **Target Supertable** dropdown, select a target supertable, or click the **Create Supertable** button on the right.

In the **Mapping** section, fill in the name of the subtable in the target supertable, for example: `t_{id}`. Fill in the mapping rules as required, where mapping supports setting default values.

<figure>
<Image img={imgStep15} alt=""/>
</figure>

Click **Preview** to view the results of the mapping.

<figure>
<Image img={imgStep16} alt=""/>
</figure>

### 8. Configure Advanced Options

The **Advanced Options** area is collapsed by default, click the `>` on the right to expand it, as shown below:

<figure>
<Image img={imgStep17} alt=""/>
</figure>

<figure>
<Image img={imgStep18} alt=""/>
</figure>

### 9. Completion of Creation

Click the **Submit** button to complete the creation of the Kafka to TDengine data synchronization task. Return to the **Data Source List** page to view the status of the task execution.
