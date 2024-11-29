---
title: EMQX Platform
description: Writing to TDengine Using EMQX Broker
slug: /third-party-tools/data-collection/emqx-platform
---

import Image from '@theme/IdealImage';
import imgStep01 from '../../assets/emqx-platform-01.png';
import imgStep02 from '../../assets/emqx-platform-02.png';
import imgStep03 from '../../assets/emqx-platform-03.png';
import imgStep04 from '../../assets/emqx-platform-04.png';
import imgStep05 from '../../assets/emqx-platform-05.png';
import imgStep06 from '../../assets/emqx-platform-06.png';
import imgStep07 from '../../assets/emqx-platform-07.png';
import imgStep08 from '../../assets/emqx-platform-08.png';
import imgStep09 from '../../assets/emqx-platform-09.png';
import imgStep10 from '../../assets/emqx-platform-10.png';
import imgStep11 from '../../assets/emqx-platform-11.png';

MQTT is a popular data transmission protocol for the Internet of Things, and [EMQX](https://github.com/emqx/emqx) is an open-source MQTT Broker software. Without any code, you can use the "Rules" feature in the EMQX Dashboard to perform simple configurations to directly write MQTT data into TDengine. EMQX supports saving data to TDengine via sending to web services and also provides a native TDengine driver implementation for direct saving in the enterprise edition.

## Prerequisites

To enable EMQX to add TDengine as a data source, the following preparations are needed:

- The TDengine cluster has been deployed and is running normally.
- The taosAdapter has been installed and is running normally. For details, please refer to the [taosAdapter User Manual](../../../tdengine-reference/components/taosadapter/).
- If you are using the simulated writing program mentioned later, you need to install a compatible version of Node.js, preferably version v12.

## Install and Start EMQX

Users can download the installation package from the [EMQX official website](https://www.emqx.com/en/downloads-and-install/broker) according to their operating system and execute the installation. After installation, start the EMQX service using `sudo emqx start` or `sudo systemctl start emqx`.

Note: This document is based on EMQX version v4.4.5; other versions may have different configuration interfaces, methods, and features due to version upgrades.

## Create Database and Table

In TDengine, create the corresponding database and table structure to receive MQTT data. Enter the TDengine CLI and execute the following SQL statements:

```sql
CREATE DATABASE test;
USE test;
CREATE TABLE sensor_data (ts TIMESTAMP, temperature FLOAT, humidity FLOAT, volume FLOAT, pm10 FLOAT, pm25 FLOAT, so2 FLOAT, no2 FLOAT, co FLOAT, sensor_id NCHAR(255), area TINYINT, coll_time TIMESTAMP);
```

## Configure EMQX Rules

Since the configuration interface differs across EMQX versions, the example below uses version v4.4.5; for other versions, please refer to the relevant official documentation.

### Log into EMQX Dashboard

Open your browser and navigate to `http://IP:18083` to log into the EMQX Dashboard. The default username is `admin` and the password is `public`.

<figure>
<Image img={imgStep01} alt=""/>
</figure>

### Create Rule

Select "Rule" under the "Rule Engine" on the left and click the "Create" button:

<figure>
<Image img={imgStep02} alt=""/>
</figure>

### Edit SQL Field

Copy the following content into the SQL edit box:

```sql
SELECT
  payload
FROM
  "sensor/data"
```

Here, `payload` represents the entire message body, and `sensor/data` is the message topic selected for this rule.

<figure>
<Image img={imgStep03} alt=""/>
</figure>

### Add Action Handler

<figure>
<Image img={imgStep04} alt=""/>
</figure>

### Add Resource

<figure>
<Image img={imgStep05} alt=""/>
</figure>

Select "Send Data to Web Service" and click the "Create Resource" button:

### Edit Resource

Select "WebHook" and fill in the "Request URL" with the address for taosAdapter providing REST services. If taosAdapter is running locally, the default address is `http://127.0.0.1:6041/rest/sql`.

Keep the other attributes at their default values.

<figure>
<Image img={imgStep06} alt=""/>
</figure>

### Edit Action

In the resource configuration, add the key/value pair for Authorization authentication. The default Authorization value corresponding to the username and password is:

```text
Basic cm9vdDp0YW9zZGF0YQ==
```

For more information, please refer to the [TDengine REST API documentation](../../../tdengine-reference/client-libraries/rest-api/).

In the message body, input the rule engine replacement template:

```sql
INSERT INTO test.sensor_data VALUES(
  now,
  ${payload.temperature},
  ${payload.humidity},
  ${payload.volume},
  ${payload.PM10},
  ${payload.pm25},
  ${payload.SO2},
  ${payload.NO2},
  ${payload.CO},
  '${payload.id}',
  ${payload.area},
  ${payload.ts}
)
```

<figure>
<Image img={imgStep07} alt=""/>
</figure>

Finally, click the "Create" button at the bottom left to save the rule.

## Write Simulation Test Program

```javascript
{{#include docs/examples/other/mock.js}}
```

Note: You can initially set a smaller value for CLIENT_NUM in the code during the test to avoid overwhelming hardware performance with a large number of concurrent clients.

<figure>
<Image img={imgStep08} alt=""/>
</figure>

## Execute Test to Simulate Sending MQTT Data

```shell
npm install mqtt mockjs --save --registry=https://registry.npm.taobao.org
node mock.js
```

<figure>
<Image img={imgStep09} alt=""/>
</figure>

## Verify EMQX Received Data

Refresh the rule engine interface in the EMQX Dashboard to see how many records were correctly received:

<figure>
<Image img={imgStep10} alt=""/>
</figure>

## Verify Data Written to TDengine

Log into TDengine CLI and query the corresponding database and table to verify whether the data has been correctly written to TDengine:

<figure>
<Image img={imgStep11} alt=""/>
</figure>

For detailed usage of EMQX, please refer to the [EMQX Official Documentation](https://docs.emqx.com/en/emqx/latest/data-integration/rules.html#rule-engine).
