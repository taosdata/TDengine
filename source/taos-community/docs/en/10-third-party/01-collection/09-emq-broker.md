---
title: EMQX Platform
slug: /third-party-tools/data-collection/emqx-platform
---

MQTT is a popular IoT data transmission protocol, and [EMQX](https://github.com/emqx/emqx) is an open-source MQTT Broker software. Without any coding, you can directly write MQTT data into TDengine by simply configuring "rules" in the EMQX Dashboard. EMQX supports saving data to TDengine by sending it to a web service and also provides a native TDengine driver in the enterprise version for direct saving.

## Prerequisites

To enable EMQX to properly add a TDengine data source, the following preparations are needed:

- TDengine cluster is deployed and running normally
- taosAdapter is installed and running normally. For details, please refer to [taosAdapter User Manual](../../../tdengine-reference/components/taosadapter)
- If using the simulation writing program mentioned later, install the appropriate version of Node.js, version 12 recommended

## Install and Start EMQX

Users can download the installation package from the [EMQX official website](https://www.emqx.io/zh/downloads) according to their operating system and execute the installation. After installation, start the EMQX service using `sudo emqx start` or `sudo systemctl start emqx`.

Note: This article is based on EMQX v4.4.5. Other versions may differ in configuration interface, configuration methods, and features as the version upgrades.

## Create Database and Table

Create the corresponding database and table structure in TDengine to receive MQTT data. Enter the TDengine CLI and copy and execute the following SQL statement:

```sql
CREATE DATABASE test;
USE test;
CREATE TABLE sensor_data (ts TIMESTAMP, temperature FLOAT, humidity FLOAT, volume FLOAT, pm10 FLOAT, pm25 FLOAT, so2 FLOAT, no2 FLOAT, co FLOAT, sensor_id NCHAR(255), area TINYINT, coll_time TIMESTAMP);
```

## Configure EMQX Rules

Since the configuration interface differs across EMQX versions, this section is only an example for v4.4.5. For other versions, please refer to the respective official documentation.

### Log in to EMQX Dashboard

Open the URL `http://IP:18083` in a browser and log in to the EMQX Dashboard. The initial username is `admin` and the password is: `public`.

![Log in to EMQX dashboard](../../assets/emqx-platform-01.png)

### Create a Rule (Rule)

Select "Rule Engine (Rule Engine)" on the left, then "Rule (Rule)" and click the "Create (Create)" button:

![Create a rule](../../assets/emqx-platform-02.png)

### Edit SQL Field

Copy the following content into the SQL edit box:

```sql
SELECT
  payload
FROM
  "sensor/data"
```

Where `payload` represents the entire message body, `sensor/data` is the message topic selected for this rule.

![Edit SQL](../../assets/emqx-platform-03.png)

### Add "Action Handler (action handler)"

![Add action handler](../../assets/emqx-platform-04.png)

### Add "Resource (Resource)"

![Add resource](../../assets/emqx-platform-05.png)

Select "Send Data to Web Service" and click the "Create Resource" button:

### Edit "Resource"

Select "WebHook" and fill in the "Request URL" with the address provided by taosAdapter for REST services. If taosadapter is started locally, the default address is `http://127.0.0.1:6041/rest/sql`.

Please keep other properties at their default values.

![Edit resource](../../assets/emqx-platform-06.png)

### Edit "Action"

Edit the resource configuration, adding an Authorization key/value pair. The default username and password corresponding Authorization value is:

```text
Basic cm9vdDp0YW9zZGF0YQ==
```

For related documentation, please refer to [TDengine REST API Documentation](../../../tdengine-reference/client-libraries/rest-api/).

Enter the rule engine replacement template in the message body:

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

![Edit action](../../assets/emqx-platform-07.png)

Finally, click the "Create" button at the bottom left to save the rule.

## Write a Mock Test Program

```js
{{#include docs/examples/other/mock.js}}
```

Note: In the code, CLIENT_NUM can be set to a smaller value at the start of the test to avoid hardware performance not being able to fully handle a large number of concurrent clients.

![CLIENT_NUM in test program](../../assets/emqx-platform-08.png)

## Execute Test Simulation Sending MQTT Data

```shell
npm install mqtt mockjs --save --registry=https://registry.npm.taobao.org
node mock.js
```

![Run test simulation](../../assets/emqx-platform-09.png)

## Verify EMQX Received Data

Refresh the EMQX Dashboard rule engine interface to see how many records were correctly received:

![Verify that data is received by EMQX](../../assets/emqx-platform-10.png)

## Verify Data Written to TDengine

Use the TDengine CLI program to log in and query the relevant database and table to verify that the data has been correctly written to TDengine:

![Verify that data is written to TDengine](../../assets/emqx-platform-11.png)

For detailed usage of EMQX, please refer to [EMQX Official Documentation](https://docs.emqx.com/en/emqx/v4.4/rule/rule-engine.html).
