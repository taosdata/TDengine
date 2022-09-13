---
sidebar_label: REST and Schemaless
title: REST and Schemaless
description: Insert data using REST API or Schemaless
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

In this section we will explain how to write into TDengine cloud service using REST API or schemaless protocols over REST interface.

## REST API

### Config

Run this command in your terminal to save the TDengine cloud token and URL as variables:

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_TOKEN="<token>"
export TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_CLOUD_TOKEN="<token>"
set TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_TOKEN="<token>"
$env:TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
</Tabs>

### Insert Data using REST API

Following command below show how to insert data into the table `d1001` of the database `test` via the command line utility `curl`.

```bash
curl -L \
  -d "INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31)" \
  $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
```

## Schemaless

### InfluxDB Line Protocol

You can use any client that supports the http protocol to access the RESTful interface address `<cloud_url>/influxdb/v1/write` to write data in InfluxDB compatible format to TDengine. The EndPoint is as follows:

```text
/influxdb/v1/write?db=<db_name>&token=<cloud_token>
```

Support InfluxDB query parameters as follows.

- `db` Specifies the database name used by TDengine
- `precision` The time precision used by TDengine

Note: InfluxDB token authorization is not supported at present. Only Basic authorization and query parameter validation are supported.

### OpenTSDB Json and Telnet Protocol

You can use any client that supports the http protocol to access the RESTful interface address `<cloud_url>/opentsdb/v1/put` to write data in OpenTSDB compatible format to TDengine. The EndPoint is as follows:

```text
/opentsdb/v1/put/json/<db>?token=<cloud_token>
/opentsdb/v1/put/telnet/<db>?token=<cloud_token>
```
