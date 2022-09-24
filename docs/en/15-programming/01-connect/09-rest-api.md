---
sidebar_label: REST and Schemaless
title: REST and Schemaless
description: Connect to TDengine Cloud Service through RESTful API or Schemaless
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## Config

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

## Usage

The TDengine REST API is based on standard HTTP protocol and provides an easy way to access TDengine. As an example, the code below is to construct an HTTP request with the URL, the token and an SQL command and run it with the command line utility `curl`.

```bash
curl -L \
  -d "select name, ntables, status from information_schema.ins_databases;" \
  $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
```

## Schemaless

### InfluxDB Line Protocol

You can use any client that supports the http protocol to access the RESTful interface address `${TDENGINE_CLOUD_URL}/influxdb/v1/write` to write data in InfluxDB compatible format to TDengine. The EndPoint is as follows:

```text
/influxdb/v1/write?db=<DB_NAME>&token=${TDENGINE_CLOUD_TOKEN}
```

Support InfluxDB query parameters as follows.

- `db` Specifies the database name used by TDengine
- `precision` The time precision used by TDengine

Note: InfluxDB token authorization is not supported at present. Only Basic authorization and query parameter validation are supported.

### OpenTSDB Json and Telnet Protocol

You can use any client that supports the http protocol to access the RESTful interface address `${TDENGINE_CLOUD_URL}/opentsdb/v1/put` to write data in OpenTSDB compatible format to TDengine. The EndPoint is as follows:

```text
/opentsdb/v1/put/json/<db>?token=${TDENGINE_CLOUD_TOKEN}
/opentsdb/v1/put/telnet/<db>?token=${TDENGINE_CLOUD_TOKEN}
```

